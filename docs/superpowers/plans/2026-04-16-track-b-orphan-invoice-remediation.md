# Track B: Production Data Containment & Orphan Invoice Remediation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop orphan QuickBooks invoices (rows in `invoices` with `job_id IS NULL`) from distorting production analytics, repair the ones we can, quarantine the ones we can't, and add an integrity check that catches regressions going forward.

**Architecture:** Three concentric layers of defense. (1) The canonical booked-revenue queries in [intelligence/metrics/revenue.py](intelligence/metrics/revenue.py) already INNER-JOIN `invoices` to `jobs`, so orphans are excluded by construction — we harden this with an explicit filter and a regression test. (2) Extend the existing [scripts/remediate_reconciliation_invoices.py](scripts/remediate_reconciliation_invoices.py) with a new invoice-first sweep that finds orphans and backlinks them to jobs (the current script is job-first and misses orphans whose matching jobs are already linked to a different invoice). (3) A new `intelligence/metrics/integrity.py` module that produces a daily alert bundle: orphan-invoice count, unlinked payments, and jobs completed >24h ago without an invoice.

**Tech Stack:** Python 3.11, PostgreSQL via psycopg2 (`%s` placeholders, `RealDictRow`), existing `auth.get_client("quickbooks")` for QBO reads, `pytest` for tests with a fixture DB.

**Scope boundaries:**
- This plan does NOT rename `revenue` → `booked_revenue` (Track A; already partially landed — shim present in [intelligence/metrics/__init__.py](intelligence/metrics/__init__.py)).
- This plan does NOT fix service-type pricing (Track C) or payment timing (Track D).
- Phase 0 blocker per [docs/revenue-remediation-plan-2026-04.md:267](docs/revenue-remediation-plan-2026-04.md#L267): Task 1 (audit) and Task 2 (diagnosis doc) must land and be reviewed before Task 4 (remediation execution).

---

## File Structure

**Create:**
- `scripts/audit_orphan_invoices.py` — read-only diagnostic + recurring audit report (dual purpose: Phase 0 diagnosis and Track B action #4).
- `intelligence/metrics/integrity.py` — daily data-integrity checks: orphan invoices, unlinked payments, jobs-without-invoice >24h.
- `docs/diagnosis-2026-04-09-invoice-spike.md` — human-written findings doc from Task 2.
- `tests/test_audit_orphan_invoices.py` — unit tests for the audit script helpers.
- `tests/test_metrics_integrity.py` — unit tests for the integrity metrics.
- `tests/test_revenue_orphan_exclusion.py` — regression test that orphan invoices never count toward booked revenue.

**Modify:**
- `scripts/remediate_reconciliation_invoices.py` — add invoice-first orphan sweep (`_fetch_orphan_invoices`, `_match_orphan_to_job`, wired into `remediate()` via a new `--mode` flag). Preserve existing `_pick_unique_candidate` ambiguity guards.
- `intelligence/metrics/revenue.py` — add explicit `AND i.job_id IS NOT NULL` defensive filter in `_sum_booked`, `_segment_booked`, and any other invoice aggregations (defense in depth even though JOIN already excludes them).
- `intelligence/metrics/__init__.py` — add `compute_integrity(db, date_str)` export and include it in the returned bundle.
- `intelligence/runner.py` — call `compute_integrity`, route alerts to `#operations` channel via `_ALERT_CHANNEL_MAP`.
- `tests/test_invoice_remediation.py` — add unit tests for the new orphan-sweep helpers.

**Do not touch:** `automations/job_completion_flow.py` (Track C), `simulation/generators/payments.py` (Track D), `intelligence/syncers/sync_quickbooks.py` (the orphan-creation behavior there is root cause but fixing it belongs to a separate sync-hardening plan — for Track B we accept orphans as a fact and clean up after them).

---

## Task 1: Read-only audit script for orphan invoices

**Purpose:** Build the diagnostic tool. Used first to diagnose the 2026-04-09 spike (Task 2) and then kept as the one-time audit report required by Track B action #4.

**Files:**
- Create: `scripts/audit_orphan_invoices.py`
- Test: `tests/test_audit_orphan_invoices.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_audit_orphan_invoices.py`:

```python
from datetime import date
from scripts import audit_orphan_invoices as audit


def test_classify_orphan_flags_qbo_mapped_no_job():
    row = {
        "id": "SS-INV-9001",
        "job_id": None,
        "quickbooks_invoice_id": "123456",
        "client_id": "SS-CLIENT-0042",
        "issue_date": "2026-04-09",
        "amount": 150.0,
    }
    assert audit._classify_orphan(row) == "qbo_mapped_no_job"


def test_classify_orphan_flags_local_only():
    row = {
        "id": "SS-INV-9002",
        "job_id": None,
        "quickbooks_invoice_id": None,
        "client_id": "SS-CLIENT-0042",
        "issue_date": "2026-04-09",
        "amount": 150.0,
    }
    assert audit._classify_orphan(row) == "local_only"


def test_group_by_day_sums_amounts():
    rows = [
        {"issue_date": "2026-04-09", "amount": 150.0, "classification": "qbo_mapped_no_job"},
        {"issue_date": "2026-04-09", "amount": 275.0, "classification": "qbo_mapped_no_job"},
        {"issue_date": "2026-04-10", "amount": 135.0, "classification": "local_only"},
    ]
    summary = audit._group_by_day(rows)
    assert summary["2026-04-09"]["count"] == 2
    assert summary["2026-04-09"]["amount"] == 425.0
    assert summary["2026-04-10"]["count"] == 1
```

- [ ] **Step 2: Run tests to confirm they fail**

Run: `python -m pytest tests/test_audit_orphan_invoices.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'scripts.audit_orphan_invoices'`.

- [ ] **Step 3: Implement the audit script**

Create `scripts/audit_orphan_invoices.py`:

```python
#!/usr/bin/env python3
"""Read-only audit of orphan invoices (invoices.job_id IS NULL).

Used for both the Phase 0 diagnosis of the 2026-04-09 spike and the
recurring Track B integrity report. Never writes to the database or QBO.

Usage:
    python scripts/audit_orphan_invoices.py
    python scripts/audit_orphan_invoices.py --since 2026-04-01 --until 2026-04-16
    python scripts/audit_orphan_invoices.py --since 2026-04-09 --until 2026-04-09 --verbose
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from database.schema import get_connection

logger = logging.getLogger("audit_orphan_invoices")


def _classify_orphan(row: dict) -> str:
    """Categorize an orphan invoice row.

    - qbo_mapped_no_job: we have a QBO mapping but no local job link
    - local_only: no QBO mapping and no job link (likely stale simulation data)
    """
    if row.get("quickbooks_invoice_id"):
        return "qbo_mapped_no_job"
    return "local_only"


def _fetch_orphans(db, since: str, until: str) -> list[dict]:
    rows = db.execute(
        """
        SELECT
            i.id,
            i.client_id,
            i.job_id,
            i.amount,
            i.status,
            i.issue_date,
            (
                SELECT m.tool_specific_id
                FROM cross_tool_mapping m
                WHERE m.canonical_id = i.id AND m.tool_name = 'quickbooks'
            ) AS quickbooks_invoice_id
        FROM invoices i
        WHERE i.job_id IS NULL
          AND i.issue_date BETWEEN %s AND %s
        ORDER BY i.issue_date, i.id
        """,
        (since, until),
    ).fetchall()
    return [dict(row) for row in rows]


def _group_by_day(rows: list[dict]) -> dict[str, dict]:
    by_day: dict[str, dict] = defaultdict(lambda: {"count": 0, "amount": 0.0, "by_class": defaultdict(int)})
    for row in rows:
        day = row["issue_date"]
        by_day[day]["count"] += 1
        by_day[day]["amount"] += float(row["amount"])
        classification = row.get("classification") or _classify_orphan(row)
        by_day[day]["by_class"][classification] += 1
    return dict(by_day)


def _fetch_clients_with_no_completed_job_on_issue_date(db, since: str, until: str) -> list[dict]:
    """Orphans whose issue_date has no completed job for that client — suspicious imports."""
    rows = db.execute(
        """
        SELECT i.id, i.client_id, i.issue_date, i.amount
        FROM invoices i
        WHERE i.job_id IS NULL
          AND i.issue_date BETWEEN %s AND %s
          AND NOT EXISTS (
              SELECT 1 FROM jobs j
              WHERE j.client_id = i.client_id
                AND j.completed_at::date = i.issue_date
                AND j.status = 'completed'
          )
        ORDER BY i.issue_date, i.id
        """,
        (since, until),
    ).fetchall()
    return [dict(row) for row in rows]


def audit(db, since: str, until: str, csv_out: Path | None = None) -> dict:
    orphans = _fetch_orphans(db, since, until)
    for row in orphans:
        row["classification"] = _classify_orphan(row)

    by_day = _group_by_day(orphans)
    no_job_on_date = _fetch_clients_with_no_completed_job_on_issue_date(db, since, until)

    summary = {
        "window": {"since": since, "until": until},
        "total_orphans": len(orphans),
        "total_orphan_amount": round(sum(float(r["amount"]) for r in orphans), 2),
        "by_day": {
            day: {
                "count": stats["count"],
                "amount": round(stats["amount"], 2),
                "by_class": dict(stats["by_class"]),
            }
            for day, stats in sorted(by_day.items())
        },
        "orphans_with_no_matching_completed_job": len(no_job_on_date),
    }

    if csv_out:
        with open(csv_out, "w", newline="") as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=["id", "client_id", "job_id", "issue_date", "amount", "status", "quickbooks_invoice_id", "classification"],
            )
            writer.writeheader()
            for row in orphans:
                writer.writerow({k: row.get(k) for k in writer.fieldnames})

    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since", default=str(date.today() - timedelta(days=30)),
                        help="Start of issue_date window (inclusive). Default: 30 days ago.")
    parser.add_argument("--until", default=str(date.today()),
                        help="End of issue_date window (inclusive). Default: today.")
    parser.add_argument("--csv", type=Path, default=None,
                        help="Optional path to dump the full orphan list as CSV.")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    )

    db = get_connection()
    try:
        summary = audit(db, args.since, args.until, args.csv)
    finally:
        db.close()

    logger.info("Orphan invoice audit summary:")
    logger.info("  window            : %s → %s", summary["window"]["since"], summary["window"]["until"])
    logger.info("  total orphans     : %d", summary["total_orphans"])
    logger.info("  total amount      : $%,.2f", summary["total_orphan_amount"])
    logger.info("  orphans with no matching completed job on issue_date: %d",
                summary["orphans_with_no_matching_completed_job"])
    logger.info("  by day:")
    for day, stats in summary["by_day"].items():
        by_class = ", ".join(f"{k}={v}" for k, v in sorted(stats["by_class"].items()))
        logger.info("    %s  count=%d amount=$%,.2f (%s)", day, stats["count"], stats["amount"], by_class)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run tests to confirm they pass**

Run: `python -m pytest tests/test_audit_orphan_invoices.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Smoke-test against the production DB**

Run: `DATABASE_URL=... python scripts/audit_orphan_invoices.py --since 2026-04-01 --until 2026-04-16`
Expected: Log output with a per-day breakdown. The 2026-04-09 line should show ~1,384 or fewer orphans (some may have been repaired by the earlier commit `c7943cf`). Capture the output for Task 2.

- [ ] **Step 6: Commit**

```bash
git add scripts/audit_orphan_invoices.py tests/test_audit_orphan_invoices.py
git commit -m "Add read-only orphan invoice audit script"
```

---

## Task 2: Diagnose the 2026-04-09 spike (Phase 0 blocker)

**Purpose:** Before running any remediation writes, understand what the 2026-04-09 import actually was. Per [docs/revenue-remediation-plan-2026-04.md:267](docs/revenue-remediation-plan-2026-04.md#L267), Track B repair actions are blocked until this is documented.

**Files:**
- Create: `docs/diagnosis-2026-04-09-invoice-spike.md`

- [ ] **Step 1: Dump the full 2026-04-09 orphan list**

Run: `python scripts/audit_orphan_invoices.py --since 2026-04-09 --until 2026-04-09 --csv /tmp/orphans-2026-04-09.csv`
Expected: A CSV with the complete orphan set for that day.

- [ ] **Step 2: Cross-reference against QBO sync logs**

Query the `automation_log` and `sync_runs` tables (if they exist) to find any sync or import activity on 2026-04-09:

```sql
SELECT action_name, trigger_source, status, created_at, action_target
FROM automation_log
WHERE created_at::date = '2026-04-09'
  AND (action_name ILIKE '%invoice%' OR action_name ILIKE '%sync%')
ORDER BY created_at;
```

If `sync_runs` exists, also:

```sql
SELECT tool_name, started_at, finished_at, records_created, records_updated, errors
FROM sync_runs
WHERE started_at::date = '2026-04-09'
ORDER BY started_at;
```

- [ ] **Step 3: Inspect a sample of the orphans in QBO directly**

Pick 5 orphan invoice IDs from the CSV. For each, look up the QBO invoice via the API:

```python
from auth import get_client
session = get_client("quickbooks")
# Use /v3/company/{COMPANY_ID}/invoice/{qbo_id} — see docs/skills/tool-api-patterns.md
```

Record: the QBO CreateTime, the PrivateNote contents (does it mention a job or SS-ID?), the CustomerRef, whether the invoice is a duplicate of a properly-linked one (check by (CustomerRef, TxnDate, Amount) uniqueness).

- [ ] **Step 4: Write the diagnosis document**

Create `docs/diagnosis-2026-04-09-invoice-spike.md` answering:
1. **What happened.** The timestamp of the import/sync run, the tool or script that ran, the input size.
2. **Are the QBO invoices real?** i.e. do they exist in QBO with that `CreateTime`, or were they invented locally?
3. **What was the trigger.** A backfill, a sync loop, a retroactive import? Quote the relevant log line or commit.
4. **Recommended disposition per class.** One of:
   - **Relink** — the invoices are legitimate and their matching jobs exist; proceed with Task 4.
   - **Quarantine** — legitimate but no matching local job; leave `job_id IS NULL` and rely on the analytics filter (Task 3) to keep them out of reports.
   - **Delete** — fabricated by a bug; remove locally AND from QBO (requires explicit finance sign-off before executing).
5. **Sign-off line.** A checkbox for the data-integrity owner to approve the recommended disposition before Task 4 runs in `--execute` mode.

- [ ] **Step 5: Commit the diagnosis doc**

```bash
git add docs/diagnosis-2026-04-09-invoice-spike.md
git commit -m "Document 2026-04-09 orphan invoice spike diagnosis"
```

**Gate:** Do not proceed to Task 4's `--execute` run until this doc is reviewed and the disposition is signed off. Tasks 3, 5, 6, 7 do not write to production and can proceed in parallel.

---

## Task 3: Harden booked-revenue exclusion of orphan invoices

**Purpose:** Track B action #1. The existing INNER JOIN already excludes orphans, but we add a defensive explicit `AND i.job_id IS NOT NULL` filter and a regression test so no future refactor accidentally reintroduces orphans into booked-revenue totals.

**Files:**
- Modify: `intelligence/metrics/revenue.py` (add defensive filter to `_sum_booked` and `_segment_booked`)
- Create: `tests/test_revenue_orphan_exclusion.py`

- [ ] **Step 1: Write the failing regression test**

Create `tests/test_revenue_orphan_exclusion.py`:

```python
"""Regression: orphan invoices (job_id IS NULL) must never count as booked revenue."""
import pytest

from intelligence.metrics.revenue import _sum_booked, _segment_booked


@pytest.fixture
def db_with_orphan_and_linked(pg_test_conn):
    """Seed: 1 completed job with a linked invoice ($150), 1 orphan invoice ($500) on the same day."""
    conn = pg_test_conn
    with conn:
        conn.execute(
            "INSERT INTO clients (id, first_name, last_name, email, client_type, status) "
            "VALUES ('SS-CLIENT-9001', 'Test', 'Client', 't@x.test', 'residential', 'active')"
        )
        conn.execute(
            "INSERT INTO jobs (id, client_id, service_type_id, scheduled_date, "
            "completed_at, status, amount) VALUES "
            "('SS-JOB-9001', 'SS-CLIENT-9001', 'std-residential', '2026-04-10', "
            "'2026-04-10 14:00', 'completed', 150.0)"
        )
        conn.execute(
            "INSERT INTO invoices (id, client_id, job_id, amount, status, issue_date) "
            "VALUES ('SS-INV-9001', 'SS-CLIENT-9001', 'SS-JOB-9001', 150.0, 'sent', '2026-04-10')"
        )
        conn.execute(
            "INSERT INTO invoices (id, client_id, job_id, amount, status, issue_date) "
            "VALUES ('SS-INV-9002', 'SS-CLIENT-9001', NULL, 500.0, 'sent', '2026-04-10')"
        )
    yield conn


def test_sum_booked_excludes_orphan_invoices(db_with_orphan_and_linked):
    total = _sum_booked(db_with_orphan_and_linked, "2026-04-10")
    assert total == 150.0, f"Expected only the job-linked $150, got ${total}"


def test_segment_booked_excludes_orphan_invoices(db_with_orphan_and_linked):
    segmented = _segment_booked(db_with_orphan_and_linked, "2026-04-10")
    assert segmented.get("residential", 0.0) == 150.0
    assert segmented.get("commercial", 0.0) == 0.0
```

Note: `pg_test_conn` should already exist in `tests/conftest.py`. If not, add one that provides a transactional Postgres connection and rolls back after each test. Check existing fixtures first — `tests/test_phase4.py` likely has a pattern to copy.

- [ ] **Step 2: Run the test to confirm it passes today (JOIN already excludes orphans)**

Run: `python -m pytest tests/test_revenue_orphan_exclusion.py -v`
Expected: PASS — because `_sum_booked` already has `JOIN jobs j ON j.id = i.job_id` which implicitly excludes orphans. The test is a regression guard.

If it FAILS, that means the JOIN is somehow not strict — investigate before proceeding.

- [ ] **Step 3: Add defensive explicit filter to revenue.py**

In [intelligence/metrics/revenue.py](intelligence/metrics/revenue.py), edit `_sum_booked` at lines 46–69. Change:

```python
def _sum_booked(db, start_date: str, end_date: Optional[str] = None) -> float:
    """Return booked revenue from job-linked invoices over a completion-date window."""
    if end_date is None:
        return db.execute(
            """
            SELECT COALESCE(SUM(i.amount), 0.0) AS total
            FROM invoices i
            JOIN jobs j ON j.id = i.job_id
            WHERE j.status = 'completed'
              AND j.completed_at::date = %s
            """,
            (start_date,),
        ).fetchone()["total"]

    return db.execute(
        """
        SELECT COALESCE(SUM(i.amount), 0.0) AS total
        FROM invoices i
        JOIN jobs j ON j.id = i.job_id
        WHERE j.status = 'completed'
          AND j.completed_at::date BETWEEN %s AND %s
        """,
        (start_date, end_date),
    ).fetchone()["total"]
```

To:

```python
def _sum_booked(db, start_date: str, end_date: Optional[str] = None) -> float:
    """Return booked revenue from job-linked invoices over a completion-date window.

    Defense in depth: the INNER JOIN already excludes orphan invoices
    (job_id IS NULL), but we add an explicit NOT NULL filter so a future
    refactor to LEFT JOIN cannot silently reintroduce them.
    """
    if end_date is None:
        return db.execute(
            """
            SELECT COALESCE(SUM(i.amount), 0.0) AS total
            FROM invoices i
            JOIN jobs j ON j.id = i.job_id
            WHERE i.job_id IS NOT NULL
              AND j.status = 'completed'
              AND j.completed_at::date = %s
            """,
            (start_date,),
        ).fetchone()["total"]

    return db.execute(
        """
        SELECT COALESCE(SUM(i.amount), 0.0) AS total
        FROM invoices i
        JOIN jobs j ON j.id = i.job_id
        WHERE i.job_id IS NOT NULL
          AND j.status = 'completed'
          AND j.completed_at::date BETWEEN %s AND %s
        """,
        (start_date, end_date),
    ).fetchone()["total"]
```

Apply the same `AND i.job_id IS NOT NULL` addition to `_segment_booked` at lines 72–85.

- [ ] **Step 4: Audit other invoice aggregations**

Run: `grep -rn "FROM invoices" intelligence/`

For each query found, verify it either:
- INNER JOINs to `jobs` on `job_id` (safe — orphans excluded)
- Is computing something where orphans belong (e.g., total-QBO-sent count)
- Should be updated to add `AND i.job_id IS NOT NULL`

Fix any queries in the "should be updated" bucket and add them to the modify list.

- [ ] **Step 5: Run the full intelligence test suite**

Run: `python tests/test_phase4.py -v -k "not live and not slack_channel"`
Expected: PASS. The defensive filter is functionally equivalent to the JOIN's implicit exclusion, so no behavior should change.

Run: `python -m pytest tests/test_revenue_orphan_exclusion.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add intelligence/metrics/revenue.py tests/test_revenue_orphan_exclusion.py
git commit -m "Harden booked-revenue queries against orphan invoices"
```

---

## Task 4: Extend remediation script with invoice-first orphan sweep

**Purpose:** Track B action #3. The current [scripts/remediate_reconciliation_invoices.py](scripts/remediate_reconciliation_invoices.py) is **job-first** — it iterates `_fetch_missing_jobs` (jobs without linked invoices) and for each one looks for an orphan candidate via `_unlinked_local_candidates` where `i.issue_date = job.issue_date` EXACTLY. This misses orphans when:
- The matching job's `completed_at::date` differs from the invoice's `issue_date` by even one day.
- The matching job is already linked to some other invoice (so `_fetch_missing_jobs` skips it).
- The orphan came from a QBO sync with no corresponding local job.

We add an **invoice-first** pass that iterates orphans and tries to match each to a job by (`client_id`, `amount`, date window), preserving the existing `_pick_unique_candidate` ambiguity guard.

**Files:**
- Modify: `scripts/remediate_reconciliation_invoices.py`
- Modify: `tests/test_invoice_remediation.py`

- [ ] **Step 1: Write the failing tests for the new helpers**

Append to `tests/test_invoice_remediation.py`:

```python
def test_match_orphan_to_job_returns_unique_candidate_by_amount_and_window():
    orphan = {
        "id": "SS-INV-9001",
        "client_id": "SS-CLIENT-0042",
        "amount": 275.0,
        "issue_date": "2026-04-10",
    }
    candidate_jobs = [
        {"id": "SS-JOB-9001", "client_id": "SS-CLIENT-0042", "amount": 275.0, "completed_at": "2026-04-10"},
        {"id": "SS-JOB-9002", "client_id": "SS-CLIENT-0042", "amount": 150.0, "completed_at": "2026-04-10"},
    ]
    match = remediation._match_orphan_to_job(orphan, candidate_jobs, window_days=1)
    assert match["id"] == "SS-JOB-9001"


def test_match_orphan_to_job_returns_none_when_ambiguous():
    orphan = {
        "id": "SS-INV-9001",
        "client_id": "SS-CLIENT-0042",
        "amount": 275.0,
        "issue_date": "2026-04-10",
    }
    candidate_jobs = [
        {"id": "SS-JOB-9001", "client_id": "SS-CLIENT-0042", "amount": 275.0, "completed_at": "2026-04-10"},
        {"id": "SS-JOB-9002", "client_id": "SS-CLIENT-0042", "amount": 275.0, "completed_at": "2026-04-10"},
    ]
    match = remediation._match_orphan_to_job(orphan, candidate_jobs, window_days=1)
    assert match is None


def test_match_orphan_to_job_respects_date_window():
    orphan = {
        "id": "SS-INV-9001",
        "client_id": "SS-CLIENT-0042",
        "amount": 275.0,
        "issue_date": "2026-04-10",
    }
    far_job = {"id": "SS-JOB-9003", "client_id": "SS-CLIENT-0042", "amount": 275.0, "completed_at": "2026-04-15"}
    match = remediation._match_orphan_to_job(orphan, [far_job], window_days=1)
    assert match is None
    match_wider = remediation._match_orphan_to_job(orphan, [far_job], window_days=10)
    assert match_wider["id"] == "SS-JOB-9003"


def test_match_orphan_to_job_rejects_cross_client_match():
    orphan = {
        "id": "SS-INV-9001",
        "client_id": "SS-CLIENT-0042",
        "amount": 275.0,
        "issue_date": "2026-04-10",
    }
    wrong_client_job = {"id": "SS-JOB-9004", "client_id": "SS-CLIENT-9999", "amount": 275.0, "completed_at": "2026-04-10"}
    assert remediation._match_orphan_to_job(orphan, [wrong_client_job], window_days=10) is None
```

- [ ] **Step 2: Run tests to confirm they fail**

Run: `python -m pytest tests/test_invoice_remediation.py -v`
Expected: FAIL with `AttributeError: module 'scripts.remediate_reconciliation_invoices' has no attribute '_match_orphan_to_job'`.

- [ ] **Step 3: Implement `_fetch_orphan_invoices` and `_match_orphan_to_job`**

In [scripts/remediate_reconciliation_invoices.py](scripts/remediate_reconciliation_invoices.py), after the existing `_unlinked_local_candidates` function (around line 348), add:

```python
def _fetch_orphan_invoices(db, since: str, until: str, limit: Optional[int]) -> list[dict]:
    """Return invoices with job_id IS NULL whose issue_date falls in the window."""
    query = """
        SELECT
            i.id,
            i.client_id,
            i.amount,
            i.issue_date,
            i.status,
            (
                SELECT m.tool_specific_id
                FROM cross_tool_mapping m
                WHERE m.canonical_id = i.id AND m.tool_name = 'quickbooks'
            ) AS quickbooks_invoice_id
        FROM invoices i
        WHERE i.job_id IS NULL
          AND i.issue_date BETWEEN %s AND %s
        ORDER BY i.issue_date, i.id
    """
    params: tuple = (since, until)
    if limit is not None:
        query += " LIMIT %s"
        params = (since, until, limit)
    rows = db.execute(query, params).fetchall()
    return [dict(row) for row in rows]


def _fetch_candidate_jobs_for_orphan(db, orphan: dict, window_days: int) -> list[dict]:
    """Return completed jobs for this client within the date window, with amount computed from service_type."""
    issue = date.fromisoformat(str(orphan["issue_date"])[:10])
    since = (issue - timedelta(days=window_days)).isoformat()
    until = (issue + timedelta(days=window_days)).isoformat()
    rows = db.execute(
        """
        SELECT j.id, j.client_id, j.service_type_id, j.amount,
               j.completed_at::date::text AS completed_at,
               j.scheduled_date::text AS scheduled_date,
               c.client_type
        FROM jobs j
        JOIN clients c ON c.id = j.client_id
        WHERE j.client_id = %s
          AND j.status = 'completed'
          AND j.completed_at::date BETWEEN %s AND %s
          AND NOT EXISTS (
              SELECT 1 FROM invoices i2 WHERE i2.job_id = j.id
          )
        ORDER BY j.completed_at
        """,
        (orphan["client_id"], since, until),
    ).fetchall()
    return [dict(row) for row in rows]


def _match_orphan_to_job(orphan: dict, candidate_jobs: list[dict], window_days: int) -> Optional[dict]:
    """Return the single job that matches this orphan, or None if zero / multiple.

    A candidate matches when:
      - client_id matches
      - amount matches within 1 cent
      - completed_at is within window_days of the orphan's issue_date
    """
    issue = date.fromisoformat(str(orphan["issue_date"])[:10])
    matches = []
    for job in candidate_jobs:
        if job["client_id"] != orphan["client_id"]:
            continue
        if abs(float(job["amount"]) - float(orphan["amount"])) >= 0.01:
            continue
        completed = date.fromisoformat(str(job["completed_at"])[:10])
        if abs((completed - issue).days) > window_days:
            continue
        matches.append(job)

    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        logger.warning(
            "Orphan %s: %d ambiguous job candidates, skipping",
            orphan["id"],
            len(matches),
        )
    return None
```

- [ ] **Step 4: Wire the new sweep into `remediate()` behind a mode flag**

Replace the `remediate()` signature and add a new branch. Near line 546, change:

```python
def remediate(db, dry_run: bool, limit: Optional[int]) -> dict[str, int]:
```

To:

```python
def remediate(
    db,
    dry_run: bool,
    limit: Optional[int],
    mode: str = "jobs",
    since: Optional[str] = None,
    until: Optional[str] = None,
    window_days: int = 3,
) -> dict[str, int]:
    """Remediate invoice data.

    mode='jobs' (default): existing job-first sweep — find jobs missing invoices,
        create or relink as needed.
    mode='orphans': new invoice-first sweep — find invoices with job_id IS NULL
        and relink each to its matching job by (client_id, amount, date window).
        Only performs DB-local backlinks; never creates QBO invoices.
    """
```

Inside `remediate()`, before the existing job loop, add:

```python
    if mode == "orphans":
        if since is None or until is None:
            raise ValueError("--since and --until are required in orphans mode")
        stats = {
            "orphans_scanned": 0,
            "orphans_linked": 0,
            "orphans_ambiguous": 0,
            "orphans_no_match": 0,
            "orphans_failed": 0,
        }
        orphans = _fetch_orphan_invoices(db, since, until, limit)
        stats["orphans_scanned"] = len(orphans)
        logger.info("Scanning %d orphan invoices in window %s → %s", len(orphans), since, until)

        for index, orphan in enumerate(orphans, start=1):
            try:
                candidates = _fetch_candidate_jobs_for_orphan(db, orphan, window_days)
                match = _match_orphan_to_job(orphan, candidates, window_days)
                if match is None:
                    if len(candidates) > 1 and any(
                        abs(float(j["amount"]) - float(orphan["amount"])) < 0.01 for j in candidates
                    ):
                        stats["orphans_ambiguous"] += 1
                    else:
                        stats["orphans_no_match"] += 1
                    continue

                if dry_run:
                    logger.info(
                        "[DRY RUN] Would link orphan %s (client=%s, $%.2f, %s) → job %s",
                        orphan["id"], orphan["client_id"], float(orphan["amount"]),
                        orphan["issue_date"], match["id"],
                    )
                    stats["orphans_linked"] += 1
                else:
                    result = _link_invoice_to_job(db, orphan["id"], match["id"])
                    if result in ("linked", "already_linked"):
                        stats["orphans_linked"] += 1

                if index % 50 == 0 or index == len(orphans):
                    logger.info(
                        "Processed %d/%d orphans (linked=%d, ambiguous=%d, no_match=%d)",
                        index, len(orphans),
                        stats["orphans_linked"], stats["orphans_ambiguous"], stats["orphans_no_match"],
                    )
            except Exception as exc:
                stats["orphans_failed"] += 1
                logger.error("Orphan %s: relink failed: %s", orphan["id"], exc)

        return stats
```

Leave the existing job-first body below unchanged; it runs when `mode == "jobs"`.

- [ ] **Step 5: Update `main()` to expose the new flags**

Replace the `main()` argparse block near line 659 with:

```python
def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Apply writes locally and in QuickBooks. Omit for dry-run mode.",
    )
    parser.add_argument(
        "--mode",
        choices=("jobs", "orphans"),
        default="jobs",
        help="jobs = job-first sweep (default). orphans = invoice-first sweep "
             "that relinks orphan invoices to completed jobs.",
    )
    parser.add_argument("--since", default=None,
                        help="Start of issue_date window for --mode orphans.")
    parser.add_argument("--until", default=None,
                        help="End of issue_date window for --mode orphans.")
    parser.add_argument("--window-days", type=int, default=3,
                        help="Allowed gap between orphan issue_date and job completed_at. Default: 3.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Optional max number of records to process.")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    )

    dry_run = not args.execute
    logger.info(
        "Starting reconciliation invoice remediation (mode=%s, %s)",
        args.mode,
        "dry-run" if dry_run else "execute",
    )

    db = get_connection()
    try:
        stats = remediate(
            db,
            dry_run=dry_run,
            limit=args.limit,
            mode=args.mode,
            since=args.since,
            until=args.until,
            window_days=args.window_days,
        )
    finally:
        db.close()

    logger.info("Remediation summary:")
    for key, value in stats.items():
        logger.info("  %s=%s", key, value)

    return 0
```

- [ ] **Step 6: Run tests to confirm they pass**

Run: `python -m pytest tests/test_invoice_remediation.py -v`
Expected: PASS (7 tests total — 3 original + 4 new).

- [ ] **Step 7: Dry-run against production for the 2026-04-09 window**

Run: `python scripts/remediate_reconciliation_invoices.py --mode orphans --since 2026-04-09 --until 2026-04-09`
Expected: Log output showing `orphans_scanned=N`, `orphans_linked=M`, `orphans_ambiguous=X`, `orphans_no_match=Y`. Capture this output and include it as an addendum in `docs/diagnosis-2026-04-09-invoice-spike.md`.

- [ ] **Step 8: Execute only after Task 2 sign-off**

**Gate:** Do not run with `--execute` until `docs/diagnosis-2026-04-09-invoice-spike.md` has the sign-off line checked.

Once signed off:

```bash
python scripts/remediate_reconciliation_invoices.py --mode orphans --since 2026-04-01 --until 2026-04-16 --execute
```

Capture the stats. Expected: `orphans_linked` should approach the number reported by `audit_orphan_invoices.py`. Remaining `orphans_no_match` rows are Task 2's "quarantine" disposition — they stay orphaned and remain excluded by Task 3's defensive filter.

- [ ] **Step 9: Commit**

```bash
git add scripts/remediate_reconciliation_invoices.py tests/test_invoice_remediation.py
git commit -m "Add invoice-first orphan sweep to reconciliation remediation"
```

---

## Task 5: Intelligence integrity metrics module

**Purpose:** Track B action #5. Add a recurring integrity check that runs every morning and alerts if:
- `orphan_invoices_count` > 0 (should be zero after Task 4)
- `payments_without_invoice_link_count` > 0
- `jobs_completed_without_invoice_after_24h_count` > 0

**Files:**
- Create: `intelligence/metrics/integrity.py`
- Create: `tests/test_metrics_integrity.py`
- Modify: `intelligence/metrics/__init__.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_metrics_integrity.py`:

```python
import pytest
from datetime import date, timedelta

from intelligence.metrics.integrity import compute


@pytest.fixture
def seeded_db(pg_test_conn):
    conn = pg_test_conn
    today = date.fromisoformat("2026-04-16")
    yesterday = today - timedelta(days=1)
    two_days_ago = today - timedelta(days=2)

    with conn:
        conn.execute(
            "INSERT INTO clients (id, first_name, last_name, email, client_type, status) "
            "VALUES ('SS-CLIENT-9001', 'Test', 'One', 't@x.test', 'residential', 'active')"
        )
        # Orphan invoice on yesterday
        conn.execute(
            "INSERT INTO invoices (id, client_id, job_id, amount, status, issue_date) "
            "VALUES ('SS-INV-9001', 'SS-CLIENT-9001', NULL, 150.0, 'sent', %s)",
            (str(yesterday),),
        )
        # A job completed 2 days ago with no invoice
        conn.execute(
            "INSERT INTO jobs (id, client_id, service_type_id, scheduled_date, "
            "completed_at, status, amount) VALUES "
            "('SS-JOB-9001', 'SS-CLIENT-9001', 'std-residential', %s, %s, 'completed', 150.0)",
            (str(two_days_ago), f"{two_days_ago} 14:00"),
        )
        # Orphan payment (no invoice_id)
        conn.execute(
            "INSERT INTO payments (id, client_id, invoice_id, amount, payment_date) "
            "VALUES ('SS-PAY-9001', 'SS-CLIENT-9001', NULL, 150.0, %s)",
            (str(yesterday),),
        )
    yield conn


def test_compute_counts_orphan_invoices(seeded_db):
    result = compute(seeded_db, "2026-04-16")
    assert result["orphan_invoices"]["count"] >= 1
    assert result["orphan_invoices"]["amount"] >= 150.0


def test_compute_counts_jobs_without_invoice_after_24h(seeded_db):
    result = compute(seeded_db, "2026-04-16")
    assert result["jobs_missing_invoice_24h"]["count"] >= 1


def test_compute_counts_payments_without_invoice(seeded_db):
    result = compute(seeded_db, "2026-04-16")
    assert result["unlinked_payments"]["count"] >= 1


def test_compute_emits_alerts_for_nonzero_counts(seeded_db):
    result = compute(seeded_db, "2026-04-16")
    alerts = result["alerts"]
    categories = {a["category"] for a in alerts}
    assert "orphan_invoices" in categories
    assert "jobs_missing_invoice_24h" in categories
    assert "unlinked_payments" in categories


def test_compute_emits_no_alerts_when_counts_zero(pg_test_conn):
    result = compute(pg_test_conn, "2026-04-16")
    assert result["orphan_invoices"]["count"] == 0
    assert result["alerts"] == []
```

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `python -m pytest tests/test_metrics_integrity.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'intelligence.metrics.integrity'`.

- [ ] **Step 3: Implement the integrity module**

Create `intelligence/metrics/integrity.py`:

```python
"""
intelligence/metrics/integrity.py

Data integrity checks for the invoice / payment / job pipeline.

Surfaces three counts used by Track B's recurring integrity monitor:

1. orphan_invoices           -- invoices.job_id IS NULL
2. unlinked_payments         -- payments.invoice_id IS NULL
3. jobs_missing_invoice_24h  -- jobs completed >24h ago with no linked invoice

Each non-zero count produces an alert routed to #operations.
"""

from datetime import date, timedelta


def _count_orphan_invoices(db, as_of: str) -> dict:
    row = db.execute(
        """
        SELECT COUNT(*) AS cnt, COALESCE(SUM(amount), 0.0) AS amount
        FROM invoices
        WHERE job_id IS NULL
          AND issue_date <= %s
        """,
        (as_of,),
    ).fetchone()
    return {"count": int(row["cnt"]), "amount": float(row["amount"])}


def _count_unlinked_payments(db, as_of: str) -> dict:
    row = db.execute(
        """
        SELECT COUNT(*) AS cnt, COALESCE(SUM(amount), 0.0) AS amount
        FROM payments
        WHERE invoice_id IS NULL
          AND payment_date <= %s
        """,
        (as_of,),
    ).fetchone()
    return {"count": int(row["cnt"]), "amount": float(row["amount"])}


def _count_jobs_missing_invoice_24h(db, as_of: str) -> dict:
    cutoff = (date.fromisoformat(as_of) - timedelta(days=1)).isoformat()
    row = db.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM jobs j
        WHERE j.status = 'completed'
          AND j.completed_at::date <= %s
          AND NOT EXISTS (
              SELECT 1 FROM invoices i WHERE i.job_id = j.id
          )
        """,
        (cutoff,),
    ).fetchone()
    return {"count": int(row["cnt"])}


def compute(db, briefing_date: str) -> dict:
    """Return the integrity metric bundle for the given briefing date."""
    orphan_invoices = _count_orphan_invoices(db, briefing_date)
    unlinked_payments = _count_unlinked_payments(db, briefing_date)
    jobs_missing = _count_jobs_missing_invoice_24h(db, briefing_date)

    alerts = []
    if orphan_invoices["count"] > 0:
        alerts.append({
            "category": "orphan_invoices",
            "severity": "warning",
            "message": (
                f"{orphan_invoices['count']} orphan invoices "
                f"(${orphan_invoices['amount']:,.2f}) — run "
                f"`scripts/remediate_reconciliation_invoices.py --mode orphans`"
            ),
        })
    if unlinked_payments["count"] > 0:
        alerts.append({
            "category": "unlinked_payments",
            "severity": "warning",
            "message": (
                f"{unlinked_payments['count']} payments with no invoice link "
                f"(${unlinked_payments['amount']:,.2f})"
            ),
        })
    if jobs_missing["count"] > 0:
        alerts.append({
            "category": "jobs_missing_invoice_24h",
            "severity": "warning",
            "message": (
                f"{jobs_missing['count']} jobs completed >24h ago with no invoice — "
                f"automation may be lagging"
            ),
        })

    return {
        "orphan_invoices": orphan_invoices,
        "unlinked_payments": unlinked_payments,
        "jobs_missing_invoice_24h": jobs_missing,
        "alerts": alerts,
    }
```

- [ ] **Step 4: Export integrity from the metrics package**

In [intelligence/metrics/__init__.py](intelligence/metrics/__init__.py), add the integrity import and include it in the bundle. Locate the existing `compute_all(db, date_str)` (or equivalent aggregator); add:

```python
from intelligence.metrics import integrity as integrity_module

# Inside compute_all(...), after the existing metric calls:
result["integrity"] = integrity_module.compute(db, date_str)
```

Do NOT add `"integrity"` to `LEGACY_REVENUE_SHIM_KEYS`. It's a new key, not a renamed one.

- [ ] **Step 5: Run tests to confirm they pass**

Run: `python -m pytest tests/test_metrics_integrity.py -v`
Expected: PASS (5 tests).

Run: `python tests/test_phase4.py -v -k "not live and not slack_channel"`
Expected: PASS — the new `integrity` key in the metrics bundle should not break any consumers.

- [ ] **Step 6: Commit**

```bash
git add intelligence/metrics/integrity.py intelligence/metrics/__init__.py tests/test_metrics_integrity.py
git commit -m "Add intelligence data-integrity metric for orphan invoices"
```

---

## Task 6: Route integrity alerts through the daily runner

**Purpose:** Make the new integrity alerts visible in the daily briefing and in Slack routing, so orphan-invoice regressions surface the same morning they happen.

**Files:**
- Modify: `intelligence/runner.py`

- [ ] **Step 1: Add `integrity` to `_ALERT_CHANNEL_MAP`**

In [intelligence/runner.py](intelligence/runner.py), locate `_ALERT_CHANNEL_MAP` (around lines 83–91). Add:

```python
_ALERT_CHANNEL_MAP: dict[str, str] = {
    "booked_revenue":   "#operations",
    "revenue":          "#operations",
    "financial_health": "#operations",
    "operations":       "#operations",
    "tasks":            "#operations",
    "sales":            "#sales",
    "marketing":        "#sales",
    "integrity":        "#operations",
}
```

- [ ] **Step 2: Ensure `_collect_alerts` picks up integrity alerts**

Search for `_collect_alerts` in `intelligence/runner.py`. It should iterate over metric modules and gather each module's `alerts` key. If it uses a hard-coded list of modules, add `"integrity"` to that list. If it dynamically iterates `metrics.items()`, no change needed.

Expected shape after the change: when `metrics["integrity"]["alerts"]` is non-empty, each alert surfaces to `#operations` with its category prefix.

- [ ] **Step 3: Smoke-test the runner in dry-run mode**

Run: `python -m intelligence.runner --skip-sync --date 2026-04-16 --dry-run`
Expected: The runner completes. If there are orphan invoices in the DB, the output log lists an `integrity` alert. No Slack post in dry-run.

- [ ] **Step 4: Verify briefing archive includes integrity counts**

Check the latest file in `briefings/context_daily_report_*.md`. It should include the integrity metrics block.

- [ ] **Step 5: Commit**

```bash
git add intelligence/runner.py
git commit -m "Route integrity alerts to #operations channel"
```

---

## Task 7: Acceptance verification

**Purpose:** Confirm the Track B acceptance criteria from [docs/revenue-remediation-plan-2026-04.md:134-138](docs/revenue-remediation-plan-2026-04.md) are met.

**Files:** None (verification only).

- [ ] **Step 1: Confirm orphan count is near zero for recent dates**

Run: `python scripts/audit_orphan_invoices.py --since 2026-04-01 --until 2026-04-16`
Expected: `total_orphans` is a small residual (those flagged as "quarantine" per Task 2's disposition). The 2026-04-09 spike number should be dramatically lower than the original 1,384.

- [ ] **Step 2: Confirm booked-revenue dashboards exclude orphans**

Run: `python -m pytest tests/test_revenue_orphan_exclusion.py -v`
Expected: PASS.

Run the daily report for a date where orphans exist:
```bash
python -m intelligence.runner --skip-sync --date 2026-04-16 --dry-run
```
Inspect the context document archived in `briefings/`. Confirm the booked-revenue figures do not include the orphan amounts (compare against the audit script's totals).

- [ ] **Step 3: Confirm the 2026-04-09 anomaly is documented**

Read `docs/diagnosis-2026-04-09-invoice-spike.md`. Confirm it has:
- What happened
- Whether invoices are real
- Trigger identified
- Disposition decision + sign-off

- [ ] **Step 4: Confirm integrity check is live in daily briefings**

Inspect the latest `briefings/context_daily_report_*.md` (after running Task 6 step 3). Confirm integrity counts appear in the output.

- [ ] **Step 5: Run the full test suite once more**

Run:
```bash
python -m pytest tests/test_audit_orphan_invoices.py tests/test_invoice_remediation.py tests/test_metrics_integrity.py tests/test_revenue_orphan_exclusion.py -v
python tests/test_phase4.py -v -k "not live and not slack_channel"
```
Expected: All PASS.

- [ ] **Step 6: Final commit and branch push**

```bash
git log --oneline main..HEAD
```
Expected: 6 commits covering Tasks 1, 3, 4, 5, 6, and the diagnosis doc.

Push the branch and open a PR referencing [docs/revenue-remediation-plan-2026-04.md](docs/revenue-remediation-plan-2026-04.md) Track B.

---

## Risks & Rollback

Per [docs/revenue-remediation-plan-2026-04.md:307](docs/revenue-remediation-plan-2026-04.md):

- **Task 3 (metric filter):** Additive guard. Revert the commit to rollback; behavior returns to the JOIN-implicit exclusion.
- **Task 4 (orphan sweep):** Additive — only UPDATEs `invoices.job_id` when `job_id IS NULL`. Never overwrites existing links (the `_link_invoice_to_job` helper enforces this). To rollback a run: restore from the pre-run `invoices` snapshot. The sweep does NOT write to QBO.
- **Task 5 & 6 (integrity):** New reporting surface only. Revert the commits to remove.
- **Worst-case wrong-relink:** If the sweep links an orphan to a job that wasn't actually its match (e.g., two jobs at the same price on the same day — ambiguity guard should prevent this, but bugs happen), the invoice appears in booked revenue for the wrong job. Detection: the daily integrity check's `jobs_missing_invoice_24h` count would *decrease* for the wrong reason. Mitigation: the ambiguity guard returns `None` for any multi-candidate match; the `--window-days 3` default is narrow.

## Out of Scope (explicit non-goals)

- Stopping the QBO syncer from creating orphans in the first place. Tracked for a separate sync-hardening plan; [intelligence/syncers/sync_quickbooks.py](intelligence/syncers/sync_quickbooks.py) lines 136–162 is the source.
- Repricing mispriced invoices (Track C).
- Repairing commercial recurring agreements (Track E).
- The revenue vs cash-collected rename (Track A — already partially landed).
