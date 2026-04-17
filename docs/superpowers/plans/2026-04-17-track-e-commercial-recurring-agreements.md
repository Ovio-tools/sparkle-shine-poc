# Track E: Commercial Recurring Agreement Remediation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the production state where 40 active commercial clients have 0 active commercial recurring agreements by backfilling canonical `recurring_agreements` rows and moving commercial scheduling onto the same model used for residential recurring work, behind a feature flag, with the notes-based fallback preserved during rollout.

**Architecture:** Three concentric layers.
1. **Representation:** Store each commercial client's schedule as one (or two, for split nightly+Saturday) `recurring_agreements` row with `frequency='weekly'` and `day_of_week` as a comma-separated list of weekday names (e.g. `"monday,wednesday,friday"` for 3x_weekly). This fits the existing CHECK constraint without DDL changes and only requires a small expansion of `_is_due_today`.
2. **Backfill + audit:** Read-only `scripts/audit_commercial_scheduling.py` surfaces the gap. Write `scripts/backfill_commercial_agreements.py` creates agreements idempotently via `(client_id, service_type_id, status='active')` uniqueness check. A reconciliation script covers the three drift cases (active-unscheduled, notes-only, agreement-stale).
3. **Rollout gate:** New flag `TRACK_E_COMMERCIAL_AGREEMENT_SCHEDULING_ENABLED` in `intelligence/config.py`. When False (default), `operations.py` Pass 1 skips commercial agreements and Pass 1b runs as today. When True, Pass 1 processes commercial agreements and Pass 1b skips any commercial client that already has an active agreement. Pass 1b's existing idempotency guard (`SELECT id FROM jobs WHERE client_id = %s AND scheduled_date = %s`) provides a second belt-and-suspenders defense against duplicate jobs.

**Tech Stack:** Python 3.11, PostgreSQL via psycopg2 (`%s` placeholders, `RealDictRow`), existing `database.mappings.generate_id` and `register_mapping`, `pytest` with in-memory fixtures.

**Scope boundaries:**
- No schema DDL changes (no ALTER of the `frequency` CHECK constraint). We use `frequency='weekly'` with expanded `day_of_week` semantics.
- No changes to `automations/job_completion_flow.py` — pricing already goes through `get_commercial_per_visit_rate()` and works whether the job was created via Pass 1 or Pass 1b.
- No changes to Jobber/QBO schedule objects. Jobber jobs are still created per-visit by `JobSchedulingGenerator`.
- The `_ensure_schema` ALTER adding `client_type` to `recurring_agreements` is already in `operations.py` — we reuse it.

---

## File Structure

**Create:**
- `scripts/audit_commercial_scheduling.py` — read-only diagnostic. Prints gap summary, per-client schedule-from-notes, mapping status, agreement status.
- `scripts/backfill_commercial_agreements.py` — idempotent creation of `recurring_agreements` rows for active commercial clients. Supports `--dry-run`. Uniqueness key: `(client_id, service_type_id, status='active')`.
- `scripts/reconcile_commercial_agreements.py` — three-way reconciliation: active-unscheduled, notes-only-no-agreement, agreement-without-recent-jobs.
- `tests/test_track_e_commercial_agreements.py` — unit tests for schedule-to-agreement translation, `_is_due_today` multi-day behavior, and the regression test that active commercial clients never drop to zero active agreements post-backfill.

**Modify:**
- `simulation/generators/operations.py`:
  - Extend `_is_due_today` (around line 249) to accept comma-separated `day_of_week` and match when today's weekday is in the set.
  - Gate Pass 1 (line 812-ish) so commercial agreements are processed only when the flag is True.
  - Gate Pass 1b (line 935-ish) so a client with an active agreement is skipped when the flag is True.
- `intelligence/config.py` — add `TRACK_E_COMMERCIAL_AGREEMENT_SCHEDULING_ENABLED: bool = False` with comment mirroring the Track D flag's comment style.
- `intelligence/metrics/operations.py` — the `commercial_gap` block already exists (lines 216-265); no changes needed but verify it still reads correctly after backfill.

**Do not touch:** `database/schema.py`, `automations/job_completion_flow.py`, `seeding/generators/gen_clients.py`.

---

## Schedule Translation Table

The backfill script uses this fixed mapping from `clients.notes` schedule hints to agreement rows:

| Notes hint | Inferred via `_commercial_scope` | Agreements created | Days of week |
|------------|-----------------------------------|--------------------|--------------| 
| `nightly` | `nightly` | 1 row, `commercial-nightly` | `monday,tuesday,wednesday,thursday,friday` |
| `nightly_plus_saturday` | falls through to `3x_weekly` in scope, but the **seed schedule** is known | 2 rows: `commercial-nightly` Mon-Fri + `deep-clean` Saturday | |
| `nightly_weekdays` | `nightly` | 1 row, `commercial-nightly` | `monday,tuesday,wednesday,thursday,friday` |
| `5x weekly` | `5x weekly` | 1 row, `commercial-nightly` | `monday,tuesday,wednesday,thursday,friday` |
| `daily` | `daily` | 1 row, `commercial-nightly` | `monday,tuesday,wednesday,thursday,friday,saturday` |
| `3x weekly` / `3x_weekly` | `3x weekly` | 1 row, `commercial-nightly` | `monday,wednesday,friday` |
| `2x weekly` / `2x_weekly` | `2x weekly` | 1 row, `commercial-nightly` | `tuesday,thursday` |
| blank / unknown | `_commercial_scope` default | 1 row, `commercial-nightly` | `monday,wednesday,friday` (3x weekly default) |

The backfill script prefers `gen_clients._COMMERCIAL_CLIENTS` schedule data (authoritative) over notes parsing when the client's company_name matches. Falls back to notes parsing for clients not in the seed list.

The `price_per_visit` comes from `get_commercial_per_visit_rate(client_id)` so agreements stay consistent with invoicing.

---

## Task 1: Extend `_is_due_today` to support multi-day agreements

**Files:**
- Modify: `simulation/generators/operations.py` (lines 249-281)
- Test: `tests/test_track_e_commercial_agreements.py` (new)

- [ ] **Step 1: Write the failing test**

```python
# tests/test_track_e_commercial_agreements.py
from datetime import date
from simulation.generators.operations import _is_due_today


def test_is_due_today_comma_separated_matches_any():
    agreement = {
        "start_date": "2026-01-01",
        "frequency": "weekly",
        "day_of_week": "monday,wednesday,friday",
    }
    assert _is_due_today(agreement, date(2026, 4, 13))  # Monday
    assert _is_due_today(agreement, date(2026, 4, 15))  # Wednesday
    assert _is_due_today(agreement, date(2026, 4, 17))  # Friday
    assert not _is_due_today(agreement, date(2026, 4, 14))  # Tuesday
    assert not _is_due_today(agreement, date(2026, 4, 18))  # Saturday


def test_is_due_today_single_day_still_works():
    agreement = {
        "start_date": "2026-01-01",
        "frequency": "weekly",
        "day_of_week": "monday",
    }
    assert _is_due_today(agreement, date(2026, 4, 13))
    assert not _is_due_today(agreement, date(2026, 4, 14))
```

- [ ] **Step 2: Run test — expect FAIL**

```bash
python -m pytest tests/test_track_e_commercial_agreements.py::test_is_due_today_comma_separated_matches_any -v
```

- [ ] **Step 3: Implement the change**

Replace the weekly branch in `_is_due_today`:

```python
def _is_due_today(agreement: dict, today: date) -> bool:
    start = date.fromisoformat(agreement["start_date"])
    freq = agreement["frequency"]

    _DOW_MAP = {
        "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
        "friday": 4, "saturday": 5, "sunday": 6,
    }

    raw_dow = (agreement.get("day_of_week") or "").lower()
    # Support comma-separated day lists for multi-day-per-week agreements
    # (used by commercial recurring agreements). Single-day agreements
    # still work — the comma split yields a one-element list.
    if raw_dow:
        day_set = {
            _DOW_MAP[d.strip()] for d in raw_dow.split(",")
            if d.strip() in _DOW_MAP
        }
        if not day_set:
            day_set = {start.weekday()}
    else:
        day_set = {start.weekday()}

    if freq == "weekly":
        return today.weekday() in day_set

    elif freq == "biweekly":
        return (
            today.weekday() in day_set
            and (today - start).days % 14 < 7
        )

    elif freq == "monthly":
        import calendar
        days_in_month = calendar.monthrange(today.year, today.month)[1]
        due_day = min(start.day, days_in_month)
        return today.day == due_day

    return False
```

- [ ] **Step 4: Run test — expect PASS**

```bash
python -m pytest tests/test_track_e_commercial_agreements.py -v
```

- [ ] **Step 5: Run full operations test suite to confirm no regression**

```bash
python -m pytest tests/test_phase5_operations.py -v
```

Expected: all existing tests still pass.

- [ ] **Step 6: Commit**

```bash
git add simulation/generators/operations.py tests/test_track_e_commercial_agreements.py
git commit -m "Extend _is_due_today to accept comma-separated day_of_week (Track E)"
```

---

## Task 2: Add TRACK_E feature flag

**Files:**
- Modify: `intelligence/config.py`

- [ ] **Step 1: Add flag below TRACK_D flag**

Append to `intelligence/config.py` immediately after `TRACK_D_PAYMENT_TIMING_ENABLED`:

```python
# Track E: commercial recurring agreement scheduling.
#
# When True, the simulation's JobSchedulingGenerator reads commercial
# recurring work from the canonical `recurring_agreements` table
# (Pass 1) and the legacy notes-based commercial scheduling (Pass 1b)
# skips any client that already has an active agreement. When False,
# commercial agreements are skipped by Pass 1 and Pass 1b runs in
# legacy mode — behavior identical to pre-Track-E production.
#
# Merge this flag False. Flip True only after
# scripts/backfill_commercial_agreements.py has been run in production
# and the audit script confirms every active commercial client has a
# matching active agreement. The flag is read at call time via getattr,
# so a config change takes effect on the next generator tick without
# a worker restart.
TRACK_E_COMMERCIAL_AGREEMENT_SCHEDULING_ENABLED: bool = False
```

- [ ] **Step 2: Commit**

```bash
git add intelligence/config.py
git commit -m "Add TRACK_E_COMMERCIAL_AGREEMENT_SCHEDULING_ENABLED flag (Track E)"
```

---

## Task 3: Create the audit script

**Files:**
- Create: `scripts/audit_commercial_scheduling.py`

- [ ] **Step 1: Implement the script**

```python
"""Read-only audit of commercial client scheduling state.

Reports:
  - active commercial clients
  - active commercial recurring agreements
  - clients missing an agreement
  - latest scheduled / completed job date per client
  - notes-derived schedule scope
  - Jobber / QBO mapping presence

Usage:
    python -m scripts.audit_commercial_scheduling
    python -m scripts.audit_commercial_scheduling --verbose
"""
import argparse
import json
from datetime import date

from database.connection import get_connection
from database.mappings import get_tool_id
from simulation.generators.operations import _commercial_scope


def _fetch_commercial_state(conn) -> list[dict]:
    rows = conn.execute(
        """
        SELECT
            c.id AS client_id,
            c.company_name,
            c.status AS client_status,
            c.notes,
            (
                SELECT COUNT(*) FROM recurring_agreements ra
                WHERE ra.client_id = c.id AND ra.status = 'active'
            ) AS active_agreements,
            (
                SELECT MAX(scheduled_date) FROM jobs j
                WHERE j.client_id = c.id
            ) AS last_job_date,
            (
                SELECT MAX(completed_at) FROM jobs j
                WHERE j.client_id = c.id AND j.status = 'completed'
            ) AS last_completed_at
        FROM clients c
        WHERE c.client_type = 'commercial'
        ORDER BY c.status DESC, c.company_name
        """
    ).fetchall()
    return [dict(r) for r in rows]


def _enrich(row: dict) -> dict:
    row["notes_schedule"] = _commercial_scope(row.get("notes"))
    row["jobber_client_id"] = get_tool_id(row["client_id"], "jobber")
    row["qbo_customer_id"] = get_tool_id(row["client_id"], "quickbooks")
    return row


def audit() -> dict:
    conn = get_connection()
    try:
        rows = [_enrich(r) for r in _fetch_commercial_state(conn)]
    finally:
        conn.close()

    active = [r for r in rows if r["client_status"] == "active"]
    missing = [r for r in active if r["active_agreements"] == 0]
    with_agreement = [r for r in active if r["active_agreements"] > 0]

    return {
        "audit_date": date.today().isoformat(),
        "active_commercial_clients": len(active),
        "active_commercial_with_agreement": len(with_agreement),
        "active_commercial_missing_agreement": len(missing),
        "coverage_pct": round(100 * len(with_agreement) / max(1, len(active)), 1),
        "per_client": rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verbose", action="store_true",
                        help="print per-client rows in addition to summary")
    args = parser.parse_args()

    report = audit()
    summary = {k: v for k, v in report.items() if k != "per_client"}
    print(json.dumps(summary, indent=2))
    if args.verbose:
        print("\nPer-client detail:")
        for row in report["per_client"]:
            print(json.dumps(row, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Smoke-test the script**

```bash
python -m scripts.audit_commercial_scheduling
```

Expected: JSON summary showing `active_commercial_missing_agreement` equal to the size of the gap (40 per the remediation plan).

- [ ] **Step 3: Commit**

```bash
git add scripts/audit_commercial_scheduling.py
git commit -m "Add commercial scheduling audit script (Track E)"
```

---

## Task 4: Create the backfill script

**Files:**
- Create: `scripts/backfill_commercial_agreements.py`

- [ ] **Step 1: Implement the script**

```python
"""Backfill recurring_agreements rows for active commercial clients.

Reads:
  - clients (client_type='commercial', status='active')
  - notes for schedule inference
  - seeding.generators.gen_clients._COMMERCIAL_CLIENTS for authoritative schedule
  - get_commercial_per_visit_rate for price_per_visit

Writes:
  - recurring_agreements rows, one per (client, service_type_id)
  - cross_tool_mapping for canonical RECUR ids (no tool-side creation —
    recurring agreements are internal records only)

Idempotency: uniqueness enforced by (client_id, service_type_id, status='active').
Re-running is safe: clients with an existing active agreement are skipped.

Usage:
    python -m scripts.backfill_commercial_agreements --dry-run
    python -m scripts.backfill_commercial_agreements --execute
"""
import argparse
import logging
from datetime import date

from database.connection import get_connection, get_column_names
from database.mappings import generate_id

logger = logging.getLogger(__name__)

# Map notes-hint / seed-schedule string → (service_type_id, day_of_week csv)
# Multi-row schedules return a list of these tuples.
_SCHEDULE_TO_AGREEMENTS: dict[str, list[tuple[str, str]]] = {
    "nightly":               [("commercial-nightly", "monday,tuesday,wednesday,thursday,friday")],
    "nightly_weekdays":      [("commercial-nightly", "monday,tuesday,wednesday,thursday,friday")],
    "5x weekly":             [("commercial-nightly", "monday,tuesday,wednesday,thursday,friday")],
    "5x_weekly":             [("commercial-nightly", "monday,tuesday,wednesday,thursday,friday")],
    "daily":                 [("commercial-nightly", "monday,tuesday,wednesday,thursday,friday,saturday")],
    "3x weekly":             [("commercial-nightly", "monday,wednesday,friday")],
    "3x_weekly":             [("commercial-nightly", "monday,wednesday,friday")],
    "2x weekly":             [("commercial-nightly", "tuesday,thursday")],
    "2x_weekly":             [("commercial-nightly", "tuesday,thursday")],
    "nightly_plus_saturday": [
        ("commercial-nightly", "monday,tuesday,wednesday,thursday,friday"),
        ("deep-clean",         "saturday"),
    ],
}

_DEFAULT_AGREEMENTS = [("commercial-nightly", "monday,wednesday,friday")]


def _seed_schedule_by_company() -> dict[str, str]:
    """Return a {company_name: schedule} lookup from gen_clients seed data."""
    try:
        from seeding.generators.gen_clients import _COMMERCIAL_CLIENTS
    except Exception:
        logger.warning("could not import _COMMERCIAL_CLIENTS; falling back to notes only")
        return {}
    return {c["company_name"]: c.get("schedule", "") for c in _COMMERCIAL_CLIENTS}


def _per_visit_rate(client_id: str, service_type_id: str) -> float | None:
    try:
        from seeding.generators.gen_clients import get_commercial_per_visit_rate
        return get_commercial_per_visit_rate(client_id, service_type_id=service_type_id)
    except Exception as e:
        logger.warning("could not resolve per-visit rate for %s (%s): %s",
                       client_id, service_type_id, e)
        return None


def _infer_schedule(company_name: str | None, notes: str | None,
                    seed_map: dict[str, str]) -> str:
    """Return a schedule key usable as a _SCHEDULE_TO_AGREEMENTS lookup."""
    seed = seed_map.get(company_name or "")
    if seed:
        return seed
    # Fall back to notes inference using the same labels _commercial_scope uses.
    from simulation.generators.operations import _commercial_scope
    return _commercial_scope(notes)


def _ensure_client_type_column(conn) -> None:
    cols = get_column_names(conn, "recurring_agreements")
    if "client_type" not in cols:
        conn.execute(
            "ALTER TABLE recurring_agreements ADD COLUMN client_type "
            "TEXT DEFAULT 'residential'"
        )
        conn.commit()


def _existing_active_agreements(conn, client_id: str) -> set[str]:
    rows = conn.execute(
        """
        SELECT service_type_id FROM recurring_agreements
        WHERE client_id = %s AND status = 'active'
        """,
        (client_id,),
    ).fetchall()
    return {r["service_type_id"] for r in rows}


def backfill(dry_run: bool) -> dict:
    conn = get_connection()
    created: list[dict] = []
    skipped: list[dict] = []
    failed: list[dict] = []

    try:
        _ensure_client_type_column(conn)
        seed_map = _seed_schedule_by_company()
        today = date.today().isoformat()

        clients = conn.execute(
            """
            SELECT id, company_name, notes
            FROM clients
            WHERE client_type = 'commercial' AND status = 'active'
            ORDER BY company_name
            """
        ).fetchall()
        clients = [dict(c) for c in clients]

        for client in clients:
            schedule_key = _infer_schedule(
                client.get("company_name"), client.get("notes"), seed_map
            )
            agreements = _SCHEDULE_TO_AGREEMENTS.get(schedule_key, _DEFAULT_AGREEMENTS)
            existing = _existing_active_agreements(conn, client["id"])

            for service_type_id, day_of_week in agreements:
                if service_type_id in existing:
                    skipped.append({
                        "client_id": client["id"],
                        "service_type_id": service_type_id,
                        "reason": "active agreement already exists",
                    })
                    continue

                price = _per_visit_rate(client["id"], service_type_id)
                if price is None:
                    failed.append({
                        "client_id": client["id"],
                        "company_name": client.get("company_name"),
                        "service_type_id": service_type_id,
                        "reason": "could not resolve per-visit rate",
                    })
                    continue

                record = {
                    "client_id": client["id"],
                    "company_name": client.get("company_name"),
                    "service_type_id": service_type_id,
                    "price_per_visit": price,
                    "day_of_week": day_of_week,
                    "schedule_key": schedule_key,
                }

                if dry_run:
                    created.append({**record, "id": "DRY-RUN"})
                    continue

                try:
                    agreement_id = generate_id("RECUR")
                    conn.execute(
                        """
                        INSERT INTO recurring_agreements
                        (id, client_id, service_type_id, crew_id, frequency,
                         price_per_visit, start_date, status, day_of_week, client_type)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        (
                            agreement_id, client["id"], service_type_id,
                            "crew-d", "weekly", price, today,
                            "active", day_of_week, "commercial",
                        ),
                    )
                    created.append({**record, "id": agreement_id})
                except Exception as e:
                    logger.exception("insert failed for %s / %s",
                                     client["id"], service_type_id)
                    failed.append({**record, "reason": str(e)})

        if not dry_run:
            conn.commit()
        return {
            "dry_run": dry_run,
            "created": created,
            "skipped": skipped,
            "failed": failed,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true")
    group.add_argument("--execute", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    result = backfill(dry_run=args.dry_run)

    print(f"created={len(result['created'])} "
          f"skipped={len(result['skipped'])} "
          f"failed={len(result['failed'])} "
          f"dry_run={result['dry_run']}")
    for row in result["created"]:
        print(f"  created {row['id']} {row['client_id']} {row['service_type_id']} "
              f"({row['day_of_week']}) @ ${row['price_per_visit']:.2f}")
    for row in result["failed"]:
        print(f"  FAILED {row['client_id']} {row['service_type_id']}: {row['reason']}")
    return 0 if not result["failed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Dry-run the script**

```bash
python -m scripts.backfill_commercial_agreements --dry-run
```

Expected: proposes ~40+ rows (some commercial clients get 2 agreements for the nightly+Saturday split). All `skipped` are zero on a fresh run.

- [ ] **Step 3: Commit**

```bash
git add scripts/backfill_commercial_agreements.py
git commit -m "Add commercial recurring agreement backfill script (Track E)"
```

---

## Task 5: Gate Pass 1 and Pass 1b in operations.py

**Files:**
- Modify: `simulation/generators/operations.py`

- [ ] **Step 1: Add the helper near the top of `JobSchedulingGenerator.execute`**

Before Pass 1 starts, read the flag:

```python
from intelligence import config as intel_config
track_e_enabled = getattr(
    intel_config, "TRACK_E_COMMERCIAL_AGREEMENT_SCHEDULING_ENABLED", False
)
```

- [ ] **Step 2: Gate Pass 1 commercial agreements**

Inside the Pass 1 loop (`for agreement in agreements:`), immediately after `_is_due_today` check, add:

```python
if agreement.get("client_type") == "commercial" and not track_e_enabled:
    continue
```

This makes Pass 1 ignore commercial agreements until the flag is flipped.

- [ ] **Step 3: Gate Pass 1b**

In Pass 1b, after loading `commercial_clients`, skip any client that has an active agreement when the flag is True:

```python
clients_with_active_agreement: set[str] = set()
if track_e_enabled:
    rows = conn.execute(
        """
        SELECT DISTINCT client_id FROM recurring_agreements
        WHERE status = 'active' AND client_type = 'commercial'
        """
    ).fetchall()
    clients_with_active_agreement = {r["client_id"] for r in rows}

for client in commercial_clients:
    if client["id"] in clients_with_active_agreement:
        # Canonical agreement path handled this client in Pass 1
        continue
    # ... existing Pass 1b logic
```

- [ ] **Step 4: Run existing operations tests**

```bash
python -m pytest tests/test_phase5_operations.py -v
```

Expected: all existing tests still pass because flag defaults False and commercial_clients with no agreements still fall through to Pass 1b.

- [ ] **Step 5: Commit**

```bash
git add simulation/generators/operations.py
git commit -m "Gate commercial scheduling source on TRACK_E flag (Track E)"
```

---

## Task 6: Write the rollout regression tests

**Files:**
- Modify: `tests/test_track_e_commercial_agreements.py`

- [ ] **Step 1: Add tests for the gate behavior**

```python
from unittest.mock import patch


def test_flag_off_commercial_agreement_ignored_by_pass1():
    """With flag False, a commercial agreement should not generate a Pass 1 job."""
    # Uses an in-memory fixture set up in the test's setUp
    # (pattern from tests/test_phase5_operations.py line 790+).
    # Seed one commercial client + one active agreement.
    # Run execute(dry_run=True) with the flag False.
    # Assert: no jobs scheduled via agreement path.


def test_flag_on_commercial_agreement_drives_pass1_and_pass1b_skips():
    """With flag True, Pass 1 schedules from agreement; Pass 1b skips the client."""
    # Seed one commercial client + one active agreement (3x weekly, today matches).
    # Run execute(dry_run=True) with the flag True.
    # Assert: exactly one job queued for that client today.


def test_active_commercial_never_drops_to_zero_coverage_after_backfill():
    """Regression: after running backfill, every active commercial client has >=1 active agreement."""
    # Seed N active commercial clients with assorted schedule notes.
    # Run backfill(dry_run=False).
    # Query: count of active commercial clients with zero active agreements.
    # Assert: equals 0.
```

Flesh out the tests using the fixture pattern at `tests/test_phase5_operations.py` lines 790-862 (in-memory psycopg2 or sqlite3 depending on how that suite runs; follow the existing convention).

- [ ] **Step 2: Run tests — expect PASS**

```bash
python -m pytest tests/test_track_e_commercial_agreements.py -v
```

- [ ] **Step 3: Commit**

```bash
git add tests/test_track_e_commercial_agreements.py
git commit -m "Add Track E rollout regression tests"
```

---

## Task 7: Create reconciliation script

**Files:**
- Create: `scripts/reconcile_commercial_agreements.py`

- [ ] **Step 1: Implement**

```python
"""Reconciliation report for commercial recurring agreements.

Identifies three drift cases:
  A. active commercial clients with zero active agreements (gap)
  B. commercial clients whose agreement has no matching jobs in the
     last 14 days (dormant agreement)
  C. commercial clients scheduled via notes but without any agreement
     (pre-Track-E legacy path still active)

Usage:
    python -m scripts.reconcile_commercial_agreements
"""
import argparse
import json

from database.connection import get_connection


def reconcile() -> dict:
    conn = get_connection()
    try:
        gap = conn.execute(
            """
            SELECT c.id, c.company_name
            FROM clients c
            WHERE c.client_type = 'commercial'
              AND c.status = 'active'
              AND c.id NOT IN (
                  SELECT client_id FROM recurring_agreements
                  WHERE status = 'active'
              )
            ORDER BY c.company_name
            """
        ).fetchall()

        dormant = conn.execute(
            """
            SELECT ra.id AS agreement_id, ra.client_id,
                   c.company_name,
                   (SELECT MAX(scheduled_date) FROM jobs
                    WHERE client_id = ra.client_id) AS last_job_date
            FROM recurring_agreements ra
            JOIN clients c ON c.id = ra.client_id
            WHERE ra.status = 'active'
              AND c.client_type = 'commercial'
              AND (
                  SELECT MAX(scheduled_date::date) FROM jobs
                  WHERE client_id = ra.client_id
              ) < CURRENT_DATE - INTERVAL '14 days'
            ORDER BY c.company_name
            """
        ).fetchall()

        notes_only = conn.execute(
            """
            SELECT c.id, c.company_name, c.notes
            FROM clients c
            WHERE c.client_type = 'commercial'
              AND c.status = 'active'
              AND (c.notes IS NOT NULL AND c.notes <> '')
              AND c.id NOT IN (
                  SELECT client_id FROM recurring_agreements
                  WHERE status = 'active'
              )
            ORDER BY c.company_name
            """
        ).fetchall()

        return {
            "gap_active_unscheduled":   [dict(r) for r in gap],
            "dormant_agreements":       [dict(r) for r in dormant],
            "notes_only_no_agreement":  [dict(r) for r in notes_only],
        }
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    report = reconcile()
    print(json.dumps({
        "gap_count":       len(report["gap_active_unscheduled"]),
        "dormant_count":   len(report["dormant_agreements"]),
        "notes_only_count": len(report["notes_only_no_agreement"]),
    }, indent=2))
    for category, rows in report.items():
        if rows:
            print(f"\n{category}:")
            for r in rows:
                print(f"  {json.dumps(r, default=str)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Smoke-test**

```bash
python -m scripts.reconcile_commercial_agreements
```

- [ ] **Step 3: Commit**

```bash
git add scripts/reconcile_commercial_agreements.py
git commit -m "Add commercial agreement reconciliation script (Track E)"
```

---

## Task 8: Rollout plan (document only — no code in this task)

**Order of operations in production:**

1. Merge PRs from Tasks 1-7 with flag False.
2. Run `python -m scripts.audit_commercial_scheduling` to confirm the gap (expected: 40 missing).
3. Run `python -m scripts.backfill_commercial_agreements --dry-run` and review the proposed agreements.
4. Run `python -m scripts.backfill_commercial_agreements --execute` and capture the output.
5. Re-run audit — confirm `active_commercial_missing_agreement` is 0.
6. Run `python -m scripts.reconcile_commercial_agreements` — confirm `gap_count == 0`.
7. Watch one full day of simulation with flag still False. Commercial jobs should still be created by Pass 1b as before (backfilled agreements exist but are ignored by Pass 1).
8. Flip `TRACK_E_COMMERCIAL_AGREEMENT_SCHEDULING_ENABLED = True` in `intelligence/config.py`.
9. Deploy. Watch one full day: confirm Pass 1 creates commercial jobs and Pass 1b no longer does. No duplicate jobs per client per day (idempotency guard plus explicit Pass 1b skip).
10. If anything goes wrong, flip the flag back to False. Agreements remain in the DB (additive), Pass 1b resumes.

---

## Acceptance Criteria (from the remediation plan)

- [x] **Audit surfaces the gap:** `scripts/audit_commercial_scheduling.py` prints the 40/0 state.
- [x] **Backfill creates agreements:** `scripts/backfill_commercial_agreements.py` creates an agreement per active commercial client per service type.
- [x] **Uniqueness enforced:** re-running the backfill does not create duplicates.
- [x] **Feature-flagged rollout:** `TRACK_E_COMMERCIAL_AGREEMENT_SCHEDULING_ENABLED` gates the switch from notes to agreements.
- [x] **No duplicate jobs:** Pass 1b skips clients with active agreements when the flag is True; idempotency guard as backup.
- [x] **Notes path remains as fallback:** Pass 1b still runs in flag-off and for clients without agreements.
- [x] **Reconciliation for drift:** `scripts/reconcile_commercial_agreements.py` identifies the three drift cases.
- [x] **Regression test:** active commercial clients never drop to zero active agreements post-backfill.

---

## Self-review

- All referenced files exist or are created.
- `_is_due_today` change is backwards-compatible with single-day `day_of_week` strings.
- Feature flag defaults False — no production behavior changes until explicit flip.
- Backfill idempotency via existing-active check + `(client_id, service_type_id, status='active')` uniqueness key.
- Price resolved via `get_commercial_per_visit_rate` (same path used by `job_completion_flow`), so agreement rates stay consistent with invoicing.
- No DDL changes required.
