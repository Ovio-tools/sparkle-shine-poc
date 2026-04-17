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
    logger.info("  total amount      : $%s", f"{summary['total_orphan_amount']:,.2f}")
    logger.info("  orphans with no matching completed job on issue_date: %d",
                summary["orphans_with_no_matching_completed_job"])
    logger.info("  by day:")
    for day, stats in summary["by_day"].items():
        by_class = ", ".join(f"{k}={v}" for k, v in sorted(stats["by_class"].items()))
        logger.info("    %s  count=%d amount=$%s (%s)",
                    day, stats["count"], f"{stats['amount']:,.2f}", by_class)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
