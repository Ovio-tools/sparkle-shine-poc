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
from __future__ import annotations

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
        "coverage_pct": round(
            100 * len(with_agreement) / max(1, len(active)), 1
        ),
        "per_client": rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="print per-client rows in addition to the summary",
    )
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
