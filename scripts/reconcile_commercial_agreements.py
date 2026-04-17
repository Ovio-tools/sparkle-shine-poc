"""Reconciliation report for commercial recurring agreements.

Identifies three drift cases:
  A. active commercial clients with zero active agreements (gap)
  B. commercial agreements whose client has no job in the last 14 days
     (dormant agreement — scheduler may have stopped firing, or client
     stopped accepting service)
  C. commercial clients scheduled via notes but without any agreement
     (pre-Track-E legacy path still active for this client)

Usage:
    python -m scripts.reconcile_commercial_agreements
"""
from __future__ import annotations

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

        # Dormant = active commercial agreement where either (a) no jobs have
        # ever been scheduled, OR (b) the most recent job is >14 days old.
        # The previous query only handled case (b): the outer `<` comparison
        # silently dropped rows where MAX(scheduled_date) IS NULL, so a brand
        # new agreement that never got scheduled would stay invisible until
        # someone manually checked. An agreement that produces zero jobs is
        # the loudest possible "scheduler never fired" signal — it must
        # surface here.
        dormant = conn.execute(
            """
            SELECT ra.id AS agreement_id,
                   ra.client_id,
                   c.company_name,
                   (
                       SELECT MAX(scheduled_date) FROM jobs
                       WHERE client_id = ra.client_id
                   ) AS last_job_date
            FROM recurring_agreements ra
            JOIN clients c ON c.id = ra.client_id
            WHERE ra.status = 'active'
              AND c.client_type = 'commercial'
              AND (
                  (SELECT MAX(scheduled_date::date) FROM jobs
                   WHERE client_id = ra.client_id) IS NULL
                  OR
                  (SELECT MAX(scheduled_date::date) FROM jobs
                   WHERE client_id = ra.client_id) < CURRENT_DATE - INTERVAL '14 days'
              )
            ORDER BY c.company_name
            """
        ).fetchall()

        notes_only = conn.execute(
            """
            SELECT c.id, c.company_name, c.notes
            FROM clients c
            WHERE c.client_type = 'commercial'
              AND c.status = 'active'
              AND c.notes IS NOT NULL
              AND c.notes <> ''
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
        "gap_count":        len(report["gap_active_unscheduled"]),
        "dormant_count":    len(report["dormant_agreements"]),
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
