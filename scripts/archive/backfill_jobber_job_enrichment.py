#!/usr/bin/env python3
"""Backfill confirmed Jobber job enrichment fields by running a full sync.

This script is intended for Railway / PostgreSQL environments after running
`scripts/migrate_jobber_job_enrichment.py`.

Usage:
    python scripts/backfill_jobber_job_enrichment.py
"""

from __future__ import annotations

import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from database.connection import get_connection
from intelligence.syncers.sync_jobber import JobberSyncer


def _snapshot(conn) -> dict[str, int]:
    row = conn.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE service_type_id = 'residential-clean') AS generic_service_type,
            COUNT(*) FILTER (WHERE notes IS NOT NULL AND BTRIM(notes) <> '') AS jobs_with_notes,
            COUNT(*) FILTER (WHERE job_title_raw IS NOT NULL AND BTRIM(job_title_raw) <> '') AS jobs_with_raw_title,
            COUNT(*) FILTER (WHERE jobber_job_type IS NOT NULL AND BTRIM(jobber_job_type) <> '') AS jobs_with_job_type,
            COUNT(*) FILTER (WHERE is_recurring_job IS TRUE) AS jobs_marked_recurring
        FROM jobs
        """
    ).fetchone()
    return dict(row)


def main() -> None:
    conn = get_connection()
    try:
        before = _snapshot(conn)
    finally:
        conn.close()

    print("Before backfill:")
    for key, value in before.items():
        print(f"  {key}: {value}")

    syncer = JobberSyncer("sparkle_shine.db")
    try:
        result = syncer.sync(since=None)
    finally:
        syncer.close()

    conn = get_connection()
    try:
        after = _snapshot(conn)
    finally:
        conn.close()

    print("\nJobber sync complete:")
    print(f"  records_synced: {result.records_synced}")
    print(f"  errors: {len(result.errors)}")
    if result.errors:
        for err in result.errors[:10]:
            print(f"    - {err}")

    print("\nAfter backfill:")
    for key, value in after.items():
        delta = value - before.get(key, 0)
        print(f"  {key}: {value} ({delta:+d})")


if __name__ == "__main__":
    main()
