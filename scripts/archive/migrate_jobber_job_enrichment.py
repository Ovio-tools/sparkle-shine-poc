#!/usr/bin/env python3
"""Add confirmed Jobber job-enrichment columns to the PostgreSQL database.

Usage:
    python scripts/migrate_jobber_job_enrichment.py
"""

from __future__ import annotations

import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from database.connection import get_connection


_STATEMENTS = [
    "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS job_title_raw TEXT",
    "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS jobber_job_type TEXT",
    "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS is_recurring_job BOOLEAN",
    "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS jobber_updated_at TEXT",
    "CREATE INDEX IF NOT EXISTS idx_jobs_jobber_updated ON jobs(jobber_updated_at)",
]


def main() -> None:
    conn = get_connection()
    try:
        with conn:
            for statement in _STATEMENTS:
                conn.execute(statement)
    finally:
        conn.close()

    print("Jobber job enrichment migration complete.")
    print("Columns ensured: job_title_raw, jobber_job_type, is_recurring_job, jobber_updated_at")


if __name__ == "__main__":
    main()
