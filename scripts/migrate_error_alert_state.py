"""
scripts/migrate_error_alert_state.py

Adds the error_alert_state table used by simulation/error_reporter.py for
Slack alert suppression (one row per active (tool_name, context_key) failure
stream). Counterpart of the DDL in automations/migrate.py — run this once
against the live PostgreSQL database so services that never execute the
automation migrations (simulation-engine, intelligence) also find the table.

NOTE: database/schema.py should gain the same DDL once the in-flight Jobber
enrichment changes to that file land; until then this script plus
automations/migrate.py are the sources of the table.

Usage:
    python scripts/migrate_error_alert_state.py [--dry-run]

Requires DATABASE_URL in the environment (point it at Railway Postgres for
the production migration).
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import get_connection

_DDL = """
CREATE TABLE IF NOT EXISTS error_alert_state (
    tool_name        TEXT NOT NULL,
    context_key      TEXT NOT NULL,
    first_seen       TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen        TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_posted_at   TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    suppressed_count INTEGER NOT NULL DEFAULT 0,
    episode_count    INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (tool_name, context_key)
)
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print the DDL without executing it",
    )
    args = parser.parse_args()

    if args.dry_run:
        print("[DRY RUN] Would execute against DATABASE_URL:")
        print(_DDL)
        return

    conn = get_connection()
    try:
        with conn:
            conn.execute(_DDL)
    finally:
        conn.close()
    print("Migration complete. Table ensured: error_alert_state")


if __name__ == "__main__":
    main()
