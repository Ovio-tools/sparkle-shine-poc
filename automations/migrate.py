"""
automations/migrate.py

Adds the 3 automation-support tables to the PostgreSQL database (DATABASE_URL).
Idempotent: safe to run multiple times (CREATE TABLE IF NOT EXISTS).

Usage:
    python automations/migrate.py
"""
import os
import sys

# Allow running from the project root
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from database.connection import get_connection

_MIGRATIONS = [
    # ------------------------------------------------------------------ #
    # poll_state — tracks last-processed state per tool per entity type
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS poll_state (
        tool_name               TEXT NOT NULL,
        entity_type             TEXT NOT NULL,
        last_processed_id       TEXT,
        last_processed_timestamp TEXT,
        last_poll_at            TEXT NOT NULL,
        PRIMARY KEY (tool_name, entity_type)
    )
    """,

    # ------------------------------------------------------------------ #
    # automation_log — audit trail for every action in every automation
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS automation_log (
        id              SERIAL PRIMARY KEY,
        run_id          TEXT NOT NULL,
        automation_name TEXT NOT NULL,
        trigger_source  TEXT,
        trigger_detail  TEXT,
        action_name     TEXT NOT NULL,
        action_target   TEXT,
        status          TEXT NOT NULL CHECK(status IN ('success','failed','skipped')),
        error_message   TEXT,
        created_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,

    # ------------------------------------------------------------------ #
    # pending_actions — delayed / scheduled actions
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS pending_actions (
        id                  SERIAL PRIMARY KEY,
        automation_name     TEXT NOT NULL,
        action_name         TEXT NOT NULL,
        trigger_context     TEXT NOT NULL,
        execute_after       TEXT NOT NULL,
        status              TEXT NOT NULL DEFAULT 'pending'
                                CHECK(status IN ('pending','executed','failed')),
        created_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        executed_at         TEXT
    )
    """,

    # Indexes for common query patterns
    "CREATE INDEX IF NOT EXISTS idx_automation_log_run_id   ON automation_log(run_id)",
    "CREATE INDEX IF NOT EXISTS idx_automation_log_status   ON automation_log(status)",
    "CREATE INDEX IF NOT EXISTS idx_pending_actions_status  ON pending_actions(status)",
    "CREATE INDEX IF NOT EXISTS idx_pending_actions_execute ON pending_actions(execute_after)",
]


def run_migration(db_path: str = None) -> None:
    conn = get_connection()
    try:
        with conn:
            for stmt in _MIGRATIONS:
                conn.execute(stmt)
    finally:
        conn.close()
    print("Migration complete.")
    print("Tables ensured: poll_state, automation_log, pending_actions")


if __name__ == "__main__":
    run_migration()
