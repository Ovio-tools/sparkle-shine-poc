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

    # ------------------------------------------------------------------ #
    # outreach_drafts — Automation #7 Sales Research & Outreach Agent Chain
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS outreach_drafts (
        id TEXT PRIMARY KEY,
        hubspot_contact_id TEXT NOT NULL,
        contact_name TEXT NOT NULL,
        contact_email TEXT NOT NULL,
        contact_type TEXT,
        template_set TEXT NOT NULL,
        template_variant TEXT NOT NULL,
        lead_source TEXT,
        research_confidence TEXT,
        match_confidence TEXT,
        estimated_annual_value REAL,
        gmail_draft_id TEXT,
        gmail_link TEXT,
        slack_message_ts TEXT,
        agent_1_output JSONB,
        agent_2_output JSONB,
        agent_3_output JSONB,
        total_tokens_used INTEGER,
        estimated_cost_usd REAL,
        status TEXT DEFAULT 'completed',
        error_message TEXT,
        created_at TIMESTAMP DEFAULT NOW()
    )
    """,

    # ------------------------------------------------------------------ #
    # sync_skip_list — circuit breaker for permanently failing syncs
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS sync_skip_list (
        tool_name           TEXT NOT NULL,
        tool_specific_id    TEXT NOT NULL,
        reason              TEXT NOT NULL,
        detail              TEXT,
        skipped_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (tool_name, tool_specific_id)
    )
    """,

    # Indexes for common query patterns
    "CREATE INDEX IF NOT EXISTS idx_automation_log_run_id   ON automation_log(run_id)",
    "CREATE INDEX IF NOT EXISTS idx_automation_log_status   ON automation_log(status)",
    "CREATE INDEX IF NOT EXISTS idx_pending_actions_status  ON pending_actions(status)",
    "CREATE INDEX IF NOT EXISTS idx_pending_actions_execute ON pending_actions(execute_after)",
    "CREATE INDEX IF NOT EXISTS idx_outreach_drafts_status  ON outreach_drafts(status)",
    "CREATE INDEX IF NOT EXISTS idx_outreach_drafts_contact ON outreach_drafts(hubspot_contact_id)",
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
    print("Tables ensured: poll_state, automation_log, pending_actions, outreach_drafts, sync_skip_list")


if __name__ == "__main__":
    run_migration()
