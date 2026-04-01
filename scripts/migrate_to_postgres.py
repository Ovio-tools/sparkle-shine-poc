#!/usr/bin/env python3
"""Migrate data from SQLite (sparkle_shine.db) to PostgreSQL (DATABASE_URL).

This is the ONE place in the codebase where sqlite3 is used for migration.
All other production code uses database.connection.get_connection (PostgreSQL).

Usage:
    python scripts/migrate_to_postgres.py
    python scripts/migrate_to_postgres.py --dry-run
    python scripts/migrate_to_postgres.py --table clients --table jobs
"""

import argparse
import json
import logging
import sqlite3
import sys
from pathlib import Path

# ── Bootstrap path so imports resolve from sparkle-shine-poc/ root ──────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database.connection import get_connection
from database.schema import init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Table migration order: FK-safe (parents before children)
# ---------------------------------------------------------------------------
TABLE_ORDER = [
    "crews",
    "marketing_campaigns",
    "employees",
    "clients",
    "leads",
    "commercial_proposals",
    "recurring_agreements",
    "jobs",
    "invoices",
    "reviews",
    "tasks",
    "calendar_events",
    "documents",
    "payments",
    "marketing_interactions",
    "document_index",
    "cross_tool_mapping",
    "daily_metrics_snapshot",
    "poll_state",
    "automation_log",
    "pending_actions",
    "won_deals",
    "gmail_metadata",
]

# Tables whose SERIAL id sequence must be reset after bulk INSERT
SERIAL_TABLES = [
    "cross_tool_mapping",
    "marketing_interactions",
    "automation_log",
    "pending_actions",
    "document_index",
]

# OAuth token files → tool_name key in oauth_tokens table
OAUTH_FILES = {
    ".jobber_tokens.json": "jobber",
    ".quickbooks_tokens.json": "quickbooks",
    "token.json": "google",
}

BATCH_SIZE = 100


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sqlite_columns(sqlite_conn: sqlite3.Connection, table: str) -> list[str]:
    """Return column names from SQLite table via PRAGMA."""
    cur = sqlite_conn.execute(f"PRAGMA table_info({table})")
    return [row[1] for row in cur.fetchall()]


def _pg_columns(pg_conn, table: str) -> set[str]:
    """Return column names from PostgreSQL table via information_schema."""
    cur = pg_conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = %s",
        (table,),
    )
    return {row["column_name"] for row in cur.fetchall()}


def _pg_table_exists(pg_conn, table: str) -> bool:
    cur = pg_conn.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = 'public' AND table_name = %s",
        (table,),
    )
    return cur.fetchone() is not None


def _reset_serial_sequence(pg_conn, table: str, dry_run: bool) -> None:
    """Reset the SERIAL sequence for table.id to MAX(id) + 1."""
    sql = (
        f"SELECT setval("
        f"    pg_get_serial_sequence('{table}', 'id'),"
        f"    COALESCE((SELECT MAX(id) FROM {table}), 0) + 1,"
        f"    false"
        f")"
    )
    if dry_run:
        logger.info("[DRY RUN] Would reset SERIAL sequence: %s", sql)
        return
    pg_conn.execute(sql)
    pg_conn.commit()
    logger.info("  Reset SERIAL sequence for %s", table)


# ---------------------------------------------------------------------------
# Core migration
# ---------------------------------------------------------------------------

def migrate_table(
    sqlite_conn: sqlite3.Connection,
    pg_conn,
    table: str,
    dry_run: bool,
) -> dict:
    """Migrate one table from SQLite → PostgreSQL.

    Returns a summary dict: {rows_read, rows_inserted, rows_failed, skipped_columns}.
    """
    summary = {"rows_read": 0, "rows_inserted": 0, "rows_failed": 0, "skipped_columns": []}

    # -- Check SQLite table exists ------------------------------------------
    sqlite_tables = {
        row[0]
        for row in sqlite_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    if table not in sqlite_tables:
        logger.warning("  [SKIP] SQLite table '%s' does not exist — skipping", table)
        return summary

    # -- Check PostgreSQL table exists --------------------------------------
    if not _pg_table_exists(pg_conn, table):
        logger.warning("  [SKIP] PostgreSQL table '%s' does not exist — skipping", table)
        return summary

    # -- Compute column intersection ----------------------------------------
    sqlite_cols = _sqlite_columns(sqlite_conn, table)
    pg_cols = _pg_columns(pg_conn, table)

    shared_cols = [c for c in sqlite_cols if c in pg_cols]
    skipped = [c for c in sqlite_cols if c not in pg_cols]

    if skipped:
        logger.info("  Columns in SQLite but not PostgreSQL (skipped): %s", skipped)
        summary["skipped_columns"] = skipped

    if not shared_cols:
        logger.warning("  [SKIP] No overlapping columns for table '%s'", table)
        return summary

    # -- Read all rows from SQLite ------------------------------------------
    sqlite_conn.row_factory = sqlite3.Row
    rows = sqlite_conn.execute(
        f"SELECT {', '.join(shared_cols)} FROM {table}"
    ).fetchall()
    summary["rows_read"] = len(rows)

    if not rows:
        logger.info("  No rows in SQLite for '%s'", table)
        return summary

    # -- Build INSERT statement ---------------------------------------------
    col_list = ", ".join(shared_cols)
    placeholders = ", ".join(["%s"] * len(shared_cols))
    insert_sql = (
        f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) "
        f"ON CONFLICT DO NOTHING"
    )

    # -- Batch INSERT -------------------------------------------------------
    if dry_run:
        logger.info(
            "  [DRY RUN] Would insert %d rows into '%s'", len(rows), table
        )
        summary["rows_inserted"] = len(rows)
        return summary

    batch = []
    for row in rows:
        try:
            batch.append(tuple(row[c] for c in shared_cols))
        except Exception as exc:
            logger.warning("  Row assembly error in '%s': %s", table, exc)
            summary["rows_failed"] += 1
            continue

        if len(batch) >= BATCH_SIZE:
            inserted, failed = _flush_batch(pg_conn, insert_sql, batch, table)
            summary["rows_inserted"] += inserted
            summary["rows_failed"] += failed
            batch = []

    if batch:
        inserted, failed = _flush_batch(pg_conn, insert_sql, batch, table)
        summary["rows_inserted"] += inserted
        summary["rows_failed"] += failed

    pg_conn.commit()
    return summary


def _flush_batch(pg_conn, insert_sql: str, batch: list, table: str) -> tuple[int, int]:
    """INSERT a batch of rows; returns (inserted, failed)."""
    try:
        pg_conn.executemany(insert_sql, batch)
        return len(batch), 0
    except Exception as exc:
        pg_conn.rollback()
        logger.error("  Batch INSERT failed for '%s': %s — retrying row-by-row", table, exc)
        inserted = 0
        failed = 0
        for row in batch:
            try:
                pg_conn.execute(insert_sql, row)
                pg_conn.commit()
                inserted += 1
            except Exception as row_exc:
                pg_conn.rollback()
                logger.warning("  Row failed in '%s': %s", table, row_exc)
                failed += 1
        return inserted, failed


# ---------------------------------------------------------------------------
# OAuth token seeding
# ---------------------------------------------------------------------------

def seed_oauth_tokens(pg_conn, repo_root: Path, dry_run: bool) -> None:
    """Read local OAuth JSON files and INSERT into oauth_tokens table."""
    logger.info("Seeding OAuth tokens...")

    for filename, tool_name in OAUTH_FILES.items():
        token_path = repo_root / filename
        if not token_path.exists():
            logger.info("  [SKIP] %s not found", token_path)
            continue

        try:
            token_data = json.loads(token_path.read_text())
        except Exception as exc:
            logger.warning("  Could not read %s: %s", filename, exc)
            continue

        if dry_run:
            logger.info(
                "  [DRY RUN] Would upsert oauth_tokens row for tool_name='%s'", tool_name
            )
            continue

        try:
            pg_conn.execute(
                """
                INSERT INTO oauth_tokens (tool_name, token_data, updated_at)
                VALUES (%s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (tool_name) DO UPDATE SET
                    token_data = EXCLUDED.token_data,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (tool_name, json.dumps(token_data)),
            )
            pg_conn.commit()
            logger.info("  Upserted oauth_tokens for '%s'", tool_name)
        except Exception as exc:
            pg_conn.rollback()
            logger.error("  Failed to upsert oauth_tokens for '%s': %s", tool_name, exc)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migrate sparkle_shine.db (SQLite) → PostgreSQL (DATABASE_URL)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log what would happen without writing to PostgreSQL",
    )
    parser.add_argument(
        "--table",
        action="append",
        dest="tables",
        metavar="TABLE",
        help="Migrate only the named table(s). May be repeated.",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent.parent
    sqlite_path = repo_root / "sparkle_shine.db"

    if not sqlite_path.exists():
        logger.error("SQLite database not found: %s", sqlite_path)
        sys.exit(1)

    tables_to_migrate = args.tables if args.tables else TABLE_ORDER

    logger.info("=" * 60)
    logger.info("Sparkle & Shine: SQLite → PostgreSQL migration")
    if args.dry_run:
        logger.info("  MODE: DRY RUN (no writes)")
    logger.info("  Source: %s", sqlite_path)
    logger.info("  Tables: %s", tables_to_migrate)
    logger.info("=" * 60)

    # -- Step 1: Open connections ------------------------------------------
    sqlite_conn = sqlite3.connect(str(sqlite_path))

    pg_conn = get_connection()

    # -- Step 2: Create PostgreSQL schema ---------------------------------
    logger.info("Initialising PostgreSQL schema...")
    if not args.dry_run:
        init_db()
        logger.info("  Schema ready")
    else:
        logger.info("  [DRY RUN] Would call init_db()")

    # -- Step 3: Migrate tables -------------------------------------------
    totals = {"rows_read": 0, "rows_inserted": 0, "rows_failed": 0}
    per_table = {}

    for table in tables_to_migrate:
        logger.info("Migrating table: %s", table)
        try:
            result = migrate_table(sqlite_conn, pg_conn, table, dry_run=args.dry_run)
        except Exception as exc:
            logger.error("  Unexpected error migrating '%s': %s", table, exc)
            result = {"rows_read": 0, "rows_inserted": 0, "rows_failed": 0, "skipped_columns": []}

        per_table[table] = result
        totals["rows_read"] += result["rows_read"]
        totals["rows_inserted"] += result["rows_inserted"]
        totals["rows_failed"] += result["rows_failed"]
        logger.info(
            "  Done: read=%d  inserted=%d  failed=%d",
            result["rows_read"],
            result["rows_inserted"],
            result["rows_failed"],
        )

    # -- Step 4: Reset SERIAL sequences -----------------------------------
    logger.info("Resetting SERIAL sequences...")
    for table in SERIAL_TABLES:
        if table in tables_to_migrate:
            try:
                _reset_serial_sequence(pg_conn, table, dry_run=args.dry_run)
            except Exception as exc:
                logger.error("  Failed to reset sequence for '%s': %s", table, exc)

    # -- Step 5: Seed OAuth tokens ----------------------------------------
    seed_oauth_tokens(pg_conn, repo_root, dry_run=args.dry_run)

    # -- Step 6: Close connections ----------------------------------------
    sqlite_conn.close()
    pg_conn.close()

    # -- Step 7: Print summary --------------------------------------------
    logger.info("")
    logger.info("=" * 60)
    logger.info("MIGRATION SUMMARY")
    logger.info("=" * 60)
    logger.info("%-30s %8s %8s %8s", "Table", "Read", "Inserted", "Failed")
    logger.info("-" * 60)
    for table in tables_to_migrate:
        r = per_table.get(table, {})
        logger.info(
            "%-30s %8d %8d %8d",
            table,
            r.get("rows_read", 0),
            r.get("rows_inserted", 0),
            r.get("rows_failed", 0),
        )
    logger.info("-" * 60)
    logger.info(
        "%-30s %8d %8d %8d",
        "TOTAL",
        totals["rows_read"],
        totals["rows_inserted"],
        totals["rows_failed"],
    )
    logger.info("=" * 60)

    if totals["rows_failed"] > 0:
        logger.warning("%d row(s) failed — review logs above", totals["rows_failed"])
        sys.exit(1)
    else:
        logger.info("Migration complete.")


if __name__ == "__main__":
    main()
