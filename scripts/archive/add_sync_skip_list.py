"""
scripts/add_sync_skip_list.py

Migration: create the sync_skip_list table on the live PostgreSQL database.
Idempotent — safe to run multiple times.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import get_connection

DDL = """
CREATE TABLE IF NOT EXISTS sync_skip_list (
    tool_name           TEXT NOT NULL,
    tool_specific_id    TEXT NOT NULL,
    reason              TEXT NOT NULL,
    detail              TEXT,
    skipped_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tool_name, tool_specific_id)
);
"""

if __name__ == "__main__":
    db = get_connection()
    with db:
        db.execute(DDL)
    print("sync_skip_list table created (or already exists).")
    db.close()
