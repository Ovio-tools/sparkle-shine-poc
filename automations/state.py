"""
automations/state.py

Simple read/write layer over the poll_state table.
Tracks the last-processed position for each (tool_name, entity_type) pair
so polling functions know where to resume.
"""
import sqlite3
from typing import Optional


def get_last_poll(
    db: sqlite3.Connection,
    tool_name: str,
    entity_type: str,
) -> Optional[dict]:
    """
    Return the poll_state row for (tool_name, entity_type) as a plain dict,
    or None if no prior poll has been recorded.
    """
    cursor = db.execute(
        """
        SELECT tool_name, entity_type, last_processed_id,
               last_processed_timestamp, last_poll_at
        FROM poll_state
        WHERE tool_name = ? AND entity_type = ?
        """,
        (tool_name, entity_type),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    # Support both sqlite3.Row and plain tuple
    if hasattr(row, "keys"):
        return dict(row)
    keys = ["tool_name", "entity_type", "last_processed_id",
            "last_processed_timestamp", "last_poll_at"]
    return dict(zip(keys, row))


def update_last_poll(
    db: sqlite3.Connection,
    tool_name: str,
    entity_type: str,
    last_id: Optional[str],
    last_timestamp: Optional[str],
) -> None:
    """
    Upsert a row in poll_state. Sets last_poll_at to the current UTC time.
    """
    with db:
        db.execute(
            """
            INSERT INTO poll_state
                (tool_name, entity_type, last_processed_id,
                 last_processed_timestamp, last_poll_at)
            VALUES (?, ?, ?, ?, datetime('now'))
            ON CONFLICT(tool_name, entity_type) DO UPDATE SET
                last_processed_id        = excluded.last_processed_id,
                last_processed_timestamp = excluded.last_processed_timestamp,
                last_poll_at             = datetime('now')
            """,
            (tool_name, entity_type, last_id, last_timestamp),
        )
