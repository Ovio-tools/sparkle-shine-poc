"""
automations/state.py

Simple read/write layer over the poll_state table.
Tracks the last-processed position for each (tool_name, entity_type) pair
so polling functions know where to resume.
"""
from typing import Optional


def get_last_poll(
    db,
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
        WHERE tool_name = %s AND entity_type = %s
        """,
        (tool_name, entity_type),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return dict(row)


def update_last_poll(
    db,
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
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT(tool_name, entity_type) DO UPDATE SET
                last_processed_id        = excluded.last_processed_id,
                last_processed_timestamp = excluded.last_processed_timestamp,
                last_poll_at             = CURRENT_TIMESTAMP
            """,
            (tool_name, entity_type, last_id, last_timestamp),
        )
