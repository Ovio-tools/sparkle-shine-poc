"""
automations/utils/id_resolver.py

Thin wrappers around the cross_tool_mapping table for use inside automations.
"""
import sqlite3
from typing import Optional


class MappingNotFoundError(Exception):
    """Raised when a cross_tool_mapping lookup finds no matching row."""


def resolve(db: sqlite3.Connection, canonical_id: str, target_tool: str) -> str:
    """
    Return the tool-specific ID for a canonical SS-ID in the given tool.

    Raises MappingNotFoundError if no mapping exists.
    """
    cursor = db.execute(
        "SELECT tool_specific_id FROM cross_tool_mapping "
        "WHERE canonical_id = ? AND tool_name = ?",
        (canonical_id, target_tool),
    )
    row = cursor.fetchone()
    if row is None:
        raise MappingNotFoundError(
            f"No mapping for canonical_id='{canonical_id}' in tool='{target_tool}'"
        )
    return row[0] if not hasattr(row, "keys") else row["tool_specific_id"]


def reverse_resolve(
    db: sqlite3.Connection, tool_specific_id: str, source_tool: str
) -> str:
    """
    Return the canonical SS-ID for a tool-specific ID.

    Raises MappingNotFoundError if no mapping exists.
    """
    cursor = db.execute(
        "SELECT canonical_id FROM cross_tool_mapping "
        "WHERE tool_specific_id = ? AND tool_name = ?",
        (tool_specific_id, source_tool),
    )
    row = cursor.fetchone()
    if row is None:
        raise MappingNotFoundError(
            f"No mapping for tool_specific_id='{tool_specific_id}' "
            f"in tool='{source_tool}'"
        )
    return row[0] if not hasattr(row, "keys") else row["canonical_id"]


def register_mapping(
    db: sqlite3.Connection,
    canonical_id: str,
    tool_name: str,
    tool_specific_id: str,
) -> None:
    """
    Insert a new cross_tool_mapping row (upsert on conflict).
    Derives entity_type from the canonical_id prefix (e.g. SS-CLIENT-0001 → CLIENT).

    Raises ValueError if tool_specific_id is already mapped to a *different*
    canonical_id — this catches cross-contaminated mappings before they are written.
    """
    # Guard: same external ID must not point to two different canonical entities.
    existing = db.execute(
        "SELECT canonical_id FROM cross_tool_mapping "
        "WHERE tool_name = ? AND tool_specific_id = ?",
        (tool_name, tool_specific_id),
    ).fetchone()
    if existing is not None:
        existing_cid = existing["canonical_id"] if hasattr(existing, "keys") else existing[0]
        if existing_cid != canonical_id:
            raise ValueError(
                f"Mapping collision: {tool_name}:{tool_specific_id} is already "
                f"registered to {existing_cid}, cannot also register to {canonical_id}"
            )

    parts = canonical_id.split("-")
    entity_type = parts[1] if len(parts) >= 3 else "UNKNOWN"

    with db:
        db.execute(
            """
            INSERT INTO cross_tool_mapping
                (canonical_id, entity_type, tool_name, tool_specific_id, synced_at)
            VALUES (?, ?, ?, ?, datetime('now'))
            ON CONFLICT(canonical_id, tool_name) DO UPDATE SET
                tool_specific_id = excluded.tool_specific_id,
                synced_at        = datetime('now')
            """,
            (canonical_id, entity_type, tool_name, tool_specific_id),
        )
