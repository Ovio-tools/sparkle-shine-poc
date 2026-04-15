"""
automations/utils/id_resolver.py

Thin wrappers around the cross_tool_mapping table for use inside automations.
"""
from typing import Optional

from database.mappings import _ENTITY_META


class MappingNotFoundError(Exception):
    """Raised when a cross_tool_mapping lookup finds no matching row."""


def _entity_type_from_canonical_id(canonical_id: str) -> str:
    """Return the entity type for a canonical SS-ID or raise on malformed values."""
    for entity_type, (prefix, _) in _ENTITY_META.items():
        if canonical_id.startswith(f"{prefix}-"):
            return entity_type

    raise ValueError(
        "canonical_id must use a known SS-TYPE-* prefix "
        f"(got {canonical_id!r})"
    )


def resolve(db, canonical_id: str, target_tool: str) -> str:
    """
    Return the tool-specific ID for a canonical SS-ID in the given tool.

    Raises MappingNotFoundError if no mapping exists.
    """
    cursor = db.execute(
        "SELECT tool_specific_id FROM cross_tool_mapping "
        "WHERE canonical_id = %s AND tool_name = %s",
        (canonical_id, target_tool),
    )
    row = cursor.fetchone()
    if row is None:
        raise MappingNotFoundError(
            f"No mapping for canonical_id='{canonical_id}' in tool='{target_tool}'"
        )
    return row["tool_specific_id"]


def reverse_resolve(
    db, tool_specific_id: str, source_tool: str, entity_type: Optional[str] = None
) -> str:
    """
    Return the canonical SS-ID for a tool-specific ID.

    Raises MappingNotFoundError if no mapping exists.
    """
    if entity_type:
        cursor = db.execute(
            "SELECT canonical_id FROM cross_tool_mapping "
            "WHERE tool_specific_id = %s AND tool_name = %s AND entity_type = %s",
            (tool_specific_id, source_tool, entity_type.upper()),
        )
    else:
        cursor = db.execute(
            "SELECT canonical_id FROM cross_tool_mapping "
            "WHERE tool_specific_id = %s AND tool_name = %s",
            (tool_specific_id, source_tool),
        )
    row = cursor.fetchone()
    if row is None:
        raise MappingNotFoundError(
            f"No mapping for tool_specific_id='{tool_specific_id}' "
            f"in tool='{source_tool}'"
        )
    return row["canonical_id"]


def register_mapping(
    db,
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
    entity_type = _entity_type_from_canonical_id(canonical_id)

    # Guard: same external ID must not point to two different canonical entities.
    existing = db.execute(
        "SELECT canonical_id FROM cross_tool_mapping "
        "WHERE tool_name = %s AND tool_specific_id = %s AND entity_type = %s",
        (tool_name, tool_specific_id, entity_type),
    ).fetchone()
    if existing is not None:
        existing_cid = existing["canonical_id"]
        if existing_cid != canonical_id:
            raise ValueError(
                f"Mapping collision: {tool_name}:{tool_specific_id} is already "
                f"registered to {existing_cid}, cannot also register to {canonical_id}"
            )

    with db:
        db.execute(
            """
            INSERT INTO cross_tool_mapping
                (canonical_id, entity_type, tool_name, tool_specific_id, synced_at)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT(canonical_id, tool_name) DO UPDATE SET
                tool_specific_id = excluded.tool_specific_id,
                synced_at        = CURRENT_TIMESTAMP
            """,
            (canonical_id, entity_type, tool_name, tool_specific_id),
        )
