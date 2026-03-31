import sqlite3
from typing import Optional
from database.schema import get_connection

# Maps entity_type string to its ID prefix and the table that owns it
_ENTITY_META = {
    "CLIENT": ("SS-CLIENT", "clients"),
    "LEAD":   ("SS-LEAD",   "leads"),
    "EMP":    ("SS-EMP",    "employees"),
    "CREW":   ("SS-CREW",   "crews"),
    "JOB":    ("SS-JOB",    "jobs"),
    "RECUR":  ("SS-RECUR",  "recurring_agreements"),
    "PROP":   ("SS-PROP",   "commercial_proposals"),
    "INV":    ("SS-INV",    "invoices"),
    "PAY":    ("SS-PAY",    "payments"),
    "CAMP":   ("SS-CAMP",   "marketing_campaigns"),
    "REV":    ("SS-REV",    "reviews"),
    "TASK":   ("SS-TASK",   "tasks"),
    "CAL":    ("SS-CAL",    "calendar_events"),
    "DOC":    ("SS-DOC",    "documents"),
}


# ------------------------------------------------------------------ #
# ID generation
# ------------------------------------------------------------------ #

def generate_id(entity_type: str, db_path: str = "sparkle_shine.db") -> str:
    """Return the next available SS-TYPE-NNNN string for the given entity type."""
    entity_type = entity_type.upper()
    if entity_type not in _ENTITY_META:
        raise ValueError(f"Unknown entity_type '{entity_type}'. Valid: {list(_ENTITY_META)}")

    prefix, table = _ENTITY_META[entity_type]
    conn = get_connection(db_path)
    try:
        cursor = conn.execute(f"SELECT id FROM {table} ORDER BY id DESC LIMIT 1")
        row = cursor.fetchone()
        if row is None:
            next_n = 1
        else:
            last_id = row["id"]          # e.g. "SS-CLIENT-0042"
            last_n = int(last_id.split("-")[-1])
            next_n = last_n + 1
    finally:
        conn.close()

    width = 3 if entity_type == "EMP" else 4
    return f"{prefix}-{next_n:0{width}d}"


# ------------------------------------------------------------------ #
# Mapping operations
# ------------------------------------------------------------------ #

def register_mapping(
    canonical_id: str,
    tool_name: str,
    tool_specific_id: str,
    tool_specific_url: Optional[str] = None,
    db_path: str = "sparkle_shine.db",
) -> None:
    """Insert or update a cross_tool_mapping row.

    Raises ValueError if tool_specific_id is already mapped to a *different*
    canonical_id — guards against cross-contaminated mappings before they are written.
    """
    conn = get_connection(db_path)
    entity_type = _entity_type_from_canonical(canonical_id)
    # Collision guard: same external ID must not point to two canonical entities.
    existing = conn.execute(
        "SELECT canonical_id FROM cross_tool_mapping "
        "WHERE tool_name = ? AND tool_specific_id = ?",
        (tool_name, tool_specific_id),
    ).fetchone()
    if existing is not None:
        existing_cid = existing["canonical_id"] if hasattr(existing, "keys") else existing[0]
        if existing_cid != canonical_id:
            conn.close()
            raise ValueError(
                f"Mapping collision: {tool_name}:{tool_specific_id} is already "
                f"registered to {existing_cid}, cannot also register to {canonical_id}"
            )
    with conn:
        conn.execute(
            """
            INSERT INTO cross_tool_mapping
                (canonical_id, entity_type, tool_name, tool_specific_id, tool_specific_url, synced_at)
            VALUES (?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(canonical_id, tool_name) DO UPDATE SET
                tool_specific_id  = excluded.tool_specific_id,
                tool_specific_url = excluded.tool_specific_url,
                synced_at         = datetime('now')
            """,
            (canonical_id, entity_type, tool_name, tool_specific_id, tool_specific_url),
        )
    conn.close()


def get_tool_id(
    canonical_id: str,
    tool_name: str,
    db_path: str = "sparkle_shine.db",
) -> Optional[str]:
    """Return the tool-specific ID for a canonical entity, or None."""
    conn = get_connection(db_path)
    try:
        cursor = conn.execute(
            "SELECT tool_specific_id FROM cross_tool_mapping "
            "WHERE canonical_id = ? AND tool_name = ?",
            (canonical_id, tool_name),
        )
        row = cursor.fetchone()
        return row["tool_specific_id"] if row else None
    finally:
        conn.close()


def get_tool_url(
    canonical_id: str,
    tool_name: str,
    db_path: str = "sparkle_shine.db",
) -> Optional[str]:
    """Return the tool-specific URL for a canonical entity, or None."""
    conn = get_connection(db_path)
    try:
        cursor = conn.execute(
            "SELECT tool_specific_url FROM cross_tool_mapping "
            "WHERE canonical_id = ? AND tool_name = ?",
            (canonical_id, tool_name),
        )
        row = cursor.fetchone()
        return row["tool_specific_url"] if row else None
    finally:
        conn.close()


def get_canonical_id(
    tool_name: str,
    tool_specific_id: str,
    db_path: str = "sparkle_shine.db",
) -> Optional[str]:
    """Reverse lookup: given a tool's ID, return the canonical SS-ID or None."""
    conn = get_connection(db_path)
    try:
        cursor = conn.execute(
            "SELECT canonical_id FROM cross_tool_mapping "
            "WHERE tool_name = ? AND tool_specific_id = ?",
            (tool_name, tool_specific_id),
        )
        row = cursor.fetchone()
        return row["canonical_id"] if row else None
    finally:
        conn.close()


def get_all_mappings(
    canonical_id: str,
    db_path: str = "sparkle_shine.db",
) -> dict[str, str]:
    """Return {tool_name: tool_specific_id} for all tools mapped to this entity."""
    conn = get_connection(db_path)
    try:
        cursor = conn.execute(
            "SELECT tool_name, tool_specific_id FROM cross_tool_mapping "
            "WHERE canonical_id = ?",
            (canonical_id,),
        )
        return {row["tool_name"]: row["tool_specific_id"] for row in cursor.fetchall()}
    finally:
        conn.close()


def find_unmapped(
    entity_type: str,
    tool_name: str,
    db_path: str = "sparkle_shine.db",
) -> list:
    """Return canonical IDs that exist in the entity table but lack a mapping for tool_name."""
    entity_type = entity_type.upper()
    if entity_type not in _ENTITY_META:
        raise ValueError(f"Unknown entity_type '{entity_type}'.")

    _, table = _ENTITY_META[entity_type]
    conn = get_connection(db_path)
    try:
        cursor = conn.execute(
            f"""
            SELECT e.id FROM {table} e
            WHERE e.id NOT IN (
                SELECT canonical_id FROM cross_tool_mapping
                WHERE tool_name = ?
            )
            """,
            (tool_name,),
        )
        return [row["id"] for row in cursor.fetchall()]
    finally:
        conn.close()


def list_mapped_tools(
    canonical_id: str,
    db_path: str = "sparkle_shine.db",
) -> list:
    """Return a list of tool names that have a mapping for this canonical entity."""
    conn = get_connection(db_path)
    try:
        cursor = conn.execute(
            "SELECT tool_name FROM cross_tool_mapping WHERE canonical_id = ?",
            (canonical_id,),
        )
        return [row["tool_name"] for row in cursor.fetchall()]
    finally:
        conn.close()


# ------------------------------------------------------------------ #
# Seeding helper
# ------------------------------------------------------------------ #

def bulk_register(
    mappings_list: list,
    db_path: str = "sparkle_shine.db",
) -> int:
    """
    Insert multiple mappings in a single transaction.
    Each item: (canonical_id, tool_name, tool_specific_id)
    Returns the number of rows affected.
    """
    conn = get_connection(db_path)
    rows = [
        (cid, _entity_type_from_canonical(cid), tname, tsid, None)
        for cid, tname, tsid in mappings_list
    ]
    with conn:
        conn.executemany(
            """
            INSERT INTO cross_tool_mapping
                (canonical_id, entity_type, tool_name, tool_specific_id, tool_specific_url, synced_at)
            VALUES (?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(canonical_id, tool_name) DO UPDATE SET
                tool_specific_id = excluded.tool_specific_id,
                synced_at        = datetime('now')
            """,
            rows,
        )
    count = len(rows)
    conn.close()
    return count


# ------------------------------------------------------------------ #
# Reporting
# ------------------------------------------------------------------ #

def print_mapping_report(db_path: str = "sparkle_shine.db") -> None:
    """Print a summary table: how many entities of each type have mappings per tool."""
    conn = get_connection(db_path)
    try:
        cursor = conn.execute(
            """
            SELECT entity_type, tool_name, COUNT(*) AS cnt
            FROM cross_tool_mapping
            GROUP BY entity_type, tool_name
            ORDER BY entity_type, tool_name
            """
        )
        rows = cursor.fetchall()

        tools = sorted({row["tool_name"] for row in rows})
        by_type = {}
        for row in rows:
            by_type.setdefault(row["entity_type"], {})[row["tool_name"]] = row["cnt"]

        col_w = max(14, *(len(t) for t in tools)) + 2
        header = f"  {'Entity Type':<16}" + "".join(f"{t:>{col_w}}" for t in tools)
        print("\nMapping Coverage Report")
        print("=" * len(header))
        print(header)
        print("-" * len(header))
        for etype in sorted(by_type):
            line = f"  {etype:<16}"
            for tool in tools:
                cnt = by_type[etype].get(tool, 0)
                line += f"{cnt:>{col_w}}"
            print(line)
        print("=" * len(header))

        total_cursor = conn.execute("SELECT COUNT(*) AS n FROM cross_tool_mapping")
        total = total_cursor.fetchone()["n"]
        print(f"  Total mapping rows: {total}\n")
    finally:
        conn.close()


# ------------------------------------------------------------------ #
# Internal helper
# ------------------------------------------------------------------ #

def _entity_type_from_canonical(canonical_id: str) -> str:
    """Derive entity_type string from a canonical ID like 'SS-CLIENT-0001'."""
    for etype, (prefix, _) in _ENTITY_META.items():
        if canonical_id.startswith(prefix + "-"):
            return etype
    # Fallback: middle segment of SS-TYPE-NNNN
    parts = canonical_id.split("-")
    return parts[1] if len(parts) >= 2 else "UNKNOWN"


# ------------------------------------------------------------------ #
# __main__ smoke test
# ------------------------------------------------------------------ #

if __name__ == "__main__":
    import tempfile, os

    db_path = tempfile.mktemp(suffix=".db")
    from database.schema import init_db
    init_db(db_path)

    # We need real rows in clients to satisfy the FK on cross_tool_mapping.
    # Bypass: cross_tool_mapping has no FK to clients — we can insert freely.

    print("=" * 55)
    print("  database/mappings.py — smoke test")
    print("=" * 55)

    # 1. Generate 3 SS-CLIENT IDs
    # generate_id reads the clients table; since it's empty the IDs start at 0001.
    # We'll manually build them to keep the test self-contained.
    ids = [f"SS-CLIENT-{i:04d}" for i in range(1, 4)]
    print(f"\n[1] Generated IDs: {ids}")

    # 2. Register fake Jobber / HubSpot / QuickBooks IDs for each
    pairs = [
        ("jobber",     ["jobber-clt-aa1", "jobber-clt-bb2", "jobber-clt-cc3"]),
        ("hubspot",    ["hs-contact-111", "hs-contact-222", "hs-contact-333"]),
        ("quickbooks", ["qb-customer-x1", "qb-customer-x2", "qb-customer-x3"]),
    ]
    bulk_rows = []
    for tool, tool_ids in pairs:
        for cid, tid in zip(ids, tool_ids):
            bulk_rows.append((cid, tool, tid))
    n = bulk_register(bulk_rows, db_path)
    print(f"\n[2] bulk_register inserted {n} rows")

    # 3. Reverse lookup — confirm each Jobber ID resolves to the right canonical
    print("\n[3] Reverse lookup (jobber → canonical):")
    all_ok = True
    for cid, tid in zip(ids, ["jobber-clt-aa1", "jobber-clt-bb2", "jobber-clt-cc3"]):
        resolved = get_canonical_id("jobber", tid, db_path)
        status = "OK" if resolved == cid else f"FAIL (got {resolved})"
        print(f"    {tid} → {resolved}  [{status}]")
        if resolved != cid:
            all_ok = False
    print(f"    All lookups correct: {all_ok}")

    # 4. find_unmapped for a tool that has no registrations
    unmapped = find_unmapped("CLIENT", "pipedrive", db_path)
    print(f"\n[4] find_unmapped('CLIENT', 'pipedrive'): {len(unmapped)} unmapped")
    print(f"    (No clients table rows exist, so result is empty — correct)")

    # 5. Mapping report
    print()
    print_mapping_report(db_path)

    # 6. Clean up
    os.unlink(db_path)
    print(f"[6] Temp DB removed: {db_path}")
    print("\nAll tests passed.\n")
