#!/usr/bin/env python3
"""Add UNIQUE(entity_type, tool_name, tool_specific_id) to cross_tool_mapping.

Prevents the race condition where concurrent automation runners silently
overwrite each other's mappings via ON CONFLICT DO UPDATE, causing
Pipedrive records to be registered under the wrong canonical IDs.

Before adding the constraint, deduplicates same-entity-type rows by
keeping the lower-numbered (older) canonical ID and removing the
higher-numbered duplicate.  All removed rows are logged to stdout.

Corresponding schema.py change must be committed alongside this script.

Usage:
    python scripts/add_mapping_unique_constraint.py [--dry-run]
"""

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from database.schema import get_connection


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    db = get_connection()

    # ── Step 1: Find same-entity-type duplicates ─────────────────────────
    dupes = db.execute("""
        SELECT entity_type, tool_name, tool_specific_id,
               array_agg(id ORDER BY canonical_id) AS row_ids,
               array_agg(canonical_id ORDER BY canonical_id) AS canonical_ids
        FROM cross_tool_mapping
        GROUP BY entity_type, tool_name, tool_specific_id
        HAVING COUNT(*) > 1
        ORDER BY entity_type, tool_name, tool_specific_id
    """).fetchall()

    if dupes:
        print(f"Found {len(dupes)} same-entity-type duplicate groups to clean up:\n")
    else:
        print("No same-entity-type duplicates found.\n")

    # ── Step 2: Remove duplicates (keep the lowest canonical_id) ─────────
    ids_to_delete = []
    for d in dupes:
        keep_id = d["row_ids"][0]
        keep_cid = d["canonical_ids"][0]
        remove_ids = d["row_ids"][1:]
        remove_cids = d["canonical_ids"][1:]
        print(
            f"  [{d['entity_type']}] {d['tool_name']}:{d['tool_specific_id']}: "
            f"keep {keep_cid} (row {keep_id}), "
            f"remove {remove_cids} (rows {remove_ids})"
        )
        ids_to_delete.extend(remove_ids)

    if ids_to_delete:
        print(f"\nTotal rows to remove: {len(ids_to_delete)}")
        if args.dry_run:
            print("[DRY RUN] Would delete these rows and add unique index.")
            db.close()
            return

        try:
            db.execute(
                "DELETE FROM cross_tool_mapping WHERE id = ANY(%s)",
                (ids_to_delete,),
            )
            print(f"Deleted {len(ids_to_delete)} duplicate rows.")
        except Exception:
            db.rollback()
            raise

    # ── Step 3: Check if the unique index already exists ─────────────────
    existing = db.execute("""
        SELECT 1 FROM pg_indexes
        WHERE tablename = 'cross_tool_mapping'
          AND indexname  = 'uq_mapping_tool_id_per_entity'
    """).fetchone()

    if existing:
        print("\nUnique index uq_mapping_tool_id_per_entity already exists. Committing cleanup only.")
        db.commit()
        db.close()
        return

    # ── Step 4: Add unique index ─────────────────────────────────────────
    if args.dry_run:
        print("\n[DRY RUN] Would CREATE UNIQUE INDEX uq_mapping_tool_id_per_entity")
        db.close()
        return

    # Commit the deletes first so the CREATE INDEX doesn't see them
    db.commit()

    # CREATE INDEX CONCURRENTLY must run outside a transaction.
    # Access the raw psycopg2 connection for autocommit control.
    raw_conn = db._conn if hasattr(db, '_conn') else db
    old_autocommit = raw_conn.autocommit
    raw_conn.autocommit = True
    try:
        cur = raw_conn.cursor()
        cur.execute("""
            CREATE UNIQUE INDEX CONCURRENTLY uq_mapping_tool_id_per_entity
            ON cross_tool_mapping (entity_type, tool_name, tool_specific_id)
        """)
        cur.close()
        print("\nCreated unique index uq_mapping_tool_id_per_entity.")
    except Exception as e:
        print(f"\nFailed to create index: {e}")
        print("You may need to run: DROP INDEX IF EXISTS uq_mapping_tool_id_per_entity; and retry.")
        raise
    finally:
        raw_conn.autocommit = old_autocommit

    db.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
