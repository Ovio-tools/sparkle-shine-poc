#!/usr/bin/env python3
"""
scripts/delete_ghost_proposals.py

One-shot cleanup: DELETE the 76 ghost commercial_proposals rows left on Railway
by the pre-PR-#7 Pipedrive syncer. Each row represents a failed
INSERT-then-register-mapping sequence from the old non-atomic code path —
the proposal row committed, the mapping insert failed (or the process died),
so the row persisted with:

  - monthly_value > 30_000    (the annual-as-monthly inflation fingerprint;
                               real monthly caps at ~$27K for commercial wons)
  - client_id IS NULL         (never linked to a canonical client)
  - lead_id IS NULL           (never linked to a lead either)
  - no cross_tool_mapping     (orphaned from every upstream tool)

Dividing by 12 would leave 3–6 copies of each real proposal at the correct
monthly, still double/quadruple-counting the pipeline. The real row still
exists (SS-PROP-0001 Barton Creek, etc., with its mappings intact), so DELETE
is the correct fix.

SAFETY
  Production writes gated behind an explicit --live flag with a 5-second
  abort window (same pattern as scripts/backfill_proposal_monthly_value.py).
  --inspect mode discovers every inbound FK to commercial_proposals from
  information_schema and refuses to proceed if any candidate is referenced.

USAGE
    # Read-only distribution + inbound FK discovery + reference check.
    python scripts/delete_ghost_proposals.py --inspect

    # List exactly which rows would be deleted; no writes.
    python scripts/delete_ghost_proposals.py --dry-run

    # Apply the DELETE (requires explicit --live + no FK references).
    python scripts/delete_ghost_proposals.py --live

Connects via DATABASE_PUBLIC_URL (for local dev against Railway) or
DATABASE_URL (inside Railway). See scripts/railway_db.py for precedent.
"""
from __future__ import annotations

import argparse
import os
import sys

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

load_dotenv()

# The inflation fingerprint. Real monthly values cap at ~$27K (commercial
# wons) and ~$4.2K (open/lost). Anything above 30K with no parent and no
# mapping is a ghost — it cannot plausibly be legitimate data.
INFLATION_THRESHOLD = 30000.0


# The filter that defines a ghost orphan. Used by every mode so inspect
# / dry-run / live are guaranteed to operate on the same set.
_CANDIDATE_WHERE = """
    cp.monthly_value > %s
    AND cp.client_id IS NULL
    AND cp.lead_id IS NULL
    AND NOT EXISTS (
        SELECT 1 FROM cross_tool_mapping m WHERE m.canonical_id = cp.id
    )
"""


def _get_url() -> str:
    url = os.environ.get("DATABASE_PUBLIC_URL") or os.environ.get("DATABASE_URL")
    if not url:
        print("ERROR: set DATABASE_PUBLIC_URL or DATABASE_URL first.", file=sys.stderr)
        raise SystemExit(1)
    return url


def _connect():
    return psycopg2.connect(_get_url(), connect_timeout=10)


def _candidate_ids(conn, threshold: float) -> list[str]:
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        f"SELECT cp.id FROM commercial_proposals cp WHERE {_CANDIDATE_WHERE}",
        (threshold,),
    )
    return [r["id"] for r in cur.fetchall()]


def _inbound_fks(conn) -> list[tuple[str, str]]:
    """Discover every (table, column) that has a FOREIGN KEY pointing at
    commercial_proposals.id, via information_schema."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT tc.table_name, kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage ccu
          ON tc.constraint_name = ccu.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND ccu.table_name = 'commercial_proposals'
          AND ccu.column_name = 'id'
        """
    )
    return [(r["table_name"], r["column_name"]) for r in cur.fetchall()]


def _count_references(conn, fks: list[tuple[str, str]], ids: list[str]) -> dict[str, int]:
    """For each (child_table, child_column), count how many rows reference
    any id in the candidate set. Returns {table.column: count}."""
    if not ids:
        return {}
    counts: dict[str, int] = {}
    cur = conn.cursor()
    for tbl, col in fks:
        cur.execute(
            # Safe: tbl and col come from information_schema, not user input.
            f"SELECT COUNT(*) FROM {tbl} WHERE {col} = ANY(%s)",
            (ids,),
        )
        counts[f"{tbl}.{col}"] = cur.fetchone()[0]
    return counts


def cmd_inspect(conn) -> int:
    ids = _candidate_ids(conn, INFLATION_THRESHOLD)
    print(f"Candidate ghost rows: {len(ids)} "
          f"(monthly_value > ${INFLATION_THRESHOLD:,.2f}, no parent, no mapping)")

    if not ids:
        print("Nothing to do.")
        return 0

    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        f"""
        SELECT cp.id, cp.title, cp.status, cp.monthly_value
        FROM commercial_proposals cp
        WHERE {_CANDIDATE_WHERE}
        ORDER BY cp.monthly_value DESC
        LIMIT 10
        """,
        (INFLATION_THRESHOLD,),
    )
    print("\nTop 10 candidates by monthly_value:")
    for row in cur.fetchall():
        print(
            f"  {row['id']:<18} {row['status']:<12} "
            f"${float(row['monthly_value']):>12,.2f}  "
            f"{(row['title'] or '')[:55]}"
        )

    fks = _inbound_fks(conn)
    print(f"\nInbound FOREIGN KEYs to commercial_proposals.id: {len(fks)}")
    for tbl, col in fks:
        print(f"  {tbl}.{col}")

    ref_counts = _count_references(conn, fks, ids)
    total_refs = sum(ref_counts.values())
    print(f"\nInbound references to candidate rows: {total_refs}")
    for path, n in ref_counts.items():
        marker = "  ⚠" if n > 0 else "   "
        print(f"{marker} {path}: {n}")

    if total_refs > 0:
        print("\nREFUSING to proceed: candidates have inbound FK references.")
        print("These rows are NOT safe to delete without first handling the children.")
        return 1

    print("\nSafe to delete — no inbound references.")
    return 0


def cmd_dry_run(conn) -> int:
    # Run the full inspect first so reference check gates dry-run too.
    rc = cmd_inspect(conn)
    if rc != 0:
        return rc

    ids = _candidate_ids(conn, INFLATION_THRESHOLD)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        f"""
        SELECT cp.id, cp.title, cp.status, cp.monthly_value
        FROM commercial_proposals cp
        WHERE {_CANDIDATE_WHERE}
        ORDER BY cp.monthly_value DESC
        """,
        (INFLATION_THRESHOLD,),
    )
    print(f"\nWould DELETE {len(ids)} row(s):")
    for row in cur.fetchall():
        print(
            f"  {row['id']:<18} {row['status']:<12} "
            f"${float(row['monthly_value']):>12,.2f}  "
            f"{(row['title'] or '')[:55]}"
        )
    return 0


def cmd_live(conn) -> int:
    # Same gating as --dry-run: reference check must pass.
    rc = cmd_inspect(conn)
    if rc != 0:
        return rc

    ids = _candidate_ids(conn, INFLATION_THRESHOLD)
    if not ids:
        print("\nNothing to delete. Exiting.")
        return 0

    print(f"\nAbout to DELETE {len(ids)} ghost row(s) from commercial_proposals.")
    print("This is destructive and runs against the configured database.")
    print("Press Ctrl+C within 5 seconds to abort...")
    import time
    try:
        time.sleep(5)
    except KeyboardInterrupt:
        print("Aborted.")
        return 1

    cur = conn.cursor()
    cur.execute(
        f"""
        DELETE FROM commercial_proposals cp
        WHERE cp.id = ANY(%s)
          AND {_CANDIDATE_WHERE}
        """,
        (ids, INFLATION_THRESHOLD),
    )
    deleted = cur.rowcount
    conn.commit()
    print(f"\nDeleted {deleted} row(s). Committed.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--inspect", action="store_true",
                      help="Read-only distribution + inbound FK discovery + reference check.")
    mode.add_argument("--dry-run", action="store_true",
                      help="List affected rows; no writes. Runs --inspect checks first.")
    mode.add_argument("--live", action="store_true",
                      help="Apply the DELETE. Requires reference check to pass.")
    args = parser.parse_args()

    conn = _connect()
    try:
        if args.inspect:
            return cmd_inspect(conn)
        if args.dry_run:
            return cmd_dry_run(conn)
        if args.live:
            return cmd_live(conn)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
