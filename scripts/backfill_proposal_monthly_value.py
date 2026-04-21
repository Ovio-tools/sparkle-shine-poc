#!/usr/bin/env python3
"""
scripts/backfill_proposal_monthly_value.py

One-shot backfill: Pipedrive syncer historically stored deal.value (which is
the ANNUAL contract value in the seeder convention: monthly_value * 12) into
commercial_proposals.monthly_value as if it were monthly. The metrics layer
then multiplied by 12 again when rendering the daily briefing, producing
12x-inflated annual figures like "$941,073/year" for proposals whose true
annual is ~$78K.

The syncer is now fixed (see intelligence/syncers/sync_pipedrive.py). This
script corrects the already-corrupted rows that were written before the fix.

SCOPE
  Only proposals with a 'pipedrive' mapping in cross_tool_mapping. That is
  how we know the row was round-tripped through the buggy syncer.

GUARDRAIL
  A threshold ($MIN_INFLATED_THRESHOLD) skips rows whose monthly_value is
  plausibly a true monthly figure (<= threshold). Seeded open/lost proposals
  cap at ~$4,200/month; seeded won proposals go up to ~$27,000/month. Any
  bug-affected row has monthly_value >= 12 * true_monthly, typically well
  above the seeded won ceiling. Use --inspect first to verify the
  distribution before choosing a threshold.

USAGE
    # Show the distribution; no writes. Safe.
    python scripts/backfill_proposal_monthly_value.py --inspect

    # List exactly which rows would be updated; no writes.
    python scripts/backfill_proposal_monthly_value.py --dry-run

    # Apply (requires explicit --live).
    python scripts/backfill_proposal_monthly_value.py --live

    # Override threshold if --inspect reveals a better cutoff.
    python scripts/backfill_proposal_monthly_value.py --dry-run --threshold 30000

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

# Default cutoff: true monthly values top out at ~$27K (commercial wons) and
# ~$4.2K (open/lost). Any monthly_value above this is assumed to be an
# annual figure stored by the buggy syncer.
DEFAULT_THRESHOLD = 30000.0


def _get_url() -> str:
    url = os.environ.get("DATABASE_PUBLIC_URL") or os.environ.get("DATABASE_URL")
    if not url:
        print("ERROR: set DATABASE_PUBLIC_URL or DATABASE_URL first.", file=sys.stderr)
        raise SystemExit(1)
    return url


def _connect():
    conn = psycopg2.connect(_get_url(), connect_timeout=10)
    return conn


def cmd_inspect(conn) -> None:
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT COUNT(*) AS n,
               MIN(cp.monthly_value) AS min_v,
               MAX(cp.monthly_value) AS max_v,
               ROUND(AVG(cp.monthly_value)::numeric, 2) AS avg_v
        FROM commercial_proposals cp
        JOIN cross_tool_mapping m ON m.canonical_id = cp.id
        WHERE m.tool_name = 'pipedrive' AND m.entity_type = 'PROP'
        """
    )
    stats = cur.fetchone()
    print("Pipedrive-mapped proposals (all statuses):")
    print(f"  rows       = {stats['n']}")
    print(f"  min mv     = {stats['min_v']}")
    print(f"  max mv     = {stats['max_v']}")
    print(f"  avg mv     = {stats['avg_v']}")

    buckets = [
        ("<=   $1,000",  "monthly_value <=   1000"),
        ("$1K – $5K",    "monthly_value >   1000 AND monthly_value <=   5000"),
        ("$5K – $30K",   "monthly_value >   5000 AND monthly_value <=  30000"),
        ("$30K – $100K", "monthly_value >  30000 AND monthly_value <= 100000"),
        ("> $100K",      "monthly_value > 100000"),
    ]
    print("\nDistribution:")
    for label, clause in buckets:
        cur.execute(
            f"""
            SELECT COUNT(*) AS n
            FROM commercial_proposals cp
            JOIN cross_tool_mapping m ON m.canonical_id = cp.id
            WHERE m.tool_name = 'pipedrive' AND m.entity_type = 'PROP' AND {clause}
            """
        )
        print(f"  {label:<14} {cur.fetchone()['n']}")

    print("\nTop 10 by monthly_value:")
    cur.execute(
        """
        SELECT cp.id, cp.title, cp.status, cp.monthly_value,
               ROUND((cp.monthly_value / 12.0)::numeric, 2) AS proposed_mv
        FROM commercial_proposals cp
        JOIN cross_tool_mapping m ON m.canonical_id = cp.id
        WHERE m.tool_name = 'pipedrive' AND m.entity_type = 'PROP'
        ORDER BY cp.monthly_value DESC NULLS LAST
        LIMIT 10
        """
    )
    for row in cur.fetchall():
        print(
            f"  {row['id']:<18} {row['status']:<12} "
            f"${row['monthly_value']:>12,.2f}  ->  ${row['proposed_mv']:>10,.2f}"
            f"  {(row['title'] or '')[:50]}"
        )


def _select_targets(conn, threshold: float):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT cp.id, cp.title, cp.status, cp.monthly_value,
               ROUND((cp.monthly_value / 12.0)::numeric, 2) AS proposed_mv
        FROM commercial_proposals cp
        JOIN cross_tool_mapping m ON m.canonical_id = cp.id
        WHERE m.tool_name = 'pipedrive'
          AND m.entity_type = 'PROP'
          AND cp.monthly_value > %s
        ORDER BY cp.monthly_value DESC
        """,
        (threshold,),
    )
    return cur.fetchall()


def cmd_dry_run(conn, threshold: float) -> None:
    rows = _select_targets(conn, threshold)
    print(f"Would update {len(rows)} row(s) (threshold: monthly_value > ${threshold:,.2f}):")
    for row in rows:
        print(
            f"  {row['id']:<18} {row['status']:<12} "
            f"${row['monthly_value']:>12,.2f}  ->  ${row['proposed_mv']:>10,.2f}"
            f"  {(row['title'] or '')[:50]}"
        )


def cmd_live(conn, threshold: float) -> None:
    rows = _select_targets(conn, threshold)
    if not rows:
        print("No rows to update. Exiting.")
        return
    print(f"About to update {len(rows)} row(s) (threshold: monthly_value > ${threshold:,.2f}).")
    print("Press Ctrl+C within 5 seconds to abort...")
    import time
    try:
        time.sleep(5)
    except KeyboardInterrupt:
        print("Aborted.")
        return

    cur = conn.cursor()
    cur.execute(
        """
        UPDATE commercial_proposals cp
        SET monthly_value = ROUND((monthly_value / 12.0)::numeric, 2)
        FROM cross_tool_mapping m
        WHERE m.canonical_id = cp.id
          AND m.tool_name = 'pipedrive'
          AND m.entity_type = 'PROP'
          AND cp.monthly_value > %s
        """,
        (threshold,),
    )
    updated = cur.rowcount
    conn.commit()
    print(f"Updated {updated} row(s). Committed.")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--inspect", action="store_true", help="Read-only distribution report.")
    mode.add_argument("--dry-run", action="store_true", help="List affected rows without writing.")
    mode.add_argument("--live", action="store_true", help="Apply the division /12 in-place.")
    parser.add_argument(
        "--threshold",
        type=float,
        default=DEFAULT_THRESHOLD,
        help=f"Skip rows with monthly_value <= this (default: {DEFAULT_THRESHOLD}).",
    )
    args = parser.parse_args()

    conn = _connect()
    try:
        if args.inspect:
            cmd_inspect(conn)
        elif args.dry_run:
            cmd_dry_run(conn, args.threshold)
        elif args.live:
            cmd_live(conn, args.threshold)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
