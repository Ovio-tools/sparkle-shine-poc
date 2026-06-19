#!/usr/bin/env python3
"""
scripts/backfill_won_deals_client_type.py

One-shot backfill: DealGenerator historically stored Pipedrive's enum option ID
(e.g. "27") in won_deals.client_type instead of the label (e.g. "residential").

Pipedrive returns enum custom-field values as option IDs on read but accepts
labels on write — so the seeder, which only writes, never noticed. The
generator, which round-trips, did.

The generator is now fixed (see simulation/generators/deals.py). This script
corrects already-corrupted rows and adds a CHECK constraint so future drift
fails loud.

Steps performed:
  1. Fetch the live Client Type option mapping from Pipedrive.
  2. Update won_deals rows whose client_type is one of the option IDs.
  3. Verify there are no remaining out-of-spec values.
  4. Add a CHECK constraint to won_deals.client_type (only if it doesn't
     already exist and step 3 passed).

USAGE
    DATABASE_PUBLIC_URL=... python scripts/backfill_won_deals_client_type.py --dry-run
    DATABASE_PUBLIC_URL=... python scripts/backfill_won_deals_client_type.py --apply

Connects via DATABASE_PUBLIC_URL (local against Railway) or DATABASE_URL
(inside Railway). See scripts/railway_db.py for precedent.
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

_VALID_LABELS = ("residential", "commercial", "one-time")
_CHECK_CONSTRAINT_NAME = "won_deals_client_type_chk"


def _get_url() -> str:
    url = os.environ.get("DATABASE_PUBLIC_URL") or os.environ.get("DATABASE_URL")
    if not url:
        print("ERROR: set DATABASE_PUBLIC_URL or DATABASE_URL first.", file=sys.stderr)
        raise SystemExit(1)
    return url


def _connect():
    return psycopg2.connect(_get_url(), connect_timeout=10)


def _fetch_pipedrive_options() -> dict[str, str]:
    """Return {option_id_str: label} for the Client Type deal field."""
    import json
    from pathlib import Path
    from auth import get_client

    tool_ids = json.loads(Path(_PROJECT_ROOT, "config", "tool_ids.json").read_text())
    field_key = tool_ids["pipedrive"]["deal_fields"]["Client Type"]

    client = get_client("pipedrive")
    resp = client.get(
        "https://api.pipedrive.com/v1/dealFields",
        params={"start": 0, "limit": 500},
        timeout=15,
    )
    resp.raise_for_status()
    for f in resp.json().get("data") or []:
        if isinstance(f, dict) and f.get("key") == field_key:
            return {str(o["id"]): o["label"] for o in (f.get("options") or [])}
    raise RuntimeError(f"Client Type field {field_key!r} not found in dealFields")


def _affected_rows(conn, mapping: dict[str, str]) -> list[dict]:
    """Return won_deals rows whose client_type is an option ID we can translate."""
    if not mapping:
        return []
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT canonical_id, client_type, service_frequency, contract_value, start_date
        FROM won_deals
        WHERE client_type = ANY(%s)
        ORDER BY start_date, canonical_id
        """,
        (list(mapping.keys()),),
    )
    return [dict(r) for r in cur.fetchall()]


def _out_of_spec_rows(conn) -> list[dict]:
    """Return won_deals rows whose client_type is not one of the valid labels."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT canonical_id, client_type
        FROM won_deals
        WHERE client_type IS NULL OR client_type <> ALL(%s)
        ORDER BY canonical_id
        """,
        (list(_VALID_LABELS),),
    )
    return [dict(r) for r in cur.fetchall()]


def _check_constraint_exists(conn) -> bool:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1
        FROM pg_constraint
        WHERE conname = %s
          AND conrelid = 'won_deals'::regclass
        """,
        (_CHECK_CONSTRAINT_NAME,),
    )
    return cur.fetchone() is not None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true",
                       help="Print actions without modifying any rows")
    group.add_argument("--apply", action="store_true",
                       help="Apply the backfill and add the CHECK constraint")
    args = parser.parse_args()

    dry_run = not args.apply

    print("Fetching Pipedrive Client Type option mapping...")
    try:
        mapping = _fetch_pipedrive_options()
    except Exception as exc:
        print(f"ERROR: could not fetch Pipedrive options: {exc}", file=sys.stderr)
        return 1
    print("  Options:")
    for opt_id, label in sorted(mapping.items(), key=lambda kv: int(kv[0])):
        print(f"    {opt_id} = {label}")

    bad_label_ids = [oid for oid, label in mapping.items() if label not in _VALID_LABELS]
    if bad_label_ids:
        print(
            f"WARNING: option labels not in {_VALID_LABELS}: "
            f"{[(oid, mapping[oid]) for oid in bad_label_ids]}"
        )

    conn = _connect()
    try:
        rows = _affected_rows(conn, mapping)
        print(f"\nFound {len(rows)} won_deals row(s) storing an option ID:")
        for r in rows:
            label = mapping.get(r["client_type"], "?")
            print(
                f"  {r['canonical_id']}  client_type={r['client_type']!r:>6} → {label!r:<14}  "
                f"service_frequency={r['service_frequency']!r}"
            )

        if not rows and not args.apply:
            existing = _out_of_spec_rows(conn)
            if existing:
                print(
                    f"\n{len(existing)} won_deals row(s) have non-translatable client_type values:"
                )
                for r in existing:
                    print(f"  {r['canonical_id']}  client_type={r['client_type']!r}")

        constraint_exists = _check_constraint_exists(conn)
        print(
            f"\nCHECK constraint {_CHECK_CONSTRAINT_NAME!r}: "
            f"{'already present' if constraint_exists else 'missing'}"
        )

        if dry_run:
            updated = len(rows)
            would_add_constraint = not constraint_exists
            print()
            print(f"[dry-run] would update:        {updated} row(s)")
            print(f"[dry-run] would add constraint: {'yes' if would_add_constraint else 'no'}")
            print()
            print("Re-run with --apply to commit.")
            return 0

        cur = conn.cursor()
        updated = 0
        for r in rows:
            label = mapping[r["client_type"]]
            if label not in _VALID_LABELS:
                print(
                    f"  SKIP {r['canonical_id']}: option label {label!r} not in "
                    f"{_VALID_LABELS}; manual review required"
                )
                continue
            cur.execute(
                "UPDATE won_deals SET client_type = %s WHERE canonical_id = %s "
                "AND client_type = %s",
                (label, r["canonical_id"], r["client_type"]),
            )
            updated += cur.rowcount
            print(f"  updated {r['canonical_id']}: {r['client_type']!r} → {label!r}")

        leftover = _out_of_spec_rows(conn)
        if leftover:
            print(
                f"\nERROR: {len(leftover)} won_deals row(s) still have client_type values "
                f"outside {_VALID_LABELS}; refusing to add CHECK constraint:"
            )
            for r in leftover:
                print(f"  {r['canonical_id']}  client_type={r['client_type']!r}")
            conn.rollback()
            return 1

        if not constraint_exists:
            cur.execute(
                f"ALTER TABLE won_deals "
                f"ADD CONSTRAINT {_CHECK_CONSTRAINT_NAME} "
                f"CHECK (client_type IN ('residential', 'commercial', 'one-time'))"
            )
            print(f"  added CHECK constraint {_CHECK_CONSTRAINT_NAME!r}")
            constraint_added = True
        else:
            constraint_added = False

        conn.commit()
        print()
        print(f"updated:          {updated}")
        print(f"constraint added: {'yes' if constraint_added else 'no (already present)'}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
