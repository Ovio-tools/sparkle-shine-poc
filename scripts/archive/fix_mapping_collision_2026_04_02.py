#!/usr/bin/env python3
"""Fix mapping collision for SS-LEAD-0242/0243/0278/0279.

Root cause: A race between concurrent automation runners caused the batch
pre-allocation in hubspot_qualified_sync.py to overwrite HubSpot mappings
via ON CONFLICT DO UPDATE.  Pipedrive records for Jasmine Reddy (person 225,
deal 246) and Samuel Evans (person 226, deal 247) were registered under the
wrong canonical IDs (Gerardo Hall's SS-LEAD-0242 and Lucas Adams's
SS-LEAD-0243).

This script:
  1. Reassigns Pipedrive mappings from SS-LEAD-0242 → SS-LEAD-0278 (Jasmine Reddy)
  2. Reassigns Pipedrive mappings from SS-LEAD-0243 → SS-LEAD-0279 (Samuel Evans)
  3. Creates Pipedrive person + deal for Lucas Adams (SS-LEAD-0243, who is an SQL)
  4. Gerardo Hall (SS-LEAD-0242) is an MQL — no Pipedrive records needed

Usage:
    python scripts/fix_mapping_collision_2026_04_02.py [--dry-run]
"""

import argparse
import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from auth import get_client
from database.schema import get_connection

# The four affected mappings: (wrong_canonical, correct_canonical, tool_name, tool_specific_id)
REASSIGNMENTS = [
    ("SS-LEAD-0242", "SS-LEAD-0278", "pipedrive",        "246"),
    ("SS-LEAD-0242", "SS-LEAD-0278", "pipedrive_person", "225"),
    ("SS-LEAD-0243", "SS-LEAD-0279", "pipedrive",        "247"),
    ("SS-LEAD-0243", "SS-LEAD-0279", "pipedrive_person", "226"),
]

# Lucas Adams needs new Pipedrive records
LUCAS_ADAMS = {
    "canonical_id":   "SS-LEAD-0243",
    "hubspot_id":     "464984539883",
    "email":          "lucas.adams@icloud.com",
    "firstname":      "Lucas",
    "lastname":       "Adams",
    "lead_source":    "Unknown",
    "client_type":    "residential",
    "lifetime_value": "0",
}

_PIPELINE_ID = 2
_STAGE_ID    = 8
_TOOL_IDS_PATH = os.path.join(PROJECT_ROOT, "config", "tool_ids.json")


def _load_field_ids():
    with open(_TOOL_IDS_PATH) as f:
        ids = json.load(f)["pipedrive"]
    return ids["person_fields"], ids["deal_fields"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    db = get_connection()

    # ── Step 1: Verify current state matches expectations ────────────────
    print("Step 1: Verifying current mapping state...")
    for wrong_cid, correct_cid, tool_name, tool_id in REASSIGNMENTS:
        row = db.execute(
            "SELECT canonical_id FROM cross_tool_mapping "
            "WHERE tool_name = %s AND tool_specific_id = %s",
            (tool_name, tool_id),
        ).fetchone()
        if row is None:
            print(f"  ERROR: No mapping found for {tool_name}:{tool_id}")
            sys.exit(1)
        if row["canonical_id"] != wrong_cid:
            print(
                f"  ERROR: Expected {tool_name}:{tool_id} mapped to {wrong_cid}, "
                f"but found {row['canonical_id']}"
            )
            sys.exit(1)
        print(f"  OK: {tool_name}:{tool_id} → {wrong_cid} (will move to {correct_cid})")

    # ── Step 2: Reassign Pipedrive mappings ──────────────────────────────
    print("\nStep 2: Reassigning Pipedrive mappings...")
    if args.dry_run:
        for wrong_cid, correct_cid, tool_name, tool_id in REASSIGNMENTS:
            print(f"  [DRY RUN] Would reassign {tool_name}:{tool_id} from {wrong_cid} → {correct_cid}")
    else:
        try:
            for wrong_cid, correct_cid, tool_name, tool_id in REASSIGNMENTS:
                db.execute(
                    "UPDATE cross_tool_mapping SET canonical_id = %s, entity_type = 'LEAD' "
                    "WHERE tool_name = %s AND tool_specific_id = %s AND canonical_id = %s",
                    (correct_cid, tool_name, tool_id, wrong_cid),
                )
                print(f"  Reassigned {tool_name}:{tool_id}: {wrong_cid} → {correct_cid}")
            db.commit()
            print("  Committed.")
        except Exception:
            db.rollback()
            raise

    # ── Step 3: Create Pipedrive records for Lucas Adams ─────────────────
    print("\nStep 3: Creating Pipedrive records for Lucas Adams (SS-LEAD-0243)...")

    # First check if Lucas already has Pipedrive mappings (idempotency)
    existing = db.execute(
        "SELECT tool_specific_id FROM cross_tool_mapping "
        "WHERE canonical_id = 'SS-LEAD-0243' AND tool_name = 'pipedrive'",
    ).fetchone()

    if existing:
        print(f"  Already has Pipedrive deal mapping: {existing['tool_specific_id']}. Skipping.")
    elif args.dry_run:
        print("  [DRY RUN] Would create Pipedrive person + deal for Lucas Adams")
    else:
        person_fields, deal_fields = _load_field_ids()
        session = get_client("pipedrive")
        base = session.base_url.rstrip("/")
        if "/v1" not in base and "/v2" not in base:
            base = f"{base}/v1"

        # Create person
        person_payload = {
            "name": "Lucas Adams",
            "email": [{"value": LUCAS_ADAMS["email"], "primary": True}],
            person_fields["HubSpot Contact ID"]: LUCAS_ADAMS["hubspot_id"],
            person_fields["Acquisition Source"]: LUCAS_ADAMS["lead_source"],
        }
        pr = session.post(f"{base}/persons", json=person_payload, timeout=30)
        pr.raise_for_status()
        person_id = str(pr.json()["data"]["id"])
        print(f"  Created Pipedrive person {person_id}")

        # Create deal
        deal_payload = {
            "title":       "Lucas Adams — Qualified Lead (Unknown)",
            "pipeline_id": _PIPELINE_ID,
            "stage_id":    _STAGE_ID,
            "value":       0,
            "currency":    "USD",
            "person_id":   int(person_id),
            deal_fields["Client Type"]:            LUCAS_ADAMS["client_type"],
            deal_fields["Estimated Monthly Value"]: 0,
            deal_fields["Lead Source"]:             LUCAS_ADAMS["lead_source"],
        }
        dr = session.post(f"{base}/deals", json=deal_payload, timeout=30)
        dr.raise_for_status()
        deal_id = str(dr.json()["data"]["id"])
        print(f"  Created Pipedrive deal {deal_id}")

        # Register mappings
        try:
            for tool_name, tool_id in [("pipedrive_person", person_id), ("pipedrive", deal_id)]:
                db.execute(
                    """
                    INSERT INTO cross_tool_mapping
                        (canonical_id, entity_type, tool_name, tool_specific_id, synced_at)
                    VALUES (%s, 'LEAD', %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT(canonical_id, tool_name) DO UPDATE SET
                        tool_specific_id = excluded.tool_specific_id,
                        synced_at        = CURRENT_TIMESTAMP
                    """,
                    ("SS-LEAD-0243", tool_name, tool_id),
                )
            db.commit()
            print(f"  Registered mappings: pipedrive_person:{person_id}, pipedrive:{deal_id}")
        except Exception:
            db.rollback()
            raise

    # ── Step 4: Verify final state ───────────────────────────────────────
    print("\nStep 4: Verification...")
    for cid in ["SS-LEAD-0242", "SS-LEAD-0243", "SS-LEAD-0278", "SS-LEAD-0279"]:
        rows = db.execute(
            "SELECT tool_name, tool_specific_id FROM cross_tool_mapping "
            "WHERE canonical_id = %s ORDER BY tool_name",
            (cid,),
        ).fetchall()
        print(f"  {cid}:")
        for r in rows:
            print(f"    {r['tool_name']:>20}: {r['tool_specific_id']}")

    db.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
