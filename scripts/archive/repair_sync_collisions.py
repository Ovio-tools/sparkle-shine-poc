"""
scripts/repair_sync_collisions.py

One-time data repair for the HubSpot-Pipedrive sync collision damage
discovered on 2026-04-06.  See docs/superpowers/specs/2026-04-06-hubspot-
pipedrive-sync-collision-fix-design.md Section 5.

Fixes:
1. Three stuck HubSpot contacts with no cross_tool_mapping entry:
   - 464805888705 (Kavya Vargas)
   - 464850822862 (Gerardo Hall)
   - 464848180951 (Lucas Adams)
   For each: find the Pipedrive person by email, find the canonical ID
   that owns it, merge the HubSpot ID under that canonical ID.

2. Person 220 dual-mapping: mapped to both SS-CLIENT-0328 and SS-LEAD-0051.
   Keep SS-CLIENT-0328, remove SS-LEAD-0051 mapping.

Safe to run multiple times (idempotent).
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import get_connection

# ── Stuck HubSpot contacts ──────────────────────────────────────────────────
STUCK_CONTACTS = [
    "464805888705",
    "464850822862",
    "464848180951",
]

# ── Person 220 dual-mapping ─────────────────────────────────────────────────
DUAL_MAPPED_PERSON = "220"
KEEP_CANONICAL = "SS-CLIENT-0328"
REMOVE_CANONICAL = "SS-LEAD-0051"


def repair_stuck_contacts(db):
    """
    For each stuck HubSpot contact, find the canonical ID that owns the
    corresponding Pipedrive person and register the HubSpot ID under it.
    """
    print("\n── Repairing stuck HubSpot contacts ──")

    for hs_id in STUCK_CONTACTS:
        # Check if already repaired (has a mapping now)
        existing = db.execute(
            "SELECT canonical_id FROM cross_tool_mapping "
            "WHERE tool_name = 'hubspot' AND tool_specific_id = %s",
            (hs_id,),
        ).fetchone()
        if existing:
            print(f"  [SKIP] {hs_id} already mapped to {existing['canonical_id']}")
            continue

        # Find the canonical ID by looking at which leads were allocated
        # around the same time and checking their Pipedrive person ownership.
        # We search automation_log for the failed sync attempts to find
        # which canonical_id was attempted.
        log_row = db.execute(
            """
            SELECT trigger_detail FROM automation_log
            WHERE automation_name = 'HubSpotQualifiedSync'
              AND action_name = 'sync_contact_to_pipedrive'
              AND status = 'failed'
              AND trigger_detail LIKE %s
            ORDER BY id DESC LIMIT 1
            """,
            (f'%{hs_id}%',),
        ).fetchone()

        if log_row and log_row["trigger_detail"]:
            import json
            detail = json.loads(log_row["trigger_detail"])
            attempted_cid = detail.get("canonical_id", "")
            if attempted_cid and attempted_cid != "unallocated":
                # Find what pipedrive_person was involved by looking at the
                # error message in the same log entry
                err_row = db.execute(
                    """
                    SELECT error_message FROM automation_log
                    WHERE automation_name = 'HubSpotQualifiedSync'
                      AND action_name = 'sync_contact_to_pipedrive'
                      AND status = 'failed'
                      AND trigger_detail LIKE %s
                    ORDER BY id DESC LIMIT 1
                    """,
                    (f'%{hs_id}%',),
                ).fetchone()

                if err_row and err_row["error_message"]:
                    err_msg = err_row["error_message"]
                    # Extract the canonical_id that owns the person from
                    # "Mapping collision: pipedrive_person:NNN is already
                    #  registered to SS-LEAD-XXXX"
                    if "already registered to" in err_msg:
                        owner_cid = err_msg.split("already registered to ")[-1].split(",")[0].strip()
                        if owner_cid.startswith("SS-"):
                            # Merge: register HubSpot ID under the owner
                            entity_type = owner_cid.split("-")[1]
                            with db:
                                db.execute(
                                    """
                                    INSERT INTO cross_tool_mapping
                                        (canonical_id, entity_type, tool_name,
                                         tool_specific_id, synced_at)
                                    VALUES (%s, %s, 'hubspot', %s, CURRENT_TIMESTAMP)
                                    ON CONFLICT(canonical_id, tool_name) DO NOTHING
                                    """,
                                    (owner_cid, entity_type, hs_id),
                                )
                            print(f"  [FIX]  {hs_id} → merged into {owner_cid}")
                            continue

        print(f"  [SKIP] {hs_id} — could not determine owner from automation_log")


def repair_dual_mapping(db):
    """Remove the SS-LEAD-0051 → pipedrive_person:220 mapping."""
    print("\n── Repairing dual-mapped person 220 ──")

    row = db.execute(
        "SELECT canonical_id FROM cross_tool_mapping "
        "WHERE tool_name = 'pipedrive_person' "
        "  AND tool_specific_id = %s "
        "  AND canonical_id = %s",
        (DUAL_MAPPED_PERSON, REMOVE_CANONICAL),
    ).fetchone()

    if not row:
        print(f"  [SKIP] No {REMOVE_CANONICAL} → pipedrive_person:{DUAL_MAPPED_PERSON} mapping found")
        return

    with db:
        db.execute(
            "DELETE FROM cross_tool_mapping "
            "WHERE tool_name = 'pipedrive_person' "
            "  AND tool_specific_id = %s "
            "  AND canonical_id = %s",
            (DUAL_MAPPED_PERSON, REMOVE_CANONICAL),
        )
    print(f"  [FIX]  Removed {REMOVE_CANONICAL} → pipedrive_person:{DUAL_MAPPED_PERSON}")

    # Verify the keeper is still intact
    keeper = db.execute(
        "SELECT canonical_id FROM cross_tool_mapping "
        "WHERE tool_name = 'pipedrive_person' "
        "  AND tool_specific_id = %s "
        "  AND canonical_id = %s",
        (DUAL_MAPPED_PERSON, KEEP_CANONICAL),
    ).fetchone()
    if keeper:
        print(f"  [OK]   {KEEP_CANONICAL} → pipedrive_person:{DUAL_MAPPED_PERSON} intact")
    else:
        print(f"  [WARN] {KEEP_CANONICAL} → pipedrive_person:{DUAL_MAPPED_PERSON} NOT found!")


def add_stuck_to_skip_list(db):
    """Add stuck contacts to sync_skip_list so they don't retry endlessly."""
    print("\n── Adding stuck contacts to sync_skip_list ──")
    for hs_id in STUCK_CONTACTS:
        with db:
            db.execute(
                """
                INSERT INTO sync_skip_list (tool_name, tool_specific_id, reason, detail)
                VALUES ('hubspot', %s, 'collision_limit', 'Pre-existing collision, repaired by script')
                ON CONFLICT (tool_name, tool_specific_id) DO NOTHING
                """,
                (hs_id,),
            )
        print(f"  [OK]   {hs_id} added to skip list")


if __name__ == "__main__":
    db = get_connection()

    dry_run = "--dry-run" in sys.argv
    if dry_run:
        print("DRY RUN — no changes will be made")
        print("(Remove --dry-run to execute)")
        db.close()
        sys.exit(0)

    repair_stuck_contacts(db)
    repair_dual_mapping(db)
    add_stuck_to_skip_list(db)

    print("\n── Done ──")
    db.close()
