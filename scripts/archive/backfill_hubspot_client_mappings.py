"""
Backfill missing HubSpot client mappings.

Default mode is read-only: search HubSpot for email-based client matches and
report what would be linked. With --apply, writes cross_tool_mapping rows.
With --create-missing, creates HubSpot contacts for emailful clients that do
not already exist in HubSpot.

Usage:
    python3 scripts/backfill_hubspot_client_mappings.py
    python3 scripts/backfill_hubspot_client_mappings.py --apply
    python3 scripts/backfill_hubspot_client_mappings.py --apply --create-missing
"""
from __future__ import annotations

import argparse
import os
import sys

from dotenv import load_dotenv

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

from auth import get_client
from database.connection import get_connection
from database.mappings import register_mapping


def _missing_clients():
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT c.id, c.first_name, c.last_name, c.email, c.phone
            FROM clients c
            WHERE COALESCE(c.email, '') <> ''
              AND NOT EXISTS (
                  SELECT 1 FROM cross_tool_mapping m
                  WHERE m.canonical_id = c.id AND m.tool_name = 'hubspot'
              )
            ORDER BY c.id
            """
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def _search_contact(hs_client, email: str):
    from hubspot.crm.contacts import PublicObjectSearchRequest

    req = PublicObjectSearchRequest(
        filter_groups=[{
            "filters": [{
                "propertyName": "email",
                "operator": "EQ",
                "value": email,
            }]
        }],
        properties=["email", "firstname", "lastname", "lifecyclestage"],
        limit=1,
    )
    result = hs_client.crm.contacts.search_api.do_search(req, _request_timeout=30)
    if not result.results:
        return None
    contact = result.results[0]
    found_email = (
        (getattr(contact, "properties", {}) or {}).get("email", "")
        .strip()
        .lower()
    )
    if found_email != email.strip().lower():
        return None
    return contact


def _create_contact(hs_client, client_row: dict):
    from hubspot.crm.contacts import SimplePublicObjectInputForCreate

    properties = {
        "email": client_row["email"],
        "lifecyclestage": "customer",
    }
    if client_row.get("first_name"):
        properties["firstname"] = client_row["first_name"]
    if client_row.get("last_name"):
        properties["lastname"] = client_row["last_name"]
    if client_row.get("phone"):
        properties["phone"] = client_row["phone"]

    return hs_client.crm.contacts.basic_api.create(
        SimplePublicObjectInputForCreate(properties=properties),
        _request_timeout=30,
    )


def _reassign_lead_mapping(hs_id: str, client_id: str) -> bool:
    """Move a HubSpot mapping from a lead canonical ID to the client."""
    conn = get_connection()
    try:
        row = conn.execute(
            """
            SELECT canonical_id
            FROM cross_tool_mapping
            WHERE tool_name = 'hubspot' AND tool_specific_id = %s
            """,
            (hs_id,),
        ).fetchone()
        if row is None:
            return False

        existing = row["canonical_id"]
        if not str(existing).startswith("SS-LEAD-"):
            return False

        conn.execute(
            """
            UPDATE cross_tool_mapping
            SET canonical_id = %s,
                entity_type = 'CLIENT',
                synced_at = CURRENT_TIMESTAMP
            WHERE canonical_id = %s
              AND tool_name = 'hubspot'
              AND tool_specific_id = %s
            """,
            (client_id, existing, hs_id),
        )
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill missing HubSpot client mappings")
    parser.add_argument("--apply", action="store_true", help="Write cross_tool_mapping rows")
    parser.add_argument(
        "--create-missing",
        action="store_true",
        help="Create HubSpot contacts for emailful clients not already present in HubSpot",
    )
    args = parser.parse_args()

    clients = _missing_clients()
    if not clients:
        print("No emailful clients are missing HubSpot mappings.")
        return 0

    hs_client = get_client("hubspot")

    found = 0
    created = 0
    skipped = 0

    for client_row in clients:
        client_id = client_row["id"]
        email = client_row["email"]
        name = f"{client_row.get('first_name') or ''} {client_row.get('last_name') or ''}".strip()
        contact = _search_contact(hs_client, email)

        if contact is not None:
            hs_id = str(contact.id)
            found += 1
            action = "link"
        elif args.create_missing:
            created_contact = _create_contact(hs_client, client_row)
            hs_id = str(created_contact.id)
            created += 1
            action = "create"
        else:
            skipped += 1
            print(f"SKIP   {client_id} {email} ({name}) -- not found in HubSpot")
            continue

        if args.apply:
            try:
                register_mapping(client_id, "hubspot", hs_id)
            except ValueError:
                if _reassign_lead_mapping(hs_id, client_id):
                    print(f"REASSIGN {client_id} {email} -> hubspot:{hs_id}")
                    continue
                raise
            print(f"{action.upper():6} {client_id} {email} -> hubspot:{hs_id}")
        else:
            print(f"WOULD {action.upper():6} {client_id} {email} -> hubspot:{hs_id}")

    print(
        f"\nSummary: {len(clients)} missing, "
        f"{found} found, {created} created, {skipped} skipped"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
