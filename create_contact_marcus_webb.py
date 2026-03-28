"""
One-off script: create a single new HubSpot contact for Sparkle & Shine,
marked as a Sales Qualified Lead, and register the canonical ID in
cross_tool_mapping.

Run:
    python3 create_contact_marcus_webb.py
"""

import os
import sys

_ROOT = os.path.dirname(os.path.abspath(__file__))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import requests
from credentials import get_credential
from database.schema import get_connection

_BASE_URL = os.getenv("HUBSPOT_BASE_URL", "https://api.hubapi.com")
_DB_PATH  = os.path.join(_ROOT, "sparkle_shine.db")

# ---------------------------------------------------------------------------
# Contact details — edit this block only
# ---------------------------------------------------------------------------

CONTACT = {
    "firstname":          "Marcus",
    "lastname":           "Webb",
    "email":              "marcus.webb@zephyrworkspaces.com",
    "phone":              "(512) 555-0147",
    "company":            "Zephyr Workspaces",               # Always include
    "jobtitle":           "Operations Director",
    "address":            "801 Congress Ave",
    "city":               "Austin",
    "state":              "TX",
    "zip":                "78701",
    "lifecyclestage":     "salesqualifiedlead",      # Always salesqualifiedlead
    "client_type":        "commercial",              # or "residential"
    "service_frequency":  "weekly",                  # weekly / biweekly / monthly
    "neighborhood":       "Downtown",
    "lead_source_detail": "website_inquiry",         # or "referral", etc.
    "hs_lead_status":     "IN_PROGRESS",
}

# ---------------------------------------------------------------------------
# Helpers — do not edit below this line
# ---------------------------------------------------------------------------

def _build_session() -> requests.Session:
    token = get_credential("HUBSPOT_ACCESS_TOKEN")
    s = requests.Session()
    s.headers.update({
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
    })
    return s


def create_contact(session: requests.Session, properties: dict) -> dict:
    url = f"{_BASE_URL}/crm/v3/objects/contacts"
    resp = session.post(url, json={"properties": properties}, timeout=30)
    if resp.status_code not in (200, 201):
        raise RuntimeError(
            f"HubSpot POST /contacts returned {resp.status_code}: {resp.text[:400]}"
        )
    return resp.json()


def _next_lead_canonical_id(db) -> str:
    row = db.execute("SELECT id FROM leads ORDER BY id DESC LIMIT 1").fetchone()
    leads_max = int(row["id"].split("-")[-1]) if row else 0

    row2 = db.execute(
        "SELECT canonical_id FROM cross_tool_mapping "
        "WHERE entity_type = 'LEAD' ORDER BY canonical_id DESC LIMIT 1"
    ).fetchone()
    mapping_max = int(row2["canonical_id"].split("-")[-1]) if row2 else 0

    return f"SS-LEAD-{max(leads_max, mapping_max) + 1:04d}"


def register_in_db(canonical_id: str, hubspot_id: str) -> None:
    db = get_connection(_DB_PATH)
    db.row_factory = __import__("sqlite3").Row
    try:
        existing = db.execute(
            "SELECT canonical_id FROM cross_tool_mapping "
            "WHERE tool_name = 'hubspot' AND tool_specific_id = ?",
            (hubspot_id,),
        ).fetchone()
        if existing is not None:
            existing_cid = existing["canonical_id"]
            if existing_cid != canonical_id:
                raise ValueError(
                    f"HubSpot contact {hubspot_id} is already registered "
                    f"to {existing_cid} — not registering again."
                )
            print(f"  [INFO] Mapping already exists: {canonical_id} → hubspot:{hubspot_id}")
            return
        with db:
            db.execute(
                """
                INSERT INTO cross_tool_mapping
                    (canonical_id, entity_type, tool_name, tool_specific_id, synced_at)
                VALUES (?, 'LEAD', 'hubspot', ?, datetime('now'))
                ON CONFLICT(canonical_id, tool_name) DO UPDATE SET
                    tool_specific_id = excluded.tool_specific_id,
                    synced_at        = datetime('now')
                """,
                (canonical_id, hubspot_id),
            )
    finally:
        db.close()


def main() -> None:
    print("=" * 60)
    print("  Sparkle & Shine — Create SQL Contact in HubSpot")
    print("=" * 60)

    session = _build_session()

    print("\nContact to create:")
    for k, v in CONTACT.items():
        print(f"  {k:<26} {v}")

    db = get_connection(_DB_PATH)
    db.row_factory = __import__("sqlite3").Row
    canonical_id = _next_lead_canonical_id(db)
    db.close()

    print(f"\n  Assigned canonical ID : {canonical_id}")
    print("\nPushing to HubSpot...")
    result = create_contact(session, CONTACT)

    hs_id    = result.get("id")
    hs_email = result.get("properties", {}).get("email", "")
    hs_stage = result.get("properties", {}).get("lifecyclestage", "")

    print("\n[OK] Contact created successfully.")
    print(f"  HubSpot ID     : {hs_id}")
    print(f"  Email          : {hs_email}")
    print(f"  Lifecycle stage: {hs_stage}")

    print(f"\nRegistering mapping: {canonical_id} → hubspot:{hs_id}")
    register_in_db(canonical_id, hs_id)
    print(f"[OK] cross_tool_mapping updated.")

    print(f"\n  View in HubSpot: https://app.hubspot.com/contacts/*/contact/{hs_id}")


if __name__ == "__main__":
    main()
