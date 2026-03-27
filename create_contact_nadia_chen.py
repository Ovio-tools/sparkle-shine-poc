"""
One-off script: create a single new HubSpot contact for Sparkle & Shine,
marked as a Sales Qualified Lead, and register the canonical ID in
cross_tool_mapping.

Contact profile: Nadia Chen — Office Manager at Vertex Coworking, a boutique
shared-office campus in North Loop Austin that submitted a website inquiry in
Mar 2026 requesting a weekly deep-clean proposal.

Run:
    python3 create_contact_nadia_chen.py
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
# Contact details
# ---------------------------------------------------------------------------

CONTACT = {
    "firstname":          "Nadia",
    "lastname":           "Chen",
    "email":              "nadia.chen@vertexcoworking.example.com",
    "phone":              "(512) 389-6047",
    "company":            "Vertex Coworking",
    "jobtitle":           "Office Manager",
    "address":            "507 Calles St",
    "city":               "Austin",
    "state":              "TX",
    "zip":                "78702",
    # Standard HubSpot lifecycle stage value for SQL
    "lifecyclestage":     "salesqualifiedlead",
    # Custom properties (pre-created in Phase 1 Step 6)
    "client_type":        "commercial",
    "service_frequency":  "weekly",
    "neighborhood":       "East Austin/Mueller",
    "lead_source_detail": "website_inquiry",
    "hs_lead_status":     "IN_PROGRESS",
}

# ---------------------------------------------------------------------------
# API helpers
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
    """
    Return the next available SS-LEAD-NNNN canonical ID, checking both the
    leads table and existing cross_tool_mapping LEAD entries so the new ID
    never collides with seeded or automation-assigned IDs.
    """
    row = db.execute("SELECT id FROM leads ORDER BY id DESC LIMIT 1").fetchone()
    leads_max = int(row["id"].split("-")[-1]) if row else 0

    row2 = db.execute(
        "SELECT canonical_id FROM cross_tool_mapping "
        "WHERE entity_type = 'LEAD' ORDER BY canonical_id DESC LIMIT 1"
    ).fetchone()
    mapping_max = int(row2["canonical_id"].split("-")[-1]) if row2 else 0

    next_n = max(leads_max, mapping_max) + 1
    return f"SS-LEAD-{next_n:04d}"


def register_in_db(canonical_id: str, hubspot_id: str) -> None:
    """
    Write canonical_id → hubspot:hubspot_id into cross_tool_mapping so the
    HubSpotQualifiedSync automation won't assign a duplicate canonical ID
    on the next sync run.
    """
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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 60)
    print("  Sparkle & Shine — Create SQL Contact in HubSpot")
    print("=" * 60)

    session = _build_session()

    print("\nContact to create:")
    for k, v in CONTACT.items():
        print(f"  {k:<26} {v}")

    # Assign canonical ID before the API call so we can register it
    # immediately on success.
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
