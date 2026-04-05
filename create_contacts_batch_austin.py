"""
One-off script: create 2 new HubSpot contacts for Sparkle & Shine and
register their canonical IDs in cross_tool_mapping.

Contact 1: Nathan Westhoff — commercial lead, referral (Austin TX)
Contact 2: Priscilla Moran — residential lead, referral (Austin TX)

Run:
    python3 create_contacts_batch_austin.py
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
# Contact details — verified unique against existing 642 HubSpot contacts
# ---------------------------------------------------------------------------

CONTACTS = [
    {
        "firstname":          "Nathan",
        "lastname":           "Westhoff",
        "email":              "n.westhoff@westhoffcapital.com",
        "phone":              "(512) 555-0183",
        "company":            "Westhoff Capital Group",
        "address":            "500 W 2nd St Ste 1800",
        "zip":                "78701",
        "city":               "Austin",
        "state":              "TX",
        "lifecyclestage":     "lead",
        "client_type":        "commercial",
        "lead_source_detail": "referral",
    },
    {
        "firstname":          "Priscilla",
        "lastname":           "Moran",
        "email":              "priscilla.moran@icloud.com",
        "phone":              "(512) 555-0341",
        "address":            "4719 Duval St",
        "zip":                "78751",
        "city":               "Austin",
        "state":              "TX",
        "lifecyclestage":     "lead",
        "client_type":        "residential",
        "lead_source_detail": "referral",
    },
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_session() -> requests.Session:
    token = get_credential("HUBSPOT_ACCESS_TOKEN")
    s = requests.Session()
    s.headers.update({
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
    })
    return s


def _email_already_exists(session: requests.Session, email: str):
    """Return the HubSpot contact ID if the email already exists, else None."""
    payload = {
        "filterGroups": [{
            "filters": [{
                "propertyName": "email",
                "operator": "EQ",
                "value": email.lower(),
            }]
        }],
        "properties": ["email", "firstname", "lastname"],
        "limit": 1,
    }
    resp = session.post(
        f"{_BASE_URL}/crm/v3/objects/contacts/search",
        json=payload,
        timeout=30,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"HubSpot search returned {resp.status_code}: {resp.text[:300]}")
    results = resp.json().get("results", [])
    return results[0]["id"] if results else None


def create_contact(session: requests.Session, properties: dict) -> dict:
    resp = session.post(
        f"{_BASE_URL}/crm/v3/objects/contacts",
        json={"properties": properties},
        timeout=30,
    )
    if resp.status_code not in (200, 201):
        raise RuntimeError(
            f"HubSpot POST /contacts returned {resp.status_code}: {resp.text[:400]}"
        )
    return resp.json()


def _next_lead_canonical_id(db, offset: int = 0) -> str:
    """
    Return the next available SS-LEAD-NNNN, bumped by `offset` for the
    second contact created in the same run (before DB is committed).
    """
    row = db.execute("SELECT id FROM leads ORDER BY id DESC LIMIT 1").fetchone()
    leads_max = int(row["id"].split("-")[-1]) if row else 0

    row2 = db.execute(
        "SELECT canonical_id FROM cross_tool_mapping "
        "WHERE entity_type = 'LEAD' ORDER BY canonical_id DESC LIMIT 1"
    ).fetchone()
    mapping_max = int(row2["canonical_id"].split("-")[-1]) if row2 else 0

    next_n = max(leads_max, mapping_max) + 1 + offset
    return f"SS-LEAD-{next_n:04d}"


def register_in_db(canonical_id: str, hubspot_id: str) -> None:
    db = get_connection(_DB_PATH)
    try:
        existing = db.execute(
            "SELECT canonical_id FROM cross_tool_mapping "
            "WHERE tool_name = 'hubspot' AND tool_specific_id = %s",
            (hubspot_id,),
        ).fetchone()
        if existing is not None:
            existing_cid = existing["canonical_id"]
            if existing_cid != canonical_id:
                raise ValueError(
                    f"HubSpot contact {hubspot_id} already registered "
                    f"to {existing_cid} — aborting."
                )
            print(f"  [INFO] Mapping already exists: {canonical_id} → hubspot:{hubspot_id}")
            return

        db.execute(
            """
            INSERT INTO cross_tool_mapping
                (canonical_id, entity_type, tool_name, tool_specific_id, synced_at)
            VALUES (%s, 'LEAD', 'hubspot', %s, CURRENT_TIMESTAMP)
            ON CONFLICT (canonical_id, tool_name) DO UPDATE SET
                tool_specific_id = EXCLUDED.tool_specific_id,
                synced_at        = CURRENT_TIMESTAMP
            """,
            (canonical_id, hubspot_id),
        )
        db.commit()
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 65)
    print("  Sparkle & Shine — Create 2 New HubSpot Contacts")
    print("=" * 65)

    session = _build_session()

    # Pre-flight uniqueness check
    print("\n[Step 1] Verifying emails don't already exist in HubSpot...")
    for contact in CONTACTS:
        existing_id = _email_already_exists(session, contact["email"])
        if existing_id:
            print(f"  [ABORT] {contact['email']} already exists (HubSpot ID: {existing_id})")
            sys.exit(1)
        print(f"  [OK]    {contact['email']} — not found, safe to create")

    # Fetch canonical ID base (both contacts use the same DB snapshot)
    db = get_connection(_DB_PATH)
    db.row_factory = __import__("sqlite3").Row
    created = []

    for offset, contact in enumerate(CONTACTS):
        canonical_id = _next_lead_canonical_id(db, offset=offset)
        created.append({"canonical_id": canonical_id, "contact": contact})
    db.close()

    # Create contacts
    print("\n[Step 2] Creating contacts in HubSpot...")
    for entry in created:
        contact      = entry["contact"]
        canonical_id = entry["canonical_id"]
        name = f"{contact['firstname']} {contact['lastname']}"

        print(f"\n  → {name} ({contact['client_type']}, {canonical_id})")
        for k, v in contact.items():
            print(f"      {k:<26} {v}")

        result = create_contact(session, contact)
        hs_id = result["id"]
        entry["hs_id"] = hs_id

        print(f"  [OK] Created — HubSpot ID: {hs_id}")
        print(f"       View: https://app.hubspot.com/contacts/*/contact/{hs_id}")

    # Register in cross_tool_mapping
    print("\n[Step 3] Registering canonical IDs in cross_tool_mapping...")
    for entry in created:
        canonical_id = entry["canonical_id"]
        hs_id        = entry["hs_id"]
        register_in_db(canonical_id, hs_id)
        print(f"  [OK] {canonical_id} → hubspot:{hs_id}")

    # Summary
    print("\n" + "=" * 65)
    print("  Summary")
    print("=" * 65)
    for entry in created:
        c = entry["contact"]
        print(
            f"  {entry['canonical_id']}  |  "
            f"{c['firstname']} {c['lastname']}  |  "
            f"{c['email']}  |  {c['client_type']}"
        )
    print("\nDone.")


if __name__ == "__main__":
    main()
