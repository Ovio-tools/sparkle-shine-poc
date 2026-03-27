# Skill: Create a New HubSpot Contact (Sales Qualified Lead)

## Overview

Use this skill whenever you need to create a brand new HubSpot contact with complete
profile details (including company name), mark the contact as a Sales Qualified Lead,
and immediately register the canonical ID in `cross_tool_mapping` so the
`HubSpotQualifiedSync` automation handles it correctly.

---

## Step 1 — Decide the contact's details

Gather all required and optional fields before writing any code.

### Required fields

| HubSpot property   | Notes                                    |
|--------------------|------------------------------------------|
| `firstname`        | Contact first name                       |
| `lastname`         | Contact last name                        |
| `email`            | Unique email (used as dedup key)         |
| `company`          | **Always include — company name**        |
| `lifecyclestage`   | Must be `"salesqualifiedlead"`           |

### Recommended additional fields

| HubSpot property     | Example value               |
|----------------------|-----------------------------|
| `phone`              | `"(512) 555-0100"`          |
| `jobtitle`           | `"Office Manager"`          |
| `address`            | `"123 Main St"`             |
| `city`               | `"Austin"`                  |
| `state`              | `"TX"`                      |
| `zip`                | `"78701"`                   |
| `hs_lead_status`     | `"IN_PROGRESS"`             |
| `client_type`        | `"commercial"` / `"residential"` |
| `service_frequency`  | `"weekly"` / `"biweekly"`   |
| `neighborhood`       | Austin neighbourhood string |
| `lead_source_detail` | `"website_inquiry"` / `"referral"` etc. |

---

## Step 2 — Create a new one-off script

Copy the template below into a new file named `create_contact_<firstname>_<lastname>.py`
in the project root (e.g. `create_contact_nadia_chen.py`).

### Script template

```python
"""
One-off script: create a single new HubSpot contact for Sparkle & Shine,
marked as a Sales Qualified Lead, and register the canonical ID in
cross_tool_mapping.

Run:
    python3 create_contact_<firstname>_<lastname>.py
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
    "firstname":          "<FIRST>",
    "lastname":           "<LAST>",
    "email":              "<email@example.com>",
    "phone":              "<(512) 555-0100>",
    "company":            "<Company Name>",          # Always include
    "jobtitle":           "<Job Title>",
    "address":            "<Street Address>",
    "city":               "Austin",
    "state":              "TX",
    "zip":                "<ZIP>",
    "lifecyclestage":     "salesqualifiedlead",      # Always salesqualifiedlead
    "client_type":        "commercial",              # or "residential"
    "service_frequency":  "weekly",                  # weekly / biweekly / monthly
    "neighborhood":       "<Neighbourhood>",
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
```

---

## Step 3 — Run the script

```bash
cd sparkle-shine-poc
python3 create_contact_<firstname>_<lastname>.py
```

### Expected output

```
============================================================
  Sparkle & Shine — Create SQL Contact in HubSpot
============================================================

Contact to create:
  firstname                  Nadia
  ...
  lifecyclestage             salesqualifiedlead

  Assigned canonical ID : SS-LEAD-0236

Pushing to HubSpot...

[OK] Contact created successfully.
  HubSpot ID     : 461642978022
  Email          : nadia.chen@vertexcoworking.example.com
  Lifecycle stage: salesqualifiedlead

Registering mapping: SS-LEAD-0236 → hubspot:461642978022
[OK] cross_tool_mapping updated.

  View in HubSpot: https://app.hubspot.com/contacts/*/contact/461642978022
```

---

## Step 4 — Verify

1. **HubSpot UI** — open the printed URL and confirm:
   - Lifecycle stage = `Sales Qualified Lead`
   - Company name is populated
   - All custom properties are present

2. **Database** — confirm the mapping was written:
   ```bash
   python3 -c "
   import sqlite3
   db = sqlite3.connect('sparkle_shine.db')
   db.row_factory = sqlite3.Row
   rows = db.execute(
       \"SELECT * FROM cross_tool_mapping ORDER BY synced_at DESC LIMIT 5\"
   ).fetchall()
   for r in rows:
       print(dict(r))
   "
   ```

3. **Automation** — the `HubSpotQualifiedSync` automation polls HubSpot every ~5 minutes
   for `salesqualifiedlead` contacts. Because `cross_tool_mapping` now has the hubspot
   row, the automation will skip the creation step and move straight to creating the
   Pipedrive person and deal on its next run.

---

## Key rules

| Rule | Why |
|---|---|
| Always set `lifecyclestage = "salesqualifiedlead"` | Any other value is ignored by `HubSpotQualifiedSync` |
| Always include `company` | Pipedrive deal title uses the company name |
| Always call `register_in_db` immediately after the API call succeeds | Prevents `HubSpotQualifiedSync` from minting a duplicate canonical ID on the next run |
| Never reuse a script file for a second contact | Each contact gets its own file; the `CONTACT` dict is a one-off snapshot |
