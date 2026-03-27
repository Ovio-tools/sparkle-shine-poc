"""
One-off script: create a single new HubSpot contact for Sparkle & Shine.

Run: python create_hubspot_contact.py
"""

import os
import sys
import requests

_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from credentials import get_credential  # noqa: E402

_BASE_URL = os.getenv("HUBSPOT_BASE_URL", "https://api.hubapi.com")

# ── New contact details ──────────────────────────────────────────────────────
NEW_CONTACT = {
    "firstname":          "Laura",
    "lastname":           "Mendez",
    "email":              "laura.mendez@austinhome.example.com",
    "phone":              "512-555-0192",
    "lifecyclestage":     "lead",
    "client_type":        "residential",
    "service_frequency":  "biweekly",
    "neighborhood":       "South Austin/Zilker",
    "lead_source_detail": "referral",
}
# ─────────────────────────────────────────────────────────────────────────────


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


def main() -> None:
    print("Connecting to HubSpot...")
    session = _build_session()

    print(f"Creating contact: {NEW_CONTACT['firstname']} {NEW_CONTACT['lastname']} "
          f"<{NEW_CONTACT['email']}>")

    result = create_contact(session, NEW_CONTACT)
    hs_id = result.get("id")
    print(f"\nContact created successfully.")
    print(f"  HubSpot ID : {hs_id}")
    print(f"  Name       : {NEW_CONTACT['firstname']} {NEW_CONTACT['lastname']}")
    print(f"  Email      : {NEW_CONTACT['email']}")
    print(f"  Phone      : {NEW_CONTACT['phone']}")
    print(f"  Stage      : {NEW_CONTACT['lifecyclestage']}")
    print(f"  Neighborhood: {NEW_CONTACT['neighborhood']}")


if __name__ == "__main__":
    main()
