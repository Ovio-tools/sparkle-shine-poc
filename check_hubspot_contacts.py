"""
Utility: fetch the first 200 HubSpot contacts (email + name) so we can
verify new contacts won't conflict with existing ones.

Run:
    python3 check_hubspot_contacts.py
"""

import os
import sys

_ROOT = os.path.dirname(os.path.abspath(__file__))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import requests
from credentials import get_credential

_BASE_URL = os.getenv("HUBSPOT_BASE_URL", "https://api.hubapi.com")


def _build_session() -> requests.Session:
    token = get_credential("HUBSPOT_ACCESS_TOKEN")
    s = requests.Session()
    s.headers.update({
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
    })
    return s


def fetch_all_contacts(session: requests.Session) -> list[dict]:
    """Page through all HubSpot contacts and return email + name for each."""
    contacts = []
    after = None
    props = "email,firstname,lastname,phone"

    while True:
        params = {"limit": 100, "properties": props}
        if after:
            params["after"] = after

        resp = session.get(f"{_BASE_URL}/crm/v3/objects/contacts", params=params, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(f"HubSpot GET /contacts returned {resp.status_code}: {resp.text[:300]}")

        data = resp.json()
        for c in data.get("results", []):
            p = c.get("properties", {})
            contacts.append({
                "id":        c["id"],
                "email":     (p.get("email") or "").lower(),
                "firstname": p.get("firstname", ""),
                "lastname":  p.get("lastname", ""),
                "phone":     p.get("phone", ""),
            })

        paging = data.get("paging", {})
        after = paging.get("next", {}).get("after")
        if not after:
            break

    return contacts


def main() -> None:
    print("=" * 60)
    print("  Sparkle & Shine — List Existing HubSpot Contacts")
    print("=" * 60)

    session = _build_session()
    contacts = fetch_all_contacts(session)

    print(f"\nTotal contacts found: {len(contacts)}\n")
    print(f"{'ID':<12} {'Email':<45} {'Name'}")
    print("-" * 80)
    for c in sorted(contacts, key=lambda x: x["email"]):
        name = f"{c['firstname']} {c['lastname']}".strip()
        print(f"{c['id']:<12} {c['email']:<45} {name}")


if __name__ == "__main__":
    main()
