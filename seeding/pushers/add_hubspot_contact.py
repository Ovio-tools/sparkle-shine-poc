"""
Add a single new commercial prospect contact to HubSpot.

Usage:
    cd sparkle-shine-poc
    python seeding/pushers/add_hubspot_contact.py

Prints the contact details, then pushes to HubSpot.
"""

from __future__ import annotations

import os
import sys

import requests

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from credentials import get_credential                        # noqa: E402
from database.mappings import register_mapping, generate_id  # noqa: E402
from seeding.utils.throttler import HUBSPOT                  # noqa: E402

# ---------------------------------------------------------------------------
# Auth / HTTP setup
# ---------------------------------------------------------------------------

_BASE_URL = os.getenv("HUBSPOT_BASE_URL", "https://api.hubapi.com")
_session: requests.Session = None  # type: ignore[assignment]


def _build_session() -> requests.Session:
    token = get_credential("HUBSPOT_ACCESS_TOKEN")
    s = requests.Session()
    s.headers.update({
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    })
    return s


def _post(path: str, payload: dict) -> dict:
    url = f"{_BASE_URL}{path}"
    HUBSPOT.wait()
    resp = _session.post(url, json=payload, timeout=30)
    if resp.status_code not in (200, 201):
        raise RuntimeError(
            f"HubSpot POST {path} returned {resp.status_code}: {resp.text[:400]}"
        )
    return resp.json()


# ---------------------------------------------------------------------------
# New contact data
# ---------------------------------------------------------------------------

_CONTACT = {
    "firstname":          "Marcus",
    "lastname":           "Thornton",
    "email":              "marcus.thornton@apexfitnessstudios.com",
    "phone":              "(512) 603-7184",
    "company":            "Apex Fitness Studios",
    "lifecyclestage":     "salesqualifiedlead",
    "client_type":        "commercial",
    "service_frequency":  "one-time",
    "lead_source_detail": "direct_outreach",
    "neighborhood":       "Domain/North Austin",
}

_DB_PATH = os.path.join(_PROJECT_ROOT, "sparkle_shine.db")


# ---------------------------------------------------------------------------
# Push
# ---------------------------------------------------------------------------

def push() -> str:
    """Create the HubSpot contact and register in cross_tool_mapping. Returns hs_id."""
    print("=== NEW HUBSPOT CONTACT: Sparkle & Shine ===\n")
    print("CONTACT")
    print(f"  Name:       {_CONTACT['firstname']} {_CONTACT['lastname']}")
    print(f"  Email:      {_CONTACT['email']}")
    print(f"  Phone:      {_CONTACT['phone']}")
    print(f"  Company:    {_CONTACT['company']}")
    print(f"  Lifecycle:  {_CONTACT['lifecyclestage']}")
    print(f"  Type:       {_CONTACT['client_type']}")
    print(f"  Source:     {_CONTACT['lead_source_detail']}")
    print(f"  Territory:  {_CONTACT['neighborhood']}")
    print()

    print("Pushing to HubSpot...")
    resp = _post("/crm/v3/objects/contacts", {"properties": _CONTACT})
    hs_id = resp["id"]
    print(f"  Contact created → hs_id={hs_id}")

    canonical_id = generate_id("LEAD", db_path=_DB_PATH)
    register_mapping(canonical_id, "hubspot", hs_id, db_path=_DB_PATH)
    print(f"  Registered in cross_tool_mapping → {canonical_id}")

    print(f"\nDone. HubSpot contact ID: {hs_id}  |  Canonical: {canonical_id}")
    return hs_id


def main() -> None:
    global _session
    _session = _build_session()
    push()


if __name__ == "__main__":
    main()
