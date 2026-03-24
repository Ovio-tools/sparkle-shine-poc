"""
Add a single new commercial prospect (person + deal) to Pipedrive.

Usage:
    python seeding/pushers/add_pipedrive_entry.py

Prints the contact and deal details, then pushes to the Pipedrive sandbox.
"""

from __future__ import annotations

import json
import os
import sys

import requests

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from credentials import get_credential                        # noqa: E402
from database.mappings import register_mapping, generate_id   # noqa: E402
from seeding.utils.throttler import PIPEDRIVE                 # noqa: E402

# ---------------------------------------------------------------------------
# Auth / HTTP setup
# ---------------------------------------------------------------------------

_API_TOKEN = get_credential("PIPEDRIVE_API_TOKEN")
_BASE_URL  = "https://api.pipedrive.com/v1"

with open(os.path.join(_PROJECT_ROOT, "config", "tool_ids.json")) as _f:
    _TOOL_IDS = json.load(_f)

_PD           = _TOOL_IDS["pipedrive"]
_PIPELINE_ID  = _PD["pipelines"]["Cleaning Services Sales"]  # 2
_STAGE_NEW_LEAD = _PD["stages"]["New Lead"]                  # 7
_DEAL_FIELDS  = _PD["deal_fields"]
_PERSON_FIELDS = _PD["person_fields"]

_session = requests.Session()
_session.headers.update({"Content-Type": "application/json"})


def _url(path: str) -> str:
    return f"{_BASE_URL}{path}?api_token={_API_TOKEN}"


def _post(path: str, payload: dict) -> dict:
    PIPEDRIVE.wait()
    PIPEDRIVE.track_call(path)
    resp = _session.post(_url(path), json=payload, timeout=30)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Pipedrive POST {path} -> {resp.status_code}: {resp.text[:400]}")
    body = resp.json()
    if not body.get("success"):
        raise RuntimeError(f"Pipedrive POST {path} success=false: {resp.text[:400]}")
    return body.get("data") or {}


# ---------------------------------------------------------------------------
# New entry definition
# ---------------------------------------------------------------------------

NEW_CONTACT = {
    "name":    "Derek Nguyen",
    "email":   "derek.nguyen@summitworkspaces.com",
    "phone":   "(512) 847-3291",
    "company": "Summit Workspaces ATX",
    "neighborhood":   "Round Rock",
    "acquisition_source": "direct_outreach",
}

NEW_DEAL = {
    "title":          "Summit Workspaces ATX — Commercial Nightly (Direct Outreach)",
    "pipeline_id":    _PIPELINE_ID,
    "stage_id":       _STAGE_NEW_LEAD,
    "currency":       "USD",
    "monthly_value":  4200,
    "client_type":    "commercial",
    "lead_source":    "direct_outreach",
    "service_scope":  "Nightly commercial cleaning, 5 nights/week. ~4,800 sq ft open office + 3 conference rooms.",
}


def _print_entry() -> None:
    monthly = NEW_DEAL["monthly_value"]
    print()
    print("=" * 60)
    print("  NEW PIPEDRIVE ENTRY — Sparkle & Shine Cleaning Co.")
    print("=" * 60)
    print()
    print("CONTACT")
    print(f"  Name:        {NEW_CONTACT['name']}")
    print(f"  Email:       {NEW_CONTACT['email']}")
    print(f"  Phone:       {NEW_CONTACT['phone']}")
    print(f"  Company:     {NEW_CONTACT['company']}")
    print(f"  Neighborhood:{NEW_CONTACT['neighborhood']}")
    print(f"  Source:      {NEW_CONTACT['acquisition_source']}")
    print()
    print("DEAL")
    print(f"  Title:       {NEW_DEAL['title']}")
    print(f"  Pipeline:    Cleaning Services Sales (ID {_PIPELINE_ID})")
    print(f"  Stage:       New Lead (ID {_STAGE_NEW_LEAD})")
    print(f"  Value:       ${monthly:,}/month  |  ${monthly * 12:,}/year")
    print(f"  Client Type: {NEW_DEAL['client_type']}")
    print(f"  Lead Source: {NEW_DEAL['lead_source']}")
    print(f"  Scope:       {NEW_DEAL['service_scope']}")
    print()


def push_entry() -> None:
    _print_entry()
    print("Pushing to Pipedrive...")
    print()

    # --- Person ---
    person_payload = {
        "name":  NEW_CONTACT["name"],
        "email": [{"value": NEW_CONTACT["email"], "primary": True}],
        "phone": [{"value": NEW_CONTACT["phone"], "primary": True}],
        _PERSON_FIELDS["Acquisition Source"]: NEW_CONTACT["acquisition_source"],
        _PERSON_FIELDS["Neighborhood"]:       NEW_CONTACT["neighborhood"],
    }
    person_data = _post("/persons", person_payload)
    person_id   = person_data["id"]
    print(f"  Person created   -> person_id={person_id}  ({NEW_CONTACT['name']})")

    # --- Deal ---
    monthly = NEW_DEAL["monthly_value"]
    deal_payload = {
        "title":       NEW_DEAL["title"],
        "pipeline_id": NEW_DEAL["pipeline_id"],
        "stage_id":    NEW_DEAL["stage_id"],
        "value":       monthly * 12,
        "currency":    NEW_DEAL["currency"],
        "person_id":   person_id,
        _DEAL_FIELDS["Client Type"]:             NEW_DEAL["client_type"],
        _DEAL_FIELDS["Estimated Monthly Value"]: monthly,
        _DEAL_FIELDS["Lead Source"]:             NEW_DEAL["lead_source"],
    }
    deal_data = _post("/deals", deal_payload)
    deal_id   = deal_data["id"]
    print(f"  Deal created     -> deal_id={deal_id}  ({NEW_DEAL['title'][:50]}...)")

    # --- Register in cross_tool_mapping ---
    db_path      = os.path.join(_PROJECT_ROOT, "sparkle_shine.db")
    canonical_id = generate_id("LEAD", db_path=db_path)
    register_mapping(canonical_id, "pipedrive_person", str(person_id), db_path=db_path)
    register_mapping(canonical_id, "pipedrive_deal",   str(deal_id),   db_path=db_path)
    print(f"  Registered in cross_tool_mapping  (canonical={canonical_id})")
    print()
    print("Done.")
    print()


if __name__ == "__main__":
    push_entry()
