"""
live_test_won_deal.py

Live test: push a new Pipedrive contact + deal, immediately mark it as won,
then confirm it's visible to the automation poller.

Usage:
    cd sparkle-shine-poc
    python seeding/pushers/live_test_won_deal.py
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

_PD              = _TOOL_IDS["pipedrive"]
_PIPELINE_ID     = _PD["pipelines"]["Cleaning Services Sales"]   # 2
_STAGE_WON       = _PD["stages"]["Closed Won"]                   # 12
_DEAL_FIELDS     = _PD["deal_fields"]
_PERSON_FIELDS   = _PD["person_fields"]

_session = requests.Session()
_session.headers.update({"Content-Type": "application/json"})


def _url(path: str) -> str:
    return f"{_BASE_URL}{path}?api_token={_API_TOKEN}"


def _post(path: str, payload: dict) -> dict:
    PIPEDRIVE.wait()
    PIPEDRIVE.track_call(path)
    resp = _session.post(_url(path), json=payload, timeout=30)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"POST {path} -> {resp.status_code}: {resp.text[:400]}")
    body = resp.json()
    if not body.get("success"):
        raise RuntimeError(f"POST {path} success=false: {resp.text[:400]}")
    return body.get("data") or {}


def _put(path: str, payload: dict) -> dict:
    PIPEDRIVE.wait()
    PIPEDRIVE.track_call(path)
    resp = _session.put(_url(path), json=payload, timeout=30)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"PUT {path} -> {resp.status_code}: {resp.text[:400]}")
    body = resp.json()
    if not body.get("success"):
        raise RuntimeError(f"PUT {path} success=false: {resp.text[:400]}")
    return body.get("data") or {}


# ---------------------------------------------------------------------------
# New entry definition
# ---------------------------------------------------------------------------

NEW_CONTACT = {
    "name":               "Sandra Kellerman",
    "email":              "sandra.kellerman@tricitiesproperty.com",
    "phone":              "(737) 218-5490",
    "company":            "Tri-Cities Property Management",
    "neighborhood":       "Cedar Park",
    "acquisition_source": "direct_outreach",
}

NEW_DEAL = {
    "title":         "Tri-Cities Property Management — Commercial Weekly (Direct Outreach)",
    "pipeline_id":   _PIPELINE_ID,
    "stage_id":      _STAGE_WON,
    "currency":      "USD",
    "monthly_value": 3600,
    "client_type":   "commercial",
    "lead_source":   "direct_outreach",
    "service_scope": "Weekly deep clean across 3 rental properties in Cedar Park. ~2,200 sq ft each.",
}


def _print_entry() -> None:
    monthly = NEW_DEAL["monthly_value"]
    print()
    print("=" * 60)
    print("  LIVE TEST — NEW PIPEDRIVE ENTRY (WON)")
    print("  Sparkle & Shine Cleaning Co.")
    print("=" * 60)
    print()
    print("CONTACT")
    print(f"  Name:         {NEW_CONTACT['name']}")
    print(f"  Email:        {NEW_CONTACT['email']}")
    print(f"  Phone:        {NEW_CONTACT['phone']}")
    print(f"  Company:      {NEW_CONTACT['company']}")
    print(f"  Neighborhood: {NEW_CONTACT['neighborhood']}")
    print(f"  Source:       {NEW_CONTACT['acquisition_source']}")
    print()
    print("DEAL")
    print(f"  Title:        {NEW_DEAL['title']}")
    print(f"  Pipeline:     Cleaning Services Sales (ID {_PIPELINE_ID})")
    print(f"  Stage:        Closed Won (ID {_STAGE_WON})")
    print(f"  Status:       won  (marked immediately on create)")
    print(f"  Value:        ${monthly:,}/month  |  ${monthly * 12:,}/year")
    print(f"  Client Type:  {NEW_DEAL['client_type']}")
    print(f"  Lead Source:  {NEW_DEAL['lead_source']}")
    print(f"  Scope:        {NEW_DEAL['service_scope']}")
    print()


def push_and_win() -> tuple[int, int, str]:
    """Push person + deal, immediately mark won. Returns (person_id, deal_id, canonical_id)."""
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
    print(f"  [1/4] Person created     -> person_id={person_id}  ({NEW_CONTACT['name']})")

    # --- Deal (created directly at Closed Won stage) ---
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
    print(f"  [2/4] Deal created       -> deal_id={deal_id}  (stage: Closed Won)")

    # --- Mark as won via PUT /deals/{id} ---
    won_data = _put(f"/deals/{deal_id}", {"status": "won"})
    won_status = won_data.get("status", "unknown")
    print(f"  [3/4] Deal marked won    -> status={won_status}")

    # --- Register in cross_tool_mapping ---
    db_path      = os.path.join(_PROJECT_ROOT, "sparkle_shine.db")
    canonical_id = generate_id("LEAD", db_path=db_path)
    register_mapping(canonical_id, "pipedrive_person", str(person_id), db_path=db_path)
    register_mapping(canonical_id, "pipedrive_deal",   str(deal_id),   db_path=db_path)
    print(f"  [4/4] Registered mapping -> canonical_id={canonical_id}")

    print()
    print("=" * 60)
    print(f"  Deal is now WON in Pipedrive.")
    print(f"  person_id={person_id}  deal_id={deal_id}  canonical={canonical_id}")
    print("=" * 60)
    print()
    print("Next step: run the automation poller to trigger NewClientOnboarding.")
    print("  cd sparkle-shine-poc")
    print("  python -m automations.runner --poll")
    print()

    return person_id, deal_id, canonical_id


if __name__ == "__main__":
    push_and_win()
