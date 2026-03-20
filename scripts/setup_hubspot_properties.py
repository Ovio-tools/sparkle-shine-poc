"""
scripts/setup_hubspot_properties.py

Creates the HubSpot custom contact properties required by the Sparkle & Shine
automations.  Safe to re-run — properties that already exist are skipped.

Usage:
    python3 scripts/setup_hubspot_properties.py
"""
import os
import sys

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(_PROJECT_ROOT, ".env"))

from auth import get_client

# Custom contact properties needed by automations
_PROPERTIES = [
    # JobCompletionFlow
    {
        "name":        "last_service_date",
        "label":       "Last Service Date",
        "type":        "date",
        "fieldType":   "date",
        "groupName":   "contactinformation",
        "description": "Date of the most recent cleaning service completed for this client.",
    },
    {
        "name":        "total_services_completed",
        "label":       "Total Services Completed",
        "type":        "number",
        "fieldType":   "number",
        "groupName":   "contactinformation",
        "description": "Running count of completed cleaning jobs for this client.",
    },
    # PaymentReceived
    {
        "name":        "last_payment_date",
        "label":       "Last Payment Date",
        "type":        "date",
        "fieldType":   "date",
        "groupName":   "contactinformation",
        "description": "Date of the most recent payment received from this client.",
    },
    {
        "name":        "total_payments_received",
        "label":       "Total Payments Received",
        "type":        "number",
        "fieldType":   "number",
        "groupName":   "contactinformation",
        "description": "Running count of payments received from this client.",
    },
    {
        "name":        "outstanding_balance",
        "label":       "Outstanding Balance",
        "type":        "number",
        "fieldType":   "number",
        "groupName":   "contactinformation",
        "description": "Current outstanding invoice balance for this client (USD).",
    },
    # NegativeReviewResponse
    {
        "name":        "at_risk",
        "label":       "At Risk",
        "type":        "enumeration",
        "fieldType":   "booleancheckbox",
        "groupName":   "contactinformation",
        "description": "Flag set to true when the client has submitted a negative review (≤2 stars).",
        "options": [
            {"label": "true",  "value": "true",  "displayOrder": 1, "hidden": False},
            {"label": "false", "value": "false", "displayOrder": 2, "hidden": False},
        ],
    },
]


def main() -> None:
    hs = get_client("hubspot")
    props_api = hs.crm.properties

    # Fetch existing custom contact property names
    try:
        existing_resp = props_api.core_api.get_all("contacts")
        existing_names = {p.name for p in existing_resp.results}
    except Exception as exc:
        print(f"[ERROR] Could not list HubSpot contact properties: {exc}")
        sys.exit(1)

    created = 0
    skipped = 0

    for prop in _PROPERTIES:
        name = prop["name"]
        if name in existing_names:
            print(f"  SKIP   {name}  (already exists)")
            skipped += 1
            continue

        try:
            from hubspot.crm.properties import ModelProperty
            mp_kwargs: dict = {
                "name":        name,
                "label":       prop["label"],
                "type":        prop["type"],
                "field_type":  prop["fieldType"],
                "group_name":  prop["groupName"],
                "description": prop.get("description", ""),
            }
            if "options" in prop:
                mp_kwargs["options"] = prop["options"]
            props_api.core_api.create("contacts", ModelProperty(**mp_kwargs))
            print(f"  CREATE {name}")
            created += 1
        except Exception as exc:
            print(f"  ERROR  {name}: {exc}")

    print()
    print(f"Done: {created} created, {skipped} already existed.")


if __name__ == "__main__":
    main()
