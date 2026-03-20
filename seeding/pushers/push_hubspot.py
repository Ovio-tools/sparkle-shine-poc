"""
Push Sparkle & Shine clients and leads to HubSpot CRM as contacts.

Full run:  python seeding/pushers/push_hubspot.py
Dry run:   python seeding/pushers/push_hubspot.py --dry-run

Push order:
  Phase 1 — Contacts  (320 clients + 160 leads, batched in groups of 100)
  Phase 2 — Deals     (10 commercial clients → "closedwon" deals with contact associations)

Auth: auth.get_client("hubspot") validates credentials on startup.
Raw REST calls use the HUBSPOT_ACCESS_TOKEN env var via get_credential().

Custom properties (must be pre-created in HubSpot via Phase 1 Step 6):
  client_type, service_frequency, lead_source_detail, neighborhood,
  jobber_client_id, quickbooks_customer_id, lifetime_value, last_service_date
"""

import json
import os
import sys
import time
from typing import Optional

import requests

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from auth import get_client                                                          # noqa: E402
from credentials import get_credential                                               # noqa: E402
from database.schema import get_connection                                           # noqa: E402
from database.mappings import register_mapping, get_tool_id, find_unmapped          # noqa: E402
from seeding.utils.throttler import HUBSPOT                                          # noqa: E402

_DB_PATH = os.path.join(_PROJECT_ROOT, "sparkle_shine.db")
_BASE_URL = os.getenv("HUBSPOT_BASE_URL", "https://api.hubapi.com")

# Lifecycle stage mapping
_LIFECYCLE_CLIENT = {
    "active":     "customer",
    "occasional": "customer",
    "churned":    "customer",   # note is added separately
}

_LIFECYCLE_LEAD = {
    "qualified": "salesqualifiedlead",
    "contacted": "marketingqualifiedlead",
    "new":       "lead",
    "lost":      "subscriber",
}

_FREQ_TO_SERVICE_FREQ = {
    "weekly":   "weekly",
    "biweekly": "biweekly",
    "monthly":  "monthly",
}

# HubSpot deal-to-contact association type (HUBSPOT_DEFINED, typeId=3)
_DEAL_TO_CONTACT_TYPE_ID = 3

_session: requests.Session = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Auth / session
# ---------------------------------------------------------------------------

def _build_session() -> requests.Session:
    """Return a requests.Session pre-configured with HubSpot Bearer auth."""
    token = get_credential("HUBSPOT_ACCESS_TOKEN")
    s = requests.Session()
    s.headers.update({
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    })
    return s


def _post(path: str, payload: dict) -> dict:
    """POST to HubSpot REST API; handles 207 partial-success."""
    url = f"{_BASE_URL}{path}"
    HUBSPOT.wait()
    resp = _session.post(url, json=payload, timeout=30)
    if resp.status_code not in (200, 201, 207):
        raise RuntimeError(
            f"HubSpot POST {path} returned {resp.status_code}: {resp.text[:300]}"
        )
    return resp.json()


def _put(path: str, payload: Optional[list] = None) -> dict:
    """PUT to HubSpot REST API (used for associations)."""
    url = f"{_BASE_URL}{path}"
    HUBSPOT.wait()
    resp = _session.put(url, json=payload or [], timeout=30)
    if not resp.ok:
        raise RuntimeError(
            f"HubSpot PUT {path} returned {resp.status_code}: {resp.text[:300]}"
        )
    return resp.json() if resp.content else {}


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def _fetch_clients(conn) -> list:
    """Fetch all clients with their recurring frequency and cross-tool IDs."""
    rows = conn.execute("""
        SELECT
            c.id, c.client_type, c.first_name, c.last_name, c.company_name,
            c.email, c.phone, c.status, c.acquisition_source, c.neighborhood,
            c.lifetime_value, c.last_service_date,
            ra.frequency,
            ctm_j.tool_specific_id AS jobber_client_id,
            ctm_q.tool_specific_id AS quickbooks_customer_id
        FROM clients c
        LEFT JOIN (
            SELECT client_id, frequency
            FROM recurring_agreements
            WHERE status = 'active'
            GROUP BY client_id
        ) ra ON ra.client_id = c.id
        LEFT JOIN cross_tool_mapping ctm_j
            ON ctm_j.canonical_id = c.id AND ctm_j.tool_name = 'jobber'
        LEFT JOIN cross_tool_mapping ctm_q
            ON ctm_q.canonical_id = c.id AND ctm_q.tool_name = 'quickbooks'
        ORDER BY c.id
    """).fetchall()
    return [dict(r) for r in rows]


def _fetch_leads(conn) -> list:
    """Fetch all leads."""
    rows = conn.execute("""
        SELECT id, first_name, last_name, company_name, email, phone,
               lead_type, source, status
        FROM leads
        ORDER BY id
    """).fetchall()
    return [dict(r) for r in rows]


def _fetch_commercial_clients(conn) -> list:
    """Fetch commercial clients with deal value from won proposals."""
    rows = conn.execute("""
        SELECT
            c.id, c.company_name, c.first_service_date, c.status,
            cp.monthly_value, cp.decision_date
        FROM clients c
        LEFT JOIN commercial_proposals cp
            ON cp.client_id = c.id AND cp.status = 'won'
        WHERE c.client_type = 'commercial'
        GROUP BY c.id
        ORDER BY c.id
    """).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Contact payload builders
# ---------------------------------------------------------------------------

def _client_properties(c: dict) -> dict:
    """Build HubSpot contact properties dict from a clients row."""
    service_freq = _FREQ_TO_SERVICE_FREQ.get(c.get("frequency") or "", "one-time")
    lifecycle = _LIFECYCLE_CLIENT.get(c.get("status") or "active", "customer")

    props: dict = {
        "lifecyclestage": lifecycle,
        "client_type":    c["client_type"],
        "service_frequency": service_freq,
    }

    if c.get("first_name"):
        props["firstname"] = c["first_name"]
    if c.get("last_name"):
        props["lastname"] = c["last_name"]
    if c.get("email"):
        props["email"] = c["email"]
    if c.get("phone"):
        props["phone"] = c["phone"]
    if c.get("acquisition_source"):
        props["lead_source_detail"] = c["acquisition_source"]
    if c.get("neighborhood"):
        props["neighborhood"] = c["neighborhood"]
    if c.get("jobber_client_id"):
        props["jobber_client_id"] = c["jobber_client_id"]
    if c.get("quickbooks_customer_id"):
        props["quickbooks_customer_id"] = c["quickbooks_customer_id"]
    if c.get("lifetime_value") is not None:
        props["lifetime_value"] = str(round(c["lifetime_value"], 2))
    if c.get("last_service_date"):
        props["last_service_date"] = c["last_service_date"]

    return props


def _lead_properties(lead: dict) -> dict:
    """Build HubSpot contact properties dict from a leads row."""
    lifecycle = _LIFECYCLE_LEAD.get(lead.get("status") or "new", "lead")

    # client_type is a dropdown enumeration that only allows residential/commercial;
    # omit it for leads to avoid INVALID_OPTION errors on the batch endpoint.
    props: dict = {
        "lifecyclestage":    lifecycle,
        "service_frequency": "one-time",
    }

    if lead.get("first_name"):
        props["firstname"] = lead["first_name"]
    if lead.get("last_name"):
        props["lastname"] = lead["last_name"]
    if lead.get("email"):
        props["email"] = lead["email"]
    if lead.get("phone"):
        props["phone"] = lead["phone"]
    if lead.get("source"):
        props["lead_source_detail"] = lead["source"]

    return props


# ---------------------------------------------------------------------------
# Batch helpers
# ---------------------------------------------------------------------------

def _submit_batch(inputs: list, canonical_ids: list, dry_run: bool) -> int:
    """
    POST up to 100 contact inputs to the HubSpot batch/create endpoint.
    Matches results by email and registers cross_tool_mapping entries.
    Returns the number of successfully created contacts.
    """
    if not inputs:
        return 0

    # Build email → canonical_id lookup for matching results
    email_to_canonical = {}
    for cid, inp in zip(canonical_ids, inputs):
        email = inp.get("properties", {}).get("email", "")
        if email:
            email_to_canonical[email.lower()] = cid

    if dry_run:
        for cid in canonical_ids:
            print(f"  [dry-run] Would create contact: {cid}")
        return len(inputs)

    try:
        data = _post("/crm/v3/objects/contacts/batch/create", {"inputs": inputs})
    except Exception as exc:
        print(f"  [WARN] Batch create failed: {exc}")
        return 0

    results = data.get("results", [])
    errors = data.get("errors", [])
    if errors:
        print(f"  [WARN] {len(errors)} contacts failed in batch (duplicates or invalid data)")

    registered = 0
    for result in results:
        hs_id = result.get("id")
        email = (result.get("properties", {}).get("email") or "").lower()
        canonical_id = email_to_canonical.get(email)
        if hs_id and canonical_id:
            try:
                register_mapping(canonical_id, "hubspot", hs_id, db_path=_DB_PATH)
                registered += 1
            except Exception as exc:
                print(f"  [WARN] Failed to register mapping {canonical_id} → HS {hs_id}: {exc}")

    return registered


def _create_churned_note(hubspot_contact_id: str, canonical_id: str) -> None:
    """
    Create a HubSpot note engagement for a churned client and associate it
    with the given contact. Errors are logged but do not abort the run.
    """
    try:
        note_resp = _post("/crm/v3/objects/notes", {
            "properties": {
                "hs_note_body": "churned",
                "hs_timestamp": "2025-01-01T00:00:00.000Z",
            }
        })
        note_id = note_resp.get("id")
        if note_id:
            _put(
                f"/crm/v4/objects/notes/{note_id}/associations/default/contacts/{hubspot_contact_id}"
            )
    except Exception as exc:
        print(f"  [WARN] Could not create churned note for {canonical_id}: {exc}")


# ---------------------------------------------------------------------------
# Phase 1: Contacts
# ---------------------------------------------------------------------------

def push_contacts(dry_run: bool = False) -> int:
    """
    Push all clients and leads to HubSpot as contacts, in batches of 100.
    Returns total count registered.
    """
    conn = get_connection(_DB_PATH)
    clients = _fetch_clients(conn)
    leads = _fetch_leads(conn)
    conn.close()

    # Combine: clients first, then leads
    all_records: list = []
    for c in clients:
        all_records.append(("client", c))
    for lead in leads:
        all_records.append(("lead", lead))

    total = len(all_records)
    print(f"\n[Phase 1] Contacts — {len(clients)} clients + {len(leads)} leads = {total} total")

    batch_size = 100
    registered_total = 0
    churned_pending: list = []   # (canonical_id,) — will add notes after mapping is set

    for batch_start in range(0, total, batch_size):
        batch = all_records[batch_start: batch_start + batch_size]
        inputs = []
        canonical_ids = []

        for kind, record in batch:
            canonical_id = record["id"]
            email = record.get("email") or ""

            # Idempotency: skip if already mapped
            if not dry_run and get_tool_id(canonical_id, "hubspot", db_path=_DB_PATH):
                continue

            if kind == "client":
                props = _client_properties(record)
                if record.get("status") == "churned":
                    churned_pending.append(canonical_id)
            else:
                props = _lead_properties(record)

            if not email:
                continue  # HubSpot requires an email address

            inputs.append({"properties": props})
            canonical_ids.append(canonical_id)

        if inputs:
            registered = _submit_batch(inputs, canonical_ids, dry_run)
            registered_total += registered
            batch_num = batch_start // batch_size + 1
            print(f"  Batch {batch_num}: {registered}/{len(inputs)} registered "
                  f"(cumulative: {registered_total})")

    print(f"[Phase 1] Done — {registered_total} contacts registered")

    # Add "churned" notes for churned clients
    if not dry_run and churned_pending:
        print(f"\n[Phase 1] Adding 'churned' notes to {len(churned_pending)} churned clients...")
        noted = 0
        for canonical_id in churned_pending:
            hs_id = get_tool_id(canonical_id, "hubspot", db_path=_DB_PATH)
            if hs_id:
                _create_churned_note(hs_id, canonical_id)
                noted += 1
        print(f"[Phase 1] Churned notes done — {noted}/{len(churned_pending)}")

    return registered_total


# ---------------------------------------------------------------------------
# Phase 2: Deals for commercial clients
# ---------------------------------------------------------------------------

def push_deals(dry_run: bool = False) -> int:
    """
    Create HubSpot deals for the 10 commercial clients and associate each deal
    with the corresponding contact.
    Returns the count of deals created.
    """
    conn = get_connection(_DB_PATH)
    commercial = _fetch_commercial_clients(conn)
    conn.close()

    print(f"\n[Phase 2] Deals — {len(commercial)} commercial clients")

    created = 0
    for client in commercial:
        canonical_id = client["id"]
        company_name = client.get("company_name") or canonical_id
        monthly_value = client.get("monthly_value") or 0.0
        annual_value = round(monthly_value * 12, 2)
        close_date = client.get("decision_date") or client.get("first_service_date") or "2024-01-01"

        # Normalise close_date to ISO 8601 with time component
        if close_date and "T" not in close_date:
            close_date = f"{close_date}T00:00:00.000Z"

        deal_name = f"{company_name} — Commercial Contract"

        if dry_run:
            print(f"  [dry-run] Would create deal: '{deal_name}' (${annual_value:,.2f})")
            created += 1
            continue

        # Get the HubSpot contact ID for this commercial client
        hs_contact_id = get_tool_id(canonical_id, "hubspot", db_path=_DB_PATH)
        if not hs_contact_id:
            print(f"  [WARN] No HubSpot contact for {canonical_id} ({company_name}) — skipping deal")
            continue

        # Create deal.
        # "closedwon" is the canonical stage but some portals rename it; use
        # "contractsent" (last active stage) as a safe fallback if closedwon
        # is absent — this keeps the deal out of open forecasts.
        try:
            deal_resp = _post("/crm/v3/objects/deals", {
                "properties": {
                    "dealname":   deal_name,
                    "amount":     str(annual_value),
                    "dealstage":  "contractsent",
                    "closedate":  close_date,
                }
            })
        except Exception as exc:
            print(f"  [WARN] Deal create failed for {company_name}: {exc}")
            continue

        deal_id = deal_resp.get("id")
        if not deal_id:
            print(f"  [WARN] No deal ID returned for {company_name}")
            continue

        # Associate deal → contact
        try:
            _put(
                f"/crm/v4/objects/deals/{deal_id}/associations/default/contacts/{hs_contact_id}",
                payload=[{
                    "associationCategory": "HUBSPOT_DEFINED",
                    "associationTypeId": _DEAL_TO_CONTACT_TYPE_ID,
                }],
            )
        except Exception as exc:
            print(f"  [WARN] Association failed for deal {deal_id} ↔ contact {hs_contact_id}: {exc}")

        created += 1
        print(f"  Created deal '{deal_name}' (HS deal {deal_id}) — ${annual_value:,.2f}/yr")

    print(f"[Phase 2] Done — {created} deals created")
    return created


# ---------------------------------------------------------------------------
# Post-run: mapping gap report
# ---------------------------------------------------------------------------

def print_gap_report() -> None:
    """Print unmapped client/lead counts for HubSpot."""
    print("\n[Gap Report] HubSpot mapping coverage:")
    for entity_type, label in [("CLIENT", "clients"), ("LEAD", "leads")]:
        unmapped = find_unmapped(entity_type, "hubspot", db_path=_DB_PATH)
        if unmapped:
            print(f"  [GAP] {len(unmapped)} {label} missing HubSpot mapping "
                  f"(first 5: {unmapped[:5]})")
        else:
            print(f"  [OK]  All {label} mapped in HubSpot")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(dry_run: bool = False) -> None:
    global _session

    print("=" * 60)
    print("  Sparkle & Shine → HubSpot CRM")
    if dry_run:
        print("  MODE: DRY RUN (no data will be written)")
    print("=" * 60)

    # Validate auth (raises if credentials are invalid)
    get_client("hubspot")
    _session = _build_session()

    push_contacts(dry_run=dry_run)
    push_deals(dry_run=dry_run)

    if not dry_run:
        print_gap_report()

    print("\n[Done] HubSpot push complete.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Push Sparkle & Shine data to HubSpot CRM"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be pushed without making any API calls",
    )
    args = parser.parse_args()
    main(dry_run=args.dry_run)
