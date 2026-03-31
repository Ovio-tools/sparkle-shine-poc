"""
automations/create_sql_won_deal.py

CreateSQLAndWonDeal — creates a brand-new HubSpot contact as a Sales Qualified Lead,
syncs it to Pipedrive as person + deal, and immediately marks the deal as Won.

Steps:
  1. POST /crm/v3/objects/contacts  (HubSpot) — lifecyclestage=salesqualifiedlead
  2. POST /v1/persons               (Pipedrive) — linked to the HubSpot contact
  3. POST /v1/deals                 (Pipedrive) — at "Qualified" stage
  4. PUT  /v1/deals/{id}            (Pipedrive) — status=won
  5. Register cross_tool_mapping entries under a new SS-LEAD-NNNN canonical ID
  6. Post Slack summary to #sales

Usage (dry run, default):
    python automations/create_sql_won_deal.py

Usage (live with defaults):
    python automations/create_sql_won_deal.py --live

Usage (live with custom contact):
    python automations/create_sql_won_deal.py --live \\
        --firstname Alice --lastname Johnson \\
        --email alice@acmecorp.com --phone "(512) 555-0199" \\
        --company "Acme Corp" --client-type commercial \\
        --lead-source referral --neighborhood Domain \\
        --lifetime-value 72000

To call programmatically:
    from automations.create_sql_won_deal import CreateSQLAndWonDeal
    automation = CreateSQLAndWonDeal(clients=get_client, db=db)
    automation.run(contact={
        "firstname": "Alice", "lastname": "Johnson",
        "email": "alice@acmecorp.com", "phone": "(512) 555-0199",
        "company": "Acme Corp", "client_type": "commercial",
        "lead_source": "referral", "neighborhood": "Domain",
        "lifetime_value": "72000",
    })
"""
from __future__ import annotations

import json
import os
import sys
from typing import Optional

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from automations.base import BaseAutomation
from automations.utils.id_resolver import MappingNotFoundError, register_mapping

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

_PIPELINE_ID   = 2    # "Cleaning Services Sales"
_STAGE_ID      = 8    # "Qualified" — mirrors HubSpot salesqualifiedlead status
_SLACK_CHANNEL = "sales"
_TOOL_IDS_PATH = os.path.join(_PROJECT_ROOT, "config", "tool_ids.json")


def _load_field_ids() -> tuple:
    """Return (person_fields, deal_fields) dicts loaded from tool_ids.json."""
    with open(_TOOL_IDS_PATH) as f:
        ids = json.load(f)["pipedrive"]
    return ids["person_fields"], ids["deal_fields"]


# Load once at module level so _create_pipedrive_records can reference them
_person_fields, _deal_fields = _load_field_ids()


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _pipedrive_base(session) -> str:
    base = session.base_url.rstrip("/")
    if not any(seg in base for seg in ("/v1", "/v2")):
        base = f"{base}/v1"
    return base


def _next_lead_id_number(db) -> int:
    """Return the next available integer for a SS-LEAD-NNNN canonical ID."""
    row = db.execute("SELECT id FROM leads ORDER BY id DESC LIMIT 1").fetchone()
    leads_max = int(row["id"].split("-")[-1]) if row else 0

    row2 = db.execute(
        "SELECT canonical_id FROM cross_tool_mapping "
        "WHERE entity_type = 'LEAD' ORDER BY canonical_id DESC LIMIT 1"
    ).fetchone()
    mapping_max = int(row2["canonical_id"].split("-")[-1]) if row2 else 0

    return max(leads_max, mapping_max) + 1


# ─────────────────────────────────────────────────────────────────────────────
# Automation class
# ─────────────────────────────────────────────────────────────────────────────

class CreateSQLAndWonDeal(BaseAutomation):
    """
    One-shot automation: creates a new contact as a Sales Qualified Lead in
    HubSpot, syncs to Pipedrive as person + deal, and marks the deal as Won.

    Parameters
    ----------
    contact : dict with keys:
        firstname, lastname, email  (required)
        phone, company, client_type, lead_source, neighborhood, lifetime_value
    """

    def run(self, contact: dict) -> Optional[dict]:
        """
        Execute the full create-SQL → sync → mark-won flow.

        Returns a result dict with hubspot_id, person_id, deal_id on success,
        or None if a fatal step failed.
        """
        run_id         = self.generate_run_id()
        trigger_source = "manual:create_sql_won_deal"

        # ── Claim canonical ID atomically ─────────────────────────────────────
        # psycopg2's implicit transaction (autocommit=False) ensures the MAX
        # read and the ID claim run atomically without needing BEGIN IMMEDIATE.
        if not self.dry_run:
            try:
                next_n       = _next_lead_id_number(self.db)
                canonical_id = f"SS-LEAD-{next_n:04d}"
                self.db.commit()
            except Exception:
                self.db.rollback()
                raise
        else:
            next_n       = _next_lead_id_number(self.db)
            canonical_id = f"SS-LEAD-{next_n:04d}"

        # ── Step 1: Create HubSpot contact ────────────────────────────────────
        hubspot_id: Optional[str] = None
        try:
            hubspot_id = self._create_hubspot_contact(contact, canonical_id)
            self.log_action(
                run_id, "create_hubspot_contact",
                f"hubspot:contact:{hubspot_id}",
                "success",
                trigger_source=trigger_source,
                trigger_detail={
                    "canonical_id": canonical_id,
                    "email":        contact.get("email"),
                    "hubspot_id":   hubspot_id,
                },
            )
        except Exception as exc:
            self.log_action(
                run_id, "create_hubspot_contact", None, "failed",
                error_message=str(exc), trigger_source=trigger_source,
                trigger_detail={"canonical_id": canonical_id},
            )
            return None

        # ── Step 2 + 3: Create Pipedrive person + deal ────────────────────────
        person_id: Optional[str] = None
        deal_id:   Optional[str] = None
        try:
            person_id, deal_id = self._create_pipedrive_records(contact, canonical_id, hubspot_id)
            self.log_action(
                run_id, "create_pipedrive_records",
                f"pipedrive:deal:{deal_id}",
                "success",
                trigger_source=trigger_source,
                trigger_detail={
                    "canonical_id": canonical_id,
                    "person_id":    person_id,
                    "deal_id":      deal_id,
                },
            )
        except Exception as exc:
            self.log_action(
                run_id, "create_pipedrive_records", None, "failed",
                error_message=str(exc), trigger_source=trigger_source,
                trigger_detail={"canonical_id": canonical_id},
            )
            return None

        # ── Step 4: Mark Pipedrive deal as Won ────────────────────────────────
        try:
            self._mark_deal_won(deal_id)
            self.log_action(
                run_id, "mark_deal_won",
                f"pipedrive:deal:{deal_id}",
                "success",
                trigger_source=trigger_source,
                trigger_detail={"deal_id": deal_id, "status": "won"},
            )
        except Exception as exc:
            self.log_action(
                run_id, "mark_deal_won", None, "failed",
                error_message=str(exc), trigger_source=trigger_source,
                trigger_detail={"deal_id": deal_id},
            )
            return None

        # ── Step 5: Register mappings ─────────────────────────────────────────
        try:
            self._register_mappings(canonical_id, hubspot_id, person_id, deal_id)
            self.log_action(
                run_id, "register_mappings",
                f"{canonical_id}",
                "success",
                trigger_source=trigger_source,
                trigger_detail={
                    "canonical_id":     canonical_id,
                    "hubspot_id":       hubspot_id,
                    "pipedrive_person": person_id,
                    "pipedrive_deal":   deal_id,
                },
            )
        except Exception as exc:
            # Mappings failing is non-fatal for the business operation
            self.log_action(
                run_id, "register_mappings", None, "failed",
                error_message=str(exc), trigger_source=trigger_source,
            )

        # ── Step 6: Post Slack summary ────────────────────────────────────────
        result = {
            "canonical_id": canonical_id,
            "hubspot_id":   hubspot_id,
            "person_id":    person_id,
            "deal_id":      deal_id,
        }
        try:
            self._post_slack_summary(contact, result)
            self.log_action(
                run_id, "post_slack_summary",
                f"slack:channel:{_SLACK_CHANNEL}",
                "success",
                trigger_source=trigger_source,
            )
        except Exception as exc:
            self.log_action(
                run_id, "post_slack_summary", None, "failed",
                error_message=str(exc), trigger_source=trigger_source,
            )

        return result

    # ── Step 1 ────────────────────────────────────────────────────────────────

    def _create_hubspot_contact(self, contact: dict, canonical_id: str) -> str:
        """
        POST /crm/v3/objects/contacts to create a new contact with
        lifecyclestage=salesqualifiedlead.  Returns the HubSpot contact ID.
        """
        properties = {
            "lifecyclestage":    "salesqualifiedlead",
            "service_frequency": "one-time",
        }
        if contact.get("firstname"):
            properties["firstname"] = contact["firstname"]
        if contact.get("lastname"):
            properties["lastname"] = contact["lastname"]
        if contact.get("email"):
            properties["email"] = contact["email"]
        if contact.get("phone"):
            properties["phone"] = contact["phone"]
        if contact.get("lead_source"):
            properties["lead_source_detail"] = contact["lead_source"]
        if contact.get("neighborhood"):
            properties["neighborhood"] = contact["neighborhood"]

        if self.dry_run:
            name = f"{contact.get('firstname', '')} {contact.get('lastname', '')}".strip()
            print(
                f"[DRY RUN] Would POST /crm/v3/objects/contacts — "
                f"name='{name}', email='{contact.get('email')}', "
                f"lifecyclestage=salesqualifiedlead"
            )
            return "dry-hs-id"

        from hubspot.crm.contacts import SimplePublicObjectInputForCreate

        hs_client = self.clients("hubspot")
        response  = hs_client.crm.contacts.basic_api.create(
            SimplePublicObjectInputForCreate(properties=properties),
            _request_timeout=30,
        )
        return str(response.id)

    # ── Steps 2 + 3 ──────────────────────────────────────────────────────────

    def _create_pipedrive_records(
        self, contact: dict, canonical_id: str, hubspot_id: str
    ) -> tuple[str, str]:
        """
        Create a Pipedrive person then a deal linked to that person.
        Returns (person_id, deal_id) as strings.
        """
        firstname = contact.get("firstname") or ""
        lastname  = contact.get("lastname")  or ""
        full_name = f"{firstname} {lastname}".strip() or contact.get("email", canonical_id)
        source    = contact.get("lead_source") or "Unknown"
        deal_title = f"{full_name} — Qualified Lead ({source.replace('_', ' ').title()})"

        try:
            monthly_value = round(float(contact.get("lifetime_value") or 0) / 24, 2)
        except (ValueError, TypeError):
            monthly_value = 0.0
        annual_value = round(monthly_value * 12, 2)

        if self.dry_run:
            org_label = f", org='{contact['company']}'" if contact.get("company") else ""
            print(
                f"[DRY RUN] Would POST /v1/persons — name='{full_name}', "
                f"email='{contact.get('email')}', hs_id={hubspot_id}{org_label}"
            )
            print(
                f"[DRY RUN] Would POST /v1/deals   — title='{deal_title}', "
                f"stage_id={_STAGE_ID}, value={annual_value} USD"
            )
            return "dry-person-id", "dry-deal-id"

        session = self.clients("pipedrive")
        base    = _pipedrive_base(session)

        # Resolve or create org
        org_id: Optional[int] = None
        if contact.get("company"):
            org_id = self._get_or_create_org(session, base, contact["company"])

        # Create person
        person_payload: dict = {
            "name": full_name,
        }
        if contact.get("email"):
            person_payload["email"] = [{"value": contact["email"], "primary": True}]
        if contact.get("phone"):
            person_payload["phone"] = [{"value": contact["phone"], "primary": True}]
        person_payload[_person_fields["HubSpot Contact ID"]] = hubspot_id
        if contact.get("lead_source"):
            person_payload[_person_fields["Acquisition Source"]] = contact["lead_source"]
        if contact.get("neighborhood"):
            person_payload[_person_fields["Neighborhood"]] = contact["neighborhood"]
        if org_id is not None:
            person_payload["org_id"] = org_id

        pr = session.post(f"{base}/persons", json=person_payload, timeout=30)
        pr.raise_for_status()
        person_id = str(pr.json()["data"]["id"])

        # Create deal at Qualified stage
        deal_payload: dict = {
            "title":       deal_title,
            "pipeline_id": _PIPELINE_ID,
            "stage_id":    _STAGE_ID,
            "value":       annual_value,
            "currency":    "USD",
            "person_id":   int(person_id),
            _deal_fields["Client Type"]:             contact.get("client_type") or "residential",
            _deal_fields["Estimated Monthly Value"]: monthly_value,
        }
        if contact.get("lead_source"):
            deal_payload[_deal_fields["Lead Source"]] = contact["lead_source"]

        dr = session.post(f"{base}/deals", json=deal_payload, timeout=30)
        dr.raise_for_status()
        deal_id = str(dr.json()["data"]["id"])

        return person_id, deal_id

    # ── Step 4 ────────────────────────────────────────────────────────────────

    def _mark_deal_won(self, deal_id: str) -> None:
        """PUT /v1/deals/{id} with status=won to close the deal."""
        if self.dry_run:
            print(f"[DRY RUN] Would PUT /v1/deals/{deal_id} — status=won")
            return

        session = self.clients("pipedrive")
        base    = _pipedrive_base(session)
        resp    = session.put(f"{base}/deals/{deal_id}", json={"status": "won"}, timeout=30)
        resp.raise_for_status()

    def _get_or_create_org(self, session, base: str, company: str) -> int:
        """Return Pipedrive org_id for company name, creating the org if needed."""
        sr = session.get(
            f"{base}/organizations/search",
            params={"term": company, "limit": 10, "exact_match": True},
            timeout=15,
        )
        for item in (sr.json().get("data", {}).get("items") or []):
            org = item.get("item") or {}
            if org.get("name", "").lower() == company.lower():
                return int(org["id"])
        cr = session.post(f"{base}/organizations", json={"name": company}, timeout=15)
        cr.raise_for_status()
        return int(cr.json()["data"]["id"])

    # ── Step 5 ────────────────────────────────────────────────────────────────

    def _register_mappings(
        self,
        canonical_id: str,
        hubspot_id:   str,
        person_id:    str,
        deal_id:      str,
    ) -> None:
        """Register HubSpot + Pipedrive person/deal mappings for the new canonical ID."""
        if self.dry_run:
            print(
                f"[DRY RUN] Would register mappings: "
                f"{canonical_id} → hubspot:{hubspot_id}, "
                f"pipedrive_person:{person_id}, pipedrive:{deal_id}"
            )
            return
        register_mapping(self.db, canonical_id, "hubspot",          hubspot_id)
        register_mapping(self.db, canonical_id, "pipedrive_person", person_id)
        register_mapping(self.db, canonical_id, "pipedrive",        deal_id)

    # ── Step 6 ────────────────────────────────────────────────────────────────

    def _post_slack_summary(self, contact: dict, result: dict) -> None:
        """Post a Won deal notification to #sales."""
        name    = f"{contact.get('firstname', '')} {contact.get('lastname', '')}".strip()
        name    = name or contact.get("email") or result["canonical_id"]
        company = contact.get("company") or ""
        source  = (contact.get("lead_source") or "Unknown").replace("_", " ").title()

        label   = f"{name} ({company})" if company else name
        text    = (
            f":trophy: New Won Deal — {label} | "
            f"Source: {source} | "
            f"HubSpot SQL → Pipedrive deal #{result['deal_id']} marked Won\n"
            f"  Canonical ID: {result['canonical_id']}"
        )
        self.send_slack(_SLACK_CHANNEL, text)


# ─────────────────────────────────────────────────────────────────────────────
# Dry-run sample contact
# ─────────────────────────────────────────────────────────────────────────────

_SAMPLE_CONTACT = {
    "firstname":      "Alice",
    "lastname":       "Johnson",
    "email":          "alice.johnson@acmecorp.com",
    "phone":          "(512) 555-0199",
    "company":        "Acme Corp",
    "client_type":    "commercial",
    "lead_source":    "referral",
    "neighborhood":   "Domain",
    "lifetime_value": "72000",
}


# ─────────────────────────────────────────────────────────────────────────────
# CLI entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    from auth import get_client
    from database.schema import get_connection

    parser = argparse.ArgumentParser(
        description="Create a HubSpot SQL contact, sync to Pipedrive, and mark the deal Won"
    )
    parser.add_argument("--live",           action="store_true", help="Make real API calls")
    parser.add_argument("--firstname",      default=_SAMPLE_CONTACT["firstname"])
    parser.add_argument("--lastname",       default=_SAMPLE_CONTACT["lastname"])
    parser.add_argument("--email",          default=_SAMPLE_CONTACT["email"])
    parser.add_argument("--phone",          default=_SAMPLE_CONTACT["phone"])
    parser.add_argument("--company",        default=_SAMPLE_CONTACT["company"])
    parser.add_argument("--client-type",    default=_SAMPLE_CONTACT["client_type"],
                        dest="client_type")
    parser.add_argument("--lead-source",    default=_SAMPLE_CONTACT["lead_source"],
                        dest="lead_source")
    parser.add_argument("--neighborhood",   default=_SAMPLE_CONTACT["neighborhood"])
    parser.add_argument("--lifetime-value", default=_SAMPLE_CONTACT["lifetime_value"],
                        dest="lifetime_value")
    args = parser.parse_args()

    dry_run = not args.live
    contact = {
        "firstname":      args.firstname,
        "lastname":       args.lastname,
        "email":          args.email,
        "phone":          args.phone,
        "company":        args.company,
        "client_type":    args.client_type,
        "lead_source":    args.lead_source,
        "neighborhood":   args.neighborhood,
        "lifetime_value": args.lifetime_value,
    }

    print("=" * 65)
    print("  CreateSQLAndWonDeal")
    print("  MODE:", "DRY RUN" if dry_run else "LIVE")
    print("=" * 65)
    print(f"\nContact:")
    for k, v in contact.items():
        if v:
            print(f"  {k:<18}: {v}")
    print()

    db = get_connection(os.path.join(_PROJECT_ROOT, "sparkle_shine.db"))

    automation = CreateSQLAndWonDeal(
        clients=get_client,
        db=db,
        dry_run=dry_run,
    )

    result = automation.run(contact=contact)

    print()
    print("─" * 65)
    if result:
        print("Result:")
        for k, v in result.items():
            print(f"  {k:<18}: {v}")
    else:
        print("Automation failed — check automation_log for details.")

    print()
    print("─" * 65)
    print("automation_log entries:")
    print("─" * 65)
    rows = db.execute(
        """
        SELECT action_name, action_target, status, error_message
        FROM automation_log
        WHERE automation_name = 'CreateSQLAndWonDeal'
        ORDER BY id DESC
        LIMIT 10
        """
    ).fetchall()
    for row in reversed(rows):
        r = dict(row)
        marker = "OK " if r["status"] == "success" else "ERR"
        print(f"  [{marker}] {r['action_name']:<45} → {r['action_target'] or 'n/a'}")
        if r["error_message"]:
            print(f"         note: {r['error_message']}")

    print()
    print("Dry-run complete." if dry_run else "Live run complete.")
    db.close()
