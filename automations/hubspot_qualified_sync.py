"""
automations/hubspot_qualified_sync.py

Automation — HubSpot Qualified Lead → Pipedrive Sync (scheduled, daily)
No trigger event. Pulls HubSpot contacts with lifecyclestage='salesqualifiedlead'
and syncs any that don't yet have a Pipedrive deal into Pipedrive as person + deal
at the "Qualified" stage.

Steps:
  1. Fetch HubSpot contacts with lifecyclestage=salesqualifiedlead (last 90 days)
  2. Filter out contacts already mapped to a Pipedrive deal in cross_tool_mapping
  3. For each new contact: create Pipedrive person + deal, register mappings
  4. Post a Slack summary to #sales-pipeline
"""
import os
import sys
from datetime import datetime, timedelta, timezone

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
_SLACK_CHANNEL = "sales-pipeline"
_LOOKBACK_DAYS = 90

# Pipedrive person custom field hashes (from config/tool_ids.json)
_FIELD_HS_CONTACT_ID = "a70495529a73cf3473d1a10528cf7052e56d217e"
_FIELD_ACQ_SOURCE    = "d021197a6120bd6de2d5dc329ce66e06b300d311"
_FIELD_NEIGHBORHOOD  = "c522a9fe547842f66319659855399dd086763f9d"

# Pipedrive deal custom field hashes (from config/tool_ids.json)
_FIELD_CLIENT_TYPE   = "0c33b3b00286f14e71a0e0845a2180d6b524dd39"
_FIELD_MONTHLY_VALUE = "f25efe3a76061b039c0aeb9482e22ea8a276e6e2"
_FIELD_LEAD_SOURCE   = "a44f485b9f59b407da74b048ed7e09c67852c447"


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _pipedrive_base(session) -> str:
    base = session.base_url.rstrip("/")
    if not any(seg in base for seg in ("/v1", "/v2")):
        base = f"{base}/v1"
    return base


def _next_lead_id_number(db) -> int:
    """
    Return the next available integer for a SS-LEAD-NNNN canonical ID.
    Checks both the leads table and any existing LEAD entries in
    cross_tool_mapping to avoid collisions across both sources.
    """
    row = db.execute("SELECT id FROM leads ORDER BY id DESC LIMIT 1").fetchone()
    leads_max = int(row["id"].split("-")[-1]) if row else 0

    row2 = db.execute(
        "SELECT canonical_id FROM cross_tool_mapping "
        "WHERE entity_type = 'LEAD' ORDER BY canonical_id DESC LIMIT 1"
    ).fetchone()
    mapping_max = int(row2["canonical_id"].split("-")[-1]) if row2 else 0

    return max(leads_max, mapping_max) + 1


# ─────────────────────────────────────────────────────────────────────────────
# Main class
# ─────────────────────────────────────────────────────────────────────────────

class HubSpotQualifiedSync(BaseAutomation):
    """
    Scheduled daily automation: syncs HubSpot salesqualifiedlead contacts
    to Pipedrive as person + deal at the "Qualified" stage.
    """

    def run(self) -> None:
        run_id         = self.generate_run_id()
        trigger_source = "scheduled:hubspot_qualified_sync"

        # ── Step 1: Fetch HubSpot qualified contacts ──────────────────────────
        contacts = []
        try:
            contacts = self._fetch_qualified_contacts()
            self.log_action(
                run_id, "fetch_hubspot_contacts",
                f"hubspot:contacts:{len(contacts)}",
                "success",
                trigger_source=trigger_source,
                trigger_detail={"count": len(contacts)},
            )
        except Exception as exc:
            self.log_action(
                run_id, "fetch_hubspot_contacts", None, "failed",
                error_message=str(exc), trigger_source=trigger_source,
            )
            return

        # ── Step 2: Filter already-synced contacts ────────────────────────────
        new_contacts = self._filter_new_contacts(contacts)
        skipped = len(contacts) - len(new_contacts)

        self.log_action(
            run_id, "filter_synced_contacts",
            f"total:{len(contacts)},new:{len(new_contacts)},skipped:{skipped}",
            "success",
            trigger_source=trigger_source,
            trigger_detail={
                "total":   len(contacts),
                "new":     len(new_contacts),
                "skipped": skipped,
            },
        )

        # ── Step 3: Sync each new contact to Pipedrive ────────────────────────
        synced = []
        failed = []

        next_id_n = _next_lead_id_number(self.db)

        for i, contact in enumerate(new_contacts):
            canonical_id = f"SS-LEAD-{next_id_n + i:04d}"
            try:
                person_id, deal_id = self._create_pipedrive_records(contact, canonical_id)
                self._register_mappings(canonical_id, contact["hubspot_id"], person_id, deal_id)
                synced.append({
                    **contact,
                    "canonical_id": canonical_id,
                    "person_id":    person_id,
                    "deal_id":      deal_id,
                })
                self.log_action(
                    run_id, "sync_contact_to_pipedrive",
                    f"pipedrive:deal:{deal_id}",
                    "success",
                    trigger_source=trigger_source,
                    trigger_detail={
                        "canonical_id": canonical_id,
                        "hubspot_id":   contact["hubspot_id"],
                        "person_id":    person_id,
                        "deal_id":      deal_id,
                    },
                )
            except Exception as exc:
                failed.append(contact)
                self.log_action(
                    run_id, "sync_contact_to_pipedrive", None, "failed",
                    error_message=str(exc),
                    trigger_source=trigger_source,
                    trigger_detail={
                        "canonical_id": canonical_id,
                        "hubspot_id":   contact["hubspot_id"],
                    },
                )

        # ── Step 4: Post Slack summary ────────────────────────────────────────
        try:
            self._post_slack_summary(synced, skipped, failed)
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

    # ── Step 1 ────────────────────────────────────────────────────────────────

    def _fetch_qualified_contacts(self) -> list:
        """
        POST /crm/v3/objects/contacts/search
        Filter: lifecyclestage = salesqualifiedlead AND createdate GT (now - 90d).
        The GT filter returns only contacts created within the last 90 days;
        older historical contacts (pre-automation) are excluded.
        """
        from hubspot.crm.contacts import PublicObjectSearchRequest

        cutoff_ms = int(
            (datetime.now(timezone.utc) - timedelta(days=_LOOKBACK_DAYS))
            .timestamp() * 1000
        )

        search_request = PublicObjectSearchRequest(
            filter_groups=[
                {
                    "filters": [
                        {
                            "propertyName": "lifecyclestage",
                            "operator":     "EQ",
                            "value":        "salesqualifiedlead",
                        },
                        {
                            "propertyName": "createdate",
                            "operator":     "GT",
                            "value":        str(cutoff_ms),
                        },
                    ]
                }
            ],
            properties=[
                "email", "firstname", "lastname", "phone",
                "client_type", "lead_source_detail", "neighborhood",
                "lifetime_value", "company",
            ],
            limit=200,
        )

        if self.dry_run:
            print(
                f"[DRY RUN] Would POST /crm/v3/objects/contacts/search "
                f"(lifecyclestage=salesqualifiedlead, createdate > {_LOOKBACK_DAYS}d ago)"
            )
            return _DRY_RUN_CONTACTS

        hs_client = self.clients("hubspot")
        response  = hs_client.crm.contacts.search_api.do_search(search_request)
        results   = response.results or []

        contacts = []
        for contact in results:
            props = contact.properties or {}
            contacts.append({
                "hubspot_id":     str(contact.id),
                "email":          props.get("email")             or "",
                "firstname":      props.get("firstname")         or "",
                "lastname":       props.get("lastname")          or "",
                "phone":          props.get("phone")             or "",
                "company":        props.get("company")           or "",
                "client_type":    props.get("client_type")       or "residential",
                "lead_source":    props.get("lead_source_detail") or "Unknown",
                "neighborhood":   props.get("neighborhood")      or "",
                "lifetime_value": props.get("lifetime_value")    or "0",
            })
        return contacts

    # ── Step 2 ────────────────────────────────────────────────────────────────

    def _filter_new_contacts(self, contacts: list) -> list:
        """
        Return only contacts that have no Pipedrive record in cross_tool_mapping.
        Checks for any tool_name starting with 'pipedrive' to handle both the
        seeding convention (pipedrive_person / pipedrive_deal) and the automation
        convention (pipedrive).
        """
        new_contacts = []
        for contact in contacts:
            # Find all canonical IDs mapped to this HubSpot contact
            rows = self.db.execute(
                "SELECT DISTINCT canonical_id FROM cross_tool_mapping "
                "WHERE tool_specific_id = ? AND tool_name = 'hubspot'",
                (contact["hubspot_id"],),
            ).fetchall()
            canonical_ids = [r[0] if not hasattr(r, "keys") else r["canonical_id"] for r in rows]

            has_pipedrive = False
            for cid in canonical_ids:
                row = self.db.execute(
                    "SELECT 1 FROM cross_tool_mapping "
                    "WHERE canonical_id = ? AND tool_name LIKE 'pipedrive%' LIMIT 1",
                    (cid,),
                ).fetchone()
                if row:
                    has_pipedrive = True
                    break

            if not has_pipedrive:
                new_contacts.append(contact)
        return new_contacts

    # ── Step 3 ────────────────────────────────────────────────────────────────

    def _create_pipedrive_records(
        self, contact: dict, canonical_id: str
    ) -> tuple[str, str]:
        """
        Create a Pipedrive person then a deal linked to that person.
        Returns (person_id, deal_id) as strings.
        """
        firstname = contact["firstname"]
        lastname  = contact["lastname"]
        full_name = f"{firstname} {lastname}".strip() or contact["email"]

        # Derive a monthly estimate from lifetime_value (lifetime / 24 months)
        try:
            monthly_value = round(float(contact["lifetime_value"]) / 24, 2)
        except (ValueError, TypeError):
            monthly_value = 0.0
        annual_value = round(monthly_value * 12, 2)

        source_label = contact["lead_source"].replace("_", " ").title()
        deal_title   = f"{full_name} — Qualified Lead ({source_label})"

        if self.dry_run:
            org_label = f", org='{contact['company']}'" if contact.get("company") else ""
            print(
                f"[DRY RUN] Would POST /v1/persons  — name='{full_name}', "
                f"email='{contact['email']}', hs_id={contact['hubspot_id']}{org_label}"
            )
            print(
                f"[DRY RUN] Would POST /v1/deals    — title='{deal_title}', "
                f"stage_id={_STAGE_ID}, value={annual_value} USD"
            )
            return "dry-person-id", "dry-deal-id"

        session = self.clients("pipedrive")
        base    = _pipedrive_base(session)

        # Create person
        person_payload = {
            "name":  full_name,
            "email": [{"value": contact["email"], "primary": True}],
            _FIELD_HS_CONTACT_ID: contact["hubspot_id"],
            _FIELD_ACQ_SOURCE:    contact["lead_source"],
            _FIELD_NEIGHBORHOOD:  contact["neighborhood"],
        }
        if contact["phone"]:
            person_payload["phone"] = [{"value": contact["phone"], "primary": True}]
        if contact.get("company"):
            person_payload["org_name"] = contact["company"]

        pr = session.post(f"{base}/persons", json=person_payload, timeout=30)
        pr.raise_for_status()
        person_id = str(pr.json()["data"]["id"])

        # Create deal
        deal_payload = {
            "title":       deal_title,
            "pipeline_id": _PIPELINE_ID,
            "stage_id":    _STAGE_ID,
            "value":       annual_value,
            "currency":    "USD",
            "person_id":   int(person_id),
            _FIELD_CLIENT_TYPE:   contact["client_type"],
            _FIELD_MONTHLY_VALUE: monthly_value,
            _FIELD_LEAD_SOURCE:   contact["lead_source"],
        }
        dr = session.post(f"{base}/deals", json=deal_payload, timeout=30)
        dr.raise_for_status()
        deal_id = str(dr.json()["data"]["id"])

        return person_id, deal_id

    def _register_mappings(
        self,
        canonical_id: str,
        hubspot_id:   str,
        person_id:    str,
        deal_id:      str,
    ) -> None:
        """Register HubSpot and Pipedrive (deal) mappings for the new canonical ID."""
        if self.dry_run:
            print(
                f"[DRY RUN] Would register mappings: "
                f"{canonical_id} → hubspot:{hubspot_id}, pipedrive:{deal_id}"
            )
            return
        register_mapping(self.db, canonical_id, "hubspot",   hubspot_id)
        register_mapping(self.db, canonical_id, "pipedrive", deal_id)

    # ── Step 4 ────────────────────────────────────────────────────────────────

    def _post_slack_summary(
        self, synced: list, skipped: int, failed: list
    ) -> None:
        """Post a sync report to #sales-pipeline."""
        if synced:
            lines = [
                f":rocket: HubSpot → Pipedrive Sync: "
                f"{len(synced)} qualified lead(s) pushed to Pipedrive\n"
            ]
            for s in synced:
                name = f"{s['firstname']} {s['lastname']}".strip() or s["email"]
                lines.append(f"  • {name} ({s['lead_source']}) → deal #{s['deal_id']}")
            if skipped:
                lines.append(
                    f"\n{skipped} contact(s) already had Pipedrive deals — skipped."
                )
            if failed:
                lines.append(
                    f":warning: {len(failed)} contact(s) failed to sync "
                    f"— check automation_log for details."
                )
            text = "\n".join(lines)
        elif failed:
            text = (
                f":warning: HubSpot → Pipedrive Sync: "
                f"{len(failed)} contact(s) failed, {skipped} skipped. "
                f"Check automation_log."
            )
        else:
            suffix = f" ({skipped} checked)" if skipped else ""
            text = (
                f":white_check_mark: HubSpot → Pipedrive Sync: "
                f"All qualified leads already have Pipedrive deals.{suffix}"
            )

        self.send_slack(_SLACK_CHANNEL, text)


# ─────────────────────────────────────────────────────────────────────────────
# Dry-run sample data
# ─────────────────────────────────────────────────────────────────────────────

_DRY_RUN_CONTACTS = [
    {
        "hubspot_id":     "dry-hs-sql-001",
        "email":          "marcos.vega@pinnacleoffices.com",
        "firstname":      "Marcos",
        "lastname":       "Vega",
        "phone":          "(512) 334-7821",
        "company":        "Pinnacle Office Solutions",
        "client_type":    "commercial",
        "lead_source":    "referral",
        "neighborhood":   "Domain",
        "lifetime_value": "86400",
    },
    {
        "hubspot_id":     "dry-hs-sql-002",
        "email":          "jennifer.kwon@austinlofts.com",
        "firstname":      "Jennifer",
        "lastname":       "Kwon",
        "phone":          "(737) 209-5543",
        "company":        "",
        "client_type":    "residential",
        "lead_source":    "google_ads",
        "neighborhood":   "Hyde Park",
        "lifetime_value": "3600",
    },
]


# ─────────────────────────────────────────────────────────────────────────────
# Dry-run sanity test
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    sys.path.insert(0, _PROJECT_ROOT)

    from auth import get_client
    from database.schema import get_connection

    live = "--live" in sys.argv
    dry_run = not live

    label = "LIVE RUN" if live else "dry-run sanity test"
    print("=" * 65)
    print(f"  HubSpotQualifiedSync — {label}")
    print("=" * 65)

    db = get_connection(os.path.join(_PROJECT_ROOT, "sparkle_shine.db"))
    db.row_factory = __import__("sqlite3").Row

    automation = HubSpotQualifiedSync(
        clients=get_client,
        db=db,
        dry_run=dry_run,
    )

    if dry_run:
        print(
            f"\nUsing {len(_DRY_RUN_CONTACTS)} synthetic HubSpot contact(s) "
            f"(salesqualifiedlead, no existing Pipedrive deal):\n"
        )
        for c in _DRY_RUN_CONTACTS:
            print(
                f"  {c['firstname']} {c['lastname']} | {c['email']} "
                f"| {c['lead_source']} | LTV={c['lifetime_value']}"
            )

    print()
    automation.run()

    print()
    print("─" * 65)
    print("automation_log entries for this run:")
    print("─" * 65)
    rows = db.execute(
        """
        SELECT action_name, action_target, status, error_message
        FROM automation_log
        WHERE automation_name = 'HubSpotQualifiedSync'
        ORDER BY id DESC
        LIMIT 10
        """
    ).fetchall()
    for row in reversed(rows):
        r = dict(row)
        marker = (
            "OK " if r["status"] == "success"
            else ("---" if r["status"] == "skipped" else "ERR")
        )
        print(f"  [{marker}] {r['action_name']:<45} → {r['action_target'] or 'n/a'}")
        if r["error_message"]:
            print(f"         note: {r['error_message']}")

    print()
    if dry_run:
        print("Dry-run complete. No external API calls were made.")
    else:
        print("Live run complete.")
    db.close()
