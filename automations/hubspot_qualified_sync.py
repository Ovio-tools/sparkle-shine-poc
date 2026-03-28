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
  4. Post a Slack summary to #sales
"""
import json
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
_SLACK_CHANNEL = "sales"
_LOOKBACK_DAYS = 90

_TOOL_IDS_PATH = os.path.join(_PROJECT_ROOT, "config", "tool_ids.json")


def _load_field_ids() -> tuple:
    """Return (person_fields, deal_fields) dicts loaded from tool_ids.json."""
    with open(_TOOL_IDS_PATH) as f:
        ids = json.load(f)["pipedrive"]
    return ids["person_fields"], ids["deal_fields"]


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
        person_fields, deal_fields = _load_field_ids()

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
        truly_new, needs_pipedrive = self._filter_new_contacts(contacts)
        skipped = len(contacts) - len(truly_new) - len(needs_pipedrive)

        self.log_action(
            run_id, "filter_synced_contacts",
            f"total:{len(contacts)},new:{len(truly_new)},repair:{len(needs_pipedrive)},skipped:{skipped}",
            "success",
            trigger_source=trigger_source,
            trigger_detail={
                "total":          len(contacts),
                "new":            len(truly_new),
                "needs_pipedrive": len(needs_pipedrive),
                "skipped":        skipped,
            },
        )

        # ── Step 3: Sync each new contact to Pipedrive ────────────────────────
        synced = []
        failed = []

        # Atomically claim the full range of canonical IDs for this batch.
        # BEGIN IMMEDIATE acquires a write lock before the MAX reads so no
        # concurrent runner process can observe the same highest ID and
        # generate duplicate SS-LEAD-NNNN values.  We pre-insert each
        # contact's HubSpot mapping inside the transaction to stake the
        # claim, then commit before making any Pipedrive API calls.
        # _register_mappings will upsert the hubspot row (same data, no-op).
        if not self.dry_run and truly_new:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                next_id_n = _next_lead_id_number(self.db)
                for i, contact in enumerate(truly_new):
                    self.db.execute(
                        """
                        INSERT INTO cross_tool_mapping
                            (canonical_id, entity_type, tool_name,
                             tool_specific_id, synced_at)
                        VALUES (?, 'LEAD', 'hubspot', ?, datetime('now'))
                        ON CONFLICT(canonical_id, tool_name) DO UPDATE SET
                            tool_specific_id = excluded.tool_specific_id,
                            synced_at        = datetime('now')
                        """,
                        (f"SS-LEAD-{next_id_n + i:04d}", contact["hubspot_id"]),
                    )
                self.db.commit()
            except Exception:
                self.db.rollback()
                raise
        else:
            next_id_n = _next_lead_id_number(self.db)

        for i, contact in enumerate(truly_new):
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

        # ── Step 3b: Repair contacts that have a HubSpot mapping but no
        #            Pipedrive mapping — reuse the existing canonical ID so
        #            register_mapping never sees a collision. ─────────────────
        for item in needs_pipedrive:
            contact      = item["contact"]
            canonical_id = item["canonical_id"]
            try:
                person_id, deal_id = self._create_pipedrive_records(contact, canonical_id)
                # Register only the Pipedrive mappings — the HubSpot mapping
                # already exists under this canonical_id, so skip it to avoid
                # triggering the collision guard.
                # If the person is already mapped to a different canonical ID
                # (e.g. they are an existing CLIENT), skip the person mapping;
                # the deal mapping is all we need here.
                try:
                    register_mapping(self.db, canonical_id, "pipedrive_person", person_id)
                except ValueError:
                    pass
                register_mapping(self.db, canonical_id, "pipedrive",        deal_id)
                synced.append({
                    **contact,
                    "canonical_id": canonical_id,
                    "person_id":    person_id,
                    "deal_id":      deal_id,
                })
                self.log_action(
                    run_id, "repair_pipedrive_mapping",
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
                    run_id, "repair_pipedrive_mapping", None, "failed",
                    error_message=str(exc),
                    trigger_source=trigger_source,
                    trigger_detail={
                        "canonical_id": canonical_id,
                        "hubspot_id":   contact["hubspot_id"],
                    },
                )

        # ── Step 4: Post Slack summary (only when there is something to report) ─
        if synced or failed:
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

    def _filter_new_contacts(self, contacts: list) -> tuple:
        """
        Classify contacts into two buckets:

        truly_new       — no hubspot mapping exists at all; needs a fresh
                          canonical ID and full Pipedrive sync.

        needs_pipedrive — a hubspot mapping exists (canonical ID already
                          assigned) but no Pipedrive mapping was ever saved,
                          e.g. because _register_mappings failed on a prior
                          run.  These must be repaired using the *existing*
                          canonical ID to avoid a collision in register_mapping.

        Contacts that already have both a hubspot *and* a pipedrive mapping
        are skipped (fully synced).

        Returns (truly_new: list[dict], needs_pipedrive: list[dict])
        where needs_pipedrive entries are {"contact": ..., "canonical_id": ...}.
        """
        truly_new       = []
        needs_pipedrive = []

        for contact in contacts:
            # Find all canonical IDs already mapped to this HubSpot contact.
            rows = self.db.execute(
                "SELECT DISTINCT canonical_id FROM cross_tool_mapping "
                "WHERE tool_specific_id = ? AND tool_name = 'hubspot'",
                (contact["hubspot_id"],),
            ).fetchall()
            canonical_ids = [r[0] if not hasattr(r, "keys") else r["canonical_id"] for r in rows]

            if not canonical_ids:
                # No hubspot mapping at all → genuinely new lead.
                truly_new.append(contact)
                continue

            # A canonical ID exists.  Check whether any of them already has
            # a Pipedrive mapping so we can decide if a repair is needed.
            has_pipedrive = False
            existing_canonical_id = canonical_ids[0]   # use the first (oldest) one
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
                # Hubspot mapping present but Pipedrive mapping missing →
                # prior sync partially failed.  Repair using the existing ID.
                needs_pipedrive.append({
                    "contact":      contact,
                    "canonical_id": existing_canonical_id,
                })

        return truly_new, needs_pipedrive

    # ── Step 3 ────────────────────────────────────────────────────────────────

    def _create_pipedrive_records(
        self, contact: dict, canonical_id: str
    ) -> tuple[str, str]:
        """
        Idempotently create (or locate an existing) Pipedrive person then deal.

        Searches Pipedrive for an existing person by email before creating, so
        re-running after a failed DB-write never produces a duplicate contact.
        Similarly, searches for an existing open deal for the person before
        creating a new one.

        Returns (person_id, deal_id) as strings.
        """
        person_fields, deal_fields = _load_field_ids()
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

        # Resolve org_id before creating the person (org_name is not accepted by the API)
        org_id = None
        if contact.get("company"):
            org_id = self._get_or_create_org(session, base, contact["company"])

        # ── Person: find existing or create ──────────────────────────────────
        person_payload = {
            "name":  full_name,
            "email": [{"value": contact["email"], "primary": True}],
            person_fields["HubSpot Contact ID"]: contact["hubspot_id"],
            person_fields["Acquisition Source"]: contact["lead_source"],
            person_fields["Neighborhood"]:       contact["neighborhood"],
        }
        if contact["phone"]:
            person_payload["phone"] = [{"value": contact["phone"], "primary": True}]
        if org_id is not None:
            person_payload["org_id"] = org_id

        person_id = self._get_or_create_person(session, base, contact["email"], person_payload)

        # ── Deal: find existing open deal for this person or create ──────────
        deal_payload = {
            "title":       deal_title,
            "pipeline_id": _PIPELINE_ID,
            "stage_id":    _STAGE_ID,
            "value":       annual_value,
            "currency":    "USD",
            "person_id":   int(person_id),
            deal_fields["Client Type"]:            contact["client_type"],
            deal_fields["Estimated Monthly Value"]: monthly_value,
            deal_fields["Lead Source"]:             contact["lead_source"],
        }
        deal_id = self._get_or_create_deal(session, base, person_id, deal_payload)

        return person_id, deal_id

    def _get_or_create_person(
        self, session, base: str, email: str, payload: dict
    ) -> str:
        """
        Return the Pipedrive person_id for the given email, creating the person
        if none exists.  Prevents duplicate persons when _create_pipedrive_records
        is retried after a failed mapping-registration.
        """
        if email:
            sr = session.get(
                f"{base}/persons/search",
                params={"term": email, "fields": "email", "exact_match": True, "limit": 5},
                timeout=15,
            )
            for item in (sr.json().get("data", {}).get("items") or []):
                person = item.get("item") or {}
                for e in (person.get("emails") or []):
                    email_val = e if isinstance(e, str) else e.get("value", "")
                    if email_val.lower() == email.lower():
                        return str(person["id"])

        pr = session.post(f"{base}/persons", json=payload, timeout=30)
        pr.raise_for_status()
        return str(pr.json()["data"]["id"])

    def _get_or_create_deal(
        self, session, base: str, person_id: str, payload: dict
    ) -> str:
        """
        Return an existing open deal_id for this person in the Qualified stage,
        creating a new deal only when none exists.  Prevents duplicate deals on
        retry runs for the same reason as _get_or_create_person.
        """
        sr = session.get(
            f"{base}/persons/{person_id}/deals",
            params={"status": "open", "limit": 20},
            timeout=15,
        )
        for deal in (sr.json().get("data") or []):
            if (
                deal.get("pipeline_id") == _PIPELINE_ID
                and deal.get("stage_id") == _STAGE_ID
            ):
                return str(deal["id"])

        dr = session.post(f"{base}/deals", json=payload, timeout=30)
        dr.raise_for_status()
        return str(dr.json()["data"]["id"])

    def _get_or_create_org(self, session, base: str, company: str) -> int:
        """Return Pipedrive org_id for company name, creating the org if it doesn't exist."""
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

    def _register_mappings(
        self,
        canonical_id: str,
        hubspot_id:   str,
        person_id:    str,
        deal_id:      str,
    ) -> None:
        """
        Register HubSpot, Pipedrive person, and Pipedrive deal mappings for
        the new canonical ID in a single atomic transaction.

        All three collision checks are run first (before any write). If all
        pass, all three rows are inserted/updated in one BEGIN…COMMIT block.
        This prevents the partial-write state where the hubspot mapping is
        committed but the pipedrive mappings are not, which was the root cause
        of the SS-LEAD-0213 duplication loop.
        """
        if self.dry_run:
            print(
                f"[DRY RUN] Would register mappings: "
                f"{canonical_id} → hubspot:{hubspot_id}, "
                f"pipedrive_person:{person_id}, pipedrive:{deal_id}"
            )
            return

        rows_to_write = [
            ("hubspot",          hubspot_id),
            ("pipedrive_person", person_id),
            ("pipedrive",        deal_id),
        ]

        # Collision guard — check all three before opening the write transaction.
        for tool_name, tool_id in rows_to_write:
            existing = self.db.execute(
                "SELECT canonical_id FROM cross_tool_mapping "
                "WHERE tool_name = ? AND tool_specific_id = ?",
                (tool_name, tool_id),
            ).fetchone()
            if existing is not None:
                ecid = existing[0] if not hasattr(existing, "keys") else existing["canonical_id"]
                if ecid != canonical_id:
                    raise ValueError(
                        f"Mapping collision: {tool_name}:{tool_id} is already "
                        f"registered to {ecid}, cannot also register to {canonical_id}"
                    )

        # All checks passed — write all three rows atomically.
        entity_type = canonical_id.split("-")[1]  # SS-LEAD-0213 → LEAD
        self.db.execute("BEGIN IMMEDIATE")
        try:
            for tool_name, tool_id in rows_to_write:
                self.db.execute(
                    """
                    INSERT INTO cross_tool_mapping
                        (canonical_id, entity_type, tool_name, tool_specific_id, synced_at)
                    VALUES (?, ?, ?, ?, datetime('now'))
                    ON CONFLICT(canonical_id, tool_name) DO UPDATE SET
                        tool_specific_id = excluded.tool_specific_id,
                        synced_at        = datetime('now')
                    """,
                    (canonical_id, entity_type, tool_name, tool_id),
                )
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise

    # ── Step 4 ────────────────────────────────────────────────────────────────

    def _post_slack_summary(
        self, synced: list, skipped: int, failed: list
    ) -> None:
        """Post a sync report to #sales."""
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
