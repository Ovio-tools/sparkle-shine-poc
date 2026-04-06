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
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta, timezone

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from automations.base import BaseAutomation

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


_CIRCUIT_BREAKER_THRESHOLD = 3   # permanent-skip after this many failures


def _allocate_lead_id(db, hubspot_id: str) -> str:
    """
    Atomically allocate the next SS-LEAD-NNNN canonical ID.

    Uses SELECT ... FOR UPDATE on the highest existing LEAD row in
    cross_tool_mapping to serialize concurrent callers, preventing two
    runners from allocating the same ID.

    If this hubspot_id is already mapped, returns the existing canonical ID
    (idempotent on retry).

    IMPORTANT: Does NOT register the HubSpot mapping here.  The mapping is
    written later by _register_mappings() together with the Pipedrive
    mappings in a single atomic transaction.  This prevents orphaned
    hubspot-only mappings when Pipedrive creation fails.

    Returns the canonical_id string (e.g. "SS-LEAD-0314").
    """
    # Check if this HubSpot contact already has a canonical ID.
    existing = db.execute(
        "SELECT canonical_id FROM cross_tool_mapping "
        "WHERE tool_name = 'hubspot' AND tool_specific_id = %s",
        (hubspot_id,),
    ).fetchone()
    if existing:
        return existing["canonical_id"]

    # Lock the highest LEAD row to serialize concurrent ID allocation.
    # This prevents two runners from reading the same max and generating
    # duplicate IDs.  The lock is held until this transaction commits.
    db.execute(
        "SELECT canonical_id FROM cross_tool_mapping "
        "WHERE entity_type = 'LEAD' "
        "ORDER BY canonical_id DESC LIMIT 1 "
        "FOR UPDATE"
    )

    # Compute next ID from both sources.
    row = db.execute("SELECT id FROM leads ORDER BY id DESC LIMIT 1").fetchone()
    leads_max = int(row["id"].split("-")[-1]) if row else 0

    row2 = db.execute(
        "SELECT canonical_id FROM cross_tool_mapping "
        "WHERE entity_type = 'LEAD' ORDER BY canonical_id DESC LIMIT 1"
    ).fetchone()
    mapping_max = int(row2["canonical_id"].split("-")[-1]) if row2 else 0

    next_n = max(leads_max, mapping_max) + 1
    canonical_id = f"SS-LEAD-{next_n:04d}"

    # Commit the serialization lock — the canonical ID is now reserved.
    # The actual hubspot mapping is NOT written here; it will be written
    # atomically with Pipedrive mappings in _register_mappings().
    db.commit()

    return canonical_id


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

        # ── Step 3: Sync each contact to Pipedrive ──────────────────────────
        # Both truly_new and needs_pipedrive contacts go through the same
        # unified flow.  The only difference is whether _allocate_lead_id()
        # is called (truly_new) or the existing canonical ID is used (repair).
        synced = []
        failed = []
        merged = []

        work_items = []
        for contact in truly_new:
            work_items.append({"contact": contact, "canonical_id": None, "is_repair": False})
        for item in needs_pipedrive:
            work_items.append({
                "contact": item["contact"],
                "canonical_id": item["canonical_id"],
                "is_repair": True,
            })

        for item in work_items:
            contact      = item["contact"]
            canonical_id = item["canonical_id"]
            is_repair    = item["is_repair"]
            action_name  = "repair_pipedrive_mapping" if is_repair else "sync_contact_to_pipedrive"

            # Circuit breaker: skip if this contact has hit the failure threshold
            if self._should_skip(contact["hubspot_id"]):
                continue

            try:
                if not is_repair:
                    if self.dry_run:
                        canonical_id = f"SS-LEAD-DRY-{contact['hubspot_id'][:6]}"
                    else:
                        canonical_id = _allocate_lead_id(self.db, contact["hubspot_id"])

                person_id, deal_id, merge_into = self._create_pipedrive_records(
                    contact, canonical_id,
                )

                if merge_into:
                    # Pipedrive person is already owned by a different canonical ID.
                    # Merge: register only the HubSpot ID under the existing owner.
                    self._register_merge(merge_into, contact["hubspot_id"], deal_id)
                    merged.append({
                        **contact,
                        "canonical_id": merge_into,
                        "merged_from": canonical_id,
                        "person_id": person_id,
                        "deal_id": deal_id,
                    })
                    self.log_action(
                        run_id, "merge_contact",
                        f"pipedrive:deal:{deal_id}",
                        "success",
                        trigger_source=trigger_source,
                        trigger_detail={
                            "merged_into": merge_into,
                            "discarded_id": canonical_id,
                            "hubspot_id": contact["hubspot_id"],
                            "person_id": person_id,
                            "deal_id": deal_id,
                            "email": contact["email"],
                        },
                    )
                else:
                    self._register_mappings(
                        canonical_id, contact["hubspot_id"], person_id, deal_id,
                        include_hubspot=not is_repair,
                    )
                    synced.append({
                        **contact,
                        "canonical_id": canonical_id,
                        "person_id":    person_id,
                        "deal_id":      deal_id,
                    })
                    self.log_action(
                        run_id, action_name,
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
                    run_id, action_name, contact["hubspot_id"], "error",
                    error_message=str(exc),
                    trigger_source=trigger_source,
                    trigger_detail={
                        "canonical_id": canonical_id or "unallocated",
                        "hubspot_id":   contact["hubspot_id"],
                    },
                )
                # Check circuit breaker after logging the failure
                self._maybe_permanently_skip(contact["hubspot_id"])

        # ── Step 4: Post Slack summary (only when there is something to report) ─
        if synced or failed or merged:
            try:
                self._post_slack_summary(synced, skipped, failed, merged)
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

    # ── Step 1 helper ─────────────────────────────────────────────────────────

    def _enrich_from_db(self, contact: dict) -> dict:
        """
        Backfill missing contact fields from the canonical leads record.

        Seeded leads pushed to HubSpot before the enrichment fix may lack
        client_type or lifetime_value.  The leads table in
        PostgreSQL is the source of truth; we look up the canonical ID via
        cross_tool_mapping (tool_name='hubspot') and read the missing fields.

        Falls back gracefully — if no DB record exists (e.g. simulation-
        generated contacts that already carried full data into HubSpot), the
        contact dict is returned unchanged.
        """
        hubspot_id = contact["hubspot_id"]

        row = self.db.execute(
            """
            SELECT l.lead_type, l.estimated_value
            FROM   cross_tool_mapping ctm
            JOIN   leads l ON l.id = ctm.canonical_id
            WHERE  ctm.tool_name        = 'hubspot'
              AND  ctm.tool_specific_id = %s
            LIMIT 1
            """,
            (hubspot_id,),
        ).fetchone()

        if not row:
            return contact

        enriched = dict(contact)
        if not enriched.get("client_type") and row["lead_type"]:
            enriched["client_type"] = row["lead_type"]
        # Only use DB estimated_value when HubSpot carried no lifetime_value
        if (enriched.get("lifetime_value") in (None, "", "0", "0.0")
                and row["estimated_value"]):
            enriched["lifetime_value"] = str(round(row["estimated_value"], 2))

        return enriched

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
            # Stable sort order prevents ID misalignment if the contact list
            # is processed across retries or concurrent runners.
            sorts=[{"propertyName": "createdate", "direction": "ASCENDING"}],
            limit=200,
        )

        if self.dry_run:
            print(
                f"[DRY RUN] Would POST /crm/v3/objects/contacts/search "
                f"(lifecyclestage=salesqualifiedlead, createdate > {_LOOKBACK_DAYS}d ago)"
            )
            return _DRY_RUN_CONTACTS

        hs_client = self.clients("hubspot")
        response  = hs_client.crm.contacts.search_api.do_search(search_request, _request_timeout=30)
        results   = response.results or []

        contacts = []
        for contact in results:
            props = contact.properties or {}
            raw = {
                "hubspot_id":     str(contact.id),
                "email":          props.get("email")              or "",
                "firstname":      props.get("firstname")          or "",
                "lastname":       props.get("lastname")           or "",
                "phone":          props.get("phone")              or "",
                "company":        props.get("company")            or "",
                "client_type":    props.get("client_type")        or "",
                "lead_source":    props.get("lead_source_detail") or "Unknown",
                "neighborhood":   props.get("neighborhood")       or "",
                "lifetime_value": props.get("lifetime_value")     or "0",
            }
            enriched = self._enrich_from_db(raw)
            # Final fallback: client_type must always have a value for Pipedrive
            if not enriched["client_type"]:
                enriched["client_type"] = "residential"
            contacts.append(enriched)
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

        Contacts in the sync_skip_list are excluded entirely (circuit breaker).

        Contacts that already have both a hubspot *and* a pipedrive mapping
        are skipped (fully synced).

        Returns (truly_new: list[dict], needs_pipedrive: list[dict])
        where needs_pipedrive entries are {"contact": ..., "canonical_id": ...}.
        """
        truly_new       = []
        needs_pipedrive = []

        # Load the skip list in one query to avoid N+1
        skip_rows = self.db.execute(
            "SELECT tool_specific_id FROM sync_skip_list "
            "WHERE tool_name = 'hubspot'"
        ).fetchall()
        skip_set = {r["tool_specific_id"] for r in skip_rows}

        for contact in contacts:
            # Circuit breaker: permanently skipped contacts
            if contact["hubspot_id"] in skip_set:
                continue

            # Find all canonical IDs already mapped to this HubSpot contact.
            rows = self.db.execute(
                "SELECT DISTINCT canonical_id FROM cross_tool_mapping "
                "WHERE tool_specific_id = %s AND tool_name = 'hubspot'",
                (contact["hubspot_id"],),
            ).fetchall()
            canonical_ids = [r["canonical_id"] for r in rows]

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
                    "WHERE canonical_id = %s AND tool_name LIKE 'pipedrive%%' LIMIT 1",
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
    ) -> tuple[str, str, str | None]:
        """
        Idempotently create (or locate an existing) Pipedrive person then deal.

        Returns (person_id, deal_id, merge_into_canonical_id).

        merge_into_canonical_id is set when the Pipedrive person already exists
        and is owned by a different canonical ID.  The caller should register
        the HubSpot mapping under merge_into_canonical_id instead of the
        freshly allocated one.
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
            return "dry-person-id", "dry-deal-id", None

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

        person_id, is_new_person, existing_owner = self._get_or_create_person(
            session, base, contact["email"], person_payload,
        )

        # ── Merge detection ──────────────────────────────────────────────────
        merge_into = None
        if not is_new_person and existing_owner and existing_owner != canonical_id:
            merge_into = existing_owner

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

        return person_id, deal_id, merge_into

    def _get_or_create_person(
        self, session, base: str, email: str, payload: dict
    ) -> tuple[str, bool, str | None]:
        """
        Return (person_id, is_new, existing_canonical_id) for the given email.

        If a Pipedrive person already exists for this email, checks
        cross_tool_mapping to see which canonical ID owns it.  The caller
        uses existing_canonical_id to decide whether to merge.
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
                        pid = str(person["id"])
                        owner = self._check_person_ownership(pid)
                        return pid, False, owner

        pr = session.post(f"{base}/persons", json=payload, timeout=30)
        pr.raise_for_status()
        return str(pr.json()["data"]["id"]), True, None

    def _check_person_ownership(self, person_id: str) -> str | None:
        """Return the canonical ID that owns this Pipedrive person, or None."""
        row = self.db.execute(
            "SELECT canonical_id FROM cross_tool_mapping "
            "WHERE tool_name = 'pipedrive_person' AND tool_specific_id = %s",
            (person_id,),
        ).fetchone()
        return row["canonical_id"] if row else None

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
        include_hubspot: bool = True,
    ) -> None:
        """
        Register mappings for the canonical ID in a single atomic transaction.

        When include_hubspot=True (truly_new flow), registers all three:
        hubspot, pipedrive_person, pipedrive.  When include_hubspot=False
        (repair flow), the hubspot mapping already exists — only Pipedrive
        mappings are written.

        All collision checks run first. If all pass, rows are written
        atomically.  ON CONFLICT DO NOTHING handles idempotent retries.
        """
        if self.dry_run:
            print(
                f"[DRY RUN] Would register mappings: "
                f"{canonical_id} → hubspot:{hubspot_id}, "
                f"pipedrive_person:{person_id}, pipedrive:{deal_id}"
            )
            return

        rows_to_write = []
        if include_hubspot:
            rows_to_write.append(("hubspot", hubspot_id))
        rows_to_write.append(("pipedrive_person", person_id))
        rows_to_write.append(("pipedrive", deal_id))

        # Collision guard — check all before opening the write transaction.
        for tool_name, tool_id in rows_to_write:
            existing = self.db.execute(
                "SELECT canonical_id FROM cross_tool_mapping "
                "WHERE tool_name = %s AND tool_specific_id = %s",
                (tool_name, tool_id),
            ).fetchone()
            if existing is not None:
                ecid = existing["canonical_id"]
                if ecid != canonical_id:
                    raise ValueError(
                        f"Mapping collision: {tool_name}:{tool_id} is already "
                        f"registered to {ecid}, cannot also register to {canonical_id}"
                    )

        # All checks passed — write rows atomically.
        entity_type = canonical_id.split("-")[1]  # SS-LEAD-0213 → LEAD
        try:
            for tool_name, tool_id in rows_to_write:
                self.db.execute(
                    """
                    INSERT INTO cross_tool_mapping
                        (canonical_id, entity_type, tool_name, tool_specific_id, synced_at)
                    VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT(canonical_id, tool_name) DO NOTHING
                    """,
                    (canonical_id, entity_type, tool_name, tool_id),
                )
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise

    def _register_merge(
        self, owner_canonical_id: str, hubspot_id: str, deal_id: str,
    ) -> None:
        """
        Merge flow: register the new HubSpot contact under the existing
        canonical owner.  The pipedrive_person mapping already exists.
        Only add the hubspot mapping (and deal if new).
        """
        if self.dry_run:
            print(
                f"[DRY RUN] Would merge hubspot:{hubspot_id} into "
                f"{owner_canonical_id}, deal:{deal_id}"
            )
            return

        entity_type = owner_canonical_id.split("-")[1]
        try:
            # Register the HubSpot ID under the existing owner
            self.db.execute(
                """
                INSERT INTO cross_tool_mapping
                    (canonical_id, entity_type, tool_name, tool_specific_id, synced_at)
                VALUES (%s, %s, 'hubspot', %s, CURRENT_TIMESTAMP)
                ON CONFLICT(canonical_id, tool_name) DO NOTHING
                """,
                (owner_canonical_id, entity_type, hubspot_id),
            )
            # Register the deal if not already mapped
            existing_deal = self.db.execute(
                "SELECT 1 FROM cross_tool_mapping "
                "WHERE canonical_id = %s AND tool_name = 'pipedrive'",
                (owner_canonical_id,),
            ).fetchone()
            if not existing_deal:
                self.db.execute(
                    """
                    INSERT INTO cross_tool_mapping
                        (canonical_id, entity_type, tool_name, tool_specific_id, synced_at)
                    VALUES (%s, %s, 'pipedrive', %s, CURRENT_TIMESTAMP)
                    ON CONFLICT(canonical_id, tool_name) DO NOTHING
                    """,
                    (owner_canonical_id, entity_type, deal_id),
                )
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise

    # ── Circuit breaker ─────────────────────────────────────────────────────

    def _should_skip(self, hubspot_id: str) -> bool:
        """Return True if this HubSpot contact is in the sync_skip_list."""
        row = self.db.execute(
            "SELECT 1 FROM sync_skip_list "
            "WHERE tool_name = 'hubspot' AND tool_specific_id = %s",
            (hubspot_id,),
        ).fetchone()
        return row is not None

    def _maybe_permanently_skip(self, hubspot_id: str) -> None:
        """
        If this HubSpot contact has failed >= _CIRCUIT_BREAKER_THRESHOLD times,
        add it to sync_skip_list so future runs skip it immediately.
        """
        row = self.db.execute(
            "SELECT COUNT(*) AS cnt FROM automation_log "
            "WHERE automation_name = 'HubSpotQualifiedSync' "
            "  AND action_name IN ('sync_contact_to_pipedrive', 'repair_pipedrive_mapping') "
            "  AND status = 'error' "
            "  AND action_target = %s",
            (hubspot_id,),
        ).fetchone()
        if row["cnt"] >= _CIRCUIT_BREAKER_THRESHOLD:
            with self.db:
                self.db.execute(
                    """
                    INSERT INTO sync_skip_list (tool_name, tool_specific_id, reason, detail)
                    VALUES ('hubspot', %s, 'collision_limit', %s)
                    ON CONFLICT (tool_name, tool_specific_id) DO NOTHING
                    """,
                    (hubspot_id, f"Failed {row['cnt']} times"),
                )

    # ── Step 4 ────────────────────────────────────────────────────────────────

    def _post_slack_summary(
        self, synced: list, skipped: int, failed: list, merged: list | None = None,
    ) -> None:
        """Post a sync report to #sales."""
        merged = merged or []
        if synced or merged:
            lines = []
            if synced:
                lines.append(
                    f":rocket: HubSpot → Pipedrive Sync: "
                    f"{len(synced)} qualified lead(s) pushed to Pipedrive\n"
                )
                for s in synced:
                    name = f"{s['firstname']} {s['lastname']}".strip() or s["email"]
                    lines.append(f"  • {name} ({s['lead_source']}) → deal #{s['deal_id']}")
            if merged:
                lines.append(
                    f"\n:link: {len(merged)} contact(s) merged into existing records"
                )
                for m in merged:
                    name = f"{m['firstname']} {m['lastname']}".strip() or m["email"]
                    lines.append(f"  • {name} → merged into {m['canonical_id']}")
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
