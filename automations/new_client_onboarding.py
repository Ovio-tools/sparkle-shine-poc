"""
automations/new_client_onboarding.py

Automation 1 — New Client Onboarding
Trigger: a deal is marked "won" in Pipedrive.

Actions (each in its own try/except so failures are isolated):
  1. Create onboarding task list in Asana (Client Success → Onboarding)
  2. Create client + first job/agreement in Jobber
  3. Create customer record in QuickBooks
  4. Tag Mailchimp subscriber as active client
  5. Post #new-clients notification in Slack
  6. Verify all four cross-tool mappings are registered (warn, don't fail)
"""
import hashlib
import json
import os
import sys
from datetime import date, timedelta
from typing import Optional

import requests

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from automations.base import BaseAutomation
from automations.utils.asana_tasks import create_tasks
from automations.utils.id_resolver import MappingNotFoundError, register_mapping

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

_JOBBER_GQL_URL = "https://api.getjobber.com/api/graphql"

_OFFICE_MANAGER_EMAIL = "maria.gonzalez@oviodigital.com"
_CREW_LEAD_EMAIL      = "maria.gonzalez@oviodigital.com"

# QBO sandbox Net 30 term ID (standard in all QBO sandboxes)
_QBO_NET30_TERM_ID = "3"

# ─────────────────────────────────────────────────────────────────────────────
# Jobber GraphQL mutations (reuse proven shapes from push_jobber.py)
# ─────────────────────────────────────────────────────────────────────────────

_CLIENT_CREATE = """
mutation ClientCreate($input: ClientCreateInput!) {
  clientCreate(input: $input) {
    client { id firstName lastName companyName }
    userErrors { message path }
  }
}
"""

_PROPERTY_CREATE = """
mutation PropertyCreate($clientId: EncodedId!, $input: PropertyCreateInput!) {
  propertyCreate(clientId: $clientId, input: $input) {
    properties { id }
    userErrors { message path }
  }
}
"""

_JOB_CREATE = """
mutation JobCreate($input: JobCreateAttributes!) {
  jobCreate(input: $input) {
    job { id title jobStatus }
    userErrors { message path }
  }
}
"""


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _load_tool_ids() -> dict:
    path = os.path.join(_PROJECT_ROOT, "config", "tool_ids.json")
    with open(path) as f:
        return json.load(f)


def _parse_name(contact_name: str) -> tuple:
    """Split 'First Last' → ('First', 'Last'). Handles single-word names."""
    parts = contact_name.strip().split(" ", 1)
    return (parts[0], parts[1]) if len(parts) == 2 else (parts[0], "")


def _normalize_client_type(raw: Optional[str]) -> str:
    """Map raw Pipedrive field value to canonical type: residential/commercial/one-time."""
    if not raw:
        return "residential"
    lower = str(raw).lower()
    if "commercial" in lower:
        return "commercial"
    if "one" in lower or "onetime" in lower or "one-time" in lower:
        return "one-time"
    return "residential"


def _due(base: date, days: int) -> str:
    return (base + timedelta(days=days)).isoformat()


def _subscriber_hash(email: str) -> str:
    return hashlib.md5(email.strip().lower().encode()).hexdigest()


def _gql_errors(resp_data: dict, mutation_key: str) -> list:
    """Extract userErrors from a Jobber GraphQL response."""
    return (
        resp_data.get("data", {})
        .get(mutation_key, {})
        .get("userErrors", [])
    ) or resp_data.get("errors", [])


# ─────────────────────────────────────────────────────────────────────────────
# Task-list templates
# ─────────────────────────────────────────────────────────────────────────────

def _build_task_list(
    client_type: str,
    client_name: str,
    close_date: date,
    first_visit_date: Optional[date] = None,
    scheduled_date: Optional[date] = None,
) -> list:
    """
    Return a list of task dicts for `asana_tasks.create_tasks`.
    Each dict: {title, due_date, assignee_email (optional)}

    Admin tasks (indices 0-2) are assigned to the office manager.
    Service tasks (index 3+) are assigned to the crew lead.
    Both fall back gracefully if tool_ids has null for these users.
    """
    fv = first_visit_date or (close_date + timedelta(days=7))
    sd = scheduled_date or (close_date + timedelta(days=7))
    om = _OFFICE_MANAGER_EMAIL
    cl = _CREW_LEAD_EMAIL

    def t(title: str, due: str, assignee: Optional[str] = None) -> dict:
        task: dict = {"title": f"{client_name} — {title}", "due_date": due}
        if assignee:
            task["assignee_email"] = assignee
        return task

    if client_type == "commercial":
        return [
            t("Send contract copy",                    _due(close_date, 1),  om),
            t("Set up QuickBooks billing (Net 30)",    _due(close_date, 2),  om),
            t("Create Jobber recurring schedule",      _due(close_date, 3),  om),
            t("Pre-service walkthrough with crew lead",_due(close_date, 5),  cl),
            t("Order specialized supplies",            _due(close_date, 5),  cl),
            t("First service visit",                   _due(close_date, 7),  cl),
            t("Quality inspection",                    _due(close_date, 8),  cl),
            t("Client check-in",                       _due(close_date, 9),  om),
            t("30-day review",                         _due(close_date, 30), om),
            t("90-day formal review",                  _due(close_date, 90), om),
        ]

    if client_type == "one-time":
        return [
            t("Confirm appointment",      _due(close_date, 1), om),
            t("Complete service",         sd.isoformat(),      cl),
            t("Send invoice",             sd.isoformat(),      om),
            t("Post-service follow-up",   _due(sd, 2),         om),
            t("Convert to recurring offer", _due(close_date, 5), om),
        ]

    # Default: residential
    return [
        t("Send welcome email",        _due(close_date, 1),  om),
        t("Collect payment method",    _due(close_date, 2),  om),
        t("Create Jobber profile",     _due(close_date, 2),  om),
        t("Schedule first visit",      _due(close_date, 3),  cl),
        t("Confirm service details",   _due(close_date, 3),  om),
        t("Post-clean follow-up call", _due(fv, 1),          om),
        t("Send review request email", _due(fv, 3),          om),
        t("Rebooking check",           _due(close_date, 7),  om),
        t("Confirm recurring schedule",_due(close_date, 14), om),
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Main class
# ─────────────────────────────────────────────────────────────────────────────

class NewClientOnboarding(BaseAutomation):
    """
    Orchestrates the six-step new-client onboarding flow.

    Expects `self.clients` to be a callable: clients("tool_name") → client/session.
    """

    # ── Entry point ───────────────────────────────────────────────────────────

    def run(self, trigger_event: dict) -> None:
        run_id = self.generate_run_id()
        trigger_source = f"pipedrive:deal:{trigger_event.get('deal_id', 'unknown')}"

        # ── Build shared context ──────────────────────────────────────────────
        ctx = self._build_context(trigger_event)
        tool_ids = _load_tool_ids()

        # ── Action 1: Asana ───────────────────────────────────────────────────
        task_count = 0
        try:
            task_gids = self._action_asana_tasks(ctx, tool_ids)
            task_count = len(task_gids)
            self.log_action(
                run_id, "create_asana_tasks",
                f"asana:project:ClientSuccess:{task_count}_tasks",
                "success",
                trigger_source=trigger_source,
                trigger_detail={"deal_id": ctx["deal_id"], "client_type": ctx["client_type"]},
            )
        except Exception as exc:
            self.log_action(
                run_id, "create_asana_tasks", None, "failed",
                error_message=str(exc), trigger_source=trigger_source,
            )

        # ── Action 2: Jobber ──────────────────────────────────────────────────
        jobber_client_id = None
        try:
            jobber_client_id, jobber_job_id = self._action_jobber(ctx)
            if not self.dry_run and jobber_client_id:
                register_mapping(self.db, ctx["canonical_id"], "jobber", jobber_client_id)
            if not self.dry_run and jobber_job_id:
                register_mapping(self.db, f"JOB:{ctx['canonical_id']}", "jobber", jobber_job_id)
            self.log_action(
                run_id, "create_jobber_client",
                f"jobber:client:{jobber_client_id}",
                "success",
                trigger_source=trigger_source,
            )
        except Exception as exc:
            self.log_action(
                run_id, "create_jobber_client", None, "failed",
                error_message=str(exc), trigger_source=trigger_source,
            )

        # ── Action 3: QuickBooks ──────────────────────────────────────────────
        try:
            qbo_customer_id = self._action_quickbooks(ctx)
            if not self.dry_run and qbo_customer_id:
                register_mapping(self.db, ctx["canonical_id"], "quickbooks_customer", qbo_customer_id)
            self.log_action(
                run_id, "create_quickbooks_customer",
                f"quickbooks:customer:{qbo_customer_id}",
                "success",
                trigger_source=trigger_source,
            )
        except Exception as exc:
            self.log_action(
                run_id, "create_quickbooks_customer", None, "failed",
                error_message=str(exc), trigger_source=trigger_source,
            )

        # ── Action 4: Mailchimp ───────────────────────────────────────────────
        try:
            self._action_mailchimp(ctx, tool_ids)
            self.log_action(
                run_id, "tag_mailchimp_subscriber",
                f"mailchimp:member:{_subscriber_hash(ctx['email'])}",
                "success",
                trigger_source=trigger_source,
            )
        except Exception as exc:
            self.log_action(
                run_id, "tag_mailchimp_subscriber", None, "failed",
                error_message=str(exc), trigger_source=trigger_source,
            )

        # ── Action 5: Slack ───────────────────────────────────────────────────
        try:
            self._action_slack(ctx, task_count)
            self.log_action(
                run_id, "post_slack_notification",
                "slack:channel:new-clients",
                "success",
                trigger_source=trigger_source,
            )
        except Exception as exc:
            self.log_action(
                run_id, "post_slack_notification", None, "failed",
                error_message=str(exc), trigger_source=trigger_source,
            )

        # ── Action 6: Cross-tool mapping verification ─────────────────────────
        self._action_verify_mappings(run_id, ctx, trigger_source)

    # ── Context builder ───────────────────────────────────────────────────────

    def _build_context(self, event: dict) -> dict:
        """Normalise trigger_event into a consistent context dict."""
        first, last = _parse_name(event.get("contact_name") or "Unknown Client")
        client_type = _normalize_client_type(event.get("client_type"))
        email = (event.get("contact_email") or "").strip().lower()
        close_date = date.today()

        canonical_id = self._get_or_create_canonical_id(
            deal_id=str(event.get("deal_id", "")),
            email=email,
            first_name=first,
            last_name=last,
            client_type=client_type,
        )

        return {
            "deal_id":       str(event.get("deal_id", "")),
            "canonical_id":  canonical_id,
            "first_name":    first,
            "last_name":     last,
            "display_name":  f"{first} {last}".strip(),
            "email":         email,
            "phone":         event.get("contact_phone") or "",
            "client_type":   client_type,
            "service_type":  event.get("service_type") or "",
            "service_freq":  event.get("service_frequency") or "",
            "deal_value":    float(event.get("deal_value") or 0),
            "neighborhood":  event.get("neighborhood") or "Austin",
            "address":       event.get("address") or "",
            "close_date":    close_date,
        }

    def _get_or_create_canonical_id(
        self,
        deal_id: str,
        email: str,
        first_name: str,
        last_name: str,
        client_type: str,
    ) -> str:
        """
        Return an existing canonical SS-CLIENT-XXXX or mint a new one.

        Lookup order:
          1. cross_tool_mapping (Pipedrive deal_id already processed)
             - If the mapping resolves to an SS-LEAD-* ID, the lead is being
               won for the first time: promote them to a proper SS-CLIENT-*
               so all downstream actions use the correct entity type.
          2. clients table (email match from prior seeding)
          3. Generate next sequential ID, insert into clients, register mapping
        """
        # 1. Already processed this deal?
        if deal_id:
            try:
                existing_id = self.reverse_resolve_id(deal_id, "pipedrive")
                # Promote lead → client when a won deal maps to an SS-LEAD-* ID.
                # This happens when HubSpotQualifiedSync created the deal while
                # the contact was still a lead; we must not carry the LEAD
                # canonical ID forward into the client onboarding flow.
                if existing_id.startswith("SS-LEAD-"):
                    return self._promote_lead_to_client(
                        lead_id=existing_id,
                        deal_id=deal_id,
                        first_name=first_name,
                        last_name=last_name,
                        email=email,
                        client_type=client_type,
                    )
                return existing_id
            except MappingNotFoundError:
                pass

        # 2. Email already in clients table (seeded data)?
        if email:
            row = self.db.execute(
                "SELECT id FROM clients WHERE email = %s", (email,)
            ).fetchone()
            if row is not None:
                cid = row["id"]
                if not self.dry_run and deal_id:
                    register_mapping(self.db, cid, "pipedrive", deal_id)
                return cid

        # 3. Mint new canonical ID.
        # psycopg2 runs with autocommit=False by default, so the connection is
        # already inside an implicit transaction.  The read-modify-insert below
        # is protected by that transaction; ON CONFLICT DO NOTHING handles the
        # rare case where two concurrent runs generate the same candidate ID.
        row_c = self.db.execute(
            "SELECT id FROM clients WHERE id LIKE 'SS-CLIENT-%' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        row_m = self.db.execute(
            "SELECT canonical_id FROM cross_tool_mapping "
            "WHERE canonical_id LIKE 'SS-CLIENT-%' ORDER BY canonical_id DESC LIMIT 1"
        ).fetchone()

        candidates = []
        if row_c:
            candidates.append(row_c["id"])
        if row_m:
            candidates.append(row_m["canonical_id"])

        if candidates:
            top = max(candidates)
            next_n = int(top.split("-")[-1]) + 1
        else:
            next_n = 1

        canonical_id = f"SS-CLIENT-{next_n:04d}"

        if not self.dry_run:
            try:
                self.db.execute(
                    """
                    INSERT INTO clients
                        (id, client_type, first_name, last_name, email, status)
                    VALUES (%s, %s, %s, %s, %s, 'active')
                    ON CONFLICT DO NOTHING
                    """,
                    (canonical_id, client_type, first_name, last_name, email),
                )
                self.db.commit()
            except Exception as ins_exc:
                self.db.rollback()
                print(f"[WARN] Could not insert client row {canonical_id}: {ins_exc}")
                raise

            if deal_id:
                register_mapping(self.db, canonical_id, "pipedrive", deal_id)

        return canonical_id

    def _promote_lead_to_client(
        self,
        lead_id: str,
        deal_id: str,
        first_name: str,
        last_name: str,
        email: str,
        client_type: str,
    ) -> str:
        """
        Mint a new SS-CLIENT-XXXX for a lead that just had their deal won.

        Steps:
          1. Allocate the next sequential SS-CLIENT-XXXX inside the implicit
             transaction (same pattern as the mint-new branch above).
          2. Insert the client row (ON CONFLICT DO NOTHING so re-runs are safe).
          3. Re-point all existing cross_tool_mapping rows for this lead to the
             new client canonical ID, so downstream tools see the CLIENT entity.
          4. Return the new client canonical ID.

        The old lead ID (SS-LEAD-*) is intentionally left intact in the leads
        table / any other tool mappings unrelated to this deal, so historical
        data is not disturbed.
        """
        if self.dry_run:
            print(
                f"[DRY RUN] Would promote {lead_id} → SS-CLIENT-XXXX "
                f"(deal_id={deal_id})"
            )
            return lead_id  # dry run: return lead_id so the rest of the flow logs cleanly

        row_c = self.db.execute(
            "SELECT id FROM clients WHERE id LIKE 'SS-CLIENT-%' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        row_m = self.db.execute(
            "SELECT canonical_id FROM cross_tool_mapping "
            "WHERE canonical_id LIKE 'SS-CLIENT-%' ORDER BY canonical_id DESC LIMIT 1"
        ).fetchone()

        candidates = []
        if row_c:
            candidates.append(row_c["id"])
        if row_m:
            candidates.append(row_m["canonical_id"])

        next_n = (int(max(candidates).split("-")[-1]) + 1) if candidates else 1
        client_id = f"SS-CLIENT-{next_n:04d}"

        try:
            self.db.execute(
                """
                INSERT INTO clients
                    (id, client_type, first_name, last_name, email, status)
                VALUES (%s, %s, %s, %s, %s, 'active')
                ON CONFLICT DO NOTHING
                """,
                (client_id, client_type, first_name, last_name, email),
            )
            # Re-point only the Pipedrive deal mapping (and pipedrive_person if
            # it shares the same tool_specific_id) from the lead ID to the new
            # client ID.  Other lead mappings (e.g. HubSpot) are left untouched.
            self.db.execute(
                """
                UPDATE cross_tool_mapping
                   SET canonical_id = %s, entity_type = 'CLIENT', synced_at = CURRENT_TIMESTAMP
                 WHERE canonical_id = %s
                   AND tool_name IN ('pipedrive', 'pipedrive_person')
                """,
                (client_id, lead_id),
            )
            self.db.commit()
        except Exception as exc:
            self.db.rollback()
            print(
                f"[WARN] Could not promote {lead_id} → {client_id}: {exc}. "
                f"Falling back to lead ID."
            )
            return lead_id

        print(
            f"[INFO] Promoted {lead_id} → {client_id} "
            f"(deal_id={deal_id}, {first_name} {last_name})"
        )
        return client_id

    # ── Action 1: Asana ───────────────────────────────────────────────────────

    def _action_asana_tasks(self, ctx: dict, tool_ids: dict) -> list:
        task_defs = _build_task_list(
            client_type=ctx["client_type"],
            client_name=ctx["display_name"],
            close_date=ctx["close_date"],
        )

        if self.dry_run:
            print(
                f"[DRY RUN] Would create {len(task_defs)} Asana tasks "
                f"in 'Client Success → Onboarding' for {ctx['display_name']}"
            )
            for td in task_defs:
                print(f"  • {td['title']}  (due {td['due_date']})")
            return [f"dry-run-gid-{i}" for i in range(len(task_defs))]

        asana_client = self.clients("asana")
        return create_tasks(
            client=asana_client,
            project_name="Client Success",
            section_name="Onboarding",
            tasks=task_defs,
            tool_ids=tool_ids,
            deduplicate_by_title=True,
        )

    # ── Action 2: Jobber ──────────────────────────────────────────────────────

    def _action_jobber(self, ctx: dict) -> tuple:
        """
        Create a Jobber client + service property, then a job or recurring agreement.
        Returns (jobber_client_id, jobber_job_id). Either may be None on partial failure.
        """
        if self.dry_run:
            print(
                f"[DRY RUN] Would create Jobber client: {ctx['display_name']} "
                f"<{ctx['email']}>"
            )
            print(
                f"[DRY RUN] Would create Jobber {'recurring job' if ctx['client_type'] != 'one-time' else 'one-time job'}"
            )
            return ("dry-run-jobber-client-id", "dry-run-jobber-job-id")

        session = self.clients("jobber")

        # Build client input (mirrors push_jobber._client_input)
        client_input: dict = {}
        if ctx["client_type"] == "commercial":
            client_input["companyName"] = ctx["display_name"]
        else:
            client_input["firstName"] = ctx["first_name"]
            client_input["lastName"] = ctx["last_name"]

        if ctx["email"]:
            client_input["emails"] = [{"description": "MAIN", "address": ctx["email"]}]
        if ctx["phone"]:
            client_input["phones"] = [{"description": "MAIN", "number": ctx["phone"]}]
        if ctx["address"]:
            client_input["billingAddress"] = {
                "street1": ctx["address"],
                "city": "Austin",
                "province": "TX",
                "country": "US",
            }
        # Create client
        resp = session.post(
            _JOBBER_GQL_URL,
            json={"query": _CLIENT_CREATE, "variables": {"input": client_input}},
            timeout=20,
        )
        resp.raise_for_status()
        body = resp.json()

        errors = _gql_errors(body, "clientCreate")
        if errors:
            raise RuntimeError(f"Jobber clientCreate errors: {errors}")

        client_node = body.get("data", {}).get("clientCreate", {}).get("client")
        if not client_node:
            raise RuntimeError("Jobber clientCreate returned no client node")

        jobber_client_id = client_node["id"]

        # Create service property
        prop_input: dict = {
            "properties": [{"address": {
                "street1": ctx["address"] or "123 Main St",
                "city": "Austin",
                "province": "TX",
                "country": "US",
            }}]
        }
        prop_resp = session.post(
            _JOBBER_GQL_URL,
            json={
                "query": _PROPERTY_CREATE,
                "variables": {"clientId": jobber_client_id, "input": prop_input},
            },
            timeout=20,
        )
        prop_resp.raise_for_status()
        prop_body = prop_resp.json()
        prop_nodes = (
            prop_body.get("data", {})
            .get("propertyCreate", {})
            .get("properties", [])
        )
        jobber_property_id = prop_nodes[0]["id"] if prop_nodes else None

        if not jobber_property_id:
            print(
                f"[WARN] No property returned for {ctx['canonical_id']} "
                f"— job creation skipped"
            )
            return (jobber_client_id, None)

        # Create job
        job_input: dict = {
            "propertyId": jobber_property_id,
            "title": ctx["service_type"] or "Cleaning Service",
            "invoicing": {
                "invoicingType": "FIXED_PRICE",
                "invoicingSchedule": "ON_COMPLETION",
            },
        }
        if ctx["close_date"]:
            job_input["timeframe"] = {"startAt": ctx["close_date"].isoformat()}

        job_resp = session.post(
            _JOBBER_GQL_URL,
            json={"query": _JOB_CREATE, "variables": {"input": job_input}},
            timeout=20,
        )
        job_resp.raise_for_status()
        job_body = job_resp.json()

        job_errors = _gql_errors(job_body, "jobCreate")
        if job_errors:
            print(f"[WARN] Jobber jobCreate errors: {job_errors}")
            return (jobber_client_id, None)

        job_node = job_body.get("data", {}).get("jobCreate", {}).get("job")
        jobber_job_id = job_node["id"] if job_node else None

        return (jobber_client_id, jobber_job_id)

    # ── Action 3: QuickBooks ──────────────────────────────────────────────────

    def _action_quickbooks(self, ctx: dict) -> Optional[str]:
        """Create a QBO customer and return its Id."""
        if self.dry_run:
            print(
                f"[DRY RUN] Would create QuickBooks customer: {ctx['display_name']} "
                f"({ctx['client_type']})"
            )
            return "dry-run-qbo-customer-id"

        from auth.quickbooks_auth import get_base_url
        headers = self.clients("quickbooks")
        base_url = get_base_url()

        body: dict = {
            "DisplayName": ctx["display_name"],
            "Notes": f"SS-ID: {ctx['canonical_id']}",
        }
        if ctx["email"]:
            body["PrimaryEmailAddr"] = {"Address": ctx["email"]}
        if ctx["phone"]:
            body["PrimaryPhone"] = {"FreeFormNumber": ctx["phone"]}
        if ctx["client_type"] == "commercial":
            body["CompanyName"] = ctx["display_name"]
            body["SalesTermRef"] = {"value": _QBO_NET30_TERM_ID}

        resp = requests.post(
            f"{base_url}/customer",
            headers=headers,
            json=body,
            params={"minorversion": "65"},
            timeout=15,
        )

        # QBO returns 400 if a customer with this DisplayName already exists.
        # In that case, look up the existing customer and return their ID.
        if resp.status_code == 400:
            error_detail = resp.json()
            fault = error_detail.get("Fault", {})
            errors = fault.get("Error", [])
            is_duplicate = any(
                e.get("code") == "6240" or "Duplicate Name" in e.get("Detail", "")
                for e in errors
            )
            if is_duplicate:
                display_name = body["DisplayName"].replace("'", "\\'")
                q = f"SELECT Id FROM Customer WHERE DisplayName = '{display_name}'"
                qr = requests.get(
                    f"{base_url}/query",
                    headers=headers,
                    params={"query": q, "minorversion": "65"},
                    timeout=15,
                )
                qr.raise_for_status()
                customers = qr.json().get("QueryResponse", {}).get("Customer", [])
                if customers:
                    return str(customers[0]["Id"])
            resp.raise_for_status()

        resp.raise_for_status()
        data = resp.json()
        customer = data.get("Customer") or data.get("customer")
        if not customer:
            raise RuntimeError(f"QBO customer create returned unexpected body: {data}")

        return str(customer.get("Id", ""))

    # ── Action 4: Mailchimp ───────────────────────────────────────────────────

    def _action_mailchimp(self, ctx: dict, tool_ids: dict) -> None:
        """Upsert the subscriber in Mailchimp and apply onboarding tags."""
        audience_id = tool_ids["mailchimp"]["audience_id"]
        sub_hash = _subscriber_hash(ctx["email"])

        # Build tags
        tags = ["active-client"]
        ct = ctx["client_type"]
        sf = ctx.get("service_freq") or ""
        if ct == "residential":
            freq_tag = (
                "residential-recurring" if sf else "residential-client"
            )
        elif ct == "commercial":
            freq_tag = "commercial-client"
        else:
            freq_tag = "residential-client"
        tags.append(freq_tag)
        if ctx["service_type"]:
            tags.append(ctx["service_type"].lower().replace(" ", "-")[:50])

        # Merge fields (using actual tags created in Phase 1 setup)
        merge_fields: dict = {
            "FNAME": ctx["first_name"],
            "LNAME": ctx["last_name"],
        }
        if ctx["phone"]:
            merge_fields["PHONE"] = ctx["phone"]
        if ctx["neighborhood"]:
            merge_fields["NEIGHBORHD"] = ctx["neighborhood"]
        merge_fields["CLIENTTYPE"] = ct
        if ctx["service_type"]:
            merge_fields["SVCTYPE"] = ctx["service_type"]

        member_body = {
            "email_address": ctx["email"],
            "status": "subscribed",
            "merge_fields": merge_fields,
        }

        if self.dry_run:
            print(
                f"[DRY RUN] Would upsert Mailchimp subscriber: {ctx['email']} "
                f"with tags {tags}"
            )
            return

        mc = self.clients("mailchimp")

        # PUT upserts (create or update)
        try:
            mc.lists.set_list_member(audience_id, sub_hash, member_body)
        except Exception as exc:
            # Fallback: POST to create if set_list_member not available on this SDK version
            try:
                mc.lists.add_list_member(audience_id, member_body)
            except Exception:
                raise exc  # raise the original error

        # Apply tags
        mc.lists.update_list_member_tags(
            audience_id,
            sub_hash,
            {"tags": [{"name": tag, "status": "active"} for tag in tags]},
        )

        # Register the Mailchimp subscriber hash in cross_tool_mapping so
        # data-integrity checks and downstream automations can find it.
        register_mapping(self.db, ctx["canonical_id"], "mailchimp", sub_hash)

    # ── Action 5: Slack ───────────────────────────────────────────────────────

    def _action_slack(self, ctx: dict, task_count: int) -> None:
        freq_label = ctx.get("service_freq") or ctx["client_type"]
        value_str = f"${ctx['deal_value']:,.0f}/{freq_label}" if ctx["deal_value"] else "TBD"

        text = (
            f":sparkles: *New Client Onboarded: {ctx['display_name']}*\n"
            f"Type: {ctx['client_type'].title()}\n"
            f"Value: {value_str}\n"
            f"Zone: {ctx['neighborhood']}\n"
            f"Onboarding tasks created in Asana ({task_count} tasks)"
        )
        self.send_slack("new-clients", text)

    # ── Action 6: Cross-tool mapping verification ─────────────────────────────

    def _action_verify_mappings(
        self, run_id: str, ctx: dict, trigger_source: str
    ) -> None:
        required_tools = ["pipedrive", "jobber", "quickbooks_customer", "mailchimp"]
        canonical_id = ctx["canonical_id"]

        cursor = self.db.execute(
            """
            SELECT tool_name FROM cross_tool_mapping
            WHERE canonical_id = %s
            """,
            (canonical_id,),
        )
        registered = {row["tool_name"] for row in cursor.fetchall()}

        all_present = True
        for tool in required_tools:
            if tool not in registered:
                all_present = False
                print(
                    f"[WARN] {canonical_id} has no mapping for '{tool}' "
                    f"— sync gap detected"
                )

        missing = [t for t in required_tools if t not in registered]
        self.log_action(
            run_id,
            "verify_cross_tool_mappings",
            f"canonical:{canonical_id}",
            "success" if all_present else "failed",
            error_message=None if all_present else (
                f"Missing mappings: " + ", ".join(missing)
            ),
            trigger_source=trigger_source,
        )
        if not all_present:
            self.send_slack(
                "operations",
                f":warning: Onboarding sync gap for `{canonical_id}`: "
                f"no mapping in {', '.join(missing)}. Manual follow-up required.",
            )


# ─────────────────────────────────────────────────────────────────────────────
# Dry-run sanity test
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    sys.path.insert(0, _PROJECT_ROOT)

    from auth import get_client
    from database.schema import get_connection

    print("=" * 65)
    print("  NewClientOnboarding — dry-run sanity test")
    print("=" * 65)

    db = get_connection(os.path.join(_PROJECT_ROOT, "sparkle_shine.db"))


    fake_event = {
        "deal_id": "DRY-RUN-9999",
        "contact_name": "Alejandra Vega",
        "contact_email": "alejandra.vega.dryrun@example.com",
        "contact_phone": "(512) 555-0199",
        "deal_value": 150.00,
        "client_type": "Residential",
        "service_type": "Recurring Biweekly",
        "service_frequency": "biweekly",
        "notes": "Dry-run test deal",
        "neighborhood": "Zilker",
        "address": "2201 Barton Springs Rd, Austin, TX 78704",
        "close_date": date.today().isoformat(),
    }

    automation = NewClientOnboarding(
        clients=get_client,
        db=db,
        dry_run=True,
    )

    print(f"\nTrigger event: deal_id={fake_event['deal_id']}, "
          f"name={fake_event['contact_name']}, "
          f"type={fake_event['client_type']}")
    print()

    automation.run(fake_event)

    print()
    print("─" * 65)
    print("automation_log entries for this run:")
    print("─" * 65)
    rows = db.execute(
        """
        SELECT action_name, action_target, status, error_message, created_at
        FROM automation_log
        WHERE automation_name = 'NewClientOnboarding'
        ORDER BY id DESC
        LIMIT 10
        """
    ).fetchall()
    for row in reversed(rows):
        r = dict(row)
        status_marker = "OK " if r["status"] == "success" else (
            "---" if r["status"] == "skipped" else "ERR"
        )
        print(
            f"  [{status_marker}] {r['action_name']:<35} → {r['action_target'] or 'n/a'}"
        )
        if r["error_message"]:
            print(f"         error: {r['error_message']}")

    print()
    print("Dry-run complete. No external API calls were made.")
    db.close()
