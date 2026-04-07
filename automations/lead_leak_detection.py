"""
automations/lead_leak_detection.py

Automation — Lead Leak Detection (scheduled, daily)
No trigger event. Fetches net-new HubSpot leads (created since the last run)
in the lead/MQL lifecycle stage, then cross-checks each against Pipedrive to
surface leads that entered marketing but were never picked up by sales.

Steps:
  1. Pull leads from HubSpot (lifecyclestage in lead/MQL, created since last run)
  2. For each lead, check cross_tool_mapping then fall back to a Pipedrive
     person search by email. A lead "leaks" if both checks come up empty AND
     it is more than 48 hours old.
  3. Create Asana follow-up tasks in "Sales Pipeline Tasks → Follow-Up"
     using deduplicate_by_title=True to prevent repeat tasks on consecutive runs.
  4. Post a count-only summary to #sales.
"""
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Optional

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from automations.base import BaseAutomation
from automations.utils.asana_tasks import create_tasks
from automations.utils.id_resolver import MappingNotFoundError

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

_SALES_ESTIMATOR_EMAIL = "maria.gonzalez@oviodigital.com"

_ASANA_PROJECT  = "Sales Pipeline Tasks"
_ASANA_SECTION  = "Follow-Up"

_SLACK_CHANNEL  = "sales"

_LEAD_STAGES            = ["lead", "marketingqualifiedlead"]
_DEFAULT_LOOKBACK_HOURS = 24   # Fallback window when no sentinel file exists
_LEAK_HOURS             = 48   # Grace period: leads < 48h old are excluded

_SENTINEL_FILE = os.path.join(_PROJECT_ROOT, "logs", ".lead_leak_last_run")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _load_tool_ids() -> dict:
    path = os.path.join(_PROJECT_ROOT, "config", "tool_ids.json")
    with open(path) as f:
        return json.load(f)


def _parse_hs_datetime(raw: Optional[str]) -> Optional[datetime]:
    """Parse a HubSpot ISO-8601 timestamp (milliseconds or full ISO). Returns None on failure."""
    if not raw:
        return None
    try:
        # HubSpot sometimes returns epoch-ms as a string
        ts_ms = int(raw)
        return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    except (ValueError, TypeError):
        pass
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _days_ago(dt: datetime) -> int:
    now = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return max(0, (now - dt).days)


def _pipedrive_base(session) -> str:
    base = session.base_url.rstrip("/")
    if not any(seg in base for seg in ("/v1", "/v2")):
        base = f"{base}/v1"
    return base


# ─────────────────────────────────────────────────────────────────────────────
# Main class
# ─────────────────────────────────────────────────────────────────────────────

class LeadLeakDetection(BaseAutomation):
    """
    Scheduled daily automation: finds HubSpot leads with no Pipedrive deal
    and creates Asana follow-up tasks for the sales estimator.
    """

    def run(self) -> None:
        run_id         = self.generate_run_id()
        trigger_source = "scheduled:lead_leak_detection"

        # Validate that the HubSpot property used for lead source attribution exists
        _configured_props = _load_tool_ids().get("hubspot", {}).get("contact_properties", [])
        if "lead_source_detail" not in _configured_props:
            print(
                "[WARN] 'lead_source_detail' not found in tool_ids hubspot.contact_properties "
                "— lead source attribution may be empty"
            )

        # ── Step 1: Pull HubSpot leads ────────────────────────────────────────
        leads = []
        try:
            leads = self._fetch_hubspot_leads()
            self.log_action(
                run_id, "fetch_hubspot_leads", f"hubspot:contacts:{len(leads)}",
                "success",
                trigger_source=trigger_source,
                trigger_detail={"count": len(leads)},
            )
        except Exception as exc:
            self.log_action(
                run_id, "fetch_hubspot_leads", None, "failed",
                error_message=str(exc), trigger_source=trigger_source,
            )
            return  # Cannot continue without leads

        # ── Step 2: Find leaked leads ──────────────────────────────────────────
        leaked = []
        try:
            leaked = self._find_leaked_leads(leads)
            self.log_action(
                run_id, "check_pipedrive_deals",
                f"pipedrive:persons:checked:{len(leads)}",
                "success",
                trigger_source=trigger_source,
                trigger_detail={"leads_checked": len(leads), "leaks_found": len(leaked)},
            )
        except Exception as exc:
            self.log_action(
                run_id, "check_pipedrive_deals", None, "failed",
                error_message=str(exc), trigger_source=trigger_source,
            )
            return

        # ── Step 3: Create Asana tasks ────────────────────────────────────────
        task_gids = []
        try:
            task_gids = self._create_asana_tasks(leaked)
            self.log_action(
                run_id, "create_asana_tasks",
                f"asana:project:{_ASANA_PROJECT}",
                "success",
                trigger_source=trigger_source,
                trigger_detail={"tasks_created": len(task_gids)},
            )
        except Exception as exc:
            self.log_action(
                run_id, "create_asana_tasks", None, "failed",
                error_message=str(exc), trigger_source=trigger_source,
            )

        # ── Step 4: Post Slack summary ────────────────────────────────────────
        try:
            self._post_slack_summary(leaked)
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

    def _fetch_hubspot_leads(self) -> list:
        """
        POST /crm/v3/objects/contacts/search
        Filters: lifecyclestage IN (lead, MQL) AND createdate > last run time.
        The cutoff is read from the sentinel file's mtime; falls back to 24h ago
        if the sentinel does not yet exist (first run).
        Returns list of dicts: hubspot_id, email, firstname, lastname,
        lead_source (read from lead_source_detail), createdate (raw string).
        """
        from hubspot.crm.contacts import PublicObjectSearchRequest

        if os.path.exists(_SENTINEL_FILE):
            cutoff_ms = int(os.path.getmtime(_SENTINEL_FILE) * 1000)
        else:
            cutoff_ms = int(
                (datetime.now(timezone.utc) - timedelta(hours=_DEFAULT_LOOKBACK_HOURS))
                .timestamp() * 1000
            )

        search_request = PublicObjectSearchRequest(
            filter_groups=[
                {
                    "filters": [
                        {
                            "propertyName": "lifecyclestage",
                            "operator": "IN",
                            "values": _LEAD_STAGES,
                        },
                        {
                            "propertyName": "createdate",
                            "operator": "GT",
                            "value": str(cutoff_ms),
                        },
                    ]
                }
            ],
            properties=["email", "firstname", "lastname", "lead_source_detail", "createdate"],
            limit=200,
        )

        if self.dry_run:
            window = "last run" if os.path.exists(_SENTINEL_FILE) else f"{_DEFAULT_LOOKBACK_HOURS}h ago (first run)"
            print(
                f"[DRY RUN] Would POST /crm/v3/objects/contacts/search "
                f"(lifecyclestage IN {_LEAD_STAGES}, createdate > {window})"
            )
            return _DRY_RUN_LEADS

        hs_client = self.clients("hubspot")
        response  = hs_client.crm.contacts.search_api.do_search(search_request, _request_timeout=30)
        results   = response.results or []

        leads = []
        for contact in results:
            props = contact.properties or {}
            leads.append({
                "hubspot_id":  str(contact.id),
                "email":       props.get("email") or "",
                "firstname":   props.get("firstname") or "",
                "lastname":    props.get("lastname") or "",
                "lead_source": props.get("lead_source_detail") or "Unknown",
                "createdate":  props.get("createdate") or "",
            })
        return leads

    # ── Step 2 ────────────────────────────────────────────────────────────────

    def _find_leaked_leads(self, leads: list) -> list:
        """
        For each lead, determine whether it has a Pipedrive deal via:
          1. cross_tool_mapping (canonical ID → pipedrive deal)
          2. Direct Pipedrive person search by email as fallback

        A lead "leaks" only when BOTH checks are empty AND createdate > 48h ago.
        Returns list of enriched dicts with added `days_ago` key.
        """
        leaked = []
        now    = datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=_LEAK_HOURS)

        for lead in leads:
            hubspot_id  = lead["hubspot_id"]
            email       = lead["email"]
            createdate  = _parse_hs_datetime(lead["createdate"])

            # Must be older than 48 hours to count as a leak
            if createdate and createdate > cutoff:
                continue

            has_deal = False

            # — Check 1: cross_tool_mapping ——————————————————————————————————
            try:
                canonical_id = self.reverse_resolve_id(hubspot_id, "hubspot")
                try:
                    self.resolve_id(canonical_id, "pipedrive")
                    has_deal = True
                except MappingNotFoundError:
                    pass
            except MappingNotFoundError:
                pass

            if has_deal:
                continue

            # — Check 2: Direct Pipedrive person search by email ——————————————
            if not self.dry_run and email:
                try:
                    has_deal = self._pipedrive_has_deal_for_email(email)
                except Exception as exc:
                    # Fallback failure is non-fatal; treat as no deal found
                    try:
                        from simulation.error_reporter import report_error
                        report_error(exc, tool_name="pipedrive",
                                     context=f"Pipedrive fallback deal search for {email}",
                                     dry_run=self.dry_run)
                    except Exception:
                        pass

            if has_deal:
                continue

            # — Confirmed leak ———————————————————————————————————————————————
            days_ago = _days_ago(createdate) if createdate else 0
            leaked.append({**lead, "days_ago": days_ago, "createdate_dt": createdate})

        return leaked

    def _pipedrive_has_deal_for_email(self, email: str) -> bool:
        """
        GET /v1/persons/search?term={email}
        If a person is found, check for associated deals.
        Returns True if at least one deal exists.
        """
        session = self.clients("pipedrive")
        base    = _pipedrive_base(session)

        resp = session.get(
            f"{base}/persons/search",
            params={"term": email, "fields": "email", "limit": 5},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()

        if not data.get("success"):
            return False

        items = data.get("data", {}).get("items", []) or []
        if not items:
            return False

        # Check deals for the first matching person
        person_id = items[0].get("item", {}).get("id")
        if not person_id:
            return False

        deal_resp = session.get(
            f"{base}/persons/{person_id}/deals",
            params={"status": "all_not_deleted", "limit": 1},
            timeout=15,
        )
        deal_resp.raise_for_status()
        deal_data = deal_resp.json()

        if not deal_data.get("success"):
            return False

        deals = deal_data.get("data") or []
        return len(deals) > 0

    # ── Step 3 ────────────────────────────────────────────────────────────────

    def _create_asana_tasks(self, leaked: list) -> list:
        """
        Create one Asana task per leaked lead in "Sales Pipeline Tasks → Follow-Up".
        Uses deduplicate_by_title=True to prevent duplicates on repeat runs.
        Returns list of created task GIDs.
        """
        if not leaked:
            return []

        tomorrow   = (date.today() + timedelta(days=1)).isoformat()
        tool_ids   = _load_tool_ids()
        task_defs  = []

        for lead in leaked:
            name        = f"{lead['firstname']} {lead['lastname']}".strip() or lead["email"]
            source      = lead["lead_source"]
            days_ago    = lead["days_ago"]
            email       = lead["email"]
            create_date = (
                lead["createdate_dt"].strftime("%Y-%m-%d")
                if lead.get("createdate_dt")
                else lead.get("createdate", "")[:10]
            )

            title = f"Follow up on leaked lead: {name} ({source})"
            description = (
                f"This lead entered HubSpot {days_ago} days ago from "
                f"{source} but has no matching Pipedrive deal.\n\n"
                f"Email: {email}\n"
                f"Created: {create_date}\n\n"
                f"Please create a Pipedrive deal or mark this lead as disqualified "
                f"in HubSpot."
            )

            task_defs.append({
                "title":          title,
                "description":    description,
                "assignee_email": _SALES_ESTIMATOR_EMAIL,
                "due_date":       tomorrow,
            })

        if self.dry_run:
            print(
                f"[DRY RUN] Would create {len(task_defs)} Asana task(s) in "
                f'"{_ASANA_PROJECT} → {_ASANA_SECTION}" '
                f"(deduplicate_by_title=True):"
            )
            for td in task_defs:
                print(f"  - {td['title']}")
            return []

        asana_client = self.clients("asana")
        return create_tasks(
            client=asana_client,
            project_name=_ASANA_PROJECT,
            section_name=_ASANA_SECTION,
            tasks=task_defs,
            tool_ids=tool_ids,
            deduplicate_by_title=True,
        )

    # ── Step 4 ────────────────────────────────────────────────────────────────

    def _post_slack_summary(self, leaked: list) -> None:
        """Post a lead qualification opportunity report to #sales."""
        if leaked:
            n        = len(leaked)
            verb     = "is" if n == 1 else "are"
            plural   = "" if n == 1 else "s"
            has_have = "hasn't" if n == 1 else "haven't"
            text = (
                f":bell: Lead Qualification Opportunity: There {verb} {n} new "
                f"lead{plural} in HubSpot that {has_have} moved to the "
                f"Qualified Sales Lead lifecycle stage.\n\n"
                f"Asana tasks created for follow-up."
            )
        else:
            text = (
                ":white_check_mark: No new unqualified leads since the last run."
            )

        self.send_slack(_SLACK_CHANNEL, text)


# ─────────────────────────────────────────────────────────────────────────────
# Dry-run sample data
# ─────────────────────────────────────────────────────────────────────────────

_three_days_ago = (
    datetime.now(timezone.utc) - timedelta(days=3)
).strftime("%Y-%m-%dT%H:%M:%S+00:00")

_DRY_RUN_LEADS = [
    {
        "hubspot_id":  "dry-hs-001",
        "email":       "alex.morrison@example.com",
        "firstname":   "Alex",
        "lastname":    "Morrison",
        "lead_source": "Google Ads",
        "createdate":  _three_days_ago,
    },
    {
        "hubspot_id":  "dry-hs-002",
        "email":       "priya.shah@example.com",
        "firstname":   "Priya",
        "lastname":    "Shah",
        "lead_source": "Website Form",
        "createdate":  _three_days_ago,
    },
]


# ─────────────────────────────────────────────────────────────────────────────
# Dry-run sanity test
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    sys.path.insert(0, _PROJECT_ROOT)

    from auth import get_client
    from database.schema import get_connection

    print("=" * 65)
    print("  LeadLeakDetection — dry-run sanity test")
    print("=" * 65)

    db = get_connection(os.path.join(_PROJECT_ROOT, "sparkle_shine.db"))

    automation = LeadLeakDetection(
        clients=get_client,
        db=db,
        dry_run=True,
    )

    print(
        f"\nUsing {len(_DRY_RUN_LEADS)} synthetic HubSpot lead(s) "
        f"(created ~3 days ago, no Pipedrive mapping):\n"
    )
    for lead in _DRY_RUN_LEADS:
        print(f"  {lead['firstname']} {lead['lastname']} | {lead['email']} | {lead['lead_source']}")

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
        WHERE automation_name = 'LeadLeakDetection'
        ORDER BY id DESC
        LIMIT 8
        """
    ).fetchall()
    for row in reversed(rows):
        r = dict(row)
        marker = (
            "OK " if r["status"] == "success"
            else ("---" if r["status"] == "skipped" else "ERR")
        )
        print(f"  [{marker}] {r['action_name']:<40} → {r['action_target'] or 'n/a'}")
        if r["error_message"]:
            print(f"         note: {r['error_message']}")

    print()
    print("Dry-run complete. No external API calls were made.")
    db.close()
