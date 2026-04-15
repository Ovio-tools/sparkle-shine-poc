"""
automations/overdue_invoice.py

Automation — Weekly AR Aging Report
Scheduled: weekly. No trigger_event.

Steps:
  1. Query QuickBooks for all invoices with Balance > 0 and DueDate < today
  2. Resolve each QBO customer to canonical SS-CLIENT and pull client details
  3. Bucket invoices into 4 tiers by days past due
  4. Create Asana tasks in Admin & Operations → To Do for Tier 2+
     (deduplicates: skips if an open task containing the invoice DocNumber
      already exists in the project)
  5. Post Slack aging report to #operations
  6. Optionally DM Maria for Tier 4 invoices if her Slack user ID is in
     tool_ids.json
"""
import json
import os
import sys
from datetime import date, timedelta
from typing import Optional

import requests as _requests

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from automations.base import BaseAutomation
from automations.utils.assignees import MARIA_EMAIL, get_assignee_email
from automations.utils.id_resolver import MappingNotFoundError
import asana


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

_BOOKKEEPER_EMAIL     = get_assignee_email("bookkeeper")
_OFFICE_MANAGER_EMAIL = get_assignee_email("office_manager")
_OWNER_EMAIL          = MARIA_EMAIL

_ASANA_PROJECT = "Admin & Operations"
_ASANA_SECTION = "To Do"


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _load_tool_ids() -> dict:
    path = os.path.join(_PROJECT_ROOT, "config", "tool_ids.json")
    with open(path) as f:
        return json.load(f)


def _fmt_amount(amount: float) -> str:
    return f"${amount:,.2f}"


def _parse_date(raw: str) -> Optional[date]:
    """Parse ISO-8601 date string. Returns None on failure."""
    if not raw:
        return None
    try:
        return date.fromisoformat(str(raw)[:10])
    except (ValueError, TypeError):
        return None


def _us_federal_holidays(year: int) -> frozenset:
    """
    Return observed US federal holiday dates for *year*.
    Fixed-date holidays shift: Saturday -> prior Friday, Sunday -> next Monday.
    """
    def nth_weekday(n: int, weekday: int, month: int) -> date:
        """nth occurrence (1-based) of weekday (0=Mon…6=Sun) in month."""
        first = date(year, month, 1)
        first_hit = first + timedelta(days=(weekday - first.weekday()) % 7)
        return first_hit + timedelta(weeks=n - 1)

    def last_weekday(weekday: int, month: int) -> date:
        """Last occurrence of weekday (0=Mon…6=Sun) in month."""
        if month == 12:
            last = date(year, 12, 31)
        else:
            last = date(year, month + 1, 1) - timedelta(days=1)
        return last - timedelta(days=(last.weekday() - weekday) % 7)

    def observe(d: date) -> date:
        if d.weekday() == 5:   # Saturday -> Friday
            return d - timedelta(days=1)
        if d.weekday() == 6:   # Sunday -> Monday
            return d + timedelta(days=1)
        return d

    return frozenset({
        observe(date(year,  1,  1)),    # New Year's Day
        nth_weekday(3, 0,  1),          # MLK Day          (3rd Mon of Jan)
        nth_weekday(3, 0,  2),          # Presidents' Day  (3rd Mon of Feb)
        last_weekday(0,    5),          # Memorial Day     (last Mon of May)
        observe(date(year,  6, 19)),    # Juneteenth
        observe(date(year,  7,  4)),    # Independence Day
        nth_weekday(1, 0,  9),          # Labor Day        (1st Mon of Sep)
        nth_weekday(2, 0, 10),          # Columbus Day     (2nd Mon of Oct)
        observe(date(year, 11, 11)),    # Veterans Day
        nth_weekday(4, 3, 11),          # Thanksgiving     (4th Thu of Nov)
        observe(date(year, 12, 25)),    # Christmas Day
    })


def _add_business_days(start: date, days: int) -> date:
    """Return start + N business days (skips weekends and US federal holidays)."""
    _holidays: dict = {}
    current = start
    added   = 0
    while added < days:
        current += timedelta(days=1)
        yr = current.year
        if yr not in _holidays:
            _holidays[yr] = _us_federal_holidays(yr)
        if current.weekday() < 5 and current not in _holidays[yr]:
            added += 1
    return current


def _tier(days_past_due: int) -> int:
    """Map days past due to a tier number (1–4). -1 means due date is unknown."""
    if days_past_due < 0:
        return 1   # unknown due date — flag for review
    if days_past_due <= 30:
        return 1
    elif days_past_due <= 60:
        return 2
    elif days_past_due <= 90:
        return 3
    else:
        return 4


# ─────────────────────────────────────────────────────────────────────────────
# Dry-run fixture data
# ─────────────────────────────────────────────────────────────────────────────

_DRY_RUN_INVOICES = [
    {
        "Id": "DRY-001",
        "DocNumber": "1042",
        "Balance": 150.00,
        "DueDate": (date.today() - timedelta(days=15)).isoformat(),
        "CustomerRef": {"value": "dry-qbo-cust-001"},
    },
    {
        "Id": "DRY-002",
        "DocNumber": "1038",
        "Balance": 500.00,
        "DueDate": (date.today() - timedelta(days=45)).isoformat(),
        "CustomerRef": {"value": "dry-qbo-cust-002"},
    },
    {
        "Id": "DRY-003",
        "DocNumber": "1031",
        "Balance": 275.00,
        "DueDate": (date.today() - timedelta(days=72)).isoformat(),
        "CustomerRef": {"value": "dry-qbo-cust-003"},
    },
    {
        "Id": "DRY-004",
        "DocNumber": "1019",
        "Balance": 1200.00,
        "DueDate": (date.today() - timedelta(days=105)).isoformat(),
        "CustomerRef": {"value": "dry-qbo-cust-004"},
    },
]


# ─────────────────────────────────────────────────────────────────────────────
# Main class
# ─────────────────────────────────────────────────────────────────────────────

class OverdueInvoiceEscalation(BaseAutomation):
    """
    Weekly AR aging scan. No trigger_event — runs on a schedule.

    Tier definitions:
      Tier 1 (1–30 days)  : watch list, report only
      Tier 2 (31–60 days) : Asana task → bookkeeper
      Tier 3 (61–90 days) : Asana task → office manager, included in Slack
      Tier 4 (90+ days)   : Asana task → office manager, Slack CRITICAL,
                             optional DM to Maria
    """

    def run(self) -> None:
        run_id         = self.generate_run_id()
        trigger_source = "scheduled:weekly:overdue_invoice_scan"
        today          = date.today()

        # ── Step 1: Fetch overdue invoices from QuickBooks ─────────────────────
        invoices: list = []
        try:
            invoices = self._fetch_overdue_invoices(today)
            self.log_action(
                run_id, "fetch_overdue_invoices",
                "quickbooks:query:Invoice",
                "success",
                trigger_source=trigger_source,
                trigger_detail={"count": len(invoices), "date": today.isoformat()},
            )
        except Exception as exc:
            self.log_action(
                run_id, "fetch_overdue_invoices", None, "failed",
                error_message=str(exc), trigger_source=trigger_source,
            )
            print(f"[OverdueInvoiceEscalation] Fatal: could not fetch QBO invoices: {exc}")
            return

        # Nothing overdue → post all-clear and exit
        if not invoices:
            self._post_all_clear(run_id, trigger_source)
            return

        # ── Enrich each invoice with client data and tier ──────────────────────
        enriched = [self._enrich_invoice(inv, today) for inv in invoices]

        tier1 = [r for r in enriched if r["tier"] == 1]
        tier2 = [r for r in enriched if r["tier"] == 2]
        tier3 = [r for r in enriched if r["tier"] == 3]
        tier4 = [r for r in enriched if r["tier"] == 4]

        # ── Step 2: Create Asana tasks for Tier 2+ ────────────────────────────
        tool_ids = _load_tool_ids()
        for record in tier2 + tier3 + tier4:
            try:
                created = self._create_asana_task(record, tool_ids)
                self.log_action(
                    run_id, "create_asana_task",
                    f"asana:task:invoice#{record['doc_number']}",
                    "success" if created else "skipped",
                    error_message=None if created else "Duplicate — task already exists for this invoice",
                    trigger_source=trigger_source,
                    trigger_detail={
                        "doc_number": record["doc_number"],
                        "client":     record["client_name"],
                        "tier":       record["tier"],
                        "balance":    record["balance"],
                    },
                )
            except Exception as exc:
                self.log_action(
                    run_id, "create_asana_task", None, "failed",
                    error_message=str(exc), trigger_source=trigger_source,
                )

        # ── Step 3: Post Slack aging report ───────────────────────────────────
        try:
            self._post_slack_report(enriched, tier2, tier3, tier4)
            self.log_action(
                run_id, "post_slack_aging_report",
                "slack:channel:operations",
                "success",
                trigger_source=trigger_source,
                trigger_detail={"total_invoices": len(enriched)},
            )
        except Exception as exc:
            self.log_action(
                run_id, "post_slack_aging_report", None, "failed",
                error_message=str(exc), trigger_source=trigger_source,
            )

        # ── Step 3b: DM Maria for Tier 4 ──────────────────────────────────────
        if tier4:
            try:
                sent = self._dm_maria_tier4(tier4, tool_ids)
                self.log_action(
                    run_id, "dm_maria_tier4",
                    "slack:dm:maria_gonzalez",
                    "success" if sent else "skipped",
                    error_message=(
                        None if sent
                        else "Maria's Slack user ID not found in tool_ids.json"
                    ),
                    trigger_source=trigger_source,
                )
            except Exception as exc:
                self.log_action(
                    run_id, "dm_maria_tier4", None, "failed",
                    error_message=str(exc), trigger_source=trigger_source,
                )

    # ── QuickBooks query ───────────────────────────────────────────────────────

    def _fetch_overdue_invoices(self, today: date) -> list:
        """
        Query QBO for all invoices with Balance > 0 and DueDate < today.
        Returns the list of Invoice dicts from QueryResponse.
        """
        from auth.quickbooks_auth import get_base_url, get_quickbooks_headers

        if self.dry_run:
            print(
                f"[DRY RUN] Would query QBO:\n"
                f"  SELECT * FROM Invoice WHERE Balance > '0' "
                f"AND DueDate < '{today}'"
            )
            return _DRY_RUN_INVOICES

        headers  = get_quickbooks_headers()
        base_url = get_base_url()

        all_invoices: list = []
        start     = 1
        page_size = 1000
        while True:
            query = (
                f"SELECT * FROM Invoice WHERE Balance > '0' "
                f"AND DueDate < '{today}' "
                f"STARTPOSITION {start} MAXRESULTS {page_size}"
            )
            resp = _requests.get(
                f"{base_url}/query",
                headers=headers,
                params={"query": query, "minorversion": "65"},
                timeout=20,
            )
            resp.raise_for_status()
            page = resp.json().get("QueryResponse", {}).get("Invoice") or []
            all_invoices.extend(page)
            if len(page) < page_size:
                break
            start += page_size

        return all_invoices

    # ── Invoice enrichment ─────────────────────────────────────────────────────

    def _enrich_invoice(self, inv: dict, today: date) -> dict:
        """
        Resolve QBO customer ID → canonical SS-CLIENT → clients table.
        Compute days_past_due and assign a tier.
        Returns a normalised record dict used across all subsequent steps.
        """
        qbo_customer_id = str((inv.get("CustomerRef") or {}).get("value") or "")
        doc_number      = str(inv.get("DocNumber") or inv.get("Id") or "")
        inv_id          = str(inv.get("Id") or "")
        balance         = float(inv.get("Balance") or 0)
        due_date        = _parse_date(inv.get("DueDate") or "")
        days_past_due   = (today - due_date).days if due_date else -1

        # Canonical ID from QBO customer ID
        canonical_id: Optional[str] = None
        try:
            canonical_id = self.reverse_resolve_id(
                qbo_customer_id, "quickbooks", entity_type="CLIENT"
            )
        except MappingNotFoundError:
            pass

        # Client details from the local clients table
        client_name = "Unknown Client"
        email       = ""
        phone       = ""
        if canonical_id:
            row = self.db.execute(
                "SELECT first_name, last_name, email, phone FROM clients WHERE id = %s",
                (canonical_id,),
            ).fetchone()
            if row:
                email = row["email"]
                phone = row["phone"]
                client_name = f"{row['first_name']} {row['last_name']}".strip() or client_name

        return {
            "inv_id":           inv_id,
            "doc_number":       doc_number,
            "balance":          balance,
            "due_date":         due_date,
            "days_past_due":    days_past_due,
            "due_date_missing": due_date is None,
            "tier":             _tier(days_past_due),
            "qbo_customer_id":  qbo_customer_id,
            "canonical_id":     canonical_id,
            "client_name":      client_name,
            "email":            email,
            "phone":            phone,
        }

    # ── Asana task creation ────────────────────────────────────────────────────

    def _create_asana_task(self, record: dict, tool_ids: dict) -> bool:
        """
        Create an Asana task in Admin & Operations → To Do for the given invoice.

        Deduplication: queries existing open tasks in the project and skips
        creation if any task name already contains the invoice DocNumber (so the
        same invoice is not tracked twice across weekly runs).

        Returns True if a task was created, False if creation was skipped.
        """
        assignee = (
            _BOOKKEEPER_EMAIL if record["tier"] == 2 else _OFFICE_MANAGER_EMAIL
        )
        business_days = 1 if record["tier"] == 4 else 3
        due_date = _add_business_days(date.today(), business_days).isoformat()

        title = (
            f"Overdue invoice #{record['doc_number']}: {record['client_name']} - "
            f"{_fmt_amount(record['balance'])} - "
            f"{record['days_past_due']} days past due"
        )
        description = (
            f"Invoice #{record['doc_number']}\n"
            f"Original due date: {record['due_date']}\n"
            f"Amount due: {_fmt_amount(record['balance'])}\n"
            f"Client: {record['client_name']}\n"
            f"Email: {record['email']}\n"
            f"Phone: {record['phone']}\n\n"
            f"Please follow up on collection."
        )

        if self.dry_run:
            print(
                f"[DRY RUN] Would create Asana task (Tier {record['tier']}):\n"
                f"  Title   : {title}\n"
                f"  Assignee: {assignee}\n"
                f"  Due date: {due_date}\n"
                f"  Notes   : {description[:100]}..."
            )
            return True

        asana_client = self.clients("asana")
        tasks_api    = asana.TasksApi(asana_client)
        project_gid  = tool_ids["asana"]["projects"][_ASANA_PROJECT]
        section_gid  = tool_ids["asana"]["sections"][_ASANA_PROJECT][_ASANA_SECTION]

        # Deduplication: search open tasks for one already referencing this invoice number
        for existing in tasks_api.get_tasks_for_project(
            project_gid,
            {"opt_fields": "name,completed"},
        ):
            if not existing.get("completed") and record["doc_number"] in existing.get("name", ""):
                return False  # already tracked — skip

        # Create the task; retry without assignee if user is not in workspace
        task_body = {
            "data": {
                "name":     title,
                "notes":    description,
                "assignee": assignee,
                "due_on":   due_date,
                "projects": [project_gid],
            }
        }
        try:
            created = tasks_api.create_task(task_body, {})
        except asana.rest.ApiException as exc:
            if exc.status == 400 and task_body["data"].get("assignee"):
                print(
                    f"[WARN] Asana task '{title}': assignee '{assignee}' "
                    f"rejected (400) — retrying without assignee"
                )
                task_body["data"].pop("assignee")
                created = tasks_api.create_task(task_body, {})
            else:
                raise
        task_gid = created["gid"]

        # Move to the target section
        asana.SectionsApi(asana_client).add_task_for_section(
            section_gid,
            {"body": {"data": {"task": task_gid}}},
        )
        return True

    # ── Slack aging report ─────────────────────────────────────────────────────

    def _post_slack_report(
        self,
        all_enriched: list,
        tier2: list,
        tier3: list,
        tier4: list,
    ) -> None:
        """
        Build and post the weekly AR aging report to #operations.

        CRITICAL (Tier 4) and ESCALATED (Tier 3) invoices are listed
        individually. Tier 2 is summarised as a count + total.
        """
        lines = [":rotating_light: *Weekly AR Aging Report*\n"]

        if tier4:
            lines.append("*CRITICAL (90+ days):*")
            for r in tier4:
                lines.append(
                    f"  • {r['client_name']} - {_fmt_amount(r['balance'])} "
                    f"- {r['days_past_due']} days - Invoice #{r['doc_number']}"
                )
            lines.append("")

        if tier3:
            lines.append("*ESCALATED (61-90 days):*")
            for r in tier3:
                lines.append(
                    f"  • {r['client_name']} - {_fmt_amount(r['balance'])} "
                    f"- {r['days_past_due']} days - Invoice #{r['doc_number']}"
                )
            lines.append("")

        if tier2:
            watch_count = len(tier2)
            watch_sum   = sum(r["balance"] for r in tier2)
            lines.append(
                f"*WATCH (31-60 days):* "
                f"{watch_count} invoice{'s' if watch_count != 1 else ''} "
                f"totaling {_fmt_amount(watch_sum)}"
            )
            lines.append("")

        total_count = len(all_enriched)
        total_sum   = sum(r["balance"] for r in all_enriched)
        lines.append(
            f"*Total overdue:* {_fmt_amount(total_sum)} across "
            f"{total_count} invoice{'s' if total_count != 1 else ''}"
        )

        self.send_slack("operations", "\n".join(lines))

    def _post_all_clear(self, run_id: str, trigger_source: str) -> None:
        """Post the no-overdue-invoices message and log it."""
        try:
            self.send_slack(
                "operations",
                ":white_check_mark: No overdue invoices this week.",
            )
            self.log_action(
                run_id, "post_slack_aging_report",
                "slack:channel:operations",
                "success",
                trigger_source=trigger_source,
            )
        except Exception as exc:
            self.log_action(
                run_id, "post_slack_aging_report", None, "failed",
                error_message=str(exc), trigger_source=trigger_source,
            )

    # ── Maria DM (Tier 4) ──────────────────────────────────────────────────────

    def _dm_maria_tier4(self, tier4: list, tool_ids: dict) -> bool:
        """
        Send Maria a direct Slack message listing all Tier 4 (90+ day) invoices.

        Looks up her Slack user ID from tool_ids["slack"]["users"]["Maria Gonzalez"].
        If the ID is absent or null, logs a warning and returns False without
        raising, so the rest of the run is unaffected.

        Returns True if the DM was sent, False if it was skipped.
        """
        maria_user_id: Optional[str] = (
            tool_ids.get("slack", {})
            .get("users", {})
            .get("Maria Gonzalez")
        )

        if not maria_user_id:
            print(
                "[OverdueInvoiceEscalation] WARNING: Maria Gonzalez's Slack user ID "
                "is not available in tool_ids.json (slack.users.Maria Gonzalez). "
                "Skipping DM."
            )
            return False

        lines = [":rotating_light: *CRITICAL: Invoices 90+ days overdue*\n"]
        for r in tier4:
            lines.append(
                f"  • {r['client_name']} - {_fmt_amount(r['balance'])} "
                f"- {r['days_past_due']} days - Invoice #{r['doc_number']}"
            )
        text = "\n".join(lines)

        if self.dry_run:
            print(f"[DRY RUN] Would DM Maria (user_id={maria_user_id}):\n{text}")
            return True

        slack_client = self.clients("slack")
        slack_client.chat_postMessage(channel=maria_user_id, text=text)
        return True


# ─────────────────────────────────────────────────────────────────────────────
# Dry-run sanity test
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    sys.path.insert(0, _PROJECT_ROOT)

    from auth import get_client
    from database.schema import get_connection

    print("=" * 65)
    print("  OverdueInvoiceEscalation — dry-run sanity test")
    print("=" * 65)

    db = get_connection(os.path.join(_PROJECT_ROOT, "sparkle_shine.db"))

    automation = OverdueInvoiceEscalation(
        clients=get_client,
        db=db,
        dry_run=True,
    )

    print(
        f"\nSimulating 4 invoices across all tiers:\n"
        f"  Tier 1: 15 days  — watch only\n"
        f"  Tier 2: 45 days  — Asana task → bookkeeper\n"
        f"  Tier 3: 72 days  — Asana task → office manager + Slack\n"
        f"  Tier 4: 105 days — Asana task → office manager + Slack CRITICAL + DM\n"
    )

    automation.run()

    print()
    print("─" * 65)
    print("automation_log entries for this run:")
    print("─" * 65)
    rows = db.execute(
        """
        SELECT action_name, action_target, status, error_message
        FROM automation_log
        WHERE automation_name = 'OverdueInvoiceEscalation'
        ORDER BY id DESC
        LIMIT 10
        """
    ).fetchall()
    for row in reversed(rows):
        r      = dict(row)
        marker = (
            "OK " if r["status"] == "success"
            else ("---" if r["status"] == "skipped" else "ERR")
        )
        print(
            f"  [{marker}] {r['action_name']:<40} → {r['action_target'] or 'n/a'}"
        )
        if r["error_message"]:
            print(f"         note: {r['error_message']}")

    print()
    print("Dry-run complete. No external API calls were made.")
    db.close()
