"""
automations/job_completion_flow.py

Automation 2 — Job Completion Flow
Trigger: a job is marked complete in Jobber (from poll_jobber_completed_jobs).

Actions (each in its own try/except so failures are isolated):
  1. Create a QuickBooks invoice for the completed job
  2. Schedule a delayed (48h) Mailchimp review request via pending_actions
  3. Update the HubSpot contact: add completion note + update engagement properties
  4. Post a Slack summary to #operations with duration variance flagging
"""
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import requests

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from automations.base import BaseAutomation
from automations.utils.id_resolver import MappingNotFoundError, register_mapping

# ─────────────────────────────────────────────────────────────────────────────
# Service catalogue — mirrors config/business.py SERVICE_TYPES
# ─────────────────────────────────────────────────────────────────────────────

_SERVICE_CATALOGUE: dict = {
    "Standard Residential Clean": {"duration_minutes": 120, "base_price": 150.00},
    "Deep Clean":                 {"duration_minutes": 210, "base_price": 275.00},
    "Move-In/Move-Out Clean":     {"duration_minutes": 240, "base_price": 325.00},
    "Recurring Weekly":           {"duration_minutes": 120, "base_price": 135.00},
    "Recurring Biweekly":         {"duration_minutes": 120, "base_price": 150.00},
    "Recurring Monthly":          {"duration_minutes": 120, "base_price": 165.00},
    "Commercial Nightly Clean":   {"duration_minutes": 180, "base_price": None},
}

# QBO item IDs — mirrors tool_ids.json quickbooks.items
_QBO_ITEM_IDS: dict = {
    "Standard Residential Clean": "19",
    "Deep Clean":                 "20",
    "Move-In/Move-Out Clean":     "21",
    "Recurring Weekly":           "22",
    "Recurring Biweekly":         "23",
    "Recurring Monthly":          "24",
    "Commercial Nightly Clean":   "25",
}

_QBO_NET30_TERM_ID   = "3"
_FALLBACK_PRICE      = 150.00
_FALLBACK_ITEM_ID    = "19"   # Standard Residential Clean
_FALLBACK_DURATION   = 120    # minutes

# HubSpot note-to-contact association type (HUBSPOT_DEFINED category, id 202)
_HS_NOTE_TO_CONTACT_TYPE_ID = 202


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _load_tool_ids() -> dict:
    path = os.path.join(_PROJECT_ROOT, "config", "tool_ids.json")
    with open(path) as f:
        return json.load(f)


def _lookup_service(service_type: str) -> dict:
    """
    Return {duration_minutes, base_price, qbo_item_id} for the given service type.
    Tries exact match first, then case-insensitive partial match.
    Falls back to Standard Residential Clean values.
    """
    if service_type in _SERVICE_CATALOGUE:
        info = _SERVICE_CATALOGUE[service_type]
        return {
            "duration_minutes": info["duration_minutes"],
            "base_price":       info["base_price"] or _FALLBACK_PRICE,
            "qbo_item_id":      _QBO_ITEM_IDS.get(service_type, _FALLBACK_ITEM_ID),
        }

    lower = service_type.lower()
    for name, info in _SERVICE_CATALOGUE.items():
        if lower in name.lower() or name.lower() in lower:
            return {
                "duration_minutes": info["duration_minutes"],
                "base_price":       info["base_price"] or _FALLBACK_PRICE,
                "qbo_item_id":      _QBO_ITEM_IDS.get(name, _FALLBACK_ITEM_ID),
            }

    return {
        "duration_minutes": _FALLBACK_DURATION,
        "base_price":       _FALLBACK_PRICE,
        "qbo_item_id":      _FALLBACK_ITEM_ID,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Main class
# ─────────────────────────────────────────────────────────────────────────────

class JobCompletionFlow(BaseAutomation):
    """
    Orchestrates the four-step job-completion flow.

    Expects `self.clients` to be callable: clients("tool_name") → client/session.
    trigger_event must come from poll_jobber_completed_jobs() and contain:
        job_id, client_id (Jobber), service_type, duration_minutes,
        crew, completion_notes, is_recurring.
    Optional: completed_at (ISO date string; defaults to today if absent).
    """

    # ── Entry point ───────────────────────────────────────────────────────────

    def run(self, trigger_event: dict) -> None:
        run_id = self.generate_run_id()
        trigger_source = f"jobber:job:{trigger_event.get('job_id', 'unknown')}"

        ctx = self._build_context(trigger_event)

        if not ctx["canonical_id"]:
            self.log_action(
                run_id, "resolve_canonical_id", None, "failed",
                error_message=(
                    f"Jobber client '{trigger_event.get('client_id')}' has no "
                    "cross_tool_mapping entry — downstream actions will be skipped."
                ),
                trigger_source=trigger_source,
            )

        # ── Action 1: QuickBooks invoice ──────────────────────────────────────
        invoice_id     = None
        invoice_amount = 0.0
        payment_terms  = "due on receipt"
        try:
            invoice_id, invoice_amount, payment_terms = self._action_quickbooks_invoice(ctx)
            if not self.dry_run and invoice_id:
                # Generate a proper SS-INV-NNNN canonical ID
                row = self.db.execute(
                    "SELECT id FROM invoices ORDER BY id DESC LIMIT 1"
                ).fetchone()
                last_n = int(
                    (row["id"] if row else "SS-INV-0000").split("-")[-1]
                )
                inv_canonical_id = f"SS-INV-{last_n + 1:04d}"
                # Persist the invoice to SQLite (source of truth)
                inv_due_date = (
                    (ctx["completion_date"] + timedelta(days=30)).isoformat()
                    if ctx["is_commercial"]
                    else ctx["completion_date"].isoformat()
                )
                with self.db:
                    self.db.execute(
                        "INSERT INTO invoices "
                        "(id, client_id, amount, status, issue_date, due_date) "
                        "VALUES (%s, %s, %s, 'sent', %s, %s)",
                        (
                            inv_canonical_id,
                            ctx["canonical_id"],
                            invoice_amount,
                            ctx["completion_date"].isoformat(),
                            inv_due_date,
                        ),
                    )
                register_mapping(
                    self.db,
                    inv_canonical_id,
                    "quickbooks",
                    invoice_id,
                )
            self.log_action(
                run_id, "create_quickbooks_invoice",
                f"quickbooks:invoice:{invoice_id}",
                "success",
                trigger_source=trigger_source,
                trigger_detail={"job_id": ctx["job_id"], "amount": invoice_amount},
            )
        except Exception as exc:
            self.log_action(
                run_id, "create_quickbooks_invoice", None, "failed",
                error_message=str(exc), trigger_source=trigger_source,
            )

        # ── Action 2: Schedule delayed review request ─────────────────────────
        try:
            self._action_schedule_review_request(ctx)
            self.log_action(
                run_id, "schedule_review_request",
                "pending_actions:send_review_request:+48h",
                "success",
                trigger_source=trigger_source,
            )
        except Exception as exc:
            self.log_action(
                run_id, "schedule_review_request", None, "failed",
                error_message=str(exc), trigger_source=trigger_source,
            )

        # ── Action 3: HubSpot engagement ─────────────────────────────────────
        try:
            self._action_hubspot_engagement(ctx)
            self.log_action(
                run_id, "update_hubspot_engagement",
                f"hubspot:contact:{ctx['hs_contact_id'] or 'unknown'}",
                "success",
                trigger_source=trigger_source,
            )
        except Exception as exc:
            self.log_action(
                run_id, "update_hubspot_engagement", None, "failed",
                error_message=str(exc), trigger_source=trigger_source,
            )

        # ── Action 4: Slack summary ───────────────────────────────────────────
        try:
            self._action_slack_summary(ctx, invoice_amount, payment_terms)
            self.log_action(
                run_id, "post_slack_summary",
                "slack:channel:operations",
                "success",
                trigger_source=trigger_source,
            )
        except Exception as exc:
            self.log_action(
                run_id, "post_slack_summary", None, "failed",
                error_message=str(exc), trigger_source=trigger_source,
            )

    # ── Context builder ───────────────────────────────────────────────────────

    def _build_context(self, event: dict) -> dict:
        """
        Resolve cross-tool IDs and normalize the trigger event.
        Missing mappings are stored as None; each action decides how to handle them.
        """
        job_id           = str(event.get("job_id", ""))
        jobber_client_id = str(event.get("client_id", ""))
        service_type     = event.get("service_type") or "Standard Residential Clean"

        # Parse completion date (poll_jobber_completed_jobs doesn't include it,
        # so callers may inject "completed_at"; fall back to today)
        raw_date = event.get("completed_at") or date.today().isoformat()
        try:
            completion_date = date.fromisoformat(str(raw_date)[:10])
        except (ValueError, TypeError):
            completion_date = date.today()

        # Resolve canonical ID from the Jobber client ID
        canonical_id: Optional[str] = None
        try:
            canonical_id = self.reverse_resolve_id(jobber_client_id, "jobber")
        except MappingNotFoundError:
            pass

        # Resolve downstream tool IDs
        qbo_customer_id: Optional[str] = None
        hs_contact_id:   Optional[str] = None
        if canonical_id:
            try:
                qbo_customer_id = self.resolve_id(canonical_id, "quickbooks")
            except MappingNotFoundError:
                pass
            try:
                hs_contact_id = self.resolve_id(canonical_id, "hubspot")
            except MappingNotFoundError:
                pass

        # Look up client name and email from the clients table
        client_name  = "Unknown Client"
        client_email = ""
        if canonical_id:
            row = self.db.execute(
                "SELECT first_name, last_name, email FROM clients WHERE id = %s",
                (canonical_id,),
            ).fetchone()
            if row:
                client_name  = f"{row['first_name']} {row['last_name']}".strip() or client_name
                client_email = row["email"] or ""

        return {
            "job_id":           job_id,
            "jobber_client_id": jobber_client_id,
            "canonical_id":     canonical_id,
            "qbo_customer_id":  qbo_customer_id,
            "hs_contact_id":    hs_contact_id,
            "client_name":      client_name,
            "client_email":     client_email,
            "service_type":     service_type,
            "service_info":     _lookup_service(service_type),
            "duration_minutes": event.get("duration_minutes"),
            "crew":             event.get("crew"),
            "completion_notes": event.get("completion_notes") or "",
            "is_recurring":     bool(event.get("is_recurring", False)),
            "completion_date":  completion_date,
            "is_commercial":    "commercial" in service_type.lower(),
        }

    # ── Action 1: QuickBooks invoice ──────────────────────────────────────────

    def _action_quickbooks_invoice(self, ctx: dict) -> tuple:
        """
        POST a new invoice to QuickBooks for the completed job.
        Returns (invoice_id: str, amount: float, payment_terms_label: str).
        """
        service_info  = ctx["service_info"]
        amount        = service_info["base_price"]
        qbo_item_id   = service_info["qbo_item_id"]
        is_commercial = ctx["is_commercial"]
        completion_date = ctx["completion_date"]
        due_date        = (
            (completion_date + timedelta(days=30)).isoformat()
            if is_commercial
            else completion_date.isoformat()
        )
        payment_terms = "Net 30" if is_commercial else "due on receipt"

        if self.dry_run:
            print(
                f"[DRY RUN] Would create QBO invoice for {ctx['client_name']}: "
                f"${amount:.2f} ({ctx['service_type']}), {payment_terms}, "
                f"due {due_date}"
            )
            return ("dry-run-qbo-invoice-id", amount, payment_terms)

        if not ctx["qbo_customer_id"]:
            raise MappingNotFoundError(
                f"No QuickBooks customer mapping for Jobber client "
                f"'{ctx['jobber_client_id']}' "
                f"(canonical: {ctx['canonical_id']})"
            )

        from auth.quickbooks_auth import get_base_url
        headers  = self.clients("quickbooks")
        base_url = get_base_url()

        body: dict = {
            "CustomerRef": {"value": ctx["qbo_customer_id"]},
            "TxnDate":     completion_date.isoformat(),
            "DueDate":     due_date,
            "Line": [
                {
                    "DetailType": "SalesItemLineDetail",
                    "Amount":     amount,
                    "SalesItemLineDetail": {
                        "ItemRef": {"value": qbo_item_id}
                    },
                }
            ],
            "PrivateNote": f"SS-JOB: {ctx['job_id']}",
        }
        if is_commercial:
            body["SalesTermRef"] = {"value": _QBO_NET30_TERM_ID}

        resp = requests.post(
            f"{base_url}/invoice",
            headers=headers,
            json=body,
            params={"minorversion": "65"},
            timeout=15,
        )
        resp.raise_for_status()
        data    = resp.json()
        invoice = data.get("Invoice") or data.get("invoice")
        if not invoice:
            raise RuntimeError(
                f"QBO invoice create returned unexpected body: {data}"
            )

        return (str(invoice.get("Id", "")), amount, payment_terms)

    # ── Action 2: Schedule delayed review request ─────────────────────────────

    def _action_schedule_review_request(self, ctx: dict) -> None:
        """
        Insert a pending_actions row so the runner sends a review-request
        email via Mailchimp 48 hours after job completion.
        No Mailchimp API call is made here.
        """
        if self.dry_run:
            print(
                f"[DRY RUN] Would schedule 'send_review_request' for "
                f"{ctx['client_email'] or ctx['client_name']} in 48h "
                f"(job {ctx['job_id']})"
            )
            return

        self.schedule_delayed_action(
            action_name="send_review_request",
            trigger_context_dict={
                "canonical_id":  ctx["canonical_id"],
                "client_email":  ctx["client_email"],
                "client_name":   ctx["client_name"],
                "service_type":  ctx["service_type"],
                "job_date":      ctx["completion_date"].isoformat(),
                "job_id":        ctx["job_id"],
            },
            delay_hours=48,
        )

    # ── Action 3: HubSpot engagement ─────────────────────────────────────────

    def _action_hubspot_engagement(self, ctx: dict) -> None:
        """
        1. Create a completion note on the HubSpot contact (with inline association).
        2. Read the current total_services_completed and outstanding_balance values.
        3. PATCH the contact with last_service_date, incremented count, and
           outstanding_balance += invoice amount (so payment_received can decrement it).
        """
        crew_str     = ctx["crew"] or "unassigned"
        duration_str = (
            f"{ctx['duration_minutes']} min" if ctx["duration_minutes"] else "N/A"
        )
        note_body = (
            f"Service completed: {ctx['service_type']} on {ctx['completion_date']}. "
            f"Crew: {crew_str}. Duration: {duration_str}."
        )

        invoice_amount = ctx["service_info"]["base_price"]

        if self.dry_run:
            print(
                f"[DRY RUN] Would create HubSpot note for contact "
                f"{ctx['hs_contact_id'] or 'unknown'}: {note_body}"
            )
            print(
                f"[DRY RUN] Would PATCH HubSpot contact "
                f"{ctx['hs_contact_id'] or 'unknown'}: "
                f"last_service_date={ctx['completion_date']}, "
                f"total_services_completed+=1, "
                f"outstanding_balance+={invoice_amount:.2f}"
            )
            return

        if not ctx["hs_contact_id"]:
            raise MappingNotFoundError(
                f"No HubSpot contact mapping for canonical ID {ctx['canonical_id']}"
            )

        from hubspot.crm.objects.notes import SimplePublicObjectInputForCreate
        from hubspot.crm.contacts import SimplePublicObjectInput
        from automations.utils.hubspot_write_lock import contact_write_lock

        hs_client  = self.clients("hubspot")
        contact_id = ctx["hs_contact_id"]

        # Timestamp: midnight UTC on the completion date
        note_timestamp = (
            datetime.combine(ctx["completion_date"], datetime.min.time())
            .replace(tzinfo=timezone.utc)
            .strftime("%Y-%m-%dT%H:%M:%SZ")
        )

        # 1. Create note with inline association to the contact
        note_input = SimplePublicObjectInputForCreate(
            properties={
                "hs_note_body": note_body,
                "hs_timestamp": note_timestamp,
            },
            associations=[
                {
                    "to": {"id": contact_id},
                    "types": [
                        {
                            "associationCategory": "HUBSPOT_DEFINED",
                            "associationTypeId":   _HS_NOTE_TO_CONTACT_TYPE_ID,
                        }
                    ],
                }
            ],
        )
        hs_client.crm.objects.notes.basic_api.create(note_input, _request_timeout=30)

        # 2. Read-modify-write counter properties under a per-contact lock.
        # HubSpot has no atomic increment: without serialization, two concurrent
        # runner invocations processing events for the same contact would both
        # read the same counter value before either writes, silently dropping
        # one increment. The file lock ensures only one process at a time
        # executes this block for a given contact.
        with contact_write_lock(contact_id):
            contact = hs_client.crm.contacts.basic_api.get_by_id(
                contact_id,
                properties=["total_services_completed", "outstanding_balance"],
                _request_timeout=30,
            )
            props = contact.properties or {}
            current_count       = int(float(props.get("total_services_completed") or 0))
            current_outstanding = float(props.get("outstanding_balance") or 0.0)

            # 3. Update engagement properties and outstanding_balance
            new_outstanding = round(current_outstanding + invoice_amount, 2)
            hs_client.crm.contacts.basic_api.update(
                contact_id,
                SimplePublicObjectInput(
                    properties={
                        "last_service_date":          ctx["completion_date"].isoformat(),
                        "total_services_completed":   str(current_count + 1),
                        "outstanding_balance":        str(new_outstanding),
                    }
                ),
                _request_timeout=30,
            )

    # ── Action 4: Slack summary ───────────────────────────────────────────────

    def _action_slack_summary(
        self, ctx: dict, invoice_amount: float, payment_terms: str
    ) -> None:
        """
        Post a completion summary to #operations.
        Appends a warning line if duration variance exceeds ±20%.
        """
        actual_min   = ctx["duration_minutes"]
        expected_min = ctx["service_info"]["duration_minutes"]

        if actual_min and expected_min:
            variance_pct = (actual_min - expected_min) / expected_min * 100
            sign         = "+" if variance_pct > 0 else ""
            direction    = "over" if variance_pct > 0 else "under"
            variance_text = f"{sign}{variance_pct:.0f}% {direction}"
        else:
            variance_pct  = None
            variance_text = "N/A"

        duration_display = f"{actual_min} min" if actual_min else "N/A"
        crew_display     = ctx["crew"] or "unassigned"
        amount_display   = f"${invoice_amount:,.2f}" if invoice_amount else "TBD"

        text = (
            f":white_check_mark: Job Completed: {ctx['service_type']} "
            f"for {ctx['client_name']}\n"
            f"Crew: {crew_display} | Duration: {duration_display} ({variance_text})\n"
            f"Invoice created: {amount_display} ({payment_terms})"
        )

        if variance_pct is not None and abs(variance_pct) > 20:
            text += (
                f"\n:warning: Duration variance: {actual_min} min vs "
                f"{expected_min} min expected "
                f"({'+' if variance_pct > 0 else ''}{variance_pct:.0f}% "
                f"{'over' if variance_pct > 0 else 'under'})"
            )

        self.send_slack("operations", text)


# ─────────────────────────────────────────────────────────────────────────────
# Dry-run sanity test
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    sys.path.insert(0, _PROJECT_ROOT)

    from auth import get_client
    from database.schema import get_connection

    print("=" * 65)
    print("  JobCompletionFlow — dry-run sanity test")
    print("=" * 65)

    db = get_connection(os.path.join(_PROJECT_ROOT, "sparkle_shine.db"))

    # 150 min actual vs 120 min expected = +25% → triggers the >20% warning
    fake_event = {
        "job_id":           "DRY-RUN-JOB-001",
        "client_id":        "dry-run-jobber-client-id",
        "service_type":     "Recurring Biweekly",
        "duration_minutes": 150,
        "crew":             "Claudia Ramirez, Leticia Morales",
        "completion_notes": "Dry-run test completion",
        "is_recurring":     True,
        "completed_at":     date.today().isoformat(),
    }

    automation = JobCompletionFlow(
        clients=get_client,
        db=db,
        dry_run=True,
    )

    print(
        f"\nTrigger event: job_id={fake_event['job_id']}, "
        f"service={fake_event['service_type']}, "
        f"duration={fake_event['duration_minutes']} min "
        f"(expected 120 min → +25% variance)"
    )
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
        WHERE automation_name = 'JobCompletionFlow'
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
        print(
            f"  [{marker}] {r['action_name']:<38} → {r['action_target'] or 'n/a'}"
        )
        if r["error_message"]:
            print(f"         error: {r['error_message']}")

    print()
    print("Dry-run complete. No external API calls were made.")
    db.close()
