"""
automations/payment_received.py

Automation 3 — Payment Received
Trigger: a payment is recorded in QuickBooks (from poll_quickbooks_payments).

Actions (each in its own try/except so failures are isolated):
  1. Update Pipedrive deal with a "payment received" activity note
     — logged as "skipped" (not "failed") if no Pipedrive mapping exists
  2. Update HubSpot contact: last_payment_date, total_payments_received,
     outstanding_balance (read → increment/decrement → write)
  3. Post a Slack notification to #operations; flag late commercial payments
     by comparing payment date to QBO invoice due date
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
from automations.utils.id_resolver import MappingNotFoundError


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _fmt_amount(amount: float) -> str:
    return f"${amount:,.2f}"


def _parse_date(raw: str) -> Optional[date]:
    """Parse an ISO-8601 date string (YYYY-MM-DD or longer). Returns None on failure."""
    if not raw:
        return None
    try:
        return date.fromisoformat(str(raw)[:10])
    except (ValueError, TypeError):
        return None


def _fetch_qbo_invoice(base_url: str, headers: dict, invoice_id: str) -> dict:
    """
    GET /invoice/{invoice_id} from QuickBooks.
    Returns the Invoice dict, or {} on any error.
    """
    if not invoice_id:
        return {}
    try:
        resp = _requests.get(
            f"{base_url}/invoice/{invoice_id}",
            headers=headers,
            params={"minorversion": "65"},
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json().get("Invoice") or {}
    except Exception:
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# Main class
# ─────────────────────────────────────────────────────────────────────────────

class PaymentReceived(BaseAutomation):
    """
    Orchestrates the three-step payment-received flow.

    Expects `self.clients` to be callable: clients("tool_name") → client/session.
    trigger_event must come from poll_quickbooks_payments() and contain:
        payment_id, amount, date, method, invoice_id, customer_id.
    """

    # ── Entry point ───────────────────────────────────────────────────────────

    def run(self, trigger_event: dict) -> None:
        run_id       = self.generate_run_id()
        trigger_source = (
            f"quickbooks:payment:{trigger_event.get('payment_id', 'unknown')}"
        )

        # ── Idempotency guard: skip if this payment was already processed ─────
        if self._already_processed(trigger_source):
            return

        ctx = self._build_context(trigger_event)

        # ── Action 1: Pipedrive activity ──────────────────────────────────────
        try:
            skipped = self._action_pipedrive_activity(ctx)
            self.log_action(
                run_id, "update_pipedrive_deal",
                f"pipedrive:deal:{ctx['pipedrive_deal_id'] or 'none'}",
                "skipped" if skipped else "success",
                error_message="No Pipedrive deal mapping" if skipped else None,
                trigger_source=trigger_source,
                trigger_detail={"payment_id": ctx["payment_id"], "amount": ctx["amount"]},
            )
        except Exception as exc:
            self.log_action(
                run_id, "update_pipedrive_deal", None, "failed",
                error_message=str(exc), trigger_source=trigger_source,
            )

        # ── Action 2: HubSpot financial properties ────────────────────────────
        # Fallback uses the DB-derived outstanding balance minus this payment.
        # _action_hubspot_financials will replace this with the live HubSpot value.
        new_outstanding = max(0.0, ctx["hs_outstanding"] - ctx["amount"])
        try:
            new_outstanding = self._action_hubspot_financials(ctx)
            self.log_action(
                run_id, "update_hubspot_financials",
                f"hubspot:contact:{ctx['hs_contact_id'] or 'unknown'}",
                "success",
                trigger_source=trigger_source,
            )
        except Exception as exc:
            self.log_action(
                run_id, "update_hubspot_financials", None, "failed",
                error_message=str(exc), trigger_source=trigger_source,
            )

        # ── Action 3: Slack notification ──────────────────────────────────────
        # Guard: skip if a Slack notification was already posted for this payment
        # (protects against partial-run retries where Actions 1-2 logged but
        # the run_id changed on the second attempt).
        already_notified = self.db.execute(
            """
            SELECT 1 FROM automation_log
            WHERE trigger_source = %s
              AND automation_name = 'PaymentReceived'
              AND action_name = 'post_slack_notification'
              AND status = 'success'
            LIMIT 1
            """,
            (trigger_source,),
        ).fetchone()
        if already_notified is None:
            try:
                self._action_slack_notification(ctx, new_outstanding)
                self.log_action(
                    run_id, "post_slack_notification",
                    "slack:channel:operations",
                    "success",
                    trigger_source=trigger_source,
                )
            except Exception as exc:
                self.log_action(
                    run_id, "post_slack_notification", None, "failed",
                    error_message=str(exc), trigger_source=trigger_source,
                )

    # ── Idempotency ────────────────────────────────────────────────────────────

    def _already_processed(self, trigger_source: str) -> bool:
        """Return True if automation_log already has any record for this trigger.

        Checks for any status (not just 'success') to prevent re-entry when a
        prior run is still in progress or partially completed.
        """
        row = self.db.execute(
            """
            SELECT 1 FROM automation_log
            WHERE trigger_source = %s
              AND automation_name = 'PaymentReceived'
            LIMIT 1
            """,
            (trigger_source,),
        ).fetchone()
        if row is not None:
            logger = __import__("logging").getLogger("automations")
            logger.info("Skipping duplicate payment: %s", trigger_source)
            return True
        return False

    # ── Context builder ───────────────────────────────────────────────────────

    def _build_context(self, event: dict) -> dict:
        """
        Resolve cross-tool IDs and normalise the trigger event.
        Does not make external API calls — ID resolution is DB-only.
        Missing mappings are stored as None; each action handles them.
        """
        qbo_customer_id = str(event.get("customer_id", ""))
        payment_date    = _parse_date(event.get("date") or "")

        # Resolve canonical ID from QBO customer ID
        canonical_id: Optional[str] = None
        try:
            canonical_id = self.reverse_resolve_id(
                qbo_customer_id, "quickbooks", entity_type="CLIENT"
            )
        except MappingNotFoundError:
            pass

        # Resolve downstream tool IDs
        pipedrive_deal_id: Optional[str] = None
        hs_contact_id:     Optional[str] = None
        if canonical_id:
            try:
                pipedrive_deal_id = self.resolve_id(canonical_id, "pipedrive")
            except MappingNotFoundError:
                pass
            try:
                hs_contact_id = self.resolve_id(canonical_id, "hubspot")
            except MappingNotFoundError:
                pass

        # Client info from DB
        client_name  = "Unknown Client"
        client_type  = "residential"
        if canonical_id:
            row = self.db.execute(
                "SELECT first_name, last_name, client_type FROM clients WHERE id = %s",
                (canonical_id,),
            ).fetchone()
            if row:
                client_name = f"{row['first_name']} {row['last_name']}".strip() or client_name
                client_type = (row["client_type"] or "residential").lower()

        # Compute unpaid invoice total from SQLite as a fallback for when the
        # HubSpot action fails. This prevents the Slack notification from always
        # showing $0.00 outstanding on HubSpot errors.
        db_outstanding = 0.0
        if canonical_id:
            row = self.db.execute(
                "SELECT COALESCE(SUM(amount), 0.0) AS total FROM invoices "
                "WHERE client_id = %s AND status != 'paid'",
                (canonical_id,),
            ).fetchone()
            if row:
                db_outstanding = float(row["total"])

        return {
            "payment_id":       str(event.get("payment_id", "")),
            "amount":           float(event.get("amount") or 0),
            "payment_date":     payment_date or date.today(),
            "method":           event.get("method") or "Unknown",
            "invoice_id":       str(event.get("invoice_id") or ""),
            "qbo_customer_id":  qbo_customer_id,
            "canonical_id":     canonical_id,
            "pipedrive_deal_id": pipedrive_deal_id,
            "hs_contact_id":    hs_contact_id,
            "client_name":      client_name,
            "client_type":      client_type,
            # Fallback outstanding balance from local SQLite (unpaid invoices).
            # _action_hubspot_financials will replace this with the live HubSpot value.
            "hs_outstanding":   db_outstanding,
        }

    # ── Action 1: Pipedrive deal activity ─────────────────────────────────────

    def _action_pipedrive_activity(self, ctx: dict) -> bool:
        """
        Create a 'done' activity on the Pipedrive deal for this payment.

        Returns True if the action was skipped (no Pipedrive mapping),
        False if it executed successfully.
        Raises on unexpected errors.
        """
        if not ctx["pipedrive_deal_id"]:
            # Not every client originates from Pipedrive — this is expected.
            return True   # skipped

        invoice_label = f"#{ctx['invoice_id']}" if ctx["invoice_id"] else "N/A"
        subject       = f"Payment received: {_fmt_amount(ctx['amount'])}"
        note          = (
            f"Payment of {_fmt_amount(ctx['amount'])} received on "
            f"{ctx['payment_date']} via {ctx['method']}. "
            f"Invoice {invoice_label}."
        )

        if self.dry_run:
            print(
                f"[DRY RUN] Would create Pipedrive activity on deal "
                f"{ctx['pipedrive_deal_id']}: {subject}"
            )
            return False

        session = self.clients("pipedrive")
        base    = session.base_url.rstrip("/")
        if not any(seg in base for seg in ("/v1", "/v2")):
            base = f"{base}/v1"

        try:
            deal_id = int(ctx["pipedrive_deal_id"])
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                f"Invalid Pipedrive deal ID: {ctx['pipedrive_deal_id']!r}"
            ) from exc

        body = {
            "subject":  subject,
            "type":     "task",
            "note":     note,
            "deal_id":  deal_id,
            "done":     1,
        }
        resp = session.post(f"{base}/activities", json=body, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if not data.get("success"):
            raise RuntimeError(
                f"Pipedrive activity create failed: {data.get('error', data)}"
            )
        return False

    # ── Action 2: HubSpot financial properties ────────────────────────────────

    def _action_hubspot_financials(self, ctx: dict) -> float:
        """
        Read the current HubSpot contact values, then PATCH:
          last_payment_date, total_payments_received (+=1),
          outstanding_balance (-= payment amount, floor 0).

        Returns the new outstanding_balance value.
        Raises MappingNotFoundError if no HubSpot contact is mapped.
        """
        if self.dry_run:
            estimated_remaining = max(0.0, ctx.get("hs_outstanding", 0.0) - ctx["amount"])
            print(
                f"[DRY RUN] Would PATCH HubSpot contact "
                f"{ctx['hs_contact_id'] or 'unknown'}: "
                f"last_payment_date={ctx['payment_date']}, "
                f"total_payments_received+=1, "
                f"outstanding_balance-={_fmt_amount(ctx['amount'])}"
            )
            return estimated_remaining

        if not ctx["hs_contact_id"]:
            raise MappingNotFoundError(
                f"No HubSpot contact mapping for canonical ID {ctx['canonical_id']}"
            )

        from hubspot.crm.contacts import SimplePublicObjectInput
        from automations.utils.hubspot_write_lock import contact_write_lock

        hs_client  = self.clients("hubspot")
        contact_id = ctx["hs_contact_id"]

        # Read-modify-write counter properties under a per-contact lock.
        # HubSpot has no atomic increment: without serialization, two concurrent
        # runner invocations processing events for the same contact would both
        # read the same counter value before either writes, silently dropping
        # one increment. The file lock ensures only one process at a time
        # executes this block for a given contact.
        with contact_write_lock(contact_id):
            contact = hs_client.crm.contacts.basic_api.get_by_id(
                contact_id,
                properties=["total_payments_received", "outstanding_balance"],
                _request_timeout=30,
            )
            props = contact.properties or {}
            current_payments    = int(props.get("total_payments_received") or 0)
            current_outstanding = float(props.get("outstanding_balance") or 0.0)

            new_payments    = current_payments + 1
            new_outstanding = max(0.0, current_outstanding - ctx["amount"])

            hs_client.crm.contacts.basic_api.update(
                contact_id,
                SimplePublicObjectInput(
                    properties={
                        "last_payment_date":        ctx["payment_date"].isoformat(),
                        "total_payments_received":  str(new_payments),
                        "outstanding_balance":      str(round(new_outstanding, 2)),
                    }
                ),
                _request_timeout=30,
            )
        return new_outstanding

    # ── Action 3: Slack notification ──────────────────────────────────────────

    def _action_slack_notification(
        self, ctx: dict, new_outstanding: float
    ) -> None:
        """
        Post a payment summary to #operations.

        For commercial clients, fetches the linked QBO invoice to check
        whether the payment arrived after the due date (late payment flag).
        """
        invoice_label = f"#{ctx['invoice_id']}" if ctx["invoice_id"] else "N/A"

        text = (
            f":moneybag: Payment Received: {_fmt_amount(ctx['amount'])} "
            f"from {ctx['client_name']}\n"
            f"Method: {ctx['method']} | Invoice: {invoice_label}\n"
            f"Outstanding balance: {_fmt_amount(new_outstanding)}"
        )

        # Late-payment check — commercial clients only
        if ctx["client_type"] == "commercial" and ctx["invoice_id"]:
            late_line = self._late_payment_line(ctx)
            if late_line:
                text += f"\n{late_line}"

        if self.dry_run:
            print(f"[DRY RUN] Would post to #operations:\n{text}")
            return

        self.send_slack("operations", text)

    def _late_payment_line(self, ctx: dict) -> Optional[str]:
        """
        Query QBO for the invoice due date. If payment_date > due_date,
        return a formatted warning string; otherwise return None.
        Silently swallows fetch errors (Slack message still posts without the line).
        """
        if self.dry_run:
            # In dry_run we still attempt the logic but skip the API call
            print(
                f"[DRY RUN] Would fetch QBO invoice {ctx['invoice_id']} "
                f"to check due date for late-payment detection"
            )
            return None

        try:
            from auth.quickbooks_auth import get_base_url
            headers  = self.clients("quickbooks")
            base_url = get_base_url()
            invoice  = _fetch_qbo_invoice(base_url, headers, ctx["invoice_id"])
        except Exception:
            return None

        due_date = _parse_date(invoice.get("DueDate") or "")
        if not due_date:
            return None

        days_late = (ctx["payment_date"] - due_date).days
        if days_late > 0:
            return (
                f":clock3: Payment was {days_late} day{'s' if days_late != 1 else ''} "
                f"past due on Net-30 terms"
            )
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Dry-run sanity test
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    sys.path.insert(0, _PROJECT_ROOT)

    from auth import get_client
    from database.schema import get_connection

    print("=" * 65)
    print("  PaymentReceived — dry-run sanity test")
    print("=" * 65)

    db = get_connection(os.path.join(_PROJECT_ROOT, "sparkle_shine.db"))

    # Simulate a payment 35 days after an invoice would be due (for late-pay logic)
    fake_event = {
        "payment_id":  "DRY-RUN-PMT-001",
        "amount":      150.00,
        "date":        date.today().isoformat(),
        "method":      "Credit Card",
        "invoice_id":  "DRY-RUN-INV-001",
        "customer_id": "dry-run-qbo-customer-id",
    }

    automation = PaymentReceived(
        clients=get_client,
        db=db,
        dry_run=True,
    )

    print(
        f"\nTrigger event: payment_id={fake_event['payment_id']}, "
        f"amount={_fmt_amount(fake_event['amount'])}, "
        f"method={fake_event['method']}, "
        f"invoice_id={fake_event['invoice_id']}"
    )
    print()

    automation.run(fake_event)

    print()
    print("─" * 65)
    print("automation_log entries for this run:")
    print("─" * 65)
    rows = db.execute(
        """
        SELECT action_name, action_target, status, error_message
        FROM automation_log
        WHERE automation_name = 'PaymentReceived'
        ORDER BY id DESC
        LIMIT 6
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
            print(f"         note: {r['error_message']}")

    print()
    print("Dry-run complete. No external API calls were made.")
    db.close()
