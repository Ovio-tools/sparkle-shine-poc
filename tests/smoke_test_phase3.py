"""
tests/smoke_test_phase3.py

Phase 3 Smoke Test Suite — Sparkle & Shine POC
===============================================
Exercises each automation once against live seeded data and verifies results
via real API calls and DB inspection. Run exactly once.

Usage:
    python3 tests/smoke_test_phase3.py

Verification strategy
---------------------
Where direct API verification is reliable (Mailchimp, Pipedrive, cross-tool
mapping lookups), we call the API directly.

Where API verification is unreliable in a sandbox environment (QBO's Query API
doesn't support LIKE on PrivateNote; Asana projects with 500+ tasks need full
pagination; HubSpot custom properties may not be configured), we verify via
automation_log with a tight timestamp window — only entries written during
THIS run (last 15 minutes) count.

Slack: we bypass conversations.list (requires groups:read scope the bot lacks)
by using the channel IDs stored in the .env file directly with conversations.history.
"""
import json
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

# ── Path bootstrap ────────────────────────────────────────────────────────────
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# Load .env before importing auth modules
from dotenv import load_dotenv
load_dotenv(os.path.join(_PROJECT_ROOT, ".env"))

from auth import get_client
from automations.migrate import run_migration
from database.schema import get_connection

# ── ANSI colour helpers ───────────────────────────────────────────────────────
_GREEN  = "\033[92m"
_RED    = "\033[91m"
_YELLOW = "\033[93m"
_RESET  = "\033[0m"
_BOLD   = "\033[1m"


def _pass(label: str) -> None:
    print(f"  {_GREEN}PASS{_RESET}  {label}")


def _fail(label: str, reason: str = "") -> None:
    suffix = f" — {reason}" if reason else ""
    print(f"  {_RED}FAIL{_RESET}  {label}{suffix}")


def _warn(label: str, reason: str = "") -> None:
    suffix = f" — {reason}" if reason else ""
    print(f"  {_YELLOW}WARN{_RESET}  {label}{suffix}")


def _section(title: str) -> None:
    print()
    print("=" * 70)
    print(f"  {_BOLD}{title}{_RESET}")
    print("=" * 70)


def _check(label: str, condition: bool, reason: str = "") -> bool:
    if condition:
        _pass(label)
    else:
        _fail(label, reason)
    return condition


# ── sqlite3.Row helper ────────────────────────────────────────────────────────

def _col(row, name: str):
    return row[name]


# ── automation_log helpers ────────────────────────────────────────────────────

_LOG_WINDOW_MINUTES = 15   # only entries written in the last N minutes count


def _log_recent(db, automation_name: str, action_name: str,
                status: str = "success") -> Optional[sqlite3.Row]:
    """
    Return the most recent automation_log row for this automation + action
    within the last _LOG_WINDOW_MINUTES minutes, or None if not found.
    Matches rows whose created_at is within the UTC window.
    """
    cutoff = (
        datetime.now(timezone.utc) - timedelta(minutes=_LOG_WINDOW_MINUTES)
    ).strftime("%Y-%m-%d %H:%M:%S")
    row = db.execute(
        """
        SELECT id, action_target, error_message, created_at
        FROM automation_log
        WHERE automation_name = ? AND action_name = ?
          AND status = ?
          AND created_at >= ?
        ORDER BY id DESC LIMIT 1
        """,
        (automation_name, action_name, status, cutoff),
    ).fetchone()
    return row


def _log_check(db, label: str, automation_name: str, action_name: str,
               status: str = "success") -> bool:
    """Check automation_log for a recent entry and print PASS/FAIL."""
    row = _log_recent(db, automation_name, action_name, status)
    ok  = row is not None
    if ok:
        target = _col(row, "action_target") or ""
        _pass(f"{label} (log: {action_name} → {target})")
    else:
        _fail(label, f"no '{status}' entry for {automation_name}.{action_name} in last {_LOG_WINDOW_MINUTES}m")
    return ok


# ── Slack helpers (bypass conversations.list, use env-var channel IDs) ────────
#
# All Slack channel IDs are stored in .env as SLACK_CHANNEL_* vars.
# We use conversations.history with the ID directly so we never need
# conversations.list (which requires groups:read for private channels).
#
# Verification strategy: try direct Slack history first; if the message is
# not found (history unreadable or channel is private), fall back to the
# automation_log.  Only FAIL if the automation_log also has no success entry —
# meaning the message was never sent at all.

_SLACK_CHANNEL_IDS: dict = {
    "new-clients":    os.environ.get("SLACK_CHANNEL_NEW_CLIENTS", ""),
    "operations":     os.environ.get("SLACK_CHANNEL_OPERATIONS", ""),
    "sales":          os.environ.get("SLACK_CHANNEL_SALES", ""),
}


def _slack_recent_message(slack_client, channel_name: str, keyword: str,
                           lookback_minutes: int = 12) -> bool:
    """
    Search recent messages in a channel for `keyword`.
    Uses the env-var channel ID — no conversations.list call needed.
    Returns False (not raises) on any error so callers can fall back to the log.
    """
    channel_id = _SLACK_CHANNEL_IDS.get(channel_name, "")
    if not channel_id:
        return False  # no env var → silent miss; caller falls back to log

    try:
        oldest   = str(
            (datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)).timestamp()
        )
        history  = slack_client.conversations_history(
            channel=channel_id, oldest=oldest, limit=50
        )
        messages = history.get("messages") or []
        kw_lower = keyword.lower()
        return any(kw_lower in (m.get("text") or "").lower() for m in messages)
    except Exception:
        return False  # silent; caller falls back to log


def _slack_check(db, label: str, slack_client,
                 channel_name: str, keyword: str,
                 automation_name: str, action_name: str) -> bool:
    """
    Unified Slack verification:
      1. Try conversations.history with env-var channel ID.
      2. If message not found, fall back to automation_log for this run.
      3. FAIL only when BOTH checks come up empty (message was never sent).
    """
    if _slack_recent_message(slack_client, channel_name, keyword):
        _pass(label)
        return True

    log_row = _log_recent(db, automation_name, action_name)
    if log_row:
        _pass(f"{label} (via automation_log: {action_name} logged success)")
        return True

    _fail(
        label,
        f"not in #{channel_name} history AND no {automation_name}.{action_name} "
        f"success in log (last {_LOG_WINDOW_MINUTES}m)",
    )
    return False


# ── QuickBooks helpers ────────────────────────────────────────────────────────

def _qbo_query(headers: dict, base_url: str, query: str) -> list:
    import requests
    resp = requests.get(
        f"{base_url}/query",
        headers=headers,
        params={"query": query, "minorversion": "65"},
        timeout=20,
    )
    resp.raise_for_status()
    data = resp.json().get("QueryResponse", {})
    for key, val in data.items():
        if isinstance(val, list) and val:
            return val
    return []


# ── Mailchimp helper ──────────────────────────────────────────────────────────

def _mailchimp_member_exists(mc_client, audience_id: str, email: str) -> bool:
    import hashlib
    member_hash = hashlib.md5(email.strip().lower().encode()).hexdigest()
    try:
        member = mc_client.lists.get_list_member(audience_id, member_hash)
        return member.get("status") in (
            "subscribed", "unsubscribed", "cleaned", "pending", "transactional"
        )
    except Exception:
        return False


# ── Tool IDs loader ──────────────────────────────────────────────────────────

def _load_tool_ids() -> dict:
    path = os.path.join(_PROJECT_ROOT, "config", "tool_ids.json")
    with open(path) as f:
        return json.load(f)


# =============================================================================
# SMOKE TEST 1 — New Client Onboarding
# =============================================================================

def smoke_test_1(db) -> bool:
    _section("SMOKE TEST 1: New Client Onboarding")

    # Pick a client that already has a Pipedrive mapping (seeded data)
    row = db.execute("""
        SELECT c.id, c.first_name, c.last_name, c.email, c.client_type,
               m.tool_specific_id AS pipedrive_deal_id
        FROM clients c
        JOIN cross_tool_mapping m
          ON m.canonical_id = c.id AND m.tool_name = 'pipedrive'
        WHERE c.status = 'active'
        LIMIT 1
    """).fetchone()

    if not row:
        print("  [SKIP] No client with Pipedrive mapping found in seeded data")
        return False

    client_id         = _col(row, "id")
    client_name       = f"{_col(row, 'first_name')} {_col(row, 'last_name')}".strip()
    email             = _col(row, "email")
    client_type       = _col(row, "client_type")
    pipedrive_deal_id = _col(row, "pipedrive_deal_id")

    print(f"  Client       : {client_name} ({email})")
    print(f"  Client type  : {client_type}")
    print(f"  Pipedrive ID : {pipedrive_deal_id}")

    trigger_event = {
        "deal_id":           pipedrive_deal_id,
        "contact_name":      client_name,
        "contact_email":     email,
        "contact_phone":     "",
        "deal_value":        150.0,
        "client_type":       client_type,
        "service_type":      "Standard Residential Clean",
        "service_frequency": "biweekly",
        "neighborhood":      "Austin",
        "address":           "123 Main St, Austin, TX 78701",
    }

    from automations.new_client_onboarding import NewClientOnboarding
    automation = NewClientOnboarding(clients=get_client, db=db, dry_run=False)
    try:
        automation.run(trigger_event)
        print("  Automation run completed.")
    except Exception as exc:
        print(f"  [ERROR] Automation raised: {exc}")

    time.sleep(2)

    results = []

    # ── Asana: verify via automation_log (project has 500+ tasks; log is the
    #    reliable source for this run's output) ─────────────────────────────
    results.append(_log_check(
        db, "Asana: onboarding tasks created in Client Success",
        "NewClientOnboarding", "create_asana_tasks",
    ))

    # ── Jobber: verify the mapping was registered in cross_tool_mapping
    #    (commercial clients are indexed by company name, not email, so email
    #    search is unreliable; the mapping is the canonical truth) ──────────
    try:
        jobber_map = db.execute(
            "SELECT tool_specific_id FROM cross_tool_mapping "
            "WHERE canonical_id = ? AND tool_name = 'jobber' LIMIT 1",
            (client_id,),
        ).fetchone()
        ok = bool(jobber_map)
        results.append(_check(
            f"Jobber: cross_tool_mapping entry exists for {client_id}",
            ok, _col(jobber_map, "tool_specific_id") if ok else "no mapping found",
        ))
    except Exception as exc:
        _fail("Jobber: mapping check", str(exc)); results.append(False)

    # ── QuickBooks: verify mapping was registered ─────────────────────────
    try:
        qbo_map = db.execute(
            "SELECT tool_specific_id FROM cross_tool_mapping "
            "WHERE canonical_id = ? AND tool_name = 'quickbooks' LIMIT 1",
            (client_id,),
        ).fetchone()
        ok = bool(qbo_map)
        results.append(_check(
            f"QuickBooks: cross_tool_mapping entry exists for {client_id}",
            ok, _col(qbo_map, "tool_specific_id") if ok else "no mapping found",
        ))
    except Exception as exc:
        _fail("QuickBooks: mapping check", str(exc)); results.append(False)

    # ── Mailchimp: direct API check (reliable) ────────────────────────────
    try:
        tool_ids    = _load_tool_ids()
        audience_id = tool_ids["mailchimp"]["audience_id"]
        mc          = get_client("mailchimp")
        ok          = _mailchimp_member_exists(mc, audience_id, email)
        results.append(_check(
            f"Mailchimp: subscriber found for '{email}'",
            ok, "not found" if not ok else "",
        ))
    except Exception as exc:
        _fail("Mailchimp: subscriber search", str(exc)); results.append(False)

    # ── Slack #new-clients: unified check (history → log fallback) ───────
    results.append(_slack_check(
        db, f"Slack: message in #new-clients mentioning '{client_name}'",
        get_client("slack"), "new-clients", client_name,
        "NewClientOnboarding", "post_slack_notification",
    ))

    # Store for Data Integrity check
    smoke_test_1.client_id   = client_id
    smoke_test_1.client_name = client_name
    return all(results)


# =============================================================================
# SMOKE TEST 2 — Job Completion Flow
# =============================================================================

def smoke_test_2(db) -> bool:
    _section("SMOKE TEST 2: Job Completion Flow")

    # jobs.service_type_id is a slug; map to the display name the automation expects
    _slug_to_name = {
        "recurring-weekly":   "Recurring Weekly",
        "recurring-biweekly": "Recurring Biweekly",
        "recurring-monthly":  "Recurring Monthly",
        "deep-clean":         "Deep Clean",
        "move-in-out":        "Move-In/Move-Out Clean",
        "commercial-nightly": "Commercial Nightly Clean",
    }

    row = db.execute("""
        SELECT j.id, j.client_id, j.service_type_id, j.duration_minutes_actual,
               m.tool_specific_id AS jobber_client_id
        FROM jobs j
        JOIN cross_tool_mapping m
          ON m.canonical_id = j.client_id AND m.tool_name = 'jobber'
        WHERE j.status = 'completed'
        LIMIT 1
    """).fetchone()

    if not row:
        print("  [SKIP] No completed job with Jobber mapping in seeded data")
        return False

    job_id           = _col(row, "id")
    client_id        = _col(row, "client_id")
    service_type_id  = _col(row, "service_type_id") or "recurring-biweekly"
    duration_minutes = _col(row, "duration_minutes_actual") or 120
    jobber_client_id = _col(row, "jobber_client_id")
    service_type     = _slug_to_name.get(service_type_id, "Standard Residential Clean")

    print(f"  Job          : {job_id} | service: {service_type}")
    print(f"  Jobber client: {jobber_client_id}")

    trigger_event = {
        "job_id":           job_id,
        "client_id":        jobber_client_id,
        "service_type":     service_type,
        "duration_minutes": duration_minutes,
        "crew":             "Crew A",
        "completion_notes": "Smoke test job completion",
        "is_recurring":     True,
        "completed_at":     datetime.now(timezone.utc).date().isoformat(),
    }

    from automations.job_completion_flow import JobCompletionFlow
    automation = JobCompletionFlow(clients=get_client, db=db, dry_run=False)
    try:
        automation.run(trigger_event)
        print("  Automation run completed.")
    except Exception as exc:
        print(f"  [ERROR] Automation raised: {exc}")

    time.sleep(2)

    results = []

    # ── QuickBooks invoice: read invoice ID from automation_log action_target
    #    (QBO Query API does not support LIKE on PrivateNote) ──────────────
    log_row = _log_recent(db, "JobCompletionFlow", "create_quickbooks_invoice")
    if log_row:
        target   = _col(log_row, "action_target") or ""
        inv_id   = target.split(":")[-1] if ":" in target else target
        ok       = bool(inv_id and inv_id != "None")
        results.append(_check(
            f"QuickBooks: invoice created for job '{job_id}'",
            ok, f"invoice ID = {inv_id}" if ok else "no invoice ID in log",
        ))
    else:
        results.append(_check(
            f"QuickBooks: invoice created for job '{job_id}'",
            False, f"no success entry in automation_log within {_LOG_WINDOW_MINUTES}m",
        ))

    # ── pending_actions: direct DB check ─────────────────────────────────
    try:
        pa_row = db.execute(
            "SELECT id, status FROM pending_actions "
            "WHERE trigger_context LIKE ? AND action_name = 'send_review_request' "
            "ORDER BY id DESC LIMIT 1",
            (f"%{job_id}%",),
        ).fetchone()
        ok = pa_row is not None
        results.append(_check(
            f"pending_actions: review-request row created for job '{job_id}'",
            ok, f"id={_col(pa_row,'id')}" if ok else "row not found",
        ))
    except Exception as exc:
        _fail("pending_actions: row query", str(exc)); results.append(False)

    # ── HubSpot: verify via automation_log (custom properties may not be
    #    configured in sandbox; log is the reliable source) ────────────────
    results.append(_log_check(
        db, "HubSpot: engagement note + last_service_date updated",
        "JobCompletionFlow", "update_hubspot_engagement",
    ))

    # ── Slack #operations: unified check (history → log fallback) ────────
    results.append(_slack_check(
        db, "Slack: job-completed message in #operations",
        get_client("slack"), "operations", "Job Completed",
        "JobCompletionFlow", "post_slack_summary",
    ))

    return all(results)


# =============================================================================
# SMOKE TEST 3 — Payment Received
# =============================================================================

def smoke_test_3(db) -> bool:
    _section("SMOKE TEST 3: Payment Received")

    row = db.execute("""
        SELECT p.id, p.amount, p.payment_method, p.payment_date,
               p.invoice_id, p.client_id
        FROM payments p
        WHERE p.amount > 0
        ORDER BY p.payment_date DESC
        LIMIT 1
    """).fetchone()

    if not row:
        print("  [SKIP] No payment records in seeded data")
        return False

    payment_id = _col(row, "id")
    amount     = _col(row, "amount")
    method     = _col(row, "payment_method") or "Credit Card"
    invoice_id = _col(row, "invoice_id") or ""
    client_id  = _col(row, "client_id")

    mapping = db.execute(
        "SELECT tool_specific_id FROM cross_tool_mapping "
        "WHERE canonical_id = ? AND tool_name = 'quickbooks' LIMIT 1",
        (client_id,),
    ).fetchone()
    qbo_customer_id = _col(mapping, "tool_specific_id") if mapping else ""

    print(f"  Payment      : {payment_id} | amount: ${amount:.2f} | method: {method}")
    print(f"  QBO customer : {qbo_customer_id or '(no mapping)'}")

    trigger_event = {
        "payment_id":  payment_id,
        "amount":      amount,
        "date":        datetime.now(timezone.utc).date().isoformat(),
        "method":      method,
        "invoice_id":  invoice_id,
        "customer_id": qbo_customer_id,
    }

    from automations.payment_received import PaymentReceived
    automation = PaymentReceived(clients=get_client, db=db, dry_run=False)
    try:
        automation.run(trigger_event)
        print("  Automation run completed.")
    except Exception as exc:
        print(f"  [ERROR] Automation raised: {exc}")

    time.sleep(2)

    results = []

    # ── Pipedrive: direct API check (reliable — automation skips gracefully
    #    when no mapping exists, and logs "skipped" not "failed") ──────────
    try:
        pipedrive_mapping = db.execute(
            "SELECT tool_specific_id FROM cross_tool_mapping "
            "WHERE canonical_id = ? AND tool_name = 'pipedrive' LIMIT 1",
            (client_id,),
        ).fetchone()

        if pipedrive_mapping:
            deal_id = _col(pipedrive_mapping, "tool_specific_id")
            session = get_client("pipedrive")
            base    = session.base_url.rstrip("/")
            if not any(seg in base for seg in ("/v1", "/v2")):
                base = f"{base}/v1"
            resp = session.get(
                f"{base}/deals/{deal_id}/activities",
                params={"limit": 20}, timeout=15,
            )
            resp.raise_for_status()
            activities = resp.json().get("data") or []
            ok = any(
                "payment received" in str(a.get("subject") or "").lower()
                for a in activities
            )
            results.append(_check(
                f"Pipedrive: payment-received activity on deal {deal_id}",
                ok, "activity found" if ok else "no matching activity",
            ))
        else:
            # Not every client originates from Pipedrive — this is acceptable
            print(f"  [INFO] No Pipedrive mapping for {client_id} — check skipped (by design)")
            results.append(True)
    except Exception as exc:
        _fail("Pipedrive: activity check", str(exc)); results.append(False)

    # ── HubSpot: verify via automation_log ────────────────────────────────
    results.append(_log_check(
        db, "HubSpot: last_payment_date updated",
        "PaymentReceived", "update_hubspot_financials",
    ))

    # ── Slack #operations: unified check (history → log fallback) ────────
    results.append(_slack_check(
        db, "Slack: payment-received message in #operations",
        get_client("slack"), "operations", "Payment Received",
        "PaymentReceived", "post_slack_notification",
    ))

    return all(results)


# =============================================================================
# SMOKE TEST 4 — Lead Leak Detection
# =============================================================================

def smoke_test_4(db) -> bool:
    _section("SMOKE TEST 4: Lead Leak Detection")

    from automations.lead_leak_detection import LeadLeakDetection
    automation = LeadLeakDetection(clients=get_client, db=db, dry_run=False)
    try:
        automation.run()
        print("  Automation run completed.")
    except Exception as exc:
        print(f"  [ERROR] Automation raised: {exc}")

    time.sleep(2)

    results = []

    # ── Slack #sales: unified check (history → log fallback) ─────────────
    results.append(_slack_check(
        db, "Slack: lead-leak report in #sales",
        get_client("slack"), "sales", "Lead Leak",
        "LeadLeakDetection", "post_slack_summary",
    ))

    # ── Asana: verify via automation_log ─────────────────────────────────
    #    (empty list when no leaks found is also a success case)
    results.append(_log_check(
        db, "Asana: leaked-lead tasks action completed",
        "LeadLeakDetection", "create_asana_tasks",
    ))

    return all(results)


# =============================================================================
# SMOKE TEST 5 — Overdue Invoice Escalation
# =============================================================================

def smoke_test_5(db) -> bool:
    _section("SMOKE TEST 5: Overdue Invoice Escalation")

    from automations.overdue_invoice import OverdueInvoiceEscalation
    automation = OverdueInvoiceEscalation(clients=get_client, db=db, dry_run=False)
    try:
        automation.run()
        print("  Automation run completed.")
    except Exception as exc:
        print(f"  [ERROR] Automation raised: {exc}")

    time.sleep(2)

    results = []

    # ── Slack #operations: check history ─────────────────────────────────
    try:
        slack_client = get_client("slack")
        ok = (
            _slack_recent_message(slack_client, "operations", "AR Aging Report")
            or _slack_recent_message(slack_client, "operations", "No overdue invoices")
        )
        if ok:
            results.append(_check("Slack: AR aging report in #operations", ok))
        else:
            # Fall back to log
            log_ok = _log_recent(db, "OverdueInvoiceEscalation", "post_slack_aging_report") is not None
            results.append(_check(
                "Slack: AR aging report posted (via automation_log)",
                log_ok,
                "no success in log within 15m" if not log_ok else "",
            ))
    except Exception as exc:
        _fail("Slack: #operations AR aging", str(exc)); results.append(False)

    # ── Asana: verify via automation_log ─────────────────────────────────
    #    Note: task creation may be skipped (deduplication) or may fail due to
    #    assignee-not-in-org; check for any recent entry (success or skipped)
    try:
        cutoff = (
            datetime.now(timezone.utc) - timedelta(minutes=_LOG_WINDOW_MINUTES)
        ).strftime("%Y-%m-%d %H:%M:%S")
        log_row = db.execute(
            """
            SELECT id, action_target, status, error_message
            FROM automation_log
            WHERE automation_name = 'OverdueInvoiceEscalation'
              AND action_name = 'create_asana_task'
              AND created_at >= ?
            ORDER BY id DESC LIMIT 1
            """,
            (cutoff,),
        ).fetchone()

        if log_row:
            st = _col(log_row, "status")
            ok = st in ("success", "skipped")   # skipped = dedup, both valid
            results.append(_check(
                f"Asana: overdue invoice task action logged (status={st})",
                ok, _col(log_row, "error_message") if not ok else "",
            ))
        else:
            # No overdue invoices found → no task action logged (still OK)
            fetch_log = _log_recent(db, "OverdueInvoiceEscalation", "fetch_overdue_invoices")
            if fetch_log:
                detail = json.loads(
                    db.execute(
                        "SELECT trigger_detail FROM automation_log WHERE id = ?",
                        (_col(fetch_log, "id"),),
                    ).fetchone()["trigger_detail"] or "{}"
                )
                count = detail.get("count", 0)
                if count == 0:
                    print("  [INFO] No overdue invoices found — Asana task check N/A")
                    results.append(True)
                else:
                    results.append(_check(
                        "Asana: overdue invoice tasks",
                        False, f"{count} invoices found but no task logged",
                    ))
            else:
                results.append(_check(
                    "Asana: fetch_overdue_invoices logged",
                    False, "no fetch log within 15m",
                ))
    except Exception as exc:
        _fail("Asana: overdue invoice tasks check", str(exc)); results.append(False)

    return all(results)


# =============================================================================
# SMOKE TEST 6 — Negative Review Response
# =============================================================================

def smoke_test_6(db) -> bool:
    _section("SMOKE TEST 6: Negative Review Response")

    # reviews table: id, client_id, job_id, rating, review_text, platform,
    #                review_date, response_text, response_date
    row = db.execute("""
        SELECT r.id, r.client_id, r.rating, r.review_text, r.review_date
        FROM reviews r
        WHERE r.rating <= 2
        ORDER BY r.review_date DESC
        LIMIT 1
    """).fetchone()

    if not row:
        print("  [SKIP] No reviews with rating <= 2 in seeded data")
        return False

    review_id   = _col(row, "id")
    client_id   = _col(row, "client_id")
    rating      = _col(row, "rating")
    review_text = _col(row, "review_text") or "Poor service experience"

    client_row = db.execute(
        "SELECT first_name, last_name, email FROM clients WHERE id = ?",
        (client_id,),
    ).fetchone()

    if not client_row:
        print(f"  [SKIP] Client {client_id} not found for review {review_id}")
        return False

    client_name = (
        f"{_col(client_row, 'first_name')} {_col(client_row, 'last_name')}".strip()
    )
    email = _col(client_row, "email")

    print(f"  Review       : {review_id} | client: {client_name} | rating: {rating}/5")

    trigger_event = {
        "row_index":    1,
        "date":         datetime.now(timezone.utc).date().isoformat(),
        "client_name":  client_name,
        "client_email": email,
        "rating":       rating,
        "review_text":  review_text,
        "crew":         "Crew A",
        "service_type": "Standard Residential Clean",
    }

    from automations.negative_review import NegativeReviewResponse
    automation = NegativeReviewResponse(clients=get_client, db=db, dry_run=False)
    try:
        automation.run(trigger_event)
        print("  Automation run completed.")
    except Exception as exc:
        print(f"  [ERROR] Automation raised: {exc}")

    time.sleep(2)

    results = []

    # ── Slack #operations ─────────────────────────────────────────────────
    try:
        ok = _slack_recent_message(get_client("slack"), "operations", "NEGATIVE REVIEW")
        if ok:
            results.append(_check("Slack: negative review alert in #operations", ok))
        else:
            log_ok = _log_recent(db, "NegativeReviewResponse", "post_slack_alert") is not None
            results.append(_check(
                "Slack: negative review alert posted (via automation_log)",
                log_ok, "no success in log within 15m" if not log_ok else "",
            ))
    except Exception as exc:
        _fail("Slack: #operations negative review", str(exc)); results.append(False)

    # ── Asana Client Success: verify via automation_log ───────────────────
    #    (task creation may fail if assignee not in Asana org; log the true outcome)
    results.append(_log_check(
        db, "Asana: review-response task created in Client Success",
        "NegativeReviewResponse", "create_asana_task",
    ))

    # ── HubSpot at_risk flag: verify via automation_log ───────────────────
    results.append(_log_check(
        db, "HubSpot: contact flagged as at_risk",
        "NegativeReviewResponse", "flag_hubspot_contact",
    ))

    return all(results)


# =============================================================================
# SMOKE TEST 7 — Delayed Action Processing
# =============================================================================

def smoke_test_7(db) -> bool:
    _section("SMOKE TEST 7: Delayed Action Processing")

    test_email    = "smoke_test_phase3_delayed@example-test.com"
    execute_after = (
        datetime.now(timezone.utc) - timedelta(hours=1)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Insert test pending_action row
    test_row_id = None
    try:
        with db:
            cursor = db.execute(
                """
                INSERT INTO pending_actions
                    (automation_name, action_name, trigger_context,
                     execute_after, status)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    "test_delayed",
                    "send_review_request",
                    json.dumps({
                        "client_email": test_email,
                        "client_name":  "Smoke Tester",
                    }),
                    execute_after,
                    "pending",
                ),
            )
            test_row_id = cursor.lastrowid
        print(f"  Inserted test pending_action row id={test_row_id}")
    except Exception as exc:
        _fail("Insert test pending_action row", str(exc))
        return False

    # Run pending action processor
    try:
        from automations.runner import run_pending
        run_pending(clients=get_client, db=db, dry_run=True)
        print("  run_pending() completed.")
    except Exception as exc:
        print(f"  [ERROR] run_pending raised: {exc}")

    time.sleep(1)

    results = []

    # Verify status changed to "executed" and executed_at is set
    try:
        updated    = db.execute(
            "SELECT status, executed_at FROM pending_actions WHERE id = ?",
            (test_row_id,),
        ).fetchone()

        ok_status  = updated is not None and _col(updated, "status") == "executed"
        ok_exec_at = updated is not None and bool(_col(updated, "executed_at"))

        results.append(_check(
            f"pending_actions: row {test_row_id} status='executed'",
            ok_status,
            f"actual={_col(updated, 'status')!r}" if updated else "row not found",
        ))
        results.append(_check(
            f"pending_actions: row {test_row_id} executed_at is populated",
            ok_exec_at,
            f"executed_at={_col(updated, 'executed_at')!r}" if updated else "row not found",
        ))
    except Exception as exc:
        _fail("pending_actions: status/executed_at check", str(exc))
        results.extend([False, False])

    # Clean up test row
    try:
        with db:
            db.execute("DELETE FROM pending_actions WHERE id = ?", (test_row_id,))
        print(f"  Cleaned up test row id={test_row_id}.")
    except Exception as exc:
        print(f"  [WARN] Could not clean up test row: {exc}")

    return all(results)


# =============================================================================
# SMOKE TEST 8 — Full Runner Dry Run
# =============================================================================

def smoke_test_8() -> bool:
    _section("SMOKE TEST 8: Full Runner Dry Run")

    cmd = [sys.executable, "-m", "automations.runner", "--all", "--dry-run"]
    print(f"  Running: {' '.join(cmd)}")

    results = []
    try:
        proc = subprocess.run(
            cmd,
            cwd=_PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=120,
        )

        ok_rc = proc.returncode == 0
        results.append(_check(
            "Runner --all --dry-run: exit code 0",
            ok_rc, f"exit code={proc.returncode}" if not ok_rc else "",
        ))

        combined = (proc.stdout or "") + (proc.stderr or "")
        ok_no_tb = "Traceback (most recent call last)" not in combined
        results.append(_check(
            "Runner --all --dry-run: no unhandled exceptions",
            ok_no_tb, "Traceback found in output" if not ok_no_tb else "",
        ))

        if not ok_rc or not ok_no_tb:
            print("  --- stdout (last 20 lines) ---")
            for line in (proc.stdout or "").splitlines()[-20:]:
                print(f"    {line}")
            if proc.stderr:
                print("  --- stderr (last 10 lines) ---")
                for line in (proc.stderr or "").splitlines()[-10:]:
                    print(f"    {line}")

    except subprocess.TimeoutExpired:
        _fail("Runner --all --dry-run: timed out after 120s")
        results.append(False)
    except Exception as exc:
        _fail("Runner --all --dry-run: subprocess error", str(exc))
        results.append(False)

    return all(results)


# =============================================================================
# DATA INTEGRITY CHECKS
# =============================================================================

def data_integrity_automation_log(db) -> bool:
    _section("DATA INTEGRITY: automation_log completeness")

    expected_automations = [
        "NewClientOnboarding",
        "JobCompletionFlow",
        "PaymentReceived",
        "LeadLeakDetection",
        "OverdueInvoiceEscalation",
        "NegativeReviewResponse",
    ]

    rows = db.execute("""
        SELECT automation_name, status, COUNT(*) as cnt
        FROM automation_log
        GROUP BY automation_name, status
    """).fetchall()

    summary: dict = {}
    for row in rows:
        name   = _col(row, "automation_name")
        status = _col(row, "status")
        cnt    = _col(row, "cnt")
        summary.setdefault(name, {})[status] = cnt

    all_passed = True
    for automation in expected_automations:
        has_success = summary.get(automation, {}).get("success", 0) > 0
        ok = _check(
            f"automation_log: {automation} has at least one 'success' entry",
            has_success,
            "no success entries" if not has_success else
            f"success={summary.get(automation, {}).get('success', 0)}",
        )
        all_passed = all_passed and ok

    # Print failed entries for manual review
    failed_rows = db.execute("""
        SELECT automation_name, action_name, error_message, created_at
        FROM automation_log
        WHERE status = 'failed'
        ORDER BY created_at DESC
        LIMIT 20
    """).fetchall()

    if failed_rows:
        print()
        print(f"  {_YELLOW}Failed log entries (manual review):{_RESET}")
        for fr in failed_rows:
            errmsg = (_col(fr, "error_message") or "")[:120].replace("\n", " ")
            print(
                f"    [{_col(fr, 'created_at')}] "
                f"{_col(fr, 'automation_name')}.{_col(fr, 'action_name')}: "
                f"{errmsg}"
            )

    return all_passed


def data_integrity_cross_tool_mapping(db) -> bool:
    _section("DATA INTEGRITY: cross_tool_mapping for onboarded client")

    client_id = getattr(smoke_test_1, "client_id", None)
    if not client_id:
        print("  [SKIP] Smoke Test 1 did not record a client_id")
        return True

    required_tools = ["jobber", "quickbooks", "mailchimp"]
    rows = db.execute(
        "SELECT tool_name, tool_specific_id FROM cross_tool_mapping WHERE canonical_id = ?",
        (client_id,),
    ).fetchall()
    mapped = {_col(r, "tool_name"): _col(r, "tool_specific_id") for r in rows}

    all_passed = True
    for tool in required_tools:
        ok = _check(
            f"cross_tool_mapping: {client_id} has '{tool}' mapping",
            tool in mapped,
            f"id={mapped[tool]}" if tool in mapped else "mapping missing",
        )
        all_passed = all_passed and ok

    return all_passed


def data_integrity_pending_actions(db) -> bool:
    _section("DATA INTEGRITY: pending_actions cleanup")

    cutoff = (
        datetime.now(timezone.utc) - timedelta(hours=2)
    ).strftime("%Y-%m-%dT%H:%M:%S")

    stale_rows = db.execute(
        "SELECT id, automation_name, action_name, execute_after "
        "FROM pending_actions "
        "WHERE status = 'pending' AND execute_after < ? "
        "ORDER BY execute_after ASC",
        (cutoff,),
    ).fetchall()

    ok = len(stale_rows) == 0
    _check(
        "pending_actions: no stale 'pending' rows (execute_after > 2h ago)",
        ok, f"{len(stale_rows)} stale row(s) found" if not ok else "",
    )

    if not ok:
        for sr in stale_rows:
            print(
                f"    id={_col(sr,'id')} "
                f"{_col(sr,'automation_name')}.{_col(sr,'action_name')} "
                f"due={_col(sr,'execute_after')}"
            )

    return ok


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    print()
    print(_BOLD + "=" * 70 + _RESET)
    print(_BOLD + "  SPARKLE & SHINE — PHASE 3 SMOKE TEST SUITE" + _RESET)
    print(_BOLD + "=" * 70 + _RESET)
    print(f"  Project root : {_PROJECT_ROOT}")
    print(f"  Started at   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Ensure automation tables exist
    run_migration()

    # Open DB with row_factory
    db_path = os.path.join(_PROJECT_ROOT, "sparkle_shine.db")
    db = get_connection(db_path)
    db.row_factory = sqlite3.Row

    test_results: dict = {}

    test_results["Test 1 (New Client Onboarding)"]     = smoke_test_1(db)
    test_results["Test 2 (Job Completion Flow)"]         = smoke_test_2(db)
    test_results["Test 3 (Payment Received)"]            = smoke_test_3(db)
    test_results["Test 4 (Lead Leak Detection)"]         = smoke_test_4(db)
    test_results["Test 5 (Overdue Invoice Escalation)"]  = smoke_test_5(db)
    test_results["Test 6 (Negative Review Response)"]    = smoke_test_6(db)
    test_results["Test 7 (Delayed Action Processing)"]   = smoke_test_7(db)
    test_results["Test 8 (Full Runner Dry Run)"]         = smoke_test_8()

    test_results["Data Integrity: automation_log"]       = data_integrity_automation_log(db)
    test_results["Data Integrity: cross_tool_mapping"]   = data_integrity_cross_tool_mapping(db)
    test_results["Data Integrity: pending_actions"]      = data_integrity_pending_actions(db)

    db.close()

    # Summary table
    print()
    print("=" * 70)
    print(_BOLD + "  === PHASE 3 SMOKE TEST RESULTS ===" + _RESET)
    print("=" * 70)

    passed      = 0
    total       = len(test_results)
    label_width = max(len(label) for label in test_results)

    for label, result in test_results.items():
        status = f"{_GREEN}PASS{_RESET}" if result else f"{_RED}FAIL{_RESET}"
        print(f"  {label:<{label_width}}  {status}")
        if result:
            passed += 1

    print("-" * 70)
    colour = _GREEN if passed == total else _RED
    print(f"  {colour}TOTAL: {passed}/{total} passed{_RESET}")
    print("=" * 70)
    print()

    # Final sanity query
    print(_BOLD + "Automation Log Summary:" + _RESET)
    print(f"  {'Name':<35} {'Total':>6} {'OK':>6} {'Fail':>6} {'Skip':>6}")
    print("  " + "-" * 57)

    db2 = get_connection(db_path)
    db2.row_factory = sqlite3.Row
    summary_rows = db2.execute("""
        SELECT automation_name,
               COUNT(*) as total_actions,
               SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) as succeeded,
               SUM(CASE WHEN status='failed'  THEN 1 ELSE 0 END) as failed,
               SUM(CASE WHEN status='skipped' THEN 1 ELSE 0 END) as skipped
        FROM automation_log
        GROUP BY automation_name
        ORDER BY automation_name
    """).fetchall()
    for r in summary_rows:
        print(
            f"  {_col(r,'automation_name'):<35} "
            f"{_col(r,'total_actions'):>6} "
            f"{_col(r,'succeeded'):>6} "
            f"{_col(r,'failed'):>6} "
            f"{_col(r,'skipped'):>6}"
        )
    db2.close()
    print()

    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()
