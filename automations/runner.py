"""
automations/runner.py

Main entry point for Sparkle & Shine automations.

Usage:
    python -m automations.runner --all
    python -m automations.runner --poll --dry-run
    python -m automations.runner --scheduled
    python -m automations.runner --pending
"""
import argparse
import datetime
import json
import logging
import os
import sys
import time

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(_PROJECT_ROOT, ".env"))
except ImportError:
    pass

from simulation.error_reporter import report_error

# ─────────────────────────────────────────────────────────────────────────────
# Logging setup
# ─────────────────────────────────────────────────────────────────────────────

_LOGS_DIR = os.path.join(_PROJECT_ROOT, "logs")
os.makedirs(_LOGS_DIR, exist_ok=True)

logger = logging.getLogger("automations")
logger.setLevel(logging.DEBUG)

if not logger.handlers:
    _stream_handler = logging.StreamHandler(sys.stdout)
    _stream_handler.setLevel(logging.INFO)
    _stream_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    )
    logger.addHandler(_stream_handler)

    _file_handler = logging.FileHandler(os.path.join(_LOGS_DIR, "automations.log"))
    _file_handler.setLevel(logging.DEBUG)
    _file_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    )
    logger.addHandler(_file_handler)


# ─────────────────────────────────────────────────────────────────────────────
# Asana rate-limit tracker
# ─────────────────────────────────────────────────────────────────────────────

class _AsanaRateLimiter:
    """
    Track Asana API requests in a rolling 60-second window.
    Pause for 60 seconds if approaching 150 requests/minute.
    """
    _LIMIT = 150
    _WINDOW = 60.0
    _BUFFER = 10  # pause when within this many calls of the limit

    def __init__(self):
        self._timestamps: list[float] = []

    def tick(self) -> None:
        """Call before each Asana API request."""
        now = time.monotonic()
        # Drop timestamps outside the rolling window
        self._timestamps = [t for t in self._timestamps if now - t < self._WINDOW]
        if len(self._timestamps) >= self._LIMIT - self._BUFFER:
            logger.warning(
                "Asana rate limit approaching (%d req in last 60s). "
                "Pausing for 60 seconds.",
                len(self._timestamps),
            )
            time.sleep(self._WINDOW)
            self._timestamps = []
        self._timestamps.append(time.monotonic())


_asana_limiter = _AsanaRateLimiter()


# ─────────────────────────────────────────────────────────────────────────────
# Poll mode
# ─────────────────────────────────────────────────────────────────────────────

def run_poll(clients, db, dry_run: bool) -> dict:
    from automations.triggers import (
        poll_pipedrive_won_deals,
        poll_jobber_completed_jobs,
        poll_quickbooks_payments,
        poll_sheets_negative_reviews,
    )
    from automations.new_client_onboarding import NewClientOnboarding
    from automations.job_completion_flow import JobCompletionFlow
    from automations.payment_received import PaymentReceived
    from automations.negative_review import NegativeReviewResponse
    from automations.hubspot_qualified_sync import HubSpotQualifiedSync

    results = {"processed": 0, "succeeded": 0, "failed": 0}

    # 1. Pipedrive won deals -> Automation 1
    logger.info("Polling Pipedrive for won deals...")
    try:
        won_deals = poll_pipedrive_won_deals(clients, db)
    except Exception as e:
        logger.error("Pipedrive poll failed — skipping: %s", e)
        report_error(e, tool_name="pipedrive", context="Polling Pipedrive for won deals", dry_run=dry_run)
        won_deals = []
    logger.info("Found %d new won deal(s).", len(won_deals))
    onboarding = NewClientOnboarding(clients, db, dry_run)
    for deal in won_deals:
        results["processed"] += 1
        try:
            onboarding.run(deal)
            results["succeeded"] += 1
        except Exception as e:
            results["failed"] += 1
            logger.error("Onboarding failed for deal %s: %s", deal.get("deal_id"), e)
        time.sleep(0.5)

    # 2. Jobber completed jobs -> Automation 2
    logger.info("Polling Jobber for completed jobs...")
    try:
        completed = poll_jobber_completed_jobs(clients, db)
    except Exception as e:
        logger.error("Jobber poll failed — skipping: %s", e)
        report_error(e, tool_name="jobber", context="Polling Jobber for completed jobs", dry_run=dry_run)
        completed = []
    logger.info("Found %d completed job(s).", len(completed))
    job_flow = JobCompletionFlow(clients, db, dry_run)
    for job in completed:
        results["processed"] += 1
        try:
            job_flow.run(job)
            results["succeeded"] += 1
        except Exception as e:
            results["failed"] += 1
            logger.error("Job flow failed for %s: %s", job.get("job_id"), e)
        time.sleep(0.5)

    # 3. QuickBooks payments -> Automation 3
    logger.info("Polling QuickBooks for new payments...")
    try:
        payments = poll_quickbooks_payments(clients, db)
    except Exception as e:
        logger.error("QuickBooks poll failed — skipping: %s", e)
        report_error(e, tool_name="quickbooks", context="Polling QuickBooks for new payments", dry_run=dry_run)
        payments = []
    logger.info("Found %d new payment(s).", len(payments))
    payment_auto = PaymentReceived(clients, db, dry_run)
    for payment in payments:
        results["processed"] += 1
        try:
            payment_auto.run(payment)
            results["succeeded"] += 1
        except Exception as e:
            results["failed"] += 1
            logger.error(
                "Payment flow failed for %s: %s", payment.get("payment_id"), e
            )
        time.sleep(0.5)

    # 4. Google Sheets reviews -> Automation 6
    logger.info("Polling Google Sheets for negative reviews...")
    try:
        reviews = poll_sheets_negative_reviews(clients, db)
    except Exception as e:
        logger.error("Google Sheets poll failed — skipping: %s", e)
        report_error(e, tool_name="google", context="Polling Google Sheets for negative reviews", dry_run=dry_run)
        reviews = []
    logger.info("Found %d negative review(s).", len(reviews))
    review_auto = NegativeReviewResponse(clients, db, dry_run)
    for review in reviews:
        results["processed"] += 1
        try:
            review_auto.run(review)
            results["succeeded"] += 1
        except Exception as e:
            results["failed"] += 1
            logger.error(
                "Review response failed for row %s: %s", review.get("row_index"), e
            )
        time.sleep(0.5)

    # 5. HubSpot SQLs -> Pipedrive deals (HubSpotQualifiedSync)
    logger.info("Running HubSpot Qualified Lead Sync...")
    results["processed"] += 1
    try:
        HubSpotQualifiedSync(clients, db, dry_run).run()
        results["succeeded"] += 1
    except Exception as e:
        results["failed"] += 1
        logger.error("HubSpot qualified sync failed: %s", e)
        report_error(e, tool_name="hubspot", context="HubSpot qualified lead sync", dry_run=dry_run)

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Scheduled mode
# ─────────────────────────────────────────────────────────────────────────────

_LEAD_LEAK_SENTINEL = os.path.join(_LOGS_DIR, ".lead_leak_last_run")
_OVERDUE_INVOICE_SENTINEL = os.path.join(_LOGS_DIR, ".overdue_invoice_last_run")


def _should_run_lead_leak() -> bool:
    """Return True if LeadLeakDetection has not run successfully in the last 24 hours."""
    if not os.path.exists(_LEAD_LEAK_SENTINEL):
        return True
    return (time.time() - os.path.getmtime(_LEAD_LEAK_SENTINEL)) >= 86400


def _mark_lead_leak_ran() -> None:
    """Touch the sentinel file to record that LeadLeakDetection just completed."""
    open(_LEAD_LEAK_SENTINEL, "w").close()


def _should_run_overdue_invoice() -> bool:
    """Return True if OverdueInvoiceEscalation has not run successfully in the last 7 days."""
    if not os.path.exists(_OVERDUE_INVOICE_SENTINEL):
        return True
    return (time.time() - os.path.getmtime(_OVERDUE_INVOICE_SENTINEL)) >= 7 * 86400


def _mark_overdue_invoice_ran() -> None:
    """Touch the sentinel file to record that OverdueInvoiceEscalation just completed."""
    open(_OVERDUE_INVOICE_SENTINEL, "w").close()


def run_scheduled(clients, db, dry_run: bool) -> dict:
    from automations.lead_leak_detection import LeadLeakDetection
    from automations.overdue_invoice import OverdueInvoiceEscalation

    results = {"processed": 0, "succeeded": 0, "failed": 0}

    # Lead Leak Detection -- at most once per 24 hours
    if _should_run_lead_leak():
        logger.info("Running Lead Leak Detection...")
        results["processed"] += 1
        try:
            LeadLeakDetection(clients, db, dry_run).run()
            results["succeeded"] += 1
            if not dry_run:
                _mark_lead_leak_ran()
        except Exception as e:
            results["failed"] += 1
            logger.error("Lead leak detection failed: %s", e)
            report_error(e, tool_name="pipedrive", context="Lead leak detection scan", dry_run=dry_run)
    else:
        logger.info("Skipping Lead Leak Detection (ran within the last 24 hours)")

    time.sleep(0.5)

    # Overdue Invoice Escalation -- only runs on Monday, at most once per 7 days
    if datetime.date.today().weekday() == 0 and _should_run_overdue_invoice():
        logger.info("Running Overdue Invoice Escalation (Monday)...")
        results["processed"] += 1
        try:
            OverdueInvoiceEscalation(clients, db, dry_run).run()
            results["succeeded"] += 1
            if not dry_run:
                _mark_overdue_invoice_ran()
        except Exception as e:
            results["failed"] += 1
            logger.error("Overdue invoice scan failed: %s", e)
            report_error(e, tool_name="quickbooks", context="Overdue invoice escalation scan", dry_run=dry_run)
    elif datetime.date.today().weekday() == 0:
        logger.info("Skipping overdue invoice scan (already ran this week)")
    else:
        logger.info("Skipping overdue invoice scan (only runs on Mondays)")

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Pending mode
# ─────────────────────────────────────────────────────────────────────────────

def run_pending(clients, db, dry_run: bool) -> dict:
    results = {"processed": 0, "succeeded": 0, "failed": 0}
    now_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    rows = db.execute(
        """
        SELECT id, automation_name, action_name, trigger_context
        FROM pending_actions
        WHERE status = 'pending' AND execute_after <= %s
        ORDER BY execute_after ASC
        """,
        (now_str,),
    ).fetchall()

    logger.info("Found %d pending action(s) due for execution.", len(rows))

    for row in rows:
        action_id = row["id"]
        action_name = row["action_name"]
        results["processed"] += 1

        try:
            context = json.loads(row["trigger_context"])
            _dispatch_pending(clients, db, action_name, context, dry_run)

            executed_at = datetime.datetime.now(datetime.timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            with db:
                db.execute(
                    "UPDATE pending_actions SET status='executed', executed_at=%s WHERE id=%s",
                    (executed_at, action_id),
                )
            results["succeeded"] += 1
            logger.debug("Pending action %d ('%s') executed.", action_id, action_name)

        except Exception as e:
            with db:
                db.execute(
                    "UPDATE pending_actions SET status='failed' WHERE id=%s",
                    (action_id,),
                )
            results["failed"] += 1
            logger.error(
                "Pending action %d ('%s') failed: %s", action_id, action_name, e
            )

        time.sleep(0.5)

    return results


def _dispatch_pending(clients, db, action_name: str, context: dict, dry_run: bool) -> None:
    """Route a pending action to its handler."""
    if action_name == "send_review_request":
        _handle_send_review_request(clients, db, context, dry_run)
    else:
        logger.warning("Unknown pending action_name '%s' — skipping.", action_name)


def _handle_send_review_request(clients, db, context: dict, dry_run: bool) -> None:
    """
    Add the 'review-requested' tag to a Mailchimp subscriber.

    Expected context keys: client_email (required), client_name (optional).
    """
    email = context.get("client_email") or context.get("email")
    if not email:
        raise ValueError("send_review_request context missing 'client_email'")

    if dry_run:
        logger.info(
            "[DRY RUN] Would add Mailchimp tag 'review-requested' for %s", email
        )
        return

    mailchimp = clients("mailchimp")

    # Resolve audience ID
    _ids_path = os.path.join(_PROJECT_ROOT, "config", "tool_ids.json")
    with open(_ids_path) as f:
        tool_ids = json.load(f)
    audience_id = tool_ids.get("mailchimp", {}).get("audience_id", "")
    if not audience_id:
        raise ValueError("mailchimp.audience_id not set in config/tool_ids.json")

    # Mailchimp member hash = md5(lowercase email)
    import hashlib
    member_hash = hashlib.md5(email.lower().encode()).hexdigest()

    # Add tag via Mailchimp POST /lists/{list_id}/members/{subscriber_hash}/tags
    response = mailchimp.lists.update_list_member_tags(
        audience_id,
        member_hash,
        {"tags": [{"name": "review-requested", "status": "active"}]},
    )
    logger.debug(
        "Mailchimp tag 'review-requested' added for %s (response: %s)", email, response
    )


# ─────────────────────────────────────────────────────────────────────────────
# Summary report
# ─────────────────────────────────────────────────────────────────────────────

def _print_summary(mode: str, dry_run: bool, totals: dict, duration_s: float) -> None:
    dry_label = "yes" if dry_run else "no"
    print("=== Automation Run Complete ===")
    print(f"Mode:                {mode}")
    print(f"Dry run:             {dry_label}")
    print(f"Triggers processed:  {totals['processed']}")
    print(f"Succeeded:           {totals['succeeded']}")
    print(f"Failed:              {totals['failed']}")
    print(f"Duration:            {duration_s:.1f}s")
    print("===============================")


def _merge(totals: dict, result: dict) -> None:
    for key in ("processed", "succeeded", "failed"):
        totals[key] += result.get(key, 0)


# ─────────────────────────────────────────────────────────────────────────────
# Health check
# ─────────────────────────────────────────────────────────────────────────────

def _run_health_check() -> None:
    """Run automation runner health checks and exit.

    Answers: 'Can the automation runner process triggers right now?'
    Called by --health before migrations or polling run.
    Exits 0 if all checks PASS or WARN, exits 1 if any FAIL.
    """
    import importlib

    from database.health import (
        HealthCheck,
        check_connection,
        check_table_inventory,
        check_sequences,
        render_table,
    )

    checks: list[HealthCheck] = []

    # 1. DB connection
    conn_check, conn = check_connection()
    checks.append(conn_check)

    _TABLES     = ["pending_actions", "poll_state"]
    _SEQ_TABLES = ["pending_actions", "automation_log"]

    if conn is None:
        for name in ("Table inventory", "Sequence health"):
            checks.append(HealthCheck(name, "SKIP", "DB unreachable"))
    else:
        try:
            # 2. Table inventory
            checks.extend(check_table_inventory(conn, _TABLES))
            # 3. Sequence health for the two SERIAL-PK automation tables
            checks.extend(check_sequences(conn, _SEQ_TABLES))
        finally:
            conn.close()

    # 4. Sentinel age
    _sentinels = [
        (_LEAD_LEAK_SENTINEL,       "Lead leak sentinel",       48 * 3600),
        (_OVERDUE_INVOICE_SENTINEL, "Overdue invoice sentinel", 14 * 86400),
    ]
    for sentinel_path, name, max_age_seconds in _sentinels:
        if not os.path.exists(sentinel_path):
            checks.append(HealthCheck(name, "PASS", "missing (first run)"))
        else:
            age_seconds = time.time() - os.path.getmtime(sentinel_path)
            if age_seconds > max_age_seconds:
                checks.append(HealthCheck(
                    name, "WARN",
                    f"last ran {age_seconds / 3600:.0f}h ago",
                ))
            else:
                checks.append(HealthCheck(name, "PASS", ""))

    # 5. Automation module imports
    _AUTOMATION_IMPORTS = [
        ("automations.new_client_onboarding",  "NewClientOnboarding"),
        ("automations.job_completion_flow",     "JobCompletionFlow"),
        ("automations.payment_received",        "PaymentReceived"),
        ("automations.negative_review",         "NegativeReviewResponse"),
        ("automations.lead_leak_detection",     "LeadLeakDetection"),
        ("automations.overdue_invoice",         "OverdueInvoiceEscalation"),
        ("automations.hubspot_qualified_sync",       "HubSpotQualifiedSync"),
    ]
    for module_path, class_name in _AUTOMATION_IMPORTS:
        try:
            mod = importlib.import_module(module_path)
            getattr(mod, class_name)
            checks.append(HealthCheck(f"Import: {class_name}", "PASS", ""))
        except (ImportError, AttributeError) as exc:
            checks.append(HealthCheck(f"Import: {class_name}", "WARN", str(exc)))

    render_table("Automation Runner — Health Check", checks)
    sys.exit(1 if any(c.status == "FAIL" for c in checks) else 0)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sparkle & Shine automation runner"
    )
    parser.add_argument(
        "--poll",
        action="store_true",
        help="Run event-based trigger checks (Pipedrive, Jobber, QBO, Sheets)",
    )
    parser.add_argument(
        "--scheduled",
        action="store_true",
        help="Run daily/weekly scheduled automations (Lead Leak, Overdue Invoice)",
    )
    parser.add_argument(
        "--pending",
        action="store_true",
        help="Process delayed actions from pending_actions table",
    )
    parser.add_argument(
        "--all",
        dest="run_all",
        action="store_true",
        help="Run all three modes in sequence (default if no flag given)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="No API writes. Prints [DRY RUN] prefix.",
    )
    parser.add_argument(
        "--health",
        action="store_true",
        help="Run service health checks and exit. Does not process any triggers.",
    )
    args = parser.parse_args()

    if args.health:
        _run_health_check()  # exits internally

    # Default to --all when no mode flag is given
    if not (args.poll or args.scheduled or args.pending or args.run_all):
        args.run_all = True

    dry_run = args.dry_run
    run_poll_mode = args.poll or args.run_all
    run_scheduled_mode = args.scheduled or args.run_all
    run_pending_mode = args.pending or args.run_all

    # Determine display mode label
    active_modes = []
    if run_poll_mode:
        active_modes.append("poll")
    if run_scheduled_mode:
        active_modes.append("scheduled")
    if run_pending_mode:
        active_modes.append("pending")
    mode_label = "+".join(active_modes) if active_modes else "none"

    start_time = time.monotonic()
    start_dt = datetime.datetime.now(datetime.timezone.utc)
    logger.info(
        "=== Run started at %s | mode=%s | dry_run=%s ===",
        start_dt.strftime("%Y-%m-%d %H:%M:%S"),
        mode_label,
        dry_run,
    )

    # Ensure automation tables exist
    from automations.migrate import run_migration
    run_migration()

    # Database + client factory
    from database.schema import get_connection
    from auth import get_client

    db = get_connection()

    totals = {"processed": 0, "succeeded": 0, "failed": 0}

    try:
        if run_poll_mode:
            logger.info("--- POLL MODE ---")
            result = run_poll(get_client, db, dry_run)
            _merge(totals, result)
            logger.info(
                "Poll mode complete: processed=%d succeeded=%d failed=%d",
                result["processed"],
                result["succeeded"],
                result["failed"],
            )

        if run_scheduled_mode:
            logger.info("--- SCHEDULED MODE ---")
            result = run_scheduled(get_client, db, dry_run)
            _merge(totals, result)
            logger.info(
                "Scheduled mode complete: processed=%d succeeded=%d failed=%d",
                result["processed"],
                result["succeeded"],
                result["failed"],
            )

        if run_pending_mode:
            logger.info("--- PENDING MODE ---")
            result = run_pending(get_client, db, dry_run)
            _merge(totals, result)
            logger.info(
                "Pending mode complete: processed=%d succeeded=%d failed=%d",
                result["processed"],
                result["succeeded"],
                result["failed"],
            )

    finally:
        db.close()

    end_dt = datetime.datetime.now(datetime.timezone.utc)
    duration_s = time.monotonic() - start_time

    logger.info(
        "=== Run finished at %s | duration=%.1fs | processed=%d succeeded=%d failed=%d ===",
        end_dt.strftime("%Y-%m-%d %H:%M:%S"),
        duration_s,
        totals["processed"],
        totals["succeeded"],
        totals["failed"],
    )

    _print_summary(mode_label, dry_run, totals, duration_s)


if __name__ == "__main__":
    main()
