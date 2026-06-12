"""
tests/test_automations/test_runner.py

Unit tests for the automation runner (automations/runner.py).
All automation classes and trigger pollers are mocked; no real API calls.
"""
import datetime
import json
from unittest.mock import MagicMock, patch, call

import pytest

from automations.runner import run_poll, run_scheduled, run_pending


# ─────────────────────────────────────────────────────────────────────────────
# Poll mode
# ─────────────────────────────────────────────────────────────────────────────

@patch("automations.runner.time.sleep")
@patch("automations.triggers.poll_sheets_negative_reviews",
       return_value=[{"row_index": 1, "rating": 1, "review_text": "bad"}])
@patch("automations.triggers.poll_quickbooks_payments",
       return_value=[{"payment_id": "p1", "amount": 100.0}])
@patch("automations.triggers.poll_jobber_completed_jobs",
       return_value=[{"job_id": "j1", "client_id": "301"}])
@patch("automations.triggers.poll_pipedrive_won_deals",
       return_value=[{"deal_id": "d1", "contact_name": "Test User"}])
@patch("automations.hubspot_qualified_sync.HubSpotQualifiedSync")
@patch("automations.new_client_onboarding.NewClientOnboarding")
@patch("automations.job_completion_flow.JobCompletionFlow")
@patch("automations.payment_received.PaymentReceived")
@patch("automations.negative_review.NegativeReviewResponse")
def test_poll_mode_fires_all_4_event_automations(
    mock_nr_cls, mock_pr_cls, mock_jcf_cls, mock_nco_cls, mock_hs_cls,
    mock_pipedrive, mock_jobber, mock_qbo, mock_sheets,
    mock_sleep,
    mock_db, mock_clients,
):
    """
    In poll mode, the 4 event-driven automations fire once per trigger event,
    and HubSpotQualifiedSync runs once per poll cycle.
    """
    result = run_poll(mock_clients, mock_db, dry_run=False)

    mock_nco_cls.return_value.run.assert_called_once_with(
        {"deal_id": "d1", "contact_name": "Test User"}
    )
    mock_jcf_cls.return_value.run.assert_called_once_with(
        {"job_id": "j1", "client_id": "301"}
    )
    mock_pr_cls.return_value.run.assert_called_once_with(
        {"payment_id": "p1", "amount": 100.0}
    )
    mock_nr_cls.return_value.run.assert_called_once_with(
        {"row_index": 1, "rating": 1, "review_text": "bad"}
    )
    mock_hs_cls.return_value.run.assert_called_once_with()

    assert result["processed"] == 5
    assert result["succeeded"] == 5
    assert result["failed"] == 0


# ─────────────────────────────────────────────────────────────────────────────
# Scheduled mode
# ─────────────────────────────────────────────────────────────────────────────

@patch("automations.runner.time.sleep")
@patch("automations.overdue_invoice.OverdueInvoiceEscalation")
@patch("automations.lead_leak_detection.LeadLeakDetection")
@patch("automations.runner._should_run_lead_leak", return_value=True)
def test_scheduled_mode_runs_lead_leak(
    mock_should_run, mock_lld_cls, mock_oie_cls, mock_sleep, mock_db, mock_clients
):
    """
    In scheduled mode, LeadLeakDetection.run() is always called regardless
    of the day of the week.
    """
    # Patch to a non-Monday so overdue invoice is skipped
    with patch("automations.runner.datetime") as mock_dt:
        mock_dt.date.today.return_value.weekday.return_value = 2  # Wednesday
        mock_dt.datetime = datetime.datetime   # keep real datetime.datetime
        result = run_scheduled(mock_clients, mock_db, dry_run=False)

    mock_lld_cls.return_value.run.assert_called_once()
    assert result["processed"] >= 1
    assert result["succeeded"] >= 1


@patch("automations.runner.time.sleep")
@patch("automations.overdue_invoice.OverdueInvoiceEscalation")
@patch("automations.lead_leak_detection.LeadLeakDetection")
def test_scheduled_mode_skips_overdue_on_non_monday(
    mock_lld_cls, mock_oie_cls, mock_sleep, mock_db, mock_clients
):
    """
    OverdueInvoiceEscalation must NOT run on any day except Monday (weekday == 0).
    """
    with patch("automations.runner.datetime") as mock_dt:
        mock_dt.date.today.return_value.weekday.return_value = 3  # Thursday
        mock_dt.datetime = datetime.datetime
        run_scheduled(mock_clients, mock_db, dry_run=False)

    mock_oie_cls.return_value.run.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# Pending mode
# ─────────────────────────────────────────────────────────────────────────────

@patch("automations.runner.time.sleep")
@patch("automations.runner._handle_send_review_request")
def test_pending_mode_processes_due_actions(
    mock_handler, mock_sleep, mock_db, mock_clients
):
    """
    Pending actions with execute_after <= now are dispatched and their row
    is updated to status='executed'.
    """
    # Insert a due pending_action (execute_after 1 hour ago)
    past_time = (
        datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=1)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")

    context = json.dumps({"client_email": "test@example.com", "client_name": "Test"})
    with mock_db:
        mock_db.execute(
            """
            INSERT INTO pending_actions
                (automation_name, action_name, trigger_context, execute_after)
            VALUES ('JobCompletionFlow', 'send_review_request', %s, %s)
            """,
            (context, past_time),
        )
    row_id = mock_db.execute(
        "SELECT id FROM pending_actions ORDER BY id DESC LIMIT 1"
    ).fetchone()["id"]

    result = run_pending(mock_clients, mock_db, dry_run=False)

    # Handler was dispatched
    mock_handler.assert_called_once()

    # Row was updated to 'executed'
    updated = mock_db.execute(
        "SELECT status FROM pending_actions WHERE id = %s", (row_id,)
    ).fetchone()
    assert updated["status"] == "executed"

    assert result["processed"] == 1
    assert result["succeeded"] == 1
    assert result["failed"] == 0


@patch("automations.runner.time.sleep")
@patch("automations.runner._handle_send_review_request")
def test_pending_mode_skips_future_actions(
    mock_handler, mock_sleep, mock_db, mock_clients
):
    """
    Pending actions with execute_after > now are NOT dispatched.
    """
    future_time = (
        datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=47)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")

    context = json.dumps({"client_email": "future@example.com"})
    with mock_db:
        mock_db.execute(
            """
            INSERT INTO pending_actions
                (automation_name, action_name, trigger_context, execute_after)
            VALUES ('JobCompletionFlow', 'send_review_request', %s, %s)
            """,
            (context, future_time),
        )

    result = run_pending(mock_clients, mock_db, dry_run=False)

    mock_handler.assert_not_called()
    assert result["processed"] == 0


# ─────────────────────────────────────────────────────────────────────────────
# Error isolation
# ─────────────────────────────────────────────────────────────────────────────

@patch("automations.runner.time.sleep")
@patch("automations.triggers.poll_sheets_negative_reviews",
       return_value=[{"row_index": 1}])
@patch("automations.triggers.poll_quickbooks_payments",
       return_value=[{"payment_id": "p1"}])
@patch("automations.triggers.poll_jobber_completed_jobs",
       return_value=[{"job_id": "j1"}])
@patch("automations.triggers.poll_pipedrive_won_deals",
       return_value=[{"deal_id": "d1"}])
@patch("automations.hubspot_qualified_sync.HubSpotQualifiedSync")
@patch("automations.new_client_onboarding.NewClientOnboarding")
@patch("automations.job_completion_flow.JobCompletionFlow")
@patch("automations.payment_received.PaymentReceived")
@patch("automations.negative_review.NegativeReviewResponse")
def test_error_isolation_one_failure_does_not_block_others(
    mock_nr_cls, mock_pr_cls, mock_jcf_cls, mock_nco_cls, mock_hs_cls,
    mock_pipedrive, mock_jobber, mock_qbo, mock_sheets,
    mock_sleep,
    mock_db, mock_clients,
):
    """
    When the first automation raises an unhandled exception, the runner
    catches it and continues processing the remaining automations.
    """
    # First automation (onboarding) blows up
    mock_nco_cls.return_value.run.side_effect = RuntimeError("Onboarding exploded")

    result = run_poll(mock_clients, mock_db, dry_run=False)

    # Onboarding failed but the remaining four automations still ran
    assert result["processed"] == 5
    assert result["failed"] == 1
    assert result["succeeded"] == 4

    # The other automations still ran
    mock_jcf_cls.return_value.run.assert_called_once()
    mock_pr_cls.return_value.run.assert_called_once()
    mock_nr_cls.return_value.run.assert_called_once()
    mock_hs_cls.return_value.run.assert_called_once_with()


# ─────────────────────────────────────────────────────────────────────────────
# Pending handlers: create_invoice / create_qbo_customer
# ─────────────────────────────────────────────────────────────────────────────

def _enqueue(db, action_name, context, automation="Test"):
    db_now = "2020-01-01T00:00:00Z"  # long past → due immediately
    with db:
        db.execute(
            "INSERT INTO pending_actions "
            "(automation_name, action_name, trigger_context, execute_after) "
            "VALUES (%s, %s, %s, %s)",
            (automation, action_name, json.dumps(context), db_now),
        )


def test_pending_create_invoice_dispatches_run_invoice_retry(
    mock_clients, mock_db
):
    """create_invoice pending actions resolve Jobber IDs and call run_invoice_retry."""
    _enqueue(mock_db, "create_invoice", {"canonical_job_id": "SS-JOB-0001"})

    with patch(
        "automations.job_completion_flow.JobCompletionFlow.run_invoice_retry",
        return_value="qbo-inv-1",
    ) as mock_retry:
        result = run_pending(mock_clients, mock_db, dry_run=False)

    assert result["succeeded"] == 1
    event = mock_retry.call_args.args[0]
    assert event["job_id"] == "601"      # jobber mapping of SS-JOB-0001
    assert event["client_id"] == "301"   # jobber mapping of SS-CLIENT-0001
    assert str(event["completed_at"]).startswith("2026-03-15")

    row = mock_db.execute(
        "SELECT status FROM pending_actions ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert row["status"] == "executed"


def test_pending_create_invoice_fails_without_jobber_mapping(
    mock_clients, mock_db
):
    """A job we can't tie back to Jobber must not be invoiced blindly."""
    with mock_db:
        mock_db.execute(
            "INSERT INTO jobs (id, client_id, service_type_id, scheduled_date, status, completed_at) "
            "VALUES ('SS-JOB-0077', 'SS-CLIENT-0001', 'std-residential', "
            "'2026-06-01', 'completed', '2026-06-01T12:00:00') ON CONFLICT DO NOTHING"
        )
    _enqueue(mock_db, "create_invoice", {"canonical_job_id": "SS-JOB-0077"})

    result = run_pending(mock_clients, mock_db, dry_run=False)

    assert result["failed"] == 1
    row = mock_db.execute(
        "SELECT status FROM pending_actions ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert row["status"] == "failed"


def test_pending_create_invoice_skips_uncompleted_job(mock_clients, mock_db):
    with mock_db:
        mock_db.execute(
            "INSERT INTO jobs (id, client_id, service_type_id, scheduled_date, status) "
            "VALUES ('SS-JOB-0078', 'SS-CLIENT-0001', 'std-residential', "
            "'2026-07-01', 'scheduled') ON CONFLICT DO NOTHING"
        )
    _enqueue(mock_db, "create_invoice", {"canonical_job_id": "SS-JOB-0078"})

    with patch(
        "automations.job_completion_flow.JobCompletionFlow.run_invoice_retry"
    ) as mock_retry:
        result = run_pending(mock_clients, mock_db, dry_run=False)

    mock_retry.assert_not_called()
    assert result["succeeded"] == 1  # nothing to do counts as handled


def test_pending_create_qbo_customer_dispatches_retry(mock_clients, mock_db):
    _enqueue(mock_db, "create_qbo_customer", {"canonical_id": "SS-CLIENT-0001"})

    with patch(
        "automations.new_client_onboarding.NewClientOnboarding.retry_quickbooks_customer",
        return_value="qbo-cust-1",
    ) as mock_retry:
        result = run_pending(mock_clients, mock_db, dry_run=False)

    assert result["succeeded"] == 1
    mock_retry.assert_called_once_with("SS-CLIENT-0001")
