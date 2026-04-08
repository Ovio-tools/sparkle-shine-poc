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
