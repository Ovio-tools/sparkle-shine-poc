"""
tests/test_automations/test_job_completion_flow.py

Unit tests for Automation 2 — JobCompletionFlow.
All external API calls are mocked; no real HTTP requests are made.
"""
import json
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest

from automations.job_completion_flow import JobCompletionFlow
from tests.test_automations.conftest import TEST_TOOL_IDS


# ── Per-file autouse: never read tool_ids.json from disk ─────────────────────

@pytest.fixture(autouse=True)
def _patch_tool_ids(monkeypatch):
    monkeypatch.setattr(
        "automations.job_completion_flow._load_tool_ids",
        lambda: TEST_TOOL_IDS,
    )


# ── Shared fixture ────────────────────────────────────────────────────────────

@pytest.fixture
def auto(mock_db, mock_clients):
    return JobCompletionFlow(clients=mock_clients, db=mock_db, dry_run=False)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_qbo_invoice_mock(invoice_id="qbo-inv-123"):
    m = MagicMock()
    m.raise_for_status.return_value = None
    m.json.return_value = {"Invoice": {"Id": invoice_id}}
    return m


# ─────────────────────────────────────────────────────────────────────────────
# Invoice creation — payment terms
# ─────────────────────────────────────────────────────────────────────────────

@patch("automations.job_completion_flow.requests.post")
def test_invoice_created_residential_due_on_receipt(
    mock_post, auto, sample_triggers
):
    """
    A non-commercial job creates an invoice with DueDate == TxnDate
    (due on receipt).
    """
    mock_post.return_value = _make_qbo_invoice_mock()

    with patch("automations.base.post_slack_message"):
        auto.run(sample_triggers["completed_job"])

    mock_post.assert_called_once()
    body = mock_post.call_args[1]["json"]
    assert body["DueDate"] == body["TxnDate"], (
        "Residential invoice DueDate should equal TxnDate (due on receipt)"
    )


@patch("automations.job_completion_flow.requests.post")
def test_invoice_created_commercial_net30(mock_post, mock_db, mock_clients):
    """
    A commercial job creates an invoice with DueDate = TxnDate + 30 days.
    """
    mock_post.return_value = _make_qbo_invoice_mock()

    commercial_job = {
        "job_id":           "602",
        "client_id":        "301",           # → SS-CLIENT-0001
        "service_type":     "Commercial Nightly Clean",
        "duration_minutes": 180,
        "crew":             "Crew A",
        "completed_at":     "2026-03-15",
    }

    auto = JobCompletionFlow(clients=mock_clients, db=mock_db, dry_run=False)
    with patch("automations.base.post_slack_message"):
        auto.run(commercial_job)

    mock_post.assert_called_once()
    body = mock_post.call_args[1]["json"]
    txn  = date.fromisoformat(body["TxnDate"])
    due  = date.fromisoformat(body["DueDate"])
    assert (due - txn).days == 30, "Commercial invoice must be Net-30"


# ─────────────────────────────────────────────────────────────────────────────
# 48-hour delayed review request
# ─────────────────────────────────────────────────────────────────────────────

@patch("automations.job_completion_flow.requests.post")
def test_review_request_delayed_48h(mock_post, auto, mock_clients, mock_db, sample_triggers):
    """
    After job completion, a pending_actions row is inserted for
    'send_review_request' — and NO Mailchimp API call is made immediately.
    """
    mock_post.return_value = _make_qbo_invoice_mock()

    with patch("automations.base.post_slack_message"):
        auto.run(sample_triggers["completed_job"])

    # A pending_actions row must exist
    row = mock_db.execute(
        "SELECT action_name, status FROM pending_actions "
        "WHERE action_name = 'send_review_request' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert row is not None, "No pending_action row for send_review_request"
    assert row["status"] == "pending"

    # Mailchimp must NOT have been called yet
    mock_clients.mailchimp.lists.update_list_member_tags.assert_not_called()
    mock_clients.mailchimp.lists.set_list_member.assert_not_called()


def test_review_request_skipped_when_client_email_missing(auto, mock_db):
    """
    Jobs without a client email must not enqueue a broken pending action.
    """
    auto._action_schedule_review_request(
        {
            "canonical_id": "SS-CLIENT-0001",
            "client_email": "",
            "client_name": "No Email Client",
            "service_type": "Standard Residential Clean",
            "completion_date": date(2026, 3, 15),
            "job_id": "SS-JOB-0999",
        }
    )

    row = mock_db.execute(
        "SELECT action_name FROM pending_actions "
        "WHERE action_name = 'send_review_request' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert row is None, "Missing-email jobs should not create pending review requests"


# ─────────────────────────────────────────────────────────────────────────────
# HubSpot engagement
# ─────────────────────────────────────────────────────────────────────────────

@patch("automations.job_completion_flow.requests.post")
def test_hubspot_note_created_and_properties_updated(
    mock_post, auto, mock_clients, sample_triggers
):
    """
    HubSpot note is created and contact properties (last_service_date,
    total_services_completed) are updated after job completion.
    """
    mock_post.return_value = _make_qbo_invoice_mock()

    with patch("automations.base.post_slack_message"):
        auto.run(sample_triggers["completed_job"])

    hs = mock_clients.hubspot

    # A note must be created via the notes API
    hs.crm.objects.notes.basic_api.create.assert_called_once()

    # Contact properties must be updated (last_service_date, total_services_completed)
    hs.crm.contacts.basic_api.update.assert_called_once()
    update_call = hs.crm.contacts.basic_api.update.call_args
    props_obj = update_call[0][1]          # SimplePublicObjectInput arg
    props = props_obj.properties
    assert "last_service_date" in props
    assert "total_services_completed" in props
    # total_services_completed should be prior value + 1 = 3 + 1 = 4
    assert props["total_services_completed"] == "4"


# ─────────────────────────────────────────────────────────────────────────────
# Duration variance flagging
# ─────────────────────────────────────────────────────────────────────────────

@patch("automations.job_completion_flow.requests.post")
def test_duration_variance_flagged_over_20_percent(
    mock_post, mock_db, mock_clients
):
    """
    When actual duration is >20% over expected, the Slack message includes
    a duration variance warning.
    """
    mock_post.return_value = _make_qbo_invoice_mock()

    # Standard Residential expected = 120 min; 150 min = +25% → flagged
    job_event = {
        "job_id":           "603",
        "client_id":        "301",
        "service_type":     "Standard Residential Clean",
        "duration_minutes": 150,
        "crew":             "Crew A",
        "completed_at":     "2026-03-15",
    }

    auto = JobCompletionFlow(clients=mock_clients, db=mock_db, dry_run=False)
    captured_texts = []

    with patch("automations.base.post_slack_message") as mock_slack:
        auto.run(job_event)
        if mock_slack.called:
            captured_texts = [c[0][2] for c in mock_slack.call_args_list]

    assert any("variance" in t.lower() or "warning" in t.lower() or "Duration variance" in t
               for t in captured_texts), (
        "Expected a duration-variance warning in the Slack message for +25% over"
    )


@patch("automations.job_completion_flow.requests.post")
def test_duration_variance_not_flagged_within_20_percent(
    mock_post, mock_db, mock_clients, sample_triggers
):
    """
    When actual duration is within ±20% of expected, NO variance warning
    is included in the Slack message.
    """
    mock_post.return_value = _make_qbo_invoice_mock()

    # Standard Residential expected = 120 min; 130 min = +8.3% → within 20%, not flagged
    auto = JobCompletionFlow(clients=mock_clients, db=mock_db, dry_run=False)
    captured_texts = []

    with patch("automations.base.post_slack_message") as mock_slack:
        auto.run(sample_triggers["completed_job"])
        if mock_slack.called:
            captured_texts = [c[0][2] for c in mock_slack.call_args_list]

    assert not any("Duration variance" in t for t in captured_texts), (
        "Did not expect a duration-variance warning for 8.3% deviation"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Fix 7: unknown Jobber client logs a resolve_canonical_id/failed entry
# ─────────────────────────────────────────────────────────────────────────────

@patch("automations.job_completion_flow.requests.post")
def test_unknown_jobber_client_logs_warning(mock_post, mock_db, mock_clients):
    """
    When the Jobber client_id has no cross_tool_mapping entry, run() must log
    a 'resolve_canonical_id' / 'failed' row so operators can identify the root
    cause of all downstream action failures.
    """
    mock_post.return_value = _make_qbo_invoice_mock()

    auto = JobCompletionFlow(clients=mock_clients, db=mock_db, dry_run=False)

    unmapped_trigger = {
        "job_id":           "999",
        "client_id":        "UNMAPPED-999",   # not in cross_tool_mapping
        "service_type":     "Standard Residential Clean",
        "duration_minutes": 120,
        "crew":             "Crew A",
        "completion_notes": "",
        "is_recurring":     False,
        "completed_at":     "2026-03-25",
    }

    with patch("automations.base.post_slack_message"):
        auto.run(unmapped_trigger)

    row = mock_db.execute(
        "SELECT status, error_message FROM automation_log "
        "WHERE action_name = 'resolve_canonical_id'"
    ).fetchone()

    assert row is not None, "Expected a resolve_canonical_id log entry"
    assert row["status"] == "failed"
    assert "UNMAPPED-999" in (row["error_message"] or "")
