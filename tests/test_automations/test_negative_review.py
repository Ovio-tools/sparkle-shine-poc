"""
tests/test_automations/test_negative_review.py

Unit tests for Automation 4 — NegativeReviewResponse.
All external API calls are mocked; no real HTTP requests are made.
"""
from unittest.mock import MagicMock, patch

import pytest

from automations.negative_review import NegativeReviewResponse, _truncate
from tests.test_automations.conftest import TEST_TOOL_IDS


# ── Per-file autouse ──────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _patch_tool_ids(monkeypatch):
    monkeypatch.setattr(
        "automations.negative_review._load_tool_ids",
        lambda: TEST_TOOL_IDS,
    )


# ── Shared fixture ────────────────────────────────────────────────────────────

@pytest.fixture
def auto(mock_db, mock_clients):
    return NegativeReviewResponse(clients=mock_clients, db=mock_db, dry_run=False)


# ─────────────────────────────────────────────────────────────────────────────
# Slack alert
# ─────────────────────────────────────────────────────────────────────────────

@patch("automations.negative_review.create_tasks")
def test_slack_alert_posted_to_operations(
    mock_create_tasks, auto, mock_clients, sample_triggers
):
    """A Slack alert is posted to #operations for a negative review."""
    mock_create_tasks.return_value = ["gid-alert-1"]

    with patch("automations.base.post_slack_message") as mock_slack:
        auto.run(sample_triggers["negative_review"])

    mock_slack.assert_called()
    # The first Slack call must go to #operations
    channel = mock_slack.call_args_list[0][0][1]
    assert channel == "operations"


@patch("automations.negative_review.create_tasks")
def test_review_text_truncated_to_150_chars(
    mock_create_tasks, auto, mock_clients
):
    """
    Review text longer than 150 characters is truncated before being included
    in the Slack message.
    """
    mock_create_tasks.return_value = ["gid-1"]

    long_review = "X" * 300   # 300 chars, well over the 150-char limit

    trigger = {
        "row_index":   10,
        "date":        "2026-03-15",
        "client_name": "Test Client",
        "client_email": "test@example.com",
        "rating":      1,
        "review_text": long_review,
        "crew":        "Crew A",
        "service_type": "Standard Residential Clean",
    }

    with patch("automations.base.post_slack_message") as mock_slack:
        auto.run(trigger)

    mock_slack.assert_called()
    slack_text = mock_slack.call_args_list[0][0][2]

    # The truncated portion must not exceed 150 chars (plus the quotes/ellipsis)
    # _truncate returns at most 150 chars of the review text
    assert long_review[:151] not in slack_text, (
        "Review text in Slack message was not truncated to 150 characters"
    )
    # Truncated text ends with "..."
    assert "..." in slack_text


# ─────────────────────────────────────────────────────────────────────────────
# Asana task creation
# ─────────────────────────────────────────────────────────────────────────────

@patch("automations.negative_review.create_tasks")
def test_asana_task_created_in_client_success(
    mock_create_tasks, auto, mock_clients, sample_triggers
):
    """
    An urgent Asana task is created in the 'Client Success → At Risk' section.
    """
    mock_create_tasks.return_value = ["gid-at-risk-1"]

    with patch("automations.base.post_slack_message"):
        auto.run(sample_triggers["negative_review"])

    mock_create_tasks.assert_called_once()
    call_kwargs = mock_create_tasks.call_args[1]
    assert call_kwargs.get("project_name") == "Client Success"
    assert call_kwargs.get("section_name") == "At Risk"


# ─────────────────────────────────────────────────────────────────────────────
# HubSpot at-risk flagging
# ─────────────────────────────────────────────────────────────────────────────

@patch("automations.negative_review.create_tasks")
def test_hubspot_contact_flagged_at_risk(
    mock_create_tasks, auto, mock_clients, sample_triggers
):
    """
    The HubSpot contact matching the reviewer's email is flagged with
    at_risk=true and a note is created.
    """
    mock_create_tasks.return_value = ["gid-1"]

    with patch("automations.base.post_slack_message"):
        auto.run(sample_triggers["negative_review"])

    hs = mock_clients.hubspot

    # Contact should be updated with at_risk = "true"
    hs.crm.contacts.basic_api.update.assert_called_once()
    update_call = hs.crm.contacts.basic_api.update.call_args
    props = update_call[0][1].properties
    assert props.get("at_risk") == "true"

    # A note should have been created on the contact
    hs.crm.objects.notes.basic_api.create.assert_called_once()


@patch("automations.negative_review.create_tasks")
def test_hubspot_not_found_logs_warning_without_crash(
    mock_create_tasks, mock_db, mock_clients
):
    """
    When HubSpot returns no contact for the reviewer's email, the automation
    logs a warning (status='skipped') but does NOT raise or stop execution.
    """
    mock_create_tasks.return_value = ["gid-1"]

    # Configure HubSpot search to return no results
    empty_result = MagicMock()
    empty_result.results = []
    mock_clients.hubspot.crm.contacts.search_api.do_search.return_value = empty_result

    trigger = {
        "row_index":    99,
        "date":         "2026-03-15",
        "client_name":  "Ghost Client",
        "client_email": "ghost.nobodyhome@example.com",
        "rating":       1,
        "review_text":  "Bad service.",
        "crew":         "Crew C",
        "service_type": "Deep Clean",
    }

    auto = NegativeReviewResponse(clients=mock_clients, db=mock_db, dry_run=False)
    with patch("automations.base.post_slack_message"):
        # Must not raise
        auto.run(trigger)

    # automation_log should have a 'skipped' (not 'failed') entry for HubSpot action
    row = mock_db.execute(
        "SELECT status FROM automation_log "
        "WHERE action_name='flag_hubspot_contact' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert row is not None
    assert row["status"] == "skipped", (
        "HubSpot contact-not-found should be logged as 'skipped', not 'failed'"
    )

    # Slack and Asana must still have been called (error isolation)
    mock_create_tasks.assert_called_once()
