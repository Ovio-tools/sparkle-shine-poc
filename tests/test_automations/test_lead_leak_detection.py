"""
tests/test_automations/test_lead_leak_detection.py

Unit tests for Automation 5 — LeadLeakDetection (scheduled, daily).
All external API calls are mocked; no real HTTP requests are made.
"""
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from automations.lead_leak_detection import LeadLeakDetection
from tests.test_automations.conftest import TEST_TOOL_IDS


# ── Per-file autouse ──────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _patch_tool_ids(monkeypatch):
    monkeypatch.setattr(
        "automations.lead_leak_detection._load_tool_ids",
        lambda: TEST_TOOL_IDS,
    )


# ── Shared fixture ────────────────────────────────────────────────────────────

@pytest.fixture
def auto(mock_db, mock_clients):
    return LeadLeakDetection(clients=mock_clients, db=mock_db, dry_run=False)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ts_days_ago(days: int) -> str:
    """Return an ISO-8601 timestamp for N days in the past (UTC)."""
    return (datetime.now(timezone.utc) - timedelta(days=days)).strftime(
        "%Y-%m-%dT%H:%M:%S+00:00"
    )


def _ts_hours_ago(hours: int) -> str:
    """Return an ISO-8601 timestamp for N hours in the past (UTC)."""
    return (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime(
        "%Y-%m-%dT%H:%M:%S+00:00"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Core leak detection
# ─────────────────────────────────────────────────────────────────────────────

@patch("automations.lead_leak_detection.create_tasks")
def test_leaked_leads_detected(
    mock_create_tasks, auto, mock_clients, mock_db, sample_triggers
):
    """
    Given 5 HubSpot leads where 3 are already mapped to Pipedrive deals and
    2 are not, the automation creates 2 Asana follow-up tasks and posts
    a Slack summary.
    """
    mock_create_tasks.return_value = ["gid-a", "gid-b"]

    # Stub _fetch_hubspot_leads to return the 5-lead fixture
    auto._fetch_hubspot_leads = MagicMock(
        return_value=sample_triggers["hs_leads_5_total"]
    )

    # Pipedrive fallback: email search returns no person for leaked leads
    mock_clients.pipedrive.get.return_value.json.return_value = {
        "success": True,
        "data": {"items": []},
    }

    with patch("automations.base.post_slack_message") as mock_slack:
        auto.run()

    # create_tasks called with exactly 2 tasks (one per leaked lead)
    mock_create_tasks.assert_called_once()
    tasks_arg = mock_create_tasks.call_args[1].get("tasks") or mock_create_tasks.call_args[0][3]
    assert len(tasks_arg) == 2, f"Expected 2 tasks for 2 leaked leads, got {len(tasks_arg)}"

    # Slack was called with the new count-only format
    mock_slack.assert_called_once()
    slack_text = mock_slack.call_args[0][2]
    assert "lead qualification opportunity" in slack_text.lower(), (
        f"Expected 'Lead Qualification Opportunity' in Slack message, got: {slack_text!r}"
    )
    assert "2" in slack_text, f"Expected count '2' in Slack message, got: {slack_text!r}"
    # Names must NOT appear in the message
    assert "alex" not in slack_text.lower() and "priya" not in slack_text.lower(), (
        f"Contact names should not appear in Slack message, got: {slack_text!r}"
    )


@patch("automations.lead_leak_detection.create_tasks")
def test_48_hour_grace_period(mock_create_tasks, auto, mock_clients):
    """
    A lead created only 12 hours ago must NOT be flagged as leaked,
    even if it has no Pipedrive deal (grace period: 48 hours).
    """
    mock_create_tasks.return_value = []

    recent_lead = [
        {
            "hubspot_id": "999",
            "email":      "fresh@example.com",
            "firstname":  "Fresh",
            "lastname":   "Lead",
            "lead_source": "Google Ads",
            "createdate": _ts_hours_ago(12),   # only 12 hours old
        }
    ]
    auto._fetch_hubspot_leads = MagicMock(return_value=recent_lead)

    with patch("automations.base.post_slack_message") as mock_slack:
        auto.run()

    # The fresh lead must not appear in any Asana task
    if mock_create_tasks.called:
        tasks_arg = mock_create_tasks.call_args[1].get("tasks") or []
        assert len(tasks_arg) == 0, "Grace-period lead should not generate an Asana task"

    # Slack message should indicate no leaks (positive message)
    mock_slack.assert_called_once()
    slack_text = mock_slack.call_args[0][2]
    assert "no new unqualified" in slack_text.lower() or "no new" in slack_text.lower()


@patch("automations.lead_leak_detection.create_tasks")
def test_no_leaks_posts_positive_message(mock_create_tasks, auto, mock_clients, mock_db):
    """
    When all HubSpot leads are already mapped to Pipedrive, the Slack
    message is the positive 'no leaked leads' message.
    """
    mock_create_tasks.return_value = []

    # Only leads 501/502/503 — all have Pipedrive mappings in mock_db
    all_mapped = [
        {
            "hubspot_id": "501",
            "email":      "jane@example.com",
            "firstname":  "Jane",
            "lastname":   "Smith",
            "lead_source": "Website",
            "createdate": _ts_days_ago(3),
        },
        {
            "hubspot_id": "502",
            "email":      "client2@example.com",
            "firstname":  "Client",
            "lastname":   "Two",
            "lead_source": "Referral",
            "createdate": _ts_days_ago(5),
        },
    ]
    auto._fetch_hubspot_leads = MagicMock(return_value=all_mapped)

    with patch("automations.base.post_slack_message") as mock_slack:
        auto.run()

    mock_slack.assert_called_once()
    slack_text = mock_slack.call_args[0][2]
    assert "no new unqualified" in slack_text.lower() or "no new" in slack_text.lower(), (
        f"Expected a positive no-leaks message, got: {slack_text!r}"
    )


@patch("automations.lead_leak_detection.create_tasks")
def test_deduplication_skips_existing_task(mock_create_tasks, auto, mock_clients):
    """
    When create_tasks runs with deduplicate_by_title=True and a task
    with the same title already exists, no new task is created.
    """
    # create_tasks returns empty list → deduplication caused it to skip
    mock_create_tasks.return_value = []

    leaked_lead = [
        {
            "hubspot_id": "504",
            "email":      "leaked1@example.com",
            "firstname":  "Leaked",
            "lastname":   "One",
            "lead_source": "Facebook Ads",
            "createdate": _ts_days_ago(4),
        }
    ]
    auto._fetch_hubspot_leads = MagicMock(return_value=leaked_lead)

    # Pipedrive fallback: no person found
    mock_clients.pipedrive.get.return_value.json.return_value = {
        "success": True, "data": {"items": []}
    }

    with patch("automations.base.post_slack_message"):
        auto.run()

    # create_tasks must have been called with deduplicate_by_title=True
    mock_create_tasks.assert_called_once()
    call_kwargs = mock_create_tasks.call_args[1]
    assert call_kwargs.get("deduplicate_by_title") is True, (
        "create_tasks must be called with deduplicate_by_title=True for deduplication"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Sentinel file cutoff
# ─────────────────────────────────────────────────────────────────────────────

def test_sentinel_file_cutoff(auto, tmp_path):
    """
    _fetch_hubspot_leads uses the sentinel file's mtime as the createdate
    cutoff when the file exists, and falls back to now-24h when it doesn't.
    """
    import time
    from unittest.mock import patch as _patch
    from hubspot.crm.contacts import PublicObjectSearchRequest

    sentinel = tmp_path / ".lead_leak_last_run"

    # ── Case 1: sentinel exists ───────────────────────────────────────────────
    sentinel.touch()
    expected_mtime_ms = int(sentinel.stat().st_mtime * 1000)

    captured = {}

    def _capture_search(search_request, **kwargs):
        captured["request"] = search_request
        mock_resp = MagicMock()
        mock_resp.results = []
        return mock_resp

    hs_mock = auto.clients("hubspot")
    hs_mock.crm.contacts.search_api.do_search.side_effect = _capture_search

    with _patch("automations.lead_leak_detection._SENTINEL_FILE", str(sentinel)):
        auto._fetch_hubspot_leads()

    filters = captured["request"].filter_groups[0]["filters"]
    cutoff_filter = next(f for f in filters if f["propertyName"] == "createdate")
    assert int(cutoff_filter["value"]) == expected_mtime_ms, (
        "When sentinel exists, cutoff should match sentinel mtime"
    )

    # ── Case 2: sentinel does not exist ──────────────────────────────────────
    missing = tmp_path / ".does_not_exist"
    before_ms = int((datetime.now(timezone.utc) - timedelta(hours=25)).timestamp() * 1000)
    after_ms  = int((datetime.now(timezone.utc) - timedelta(hours=23)).timestamp() * 1000)

    with _patch("automations.lead_leak_detection._SENTINEL_FILE", str(missing)):
        auto._fetch_hubspot_leads()

    filters2 = captured["request"].filter_groups[0]["filters"]
    cutoff_filter2 = next(f for f in filters2 if f["propertyName"] == "createdate")
    cutoff_val = int(cutoff_filter2["value"])
    assert before_ms < cutoff_val < after_ms, (
        f"Without sentinel, cutoff should be ~24h ago; got {cutoff_val}"
    )
