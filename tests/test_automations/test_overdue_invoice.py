"""
tests/test_automations/test_overdue_invoice.py

Unit tests for Automation 6 — OverdueInvoiceEscalation (weekly, Mondays).
All external API calls are mocked; no real HTTP requests are made.
"""
from datetime import date, timedelta
from unittest.mock import MagicMock, patch, call

import pytest

from automations.overdue_invoice import OverdueInvoiceEscalation, _tier
from tests.test_automations.conftest import TEST_TOOL_IDS


# ── Per-file autouse ──────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _patch_tool_ids(monkeypatch):
    monkeypatch.setattr(
        "automations.overdue_invoice._load_tool_ids",
        lambda: TEST_TOOL_IDS,
    )


# ── Shared fixtures ───────────────────────────────────────────────────────────

@pytest.fixture
def auto(mock_db, mock_clients):
    return OverdueInvoiceEscalation(clients=mock_clients, db=mock_db, dry_run=False)


def _overdue_invoice(inv_id, doc_number, balance, days_past_due, qbo_customer_id="999"):
    """Build a minimal QBO invoice dict that is N days past due."""
    due = (date.today() - timedelta(days=days_past_due)).isoformat()
    return {
        "Id":          inv_id,
        "DocNumber":   doc_number,
        "Balance":     balance,
        "DueDate":     due,
        "CustomerRef": {"value": qbo_customer_id},
    }


# ─────────────────────────────────────────────────────────────────────────────
# Tier bucketing
# ─────────────────────────────────────────────────────────────────────────────

def test_aging_buckets_correct():
    """
    Invoices at 15, 35, 65, and 95 days past due map to tiers 1, 2, 3, and 4
    respectively.
    """
    assert _tier(15) == 1,  "15 days → Tier 1"
    assert _tier(30) == 1,  "30 days → Tier 1 (boundary)"
    assert _tier(31) == 2,  "31 days → Tier 2"
    assert _tier(35) == 2,  "35 days → Tier 2"
    assert _tier(60) == 2,  "60 days → Tier 2 (boundary)"
    assert _tier(61) == 3,  "61 days → Tier 3"
    assert _tier(65) == 3,  "65 days → Tier 3"
    assert _tier(90) == 3,  "90 days → Tier 3 (boundary)"
    assert _tier(91) == 4,  "91 days → Tier 4"
    assert _tier(95) == 4,  "95 days → Tier 4"


# ─────────────────────────────────────────────────────────────────────────────
# Asana task creation (tiers)
# ─────────────────────────────────────────────────────────────────────────────

def test_asana_tasks_created_for_tier2_plus(mock_db, mock_clients):
    """
    Asana tasks are created only for Tier 2+ invoices; Tier 1 is watch-list only
    and must NOT result in an Asana task.
    """
    invoices = [
        _overdue_invoice("I-1", "1001", 150.0, 15),  # Tier 1 — no task
        _overdue_invoice("I-2", "1002", 500.0, 35),  # Tier 2 — task expected
    ]

    mock_tasks_api = MagicMock()
    mock_tasks_api.create_task.return_value = {"gid": "mock-gid-t2"}
    mock_tasks_api.get_tasks_for_project.return_value = iter([])   # no existing tasks

    mock_sections_api = MagicMock()

    auto = OverdueInvoiceEscalation(clients=mock_clients, db=mock_db, dry_run=False)
    auto._fetch_overdue_invoices = MagicMock(return_value=invoices)

    with patch("automations.overdue_invoice.asana.TasksApi", return_value=mock_tasks_api), \
         patch("automations.overdue_invoice.asana.SectionsApi", return_value=mock_sections_api), \
         patch("automations.base.post_slack_message"):
        auto.run()

    # create_task should have been called exactly once (for Tier 2 only)
    assert mock_tasks_api.create_task.call_count == 1, (
        "Expected exactly 1 Asana task (Tier 2); Tier 1 should be skipped"
    )


def test_assignee_bookkeeper_for_tier2(mock_db, mock_clients):
    """Tier 2 invoices (31-60 days) are assigned to the bookkeeper."""
    invoices = [_overdue_invoice("I-2", "1002", 500.0, 35)]

    mock_tasks_api = MagicMock()
    mock_tasks_api.create_task.return_value = {"gid": "gid-t2"}
    mock_tasks_api.get_tasks_for_project.return_value = iter([])

    auto = OverdueInvoiceEscalation(clients=mock_clients, db=mock_db, dry_run=False)
    auto._fetch_overdue_invoices = MagicMock(return_value=invoices)

    with patch("automations.overdue_invoice.asana.TasksApi", return_value=mock_tasks_api), \
         patch("automations.overdue_invoice.asana.SectionsApi", return_value=MagicMock()), \
         patch("automations.base.post_slack_message"):
        auto.run()

    mock_tasks_api.create_task.assert_called_once()
    task_body = mock_tasks_api.create_task.call_args[0][0]
    assert task_body["data"]["assignee"] == "sandra.flores@oviodigital.com", (
        "Tier 2 tasks must be assigned to the bookkeeper (sandra.flores)"
    )


def test_assignee_office_manager_for_tier3_plus(mock_db, mock_clients):
    """Tier 3+ invoices (61+ days) are assigned to the office manager."""
    invoices = [
        _overdue_invoice("I-3", "1003", 275.0, 65),   # Tier 3
        _overdue_invoice("I-4", "1004", 800.0, 95),   # Tier 4
    ]

    mock_tasks_api = MagicMock()
    mock_tasks_api.create_task.return_value = {"gid": "gid-t3"}
    mock_tasks_api.get_tasks_for_project.return_value = iter([])

    auto = OverdueInvoiceEscalation(clients=mock_clients, db=mock_db, dry_run=False)
    auto._fetch_overdue_invoices = MagicMock(return_value=invoices)

    with patch("automations.overdue_invoice.asana.TasksApi", return_value=mock_tasks_api), \
         patch("automations.overdue_invoice.asana.SectionsApi", return_value=MagicMock()), \
         patch("automations.base.post_slack_message"):
        auto.run()

    assert mock_tasks_api.create_task.call_count == 2
    for c in mock_tasks_api.create_task.call_args_list:
        assignee = c[0][0]["data"]["assignee"]
        assert assignee == "patricia.nguyen@oviodigital.com", (
            f"Tier 3/4 tasks must be assigned to office manager, got {assignee!r}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Deduplication
# ─────────────────────────────────────────────────────────────────────────────

def test_deduplication_skips_existing_task(mock_db, mock_clients):
    """
    When an open Asana task already references the invoice DocNumber,
    no duplicate task is created (create_task is NOT called).
    """
    invoices = [_overdue_invoice("I-2", "1002", 500.0, 35)]

    existing_task_name = "Overdue invoice: Unknown Client - $500.00 - 35 days past due"
    # Simpler: just include the DocNumber in the existing task name
    mock_tasks_api = MagicMock()
    mock_tasks_api.get_tasks_for_project.return_value = iter([
        {"name": f"Overdue invoice collection for invoice #1002", "completed": False}
    ])

    auto = OverdueInvoiceEscalation(clients=mock_clients, db=mock_db, dry_run=False)
    auto._fetch_overdue_invoices = MagicMock(return_value=invoices)

    with patch("automations.overdue_invoice.asana.TasksApi", return_value=mock_tasks_api), \
         patch("automations.overdue_invoice.asana.SectionsApi", return_value=MagicMock()), \
         patch("automations.base.post_slack_message"):
        auto.run()

    # create_task must NOT be called (duplicate detected)
    mock_tasks_api.create_task.assert_not_called()

    # Log entry should be 'skipped'
    row = mock_db.execute(
        "SELECT status FROM automation_log "
        "WHERE action_name='create_asana_task' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert row is not None
    assert row["status"] == "skipped"


# ─────────────────────────────────────────────────────────────────────────────
# All-clear message
# ─────────────────────────────────────────────────────────────────────────────

def test_no_overdue_posts_positive_message(mock_db, mock_clients):
    """
    When there are no overdue invoices, the Slack message is the positive
    'no overdue invoices' all-clear.
    """
    auto = OverdueInvoiceEscalation(clients=mock_clients, db=mock_db, dry_run=False)
    auto._fetch_overdue_invoices = MagicMock(return_value=[])

    with patch("automations.base.post_slack_message") as mock_slack:
        auto.run()

    mock_slack.assert_called_once()
    slack_text = mock_slack.call_args[0][2]
    assert "no overdue" in slack_text.lower(), (
        f"Expected a positive all-clear message, got: {slack_text!r}"
    )
