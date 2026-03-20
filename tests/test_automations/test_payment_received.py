"""
tests/test_automations/test_payment_received.py

Unit tests for Automation 3 — PaymentReceived.
All external API calls are mocked; no real HTTP requests are made.
"""
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest

from automations.payment_received import PaymentReceived
from automations.utils.id_resolver import MappingNotFoundError
from tests.test_automations.conftest import TEST_TOOL_IDS


# ── Per-file autouse ──────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _patch_tool_ids():
    # payment_received does not define _load_tool_ids; this is a no-op placeholder
    # to keep the fixture pattern consistent across all test files.
    yield


# ── Shared fixture ────────────────────────────────────────────────────────────

@pytest.fixture
def auto(mock_db, mock_clients):
    return PaymentReceived(clients=mock_clients, db=mock_db, dry_run=False)


# ─────────────────────────────────────────────────────────────────────────────
# Pipedrive activity
# ─────────────────────────────────────────────────────────────────────────────

def test_pipedrive_activity_created(auto, mock_clients, mock_db, sample_triggers):
    """
    When a payment is received and the client has a Pipedrive deal mapping,
    a 'done' activity is created on that deal.
    """
    with patch("automations.base.post_slack_message"):
        auto.run(sample_triggers["payment"])

    # Pipedrive session.post must be called with /activities URL
    mock_clients.pipedrive.post.assert_called_once()
    url = mock_clients.pipedrive.post.call_args[0][0]
    assert "activities" in url


def test_pipedrive_skipped_when_no_deal(mock_db, mock_clients):
    """
    When the QBO customer has no Pipedrive deal mapping,
    the automation logs 'skipped' (not 'failed') and continues.
    """
    # customer_id "999" is NOT in cross_tool_mapping → canonical_id is None
    # → pipedrive_deal_id is None → action is skipped
    payment_no_deal = {
        "payment_id":  "802",
        "amount":      75.00,
        "date":        "2026-03-15",
        "method":      "check",
        "invoice_id":  "",
        "customer_id": "999",   # unmapped QBO customer
    }

    auto = PaymentReceived(clients=mock_clients, db=mock_db, dry_run=False)
    with patch("automations.base.post_slack_message"):
        auto.run(payment_no_deal)

    # Pipedrive must NOT be called
    mock_clients.pipedrive.post.assert_not_called()

    # automation_log must have status='skipped' for update_pipedrive_deal
    row = mock_db.execute(
        "SELECT status FROM automation_log "
        "WHERE action_name='update_pipedrive_deal' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert row is not None
    assert row["status"] == "skipped"


# ─────────────────────────────────────────────────────────────────────────────
# HubSpot financial properties
# ─────────────────────────────────────────────────────────────────────────────

def test_hubspot_financial_properties_updated(
    auto, mock_clients, sample_triggers
):
    """
    After a payment, HubSpot contact properties are updated:
    last_payment_date, total_payments_received (+=1), outstanding_balance (-=amount).
    """
    with patch("automations.base.post_slack_message"):
        auto.run(sample_triggers["payment"])

    hs = mock_clients.hubspot
    hs.crm.contacts.basic_api.update.assert_called_once()

    update_call = hs.crm.contacts.basic_api.update.call_args
    props = update_call[0][1].properties
    assert "last_payment_date" in props
    assert "total_payments_received" in props
    # Prior mock value = 2; after += 1 → should be "3"
    assert props["total_payments_received"] == "3"
    assert "outstanding_balance" in props
    # Prior = 300.00 - 150.00 (payment amount) = 150.00
    assert float(props["outstanding_balance"]) == pytest.approx(150.00)


# ─────────────────────────────────────────────────────────────────────────────
# Late-payment Slack flag
# ─────────────────────────────────────────────────────────────────────────────

def test_late_payment_flagged_in_slack(mock_db, mock_clients):
    """
    For a commercial client whose payment arrives 45 days after the invoice
    due date, the Slack message includes a late-payment warning.
    """
    # mock_db already has SS-CLIENT-0001 as 'commercial' and QBO customer 401
    # Invoice due date: 45 days before payment date
    payment_date = date(2026, 3, 15)
    due_date     = payment_date - timedelta(days=45)

    # Mock the QBO GET /invoice/{id} call
    mock_invoice_response = MagicMock()
    mock_invoice_response.raise_for_status.return_value = None
    mock_invoice_response.json.return_value = {
        "Invoice": {"Id": "701", "DueDate": due_date.isoformat(), "Balance": 150.0}
    }

    slack_texts = []

    with patch("automations.payment_received._requests.get", return_value=mock_invoice_response), \
         patch("automations.base.post_slack_message") as mock_slack:

        auto = PaymentReceived(clients=mock_clients, db=mock_db, dry_run=False)
        auto.run({
            "payment_id":  "803",
            "amount":      150.00,
            "date":        payment_date.isoformat(),
            "method":      "check",
            "invoice_id":  "701",
            "customer_id": "401",           # → SS-CLIENT-0001 (commercial)
        })

        if mock_slack.called:
            slack_texts = [c[0][2] for c in mock_slack.call_args_list]

    assert any("past due" in t.lower() or "days past" in t for t in slack_texts), (
        "Expected a late-payment warning for a payment 45 days after due date"
    )
