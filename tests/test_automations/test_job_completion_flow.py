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

    row = auto.db.execute(
        "SELECT job_id, status FROM invoices ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert row is not None
    assert row["job_id"] == "SS-JOB-0001"
    assert row["status"] == "sent"


@patch("automations.job_completion_flow.requests.post")
def test_invoice_created_commercial_net30(mock_post, mock_db, mock_clients):
    """
    A commercial job with a resolvable contract rate creates an invoice
    with DueDate = TxnDate + 30 days. We mock the rate resolver because
    the test DB has no recurring_agreements row; without the mock the
    automation would correctly refuse to invoice (see the dedicated
    refuse-to-invoice regression tests below).
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
    with patch(
        "seeding.generators.gen_clients.get_commercial_per_visit_rate",
        return_value=480.0,
    ), patch("automations.base.post_slack_message"):
        auto.run(commercial_job)

    mock_post.assert_called_once()
    body = mock_post.call_args[1]["json"]
    txn  = date.fromisoformat(body["TxnDate"])
    due  = date.fromisoformat(body["DueDate"])
    assert (due - txn).days == 30, "Commercial invoice must be Net-30"


# ─────────────────────────────────────────────────────────────────────────────
# Canonical pricing — one case per service type in config.business.SERVICE_TYPES
# ─────────────────────────────────────────────────────────────────────────────

_CANONICAL_PRICING_CASES = [
    # (canonical service_type_id, expected amount, expected QBO item id)
    ("std-residential",    150.00, "19"),
    ("deep-clean",         275.00, "20"),
    ("move-in-out",        325.00, "21"),
    ("recurring-weekly",   135.00, "22"),
    ("recurring-biweekly", 150.00, "23"),
    ("recurring-monthly",  165.00, "24"),
]


@pytest.mark.parametrize(
    "canonical_id,expected_amount,expected_item_id",
    _CANONICAL_PRICING_CASES,
    ids=[case[0] for case in _CANONICAL_PRICING_CASES],
)
@patch("automations.job_completion_flow.requests.post")
def test_invoice_pricing_canonical_service_types(
    mock_post, mock_db, mock_clients, sample_triggers,
    canonical_id, expected_amount, expected_item_id,
):
    """
    Every residential canonical service ID on the job record must drive its
    own invoice amount and QBO item, regardless of the free-text trigger label.
    """
    mock_post.return_value = _make_qbo_invoice_mock()
    with mock_db:
        mock_db.execute(
            "UPDATE jobs SET service_type_id = %s WHERE id = %s",
            (canonical_id, "SS-JOB-0001"),
        )

    trigger = dict(sample_triggers["completed_job"])
    trigger["service_type"] = "Standard Residential Clean"  # deliberately misleading

    auto = JobCompletionFlow(clients=mock_clients, db=mock_db, dry_run=False)
    with patch("automations.base.post_slack_message"):
        auto.run(trigger)

    body = mock_post.call_args[1]["json"]
    assert body["Line"][0]["Amount"] == expected_amount, (
        f"{canonical_id}: expected ${expected_amount} but got ${body['Line'][0]['Amount']}"
    )
    assert body["Line"][0]["SalesItemLineDetail"]["ItemRef"]["value"] == expected_item_id


# ─────────────────────────────────────────────────────────────────────────────
# commercial-nightly: fallback-only regression test
# ─────────────────────────────────────────────────────────────────────────────

@patch("automations.job_completion_flow.requests.post")
def test_commercial_nightly_uses_contract_rate_not_fallback(
    mock_post, mock_db, mock_clients, sample_triggers
):
    """
    commercial-nightly has no base price in the catalogue, so the automation
    must resolve the per-visit rate from recurring_agreements via
    get_commercial_per_visit_rate(). The returned amount must be the contract
    rate and NEVER the generic $150 fallback.

    We patch only the rate resolver — _lookup_service itself runs for real —
    so this exercises the full canonical-ID + runtime-rate path.
    """
    contract_rate = 461.54
    mock_post.return_value = _make_qbo_invoice_mock()
    with mock_db:
        mock_db.execute(
            "UPDATE jobs SET service_type_id = %s WHERE id = %s",
            ("commercial-nightly", "SS-JOB-0001"),
        )

    trigger = dict(sample_triggers["completed_job"])
    trigger["service_type"] = "Commercial Nightly Clean"

    auto = JobCompletionFlow(clients=mock_clients, db=mock_db, dry_run=False)
    with patch(
        "seeding.generators.gen_clients.get_commercial_per_visit_rate",
        return_value=contract_rate,
    ) as mock_rate, patch("automations.base.post_slack_message"):
        auto.run(trigger)

    mock_rate.assert_called_once()
    kwargs = mock_rate.call_args.kwargs
    assert kwargs["client_id"] == "SS-CLIENT-0001"
    assert kwargs["service_type_id"] == "commercial-nightly"

    body = mock_post.call_args[1]["json"]
    assert body["Line"][0]["Amount"] == contract_rate, (
        "commercial-nightly invoice must use the contract rate, not the fallback"
    )
    assert body["Line"][0]["Amount"] != 150.00, (
        "commercial-nightly must never price at the $150 fallback when a rate is resolvable"
    )
    assert body["Line"][0]["SalesItemLineDetail"]["ItemRef"]["value"] == "25"


# ─────────────────────────────────────────────────────────────────────────────
# commercial-nightly: unresolvable rate must REFUSE to invoice
# ─────────────────────────────────────────────────────────────────────────────

@patch("automations.job_completion_flow.requests.post")
def test_commercial_nightly_no_client_mapping_refuses_invoice(
    mock_post, mock_db, mock_clients
):
    """
    A known commercial-nightly job whose Jobber client has no
    cross_tool_mapping entry must NOT fall back to $150 / residential
    pricing. Instead, the automation:
      1. emits a Slack alert via error_reporter (billing risk),
      2. logs create_quickbooks_invoice as 'failed',
      3. makes NO HTTP call to QuickBooks.
    """
    with mock_db:
        # Upgrade the seeded job to commercial-nightly so the resolver
        # picks up the canonical service type, but trigger with a
        # Jobber client_id that does NOT exist in cross_tool_mapping.
        mock_db.execute(
            "UPDATE jobs SET service_type_id = %s WHERE id = %s",
            ("commercial-nightly", "SS-JOB-0001"),
        )

    trigger = {
        "job_id":           "601",           # maps to SS-JOB-0001
        "client_id":        "UNMAPPED-999",  # intentionally not in mapping
        "service_type":     "Commercial Nightly Clean",
        "duration_minutes": 180,
        "crew":             "Crew A",
        "completion_notes": "",
        "is_recurring":     True,
        "completed_at":     "2026-03-15",
    }

    auto = JobCompletionFlow(clients=mock_clients, db=mock_db, dry_run=False)

    with patch(
        "simulation.error_reporter.report_error", return_value=True
    ) as mock_report, patch("automations.base.post_slack_message"):
        auto.run(trigger)

    # No QuickBooks HTTP call should have been made.
    assert mock_post.call_count == 0, (
        "commercial-nightly without a client mapping must not POST an invoice"
    )

    # A fallback-pricing alert must have been raised for the billing risk.
    mock_report.assert_called()
    assert mock_report.call_args.kwargs.get("tool_name") == "quickbooks"

    # No invoices row should have been written.
    row = mock_db.execute(
        "SELECT COUNT(*) AS n FROM invoices WHERE job_id = %s",
        ("SS-JOB-0001",),
    ).fetchone()
    assert row["n"] == 0, "No invoice row should be written when pricing is unresolved"

    # The automation log should record the failure so ops can find it.
    log = mock_db.execute(
        "SELECT status, error_message FROM automation_log "
        "WHERE action_name = 'create_quickbooks_invoice' "
        "ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert log is not None
    assert log["status"] == "failed"
    assert "commercial-nightly" in (log["error_message"] or "").lower()


@patch("automations.job_completion_flow.requests.post")
def test_commercial_nightly_rate_resolver_error_refuses_invoice(
    mock_post, mock_db, mock_clients, sample_triggers
):
    """
    A known commercial-nightly job whose contract rate cannot be resolved
    (e.g., no matching recurring_agreements row) must also REFUSE to
    invoice at the $150 residential fallback. This guards against the
    silent under-bill that Track C is designed to eliminate.
    """
    with mock_db:
        mock_db.execute(
            "UPDATE jobs SET service_type_id = %s WHERE id = %s",
            ("commercial-nightly", "SS-JOB-0001"),
        )

    trigger = dict(sample_triggers["completed_job"])
    trigger["service_type"] = "Commercial Nightly Clean"

    auto = JobCompletionFlow(clients=mock_clients, db=mock_db, dry_run=False)

    with patch(
        "seeding.generators.gen_clients.get_commercial_per_visit_rate",
        side_effect=ValueError("no matching agreement"),
    ) as mock_rate, patch(
        "simulation.error_reporter.report_error", return_value=True
    ) as mock_report, patch("automations.base.post_slack_message"):
        auto.run(trigger)

    mock_rate.assert_called_once()
    assert mock_post.call_count == 0, (
        "commercial-nightly with unresolvable rate must not POST an invoice"
    )
    mock_report.assert_called()

    # HubSpot outstanding_balance must NOT be incremented when the invoice
    # was skipped, otherwise HS drifts away from QuickBooks.
    hs = mock_clients.hubspot
    hs.crm.contacts.basic_api.update.assert_called_once()
    props = hs.crm.contacts.basic_api.update.call_args[0][1].properties
    # Fixture seeds outstanding_balance at "300.00"; value must stay there
    # (numerically), even if stringification drops trailing zeros.
    assert float(props["outstanding_balance"]) == 300.00, (
        "outstanding_balance must not change when no invoice was created"
    )
    # The service completion itself is still recorded: count increments.
    assert props.get("total_services_completed") == "4"


# ─────────────────────────────────────────────────────────────────────────────
# Fallback-pricing guardrail: unknown service labels must alert ops
# ─────────────────────────────────────────────────────────────────────────────

@patch("automations.job_completion_flow.requests.post")
def test_unknown_service_type_emits_fallback_alert(
    mock_post, mock_db, mock_clients, sample_triggers
):
    """
    When neither the Jobber label nor the job's canonical service_type_id
    matches a known service, the automation must:
      1. fall back to $150 / standard residential,
      2. emit a Slack alert via simulation.error_reporter.report_error so
         ops notice the mispriced invoice in production.
    """
    mock_post.return_value = _make_qbo_invoice_mock()
    with mock_db:
        # Seed service_type_id with a value the catalogue does not recognize
        # (schema requires it to be non-null). The free-text label is also
        # unknown, so the flow must fall back to $150 and alert.
        mock_db.execute(
            "UPDATE jobs SET service_type_id = %s WHERE id = %s",
            ("office-carpet-shampoo", "SS-JOB-0001"),
        )

    trigger = dict(sample_triggers["completed_job"])
    trigger["service_type"] = "Office Carpet Shampoo"  # no alias, unknown

    auto = JobCompletionFlow(clients=mock_clients, db=mock_db, dry_run=False)

    with patch(
        "automations.job_completion_flow.logger.warning"
    ) as mock_warn, patch(
        "simulation.error_reporter.report_error", return_value=True
    ) as mock_report, patch("automations.base.post_slack_message"):
        auto.run(trigger)

    mock_report.assert_called()
    report_call = mock_report.call_args
    assert report_call.kwargs.get("tool_name") == "quickbooks"
    assert report_call.kwargs.get("severity") == "warning"
    assert "Office Carpet Shampoo" in str(report_call.args[0])

    body = mock_post.call_args[1]["json"]
    assert body["Line"][0]["Amount"] == 150.00  # std-residential fallback
    assert body["Line"][0]["SalesItemLineDetail"]["ItemRef"]["value"] == "19"

    # A logger.warning entry is expected as well.
    assert any(
        "fallback pricing" in " ".join(str(a) for a in call.args).lower()
        for call in mock_warn.call_args_list
    )


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
