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


def _make_jobber_invoice_mock(invoice_id="jobber-inv-123"):
    m = MagicMock()
    m.raise_for_status.return_value = None
    m.json.return_value = {
        "data": {
            "invoiceCreate": {
                "invoice": {
                    "id": invoice_id,
                    "invoiceNumber": "SS-INV-0001",
                    "invoiceStatus": "draft",
                },
                "userErrors": [],
            }
        }
    }
    return m


def _seed_commercial_agreement(
    db,
    *,
    client_id="SS-CLIENT-0001",
    price_per_visit=480.0,
    start_date="2026-03-01",
    end_date=None,
    status="active",
):
    with db:
        db.execute(
            """
            INSERT INTO recurring_agreements
                (id, client_id, service_type_id, frequency, price_per_visit,
                 start_date, end_date, status, day_of_week)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                f"SS-RECUR-{client_id[-4:]}",
                client_id,
                "commercial-nightly",
                "weekly",
                price_per_visit,
                start_date,
                end_date,
                status,
                "monday,tuesday,wednesday,thursday,friday",
            ),
        )


@pytest.fixture(autouse=True)
def _patch_jobber_invoice_writeback(mock_clients):
    def _post(*args, **kwargs):
        payload = kwargs.get("json") or {}
        query = payload.get("query") or ""
        if "invoiceCreate" not in query:
            raise AssertionError(f"Unexpected Jobber GraphQL query in test: {query}")
        return _make_jobber_invoice_mock()

    mock_clients.jobber.post.side_effect = _post


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
    with DueDate = TxnDate + 30 days.
    """
    mock_post.return_value = _make_qbo_invoice_mock()
    _seed_commercial_agreement(mock_db, price_per_visit=480.0)

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


@patch("automations.job_completion_flow.requests.post")
def test_jobber_writeback_creates_draft_invoice_and_mapping(
    mock_post, mock_db, mock_clients, sample_triggers
):
    mock_post.return_value = _make_qbo_invoice_mock(invoice_id="qbo-inv-999")

    auto = JobCompletionFlow(clients=mock_clients, db=mock_db, dry_run=False)
    with patch("automations.base.post_slack_message"):
        auto.run(sample_triggers["completed_job"])

    mock_clients.jobber.post.assert_called_once()
    payload = mock_clients.jobber.post.call_args.kwargs["json"]
    jobber_input = payload["variables"]["input"]
    assert jobber_input["clientId"] == "301"
    assert jobber_input["jobId"] == "601"
    assert jobber_input["invoiceNumber"].startswith("SS-INV-")
    assert jobber_input["markSent"] is False
    assert jobber_input["allowReviewRequest"] is False
    assert jobber_input["tax"]["taxCalculationMethod"] == "EXCLUSIVE"
    assert jobber_input["lineItems"][0]["category"] == "SERVICE"
    assert "qbo-inv-999" in jobber_input["lineItems"][0]["description"]

    inv = mock_db.execute(
        "SELECT id FROM invoices ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert inv is not None

    mapping = mock_db.execute(
        "SELECT tool_specific_id FROM cross_tool_mapping "
        "WHERE canonical_id = %s AND tool_name = 'jobber'",
        (inv["id"],),
    ).fetchone()
    assert mapping is not None
    assert mapping["tool_specific_id"] == "jobber-inv-123"

    log = mock_db.execute(
        "SELECT status, action_target FROM automation_log "
        "WHERE action_name = 'create_jobber_invoice_writeback' "
        "ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert log is not None
    assert log["status"] == "success"
    assert log["action_target"] == "jobber:invoice:jobber-inv-123"


@patch("automations.job_completion_flow.requests.post")
def test_jobber_writeback_failure_is_logged_without_blocking_rest_of_flow(
    mock_post, mock_db, mock_clients, sample_triggers
):
    mock_post.return_value = _make_qbo_invoice_mock()

    failed_jobber = MagicMock()
    failed_jobber.raise_for_status.return_value = None
    failed_jobber.json.return_value = {
        "data": {
            "invoiceCreate": {
                "invoice": None,
                "userErrors": [{"message": "Invoice number has already been taken"}],
            }
        }
    }
    mock_clients.jobber.post.side_effect = lambda *args, **kwargs: failed_jobber

    auto = JobCompletionFlow(clients=mock_clients, db=mock_db, dry_run=False)
    with patch("automations.base.post_slack_message") as mock_slack:
        auto.run(sample_triggers["completed_job"])

    assert mock_slack.called, "Slack summary should still run when Jobber writeback fails"

    inv = mock_db.execute(
        "SELECT id FROM invoices ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert inv is not None
    mapping = mock_db.execute(
        "SELECT tool_specific_id FROM cross_tool_mapping "
        "WHERE canonical_id = %s AND tool_name = 'jobber'",
        (inv["id"],),
    ).fetchone()
    assert mapping is None

    log = mock_db.execute(
        "SELECT status, error_message FROM automation_log "
        "WHERE action_name = 'create_jobber_invoice_writeback' "
        "ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert log is not None
    assert log["status"] == "failed"
    assert "already been taken" in (log["error_message"] or "")


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
    must resolve the per-visit rate from recurring_agreements. The returned
    amount must be the contract rate and NEVER the generic $150 fallback.
    """
    contract_rate = 461.54
    mock_post.return_value = _make_qbo_invoice_mock()
    with mock_db:
        mock_db.execute(
            "UPDATE jobs SET service_type_id = %s WHERE id = %s",
            ("commercial-nightly", "SS-JOB-0001"),
        )
    _seed_commercial_agreement(mock_db, price_per_visit=contract_rate)

    trigger = dict(sample_triggers["completed_job"])
    trigger["service_type"] = "Commercial Nightly Clean"

    auto = JobCompletionFlow(clients=mock_clients, db=mock_db, dry_run=False)
    with patch("automations.base.post_slack_message"):
        auto.run(trigger)

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
        "simulation.error_reporter.report_error", return_value=True
    ) as mock_report, patch("automations.base.post_slack_message"):
        auto.run(trigger)

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


@patch("automations.job_completion_flow.requests.post")
def test_slack_summary_prefers_canonical_crew_name_over_assignees(
    mock_post, mock_db, mock_clients, sample_triggers
):
    mock_post.return_value = _make_qbo_invoice_mock()
    with mock_db:
        mock_db.execute(
            "INSERT INTO crews (id, name, zone) VALUES (%s, %s, %s)",
            ("crew-a", "Crew A", "West Austin"),
        )
        mock_db.execute(
            "UPDATE jobs SET crew_id = %s WHERE id = %s",
            ("crew-a", "SS-JOB-0001"),
        )

    trigger = dict(sample_triggers["completed_job"])
    trigger["jobber_assigned_users"] = [
        {
            "id": "user-1",
            "name": "Claudia Ramirez",
            "email": "claudia.ramirez@oviodigital.com",
        }
    ]

    auto = JobCompletionFlow(clients=mock_clients, db=mock_db, dry_run=False)
    with patch("automations.base.post_slack_message") as mock_slack:
        auto.run(trigger)

    text = mock_slack.call_args[0][2]
    assert "Crew: Crew A" in text
    assert "Claudia Ramirez" not in text


@patch("automations.job_completion_flow.requests.post")
def test_slack_summary_falls_back_to_assignee_names(
    mock_post, mock_db, mock_clients, sample_triggers
):
    mock_post.return_value = _make_qbo_invoice_mock()
    trigger = dict(sample_triggers["completed_job"])
    trigger["crew"] = None
    trigger["jobber_assigned_users"] = [
        {
            "id": "user-1",
            "name": "Claudia Ramirez",
            "email": "claudia.ramirez@oviodigital.com",
        },
        {
            "id": "user-2",
            "name": "Leticia Morales",
            "email": "leticia.morales@oviodigital.com",
        },
    ]

    auto = JobCompletionFlow(clients=mock_clients, db=mock_db, dry_run=False)
    with patch("automations.base.post_slack_message") as mock_slack:
        auto.run(trigger)

    text = mock_slack.call_args[0][2]
    assert "Crew: Claudia Ramirez, Leticia Morales" in text


@patch("automations.job_completion_flow.requests.post")
def test_slack_summary_falls_back_to_canonical_duration(
    mock_post, mock_db, mock_clients, sample_triggers
):
    mock_post.return_value = _make_qbo_invoice_mock()
    with mock_db:
        mock_db.execute(
            "UPDATE jobs SET duration_minutes_actual = %s WHERE id = %s",
            (125, "SS-JOB-0001"),
        )

    trigger = dict(sample_triggers["completed_job"])
    trigger["duration_minutes"] = None

    auto = JobCompletionFlow(clients=mock_clients, db=mock_db, dry_run=False)
    with patch("automations.base.post_slack_message") as mock_slack:
        auto.run(trigger)

    text = mock_slack.call_args[0][2]
    assert "Duration: 125 min" in text


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


# ─────────────────────────────────────────────────────────────────────────────
# Missing QBO customer mapping → queued retry + failure-time alert
# ─────────────────────────────────────────────────────────────────────────────

def _seed_client_without_qbo(db, *, suffix="0090"):
    """Client with jobber mapping but NO quickbooks mapping, plus a completed job."""
    client_id = f"SS-CLIENT-{suffix}"
    job_id = f"SS-JOB-{suffix}"
    with db:
        db.execute(
            "INSERT INTO clients (id, client_type, first_name, last_name, email, status) "
            "VALUES (%s, 'residential', 'No', 'Qbo', %s, 'active') ON CONFLICT DO NOTHING",
            (client_id, f"noqbo{suffix}@example.com"),
        )
        db.execute(
            "INSERT INTO jobs (id, client_id, service_type_id, scheduled_date, status, completed_at) "
            "VALUES (%s, %s, 'std-residential', '2026-06-09', 'completed', '2026-06-09T15:41:00') "
            "ON CONFLICT DO NOTHING",
            (job_id, client_id),
        )
        for cid, etype, tool, tid in [
            (client_id, "CLIENT", "jobber", f"jc-{suffix}"),
            (job_id, "JOB", "jobber", f"jj-{suffix}"),
        ]:
            db.execute(
                "INSERT INTO cross_tool_mapping (canonical_id, entity_type, tool_name, tool_specific_id) "
                "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                (cid, etype, tool, tid),
            )
    return client_id, job_id


def _pending_create_invoice_rows(db, job_id):
    return db.execute(
        "SELECT * FROM pending_actions "
        "WHERE action_name = 'create_invoice' AND trigger_context LIKE %s",
        (f'%"{job_id}"%',),
    ).fetchall()


@patch("simulation.error_reporter.report_error")
@patch("automations.job_completion_flow.requests.post")
def test_missing_qbo_mapping_queues_retry_and_alerts(
    mock_post, mock_report, auto
):
    _, job_id = _seed_client_without_qbo(auto.db)
    event = {
        "job_id": "jj-0090",
        "client_id": "jc-0090",
        "service_type": "Standard Residential Clean",
        "duration_minutes": 120,
        "is_recurring": False,
        "completed_at": "2026-06-09",
    }

    with patch("automations.base.post_slack_message"):
        auto.run(event)

    # No QBO invoice POST happened (mapping check precedes the HTTP call)
    qbo_calls = [c for c in mock_post.call_args_list if "/invoice" in str(c)]
    assert qbo_calls == []

    # A delayed retry was queued exactly once
    rows = _pending_create_invoice_rows(auto.db, job_id)
    assert len(rows) == 1
    assert json.loads(rows[0]["trigger_context"])["canonical_job_id"] == job_id

    # The failure alerted at failure time
    mock_report.assert_called_once()
    assert mock_report.call_args.kwargs["tool_name"] == "quickbooks"

    # And was logged as failed
    log = auto.db.execute(
        "SELECT status FROM automation_log "
        "WHERE action_name = 'create_quickbooks_invoice' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert log["status"] == "failed"


@patch("simulation.error_reporter.report_error")
@patch("automations.job_completion_flow.requests.post")
def test_missing_qbo_mapping_does_not_duplicate_pending(
    mock_post, mock_report, auto
):
    _, job_id = _seed_client_without_qbo(auto.db)
    event = {
        "job_id": "jj-0090",
        "client_id": "jc-0090",
        "service_type": "Standard Residential Clean",
        "is_recurring": False,
        "completed_at": "2026-06-09",
    }
    with patch("automations.base.post_slack_message"):
        auto.run(event)
        auto.run(event)

    assert len(_pending_create_invoice_rows(auto.db, job_id)) == 1


# ─────────────────────────────────────────────────────────────────────────────
# run_invoice_retry
# ─────────────────────────────────────────────────────────────────────────────

@patch("automations.job_completion_flow.requests.post")
def test_run_invoice_retry_creates_invoice(mock_post, auto, sample_triggers):
    mock_post.return_value = _make_qbo_invoice_mock(invoice_id="qbo-retry-777")

    result = auto.run_invoice_retry(sample_triggers["completed_job"])

    assert result == "qbo-retry-777"
    row = auto.db.execute(
        "SELECT id, job_id, status FROM invoices WHERE job_id = 'SS-JOB-0001'"
    ).fetchone()
    assert row is not None
    assert row["status"] == "sent"
    mapping = auto.db.execute(
        "SELECT tool_specific_id FROM cross_tool_mapping "
        "WHERE canonical_id = %s AND tool_name = 'quickbooks'",
        (row["id"],),
    ).fetchone()
    assert mapping["tool_specific_id"] == "qbo-retry-777"


@patch("automations.job_completion_flow.requests.post")
def test_run_invoice_retry_skips_existing_invoice(mock_post, auto, sample_triggers):
    with auto.db:
        auto.db.execute(
            "INSERT INTO invoices (id, client_id, job_id, amount, status, issue_date, due_date) "
            "VALUES ('SS-INV-9001', 'SS-CLIENT-0001', 'SS-JOB-0001', 150.0, 'sent', "
            "'2026-03-15', '2026-03-15')"
        )

    result = auto.run_invoice_retry(sample_triggers["completed_job"])

    assert result == "SS-INV-9001"
    mock_post.assert_not_called()


@patch("automations.job_completion_flow.requests.post")
def test_run_invoice_retry_raises_on_missing_mapping(mock_post, auto):
    """Retry failures must raise so pending mode marks the action failed."""
    from automations.utils.id_resolver import MappingNotFoundError
    _seed_client_without_qbo(auto.db, suffix="0091")
    event = {
        "job_id": "jj-0091",
        "client_id": "jc-0091",
        "service_type": "Standard Residential Clean",
        "is_recurring": False,
        "completed_at": "2026-06-09",
    }
    with pytest.raises(MappingNotFoundError):
        auto.run_invoice_retry(event)
