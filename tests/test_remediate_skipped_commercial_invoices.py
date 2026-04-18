from types import SimpleNamespace
from unittest.mock import MagicMock

from scripts.remediate_skipped_commercial_invoices import (
    TargetJob,
    _build_trigger_event,
    _increment_hubspot_outstanding_balance,
    _validate_target,
)


def _target(**overrides) -> TargetJob:
    data = dict(
        canonical_job_id="SS-JOB-5553",
        canonical_client_id="SS-CLIENT-0313",
        company_name="Mueller Tech Suites",
        job_title_raw="Commercial Nightly Clean - Mueller Tech Suites",
        service_type_id="commercial-nightly",
        duration_minutes_actual=180,
        crew_id="crew-d",
        completed_at="2026-04-17T20:48:41.991187",
        jobber_job_id="jobber-job-1",
        jobber_client_id="jobber-client-1",
        qbo_customer_id="qbo-customer-1",
        hs_contact_id="hs-contact-1",
        invoice_count=0,
    )
    data.update(overrides)
    return TargetJob(**data)


def test_validate_target_rejects_existing_invoice():
    target = _target(invoice_count=1)
    try:
        _validate_target(target)
    except RuntimeError as exc:
        assert "already has 1 linked invoice" in str(exc)
    else:
        raise AssertionError("expected RuntimeError for invoice_count>0")


def test_build_trigger_event_uses_existing_job_data():
    target = _target()
    event = _build_trigger_event(target)
    assert event["job_id"] == "jobber-job-1"
    assert event["client_id"] == "jobber-client-1"
    assert event["service_type"] == "Commercial Nightly Clean - Mueller Tech Suites"
    assert event["completed_at"] == "2026-04-17"


def test_increment_hubspot_outstanding_balance_updates_balance():
    hs_client = MagicMock()
    hs_client.crm.contacts.basic_api.get_by_id.return_value = SimpleNamespace(
        properties={"outstanding_balance": "300.00"}
    )

    _increment_hubspot_outstanding_balance(
        hs_client, "hs-contact-1", 538.46, dry_run=False
    )

    hs_client.crm.contacts.basic_api.update.assert_called_once()
    props = hs_client.crm.contacts.basic_api.update.call_args.args[1].properties
    assert float(props["outstanding_balance"]) == 838.46
