from datetime import date
from scripts import audit_orphan_invoices as audit


def test_classify_orphan_flags_qbo_mapped_no_job():
    row = {
        "id": "SS-INV-9001",
        "job_id": None,
        "quickbooks_invoice_id": "123456",
        "client_id": "SS-CLIENT-0042",
        "issue_date": "2026-04-09",
        "amount": 150.0,
    }
    assert audit._classify_orphan(row) == "qbo_mapped_no_job"


def test_classify_orphan_flags_local_only():
    row = {
        "id": "SS-INV-9002",
        "job_id": None,
        "quickbooks_invoice_id": None,
        "client_id": "SS-CLIENT-0042",
        "issue_date": "2026-04-09",
        "amount": 150.0,
    }
    assert audit._classify_orphan(row) == "local_only"


def test_group_by_day_sums_amounts():
    rows = [
        {"issue_date": "2026-04-09", "amount": 150.0, "classification": "qbo_mapped_no_job"},
        {"issue_date": "2026-04-09", "amount": 275.0, "classification": "qbo_mapped_no_job"},
        {"issue_date": "2026-04-10", "amount": 135.0, "classification": "local_only"},
    ]
    summary = audit._group_by_day(rows)
    assert summary["2026-04-09"]["count"] == 2
    assert summary["2026-04-09"]["amount"] == 425.0
    assert summary["2026-04-10"]["count"] == 1
