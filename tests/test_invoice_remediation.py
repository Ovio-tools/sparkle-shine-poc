from scripts import remediate_reconciliation_invoices as remediation


def test_customer_body_prefers_company_name_for_commercial():
    client = {
        "id": "SS-CLIENT-0001",
        "client_type": "commercial",
        "company_name": "Acme Office Park",
        "first_name": "Jane",
        "last_name": "Smith",
        "email": "ops@acme.test",
        "phone": "555-1212",
    }

    body = remediation._customer_body(client)

    assert body["DisplayName"] == "Acme Office Park"
    assert body["CompanyName"] == "Acme Office Park"
    assert body["Notes"] == "SS-ID: SS-CLIENT-0001"


def test_invoice_details_for_residential_job_uses_fixed_price():
    job = {
        "id": "SS-JOB-0001",
        "client_id": "SS-CLIENT-0001",
        "client_type": "residential",
        "service_type_id": "recurring-weekly",
        "service_date": "2026-04-14",
        "scheduled_date": "2026-04-14",
    }

    details = remediation._invoice_details_for_job(job)

    assert details["amount"] == 135.0
    assert details["issue_date"] == "2026-04-14"
    assert details["due_date"] == "2026-04-14"
    assert details["qbo_item_id"] == "22"


def test_pick_unique_candidate_requires_single_exact_amount_match():
    candidates = [
        {"id": "SS-INV-0001", "amount": 150.0},
        {"id": "SS-INV-0002", "amount": 275.0},
    ]

    chosen = remediation._pick_unique_candidate(candidates, 275.0)
    ambiguous = remediation._pick_unique_candidate(candidates + [{"id": "SS-INV-0003", "amount": 275.0}], 275.0)

    assert chosen["id"] == "SS-INV-0002"
    assert ambiguous is None
