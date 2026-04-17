from scripts import remediate_reconciliation_invoices as remediation


# ── Orphan-mode match logic (pure function tests, no DB needed) ──────────────

def _make_orphan(amount=150.0, client_type="residential", issue_date="2026-04-09"):
    return {
        "id": "SS-INV-9001",
        "client_id": "SS-CLIENT-0001",
        "client_type": client_type,
        "issue_date": issue_date,
        "amount": amount,
    }


def _make_job(job_id, service_type_id="recurring-weekly", service_date="2026-04-09"):
    return {
        "id": job_id,
        "client_id": "SS-CLIENT-0001",
        "service_type_id": service_type_id,
        "scheduled_date": service_date,
        "service_date": service_date,
    }


def test_match_orphan_returns_no_candidate_when_job_list_empty():
    orphan = _make_orphan()
    matched, reason = remediation._match_orphan_to_job(orphan, [])
    assert matched is None
    assert reason == "no_candidate_job_on_issue_date"


def test_match_orphan_picks_unique_amount_match():
    orphan = _make_orphan(amount=275.0)
    # deep-clean is $275, recurring-weekly is $135.
    candidates = [
        _make_job("SS-JOB-0001", service_type_id="recurring-weekly"),
        _make_job("SS-JOB-0002", service_type_id="deep-clean"),
    ]
    matched, reason = remediation._match_orphan_to_job(orphan, candidates)
    assert matched is not None and matched["id"] == "SS-JOB-0002"
    assert reason == "linked_unique_amount_match"


def test_match_orphan_refuses_ambiguous_amount_collision():
    """Two completed jobs with the same expected amount — the operator
    must resolve this manually rather than letting the script guess and
    corrupt revenue attribution."""
    orphan = _make_orphan(amount=275.0)
    candidates = [
        _make_job("SS-JOB-0001", service_type_id="deep-clean"),
        _make_job("SS-JOB-0002", service_type_id="deep-clean"),
    ]
    matched, reason = remediation._match_orphan_to_job(orphan, candidates)
    assert matched is None
    assert reason == "ambiguous_multiple_amount_matches"


def test_match_orphan_refuses_when_no_candidate_amount_matches():
    orphan = _make_orphan(amount=999.99)
    candidates = [
        _make_job("SS-JOB-0001", service_type_id="recurring-weekly"),
        _make_job("SS-JOB-0002", service_type_id="deep-clean"),
    ]
    matched, reason = remediation._match_orphan_to_job(orphan, candidates)
    assert matched is None
    assert reason == "no_candidate_with_matching_amount"


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
