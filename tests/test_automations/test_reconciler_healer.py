"""
tests/test_automations/test_reconciler_healer.py

The reconciler's automation health check must queue create_invoice
pending_actions for uninvoiced completed jobs (self-healing) instead of
only alerting. Uses the PG test database via the mock_db fixture;
Reconciler connects through the same DATABASE_URL.
"""
import json
from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

from simulation.reconciliation.reconciler import Reconciler


def _seed_uninvoiced_job(db, job_id="SS-JOB-0810", hours_ago=30):
    completed_at = (datetime.utcnow() - timedelta(hours=hours_ago)).strftime(
        "%Y-%m-%dT%H:%M:%S"
    )
    with db:
        db.execute(
            "INSERT INTO jobs (id, client_id, service_type_id, scheduled_date, status, completed_at) "
            "VALUES (%s, 'SS-CLIENT-0001', 'std-residential', '2026-06-09', 'completed', %s) "
            "ON CONFLICT DO NOTHING",
            (job_id, completed_at),
        )
    return job_id


def _pending_rows(db, job_id):
    return db.execute(
        "SELECT * FROM pending_actions "
        "WHERE action_name = 'create_invoice' AND trigger_context LIKE %s",
        (f'%"{job_id}"%',),
    ).fetchall()


@pytest.fixture(autouse=True)
def _invoice_seeded_job(mock_db):
    """conftest seeds SS-JOB-0001 as completed and uninvoiced — give it an
    invoice so it doesn't appear in every health-check sweep."""
    with mock_db:
        mock_db.execute(
            "INSERT INTO invoices (id, client_id, job_id, amount, status, issue_date, due_date) "
            "VALUES ('SS-INV-0001A', 'SS-CLIENT-0001', 'SS-JOB-0001', 150.0, 'sent', "
            "'2026-03-15', '2026-03-15') ON CONFLICT DO NOTHING"
        )


@pytest.fixture
def reconciler(mock_db):
    # mock_db prepares/truncates the shared test database; Reconciler opens
    # its own connections against the same DATABASE_URL.
    return Reconciler(dry_run=False)


@patch("simulation.reconciliation.reconciler.report_reconciliation_issue")
def test_uninvoiced_job_queues_create_invoice(mock_report, reconciler, mock_db):
    job_id = _seed_uninvoiced_job(mock_db)

    findings = reconciler.run_automation_health_check()

    assert len(findings) == 1
    rows = _pending_rows(mock_db, job_id)
    assert len(rows) == 1
    assert rows[0]["automation_name"] == "ReconciliationHealer"
    assert json.loads(rows[0]["trigger_context"])["canonical_job_id"] == job_id
    # due immediately (execute_after in the past or now)
    assert rows[0]["execute_after"] <= datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    mock_report.assert_called_once()


@patch("simulation.reconciliation.reconciler.report_reconciliation_issue")
def test_second_sweep_does_not_duplicate_queue(mock_report, reconciler, mock_db):
    job_id = _seed_uninvoiced_job(mock_db)

    reconciler.run_automation_health_check()
    reconciler.run_automation_health_check()

    assert len(_pending_rows(mock_db, job_id)) == 1


@patch("simulation.reconciliation.reconciler.report_reconciliation_issue")
def test_invoiced_job_not_queued(mock_report, reconciler, mock_db):
    job_id = _seed_uninvoiced_job(mock_db, job_id="SS-JOB-0811")
    with mock_db:
        mock_db.execute(
            "INSERT INTO invoices (id, client_id, job_id, amount, status, issue_date, due_date) "
            "VALUES ('SS-INV-0811', 'SS-CLIENT-0001', %s, 150.0, 'sent', '2026-06-09', '2026-06-09')",
            (job_id,),
        )

    findings = reconciler.run_automation_health_check()

    assert findings == []
    assert _pending_rows(mock_db, job_id) == []
    mock_report.assert_not_called()


@patch("simulation.reconciliation.reconciler.report_reconciliation_issue")
def test_dry_run_queues_nothing(mock_report, mock_db):
    job_id = _seed_uninvoiced_job(mock_db, job_id="SS-JOB-0812")

    Reconciler(dry_run=True).run_automation_health_check()

    assert _pending_rows(mock_db, job_id) == []
    mock_report.assert_not_called()
