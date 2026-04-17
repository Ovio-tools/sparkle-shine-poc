"""Regression: orphan invoices (job_id IS NULL) must never count as booked revenue."""
import pytest

from intelligence.metrics.revenue import _sum_booked, _segment_booked


@pytest.fixture
def db_with_orphan_and_linked(pg_test_conn):
    """Seed: 1 completed job with a linked invoice ($150), 1 orphan invoice ($500) on the same day."""
    conn = pg_test_conn
    with conn:
        conn.execute(
            "INSERT INTO clients (id, first_name, last_name, email, client_type, status) "
            "VALUES ('SS-CLIENT-9001', 'Test', 'Client', 't@x.test', 'residential', 'active')"
        )
        conn.execute(
            "INSERT INTO jobs (id, client_id, service_type_id, scheduled_date, "
            "completed_at, status) VALUES "
            "('SS-JOB-9001', 'SS-CLIENT-9001', 'std-residential', '2026-04-10', "
            "'2026-04-10 14:00', 'completed')"
        )
        conn.execute(
            "INSERT INTO invoices (id, client_id, job_id, amount, status, issue_date) "
            "VALUES ('SS-INV-9001', 'SS-CLIENT-9001', 'SS-JOB-9001', 150.0, 'sent', '2026-04-10')"
        )
        conn.execute(
            "INSERT INTO invoices (id, client_id, job_id, amount, status, issue_date) "
            "VALUES ('SS-INV-9002', 'SS-CLIENT-9001', NULL, 500.0, 'sent', '2026-04-10')"
        )
    yield conn


def test_sum_booked_excludes_orphan_invoices(db_with_orphan_and_linked):
    total = _sum_booked(db_with_orphan_and_linked, "2026-04-10")
    assert total == 150.0, f"Expected only the job-linked $150, got ${total}"


def test_segment_booked_excludes_orphan_invoices(db_with_orphan_and_linked):
    segmented = _segment_booked(db_with_orphan_and_linked, "2026-04-10")
    assert segmented.get("residential", 0.0) == 150.0
    assert segmented.get("commercial", 0.0) == 0.0
