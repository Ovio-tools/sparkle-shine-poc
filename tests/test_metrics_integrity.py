"""Regression tests for intelligence/metrics/integrity.py.

Covers the three counts surfaced by the daily briefing's DATA INTEGRITY
section (orphan invoices, dangling payments, stale completed jobs) plus the
alert-severity routing.
"""
from datetime import date, datetime, timedelta, timezone

import pytest

from intelligence.metrics import integrity


# ── Seeding helpers ──────────────────────────────────────────────────────────

def _seed_client(conn, client_id="SS-CLIENT-9101", first_name="Test"):
    with conn:
        conn.execute(
            "INSERT INTO clients (id, first_name, last_name, email, "
            "client_type, status) VALUES (%s, %s, 'Client', %s, "
            "'residential', 'active')",
            (client_id, first_name, f"{client_id.lower()}@x.test"),
        )


def _seed_job(conn, job_id, client_id, completed_at, status="completed"):
    with conn:
        conn.execute(
            "INSERT INTO jobs (id, client_id, service_type_id, scheduled_date, "
            "completed_at, status) VALUES (%s, %s, 'std-residential', %s, %s, %s)",
            (job_id, client_id, completed_at[:10], completed_at, status),
        )


def _seed_invoice(conn, invoice_id, client_id, job_id, amount, issue_date):
    with conn:
        conn.execute(
            "INSERT INTO invoices (id, client_id, job_id, amount, status, issue_date) "
            "VALUES (%s, %s, %s, %s, 'sent', %s)",
            (invoice_id, client_id, job_id, amount, issue_date),
        )


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_orphan_invoice_count_and_amount(pg_test_conn):
    _seed_client(pg_test_conn)
    _seed_job(pg_test_conn, "SS-JOB-9101", "SS-CLIENT-9101",
              "2026-04-10 14:00")
    # One job-linked invoice ($150) — does NOT count as orphan.
    _seed_invoice(pg_test_conn, "SS-INV-9101", "SS-CLIENT-9101",
                  "SS-JOB-9101", 150.0, "2026-04-10")
    # Two orphan invoices — these ARE what we want to count.
    _seed_invoice(pg_test_conn, "SS-INV-9102", "SS-CLIENT-9101",
                  None, 500.0, "2026-04-09")
    _seed_invoice(pg_test_conn, "SS-INV-9103", "SS-CLIENT-9101",
                  None, 245.75, "2026-04-09")

    result = integrity.compute(pg_test_conn, "2026-04-17")
    assert result["orphan_invoices"]["count"] == 2
    assert result["orphan_invoices"]["amount"] == pytest.approx(745.75)
    sample_ids = {r["id"] for r in result["orphan_invoices"]["sample"]}
    assert sample_ids == {"SS-INV-9102", "SS-INV-9103"}


def test_no_orphans_no_alert(pg_test_conn):
    _seed_client(pg_test_conn)
    _seed_job(pg_test_conn, "SS-JOB-9201", "SS-CLIENT-9101",
              "2026-04-17 10:00")
    _seed_invoice(pg_test_conn, "SS-INV-9201", "SS-CLIENT-9101",
                  "SS-JOB-9201", 150.0, "2026-04-17")

    result = integrity.compute(pg_test_conn, "2026-04-17")
    assert result["orphan_invoices"]["count"] == 0
    assert result["stale_completed_jobs"]["count"] == 0
    assert result["payments_missing_invoice_link"]["count"] == 0
    assert result["alerts"] == []


def test_stale_completed_job_detected(pg_test_conn):
    _seed_client(pg_test_conn)
    # Completed 48h before briefing date — should be flagged.
    old = (datetime(2026, 4, 15, 10, 0)).isoformat(sep=" ")
    _seed_job(pg_test_conn, "SS-JOB-9301", "SS-CLIENT-9101", old)
    # Completed the morning of the briefing — fresh, should NOT be flagged.
    fresh = (datetime(2026, 4, 17, 8, 0)).isoformat(sep=" ")
    _seed_job(pg_test_conn, "SS-JOB-9302", "SS-CLIENT-9101", fresh)

    result = integrity.compute(pg_test_conn, "2026-04-17")
    assert result["stale_completed_jobs"]["count"] == 1
    assert result["stale_completed_jobs"]["sample"][0]["id"] == "SS-JOB-9301"
    # An alert should be emitted for the stale job.
    assert any("without an invoice for 24h+" in a for a in result["alerts"])


def test_orphan_invoice_alert_severity_crosses_critical_threshold(pg_test_conn):
    _seed_client(pg_test_conn)
    # Seed 60 orphan invoices to cross the 50-count critical threshold.
    for i in range(60):
        _seed_invoice(
            pg_test_conn, f"SS-INV-9{400+i:04d}", "SS-CLIENT-9101",
            None, 100.0, "2026-04-09",
        )

    result = integrity.compute(pg_test_conn, "2026-04-17")
    assert result["orphan_invoices"]["count"] == 60
    # The alert text must contain 'critical' so runner._is_critical picks it up.
    matching = [a for a in result["alerts"] if "orphan" in a or "job_id IS NULL" in a]
    assert matching, f"no orphan alert found in {result['alerts']}"
    assert "critical" in matching[0].lower()


def _seed_quarantine(conn, invoice_id, client_id, issue_date, amount,
                     lane="auto_quarantine", reason="NO_NEARBY_COMPLETED_JOB_3D",
                     released=False):
    with conn:
        conn.execute(
            "INSERT INTO invoice_quarantine "
            "(invoice_id, client_id, issue_date, amount, quarantine_lane, "
            "reason_code, source, snapshot_date, reviewed_by, released_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, 'test', %s, 'test', %s)",
            (invoice_id, client_id, issue_date, amount, lane, reason,
             issue_date, datetime.now(timezone.utc) if released else None),
        )


def test_quarantined_orphans_excluded_from_alert(pg_test_conn):
    """Unreleased quarantine rows must drop out of the orphan count and the
    alert text must expose the quarantined total so operators can reconcile
    the two buckets."""
    _seed_client(pg_test_conn)
    # One unaddressed orphan (will drive the alert).
    _seed_invoice(pg_test_conn, "SS-INV-9501", "SS-CLIENT-9101",
                  None, 150.0, "2026-04-09")
    # Two orphans that operators quarantined. These must NOT count.
    _seed_invoice(pg_test_conn, "SS-INV-9502", "SS-CLIENT-9101",
                  None, 200.0, "2026-04-09")
    _seed_invoice(pg_test_conn, "SS-INV-9503", "SS-CLIENT-9101",
                  None, 300.0, "2026-04-09")
    _seed_quarantine(pg_test_conn, "SS-INV-9502", "SS-CLIENT-9101",
                     "2026-04-09", 200.0)
    _seed_quarantine(pg_test_conn, "SS-INV-9503", "SS-CLIENT-9101",
                     "2026-04-09", 300.0)

    result = integrity.compute(pg_test_conn, "2026-04-17")
    assert result["orphan_invoices"]["count"] == 1
    assert result["orphan_invoices"]["amount"] == pytest.approx(150.0)
    assert result["orphan_invoices"]["quarantined_count"] == 2
    assert result["orphan_invoices"]["quarantined_amount"] == pytest.approx(500.0)
    orphan_alert = next(a for a in result["alerts"] if "job_id IS NULL" in a)
    assert "excludes 2 quarantined" in orphan_alert


def test_released_quarantine_rows_return_to_orphan_count(pg_test_conn):
    """A quarantine row with released_at set means the operator reversed the
    hold, so the invoice should flow back into the orphan count."""
    _seed_client(pg_test_conn)
    _seed_invoice(pg_test_conn, "SS-INV-9601", "SS-CLIENT-9101",
                  None, 175.0, "2026-04-09")
    _seed_quarantine(pg_test_conn, "SS-INV-9601", "SS-CLIENT-9101",
                     "2026-04-09", 175.0, released=True)

    result = integrity.compute(pg_test_conn, "2026-04-17")
    assert result["orphan_invoices"]["count"] == 1
    assert result["orphan_invoices"]["quarantined_count"] == 0


def test_integrity_keys_expose_sample_for_operator(pg_test_conn):
    """The sample arrays are consumed by operators running the
    audit script — they must exist (even if empty) so downstream code
    does not have to guess the schema."""
    result = integrity.compute(pg_test_conn, "2026-04-17")
    for key in ("orphan_invoices", "payments_missing_invoice_link",
                "stale_completed_jobs"):
        assert "sample" in result[key]
        assert isinstance(result[key]["sample"], list)
    assert isinstance(result["alerts"], list)
