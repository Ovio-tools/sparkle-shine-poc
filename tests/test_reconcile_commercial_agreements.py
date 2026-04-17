"""Regression tests for scripts/reconcile_commercial_agreements.py.

Primary regression: the dormant-query used to skip commercial agreements
that had never produced a job (MAX(scheduled_date) IS NULL), so a scheduler
misconfiguration was invisible until someone manually spot-checked. After
the fix, the zero-jobs case is reported as dormant alongside the stale
(>14 days) case.
"""
from __future__ import annotations

import os

import pytest


@pytest.fixture
def reconcile_fn(pg_test_conn, monkeypatch):
    """Point the script's get_connection at the test DB and return reconcile()."""
    os.environ["DATABASE_URL"] = os.environ.get("DATABASE_URL", "")
    from scripts import reconcile_commercial_agreements as mod

    # The script's get_connection reads DATABASE_URL, which pg_test_conn has
    # already set. We still patch its internal reference so a single shared
    # connection is used and our committed fixture rows are visible.
    monkeypatch.setattr(mod, "get_connection", lambda: pg_test_conn)
    yield mod.reconcile


def _seed_commercial_client(conn, client_id: str, company: str):
    with conn:
        conn.execute(
            "INSERT INTO clients (id, first_name, last_name, email, "
            "company_name, client_type, status) "
            "VALUES (%s, 'Contact', 'Person', %s, %s, 'commercial', 'active')",
            (client_id, f"{client_id.lower()}@x.test", company),
        )


def _seed_agreement(conn, agreement_id: str, client_id: str):
    with conn:
        conn.execute(
            "INSERT INTO recurring_agreements (id, client_id, service_type_id, "
            "frequency, price_per_visit, start_date, status, day_of_week) "
            "VALUES (%s, %s, 'commercial-nightly', 'weekly', 450.0, "
            "'2026-01-01', 'active', 'monday,tuesday,wednesday,thursday,friday')",
            (agreement_id, client_id),
        )


def _seed_job(conn, job_id: str, client_id: str, scheduled_date: str):
    with conn:
        conn.execute(
            "INSERT INTO jobs (id, client_id, service_type_id, scheduled_date, "
            "status) VALUES (%s, %s, 'commercial-nightly', %s, 'scheduled')",
            (job_id, client_id, scheduled_date),
        )


# ── The regression this task exists to prevent ────────────────────────────────

def test_active_commercial_agreement_with_zero_jobs_is_flagged_dormant(
    pg_test_conn, reconcile_fn
):
    """Before the fix, a brand-new active commercial agreement with no jobs
    ever scheduled was silently omitted from the dormant list because
    MAX(scheduled_date) IS NULL fails a `<` comparison. After the fix,
    the agreement is surfaced so operators know the scheduler never fired.
    """
    _seed_commercial_client(pg_test_conn, "SS-CLIENT-7001", "Zero Jobs Co")
    _seed_agreement(pg_test_conn, "SS-RECUR-7001", "SS-CLIENT-7001")
    # Deliberately: no jobs seeded for this client.

    report = reconcile_fn()
    dormant_ids = {row["agreement_id"] for row in report["dormant_agreements"]}

    assert "SS-RECUR-7001" in dormant_ids, (
        "Zero-jobs-ever agreement was dropped by the dormant query — "
        "the MAX(scheduled_date) IS NULL case must be surfaced"
    )


def test_agreement_with_recent_job_is_not_flagged_dormant(
    pg_test_conn, reconcile_fn
):
    _seed_commercial_client(pg_test_conn, "SS-CLIENT-7002", "Healthy Co")
    _seed_agreement(pg_test_conn, "SS-RECUR-7002", "SS-CLIENT-7002")
    # Job scheduled today — well within the 14-day dormant window.
    from datetime import date
    _seed_job(pg_test_conn, "SS-JOB-7002", "SS-CLIENT-7002",
              date.today().isoformat())

    report = reconcile_fn()
    dormant_ids = {row["agreement_id"] for row in report["dormant_agreements"]}
    assert "SS-RECUR-7002" not in dormant_ids


def test_agreement_with_stale_job_over_14_days_is_flagged_dormant(
    pg_test_conn, reconcile_fn
):
    from datetime import date, timedelta
    stale_day = (date.today() - timedelta(days=30)).isoformat()

    _seed_commercial_client(pg_test_conn, "SS-CLIENT-7003", "Stale Co")
    _seed_agreement(pg_test_conn, "SS-RECUR-7003", "SS-CLIENT-7003")
    _seed_job(pg_test_conn, "SS-JOB-7003", "SS-CLIENT-7003", stale_day)

    report = reconcile_fn()
    dormant_ids = {row["agreement_id"] for row in report["dormant_agreements"]}
    assert "SS-RECUR-7003" in dormant_ids
