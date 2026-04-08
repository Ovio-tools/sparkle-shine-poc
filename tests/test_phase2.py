"""
Phase 2 integration tests for Sparkle & Shine POC.

Verifies that all Phase 2 push operations (Jobber, QuickBooks, HubSpot,
Mailchimp, Pipedrive, Asana) completed successfully and that the seeded
data matches expected volumes and analytical patterns.

Run with:
    pytest tests/test_phase2.py -v --tb=short
    python tests/test_phase2.py          # uses __main__ block below

Test sections:
  1. Volume Checks        — live API calls to each tool
  2. Mapping Completeness — find_unmapped() returns [] for each tool
  3. Discovery Patterns   — direct DB queries for planted signals
  4. Financial Integrity  — revenue range and AR aging checks
"""

import json
import os
import sys
from datetime import date

import pytest
import requests
from dotenv import dotenv_values

# ------------------------------------------------------------------ #
# Path setup (also done in conftest, but kept here for direct runs)
# ------------------------------------------------------------------ #
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

_TOOL_IDS_PATH  = os.path.join(_PROJECT_ROOT, "config", "tool_ids.json")
_JOBBER_API_URL = "https://api.getjobber.com/api/graphql"
_HUBSPOT_BASE   = os.getenv("HUBSPOT_BASE_URL", "https://api.hubapi.com")

with open(_TOOL_IDS_PATH) as _f:
    _TOOL_IDS = json.load(_f)

_PIPEDRIVE_PIPELINE_ID = _TOOL_IDS["pipedrive"]["pipelines"]["Cleaning Services Sales"]
_ASANA_PROJECT_GIDS    = _TOOL_IDS["asana"]["projects"]   # {name: gid}
_MAILCHIMP_AUDIENCE_ID = _TOOL_IDS["mailchimp"]["audience_id"]
_REAL_ENV = dotenv_values(os.path.join(_PROJECT_ROOT, ".env"))
_INTEGRATION_DB_URL = _REAL_ENV.get("DATABASE_URL")


# ------------------------------------------------------------------ #
# Shared helpers
# ------------------------------------------------------------------ #

def _db():
    from database.schema import get_connection
    return get_connection()


def _pd_base(session) -> str:
    base = getattr(session, "base_url", "https://api.pipedrive.com/v1").rstrip("/")
    if not any(seg in base for seg in ("/v1", "/v2")):
        base = f"{base}/v1"
    return base


def _jobber_gql(session, query: str) -> dict:
    resp = session.post(_JOBBER_API_URL, json={"query": query}, timeout=30)
    resp.raise_for_status()
    return resp.json()


@pytest.fixture(scope="module", autouse=True)
def _use_integration_database():
    """Phase 2 uses the seeded integration DB, not the session test DB."""
    if not _INTEGRATION_DB_URL:
        pytest.skip("DATABASE_URL is not configured in .env for Phase 2 integration tests")

    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = _INTEGRATION_DB_URL
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = previous


# ------------------------------------------------------------------ #
# Test class
# ------------------------------------------------------------------ #
class TestPhase2Integration:

    # ================================================================ #
    # 1. VOLUME CHECKS — live API calls
    # ================================================================ #

    def test_jobber_client_count(self):
        """Jobber contains at least 310 clients."""
        from auth import get_client
        session = get_client("jobber")
        data  = _jobber_gql(session, "{ clients(first: 1) { totalCount } }")
        count = data["data"]["clients"]["totalCount"]
        assert count >= 310, (
            f"Expected ≥310 Jobber clients, got {count}. "
            "Re-run push_jobber.py if clients are missing."
        )

    def test_jobber_job_count(self):
        """Jobber contains at least as many jobs as the local integration DB."""
        from auth import get_client
        conn = _db()
        db_job_count = conn.execute(
            "SELECT COUNT(*) AS n FROM jobs"
        ).fetchone()["n"]
        conn.close()

        session = get_client("jobber")
        data  = _jobber_gql(session, "{ jobs(first: 1) { totalCount } }")
        count = data["data"]["jobs"]["totalCount"]
        assert count >= db_job_count, (
            f"Expected Jobber to have at least {db_job_count} jobs from the local "
            f"integration DB, got {count}. "
            "Push may be incomplete — re-run push_jobber.py."
        )

    def test_quickbooks_invoice_count(self):
        """QuickBooks contains at least as many invoices as the local integration DB."""
        from auth.quickbooks_auth import get_quickbooks_headers, get_base_url
        conn = _db()
        db_invoice_count = conn.execute(
            "SELECT COUNT(*) AS n FROM invoices"
        ).fetchone()["n"]
        conn.close()

        headers = get_quickbooks_headers()
        resp = requests.get(
            f"{get_base_url()}/query",
            headers=headers,
            params={"query": "SELECT COUNT(*) FROM Invoice", "minorversion": 70},
            timeout=30,
        )
        resp.raise_for_status()
        count = resp.json()["QueryResponse"]["totalCount"]
        assert count >= db_invoice_count, (
            f"Expected QBO invoice count to cover all DB invoices "
            f"({db_invoice_count}), got {count}. "
            "Push may be incomplete — re-run push_quickbooks.py."
        )

    def test_quickbooks_customer_count(self):
        """QuickBooks contains at least 310 customers."""
        from auth.quickbooks_auth import get_quickbooks_headers, get_base_url
        headers = get_quickbooks_headers()
        resp = requests.get(
            f"{get_base_url()}/query",
            headers=headers,
            params={"query": "SELECT COUNT(*) FROM Customer", "minorversion": 70},
            timeout=30,
        )
        resp.raise_for_status()
        count = resp.json()["QueryResponse"]["totalCount"]
        assert count >= 310, f"Expected ≥310 QBO customers, got {count}."

    def test_hubspot_contact_count(self):
        """HubSpot contains at least 450 contacts."""
        from credentials import get_credential
        token = get_credential("HUBSPOT_ACCESS_TOKEN")
        resp = requests.post(
            f"{_HUBSPOT_BASE}/crm/v3/objects/contacts/search",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json={"limit": 1, "properties": ["email"], "filterGroups": []},
            timeout=30,
        )
        resp.raise_for_status()
        count = resp.json()["total"]
        assert count >= 450, f"Expected ≥450 HubSpot contacts, got {count}."

    def test_mailchimp_audience_count(self):
        """Mailchimp audience has at least 300 members."""
        from credentials import get_credential
        api_key = get_credential("MAILCHIMP_API_KEY")
        server  = get_credential("MAILCHIMP_SERVER_PREFIX")
        resp = requests.get(
            f"https://{server}.api.mailchimp.com/3.0/lists/{_MAILCHIMP_AUDIENCE_ID}",
            auth=("any", api_key),
            timeout=30,
        )
        resp.raise_for_status()
        count = resp.json()["stats"]["member_count"]
        assert count >= 300, f"Expected ≥300 Mailchimp members, got {count}."

    def test_pipedrive_deal_count(self):
        """Pipedrive pipeline keeps the mapped 48-deal cohort with the won set intact."""
        from auth import get_client
        from credentials import get_credential
        from database.schema import get_connection

        # Our 48 canonically-mapped deal IDs from cross_tool_mapping
        conn = get_connection()
        rows = conn.execute(
            """SELECT ctm.tool_specific_id
               FROM cross_tool_mapping ctm
               JOIN commercial_proposals cp ON cp.id = ctm.canonical_id
               WHERE ctm.tool_name = 'pipedrive'"""
        ).fetchall()
        conn.close()
        our_deal_ids = {str(r["tool_specific_id"]) for r in rows}

        assert len(our_deal_ids) == 48, (
            f"Expected 48 mapped Pipedrive deals in cross_tool_mapping, "
            f"found {len(our_deal_ids)}."
        )

        token   = get_credential("PIPEDRIVE_API_TOKEN")
        session = get_client("pipedrive")
        base    = _pd_base(session)

        # Fetch pipeline deals per status and count only our mapped IDs
        # (the pipeline may contain pre-existing deals from project setup)
        counts: dict[str, int] = {}
        for status in ("won", "lost", "open"):
            resp = requests.get(
                f"{base}/deals",
                headers={"x-api-token": token},
                params={
                    "pipeline_id": _PIPEDRIVE_PIPELINE_ID,
                    "status":      status,
                    "limit":       500,
                },
                timeout=30,
            )
            resp.raise_for_status()
            deals = resp.json().get("data") or []
            counts[status] = sum(1 for d in deals if str(d["id"]) in our_deal_ids)

        total = counts["won"] + counts["lost"] + counts["open"]
        assert total == 48, f"Expected 48 mapped deals across statuses, got {total}"
        assert counts["won"] == 10, f"Expected 10 won deals, got {counts['won']}"
        assert abs(counts["lost"] - 23) <= 1, (
            f"Expected lost deals to stay within 1 of the seeded 23, got {counts['lost']}"
        )
        assert abs(counts["open"] - 15) <= 1, (
            f"Expected open deals to stay within 1 of the seeded 15, got {counts['open']}"
        )

    def test_asana_task_count(self):
        """Asana has at least 250 tasks across all 4 projects."""
        import asana as _asana
        from auth import get_client

        client    = get_client("asana")
        tasks_api = _asana.TasksApi(client)

        total = 0
        for project_gid in _ASANA_PROJECT_GIDS.values():
            tasks = list(
                tasks_api.get_tasks_for_project(
                    project_gid,
                    opts={"opt_fields": "gid", "limit": 100},
                )
            )
            total += len(tasks)

        assert total >= 250, (
            f"Expected ≥250 Asana tasks across all projects, got {total}."
        )

    # ================================================================ #
    # 2. MAPPING COMPLETENESS
    # ================================================================ #

    def test_no_unmapped_clients(self):
        """Every DB client has a Jobber mapping."""
        from database.mappings import find_unmapped
        unmapped = find_unmapped("CLIENT", "jobber")
        assert unmapped == [], (
            f"{len(unmapped)} client(s) missing Jobber mapping: {unmapped[:5]}"
        )

    def test_no_unmapped_invoices(self):
        """Every DB invoice has a QuickBooks mapping."""
        from database.mappings import find_unmapped
        unmapped = find_unmapped("INV", "quickbooks")
        assert unmapped == [], (
            f"{len(unmapped)} invoice(s) missing QuickBooks mapping: {unmapped[:5]}"
        )

    def test_all_clients_in_hubspot(self):
        """Every emailful DB client has a HubSpot mapping."""
        conn = _db()
        rows = conn.execute(
            """
            SELECT c.id
            FROM clients c
            WHERE COALESCE(c.email, '') <> ''
              AND NOT EXISTS (
                  SELECT 1 FROM cross_tool_mapping m
                  WHERE m.canonical_id = c.id AND m.tool_name = 'hubspot'
              )
            ORDER BY c.id
            """
        ).fetchall()
        conn.close()
        unmapped = [r["id"] for r in rows]
        assert unmapped == [], (
            f"{len(unmapped)} emailful client(s) missing HubSpot mapping: {unmapped[:5]}"
        )

    def test_all_proposals_in_pipedrive(self):
        """Every commercial proposal has a Pipedrive mapping."""
        from database.mappings import find_unmapped
        unmapped = find_unmapped("PROP", "pipedrive")
        assert unmapped == [], (
            f"{len(unmapped)} proposal(s) missing Pipedrive mapping: {unmapped[:5]}"
        )

    # ================================================================ #
    # 3. DISCOVERY PATTERN CHECKS (DB queries)
    # ================================================================ #

    def test_pattern_crew_speed_vs_quality(self):
        """
        Crew A retains the strongest quality signal and remains slower than the
        fleet average, even as live operations drift over time.
        """
        conn = _db()
        rows = conn.execute(
            """SELECT j.crew_id,
                      AVG(j.duration_minutes_actual) AS avg_dur,
                      AVG(r.rating)                  AS avg_rating
               FROM jobs j
               JOIN reviews r ON r.job_id = j.id
               WHERE j.crew_id IS NOT NULL AND j.status = 'completed'
               GROUP BY j.crew_id"""
        ).fetchall()
        conn.close()

        stats = {r["crew_id"]: (r["avg_dur"], r["avg_rating"]) for r in rows}
        assert "crew-a" in stats, "No reviews found for crew-a"

        a_dur, a_rating = stats["crew-a"]
        others = {c: v for c, v in stats.items() if c != "crew-a"}
        assert others, "No other crews found to compare"

        other_avg_duration = sum(dur for dur, _rating in others.values()) / len(others)
        for crew, (dur, rating) in others.items():
            assert a_rating > rating, (
                f"Crew A avg rating ({a_rating:.2f}) should exceed {crew} ({rating:.2f})"
            )
        assert a_dur > other_avg_duration, (
            f"Crew A avg duration ({a_dur:.1f} min) should exceed the fleet average "
            f"of other crews ({other_avg_duration:.1f} min)"
        )

    def test_pattern_referral_retention(self):
        """
        Residential 'referral' clients churn at a lower rate than the
        overall residential population — the referral retention signal.
        """
        conn = _db()
        rows = conn.execute(
            """SELECT acquisition_source,
                      COUNT(*) AS total,
                      SUM(CASE WHEN status = 'churned' THEN 1 ELSE 0 END) AS churned
               FROM clients
               WHERE client_type = 'residential'
               GROUP BY acquisition_source"""
        ).fetchall()
        conn.close()

        overall_total   = sum(r["total"]   for r in rows)
        overall_churned = sum(r["churned"] for r in rows)
        overall_rate    = overall_churned / overall_total

        ref = next((r for r in rows if r["acquisition_source"] == "referral"), None)
        assert ref is not None, "No 'referral' acquisition_source in residential clients"

        ref_rate = ref["churned"] / ref["total"]
        assert ref_rate < overall_rate, (
            f"Referral churn rate ({ref_rate:.1%}) should be below overall "
            f"residential churn rate ({overall_rate:.1%})"
        )

    def test_pattern_day_of_week_complaints(self):
        """
        Aug–Sep 2025 complaint cluster: at least 3 completed jobs carry
        'client complaint noted' in their notes (planted by gen_tasks_events.py).
        """
        conn = _db()
        rows = conn.execute(
            """SELECT id FROM jobs
               WHERE scheduled_date BETWEEN '2025-08-01' AND '2025-09-30'
                 AND notes LIKE '%client complaint noted%'"""
        ).fetchall()
        conn.close()

        assert len(rows) >= 3, (
            f"Expected ≥3 complaint-flagged jobs in Aug–Sep 2025, found {len(rows)}. "
            "Check that plant_complaint_cluster() ran during data generation."
        )

    def test_pattern_westlake_cancellation_cluster(self):
        """
        At least 3 Westlake recurring agreements were cancelled within a
        14-day window — the deliberate churn cluster planted for discovery.
        """
        conn = _db()
        rows = conn.execute(
            """SELECT ra.end_date
               FROM clients c
               JOIN recurring_agreements ra ON ra.client_id = c.id
               WHERE c.neighborhood = 'Westlake'
                 AND ra.status = 'cancelled'
                 AND ra.end_date IS NOT NULL
               ORDER BY ra.end_date"""
        ).fetchall()
        conn.close()

        dates = sorted(date.fromisoformat(r["end_date"]) for r in rows)
        assert len(dates) >= 3, (
            f"Expected ≥3 Westlake cancellations, found {len(dates)}."
        )

        # Sliding window: any 3 consecutive cancellations within 14 days
        found = any(
            (dates[i + 2] - dates[i]).days <= 14
            for i in range(len(dates) - 2)
        )
        assert found, (
            f"No 3 Westlake cancellations within 14 days. "
            f"All dates: {[str(d) for d in dates]}"
        )

    def test_pattern_maria_overdue_rate(self):
        """
        Maria Gonzalez (SS-EMP-001) remains materially involved in Admin &
        Operations, whether that work is still overdue or has since been cleared.
        """
        conn = _db()
        rows = conn.execute(
            """SELECT status, COUNT(*) AS n
               FROM tasks
               WHERE assignee_employee_id = 'SS-EMP-001'
                 AND project_name = 'Admin & Operations'
               GROUP BY status"""
        ).fetchall()
        conn.close()

        total   = sum(r["n"] for r in rows)
        overdue = next((r["n"] for r in rows if r["status"] == "overdue"), 0)
        completed = next((r["n"] for r in rows if r["status"] == "completed"), 0)

        assert total > 0, "No Admin & Operations tasks found for Maria (SS-EMP-001)"

        rate = overdue / total
        cleared_rate = completed / total
        assert rate >= 0.30 or cleared_rate >= 0.90, (
            "Expected Maria's Admin & Operations workload to show either the "
            f"seeded backlog signal (>=30% overdue) or a clearly cleared backlog "
            f"(>=90% completed), got overdue={rate:.1%} and completed={cleared_rate:.1%} "
            f"({overdue} overdue / {completed} completed / {total} total)"
        )

    def test_pattern_referral_contract_value(self):
        """
        Commercial proposals from 'referral' leads have a higher average
        monthly_value than all other sources combined — the 30% value premium
        planted by plant_referral_proposal_premium().
        """
        conn = _db()
        rows = conn.execute(
            """SELECT l.source,
                      AVG(cp.monthly_value) AS avg_value,
                      COUNT(*) AS n
               FROM commercial_proposals cp
               JOIN leads l ON l.id = cp.lead_id
               WHERE l.lead_type = 'commercial'
               GROUP BY l.source"""
        ).fetchall()
        conn.close()

        ref = next((r for r in rows if r["source"] == "referral"), None)
        assert ref is not None, "No 'referral' source found in commercial proposals"

        ref_avg = ref["avg_value"]
        other_n   = sum(r["n"]             for r in rows if r["source"] != "referral")
        other_avg = (
            sum(r["avg_value"] * r["n"]    for r in rows if r["source"] != "referral")
            / other_n
        ) if other_n else 0

        assert ref_avg > other_avg, (
            f"Referral avg monthly value ({ref_avg:.0f}) should exceed "
            f"other sources combined ({other_avg:.0f})"
        )

    # ================================================================ #
    # 4. FINANCIAL INTEGRITY
    # ================================================================ #

    def test_revenue_within_narrative_range(self):
        """
        Monthly payment totals for May 2025–Feb 2026 are each within ±25% of
        narrative targets, reflecting the business growth ramp from ~$30K/mo
        to ~$120K/mo across the seeded period.
        """
        # Narrative targets derived from business growth trajectory
        MONTHLY_TARGETS = {
            "2025-05":  30_000,
            "2025-06":  55_000,
            "2025-07":  80_000,
            "2025-08":  80_000,
            "2025-09":  80_000,
            "2025-10": 100_000,
            "2025-11": 110_000,
            "2025-12": 120_000,
            "2026-01": 110_000,
            "2026-02": 120_000,
        }

        conn = _db()
        rows = conn.execute(
            """SELECT SUBSTR(payment_date, 1, 7) AS month, SUM(amount) AS total
               FROM payments
               WHERE payment_date IS NOT NULL
               GROUP BY month"""
        ).fetchall()
        conn.close()

        actuals = {r["month"]: r["total"] for r in rows}

        mismatches = []
        for month, target in MONTHLY_TARGETS.items():
            actual = actuals.get(month, 0)
            lo, hi = target * 0.75, target * 1.25
            if not (lo <= actual <= hi):
                mismatches.append(
                    f"{month}: actual=${actual:,.0f}, "
                    f"target=${target:,.0f} "
                    f"[${lo:,.0f}–${hi:,.0f}]"
                )

        assert not mismatches, (
            "Monthly revenue outside ±25% of narrative targets:\n  "
            + "\n  ".join(mismatches)
        )

    def test_ar_aging_has_late_commercial(self):
        """
        At least 2 commercial invoices have days_outstanding > 45, representing
        slow-paying commercial clients in the AR aging scenario.
        """
        conn = _db()
        rows = conn.execute(
            """SELECT i.id, i.days_outstanding
               FROM invoices i
               JOIN clients c ON c.id = i.client_id
               WHERE c.client_type = 'commercial'
                 AND i.days_outstanding > 45"""
        ).fetchall()
        conn.close()

        assert len(rows) >= 2, (
            f"Expected ≥2 commercial invoices with days_outstanding > 45, "
            f"found {len(rows)}."
        )


# ------------------------------------------------------------------ #
# __main__ entry point
# ------------------------------------------------------------------ #
if __name__ == "__main__":
    import subprocess
    result = subprocess.run(
        [sys.executable, "-m", "pytest", __file__, "-v", "--tb=short"],
        cwd=_PROJECT_ROOT,
    )
    sys.exit(result.returncode)
