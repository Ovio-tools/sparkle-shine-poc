"""
tests/test_automations/test_similar_jobs_agent.py

Unit tests for similar_jobs_agent helpers.
No DB, no API calls — pure logic.
"""
import os
import sys

_PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import pytest
from automations.agents.similar_jobs_agent import (
    _infer_property_type,
    _extract_zip_prefix,
)


# ── _infer_property_type ──────────────────────────────────────────────────────

def test_infer_residential_returns_home():
    assert _infer_property_type("residential", None) == "home"

def test_infer_one_time_returns_home():
    assert _infer_property_type("one-time", "Some Name") == "home"

def test_infer_commercial_dental():
    assert _infer_property_type("commercial", "Barton Creek Dental") == "medical"

def test_infer_commercial_clinic():
    assert _infer_property_type("commercial", "Austin Wellness Clinic") == "medical"

def test_infer_commercial_restaurant():
    assert _infer_property_type("commercial", "The Blue Grill") == "restaurant"

def test_infer_commercial_cafe():
    assert _infer_property_type("commercial", "Sip Coffee Cafe") == "restaurant"

def test_infer_commercial_salon():
    assert _infer_property_type("commercial", "Luxe Hair Salon") == "retail"

def test_infer_commercial_boutique():
    assert _infer_property_type("commercial", "South Congress Boutique") == "retail"

def test_infer_commercial_law():
    assert _infer_property_type("commercial", "Gonzalez Law Group") == "office"

def test_infer_commercial_accounting():
    assert _infer_property_type("commercial", "TX Accounting Partners") == "office"

def test_infer_commercial_no_keyword_returns_commercial():
    assert _infer_property_type("commercial", "Acme Corp") == "commercial"

def test_infer_commercial_none_company_name_returns_commercial():
    assert _infer_property_type("commercial", None) == "commercial"

def test_infer_case_insensitive():
    assert _infer_property_type("commercial", "DENTAL ASSOCIATES") == "medical"


# ── _extract_zip_prefix ────────────────────────────────────────────────────────

def test_extract_zip_prefix_from_full_address():
    assert _extract_zip_prefix("2401 Westlake Dr, Austin TX 78746") == "787"

def test_extract_zip_prefix_from_zip_only():
    assert _extract_zip_prefix("78701") == "787"

def test_extract_zip_prefix_no_zip_returns_empty():
    assert _extract_zip_prefix("Westlake Dr") == ""

def test_extract_zip_prefix_empty_string():
    assert _extract_zip_prefix("") == ""

def test_extract_zip_prefix_none_treated_as_empty():
    assert _extract_zip_prefix(None) == ""

from datetime import date, timedelta
from automations.agents.similar_jobs_agent import (
    _infer_property_type,
    _extract_zip_prefix,
    _build_lead_ctx,
    _score_candidate,
)


# ── _build_lead_ctx ───────────────────────────────────────────────────────────

def test_build_lead_ctx_residential():
    contact = {
        "contact_type": "residential",
        "service_interest": "recurring-biweekly",
        "neighborhood": "Westlake",
        "address": "2401 Westlake Dr, Austin TX 78746",
        "zip": "78746",
        "company": None,
    }
    ctx = _build_lead_ctx(contact)
    assert ctx["service_interest"] == "recurring-biweekly"
    assert ctx["contact_type"] == "residential"
    assert ctx["property_type"] == "home"
    assert ctx["neighborhood"] == "Westlake"
    assert ctx["zip_prefix"] == "787"
    assert isinstance(ctx["crew_zone"], str)  # may be empty if no zone match


def test_build_lead_ctx_commercial_medical():
    contact = {
        "contact_type": "commercial",
        "service_interest": "commercial-nightly",
        "neighborhood": "East Austin",
        "address": "500 E 6th St, Austin TX 78702",
        "zip": "",
        "company": "Austin Dental Group",
    }
    ctx = _build_lead_ctx(contact)
    assert ctx["property_type"] == "medical"
    assert ctx["zip_prefix"] == "787"


def test_build_lead_ctx_falls_back_to_address_for_zip():
    contact = {
        "contact_type": "residential",
        "service_interest": "",
        "neighborhood": "",
        "address": "100 Main St Austin TX 78701",
        "zip": "",
        "company": None,
    }
    ctx = _build_lead_ctx(contact)
    assert ctx["zip_prefix"] == "787"


# ── _score_candidate ──────────────────────────────────────────────────────────

def _today_str(offset_days=0) -> str:
    return (date.today() - timedelta(days=offset_days)).isoformat()


def _make_lead_ctx(**overrides):
    base = {
        "service_interest": "recurring-biweekly",
        "contact_type": "residential",
        "property_type": "home",
        "neighborhood": "Westlake",
        "crew_zone": "West Austin",
        "zip_prefix": "787",
    }
    base.update(overrides)
    return base


def _make_row(**overrides):
    base = {
        "service_type_id": "recurring-biweekly",
        "client_type": "residential",
        "company_name": None,
        "neighborhood": "Westlake",
        "crew_zone": "West Austin",
        "client_address": "2401 Westlake Dr Austin TX 78746",
        "scheduled_date": _today_str(10),
        "status": "completed",
    }
    base.update(overrides)
    return base


def test_score_perfect_match():
    ctx = _make_lead_ctx()
    row = _make_row()
    score = _score_candidate(ctx, row)
    # Service exact (40) + property exact (20) + neighborhood exact (25) + recency <=30d (15) = 100
    assert score == 100


def test_score_service_type_mismatch_client_type_match():
    ctx = _make_lead_ctx()
    row = _make_row(service_type_id="std-residential")
    score = _score_candidate(ctx, row)
    # client_type match only (20) + property (20) + neighborhood (25) + recency (15) = 80
    assert score == 80


def test_score_no_service_or_type_match():
    ctx = _make_lead_ctx()
    row = _make_row(service_type_id="commercial-nightly", client_type="commercial",
                    company_name="Acme Corp")
    score = _score_candidate(ctx, row)
    # Service 0 + property mismatch (home vs commercial = 0) + neighborhood 25 + recency 15 = 40
    assert score == 40


def test_score_zone_match_no_neighborhood():
    ctx = _make_lead_ctx(neighborhood="Tarrytown")
    row = _make_row(neighborhood="Rollingwood", crew_zone="West Austin")
    score = _score_candidate(ctx, row)
    # Service 40 + property 20 + zone match 15 + recency 15 = 90
    assert score == 90


def test_score_zip_prefix_match_no_neighborhood_or_zone():
    ctx = _make_lead_ctx(neighborhood="", crew_zone="")
    row = _make_row(neighborhood="", crew_zone="", client_address="999 Oak Ln Austin TX 78745")
    score = _score_candidate(ctx, row)
    # Service 40 + property 20 + zip 12 + recency 15 = 87
    assert score == 87


def test_score_no_geo_match():
    ctx = _make_lead_ctx(neighborhood="", crew_zone="", zip_prefix="")
    row = _make_row(neighborhood="", crew_zone="", client_address="")
    score = _score_candidate(ctx, row)
    # Service 40 + property 20 + geo 0 + recency 15 = 75
    assert score == 75


def test_score_recency_90_days():
    ctx = _make_lead_ctx()
    row = _make_row(scheduled_date=_today_str(60))
    score = _score_candidate(ctx, row)
    # 40 + 20 + 25 + 10 = 95
    assert score == 95


def test_score_recency_180_days():
    ctx = _make_lead_ctx()
    row = _make_row(scheduled_date=_today_str(120))
    score = _score_candidate(ctx, row)
    # 40 + 20 + 25 + 5 = 90
    assert score == 90


def test_score_recency_over_180_days():
    ctx = _make_lead_ctx()
    row = _make_row(scheduled_date=_today_str(200))
    score = _score_candidate(ctx, row)
    # 40 + 20 + 25 + 0 = 85
    assert score == 85


def test_score_both_commercial_different_subtype():
    ctx = _make_lead_ctx(
        contact_type="commercial", property_type="medical",
        service_interest="commercial-nightly",
    )
    row = _make_row(
        service_type_id="commercial-nightly", client_type="commercial",
        company_name="South Congress Boutique",  # retail
        neighborhood="East Austin",
        crew_zone="East Austin",
    )
    score = _score_candidate(ctx, row)
    # Service exact 40 + both commercial diff subtype 10 + zip prefix 12 + recency 15 = 77
    assert score == 77

from unittest.mock import patch, MagicMock
from automations.agents.similar_jobs_agent import find_similar_jobs

# ── find_similar_jobs integration (mocked DB + mocked Sonnet) ────────────────

def _make_db_rows():
    """Simulate psycopg2 rows returned by the new SQL query."""
    from datetime import date, timedelta
    recent = (date.today() - timedelta(days=15)).isoformat()
    older  = (date.today() - timedelta(days=100)).isoformat()
    return [
        {
            "job_id": "SS-JOB-0001",
            "service_type_id": "recurring-biweekly",
            "scheduled_date": recent,
            "status": "completed",
            "job_address": "2401 Westlake Dr",
            "neighborhood": "Westlake",
            "client_type": "residential",
            "company_name": None,
            "client_address": "2401 Westlake Dr Austin TX 78746",
            "crew_zone": "West Austin",
            "job_total": 150.0,
        },
        {
            "job_id": "SS-JOB-0002",
            "service_type_id": "std-residential",
            "scheduled_date": older,
            "status": "scheduled",
            "job_address": "800 South Lamar",
            "neighborhood": "South Austin",
            "client_type": "residential",
            "company_name": None,
            "client_address": "800 South Lamar Austin TX 78704",
            "crew_zone": "South Austin",
            "job_total": 135.0,
        },
    ]


def _make_sonnet_response(rows):
    mock_resp = MagicMock()
    mock_content = MagicMock()
    mock_content.type = "text"
    import json
    mock_content.text = json.dumps([
        {"job_id": r["job_id"], "description": f"A clean home in {r['neighborhood']}."}
        for r in rows
    ])
    mock_resp.content = [mock_content]
    return mock_resp


@patch("automations.agents.similar_jobs_agent.get_connection")
@patch("automations.agents.similar_jobs_agent.anthropic.Anthropic")
def test_find_similar_jobs_returns_matches(mock_anthropic_cls, mock_get_conn):
    rows = _make_db_rows()

    # Mock DB
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = rows
    mock_cursor.description = [(k,) for k in rows[0].keys()]
    mock_conn.execute.return_value = mock_cursor
    mock_conn.__enter__ = lambda s: s
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_get_conn.return_value = mock_conn

    # Mock Sonnet
    mock_client = MagicMock()
    mock_client.messages.create.return_value = _make_sonnet_response(rows)
    mock_anthropic_cls.return_value = mock_client

    contact = {
        "contact_type": "residential",
        "service_interest": "recurring-biweekly",
        "neighborhood": "Westlake",
        "address": "2401 Westlake Dr Austin TX 78746",
        "zip": "78746",
        "company": None,
    }
    result = find_similar_jobs(contact)

    assert len(result["matches"]) <= 2
    assert len(result["matches"]) >= 1
    assert result["match_confidence"] in ("high", "medium", "low")
    assert result["matches"][0]["description"] != ""
    # Top match should be the Westlake biweekly job (highest score)
    assert result["matches"][0]["job_id"] == "SS-JOB-0001"


@patch("automations.agents.similar_jobs_agent.get_connection")
@patch("automations.agents.similar_jobs_agent.anthropic.Anthropic")
def test_find_similar_jobs_empty_db_returns_no_results(mock_anthropic_cls, mock_get_conn):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_cursor.description = []
    mock_conn.execute.return_value = mock_cursor
    mock_conn.__enter__ = lambda s: s
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_get_conn.return_value = mock_conn

    contact = {
        "contact_type": "residential",
        "service_interest": "recurring-biweekly",
        "neighborhood": "Westlake",
        "address": "",
        "zip": "",
        "company": None,
    }
    result = find_similar_jobs(contact)
    assert result["matches"] == []
    assert result["match_confidence"] == "low"
    assert result["estimated_annual_value"] is None

from automations.agents.similar_jobs_agent import _SYSTEM_PROMPT

def test_system_prompt_mentions_property_type():
    assert "property_type" in _SYSTEM_PROMPT.lower() or "property type" in _SYSTEM_PROMPT.lower()

def test_system_prompt_mentions_job_status():
    assert "job_status" in _SYSTEM_PROMPT.lower() or "ongoing" in _SYSTEM_PROMPT.lower()
