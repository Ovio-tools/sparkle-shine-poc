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
    assert _extract_zip_prefix("") == ""
