# Similar Jobs Matching Enhancement — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix "No strong Match" in the Slack sales notification by expanding the job search to include scheduled jobs and adding property type + ZIP proximity scoring.

**Architecture:** Replace the single scored SQL query in `similar_jobs_agent.py` with a two-phase approach — a raw SQL fetch of the top 10 non-cancelled jobs, followed by Python re-ranking using richer signals (property type inferred from `company_name` keywords, ZIP prefix proximity, neighborhood, zone, service match, recency). The Sonnet prompt gains `property_type` and `job_status` so descriptions mention the property subtype naturally.

**Tech Stack:** Python 3, psycopg2, Anthropic SDK (`claude-sonnet-4-6`), pytest, PostgreSQL test DB via `TEST_DATABASE_URL`.

---

## File Map

| File | Change |
|---|---|
| `automations/agents/similar_jobs_agent.py` | Replace `_SIMILARITY_SQL`, add `_infer_property_type`, `_extract_zip_prefix`, `_build_lead_ctx`, `_score_candidate`; update `_SYSTEM_PROMPT`; rewrite `find_similar_jobs` body |
| `tests/test_automations/test_similar_jobs_agent.py` | **Create** — unit tests for all new helpers + integration test for `find_similar_jobs` against test DB |

---

## Task 1: `_infer_property_type` and `_extract_zip_prefix`

**Files:**
- Modify: `automations/agents/similar_jobs_agent.py`
- Create: `tests/test_automations/test_similar_jobs_agent.py`

- [ ] **Step 1.1: Create the test file with failing tests for both helpers**

Create `tests/test_automations/test_similar_jobs_agent.py`:

```python
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
    # The function receives a str (caller passes "" for None), but let's be safe
    assert _extract_zip_prefix("") == ""
```

- [ ] **Step 1.2: Run tests — expect ImportError or NameError (functions don't exist yet)**

```bash
cd "/Users/ovieoghor/Documents/Claude Code Exercises/Simulation Exercise/sparkle-shine-poc"
python -m pytest tests/test_automations/test_similar_jobs_agent.py -v 2>&1 | head -40
```

Expected: `ImportError: cannot import name '_infer_property_type'`

- [ ] **Step 1.3: Add `_infer_property_type` and `_extract_zip_prefix` to `similar_jobs_agent.py`**

Open `automations/agents/similar_jobs_agent.py`. Add `import re` to the existing imports at the top (after `import json`). Then add both functions after the `_NEIGHBORHOOD_TO_ZONE` block (around line 52, before `_SIMILARITY_SQL`):

```python
import re
```

```python
# ---------------------------------------------------------------------------
# Property type inference (no schema change — inferred from company_name)
# ---------------------------------------------------------------------------

_MEDICAL_KEYWORDS  = ("dental", "dentist", "orthodontic", "medical", "clinic",
                      "health", "therapy", "wellness", "hospital", "chiropractic")
_RESTAURANT_KEYWORDS = ("restaurant", "cafe", "bar", "kitchen", "grill",
                        "eatery", "diner", "bistro", "brewery")
_RETAIL_KEYWORDS   = ("boutique", "salon", "spa", "shop", "store", "market",
                      "barber", "nail")
_OFFICE_KEYWORDS   = ("office", "consulting", "law", "accounting", "financial",
                      "realty", "insurance", "advisory", "associates", "group")


def _infer_property_type(client_type: str, company_name: str | None) -> str:
    """Infer property subtype from client_type and company_name keywords.

    Returns one of: 'home', 'medical', 'restaurant', 'retail', 'office',
    'commercial' (fallback for unrecognised commercial).
    """
    if (client_type or "").lower() in ("residential", "one-time"):
        return "home"
    name = (company_name or "").lower()
    if any(k in name for k in _MEDICAL_KEYWORDS):
        return "medical"
    if any(k in name for k in _RESTAURANT_KEYWORDS):
        return "restaurant"
    if any(k in name for k in _RETAIL_KEYWORDS):
        return "retail"
    if any(k in name for k in _OFFICE_KEYWORDS):
        return "office"
    return "commercial"


# ---------------------------------------------------------------------------
# ZIP prefix extraction for geographic proximity
# ---------------------------------------------------------------------------

_ZIP_RE = re.compile(r"\b(\d{5})\b")


def _extract_zip_prefix(text: str) -> str:
    """Extract first 5-digit ZIP code from a string and return its 3-digit prefix.

    Returns '' if no 5-digit sequence is found.
    """
    m = _ZIP_RE.search(text or "")
    return m.group(1)[:3] if m else ""
```

- [ ] **Step 1.4: Run tests — all should pass**

```bash
cd "/Users/ovieoghor/Documents/Claude Code Exercises/Simulation Exercise/sparkle-shine-poc"
python -m pytest tests/test_automations/test_similar_jobs_agent.py -v -k "infer or zip"
```

Expected: All 17 tests PASS.

- [ ] **Step 1.5: Commit**

```bash
cd "/Users/ovieoghor/Documents/Claude Code Exercises/Simulation Exercise/sparkle-shine-poc"
git add automations/agents/similar_jobs_agent.py \
        tests/test_automations/test_similar_jobs_agent.py
git commit -m "feat(similar-jobs): add _infer_property_type and _extract_zip_prefix helpers"
```

---

## Task 2: `_build_lead_ctx` and `_score_candidate`

**Files:**
- Modify: `automations/agents/similar_jobs_agent.py`
- Modify: `tests/test_automations/test_similar_jobs_agent.py`

- [ ] **Step 2.1: Add failing tests for `_build_lead_ctx` and `_score_candidate`**

Append to `tests/test_automations/test_similar_jobs_agent.py`:

```python
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
    # Service exact 40 + both commercial diff subtype 10 + crew_zone 15 + recency 15 = 80
    assert score == 80
```

- [ ] **Step 2.2: Run tests — expect ImportError**

```bash
cd "/Users/ovieoghor/Documents/Claude Code Exercises/Simulation Exercise/sparkle-shine-poc"
python -m pytest tests/test_automations/test_similar_jobs_agent.py::test_build_lead_ctx_residential -v 2>&1 | head -20
```

Expected: `ImportError: cannot import name '_build_lead_ctx'`

- [ ] **Step 2.3: Add `_build_lead_ctx` and `_score_candidate` to `similar_jobs_agent.py`**

Add both functions after `_extract_zip_prefix` (before `_SIMILARITY_SQL`):

```python
# ---------------------------------------------------------------------------
# Lead context builder (assembled once per find_similar_jobs call)
# ---------------------------------------------------------------------------

def _build_lead_ctx(contact: dict) -> dict:
    """Build a normalised context dict for the incoming lead.

    Keys: service_interest, contact_type, property_type, neighborhood,
          crew_zone, zip_prefix.
    """
    contact_type = (contact.get("contact_type") or "").lower()
    neighborhood  = contact.get("neighborhood") or ""
    address       = contact.get("address") or ""
    company       = contact.get("company") or contact.get("company_name") or ""
    zip_val       = contact.get("zip") or ""
    zip_prefix    = _extract_zip_prefix(zip_val) or _extract_zip_prefix(address)

    return {
        "service_interest": contact.get("service_interest") or "",
        "contact_type":     contact_type,
        "property_type":    _infer_property_type(contact_type, company),
        "neighborhood":     neighborhood,
        "crew_zone":        _derive_crew_zone(neighborhood, address),
        "zip_prefix":       zip_prefix,
    }


# ---------------------------------------------------------------------------
# Python scorer (replaces SQL CASE scoring)
# Max 100 pts: service 40 + property 20 + geography 25 + recency 15
# ---------------------------------------------------------------------------

def _score_candidate(lead_ctx: dict, row: dict) -> int:
    """Score a DB row against the lead context. Returns int 0-100."""
    from datetime import date

    score = 0

    # ── Service match (40 pts) ─────────────────────────────────────────────
    svc_match   = row.get("service_type_id", "") == lead_ctx["service_interest"]
    type_match  = (row.get("client_type") or "").lower() == lead_ctx["contact_type"]
    if svc_match and type_match:
        score += 40
    elif type_match:
        score += 20

    # ── Property type (20 pts) ─────────────────────────────────────────────
    row_prop = _infer_property_type(
        row.get("client_type") or "",
        row.get("company_name"),
    )
    lead_prop = lead_ctx["property_type"]
    if row_prop == lead_prop:
        score += 20
    elif row_prop != "home" and lead_prop != "home":
        # both commercial, different subtype
        score += 10

    # ── Geography (25 pts) ────────────────────────────────────────────────
    if lead_ctx["neighborhood"] and (
        (row.get("neighborhood") or "").lower() == lead_ctx["neighborhood"].lower()
    ):
        score += 25
    elif lead_ctx["crew_zone"] and (
        (row.get("crew_zone") or "").lower() == lead_ctx["crew_zone"].lower()
    ):
        score += 15
    else:
        row_zip = _extract_zip_prefix(
            row.get("client_address") or row.get("job_address") or ""
        )
        if row_zip and row_zip == lead_ctx["zip_prefix"]:
            score += 12

    # ── Recency (15 pts) ──────────────────────────────────────────────────
    raw_date = row.get("scheduled_date")
    if raw_date:
        try:
            if isinstance(raw_date, str):
                job_date = date.fromisoformat(raw_date[:10])
            else:
                job_date = raw_date  # already a date object from psycopg2
            days_ago = (date.today() - job_date).days
            if days_ago <= 30:
                score += 15
            elif days_ago <= 90:
                score += 10
            elif days_ago <= 180:
                score += 5
        except (ValueError, TypeError):
            pass

    return score
```

- [ ] **Step 2.4: Run tests — all scoring + lead_ctx tests should pass**

```bash
cd "/Users/ovieoghor/Documents/Claude Code Exercises/Simulation Exercise/sparkle-shine-poc"
python -m pytest tests/test_automations/test_similar_jobs_agent.py -v
```

Expected: All tests PASS (including the 17 from Task 1 + new ones from Task 2).

- [ ] **Step 2.5: Commit**

```bash
cd "/Users/ovieoghor/Documents/Claude Code Exercises/Simulation Exercise/sparkle-shine-poc"
git add automations/agents/similar_jobs_agent.py \
        tests/test_automations/test_similar_jobs_agent.py
git commit -m "feat(similar-jobs): add _build_lead_ctx and _score_candidate with Python scoring"
```

---

## Task 3: Replace SQL query and rewrite `find_similar_jobs`

**Files:**
- Modify: `automations/agents/similar_jobs_agent.py`
- Modify: `tests/test_automations/test_similar_jobs_agent.py`

- [ ] **Step 3.1: Add a failing integration test for `find_similar_jobs`**

Append to `tests/test_automations/test_similar_jobs_agent.py`:

```python
import os
import pytest
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
```

- [ ] **Step 3.2: Run — expect test failure (old SQL still used)**

```bash
cd "/Users/ovieoghor/Documents/Claude Code Exercises/Simulation Exercise/sparkle-shine-poc"
python -m pytest tests/test_automations/test_similar_jobs_agent.py::test_find_similar_jobs_returns_matches -v 2>&1 | head -30
```

Expected: FAIL — the mock cursor's `fetchall` returns dicts but the old code uses `cursor.description` differently, or the SQL doesn't match the new column set.

- [ ] **Step 3.3: Replace `_SIMILARITY_SQL` and rewrite `find_similar_jobs` in `similar_jobs_agent.py`**

**Replace** the existing `_SIMILARITY_SQL` constant (lines ~62-104) with:

```python
_SIMILARITY_SQL = """
SELECT
    j.id                AS job_id,
    j.service_type_id,
    j.scheduled_date,
    j.status,
    j.address           AS job_address,
    c.neighborhood,
    c.client_type,
    c.company_name,
    c.address           AS client_address,
    cr.zone             AS crew_zone,
    inv.amount          AS job_total
FROM jobs j
JOIN  clients c  ON c.id = j.client_id
LEFT JOIN crews  cr ON cr.id = j.crew_id
LEFT JOIN (
    SELECT job_id, MAX(amount) AS amount
    FROM   invoices
    GROUP  BY job_id
) inv ON inv.job_id = j.id
WHERE j.status IN ('scheduled', 'completed')
ORDER BY j.scheduled_date DESC
LIMIT 10
"""
```

**Replace** the `find_similar_jobs` function body (lines ~196-297) with:

```python
def find_similar_jobs(contact: dict) -> dict:
    """
    Find jobs most similar to a new lead and format them with Sonnet.

    Args:
        contact: dict with keys: contact_type, service_interest, address,
                 city, neighborhood, zip, company (any may be empty/None)

    Returns:
        dict with keys: matches (list), match_confidence (str),
                        estimated_annual_value (float | None)
    """
    lead_ctx = _build_lead_ctx(contact)

    # ------------------------------------------------------------------
    # Step 1: SQL fetch — top 10 non-cancelled jobs, ordered by recency
    # ------------------------------------------------------------------
    rows: list[dict] = []
    try:
        conn = get_connection()
        try:
            cursor = conn.execute(_SIMILARITY_SQL)
            cols = [d[0] for d in cursor.description]
            rows = [dict(zip(cols, r)) for r in cursor.fetchall()]
        finally:
            conn.close()
    except Exception as exc:
        logger.error("similar_jobs SQL query failed: %s", exc)
        return _NO_RESULTS

    if not rows:
        return _NO_RESULTS

    # ------------------------------------------------------------------
    # Step 2: Python re-rank — score each row and take top 2
    # ------------------------------------------------------------------
    scored = sorted(
        rows,
        key=lambda r: _score_candidate(lead_ctx, r),
        reverse=True,
    )
    top_rows = scored[:2]

    top_score = _score_candidate(lead_ctx, top_rows[0])
    confidence = _confidence_from_score(top_score)
    annual_value = _estimate_annual_value(top_rows[0], confidence)

    # ------------------------------------------------------------------
    # Step 3: Format matches with Claude Sonnet
    # ------------------------------------------------------------------
    formatted_rows: list[dict] = []
    try:
        client = anthropic.Anthropic()
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            system=_SYSTEM_PROMPT,
            messages=[
                {
                    "role": "user",
                    "content": json.dumps(
                        [
                            {
                                "job_id":        r["job_id"],
                                "service_type_id": r.get("service_type_id"),
                                "property_type": _infer_property_type(
                                    r.get("client_type") or "",
                                    r.get("company_name"),
                                ),
                                "neighborhood":  r.get("neighborhood"),
                                "crew_zone":     r.get("crew_zone"),
                                "scheduled_date": r.get("scheduled_date"),
                                "job_status":    r.get("status"),
                                "similarity_score": _score_candidate(lead_ctx, r),
                            }
                            for r in top_rows
                        ],
                        default=str,
                    ),
                }
            ],
        )
        text = next(
            (b.text for b in response.content if b.type == "text"), "[]"
        )
        descriptions: list[dict] = json.loads(text)
        desc_by_id = {d["job_id"]: d["description"] for d in descriptions}
        for row in top_rows:
            formatted_rows.append(
                {**row, "description": desc_by_id.get(row["job_id"], "")}
            )
    except Exception as exc:
        logger.error(
            "similar_jobs Sonnet formatting failed, using fallback: %s", exc
        )
        for row in top_rows:
            formatted_rows.append(
                {**row, "description": _fallback_description(row)}
            )

    return {
        "matches": formatted_rows,
        "match_confidence": confidence,
        "estimated_annual_value": annual_value,
    }
```

- [ ] **Step 3.4: Run all tests — all should pass**

```bash
cd "/Users/ovieoghor/Documents/Claude Code Exercises/Simulation Exercise/sparkle-shine-poc"
python -m pytest tests/test_automations/test_similar_jobs_agent.py -v
```

Expected: All tests PASS.

- [ ] **Step 3.5: Run the existing automation test suite to confirm no regressions**

```bash
cd "/Users/ovieoghor/Documents/Claude Code Exercises/Simulation Exercise/sparkle-shine-poc"
python -m pytest tests/test_automations/ -v 2>&1 | tail -20
```

Expected: All previously passing tests still PASS.

- [ ] **Step 3.6: Commit**

```bash
cd "/Users/ovieoghor/Documents/Claude Code Exercises/Simulation Exercise/sparkle-shine-poc"
git add automations/agents/similar_jobs_agent.py \
        tests/test_automations/test_similar_jobs_agent.py
git commit -m "feat(similar-jobs): replace SQL scoring with Python re-rank, expand to scheduled+completed jobs"
```

---

## Task 4: Update Sonnet system prompt

**Files:**
- Modify: `automations/agents/similar_jobs_agent.py`
- Modify: `tests/test_automations/test_similar_jobs_agent.py`

- [ ] **Step 4.1: Add a failing test for prompt content**

Append to `tests/test_automations/test_similar_jobs_agent.py`:

```python
from automations.agents.similar_jobs_agent import _SYSTEM_PROMPT

def test_system_prompt_mentions_property_type():
    assert "property_type" in _SYSTEM_PROMPT.lower() or "property type" in _SYSTEM_PROMPT.lower()

def test_system_prompt_mentions_job_status():
    assert "job_status" in _SYSTEM_PROMPT.lower() or "ongoing" in _SYSTEM_PROMPT.lower()
```

- [ ] **Step 4.2: Run — expect FAIL**

```bash
cd "/Users/ovieoghor/Documents/Claude Code Exercises/Simulation Exercise/sparkle-shine-poc"
python -m pytest tests/test_automations/test_similar_jobs_agent.py::test_system_prompt_mentions_property_type \
                tests/test_automations/test_similar_jobs_agent.py::test_system_prompt_mentions_job_status -v
```

Expected: Both FAIL (current prompt doesn't mention property_type or job_status).

- [ ] **Step 4.3: Replace `_SYSTEM_PROMPT` in `similar_jobs_agent.py`**

**Replace** the existing `_SYSTEM_PROMPT` constant (lines ~110-124) with:

```python
_SYSTEM_PROMPT = (
    "You are a sales assistant for Sparkle & Shine Cleaning Co., an Austin-based "
    "residential and commercial cleaning company. Given a JSON array of completed or "
    "active cleaning jobs, write a brief natural-language description of each job "
    "suitable for use in a sales conversation with a new prospect.\n\n"
    "Rules:\n"
    "- Never include client names. Refer to locations by neighbourhood only "
    "(e.g. 'a Westlake home', 'an East Austin office').\n"
    "- Use the property_type field to describe the space naturally: 'home' → "
    "'a family home', 'medical' → 'a dental office' or 'a medical clinic', "
    "'restaurant' → 'a restaurant kitchen', 'retail' → 'a retail boutique', "
    "'office' → 'a professional office', 'commercial' → 'a commercial space'.\n"
    "- Use job_status to set tense: if 'scheduled' or ongoing, say 'we currently "
    "clean' or 'we're actively servicing'; if 'completed', use past tense.\n"
    "- Describe the service type, location, and any relevant context "
    "(e.g. frequency, recency).\n"
    "- Keep each description to 2-3 sentences.\n"
    "- Return ONLY a JSON array. Each element must have exactly two keys: "
    "\"job_id\" (copied verbatim from input) and \"description\" (your text).\n"
    "- No markdown, no wrapper text, no extra keys — valid JSON only."
)
```

- [ ] **Step 4.4: Run all tests**

```bash
cd "/Users/ovieoghor/Documents/Claude Code Exercises/Simulation Exercise/sparkle-shine-poc"
python -m pytest tests/test_automations/test_similar_jobs_agent.py -v
```

Expected: All tests PASS.

- [ ] **Step 4.5: Commit**

```bash
cd "/Users/ovieoghor/Documents/Claude Code Exercises/Simulation Exercise/sparkle-shine-poc"
git add automations/agents/similar_jobs_agent.py \
        tests/test_automations/test_similar_jobs_agent.py
git commit -m "feat(similar-jobs): update Sonnet prompt to include property_type and job_status context"
```

---

## Task 5: End-to-end verification

- [ ] **Step 5.1: Run the full automation test suite**

```bash
cd "/Users/ovieoghor/Documents/Claude Code Exercises/Simulation Exercise/sparkle-shine-poc"
python -m pytest tests/test_automations/ -v 2>&1 | tail -30
```

Expected: All tests PASS with no regressions.

- [ ] **Step 5.2: Dry-run automation 07**

```bash
cd "/Users/ovieoghor/Documents/Claude Code Exercises/Simulation Exercise/sparkle-shine-poc"
python -m automations.runner --poll --dry-run 2>&1 | grep -i "similar\|match\|job"
```

Expected: Log lines showing candidate rows fetched and scored, no "similar_jobs SQL query failed" errors.

- [ ] **Step 5.3: Smoke-test `find_similar_jobs` directly against the live DB**

```bash
cd "/Users/ovieoghor/Documents/Claude Code Exercises/Simulation Exercise/sparkle-shine-poc"
python - <<'EOF'
import os
os.environ.setdefault("DATABASE_URL", "postgresql://localhost/sparkle_shine")
from automations.agents.similar_jobs_agent import find_similar_jobs

# Residential lead
result = find_similar_jobs({
    "contact_type": "residential",
    "service_interest": "recurring-biweekly",
    "neighborhood": "Westlake",
    "address": "2401 Westlake Dr Austin TX 78746",
    "zip": "78746",
    "company": None,
})
print("Residential result:", result.get("match_confidence"), len(result.get("matches", [])), "matches")
if result["matches"]:
    print("  Description:", result["matches"][0].get("description", "")[:120])

# Commercial lead
result2 = find_similar_jobs({
    "contact_type": "commercial",
    "service_interest": "commercial-nightly",
    "neighborhood": "East Austin",
    "address": "500 E 6th St Austin TX 78702",
    "zip": "78702",
    "company": "Austin Dental Group",
})
print("Commercial result:", result2.get("match_confidence"), len(result2.get("matches", [])), "matches")
if result2["matches"]:
    print("  Description:", result2["matches"][0].get("description", "")[:120])
EOF
```

Expected:
- Match confidence is `"high"` or `"medium"` (not `"low"`) when the DB has jobs
- Descriptions mention a property type (e.g., "a family home", "a dental office")
- No `"No strong match"` in descriptions

- [ ] **Step 5.4: Final commit if any fixes were needed**

```bash
cd "/Users/ovieoghor/Documents/Claude Code Exercises/Simulation Exercise/sparkle-shine-poc"
git add -p
git commit -m "fix(similar-jobs): e2e verification fixes"
```

---

## Self-Review Checklist

**Spec coverage:**
- [x] SQL expanded to `IN ('scheduled', 'completed')` — Task 3
- [x] `LIMIT 10` raw fetch, Python re-ranks to top 2 — Tasks 2 + 3
- [x] `_infer_property_type` keyword matching — Task 1
- [x] `_extract_zip_prefix` for geographic proximity — Task 1
- [x] `_build_lead_ctx` assembles lead signals — Task 2
- [x] `_score_candidate` 40+20+25+15 scoring — Task 2
- [x] `find_similar_jobs` rewritten — Task 3
- [x] `_SYSTEM_PROMPT` updated with `property_type` + `job_status` — Task 4
- [x] `property_type` and `job_status` added to Sonnet payload — Task 3, Step 3.3
- [x] No changes to `slack_sales_notify.py` — descriptions flow through existing block

**Type consistency:**
- `_score_candidate(lead_ctx: dict, row: dict) -> int` — used consistently in Tasks 2 and 3
- `_build_lead_ctx(contact: dict) -> dict` — returns keys used by `_score_candidate`
- `_infer_property_type(client_type: str, company_name: str | None) -> str` — called with `row.get("client_type")` and `row.get("company_name")` in Task 3
- `_extract_zip_prefix(text: str) -> str` — called with `str` in all usages ✓
