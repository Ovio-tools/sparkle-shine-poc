"""
automations/agents/similar_jobs_agent.py

Agent 2: Similar Job Matching

Given a new lead (contact dict), find similar past or active jobs and
format the results with Claude Sonnet for human-readable output.

Steps:
  1. Build lead context (property type, ZIP prefix, neighborhood, crew zone).
  2. SQL fetch: top 10 non-cancelled jobs ordered by recency (no SQL scoring).
  3. Python re-rank: score each row via _score_candidate (max 100 pts).
  4. Format top 2 with claude-sonnet-4-6, including property_type + job_status.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import date

import anthropic

from config.business import CREWS, ZONES
from database.connection import get_connection

logger = logging.getLogger("automation_07")

# ---------------------------------------------------------------------------
# Neighbourhood → crew-zone lookup
# ---------------------------------------------------------------------------

# Build a flat map: lowercase_neighbourhood_name → crew zone string
_NEIGHBORHOOD_TO_ZONE: dict[str, str] = {}
for _crew in CREWS:
    _zone = _crew["zone"]
    for _nbhd in ZONES.get(_crew["id"], []):
        _NEIGHBORHOOD_TO_ZONE[_nbhd.lower()] = _zone


def _derive_crew_zone(neighborhood: str | None, address: str | None) -> str:
    """Return the crew zone for a given neighbourhood or address.

    Tries a substring match against every known neighbourhood name.
    Returns '' when no match is found (CASE ELSE 5 applies).
    """
    text = ((neighborhood or "") + " " + (address or "")).strip().lower()
    if not text:
        return ""
    for nbhd_key, zone in _NEIGHBORHOOD_TO_ZONE.items():
        if nbhd_key in text:
            return zone
    return ""


# ---------------------------------------------------------------------------
# Property type inference (no schema change — inferred from company_name)
# ---------------------------------------------------------------------------

_MEDICAL_KEYWORDS  = ("dental", "dentist", "orthodontic", "medical", "clinic",
                      "health", "therapy", "wellness", "hospital")
_RESTAURANT_KEYWORDS = ("restaurant", "cafe", "bar", "kitchen", "grill",
                        "eatery", "diner")
_RETAIL_KEYWORDS   = ("boutique", "salon", "spa", "shop", "store", "market")
_OFFICE_KEYWORDS   = ("office", "consulting", "law", "accounting", "financial",
                      "realty", "insurance")


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


def _extract_zip_prefix(text: str | None) -> str:
    """Extract first 5-digit ZIP code from a string and return its 3-digit prefix.

    Returns '' if no 5-digit sequence is found.
    """
    m = _ZIP_RE.search(text or "")
    return m.group(1)[:3] if m else ""


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
    lead_neighborhood = lead_ctx["neighborhood"]
    lead_crew_zone    = lead_ctx["crew_zone"]
    if lead_neighborhood and (
        (row.get("neighborhood") or "").lower() == lead_neighborhood.lower()
    ):
        score += 25
    elif lead_crew_zone and (
        (row.get("crew_zone") or "").lower() == lead_crew_zone.lower()
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


# ---------------------------------------------------------------------------
# SQL fetch — returns up to 10 recent non-cancelled jobs.
# No scoring in SQL; Python re-ranking via _score_candidate handles that.
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# Sonnet system prompt (Section 5 Step 2)
# ---------------------------------------------------------------------------

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

# Sentinel for the no-results / error path
_NO_RESULTS: dict = {
    "matches": [],
    "match_confidence": "low",
    "estimated_annual_value": None,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _confidence_from_score(score) -> str:
    try:
        s = float(score)
    except (TypeError, ValueError):
        return "low"
    if s >= 80:
        return "high"
    if s >= 50:
        return "medium"
    return "low"


def _get_frequency(service_type_id: str) -> str | None:
    """Infer visit frequency from service_type_id."""
    stype = (service_type_id or "").lower()
    if "biweekly" in stype:
        return "biweekly"
    if "weekly" in stype:
        return "weekly"
    if "monthly" in stype:
        return "monthly"
    if "nightly" in stype or "commercial" in stype:
        return "nightly"
    return None  # one-time


def _estimate_annual_value(row: dict, confidence: str) -> float | None:
    """Estimate annualised revenue from the top-match job row."""
    if confidence == "low":
        return None

    job_total = row.get("job_total")
    frequency = _get_frequency(row.get("service_type_id") or "")

    if frequency == "weekly":
        return (job_total or 0) * 52
    if frequency == "biweekly":
        return (job_total or 0) * 26
    if frequency == "monthly":
        return (job_total or 0) * 12
    if frequency == "nightly":
        return (job_total or 0) * 260
    # one-time: use job total as-is
    return job_total


def _fallback_description(row: dict) -> str:
    """Minimal description used when Sonnet is unavailable."""
    neighbourhood = row.get("neighborhood") or "Austin"
    stype = (row.get("service_type_id") or "cleaning service").replace("-", " ")
    date = row.get("scheduled_date") or "recently"
    return f"Completed {stype} in {neighbourhood} on {date}."


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

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
            rows = [r if isinstance(r, dict) else dict(zip(cols, r)) for r in cursor.fetchall()]
        finally:
            conn.close()
    except Exception as exc:
        logger.error("similar_jobs SQL query failed: %s", exc)
        try:
            from simulation.error_reporter import report_error
            report_error(exc, tool_name="database",
                         context="SQL query for similar jobs in sales outreach agent")
        except Exception:
            pass
        return {**_NO_RESULTS, "matches": []}

    if not rows:
        return {**_NO_RESULTS, "matches": []}

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
