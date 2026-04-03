"""
automations/agents/similar_jobs_agent.py

Agent 2: Similar Job Matching

Given a new lead (contact dict), find completed jobs that are similar
based on service type, geography, and recency. Format the results with
Claude Sonnet for human-readable output.

Steps:
  1. SQL similarity query against the jobs/clients/crews/invoices tables.
  2. Format raw rows with claude-sonnet-4-6 into natural-language descriptions.
"""

from __future__ import annotations

import json
import logging
import re

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
# SQL similarity query
# Three scoring dimensions, max possible score = 50 + 30 + 20 = 100.
#
# Parameter tuple: (requested_service, contact_type, contact_type,
#                   lead_neighborhood, lead_crew_zone)
# ---------------------------------------------------------------------------

_SIMILARITY_SQL = """
SELECT
    j.id                AS job_id,
    j.service_type_id,
    j.scheduled_date,
    c.neighborhood,
    c.client_type,
    cr.zone             AS crew_zone,
    inv.amount          AS job_total,
    (
        -- Service type alignment (50 pts)
        CASE
            WHEN j.service_type_id = %s AND c.client_type = %s THEN 50
            WHEN c.client_type = %s THEN 25
            ELSE 0
        END
        +
        -- Geographic proximity (30 pts)
        CASE
            WHEN c.neighborhood = %s THEN 30
            WHEN cr.zone = %s THEN 15
            ELSE 5
        END
        +
        -- Recency bonus (20 pts)
        CASE
            WHEN j.scheduled_date::date >= CURRENT_DATE - INTERVAL '30 days' THEN 20
            WHEN j.scheduled_date::date >= CURRENT_DATE - INTERVAL '90 days' THEN 10
            ELSE 0
        END
    ) AS similarity_score
FROM jobs j
JOIN  clients c  ON c.id = j.client_id
LEFT JOIN crews  cr ON cr.id = j.crew_id
LEFT JOIN (
    SELECT job_id, MAX(amount) AS amount
    FROM   invoices
    GROUP  BY job_id
) inv ON inv.job_id = j.id
WHERE j.status = 'completed'
ORDER BY similarity_score DESC
LIMIT 2
"""

# ---------------------------------------------------------------------------
# Sonnet system prompt (Section 5 Step 2)
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = (
    "You are a sales assistant for Sparkle & Shine Cleaning Co., an Austin-based "
    "residential and commercial cleaning company. Given a JSON array of completed "
    "cleaning jobs, write a brief natural-language description of each job suitable "
    "for use in a sales conversation with a new prospect.\n\n"
    "Rules:\n"
    "- Never include client names. Refer to locations by neighbourhood only "
    "(e.g. 'a Westlake home', 'an East Austin office').\n"
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
    Find completed jobs most similar to a new lead and format them.

    Args:
        contact: dict with keys: contact_type, service_interest,
                 address, city, neighborhood (any may be empty/None)

    Returns:
        dict with keys: matches (list of job dicts), match_confidence,
                       estimated_annual_value (float or None)
    """
    requested_service = contact.get("service_interest") or ""
    contact_type = contact.get("contact_type") or ""
    neighborhood = contact.get("neighborhood") or ""
    address = contact.get("address") or ""

    crew_zone = _derive_crew_zone(neighborhood, address)

    params = (
        requested_service,
        contact_type,
        contact_type,
        neighborhood,
        crew_zone,
    )

    # ------------------------------------------------------------------
    # Step 1: SQL similarity query
    # ------------------------------------------------------------------
    rows: list[dict] = []
    try:
        conn = get_connection()
        try:
            cursor = conn.execute(_SIMILARITY_SQL, params)
            rows = [dict(r) for r in cursor.fetchall()]
        finally:
            conn.close()
    except Exception as exc:
        logger.error("similar_jobs SQL query failed: %s", exc)
        return _NO_RESULTS

    if not rows:
        return _NO_RESULTS

    top_score = rows[0].get("similarity_score")
    confidence = _confidence_from_score(top_score)
    annual_value = _estimate_annual_value(rows[0], confidence)

    # ------------------------------------------------------------------
    # Step 2: Format matches with Claude Sonnet
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
                                "job_id": r["job_id"],
                                "service_type_id": r.get("service_type_id"),
                                "neighborhood": r.get("neighborhood"),
                                "crew_zone": r.get("crew_zone"),
                                "scheduled_date": r.get("scheduled_date"),
                                "similarity_score": r.get("similarity_score"),
                            }
                            for r in rows
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
        for row in rows:
            formatted_rows.append(
                {**row, "description": desc_by_id.get(row["job_id"], "")}
            )
    except Exception as exc:
        logger.error(
            "similar_jobs Sonnet formatting failed, using fallback: %s", exc
        )
        for row in rows:
            formatted_rows.append(
                {**row, "description": _fallback_description(row)}
            )

    return {
        "matches": formatted_rows,
        "match_confidence": confidence,
        "estimated_annual_value": annual_value,
    }
