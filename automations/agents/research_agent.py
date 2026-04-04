"""
automations/agents/research_agent.py

Agent 1 -- Lead Research

Uses Claude Opus with web search to gather publicly available context about a
new HubSpot contact before the sales team makes first contact.

Public API
----------
    research_lead(contact: dict) -> dict
"""

from __future__ import annotations

import json
import logging
import re

import anthropic

__all__ = ["research_lead"]

logger = logging.getLogger("automation_07")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MODEL = "claude-opus-4-6"

_SYSTEM_PROMPT = """\
You are a research assistant for Sparkle & Shine Cleaning Co., an Austin-based \
residential and commercial cleaning company. Your job is to research new leads \
using publicly available information so the sales team can prioritise and \
personalise their outreach.

SEARCH STRATEGY
1. Search for the person's full name combined with the company name (if provided) \
to find LinkedIn profiles, business websites, and news mentions.
2. Search for the company name combined with "Austin" (or the provided city) to \
identify the business type, size, and industry.
3. Search for the address or neighbourhood to understand the area: residential \
vs. commercial corridor, typical property sizes, demographics.
4. If no company is given, search for the person's name with their address or \
city to determine whether they are a homeowner, renter, or business operator.
5. Combine findings across searches before forming conclusions. Prefer recent \
results (within the last two years).

ALLOWED INFORMATION -- gather when publicly available
- Business type, industry, and size (employee count, number of locations)
- Business website, Google Business listing, and social media presence \
(professional/business pages only)
- Office or commercial property details relevant to cleaning scope
- Neighbourhood or area characteristics (commercial density, typical property \
type, local business climate)
- Recent news about the business: expansions, new locations, awards, press releases
- Contact's professional role or title
- Whether the contact appears to be a homeowner vs. renter (from public records, \
Zillow-style listings, or business directories)
- General area demographics that inform cleaning service demand

BLOCKED INFORMATION -- never research, infer, or include
- Personal financial details (income, credit, debt)
- Personal relationships, family members, or household composition
- Medical, health, or disability information
- Political affiliations, donations, or religious beliefs
- Personal social media (non-business Instagram, Facebook, X/Twitter, etc.)
- Any information that a reasonable person would consider private or sensitive

CONFIDENCE SCORING RULES
Assign research_confidence based on the quality of findings:
- "high":   Two or more independent sources confirm the contact/business type \
and provide consistent, actionable details. Clear picture of cleaning scope.
- "medium": Some useful information found but from a single source, or the \
picture is incomplete. Reasonable enough to act on with caveats.
- "low":    Minimal or no verifiable information found; contact or company \
could not be confirmed; results were too generic to be useful.

OUTPUT FORMAT
Respond with ONLY a valid JSON object -- no markdown fences, no explanatory \
text before or after. Use exactly these keys:

{
  "contact_type_inferred": "<residential | commercial | unknown>",
  "business_type": "<industry / business description, or null for residential>",
  "business_details": "<key facts about the business or property relevant to cleaning scope>",
  "neighborhood_context": "<description of the neighbourhood or area>",
  "notable_details": "<anything notable that would help personalise outreach>",
  "research_confidence": "<high | medium | low>",
  "raw_summary": "<2-3 sentence plain-English summary of all findings>"
}

If a field has no meaningful content, use an empty string "" (never null except \
for business_type on a residential contact).
"""

_FALLBACK: dict = {
    "contact_type_inferred": "unknown",
    "business_type": None,
    "business_details": "",
    "neighborhood_context": "",
    "notable_details": "",
    "research_confidence": "low",
    "raw_summary": "Research unavailable -- API error",
}

_TOOLS: list = []  # Web search disabled for POC


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_user_prompt(contact: dict) -> str:
    """Build the user message from the contact dict.

    Any field that is missing, None, or blank is replaced with "Not provided".
    """

    def _val(key: str) -> str:
        v = contact.get(key)
        return str(v).strip() if v and str(v).strip() else "Not provided"

    return (
        "Research this new lead for Sparkle & Shine Cleaning Co.:\n\n"
        f"First name:    {_val('firstname')}\n"
        f"Last name:     {_val('lastname')}\n"
        f"Company:       {_val('company')}\n"
        f"Address:       {_val('address')}\n"
        f"City:          {_val('city')}\n"
        f"Contact type (if known): {_val('contact_type')}\n\n"
        "Search for publicly available information about this contact and return "
        "the structured JSON response described in your instructions."
    )


def _extract_json(text: str) -> dict:
    """Parse JSON from the model response.

    Strips markdown code fences if present, then attempts json.loads.
    Falls back to a regex search for the first {...} block.
    Raises ValueError if nothing parseable is found.
    """
    # Strip leading/trailing whitespace
    text = text.strip()

    # Remove markdown code fences (```json ... ``` or ``` ... ```)
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    text = text.strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Regex fallback: extract the first {...} block (including nested braces)
    match = re.search(r"\{[\s\S]*\}", text)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            pass

    raise ValueError(f"No valid JSON found in model response: {text[:300]!r}")


# ---------------------------------------------------------------------------
# Public function
# ---------------------------------------------------------------------------

def research_lead(contact: dict) -> dict:
    """
    Research a new lead using Claude Opus with web search.

    Args:
        contact: dict with keys: firstname, lastname, company, address,
                 city, contact_type (any may be empty/None)

    Returns:
        dict with keys: contact_type_inferred, business_type, business_details,
                       neighborhood_context, notable_details, research_confidence,
                       raw_summary
    """
    user_prompt = _build_user_prompt(contact)

    try:
        client = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY from env
        response = client.messages.create(
            model=_MODEL,
            max_tokens=1024,
            system=_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
            timeout=60,
        )
    except Exception as exc:
        logger.error(
            "Anthropic API call failed for contact %r %r: %s",
            contact.get("firstname"),
            contact.get("lastname"),
            exc,
        )
        return dict(_FALLBACK)

    text_blocks = [
        block.text
        for block in response.content
        if hasattr(block, "type") and block.type == "text"
    ]
    text = text_blocks[0] if text_blocks else ""

    if not text:
        logger.error(
            "No text block in Anthropic response for contact %r %r",
            contact.get("firstname"),
            contact.get("lastname"),
        )
        return dict(_FALLBACK)

    try:
        result = _extract_json(text)
    except (ValueError, Exception) as exc:
        logger.error(
            "JSON parse failed for contact %r %r: %s | raw text: %s",
            contact.get("firstname"),
            contact.get("lastname"),
            exc,
            text[:500],
        )
        return dict(_FALLBACK)

    return result
