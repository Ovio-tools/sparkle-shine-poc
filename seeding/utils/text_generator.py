"""
Thin wrapper around the Anthropic API for generating realistic text fields.

Model: claude-sonnet-4-20250514
Max tokens per call: 300

Results are cached in memory (LOCAL_CACHE) to avoid redundant API calls on reruns.

Usage:
    from seeding.utils.text_generator import generate_job_note, batch_generate

    note = generate_job_note("Deep Clean", "South Austin")
    review = generate_review_text(5, "Recurring Weekly")
"""

import hashlib
import json
import time
import logging
from typing import Dict, List

import anthropic

logger = logging.getLogger(__name__)

MODEL = "claude-sonnet-4-20250514"
MAX_TOKENS = 300
BATCH_DELAY = 0.5  # seconds between calls in batch_generate

LOCAL_CACHE: Dict[str, str] = {}


def cache_key(*args, **kwargs) -> str:
    """
    Produce a stable cache key from arbitrary positional and keyword arguments.
    """
    payload = json.dumps({"args": args, "kwargs": kwargs}, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()


def _call_api(prompt: str) -> str:
    """Make a single API call and return the text content."""
    client = anthropic.Anthropic()
    message = client.messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        messages=[{"role": "user", "content": prompt}],
    )
    return message.content[0].text.strip()


def _cached_call(key: str, prompt: str) -> str:
    """Return cached result if available, otherwise call the API and cache it."""
    if key in LOCAL_CACHE:
        return LOCAL_CACHE[key]
    result = _call_api(prompt)
    LOCAL_CACHE[key] = result
    return result


# ---------------------------------------------------------------------------
# Public generation functions
# ---------------------------------------------------------------------------

def generate_job_note(
    service_type: str,
    neighborhood: str,
    any_issues: bool = False,
) -> str:
    """
    Generate a brief 1-2 sentence internal job note a cleaner might leave.

    Example output: "Completed deep clean of 3BR. Client requested extra time on kitchen."
    """
    key = cache_key("generate_job_note", service_type, neighborhood, any_issues)
    issues_clause = (
        " There were some issues worth noting (e.g. hard-to-reach area, missing supplies, pet odor, client complaint)."
        if any_issues
        else ""
    )
    prompt = (
        f"Write a brief 1-2 sentence internal job note that a house cleaner would leave "
        f"after completing a {service_type} job in the {neighborhood} neighborhood of Austin, TX."
        f"{issues_clause} "
        f"Keep it casual and practical. No greeting or sign-off. Plain text only."
    )
    return _cached_call(key, prompt)


def generate_review_text(rating: int, service_type: str) -> str:
    """
    Generate a realistic customer review matching the star rating (1-5).

    1-2 stars: complaint tone. 3: lukewarm. 4-5: positive/enthusiastic.
    """
    key = cache_key("generate_review_text", rating, service_type)

    if rating <= 2:
        tone = "negative or disappointed, mentioning a specific complaint"
    elif rating == 3:
        tone = "lukewarm or mixed, acknowledging both positives and negatives"
    else:
        tone = "positive or enthusiastic, praising specific aspects of the service"

    prompt = (
        f"Write a realistic {rating}-star customer review for a {service_type} "
        f"cleaning service in Austin, TX. The tone should be {tone}. "
        f"2-4 sentences. No name. No emojis. Plain text only."
    )
    return _cached_call(key, prompt)


def generate_pipedrive_activity_note(
    activity_type: str,
    deal_stage: str,
    outcome: str,
) -> str:
    """
    Generate a realistic sales activity note (call recap, email summary, site visit notes).
    """
    key = cache_key(
        "generate_pipedrive_activity_note", activity_type, deal_stage, outcome
    )
    prompt = (
        f"Write a realistic CRM activity note for a cleaning company sales rep. "
        f"Activity type: {activity_type}. Current deal stage: {deal_stage}. Outcome: {outcome}. "
        f"2-3 sentences, written as an internal note. Plain text only. No headers."
    )
    return _cached_call(key, prompt)


def generate_cancellation_reason(client_type: str) -> str:
    """
    Generate a realistic cancellation reason for a churned client.
    """
    key = cache_key("generate_cancellation_reason", client_type)
    prompt = (
        f"Write a single realistic cancellation reason (1 sentence) that a {client_type} "
        f"client of a residential/commercial cleaning service in Austin, TX might give "
        f"when cancelling their service. Be specific and believable. Plain text only."
    )
    return _cached_call(key, prompt)


def batch_generate(prompts: List[Dict]) -> List[str]:
    """
    Run multiple generation calls sequentially with a 0.5s delay between each.

    Args:
        prompts: List of dicts with keys ``function_name`` and ``kwargs``.

    Returns:
        List of string results in the same order as ``prompts``.
        Failed calls produce an empty string and log the error.

    Example::

        results = batch_generate([
            {"function_name": "generate_job_note",
             "kwargs": {"service_type": "Deep Clean", "neighborhood": "South Austin"}},
            {"function_name": "generate_review_text",
             "kwargs": {"rating": 4, "service_type": "Recurring Weekly"}},
        ])
    """
    _function_map = {
        "generate_job_note": generate_job_note,
        "generate_review_text": generate_review_text,
        "generate_pipedrive_activity_note": generate_pipedrive_activity_note,
        "generate_cancellation_reason": generate_cancellation_reason,
    }

    results: list[str] = []

    for i, entry in enumerate(prompts):
        func_name = entry.get("function_name", "")
        kwargs = entry.get("kwargs", {})
        func = _function_map.get(func_name)

        if func is None:
            logger.error("batch_generate: unknown function '%s' at index %d", func_name, i)
            results.append("")
            continue

        try:
            result = func(**kwargs)
            results.append(result)
        except Exception as exc:
            logger.error(
                "batch_generate: error calling %s at index %d: %s",
                func_name,
                i,
                exc,
            )
            results.append("")

        if i < len(prompts) - 1:
            time.sleep(BATCH_DELAY)

    return results
