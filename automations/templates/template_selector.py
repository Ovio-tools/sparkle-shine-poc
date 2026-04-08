"""
automations/templates/template_selector.py

Template selection logic for sales outreach emails.
"""

import random


def select_template(contact_type, research_confidence, match_confidence=None):
    """
    Determine template set and variant.

    Template sets: residential, commercial, hybrid
    Variants: A (similar work angle), B (expertise/needs angle), C (short and direct)

    Parameters
    ----------
    contact_type        : str -- "residential", "commercial", or other
    research_confidence : str -- "high", "medium", or "low"
    match_confidence    : str -- "high", "medium", or "low"

    Backward compatibility
    ----------------------
    Legacy call sites may still pass only two arguments:
    select_template(contact_type, match_confidence). In that case, we treat the
    second argument as match_confidence and assume low research confidence.

    Returns
    -------
    tuple of (template_set, variant)
    """
    if match_confidence is None:
        match_confidence = research_confidence
        research_confidence = "low"

    normalized_contact_type = (contact_type or "").strip().lower()
    research_confidence = (research_confidence or "").strip().lower()
    match_confidence = (match_confidence or "").strip().lower()

    # Step 1: template set
    if normalized_contact_type == "residential":
        template_set = "residential"
    elif normalized_contact_type == "commercial":
        template_set = "commercial"
    else:
        template_set = "hybrid"

    # Step 2: variant selection follows the Automation 07 spec.
    if research_confidence == "high" and match_confidence == "high":
        variant = random.choice(["A", "B"])
    elif match_confidence == "high":
        variant = "A"
    elif research_confidence in ("high", "medium") and match_confidence == "medium":
        variant = "B"
    else:
        variant = "C"

    return template_set, variant
