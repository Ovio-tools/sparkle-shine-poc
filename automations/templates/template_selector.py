"""
automations/templates/template_selector.py

Template selection logic for sales outreach emails.
"""

import random


def select_template(contact_type, match_confidence):
    """
    Determine template set and variant.

    Template sets: residential, commercial, hybrid
    Variants: A (similar work angle), B (expertise/needs angle), C (short and direct)

    Parameters
    ----------
    contact_type     : str -- "residential", "commercial", or other
    match_confidence : str -- "high", "medium", or "low"

    Returns
    -------
    tuple of (template_set, variant)
    """
    # Step 1: template set
    if contact_type == "residential":
        template_set = "residential"
    elif contact_type == "commercial":
        template_set = "commercial"
    else:
        template_set = "hybrid"

    # Step 2: variant
    if match_confidence == "high":
        variant = random.choice(["A", "B"])
    elif match_confidence == "medium":
        variant = "B"
    else:
        variant = "C"

    return template_set, variant
