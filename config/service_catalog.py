"""
config/service_catalog.py

Single source of truth for canonical service IDs, free-text aliases,
QBO item IDs, and the label-to-canonical-ID normalization used by both
the intelligence sync layer and the automations layer.

Canonical IDs are defined in config.business.SERVICE_TYPES. This module
exposes:

  - CANONICAL_SERVICE_IDS: frozenset of valid canonical IDs.
  - SERVICE_CATALOGUE: canonical ID -> metadata (display_name,
    duration_minutes, base_price, qbo_item_id, service_category).
  - SERVICE_ALIASES: normalized free-text label -> canonical ID.
  - canonical_service_id(value): resolve a canonical ID, alias, or
    free-text label to a canonical ID. Returns None when unrecognized
    so callers can escalate (e.g., emit a fallback-pricing alert).
  - get_service_metadata(canonical_id): metadata for a known canonical ID.
"""
from __future__ import annotations

import re
from typing import Optional

from config.business import SERVICE_TYPES


# QBO service item IDs live in the integration layer, not the business
# profile. Kept here so the catalogue stays authoritative for invoice
# pricing without leaking QBO identifiers into config/business.py.
_SERVICE_QBO_ITEM_IDS: dict[str, str] = {
    "std-residential":    "19",
    "deep-clean":         "20",
    "move-in-out":        "21",
    "recurring-weekly":   "22",
    "recurring-biweekly": "23",
    "recurring-monthly":  "24",
    "commercial-nightly": "25",
}


def _build_catalogue() -> dict[str, dict]:
    catalogue: dict[str, dict] = {}
    for entry in SERVICE_TYPES:
        service_id = entry["id"]
        catalogue[service_id] = {
            "display_name":     entry["name"],
            "duration_minutes": entry["duration_minutes"],
            "base_price":       entry["base_price"],
            "service_category": entry["service_category"],
            "qbo_item_id":      _SERVICE_QBO_ITEM_IDS.get(service_id),
        }
    return catalogue


SERVICE_CATALOGUE: dict[str, dict] = _build_catalogue()
CANONICAL_SERVICE_IDS: frozenset[str] = frozenset(SERVICE_CATALOGUE.keys())


# Specificity ranking used by the fuzzy matcher to disambiguate a label
# that matches aliases for multiple canonical IDs. Recurring and commercial
# aliases describe a frequency or channel that is more specific than the
# generic "residential clean" base; without this priority, a label like
# "Monthly Recurring Residential Clean" would match the `residential clean`
# alias first (both candidates have the same token length) and resolve to
# `std-residential`, silently undercharging the customer.
_CANONICAL_PRIORITY: dict[str, int] = {
    "commercial-nightly": 3,
    "recurring-weekly":   3,
    "recurring-biweekly": 3,
    "recurring-monthly":  3,
    "move-in-out":        2,
    "deep-clean":         2,
    "std-residential":    1,
}


# Free-text label -> canonical ID. Keys are normalized (lowercase, single
# spaces, dashes/underscores folded). canonical_service_id() handles the
# normalization so callers pass raw strings.
SERVICE_ALIASES: dict[str, str] = {
    "standard residential clean": "std-residential",
    "std residential":            "std-residential",
    "std residential clean":      "std-residential",
    "residential clean":          "std-residential",
    "residential":                "std-residential",

    "deep clean":                 "deep-clean",
    "deep cleaning":              "deep-clean",

    "move in move out clean":     "move-in-out",
    "move in out clean":          "move-in-out",
    "move in out":                "move-in-out",
    "move in move out":           "move-in-out",
    "move out":                   "move-in-out",
    "move in":                    "move-in-out",

    "recurring weekly":           "recurring-weekly",
    "weekly recurring":           "recurring-weekly",
    "weekly":                     "recurring-weekly",

    "recurring biweekly":         "recurring-biweekly",
    "biweekly recurring":         "recurring-biweekly",
    "biweekly":                   "recurring-biweekly",

    "recurring monthly":          "recurring-monthly",
    "monthly recurring":          "recurring-monthly",
    "monthly":                    "recurring-monthly",

    "commercial nightly clean":   "commercial-nightly",
    "commercial nightly":         "commercial-nightly",
    "commercial":                 "commercial-nightly",
}


def _normalize_label(value: Optional[str]) -> str:
    """Fold casing, punctuation, and whitespace for alias lookup."""
    if not value:
        return ""
    text = re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()
    return re.sub(r"\s+", " ", text)


def canonical_service_id(value: Optional[str]) -> Optional[str]:
    """
    Resolve `value` to a canonical service ID.

    Accepts:
      - A canonical ID (e.g., "deep-clean") — returned as-is.
      - A known free-text label (e.g., "Standard Residential Clean").
      - A partial/fuzzy label matched against SERVICE_ALIASES.

    Returns None when nothing matches, so callers can emit a fallback
    alert instead of silently defaulting.
    """
    if not value:
        return None

    raw = value.strip()
    if raw in CANONICAL_SERVICE_IDS:
        return raw

    normalized = _normalize_label(raw)
    if not normalized:
        return None

    if normalized in SERVICE_ALIASES:
        return SERVICE_ALIASES[normalized]

    # Also accept dashed-canonical forms inside a label (e.g., "deep-clean").
    dashed = normalized.replace(" ", "-")
    if dashed in CANONICAL_SERVICE_IDS:
        return dashed

    # Fuzzy fallback: a Jobber title like "Biweekly Recurring Residential
    # Clean" contains BOTH a frequency token ("biweekly recurring") and a
    # generic residential token ("residential clean"). Collect every alias
    # that matches, then pick the highest-priority canonical (frequency or
    # commercial beats generic residential); tiebreak by alias length so
    # the most specific phrase wins. Returning the first match by dict
    # order would silently downgrade recurring labels to std-residential.
    padded = f" {normalized} "
    matches: list[tuple[str, str]] = []
    for alias, canonical in SERVICE_ALIASES.items():
        padded_alias = f" {alias} "
        if padded_alias in padded or padded in padded_alias:
            matches.append((canonical, alias))

    if not matches:
        return None

    matches.sort(
        key=lambda item: (_CANONICAL_PRIORITY.get(item[0], 0), len(item[1])),
        reverse=True,
    )
    return matches[0][0]


def get_service_metadata(canonical_id: str) -> Optional[dict]:
    """Return a copy of the catalogue entry for `canonical_id`, or None."""
    entry = SERVICE_CATALOGUE.get(canonical_id)
    return dict(entry) if entry else None
