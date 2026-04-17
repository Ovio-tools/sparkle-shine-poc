"""
intelligence/metrics/__init__.py

Entry point for the metrics layer. Opens one DB connection, runs all 6
modules, and returns a combined dict for the context builder.
"""

from datetime import datetime

from database.schema import get_connection
from intelligence.metrics import (
    financial_health,
    marketing,
    operations,
    revenue,
    sales,
    tasks,
)

# Legacy top-level keys that alias the canonical `booked_revenue` dict.
# Kept so older consumers (templates, fixtures, external callers) keep
# working through Track A's rename window. New code should read from
# `booked_revenue`. See docs/revenue-remediation-plan-2026-04.md Track A.
LEGACY_REVENUE_SHIM_KEYS: tuple[str, ...] = ("revenue",)


def compute_all_metrics(db_path: str, briefing_date: str) -> dict:
    """Run all 6 metrics modules and return a combined dict.

    Args:
        db_path: Path to sparkle_shine.db
        briefing_date: ISO date string, e.g. "2026-03-17"

    Returns:
        {
            "booked_revenue": ...,   # canonical — booked revenue + cash collected
            "revenue": ...,          # compatibility shim; same object as booked_revenue
            "operations": ...,
            "sales": ...,
            "financial_health": ...,
            "marketing": ...,
            "tasks": ...,
            "computed_at": "<ISO datetime>"
        }
    """
    db = get_connection(db_path)
    try:
        revenue_metrics = revenue.compute(db, briefing_date)
        result = {
            "booked_revenue": revenue_metrics,
            "operations": operations.compute(db, briefing_date),
            "sales": sales.compute(db, briefing_date),
            "financial_health": financial_health.compute(db, briefing_date),
            "marketing": marketing.compute(db, briefing_date),
            "tasks": tasks.compute(db, briefing_date),
            "computed_at": datetime.utcnow().isoformat(),
        }
        for legacy_key in LEGACY_REVENUE_SHIM_KEYS:
            result[legacy_key] = revenue_metrics
        return result
    finally:
        db.close()
