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


def compute_all_metrics(db_path: str, briefing_date: str) -> dict:
    """Run all 6 metrics modules and return a combined dict.

    Args:
        db_path: Path to sparkle_shine.db
        briefing_date: ISO date string, e.g. "2026-03-17"

    Returns:
        {
            "revenue": ...,
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
        return {
            "revenue": revenue.compute(db, briefing_date),
            "operations": operations.compute(db, briefing_date),
            "sales": sales.compute(db, briefing_date),
            "financial_health": financial_health.compute(db, briefing_date),
            "marketing": marketing.compute(db, briefing_date),
            "tasks": tasks.compute(db, briefing_date),
            "computed_at": datetime.utcnow().isoformat(),
        }
    finally:
        db.close()
