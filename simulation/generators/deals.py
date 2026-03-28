"""
simulation/generators/deals.py

Advances existing Pipedrive deals through the sales pipeline.
Each execute() call picks one open deal and probabilistically
advances it, marks it lost, or leaves it unchanged.

Type 2 generator: progresses existing records (does not create new ones).
# Dry-run convention: reads always allowed; writes (API + SQLite) skipped.
"""
from __future__ import annotations

import json
import random
import sqlite3
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

from auth import get_client
from config.business import SERVICE_TYPES
from database.mappings import get_canonical_id
from intelligence.logging_config import setup_logging
from simulation.config import (
    COMMERCIAL_SERVICE_WEIGHTS,
    CREW_ASSIGNMENT_WEIGHTS,
    DAILY_VOLUMES,
    SERVICE_TYPE_WEIGHTS,
)

logger = setup_logging("simulation.deals")


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------

@dataclass
class GeneratorResult:
    success: bool
    message: str = ""


# ---------------------------------------------------------------------------
# Module-level price/range lookups (derived from config/business.py)
# ---------------------------------------------------------------------------

_SERVICE_ID_PRICES = {
    st["id"]: st["base_price"] for st in SERVICE_TYPES if st.get("base_price")
}

_FREQ_TO_SERVICE_ID = {
    "weekly_recurring":     "recurring-weekly",
    "biweekly_recurring":   "recurring-biweekly",
    "monthly_recurring":    "recurring-monthly",
    "one_time_standard":    "std-residential",
    "one_time_deep_clean":  "deep-clean",
    "one_time_move_in_out": "move-in-out",
}

_COMMERCIAL_RANGES = {
    "nightly_clean":      (1500, 4500),
    "weekend_deep_clean": (300, 800),
    "one_time_project":   (500, 2000),
}


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def _advance_age_weight(days: float) -> float:
    """Age bracket multiplier for advance probability."""
    if days <= 3:
        return 1.0
    elif days <= 7:
        return 1.5
    elif days <= 14:
        return 2.0
    else:
        return 0.5


def _loss_age_weight(days: float) -> float:
    """Age bracket multiplier for loss probability (inverted — stale deals bleed out)."""
    if days <= 3:
        return 0.5
    elif days <= 7:
        return 1.0
    elif days <= 14:
        return 1.5
    else:
        return 2.5


def _add_business_days(start: date, n: int) -> date:
    """Return the date that is `n` business days (Mon–Fri) after `start`."""
    d = start
    added = 0
    while added < n:
        d += timedelta(days=1)
        if d.weekday() < 5:
            added += 1
    return d


# ---------------------------------------------------------------------------
# DealGenerator
# ---------------------------------------------------------------------------

class DealGenerator:
    """
    Advances existing Pipedrive deals through the sales pipeline.

    Registered with the simulation engine as the "deals" generator.
    Engine calls execute(dry_run=...) on each tick.
    """

    def __init__(self, db_path: str = "sparkle_shine.db"):
        raise NotImplementedError("Task 2 — implement __init__")

    def execute(self, dry_run: bool = False) -> GeneratorResult:
        raise NotImplementedError("Task 6 — implement execute")

    def _pick_deal(self) -> Optional[dict]:
        raise NotImplementedError("Task 3 — implement _pick_deal")

    def calculate_advance_probability(self, deal: dict) -> float:
        """Return probability [0, 1] that this deal advances today."""
        base = DAILY_VOLUMES["deal_progression"]["stage_advance_probability"]
        age = self._days_in_stage(deal)
        return base * _advance_age_weight(age)

    def calculate_loss_probability(self, deal: dict) -> float:
        """Return probability [0, 1] that this deal is lost today."""
        base = DAILY_VOLUMES["deal_progression"]["lost_probability_per_stage"]
        age = self._days_in_stage(deal)
        return base * _loss_age_weight(age)

    def _advance_deal(self, deal: dict, dry_run: bool = False) -> GeneratorResult:
        raise NotImplementedError("Task 4 — implement _advance_deal")

    def _complete_won_deal(self, deal: dict, contract: dict, dry_run: bool = False) -> None:
        raise NotImplementedError("Task 5 — implement _complete_won_deal")

    def _log_activity(self, deal_id: int, note: str, dry_run: bool = False) -> None:
        raise NotImplementedError("Task 3 — implement _log_activity")

    def _ensure_schema(self, conn: sqlite3.Connection) -> None:
        raise NotImplementedError("Task 2 — implement _ensure_schema")

    # ── Internal helpers ────────────────────────────────────────────────────

    def _days_in_stage(self, deal: dict) -> float:
        """Days the deal has been in its current stage (3-level fallback)."""
        now = datetime.utcnow()

        # Level 1: Pipedrive native stage_change_time
        sct = deal.get("stage_change_time")
        if sct:
            return (now - datetime.fromisoformat(sct.replace(" ", "T"))).days

        # Level 2: SQLite commercial_proposals.stage_change_time (SS-PROP only)
        try:
            canonical_id = get_canonical_id("pipedrive", str(deal["id"]), self.db_path)
            if canonical_id and canonical_id.startswith("SS-PROP-"):
                with sqlite3.connect(self.db_path) as conn:
                    row = conn.execute(
                        "SELECT stage_change_time FROM commercial_proposals WHERE id = ?",
                        (canonical_id,),
                    ).fetchone()
                    if row and row[0]:
                        return (now - datetime.fromisoformat(row[0])).days
        except Exception:
            pass

        # Level 3: update_time as last-resort proxy
        update_time = deal.get("update_time", "")
        if update_time:
            return (now - datetime.fromisoformat(update_time.replace(" ", "T"))).days
        return 0.0

    def _pick_service_frequency(self, client_type: str, emv: float) -> str:
        raise NotImplementedError("Task 4 — implement _pick_service_frequency")

    def _build_contract(self, deal: dict) -> dict:
        raise NotImplementedError("Task 4 — implement _build_contract")

    @staticmethod
    def _get_contract_value(service_frequency: str) -> float:
        raise NotImplementedError("Task 4 — implement _get_contract_value")
