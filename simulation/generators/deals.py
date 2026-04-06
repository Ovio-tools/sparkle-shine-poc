"""
simulation/generators/deals.py

Advances existing Pipedrive deals through the sales pipeline.
Each execute() call picks one open deal and probabilistically
advances it, marks it lost, or leaves it unchanged.

Type 2 generator: progresses existing records (does not create new ones).
Dry-run convention: reads always allowed; writes (API + SQLite) skipped.
"""
from __future__ import annotations

import json
import random
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

from auth import get_client
from config.business import SERVICE_TYPES
from database.connection import get_connection, get_column_names
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

    def __init__(self):
        tool_ids = json.loads(Path("config/tool_ids.json").read_text())
        stages = tool_ids["pipedrive"]["stages"]
        self._stage_order = [
            stages["New Lead"],
            stages["Qualified"],
            stages["Site Visit Scheduled"],
            stages["Proposal Sent"],
            stages["Negotiation"],
            stages["Closed Won"],
        ]
        self._won_stage_id  = stages["Closed Won"]
        self._lost_stage_id = stages["Closed Lost"]
        self._stage_names   = {v: k for k, v in stages.items()}

        fields = tool_ids["pipedrive"]["deal_fields"]
        self._client_type_field = fields["Client Type"]
        self._service_type_field = fields["Service Type"]
        self._emv_field          = fields["Estimated Monthly Value"]

        with get_connection() as conn:
            self._ensure_schema(conn)

    def execute(self, dry_run: bool = False) -> GeneratorResult:
        """Pick one open deal and advance, lose, or leave it unchanged."""
        try:
            deal = self._pick_deal()
        except Exception as e:
            return GeneratorResult(success=False, message=f"pipedrive fetch failed: {e}")

        if deal is None:
            return GeneratorResult(success=False, message="no open deals")

        return self._advance_deal(deal, dry_run=dry_run)

    def _pick_deal(self) -> Optional[dict]:
        """Fetch open deals from Pipedrive and return one at random (uniform).

        Filters out deals already at the Closed Won stage — these are
        zombie deals whose Pipedrive status was not correctly set to 'won'.
        Without this filter they waste execution slots (picked but no-op).
        """
        time.sleep(0.15)
        client = get_client("pipedrive")
        resp = client.get(
            "https://api.pipedrive.com/v1/deals",
            params={"status": "open", "sort": "update_time DESC", "limit": 100},
        )
        resp.raise_for_status()
        deals = resp.json().get("data") or []
        # Exclude deals already at Closed Won / Closed Lost stages
        deals = [d for d in deals if d.get("stage_id") not in (self._won_stage_id, self._lost_stage_id)]
        if not deals:
            return None
        return random.choices(deals, k=1)[0]

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
        """Roll advance and loss dice; execute the winning action or return no-change."""
        deal_id = deal["id"]

        # ── Roll 1: advance ──────────────────────────────────────────────────────
        if random.random() < self.calculate_advance_probability(deal):
            current_stage_id = deal.get("stage_id")
            if current_stage_id not in self._stage_order:
                return GeneratorResult(
                    success=False,
                    message=f"deal {deal_id}: unknown stage {current_stage_id}",
                )
            idx = self._stage_order.index(current_stage_id)
            if idx >= len(self._stage_order) - 1:
                return GeneratorResult(success=True, message="no change")
            next_stage_id = self._stage_order[idx + 1]

            if next_stage_id == self._won_stage_id:
                contract = self._build_contract(deal)
                self._complete_won_deal(deal, contract, dry_run=dry_run)
                return GeneratorResult(success=True, message=f"deal {deal_id} advanced to Won")

            if not dry_run:
                time.sleep(0.15)
                client = get_client("pipedrive")
                resp = client.put(
                    f"https://api.pipedrive.com/v1/deals/{deal_id}",
                    json={"stage_id": next_stage_id},
                )
                if resp.status_code not in (200, 201):
                    logger.warning("PUT deals/%s failed: %s", deal_id, resp.status_code)
                    return GeneratorResult(success=False, message=f"PUT deals failed: {resp.status_code}")

                # Write stage_change_time to DB for SS-PROP deals only
                canonical_id = get_canonical_id("pipedrive", str(deal_id))
                if canonical_id and canonical_id.startswith("SS-PROP-"):
                    with get_connection() as conn:
                        conn.execute(
                            "UPDATE commercial_proposals SET stage_change_time = CURRENT_TIMESTAMP WHERE id = %s",
                            (canonical_id,),
                        )

                stage_name = self._stage_names.get(next_stage_id, str(next_stage_id))
                self._log_activity(deal_id, f"Deal advanced to {stage_name}.", dry_run=dry_run)
            else:
                logger.debug("[dry_run] Would advance deal %s to stage %s", deal_id, next_stage_id)

            return GeneratorResult(success=True, message=f"deal {deal_id} advanced to stage {next_stage_id}")

        # ── Roll 2: loss (only if advance did not fire) ──────────────────────────
        if random.random() < self.calculate_loss_probability(deal):
            if not dry_run:
                reason = random.choice(DAILY_VOLUMES["deal_progression"]["lost_reasons"])
                time.sleep(0.15)
                client = get_client("pipedrive")
                resp = client.put(
                    f"https://api.pipedrive.com/v1/deals/{deal_id}",
                    json={"status": "lost", "stage_id": self._lost_stage_id},
                )
                if resp.status_code not in (200, 201):
                    logger.warning("PUT deals/%s (loss) failed: %s", deal_id, resp.status_code)
                    return GeneratorResult(success=False, message=f"PUT deals failed: {resp.status_code}")
                self._log_activity(deal_id, f"Deal lost. Reason: {reason}", dry_run=dry_run)
            else:
                logger.debug("[dry_run] Would mark deal %s as lost", deal_id)
            return GeneratorResult(success=True, message=f"deal {deal_id} marked lost")

        return GeneratorResult(success=True, message="no change")

    def _complete_won_deal(self, deal: dict, contract: dict, dry_run: bool = False) -> None:
        """Write won-deal contract details to Pipedrive and SQLite."""
        deal_id = deal["id"]

        if not dry_run:
            time.sleep(0.15)
            client = get_client("pipedrive")
            resp = client.put(
                f"https://api.pipedrive.com/v1/deals/{deal_id}",
                json={
                    "stage_id": self._won_stage_id,
                    "status": "won",
                    self._client_type_field:  contract["contract_type"],
                    self._service_type_field: contract["service_frequency"],
                    self._emv_field:          contract["contract_value"],
                },
            )
            if resp.status_code not in (200, 201):
                logger.warning("PUT deals/%s (won) failed: %s", deal_id, resp.status_code)
                return
        else:
            logger.debug("[dry_run] Would mark deal %s as won", deal_id)

        # DB: commercial deals (SS-PROP) only
        # canonical ID lookup always performed — reads are permitted in dry_run (Type 2 generator)
        canonical_id = get_canonical_id("pipedrive", str(deal_id))
        if canonical_id is None:
            logger.warning(
                "Won deal %s has no canonical ID mapping — skipping DB update", deal_id
            )
        elif canonical_id.startswith("SS-PROP-"):
            if not dry_run:
                with get_connection() as conn:
                    conn.execute(
                        "UPDATE commercial_proposals "
                        "SET status='won', start_date=%s, crew_assignment=%s "
                        "WHERE id=%s",
                        (
                            contract["start_date"].isoformat(),
                            contract["crew_assignment"],
                            canonical_id,
                        ),
                    )

        # won_deals: written for ALL won deals (SS-PROP and SS-LEAD).
        # This is the operations generator's trigger table — it needs start_date
        # queryable for both commercial and residential won deals.
        if not dry_run:
            with get_connection() as conn:
                conn.execute(
                    "INSERT INTO won_deals "
                    "(canonical_id, client_type, service_frequency, contract_value, "
                    " start_date, crew_assignment, pipedrive_deal_id) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT (canonical_id) DO NOTHING",
                    (
                        canonical_id,
                        contract["contract_type"],
                        contract["service_frequency"],
                        contract["contract_value"],
                        contract["start_date"].isoformat(),
                        contract["crew_assignment"],
                        deal["id"],
                    ),
                )

        start = contract["start_date"].isoformat()
        crew  = contract["crew_assignment"]
        svc   = contract["service_frequency"]
        val   = contract["contract_value"]
        note = (
            f"Deal won. Contract details:\n"
            f"Start date: {start}\n"
            f"Crew: {crew}\n"
            f"Service: {svc}, ${val:.2f}/visit"
        )
        self._log_activity(deal_id, note, dry_run=dry_run)

    def _log_activity(self, deal_id: int, note: str, dry_run: bool = False) -> None:
        """POST a note activity to Pipedrive for the given deal."""
        if dry_run:
            logger.debug("[dry_run] Activity note for deal %s: %s", deal_id, note)
            return
        time.sleep(0.15)
        client = get_client("pipedrive")
        resp = client.post(
            "https://api.pipedrive.com/v1/activities",
            json={
                "deal_id": deal_id,
                "subject": "Stage update",
                "type": "note",
                "note": note,
                "done": 1,
            },
        )
        if resp.status_code not in (200, 201):
            logger.warning("POST activities for deal %s failed: %s", deal_id, resp.status_code)

    def _ensure_schema(self, conn) -> None:
        """Add missing columns to commercial_proposals if absent."""
        existing = get_column_names(conn, "commercial_proposals")
        for col_name, col_type in [
            ("start_date",        "TEXT"),
            ("crew_assignment",   "TEXT"),
            ("stage_change_time", "TEXT"),
        ]:
            if col_name not in existing:
                conn.execute(
                    f"ALTER TABLE commercial_proposals ADD COLUMN {col_name} {col_type}"
                )

        # Primary owner of won_deals table — operations generator reads from it but
        # deals generator writes first, so the table must exist before any won deal fires.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS won_deals (
                canonical_id      TEXT PRIMARY KEY,
                client_type       TEXT NOT NULL,
                service_frequency TEXT NOT NULL,
                contract_value    REAL,
                start_date        TEXT NOT NULL,
                crew_assignment   TEXT,
                pipedrive_deal_id INTEGER
            )
        """)

    # ── Internal helpers ────────────────────────────────────────────────────

    def _days_in_stage(self, deal: dict) -> float:
        """Days the deal has been in its current stage (3-level fallback)."""
        now = datetime.utcnow()

        # Level 1: Pipedrive native stage_change_time
        sct = deal.get("stage_change_time")
        if sct:
            return (now - datetime.fromisoformat(sct.replace(" ", "T"))).days

        # Level 2: DB commercial_proposals.stage_change_time (SS-PROP only)
        try:
            canonical_id = get_canonical_id("pipedrive", str(deal["id"]))
            if canonical_id and canonical_id.startswith("SS-PROP-"):
                with get_connection() as conn:
                    row = conn.execute(
                        "SELECT stage_change_time FROM commercial_proposals WHERE id = %s",
                        (canonical_id,),
                    ).fetchone()
                    if row and row["stage_change_time"]:
                        return (now - datetime.fromisoformat(row["stage_change_time"])).days
        except Exception:
            pass

        # Level 3: update_time as last-resort proxy
        update_time = deal.get("update_time", "")
        if update_time:
            return (now - datetime.fromisoformat(update_time.replace(" ", "T"))).days
        return 0.0

    def _pick_service_frequency(self, client_type: str, emv: float) -> str:
        """Pick service frequency from the pool matching client type and EMV."""
        if client_type == "commercial":
            return random.choices(
                list(COMMERCIAL_SERVICE_WEIGHTS.keys()),
                weights=list(COMMERCIAL_SERVICE_WEIGHTS.values()),
                k=1,
            )[0]
        if emv > 0:
            pool = ["weekly_recurring", "biweekly_recurring", "monthly_recurring"]
        else:
            pool = ["one_time_standard", "one_time_deep_clean", "one_time_move_in_out"]
        weights = [SERVICE_TYPE_WEIGHTS[k] for k in pool]
        return random.choices(pool, weights=weights, k=1)[0]

    def _build_contract(self, deal: dict) -> dict:
        """Build the contract dict when a deal advances to Won."""
        client_type = deal.get(self._client_type_field) or "residential"
        emv_raw = deal.get(self._emv_field)
        try:
            emv = float(emv_raw) if emv_raw is not None else 0.0
        except (TypeError, ValueError):
            emv = 0.0

        service_frequency = self._pick_service_frequency(client_type, emv)
        contract_value    = self._get_contract_value(service_frequency)
        crew = random.choices(
            list(CREW_ASSIGNMENT_WEIGHTS.keys()),
            weights=list(CREW_ASSIGNMENT_WEIGHTS.values()),
            k=1,
        )[0]
        start_date = _add_business_days(date.today(), random.randint(5, 10))
        return {
            "contract_type":     client_type,
            "service_frequency": service_frequency,
            "contract_value":    contract_value,
            "start_date":        start_date,
            "crew_assignment":   crew,
        }

    @staticmethod
    def _get_contract_value(service_frequency: str) -> float:
        """Return contract value derived from service type base price."""
        service_id = _FREQ_TO_SERVICE_ID.get(service_frequency)
        if service_id:
            return _SERVICE_ID_PRICES.get(service_id, 150.00)
        lo, hi = _COMMERCIAL_RANGES.get(service_frequency, (500, 2000))
        return round(random.uniform(lo, hi), 2)
