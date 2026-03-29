# Simulation Engine Framework Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create the `simulation/` package with fully calibrated config, variation math, and a complete `SimulationEngine` class, plus update `intelligence/config.py` revenue targets.

**Architecture:** A single `SimulationEngine` class owns the event loop, generator registry, checkpoint state, and signal handling. `plan_day()` builds a shuffled list of `GeneratorCall` namedtuples; `run_once()` pops and dispatches them with Poisson-like timing delays; `run()` calls `run_once()` in a continuous loop with a midnight sleep. Generators are registered via conditional imports so the engine runs cleanly before any generator modules exist.

**Tech Stack:** Python 3, `unittest` + `unittest.mock` for tests, `argparse` for CLI, `signal` for graceful shutdown, `pathlib.Path` for file I/O, no new dependencies.

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `simulation/__init__.py` | Create | Empty — makes `simulation` a package |
| `simulation/config.py` | Create | All calibrated config: daily volumes, churn rates, job variety, seasonal/day-of-week weights, `config_math_trace()` |
| `simulation/variation.py` | Create | `get_daily_multiplier()`, `get_adjusted_volume()`, `should_event_happen()`, `get_next_event_delay()` |
| `simulation/engine.py` | Create | `SimulationEngine` class, `GeneratorCall` namedtuple, CLI `__main__` block |
| `intelligence/config.py` | Modify | Replace `REVENUE_TARGETS` dict only; all other constants untouched |
| `tests/test_simulation.py` | Create | All unit tests for the above (no live API calls) |

---

## Task 1: simulation package + config.py

**Files:**
- Create: `simulation/__init__.py`
- Create: `simulation/config.py`
- Create: `tests/test_simulation.py`

- [ ] **Step 1.1: Create the empty package init**

```python
# simulation/__init__.py
```

(The file is empty. Create it so `simulation` is a Python package.)

- [ ] **Step 1.2: Write the failing tests for config.py**

Create `tests/test_simulation.py`:

```python
"""
tests/test_simulation.py

Unit tests for the simulation engine framework.
No live API calls. No SQLite writes to sparkle_shine.db.

Run:
    python tests/test_simulation.py -v
    python tests/test_simulation.py -v -k "Config"
"""

from __future__ import annotations

import io
import json
import os
import random
import sys
import tempfile
import unittest
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


class TestSimulationConfig(unittest.TestCase):

    def test_daily_volumes_keys_exist(self):
        from simulation.config import DAILY_VOLUMES
        self.assertIn("new_contacts", DAILY_VOLUMES)
        self.assertIn("deal_progression", DAILY_VOLUMES)
        self.assertIn("job_completion", DAILY_VOLUMES)
        self.assertIn("payments", DAILY_VOLUMES)
        self.assertIn("churn", DAILY_VOLUMES)
        self.assertIn("task_completion", DAILY_VOLUMES)

    def test_new_contacts_values_are_numeric(self):
        from simulation.config import DAILY_VOLUMES
        nc = DAILY_VOLUMES["new_contacts"]
        self.assertIsInstance(nc["base_min"], int)
        self.assertIsInstance(nc["base_max"], int)
        self.assertLess(nc["base_min"], nc["base_max"])
        self.assertIsInstance(nc["sql_fraction"], float)
        self.assertGreater(nc["sql_fraction"], 0.0)
        self.assertLess(nc["sql_fraction"], 1.0)

    def test_lifecycle_distribution_sums_to_one(self):
        from simulation.config import DAILY_VOLUMES
        dist = DAILY_VOLUMES["new_contacts"]["lifecycle_distribution"]
        total = sum(dist.values())
        self.assertAlmostEqual(total, 1.0, places=5)

    def test_churn_rates_are_positive_fractions(self):
        from simulation.config import DAILY_VOLUMES
        churn = DAILY_VOLUMES["churn"]
        self.assertGreater(churn["monthly_residential_churn_rate"], 0.0)
        self.assertLess(churn["monthly_residential_churn_rate"], 1.0)
        self.assertGreater(churn["monthly_commercial_churn_rate"], 0.0)
        self.assertLess(churn["monthly_commercial_churn_rate"], 1.0)

    def test_seasonal_weights_cover_all_months(self):
        from simulation.config import SEASONAL_WEIGHTS
        self.assertEqual(set(SEASONAL_WEIGHTS.keys()), set(range(1, 13)))

    def test_day_of_week_weights_cover_all_days(self):
        from simulation.config import DAY_OF_WEEK_WEIGHTS
        self.assertEqual(set(DAY_OF_WEEK_WEIGHTS.keys()), set(range(7)))

    def test_config_math_trace_runs_without_error(self):
        from simulation.config import config_math_trace
        # Should print output and not raise
        captured = io.StringIO()
        with patch("sys.stdout", captured):
            config_math_trace()
        output = captured.getvalue()
        self.assertIn("NET MONTHLY CLIENT CHANGE", output)
        self.assertIn("HEALTHY GROWTH", output)

    def test_job_variety_rates_sum_to_one_residential(self):
        from simulation.config import JOB_VARIETY
        res = JOB_VARIETY["residential_recurring"]
        total = res["regular_clean_rate"] + res["deep_clean_rate"] + res["add_on_rate"]
        self.assertAlmostEqual(total, 1.0, places=5)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 1.3: Run tests to verify they fail**

```bash
cd /Users/ovieoghor/Documents/Claude\ Code\ Exercises/Simulation\ Exercise/sparkle-shine-poc
python tests/test_simulation.py -v -k "Config"
```

Expected: `ModuleNotFoundError: No module named 'simulation'`

- [ ] **Step 1.4: Create simulation/config.py**

```python
"""
simulation/config.py

Calibrated configuration for the Sparkle & Shine simulation engine.
All numeric values have inline source citations (L1).
After any change, run config_math_trace() to verify net +3 to +5 clients/month (L2).
"""

# ────────────────────────────────────────────────────────────────
# DAILY ACTIVITY VOLUMES
# Calibrated to produce net +3 to +5 new clients per month.
# Run config_math_trace() to verify after any changes.
# ────────────────────────────────────────────────────────────────

DAILY_VOLUMES = {
    "new_contacts": {
        "base_min": 3,
        "base_max": 8,
        # Source: HomeAdvisor/Angi data shows 3-10 daily inquiries
        # for established home service businesses in metro areas.
        # Austin market is mid-range. Using 3-8 as base before
        # seasonal/day-of-week adjustments.

        "sql_fraction": 0.35,
        # ESTIMATED -- reasoning: 35% of cleaning inquiries are
        # serious enough to warrant a sales conversation. Higher
        # than SaaS (5-15%) because cleaning is a considered but
        # not complex purchase. Adjusted up from 0.30 to produce
        # enough pipeline to support ~8-10 wins/month.

        "lifecycle_distribution": {
            "subscriber": 0.20,                # newsletter signups, not ready to buy
            "lead": 0.25,                      # showed interest, needs nurturing
            "marketing_qualified_lead": 0.20,  # engaged with content/campaigns
            "sales_qualified_lead": 0.35,      # matches sql_fraction
        }
    },

    "deal_progression": {
        "stage_advance_probability": 0.15,
        # ESTIMATED -- reasoning: on any given day, there's a 15%
        # chance an open deal moves forward. At 5 stages, average
        # cycle is ~33 days (consistent with home services sales).

        "won_probability_from_negotiation": 0.40,
        # ESTIMATED -- reasoning: 40% close rate from final stage.
        # Home services close rates are 30-50% per HomeAdvisor.
        # Using 40% (mid-range) to produce ~8-10 wins/month from
        # the pipeline of ~20-25 deals reaching Negotiation.
        # Adjusted up from 0.35 to offset pipeline leakage.

        "lost_probability_per_stage": 0.03,
        # ESTIMATED -- reasoning: 3% daily loss rate per stage.
        # Lower than original 5% to reduce pipeline leakage.
        # At 3%/day over 5 stages (~33 day cycle), roughly 40%
        # of leads are lost before reaching Negotiation.
        # Combined with 40% Negotiation win rate, overall
        # lead-to-close is ~24%, consistent with industry data.

        "lost_reasons": [
            "Budget constraints",
            "Chose competitor",
            "Project postponed",
            "No response after follow-up",
            "Scope mismatch",
        ]
    },

    "job_completion": {
        "on_time_rate": 0.92,
        # Source: Field service industry benchmark. 90-95% completion
        # rate for scheduled residential cleaning visits.

        "cancellation_rate": 0.03,
        "no_show_rate": 0.02,
        "reschedule_rate": 0.03,
        "duration_variance_percent": 15,
    },

    "payments": {
        "on_time_rate": 0.75,
        # Source: QuickBooks small business report 2024: 73% of
        # invoices paid within terms for service businesses.

        "late_1_30_rate": 0.15,
        "late_31_60_rate": 0.07,
        "late_60_plus_rate": 0.02,
        "non_payment_rate": 0.01,
    },

    "churn": {
        "monthly_residential_churn_rate": 0.025,
        # ESTIMATED -- reasoning: 2.5% monthly = ~26% annual.
        # Cleaning industry annual churn is typically 25-35%.
        # Using lower end because Austin is a stable market and
        # the business has a referral-heavy client base.
        # At ~180 active residential clients, this produces
        # ~4.5 churned clients/month.
        # CRITICAL: original value was 0.04 (4%), which produced
        # 7-8 churns/month and a shrinking business. Reduced per
        # config_math_trace() validation.

        "monthly_commercial_churn_rate": 0.015,
        # ESTIMATED -- reasoning: 1.5% monthly = ~17% annual.
        # Commercial contracts are stickier (net-30 billing,
        # longer onboarding investment, switching costs).
        # At ~8-10 active commercial clients, this produces
        # ~0.15 churns/month (roughly 1 every 6-7 months).

        "churn_reasons": [
            "Moving out of area",
            "Switching to competitor",
            "Budget cuts",
            "Dissatisfied with service",
            "No longer needs service",
            "Seasonal -- will return",
        ]
    },

    "task_completion": {
        "daily_completion_rate": 0.3,
        "maria_completion_rate": 0.15,
    },
}

# ────────────────────────────────────────────────────────────────
# EXISTING CLIENT JOB SCHEDULING
# ────────────────────────────────────────────────────────────────

JOB_VARIETY = {
    "residential_recurring": {
        "regular_clean_rate": 0.85,
        # 85% of scheduled visits are the standard service.

        "deep_clean_rate": 0.10,
        # ESTIMATED -- reasoning: most recurring clients do a
        # deep clean every 2-3 months. For biweekly clients
        # that's roughly 1 in 5-6 visits = ~17%. For weekly
        # clients it's 1 in 8-12 = ~10%. Using 10% as a
        # blended average across all frequencies.

        "add_on_rate": 0.05,
        # Source: Maid Brigade franchise data suggests 3-8% add-on
        # attachment rate for residential recurring clients.

        "deep_clean_price_multiplier": 1.80,
        # Standard clean $150, deep clean $275 = 1.83x multiplier.

        "add_on_options": [
            {"name": "Interior windows", "price": 45},
            {"name": "Refrigerator deep clean", "price": 35},
            {"name": "Oven cleaning", "price": 40},
            {"name": "Laundry (wash, dry, fold)", "price": 30},
            {"name": "Garage sweep and organize", "price": 55},
            {"name": "Patio/balcony cleaning", "price": 40},
        ],

        "seasonal_deep_clean_boost": {
            # Months where deep clean probability increases
            3: 1.5,   # March: spring cleaning
            4: 1.5,   # April: spring cleaning
            6: 1.3,   # June: summer prep
            11: 1.3,  # November: pre-holiday
            12: 1.5,  # December: holiday prep
        },
    },

    "commercial_recurring": {
        "standard_service_rate": 0.90,
        # 90% of commercial visits are the contracted service.

        "extra_service_rate": 0.10,
        # ESTIMATED -- reasoning: 10% include an add-on. Creates
        # the "upsell signal" pattern the intelligence layer detects.

        "extra_service_options": [
            {"name": "Carpet spot treatment", "price": 75},
            {"name": "Window washing (interior)", "price": 120},
            {"name": "Floor waxing", "price": 150},
            {"name": "Restroom deep sanitization", "price": 60},
            {"name": "Breakroom deep clean", "price": 45},
        ],
    },
}

# ────────────────────────────────────────────────────────────────
# TIMING AND VARIATION
# ────────────────────────────────────────────────────────────────

BUSINESS_HOURS = {
    "start": 7, "end": 18, "peak_start": 9, "peak_end": 14,
}

SEASONAL_WEIGHTS = {
    1: 0.70, 2: 0.85, 3: 0.95, 4: 1.00, 5: 1.05, 6: 1.25,
    7: 1.30, 8: 1.10, 9: 0.90, 10: 1.00, 11: 1.10, 12: 1.20,
}

DAY_OF_WEEK_WEIGHTS = {
    0: 1.15, 1: 1.10, 2: 1.05, 3: 1.00, 4: 0.90, 5: 0.40, 6: 0.20,
}

SERVICE_TYPE_WEIGHTS = {
    "weekly_recurring": 0.25,
    "biweekly_recurring": 0.35,
    "monthly_recurring": 0.15,
    "one_time_standard": 0.10,
    "one_time_deep_clean": 0.10,
    "one_time_move_in_out": 0.05,
}

COMMERCIAL_SERVICE_WEIGHTS = {
    "nightly_clean": 0.50,
    "weekend_deep_clean": 0.30,
    "one_time_project": 0.20,
}

CREW_ASSIGNMENT_WEIGHTS = {
    "Crew A": 0.30, "Crew B": 0.25, "Crew C": 0.25, "Crew D": 0.20,
}


# ────────────────────────────────────────────────────────────────
# CONFIG VALIDATION (L2)
# ────────────────────────────────────────────────────────────────

def config_math_trace():
    """Print expected monthly outcomes from daily probabilities.
    Run this after any config change to verify the business
    trajectory is realistic.

    Target: net +3 to +5 clients per month (slight growth).
    If this shows negative growth, the config is broken.
    """
    avg_daily_contacts = (DAILY_VOLUMES["new_contacts"]["base_min"]
                          + DAILY_VOLUMES["new_contacts"]["base_max"]) / 2
    avg_monthly_contacts = avg_daily_contacts * 22  # ~22 business days

    sqls_per_month = avg_monthly_contacts * DAILY_VOLUMES["new_contacts"]["sql_fraction"]

    deal_config = DAILY_VOLUMES["deal_progression"]
    loss_per_stage = deal_config["lost_probability_per_stage"]
    avg_days_per_stage = 1 / deal_config["stage_advance_probability"]
    stages_before_negotiation = 4
    cumulative_loss = 1.0
    for _ in range(stages_before_negotiation):
        days_in_stage = avg_days_per_stage
        survive_rate = (1 - loss_per_stage) ** days_in_stage
        cumulative_loss *= survive_rate
    deals_reaching_negotiation = sqls_per_month * cumulative_loss

    wins_per_month = deals_reaching_negotiation * deal_config["won_probability_from_negotiation"]

    churn_config = DAILY_VOLUMES["churn"]
    residential_churn = 180 * churn_config["monthly_residential_churn_rate"]
    commercial_churn = 9 * churn_config["monthly_commercial_churn_rate"]
    total_churn = residential_churn + commercial_churn

    net_change = wins_per_month - total_churn

    print("=" * 55)
    print("CONFIG MATH TRACE -- Expected Monthly Outcomes")
    print("=" * 55)
    print(f"  Avg daily contacts:          {avg_daily_contacts:.1f}")
    print(f"  Monthly contacts (~22 days): {avg_monthly_contacts:.0f}")
    print(f"  Monthly SQLs (x{DAILY_VOLUMES['new_contacts']['sql_fraction']:.0%}):       {sqls_per_month:.0f}")
    print(f"  Pipeline survival to Negotiation: {cumulative_loss:.0%}")
    print(f"  Deals reaching Negotiation:  {deals_reaching_negotiation:.0f}")
    print(f"  Won deals (x{deal_config['won_probability_from_negotiation']:.0%}):          {wins_per_month:.1f}")
    print(f"  ---")
    print(f"  Residential churn (180 x {churn_config['monthly_residential_churn_rate']:.1%}): {residential_churn:.1f}")
    print(f"  Commercial churn (9 x {churn_config['monthly_commercial_churn_rate']:.1%}):   {commercial_churn:.1f}")
    print(f"  Total churn:                 {total_churn:.1f}")
    print(f"  ---")
    print(f"  NET MONTHLY CLIENT CHANGE:   {net_change:+.1f}")
    print(f"  ---")
    if net_change < 0:
        print("  *** WARNING: SHRINKING BUSINESS. Fix churn or win rate. ***")
    elif net_change < 2:
        print("  FLAT to slight growth. Consider raising SQL fraction or win rate.")
    elif net_change <= 6:
        print("  HEALTHY GROWTH. Target range is +3 to +5.")
    else:
        print("  RAPID GROWTH. May be unrealistic. Consider lowering win rate.")
    print("=" * 55)


if __name__ == "__main__":
    config_math_trace()
```

- [ ] **Step 1.5: Run tests to verify they pass**

```bash
python tests/test_simulation.py -v -k "Config"
```

Expected: 8 tests, all PASS.

- [ ] **Step 1.6: Verify config_math_trace() output directly**

```bash
python simulation/config.py
```

Expected output includes `HEALTHY GROWTH. Target range is +3 to +5.` and a positive `NET MONTHLY CLIENT CHANGE`.

- [ ] **Step 1.7: Commit**

```bash
git add simulation/__init__.py simulation/config.py tests/test_simulation.py
git commit -m "Add simulation package with calibrated config and config_math_trace (Step 1)"
```

---

## Task 2: simulation/variation.py

**Files:**
- Create: `simulation/variation.py`
- Modify: `tests/test_simulation.py` (add `TestVariation` class)

- [ ] **Step 2.1: Write failing tests for variation.py**

Append to `tests/test_simulation.py` (before the `if __name__ == "__main__":` block):

```python
class TestVariation(unittest.TestCase):

    def setUp(self):
        # Fix RNG for deterministic tests
        random.seed(42)

    def test_get_daily_multiplier_returns_positive_float(self):
        from simulation.variation import get_daily_multiplier
        result = get_daily_multiplier(date(2026, 3, 27))  # Friday in March
        self.assertIsInstance(result, float)
        self.assertGreater(result, 0.0)

    def test_get_daily_multiplier_is_deterministic_with_seed(self):
        from simulation.variation import get_daily_multiplier
        random.seed(99)
        first = get_daily_multiplier(date(2026, 6, 15))
        random.seed(99)
        second = get_daily_multiplier(date(2026, 6, 15))
        self.assertEqual(first, second)

    def test_summer_multiplier_greater_than_winter(self):
        from simulation.variation import get_daily_multiplier
        # Use many samples to smooth out the noise component
        random.seed(0)
        summer_vals = [get_daily_multiplier(date(2026, 7, 7)) for _ in range(50)]  # Monday in July
        random.seed(0)
        winter_vals = [get_daily_multiplier(date(2026, 1, 5)) for _ in range(50)]  # Monday in Jan
        self.assertGreater(sum(summer_vals) / len(summer_vals),
                           sum(winter_vals) / len(winter_vals))

    def test_get_adjusted_volume_returns_non_negative_int(self):
        from simulation.variation import get_adjusted_volume
        result = get_adjusted_volume(3, 8, date(2026, 3, 27))
        self.assertIsInstance(result, int)
        self.assertGreaterEqual(result, 0)

    def test_get_adjusted_volume_respects_zero_floor(self):
        from simulation.variation import get_adjusted_volume
        # Even a weekend in January should not go negative
        for _ in range(20):
            result = get_adjusted_volume(0, 1, date(2026, 1, 4))  # Sunday Jan
            self.assertGreaterEqual(result, 0)

    def test_should_event_happen_never_for_zero_probability(self):
        from simulation.variation import should_event_happen
        for _ in range(100):
            self.assertFalse(should_event_happen(0.0, date(2026, 3, 27)))

    def test_should_event_happen_always_for_very_high_probability(self):
        from simulation.variation import should_event_happen
        # probability=10.0 adjusted by any multiplier still exceeds random() range of [0,1)
        for _ in range(50):
            self.assertTrue(should_event_happen(10.0, date(2026, 3, 27)))

    def test_get_next_event_delay_returns_positive_number(self):
        from simulation.variation import get_next_event_delay
        result = get_next_event_delay(date(2026, 3, 27))
        self.assertIsInstance(result, float)
        self.assertGreaterEqual(result, 30.0)

    def test_get_next_event_delay_minimum_is_30_seconds(self):
        from simulation.variation import get_next_event_delay
        for _ in range(30):
            result = get_next_event_delay(date(2026, 1, 4))  # low-volume day
            self.assertGreaterEqual(result, 30.0)
```

- [ ] **Step 2.2: Run tests to verify they fail**

```bash
python tests/test_simulation.py -v -k "Variation"
```

Expected: `ModuleNotFoundError: No module named 'simulation.variation'`

- [ ] **Step 2.3: Create simulation/variation.py**

```python
"""
simulation/variation.py

Applies daily multipliers (seasonal × day-of-week × noise) to volume
and probability values. Imported by the simulation engine and generators.
"""

import random
from datetime import date, datetime

from simulation.config import BUSINESS_HOURS, DAY_OF_WEEK_WEIGHTS, SEASONAL_WEIGHTS


def get_daily_multiplier(target_date: date) -> float:
    """Combine seasonal weight, day-of-week weight, and ±15% random noise.

    When the engine seeds the RNG with the target date (L7), calling this
    with the same date twice in the same process returns identical results.
    """
    seasonal = SEASONAL_WEIGHTS.get(target_date.month, 1.0)
    day_of_week = DAY_OF_WEEK_WEIGHTS.get(target_date.weekday(), 1.0)
    noise = random.uniform(0.85, 1.15)
    return seasonal * day_of_week * noise


def get_adjusted_volume(base_min: int, base_max: int, target_date: date) -> int:
    """Return a daily event count scaled by the daily multiplier. Minimum 0."""
    base = random.randint(base_min, base_max)
    multiplied = base * get_daily_multiplier(target_date)
    return max(0, round(multiplied))


def should_event_happen(probability: float, target_date: date) -> bool:
    """Return True with probability adjusted by the daily multiplier.

    Used for low-frequency per-entity events (churn checks, add-ons).
    A probability of 0.0 always returns False. Values above 1.0 after
    adjustment always return True since random.random() is in [0, 1).
    """
    adjusted = probability * get_daily_multiplier(target_date)
    return random.random() < adjusted


def get_next_event_delay(target_date: date) -> float:
    """Return seconds until the next simulated event.

    Busier days (higher multiplier) produce shorter delays.
    Peak hours (9-14): 3-15 min. Business hours (7-18): 10-30 min.
    Off-hours: 45-120 min. Minimum 30s regardless of multiplier.
    """
    now = datetime.now()
    hour = now.hour
    multiplier = get_daily_multiplier(target_date)

    peak_start = BUSINESS_HOURS["peak_start"]
    peak_end = BUSINESS_HOURS["peak_end"]
    biz_start = BUSINESS_HOURS["start"]
    biz_end = BUSINESS_HOURS["end"]

    if peak_start <= hour < peak_end:
        base_delay = random.uniform(180, 900)    # 3-15 minutes
    elif biz_start <= hour < biz_end:
        base_delay = random.uniform(600, 1800)   # 10-30 minutes
    else:
        base_delay = random.uniform(2700, 7200)  # 45-120 minutes

    return max(30.0, base_delay / max(multiplier, 0.1))
```

- [ ] **Step 2.4: Run tests to verify they pass**

```bash
python tests/test_simulation.py -v -k "Variation"
```

Expected: 9 tests, all PASS.

- [ ] **Step 2.5: Commit**

```bash
git add simulation/variation.py tests/test_simulation.py
git commit -m "Add simulation/variation.py with daily multiplier and timing functions (Step 2)"
```

---

## Task 3: SimulationEngine skeleton — init, register, GeneratorCall

**Files:**
- Create: `simulation/engine.py`
- Modify: `tests/test_simulation.py` (add `TestSimulationEngineInit`)

- [ ] **Step 3.1: Write failing tests for engine init**

Append to `tests/test_simulation.py`:

```python
class TestSimulationEngineInit(unittest.TestCase):

    def test_engine_initializes_with_defaults(self):
        from simulation.engine import SimulationEngine
        with patch("signal.signal"):  # don't register real signal handlers in tests
            engine = SimulationEngine()
        self.assertFalse(engine.dry_run)
        self.assertEqual(engine.speed, 1.0)
        self.assertIsNone(engine.target_date)
        self.assertTrue(engine.running)
        self.assertEqual(engine.event_count, 0)
        self.assertEqual(engine.error_count, 0)

    def test_engine_seeds_rng_when_date_provided(self):
        from simulation.engine import SimulationEngine
        with patch("signal.signal"):
            engine = SimulationEngine(target_date=date(2026, 3, 27))
        # Verify current_date is set to target_date
        self.assertEqual(engine.current_date, date(2026, 3, 27))
        # Two engines with same date should produce same random values
        with patch("signal.signal"):
            e1 = SimulationEngine(target_date=date(2026, 3, 27))
            r1 = random.random()
            e2 = SimulationEngine(target_date=date(2026, 3, 27))
            r2 = random.random()
        self.assertEqual(r1, r2)

    def test_engine_skips_checkpoint_when_date_provided(self):
        from simulation.engine import SimulationEngine
        with patch("signal.signal"):
            with patch.object(SimulationEngine, "load_checkpoint") as mock_load:
                SimulationEngine(target_date=date(2026, 3, 27))
        mock_load.assert_not_called()

    def test_engine_loads_checkpoint_in_continuous_mode(self):
        from simulation.engine import SimulationEngine
        with patch("signal.signal"):
            with patch.object(SimulationEngine, "load_checkpoint") as mock_load:
                mock_load.return_value = None
                SimulationEngine()
        mock_load.assert_called_once()

    def test_register_adds_generator(self):
        from simulation.engine import SimulationEngine
        with patch("signal.signal"):
            engine = SimulationEngine()
        mock_gen = MagicMock()
        engine.register("test_gen", mock_gen)
        self.assertIn("test_gen", engine._generators)
        self.assertIs(engine._generators["test_gen"], mock_gen)

    def test_engine_runs_without_generators(self):
        """Zero registered generators should warn but not raise."""
        from simulation.engine import SimulationEngine
        with patch("signal.signal"):
            # _register_generators will find nothing (no generator modules exist yet)
            engine = SimulationEngine()
        # Engine should be in a valid state
        self.assertTrue(engine.running)
        self.assertEqual(engine._generators, {})

    def test_generator_call_namedtuple(self):
        from simulation.engine import GeneratorCall
        gc = GeneratorCall(generator_name="contacts", kwargs={"foo": "bar"})
        self.assertEqual(gc.generator_name, "contacts")
        self.assertEqual(gc.kwargs, {"foo": "bar"})
```

- [ ] **Step 3.2: Run tests to verify they fail**

```bash
python tests/test_simulation.py -v -k "EngineInit"
```

Expected: `ModuleNotFoundError: No module named 'simulation.engine'`

- [ ] **Step 3.3: Create simulation/engine.py with init skeleton**

```python
"""
simulation/engine.py

Core event loop for the Sparkle & Shine simulation engine.

CLI:
    python -m simulation.engine
    python -m simulation.engine --dry-run
    python -m simulation.engine --speed 10 --once --date 2026-03-27
    python -m simulation.engine --verbose
"""

import argparse
import json
import logging
import random
import signal
import sys
import time
from collections import defaultdict, namedtuple
from datetime import date, datetime, timedelta
from pathlib import Path

from intelligence.logging_config import setup_logging
from simulation.config import DAILY_VOLUMES
from simulation.variation import get_adjusted_volume, get_next_event_delay, should_event_happen

logger = setup_logging(__name__)

GeneratorCall = namedtuple("GeneratorCall", ["generator_name", "kwargs"])

CHECKPOINT_FILE = Path("simulation/checkpoint.json")


class SimulationEngine:
    """Real-time simulation engine for Sparkle & Shine business events.

    Runs as a continuous process (run()) or for a single day (run_once()).
    Generators are registered via _register_generators() with conditional
    imports so the engine runs cleanly before any generator modules exist.

    Args:
        dry_run: If True, skip all API calls, SQLite writes, and checkpoint saves.
        speed: Time multiplier for event delays (2.0 = twice as fast).
        target_date: Simulate a specific date; seeds RNG and ignores checkpoint.
        verbose: Enable DEBUG logging.
        db_path: Path to sparkle_shine.db (used by reconciliation hook).
    """

    def __init__(
        self,
        dry_run: bool = False,
        speed: float = 1.0,
        target_date: date | None = None,
        verbose: bool = False,
        db_path: str = "sparkle_shine.db",
    ):
        self.dry_run = dry_run
        self.speed = speed
        self.target_date = target_date
        self.verbose = verbose
        self.db_path = db_path
        self.running = True
        self.counters: dict[str, int] = defaultdict(int)
        self.event_count: int = 0
        self.error_count: int = 0
        self._generators: dict = {}

        # --date wins: seed RNG and skip checkpoint (L7)
        if target_date is not None:
            self.current_date = target_date
            random.seed(hash(str(target_date)))
        else:
            self.current_date = date.today()
            self.load_checkpoint()

        self._register_generators()
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        signal.signal(signal.SIGINT, self.handle_shutdown)

    def register(self, name: str, generator) -> None:
        """Register a generator instance under the given event name."""
        self._generators[name] = generator

    def _register_generators(self) -> None:
        """Attempt to import and register each generator module.

        Uses conditional imports so the engine runs cleanly when generator
        modules don't exist yet. Add new generators here as they are built.
        """
        # ContactGenerator — simulation/generators/contacts.py
        try:
            from simulation.generators.contacts import ContactGenerator
            self.register("contacts", ContactGenerator())
        except ImportError:
            logger.warning("ContactGenerator not found — skipping")

        # DealGenerator — simulation/generators/deals.py
        try:
            from simulation.generators.deals import DealGenerator
            self.register("deals", DealGenerator())
        except ImportError:
            logger.warning("DealGenerator not found — skipping")

        # ChurnGenerator — simulation/generators/churn.py
        try:
            from simulation.generators.churn import ChurnGenerator
            self.register("churn", ChurnGenerator())
        except ImportError:
            logger.warning("ChurnGenerator not found — skipping")

        if not self._generators:
            logger.warning(
                "No generators registered. Engine will produce 0 events. "
                "Build generator modules and add them to _register_generators()."
            )
```

- [ ] **Step 3.4: Run tests to verify they pass**

```bash
python tests/test_simulation.py -v -k "EngineInit"
```

Expected: 7 tests, all PASS.

- [ ] **Step 3.5: Commit**

```bash
git add simulation/engine.py tests/test_simulation.py
git commit -m "Add SimulationEngine skeleton with init, register, and GeneratorCall (Step 3)"
```

---

## Task 4: plan_day() and pick_next_generator()

**Files:**
- Modify: `simulation/engine.py` (add `plan_day`, `pick_next_generator`)
- Modify: `tests/test_simulation.py` (add `TestPlanDay`)

- [ ] **Step 4.1: Write failing tests for plan_day**

Append to `tests/test_simulation.py`:

```python
class TestPlanDay(unittest.TestCase):

    def _make_engine(self, target_date=None):
        with patch("signal.signal"):
            return SimulationEngine(
                dry_run=True,
                target_date=target_date or date(2026, 3, 27),
            )

    def setUp(self):
        from simulation.engine import SimulationEngine as SE
        globals()["SimulationEngine"] = SE

    def test_plan_day_returns_list(self):
        engine = self._make_engine()
        plan = engine.plan_day(date(2026, 3, 27))
        self.assertIsInstance(plan, list)

    def test_plan_day_returns_generator_calls(self):
        from simulation.engine import GeneratorCall
        engine = self._make_engine()
        plan = engine.plan_day(date(2026, 3, 27))
        for item in plan:
            self.assertIsInstance(item, GeneratorCall)
            self.assertIsInstance(item.generator_name, str)
            self.assertIsInstance(item.kwargs, dict)

    def test_plan_day_is_shuffled(self):
        """Same-date plan run twice with different RNG state should differ."""
        engine = self._make_engine(target_date=date(2026, 3, 27))
        random.seed(1)
        plan_a = [gc.generator_name for gc in engine.plan_day(date(2026, 3, 27))]
        random.seed(999)
        plan_b = [gc.generator_name for gc in engine.plan_day(date(2026, 3, 27))]
        # With enough events, the two orderings should differ
        if len(plan_a) > 3 and len(plan_b) > 3:
            self.assertNotEqual(plan_a, plan_b)

    def test_plan_day_deterministic_with_seeded_date(self):
        """Same date + same seed → same plan."""
        engine = self._make_engine(target_date=date(2026, 3, 27))
        # Re-seed to identical state
        random.seed(hash(str(date(2026, 3, 27))))
        plan_a = [gc.generator_name for gc in engine.plan_day(date(2026, 3, 27))]
        random.seed(hash(str(date(2026, 3, 27))))
        plan_b = [gc.generator_name for gc in engine.plan_day(date(2026, 3, 27))]
        self.assertEqual(plan_a, plan_b)

    def test_plan_day_weekend_produces_fewer_contacts(self):
        """Sunday should produce fewer contact events than Monday on average."""
        engine = self._make_engine()
        monday_counts = []
        sunday_counts = []
        for seed in range(20):
            random.seed(seed)
            monday_counts.append(
                sum(1 for gc in engine.plan_day(date(2026, 3, 30))  # Monday
                    if gc.generator_name == "contacts")
            )
            random.seed(seed)
            sunday_counts.append(
                sum(1 for gc in engine.plan_day(date(2026, 3, 29))  # Sunday
                    if gc.generator_name == "contacts")
            )
        avg_monday = sum(monday_counts) / len(monday_counts)
        avg_sunday = sum(sunday_counts) / len(sunday_counts)
        self.assertGreater(avg_monday, avg_sunday)

    def test_pick_next_generator_pops_first(self):
        from simulation.engine import GeneratorCall
        engine = self._make_engine()
        plan = [
            GeneratorCall("contacts", {}),
            GeneratorCall("deals", {}),
            GeneratorCall("churn", {"client_type": "residential"}),
        ]
        first = engine.pick_next_generator(plan)
        self.assertEqual(first.generator_name, "contacts")
        self.assertEqual(len(plan), 2)

    def test_pick_next_generator_returns_none_on_empty(self):
        engine = self._make_engine()
        self.assertIsNone(engine.pick_next_generator([]))
```

- [ ] **Step 4.2: Run tests to verify they fail**

```bash
python tests/test_simulation.py -v -k "PlanDay"
```

Expected: `AttributeError: 'SimulationEngine' object has no attribute 'plan_day'`

- [ ] **Step 4.3: Add plan_day() and pick_next_generator() to SimulationEngine**

Add these two methods to the `SimulationEngine` class in `simulation/engine.py`:

```python
    def plan_day(self, target_date: date) -> list:
        """Build a shuffled list of GeneratorCall events for the day.

        Uses DAILY_VOLUMES and get_adjusted_volume() to determine event counts.
        The list is shuffled so events are interleaved across the day — contacts,
        deal progressions, and churn checks distributed randomly rather than
        batched by category.

        Args:
            target_date: The date being simulated (drives seasonal/day-of-week scaling).

        Returns:
            Shuffled list of GeneratorCall namedtuples.
        """
        plan = []
        vol = DAILY_VOLUMES

        # ── New contacts (individual events) ────────────────────────────────
        n_contacts = get_adjusted_volume(
            vol["new_contacts"]["base_min"],
            vol["new_contacts"]["base_max"],
            target_date,
        )
        for _ in range(n_contacts):
            plan.append(GeneratorCall("contacts", {}))

        # ── Deal progressions ────────────────────────────────────────────────
        # Estimate active pipeline: ~30 deals at any given time.
        # Derived from: sqls/month (~42) × avg cycle (~33 days / 30 days) ≈ 46;
        # using conservative 30 to avoid over-generating events on slow days.
        # The deals generator queries the database for actual open deals.
        deal_config = vol["deal_progression"]
        for _ in range(30):
            if should_event_happen(deal_config["stage_advance_probability"], target_date):
                plan.append(GeneratorCall("deals", {}))

        # ── Residential churn checks ─────────────────────────────────────────
        # ~180 active residential clients per CLAUDE.md data volumes.
        # Convert monthly rate to per-business-day rate (÷22).
        daily_res_churn = vol["churn"]["monthly_residential_churn_rate"] / 22
        for _ in range(180):
            if should_event_happen(daily_res_churn, target_date):
                plan.append(GeneratorCall("churn", {"client_type": "residential"}))

        # ── Commercial churn checks ──────────────────────────────────────────
        # ~9 active commercial clients per CLAUDE.md data volumes.
        daily_com_churn = vol["churn"]["monthly_commercial_churn_rate"] / 22
        for _ in range(9):
            if should_event_happen(daily_com_churn, target_date):
                plan.append(GeneratorCall("churn", {"client_type": "commercial"}))

        random.shuffle(plan)
        return plan

    def pick_next_generator(self, plan: list) -> "GeneratorCall | None":
        """Pop and return the next GeneratorCall from the plan.

        Returns None when the plan is exhausted.
        """
        if not plan:
            return None
        return plan.pop(0)
```

- [ ] **Step 4.4: Run tests to verify they pass**

```bash
python tests/test_simulation.py -v -k "PlanDay"
```

Expected: 7 tests, all PASS.

- [ ] **Step 4.5: Commit**

```bash
git add simulation/engine.py tests/test_simulation.py
git commit -m "Add plan_day() and pick_next_generator() to SimulationEngine (Step 4)"
```

---

## Task 5: dispatch() with error handling

**Files:**
- Modify: `simulation/engine.py` (add `dispatch`)
- Modify: `tests/test_simulation.py` (add `TestDispatch`)

- [ ] **Step 5.1: Write failing tests for dispatch**

Append to `tests/test_simulation.py`:

```python
class TestDispatch(unittest.TestCase):

    def _make_engine(self):
        with patch("signal.signal"):
            return SimulationEngine(dry_run=True, target_date=date(2026, 3, 27))

    def setUp(self):
        from simulation.engine import SimulationEngine as SE, GeneratorCall as GC
        globals()["SimulationEngine"] = SE
        globals()["GeneratorCall"] = GC

    def test_dispatch_calls_registered_generator(self):
        engine = self._make_engine()
        mock_gen = MagicMock()
        engine.register("contacts", mock_gen)
        engine.dispatch(GeneratorCall("contacts", {"foo": "bar"}))
        mock_gen.execute.assert_called_once_with(dry_run=True, foo="bar")

    def test_dispatch_increments_counter_on_success(self):
        engine = self._make_engine()
        mock_gen = MagicMock()
        engine.register("contacts", mock_gen)
        engine.dispatch(GeneratorCall("contacts", {}))
        self.assertEqual(engine.counters["contacts"], 1)
        self.assertEqual(engine.event_count, 1)
        self.assertEqual(engine.error_count, 0)

    def test_dispatch_increments_event_count_on_failure(self):
        engine = self._make_engine()
        mock_gen = MagicMock()
        mock_gen.execute.side_effect = RuntimeError("generator exploded")
        engine.register("contacts", mock_gen)
        engine.dispatch(GeneratorCall("contacts", {}))
        # event_count increments on attempt; counters["contacts"] does NOT increment
        self.assertEqual(engine.event_count, 1)
        self.assertEqual(engine.error_count, 1)
        self.assertEqual(engine.counters["contacts"], 0)

    def test_dispatch_does_not_crash_on_generator_exception(self):
        engine = self._make_engine()
        mock_gen = MagicMock()
        mock_gen.execute.side_effect = ValueError("bad data")
        engine.register("contacts", mock_gen)
        # Should not raise
        try:
            engine.dispatch(GeneratorCall("contacts", {}))
        except Exception:
            self.fail("dispatch() raised an exception on generator failure")

    def test_dispatch_warns_on_unknown_generator(self):
        engine = self._make_engine()
        with self.assertLogs("simulation.engine", level="WARNING") as cm:
            engine.dispatch(GeneratorCall("unknown_generator", {}))
        self.assertTrue(any("unknown_generator" in line for line in cm.output))

    def test_dispatch_skips_checkpoint_in_dry_run(self):
        engine = self._make_engine()  # dry_run=True
        mock_gen = MagicMock()
        engine.register("contacts", mock_gen)
        with patch.object(engine, "save_checkpoint") as mock_save:
            for _ in range(15):  # trigger the every-10 checkpoint logic
                engine.dispatch(GeneratorCall("contacts", {}))
        mock_save.assert_not_called()

    def test_dispatch_saves_checkpoint_every_10_events(self):
        with patch("signal.signal"):
            engine = SimulationEngine(dry_run=False, target_date=date(2026, 3, 27))
        mock_gen = MagicMock()
        engine.register("contacts", mock_gen)
        with patch.object(engine, "save_checkpoint") as mock_save:
            for _ in range(25):
                engine.dispatch(GeneratorCall("contacts", {}))
        # Should have been called at event 10 and event 20
        self.assertEqual(mock_save.call_count, 2)
```

- [ ] **Step 5.2: Run tests to verify they fail**

```bash
python tests/test_simulation.py -v -k "Dispatch"
```

Expected: `AttributeError: 'SimulationEngine' object has no attribute 'dispatch'`

- [ ] **Step 5.3: Add dispatch() to SimulationEngine**

Add this method to the `SimulationEngine` class:

```python
    def dispatch(self, generator_call: "GeneratorCall") -> None:
        """Execute one generator call, handling errors without crashing the engine.

        Tracking:
            event_count  — increments on every attempt (success or failure)
            counters[name] — increments only on success
            error_count  — increments on failure

        Checkpoint saved every 10 events (skipped in dry_run mode, per L10/L15).
        """
        name = generator_call.generator_name
        generator = self._generators.get(name)
        if not generator:
            logger.warning(f"No generator registered for '{name}', skipping")
            return

        try:
            generator.execute(dry_run=self.dry_run, **generator_call.kwargs)
            self.counters[name] += 1
        except Exception as e:
            self.error_count += 1
            logger.exception(f"{name} generator failed: {e}")
            # Report to Slack #automation-failure — but never let a broken reporter
            # crash the engine or mask the original error.
            try:
                from simulation.error_reporter import report_error
                report_error(
                    e,
                    tool_name=name,
                    context=f"running {name} generator",
                    dry_run=self.dry_run,
                )
            except Exception:
                pass  # original error already logged above

        self.event_count += 1
        if not self.dry_run and self.event_count % 10 == 0:
            self.save_checkpoint()
```

- [ ] **Step 5.4: Run tests to verify they pass**

```bash
python tests/test_simulation.py -v -k "Dispatch"
```

Expected: 7 tests, all PASS.

- [ ] **Step 5.5: Commit**

```bash
git add simulation/engine.py tests/test_simulation.py
git commit -m "Add dispatch() with error isolation and checkpoint trigger (Step 5)"
```

---

## Task 6: save_checkpoint() and load_checkpoint()

**Files:**
- Modify: `simulation/engine.py` (add `save_checkpoint`, `load_checkpoint`, `log_daily_summary`)
- Modify: `tests/test_simulation.py` (add `TestCheckpoint`)

- [ ] **Step 6.1: Write failing tests for checkpoint**

Append to `tests/test_simulation.py`:

```python
class TestCheckpoint(unittest.TestCase):

    def setUp(self):
        from simulation.engine import SimulationEngine as SE
        globals()["SimulationEngine"] = SE
        # Use a temp file so tests never touch the real checkpoint
        self._tmp = tempfile.NamedTemporaryFile(suffix=".json", delete=False)
        self._tmp.close()
        self._checkpoint_path = Path(self._tmp.name)

    def tearDown(self):
        if self._checkpoint_path.exists():
            self._checkpoint_path.unlink()

    def _make_engine(self, dry_run=False):
        with patch("signal.signal"):
            engine = SimulationEngine(dry_run=dry_run, target_date=date(2026, 3, 27))
        engine._checkpoint_file = self._checkpoint_path
        return engine

    def test_save_and_load_roundtrip(self):
        engine = self._make_engine()
        engine.counters["contacts"] = 5
        engine.counters["deals"] = 3
        engine.event_count = 8
        engine.error_count = 1
        engine.current_date = date(2026, 3, 27)
        engine.save_checkpoint()

        # Load into a fresh engine
        engine2 = self._make_engine()
        engine2.counters = defaultdict(int)
        engine2.event_count = 0
        engine2._checkpoint_file = self._checkpoint_path
        engine2.load_checkpoint()

        self.assertEqual(engine2.counters["contacts"], 5)
        self.assertEqual(engine2.counters["deals"], 3)
        self.assertEqual(engine2.event_count, 8)
        self.assertEqual(engine2.error_count, 1)
        self.assertEqual(engine2.current_date, date(2026, 3, 27))

    def test_save_checkpoint_skipped_in_dry_run(self):
        engine = self._make_engine(dry_run=True)
        engine.save_checkpoint()
        self.assertFalse(self._checkpoint_path.exists())

    def test_load_checkpoint_returns_none_when_missing(self):
        engine = self._make_engine()
        engine._checkpoint_file = Path("/tmp/nonexistent_checkpoint_xyz.json")
        result = engine.load_checkpoint()
        self.assertIsNone(result)

    def test_checkpoint_file_contains_valid_json(self):
        engine = self._make_engine()
        engine.counters["contacts"] = 2
        engine.event_count = 2
        engine.current_date = date(2026, 3, 27)
        engine.save_checkpoint()
        data = json.loads(self._checkpoint_path.read_text())
        self.assertIn("date", data)
        self.assertIn("counters", data)
        self.assertIn("event_count", data)
        self.assertIn("last_event_time", data)
        self.assertEqual(data["date"], "2026-03-27")

    def test_log_daily_summary_includes_error_count(self):
        engine = self._make_engine()
        engine.counters["contacts"] = 5
        engine.event_count = 6
        engine.error_count = 1
        engine.current_date = date(2026, 3, 27)
        with self.assertLogs("simulation.engine", level="INFO") as cm:
            engine.log_daily_summary()
        log_line = " ".join(cm.output)
        self.assertIn("1 error", log_line)
        self.assertIn("contacts=5", log_line)
        self.assertIn("2026-03-27", log_line)
```

- [ ] **Step 6.2: Run tests to verify they fail**

```bash
python tests/test_simulation.py -v -k "Checkpoint"
```

Expected: `AttributeError: 'SimulationEngine' object has no attribute 'save_checkpoint'`

- [ ] **Step 6.3: Add save_checkpoint, load_checkpoint, log_daily_summary to SimulationEngine**

Add these three methods to the `SimulationEngine` class:

```python
    def save_checkpoint(self) -> None:
        """Write engine state to simulation/checkpoint.json.

        Skipped entirely when dry_run=True. A dry-run checkpoint would cause
        the next real run to skip the day by loading partial state.
        """
        if self.dry_run:
            return
        state = {
            "date": self.current_date.isoformat(),
            "counters": dict(self.counters),
            "last_event_time": datetime.utcnow().isoformat(),
            "event_count": self.event_count,
            "error_count": self.error_count,
        }
        checkpoint_file = getattr(self, "_checkpoint_file", CHECKPOINT_FILE)
        checkpoint_file.write_text(json.dumps(state, indent=2))
        logger.debug(f"Checkpoint saved: {self.event_count} events on {self.current_date}")

    def load_checkpoint(self) -> dict | None:
        """Read engine state from simulation/checkpoint.json if it exists.

        Returns the raw state dict, or None if no checkpoint file is present.
        Restores counters, event_count, error_count, and current_date.
        """
        checkpoint_file = getattr(self, "_checkpoint_file", CHECKPOINT_FILE)
        if not checkpoint_file.exists():
            return None
        state = json.loads(checkpoint_file.read_text())
        self.current_date = date.fromisoformat(state["date"])
        self.counters = defaultdict(int, state.get("counters", {}))
        self.event_count = state.get("event_count", 0)
        self.error_count = state.get("error_count", 0)
        logger.info(
            f"Resumed from checkpoint: {self.current_date}, "
            f"{self.event_count} events already processed"
        )
        return state

    def log_daily_summary(self) -> None:
        """Log a one-line summary of the day's event counts.

        Format: Daily summary YYYY-MM-DD: N events (E errors): gen1=X, gen2=Y, ...
        """
        error_label = f"{self.error_count} error{'s' if self.error_count != 1 else ''}"
        counts = ", ".join(
            f"{k}={v}" for k, v in sorted(self.counters.items())
        ) or "none"
        logger.info(
            f"Daily summary {self.current_date}: "
            f"{self.event_count} events ({error_label}): {counts}"
        )
```

- [ ] **Step 6.4: Run tests to verify they pass**

```bash
python tests/test_simulation.py -v -k "Checkpoint"
```

Expected: 5 tests, all PASS.

- [ ] **Step 6.5: Commit**

```bash
git add simulation/engine.py tests/test_simulation.py
git commit -m "Add save/load checkpoint and log_daily_summary to SimulationEngine (Step 6)"
```

---

## Task 7: run_once()

**Files:**
- Modify: `simulation/engine.py` (add `run_once`)
- Modify: `tests/test_simulation.py` (add `TestRunOnce`)

- [ ] **Step 7.1: Write failing tests for run_once**

Append to `tests/test_simulation.py`:

```python
class TestRunOnce(unittest.TestCase):

    def setUp(self):
        from simulation.engine import SimulationEngine as SE, GeneratorCall as GC
        globals()["SimulationEngine"] = SE
        globals()["GeneratorCall"] = GC

    def _make_engine_with_mock_gen(self):
        with patch("signal.signal"):
            engine = SimulationEngine(dry_run=True, target_date=date(2026, 3, 27))
        mock_gen = MagicMock()
        engine.register("contacts", mock_gen)
        return engine, mock_gen

    def test_run_once_returns_dict(self):
        engine, _ = self._make_engine_with_mock_gen()
        with patch("time.sleep"):
            result = engine.run_once(date(2026, 3, 27))
        self.assertIsInstance(result, dict)

    def test_run_once_dispatches_events(self):
        engine, mock_gen = self._make_engine_with_mock_gen()
        with patch("time.sleep"):
            engine.run_once(date(2026, 3, 27))
        self.assertGreater(mock_gen.execute.call_count, 0)

    def test_run_once_sets_current_date(self):
        engine, _ = self._make_engine_with_mock_gen()
        target = date(2026, 4, 15)
        with patch("time.sleep"):
            engine.run_once(target)
        self.assertEqual(engine.current_date, target)

    def test_run_once_stops_when_running_is_false(self):
        engine, mock_gen = self._make_engine_with_mock_gen()

        call_count = 0
        def slow_execute(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                engine.running = False

        mock_gen.execute.side_effect = slow_execute

        with patch("time.sleep"):
            engine.run_once(date(2026, 3, 27))

        # Engine stopped early — should not have processed all events
        self.assertFalse(engine.running)
        self.assertLess(engine.event_count, 50)

    def test_run_once_returns_partial_counts_on_early_stop(self):
        engine, mock_gen = self._make_engine_with_mock_gen()
        engine.running = False  # stop immediately
        with patch("time.sleep"):
            result = engine.run_once(date(2026, 3, 27))
        # No events dispatched since running was False from the start
        self.assertEqual(engine.event_count, 0)

    def test_run_once_sleeps_between_events(self):
        engine, mock_gen = self._make_engine_with_mock_gen()
        with patch("time.sleep") as mock_sleep:
            engine.run_once(date(2026, 3, 27))
        # sleep should be called once per event dispatched
        self.assertEqual(mock_sleep.call_count, mock_gen.execute.call_count)
```

- [ ] **Step 7.2: Run tests to verify they fail**

```bash
python tests/test_simulation.py -v -k "RunOnce"
```

Expected: `AttributeError: 'SimulationEngine' object has no attribute 'run_once'`

- [ ] **Step 7.3: Add run_once() to SimulationEngine**

Add this method to the `SimulationEngine` class:

```python
    def run_once(self, target_date: date) -> dict:
        """Run exactly one full simulated day.

        Builds a shuffled plan of events, dispatches each with a timing delay,
        then logs the daily summary. Stops early if self.running becomes False
        (set by handle_shutdown on SIGTERM/SIGINT).

        Used by --once CLI flag and directly in tests.

        Args:
            target_date: The date to simulate.

        Returns:
            Dict of successful event counts by generator name (partial if interrupted).
        """
        self.current_date = target_date
        plan = self.plan_day(target_date)

        while plan and self.running:
            if not self.running:
                break
            generator_call = self.pick_next_generator(plan)
            delay = get_next_event_delay(target_date) / max(self.speed, 0.001)
            time.sleep(delay)
            self.dispatch(generator_call)

        self.log_daily_summary()
        return dict(self.counters)
```

- [ ] **Step 7.4: Run tests to verify they pass**

```bash
python tests/test_simulation.py -v -k "RunOnce"
```

Expected: 6 tests, all PASS.

- [ ] **Step 7.5: Commit**

```bash
git add simulation/engine.py tests/test_simulation.py
git commit -m "Add run_once() to SimulationEngine (Step 7)"
```

---

## Task 8: handle_shutdown() and signal handling

**Files:**
- Modify: `simulation/engine.py` (add `handle_shutdown`)
- Modify: `tests/test_simulation.py` (add `TestShutdown`)

- [ ] **Step 8.1: Write failing tests for handle_shutdown**

Append to `tests/test_simulation.py`:

```python
class TestShutdown(unittest.TestCase):

    def setUp(self):
        from simulation.engine import SimulationEngine as SE
        globals()["SimulationEngine"] = SE

    def _make_engine(self, dry_run=False):
        with patch("signal.signal"):
            return SimulationEngine(dry_run=dry_run, target_date=date(2026, 3, 27))

    def test_handle_shutdown_sets_running_false(self):
        engine = self._make_engine()
        self.assertTrue(engine.running)
        engine.handle_shutdown(signal.SIGTERM, None)
        self.assertFalse(engine.running)

    def test_handle_shutdown_saves_checkpoint_when_not_dry_run(self):
        engine = self._make_engine(dry_run=False)
        with patch.object(engine, "save_checkpoint") as mock_save:
            with patch.object(engine, "log_daily_summary"):
                engine.handle_shutdown(signal.SIGTERM, None)
        mock_save.assert_called_once()

    def test_handle_shutdown_skips_checkpoint_in_dry_run(self):
        engine = self._make_engine(dry_run=True)
        with patch.object(engine, "save_checkpoint") as mock_save:
            with patch.object(engine, "log_daily_summary"):
                engine.handle_shutdown(signal.SIGTERM, None)
        mock_save.assert_not_called()

    def test_handle_shutdown_logs_daily_summary(self):
        engine = self._make_engine()
        with patch.object(engine, "save_checkpoint"):
            with patch.object(engine, "log_daily_summary") as mock_summary:
                engine.handle_shutdown(signal.SIGINT, None)
        mock_summary.assert_called_once()

    def test_handle_shutdown_does_not_call_sys_exit(self):
        engine = self._make_engine()
        with patch.object(engine, "save_checkpoint"):
            with patch.object(engine, "log_daily_summary"):
                with patch("sys.exit") as mock_exit:
                    engine.handle_shutdown(signal.SIGTERM, None)
        mock_exit.assert_not_called()
```

- [ ] **Step 8.2: Run tests to verify they fail**

```bash
python tests/test_simulation.py -v -k "Shutdown"
```

Expected: `AttributeError: 'SimulationEngine' object has no attribute 'handle_shutdown'`

- [ ] **Step 8.3: Add handle_shutdown() to SimulationEngine**

Add this method to the `SimulationEngine` class:

```python
    def handle_shutdown(self, signum: int, frame) -> None:
        """Handle SIGTERM or SIGINT by setting self.running = False.

        Does NOT call sys.exit(). The main loop checks self.running and
        exits naturally, allowing the __main__ block to call sys.exit(0)
        at the top level. This avoids messy state from exiting mid-sleep
        or mid-API-call.
        """
        logger.info(f"Shutdown signal received (signal {signum}). Stopping after current event.")
        self.running = False
        if not self.dry_run:
            self.save_checkpoint()
        self.log_daily_summary()
```

- [ ] **Step 8.4: Run tests to verify they pass**

```bash
python tests/test_simulation.py -v -k "Shutdown"
```

Expected: 5 tests, all PASS.

- [ ] **Step 8.5: Commit**

```bash
git add simulation/engine.py tests/test_simulation.py
git commit -m "Add handle_shutdown() with self.running flag pattern (Step 8)"
```

---

## Task 9: run() with reconciliation hook and midnight sleep

**Files:**
- Modify: `simulation/engine.py` (add `run`)
- Modify: `tests/test_simulation.py` (add `TestRun`)

- [ ] **Step 9.1: Write failing tests for run()**

Append to `tests/test_simulation.py`:

```python
class TestRun(unittest.TestCase):

    def setUp(self):
        from simulation.engine import SimulationEngine as SE
        globals()["SimulationEngine"] = SE

    def test_run_calls_run_once_then_stops(self):
        """run() should call run_once() and stop when running is set to False."""
        with patch("signal.signal"):
            engine = SimulationEngine(dry_run=True, target_date=date(2026, 3, 27))

        call_count = 0
        def fake_run_once(d):
            nonlocal call_count
            call_count += 1
            engine.running = False  # stop after first iteration
            return {}

        with patch.object(engine, "run_once", side_effect=fake_run_once):
            with patch("time.sleep"):
                engine.run()

        self.assertEqual(call_count, 1)

    def test_run_does_not_crash_when_reconciler_missing(self):
        """ImportError from missing reconciler module must not crash run()."""
        with patch("signal.signal"):
            engine = SimulationEngine(dry_run=True, target_date=date(2026, 3, 27))

        def fake_run_once(d):
            engine.running = False
            return {}

        with patch.object(engine, "run_once", side_effect=fake_run_once):
            with patch("time.sleep"):
                # Should complete without raising even with no reconciler
                try:
                    engine.run()
                except Exception:
                    self.fail("run() crashed when reconciler module was missing")

    def test_run_does_not_crash_when_reconciler_raises(self):
        """Exception from reconciler.daily_sweep() must not crash run()."""
        with patch("signal.signal"):
            engine = SimulationEngine(dry_run=True, target_date=date(2026, 3, 27))

        def fake_run_once(d):
            engine.running = False
            return {}

        mock_reconciler = MagicMock()
        mock_reconciler.return_value.daily_sweep.side_effect = RuntimeError("sweep failed")

        with patch.object(engine, "run_once", side_effect=fake_run_once):
            with patch("time.sleep"):
                with patch.dict("sys.modules", {
                    "simulation.reconciliation": MagicMock(),
                    "simulation.reconciliation.reconciler": MagicMock(Reconciler=mock_reconciler),
                }):
                    try:
                        engine.run()
                    except Exception:
                        self.fail("run() crashed on reconciler exception")
```

- [ ] **Step 9.2: Run tests to verify they fail**

```bash
python tests/test_simulation.py -v -k "TestRun"
```

Expected: `AttributeError: 'SimulationEngine' object has no attribute 'run'`

- [ ] **Step 9.3: Add run() to SimulationEngine**

Add this method to the `SimulationEngine` class:

```python
    def run(self) -> None:
        """Continuous event loop. Runs until SIGTERM/SIGINT sets self.running = False.

        Each iteration: simulate one full day, run the reconciliation hook,
        then sleep until midnight (waking every second to check self.running).
        """
        while self.running:
            today = date.today()
            self.run_once(today)

            # Daily reconciliation sweep — no-op until reconciler is built (Step 7).
            # ImportError guard keeps the engine running before the module exists.
            try:
                from simulation.reconciliation.reconciler import Reconciler
                reconciler = Reconciler(self.db_path)
                reconciler.daily_sweep()
            except ImportError:
                pass
            except Exception as e:
                logger.error(f"Daily reconciliation failed: {e}")

            if not self.running:
                break

            # Sleep until midnight. Check self.running every second so a shutdown
            # signal is not ignored during a long sleep.
            tomorrow_midnight = datetime.combine(
                today + timedelta(days=1), datetime.min.time()
            )
            while self.running:
                remaining = (tomorrow_midnight - datetime.now()).total_seconds()
                if remaining <= 0:
                    break
                time.sleep(min(1.0, remaining))

        logger.info("Engine stopped.")
```

- [ ] **Step 9.4: Run tests to verify they pass**

```bash
python tests/test_simulation.py -v -k "TestRun"
```

Expected: 3 tests, all PASS.

- [ ] **Step 9.5: Commit**

```bash
git add simulation/engine.py tests/test_simulation.py
git commit -m "Add run() with daily reconciliation hook and interruptible midnight sleep (Step 9)"
```

---

## Task 10: CLI __main__ block

**Files:**
- Modify: `simulation/engine.py` (add `__main__` block at module bottom)
- Modify: `tests/test_simulation.py` (add `TestCLI`)

- [ ] **Step 10.1: Write failing tests for the CLI**

Append to `tests/test_simulation.py`:

```python
class TestCLI(unittest.TestCase):
    """Smoke tests for the CLI entry point via subprocess."""

    _ENGINE_MODULE = [
        sys.executable, "-m", "simulation.engine"
    ]
    _CWD = _PROJECT_ROOT

    def test_help_flag_exits_cleanly(self):
        result = subprocess.run(
            self._ENGINE_MODULE + ["--help"],
            capture_output=True, text=True, cwd=self._CWD
        )
        self.assertEqual(result.returncode, 0)
        self.assertIn("--dry-run", result.stdout)
        self.assertIn("--speed", result.stdout)
        self.assertIn("--date", result.stdout)
        self.assertIn("--once", result.stdout)
        self.assertIn("--verbose", result.stdout)

    def test_dry_run_once_exits_cleanly(self):
        """--dry-run --once should run one day and exit with code 0."""
        result = subprocess.run(
            self._ENGINE_MODULE + ["--dry-run", "--once", "--date", "2026-03-27",
                                   "--speed", "999999"],
            capture_output=True, text=True, cwd=self._CWD,
            timeout=15
        )
        self.assertEqual(result.returncode, 0)

    def test_dry_run_once_produces_daily_summary(self):
        """--dry-run --once should log a daily summary line."""
        result = subprocess.run(
            self._ENGINE_MODULE + ["--dry-run", "--once", "--date", "2026-03-27",
                                   "--speed", "999999"],
            capture_output=True, text=True, cwd=self._CWD,
            timeout=15
        )
        combined = result.stdout + result.stderr
        self.assertIn("Daily summary", combined)

    def test_dry_run_does_not_create_checkpoint(self):
        """--dry-run must not write simulation/checkpoint.json."""
        checkpoint = Path(_PROJECT_ROOT) / "simulation" / "checkpoint.json"
        if checkpoint.exists():
            checkpoint.unlink()
        subprocess.run(
            self._ENGINE_MODULE + ["--dry-run", "--once", "--date", "2026-03-01",
                                   "--speed", "999999"],
            capture_output=True, text=True, cwd=self._CWD,
            timeout=15
        )
        self.assertFalse(checkpoint.exists(),
            "dry-run must not create simulation/checkpoint.json")
```

- [ ] **Step 10.2: Run tests to verify they fail**

```bash
python tests/test_simulation.py -v -k "CLI"
```

Expected: Tests fail because `simulation/engine.py` has no `__main__` block yet (no `if __name__ == "__main__":` entry point).

- [ ] **Step 10.3: Add __main__ block to simulation/engine.py**

Append to the bottom of `simulation/engine.py` (after the class definition):

```python
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Sparkle & Shine simulation engine — generates live business events."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Log actions without making API calls, writing to SQLite, or saving checkpoints"
    )
    parser.add_argument(
        "--speed", type=float, default=1.0,
        help="Time multiplier for event delays (2.0 = twice as fast, 0.5 = half speed)"
    )
    parser.add_argument(
        "--date", dest="target_date", default=None, metavar="YYYY-MM-DD",
        help="Simulate a specific date; seeds RNG for reproducibility (L7); ignores checkpoint"
    )
    parser.add_argument(
        "--once", action="store_true",
        help="Run one full day then exit"
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Enable DEBUG logging"
    )
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    target_date = date.fromisoformat(args.target_date) if args.target_date else None

    engine = SimulationEngine(
        dry_run=args.dry_run,
        speed=args.speed,
        target_date=target_date,
        verbose=args.verbose,
    )

    if args.once:
        run_date = target_date if target_date is not None else date.today()
        engine.run_once(run_date)
    else:
        engine.run()

    sys.exit(0)
```

- [ ] **Step 10.4: Run tests to verify they pass**

```bash
python tests/test_simulation.py -v -k "CLI"
```

Expected: 4 tests, all PASS.

- [ ] **Step 10.5: Run the full test suite**

```bash
python tests/test_simulation.py -v
```

Expected: All tests pass. Confirm the count matches what was added across all tasks.

- [ ] **Step 10.6: Commit**

```bash
git add simulation/engine.py tests/test_simulation.py
git commit -m "Add CLI __main__ block to simulation.engine (Step 10)"
```

---

## Task 11: Update intelligence/config.py REVENUE_TARGETS

**Files:**
- Modify: `intelligence/config.py` (replace `REVENUE_TARGETS` dict only)
- Modify: `tests/test_simulation.py` (add `TestRevenueTargets`)

- [ ] **Step 11.1: Write failing tests for new revenue targets**

Append to `tests/test_simulation.py`:

```python
class TestRevenueTargets(unittest.TestCase):

    def test_all_expected_months_present(self):
        from intelligence.config import REVENUE_TARGETS
        expected_months = [
            (2025, 4), (2025, 5), (2025, 6), (2025, 7), (2025, 8),
            (2025, 9), (2025, 10), (2025, 11), (2025, 12),
            (2026, 1), (2026, 2), (2026, 3), (2026, 4), (2026, 5),
            (2026, 6), (2026, 7), (2026, 8), (2026, 9), (2026, 10),
            (2026, 11), (2026, 12),
        ]
        for month in expected_months:
            self.assertIn(month, REVENUE_TARGETS,
                          f"Missing REVENUE_TARGETS key: {month}")

    def test_each_target_is_low_high_tuple(self):
        from intelligence.config import REVENUE_TARGETS
        for key, value in REVENUE_TARGETS.items():
            self.assertIsInstance(value, tuple, f"Expected tuple for {key}")
            self.assertEqual(len(value), 2, f"Expected (low, high) tuple for {key}")
            low, high = value
            self.assertLess(low, high, f"Low must be less than high for {key}")

    def test_ramp_up_targets_are_lower_than_mature(self):
        """Apr 2025 ramp-up target must be below Oct 2025 mature target."""
        from intelligence.config import REVENUE_TARGETS
        ramp_high = REVENUE_TARGETS[(2025, 4)][1]
        mature_low = REVENUE_TARGETS[(2025, 10)][0]
        self.assertLess(ramp_high, mature_low,
                        "Ramp-up upper bound should be below mature lower bound")

    def test_forward_months_cover_simulation_era(self):
        """Simulation-era months (Apr 2026 onward) must have targets."""
        from intelligence.config import REVENUE_TARGETS
        for month in range(4, 13):
            self.assertIn((2026, month), REVENUE_TARGETS)
```

- [ ] **Step 11.2: Run tests to verify they fail**

```bash
python tests/test_simulation.py -v -k "RevenueTargets"
```

Expected: Tests fail — current `REVENUE_TARGETS` has wrong values and is missing forward months.

- [ ] **Step 11.3: Update REVENUE_TARGETS in intelligence/config.py**

Open `intelligence/config.py` and replace the `REVENUE_TARGETS` dict (lines 12-25) with:

```python
# Revenue targets by (year, month) -> (low, high)
# Historical months use actual trajectory from seeded data (ramp-up then maturity).
# Forward months reflect the simulation era with steady seasonal growth.
# Previous targets assumed maturity from Apr 2025 — caused 11/12 months to fail.
REVENUE_TARGETS: dict[tuple[int, int], tuple[int, int]] = {
    # Historical months (seeded data — targets match actual revenue trajectory)
    (2025, 4):  (18000,  30000),    # Ramp-up: minimal commercial, first month
    (2025, 5):  (35000,  55000),    # Ramp-up: growing residential base
    (2025, 6):  (65000,  85000),    # Summer surge starts
    (2025, 7):  (75000,  95000),    # Summer peak
    (2025, 8):  (75000,  95000),    # Post-summer (2 cleaners quit — held steady)
    (2025, 9):  (90000, 110000),    # Approaching maturity
    (2025, 10): (105000, 125000),   # Big commercial win (Barton Creek Medical Group)
    (2025, 11): (110000, 130000),   # Stabilization + referral program
    (2025, 12): (120000, 145000),   # Holiday peak
    (2026, 1):  (115000, 135000),   # January dip
    (2026, 2):  (110000, 130000),   # Recovery
    (2026, 3):  (115000, 135000),   # Recovery / spring pipeline building
    # Forward months (simulation era — steady seasonal growth)
    (2026, 4):  (125000, 150000),
    (2026, 5):  (130000, 155000),
    (2026, 6):  (145000, 175000),   # Summer surge
    (2026, 7):  (150000, 180000),
    (2026, 8):  (140000, 165000),
    (2026, 9):  (125000, 150000),   # Seasonal dip
    (2026, 10): (135000, 160000),
    (2026, 11): (140000, 170000),
    (2026, 12): (155000, 185000),   # Holiday peak
}
```

- [ ] **Step 11.4: Run tests to verify they pass**

```bash
python tests/test_simulation.py -v -k "RevenueTargets"
```

Expected: 4 tests, all PASS.

- [ ] **Step 11.5: Run the full test suite one final time**

```bash
python tests/test_simulation.py -v
```

Expected: All tests pass.

- [ ] **Step 11.6: Verify engine smoke test end-to-end**

```bash
cd /Users/ovieoghor/Documents/Claude\ Code\ Exercises/Simulation\ Exercise/sparkle-shine-poc
python -m simulation.engine --dry-run --once --date 2026-03-27 --speed 999999
```

Expected output includes:
- Warning lines for each missing generator (ContactGenerator, DealGenerator, ChurnGenerator)
- `Daily summary 2026-03-27: 0 events (0 errors): none`
- Exit code 0

- [ ] **Step 11.7: Commit**

```bash
git add intelligence/config.py tests/test_simulation.py
git commit -m "Update REVENUE_TARGETS to match actual ramp-up trajectory; add revenue target tests (Step 11)"
```

---

## Self-Review

**Spec coverage check:**

| Spec requirement | Task |
|---|---|
| `simulation/__init__.py` empty | Task 1 |
| `simulation/config.py` with L1 comments | Task 1 |
| `config_math_trace()` verifies +3 to +5 growth | Task 1 |
| `simulation/variation.py` — 4 functions | Task 2 |
| `SimulationEngine.__init__` with `self.running`, checkpoint/date logic, `_register_generators()`, signal handlers | Task 3 |
| `plan_day()` shuffled, uses `get_adjusted_volume` + `should_event_happen` | Task 4 |
| `pick_next_generator()` simple pop | Task 4 |
| `dispatch()` with `event_count`/`counters`/`error_count` split, error isolation, `report_error`, checkpoint every 10 | Task 5 |
| `save_checkpoint()` / `load_checkpoint()` round-trip, skip on dry_run | Task 6 |
| `log_daily_summary()` format with error count | Task 6 |
| `run_once()` timing delay, `self.running` check, partial return | Task 7 |
| `handle_shutdown()` sets `self.running=False`, no `sys.exit()` | Task 8 |
| `run()` continuous loop with `while self.running`, reconciliation hook, interruptible midnight sleep | Task 9 |
| CLI with all 5 flags | Task 10 |
| `intelligence/config.py` REVENUE_TARGETS update with forward months | Task 11 |

**Placeholder scan:** No TBDs, no "implement later", no vague steps. Every step has exact code or exact commands. ✓

**Type consistency:**
- `GeneratorCall` namedtuple defined in Task 3, used consistently in Tasks 4, 5, 7 with same field names `generator_name` and `kwargs`. ✓
- `plan_day()` returns `list` of `GeneratorCall`; `pick_next_generator(plan)` takes that same list. ✓
- `dispatch(generator_call)` takes `GeneratorCall` — consistent with callers in `run_once()`. ✓
- `engine._checkpoint_file` override used in checkpoint tests matches the `getattr(self, "_checkpoint_file", CHECKPOINT_FILE)` pattern in `save_checkpoint()` / `load_checkpoint()`. ✓
- `engine._generators` dict used in `dispatch()` and set by `register()` — consistent throughout. ✓
