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


if __name__ == "__main__":
    unittest.main()
