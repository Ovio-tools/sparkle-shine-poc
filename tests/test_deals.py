"""
tests/test_deals.py

Unit and integration tests for DealGenerator.
No live API calls. No writes to sparkle_shine.db.

Run:
    python tests/test_deals.py -v
    python tests/test_deals.py -v -k "Probability"
"""
from __future__ import annotations

import os
import random
import sqlite3
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta
from unittest.mock import MagicMock, patch

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


def _bare_gen(db_path=":memory:"):
    """Return a DealGenerator with __init__ bypassed; only db_path set."""
    from simulation.generators.deals import DealGenerator
    gen = object.__new__(DealGenerator)
    gen.db_path = db_path
    return gen


class TestProbabilities(unittest.TestCase):
    """Pure unit tests — no API calls, no SQLite writes.

    datetime.utcnow is pinned to 2026-03-28 12:00:00 so that
    stage_change_time offsets produce exact integer day values.
    """

    def setUp(self):
        self._patcher = patch("simulation.generators.deals.datetime")
        mock_dt = self._patcher.start()
        mock_dt.utcnow.return_value = datetime(2026, 3, 28, 12, 0, 0)
        mock_dt.fromisoformat = datetime.fromisoformat

    def tearDown(self):
        self._patcher.stop()

    def _deal(self, days_ago: int) -> dict:
        """Deal with stage_change_time exactly `days_ago` days before pinned now."""
        sct = (datetime(2026, 3, 28, 12, 0, 0) - timedelta(days=days_ago)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        return {"id": 1, "stage_change_time": sct, "update_time": sct}

    # ── calculate_advance_probability ────────────────────────────────────────

    def test_advance_probability_days_2(self):
        gen = _bare_gen()
        self.assertAlmostEqual(gen.calculate_advance_probability(self._deal(2)), 0.150, places=3)

    def test_advance_probability_days_5(self):
        gen = _bare_gen()
        self.assertAlmostEqual(gen.calculate_advance_probability(self._deal(5)), 0.225, places=3)

    def test_advance_probability_days_10(self):
        gen = _bare_gen()
        self.assertAlmostEqual(gen.calculate_advance_probability(self._deal(10)), 0.300, places=3)

    def test_advance_probability_days_20(self):
        gen = _bare_gen()
        self.assertAlmostEqual(gen.calculate_advance_probability(self._deal(20)), 0.075, places=3)

    # ── calculate_loss_probability ───────────────────────────────────────────

    def test_loss_probability_days_2(self):
        gen = _bare_gen()
        self.assertAlmostEqual(gen.calculate_loss_probability(self._deal(2)), 0.015, places=3)

    def test_loss_probability_days_5(self):
        gen = _bare_gen()
        self.assertAlmostEqual(gen.calculate_loss_probability(self._deal(5)), 0.030, places=3)

    def test_loss_probability_days_10(self):
        gen = _bare_gen()
        self.assertAlmostEqual(gen.calculate_loss_probability(self._deal(10)), 0.045, places=3)

    def test_loss_probability_days_20(self):
        gen = _bare_gen()
        self.assertAlmostEqual(gen.calculate_loss_probability(self._deal(20)), 0.075, places=3)


if __name__ == "__main__":
    unittest.main()
