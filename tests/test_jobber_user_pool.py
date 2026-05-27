"""Unit tests for simulation.jobber_user_pool.UserPool."""
from __future__ import annotations

import logging
import os
import sys
import unittest
from datetime import date, datetime, timedelta

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from simulation.jobber_user_pool import (
    UserPool,
    _overlaps,
    load_user_pool_from_config,
)


class TestOverlapHelper(unittest.TestCase):
    def test_disjoint_intervals_do_not_overlap(self):
        a = datetime(2026, 5, 26, 9, 0)
        b = datetime(2026, 5, 26, 10, 0)
        c = datetime(2026, 5, 26, 10, 0)  # touching at the boundary
        d = datetime(2026, 5, 26, 11, 0)
        self.assertFalse(_overlaps(a, b, c, d))

    def test_partial_overlap_detected(self):
        a = datetime(2026, 5, 26, 9, 0)
        b = datetime(2026, 5, 26, 10, 30)
        c = datetime(2026, 5, 26, 10, 0)
        d = datetime(2026, 5, 26, 11, 0)
        self.assertTrue(_overlaps(a, b, c, d))

    def test_full_containment_detected(self):
        outer_s = datetime(2026, 5, 26, 9, 0)
        outer_e = datetime(2026, 5, 26, 12, 0)
        inner_s = datetime(2026, 5, 26, 10, 0)
        inner_e = datetime(2026, 5, 26, 11, 0)
        self.assertTrue(_overlaps(outer_s, outer_e, inner_s, inner_e))


class TestAssign(unittest.TestCase):
    def setUp(self):
        self.start = datetime(2026, 5, 26, 9, 0)
        self.end = datetime(2026, 5, 26, 11, 0)

    def test_returns_count_users_when_all_free(self):
        pool = UserPool(["u1", "u2", "u3", "u4"])
        picked = pool.assign(self.start, self.end, 2)
        self.assertEqual(len(picked), 2)
        self.assertEqual(set(picked), {"u1", "u2"})

    def test_lru_cursor_advances_across_calls(self):
        pool = UserPool(["u1", "u2", "u3", "u4"])
        first = pool.assign(self.start, self.end, 2)
        # Second call at a non-overlapping time should pick the next two users
        later_start = datetime(2026, 5, 26, 12, 0)
        later_end = datetime(2026, 5, 26, 13, 0)
        second = pool.assign(later_start, later_end, 2)
        self.assertEqual(first, ["u1", "u2"])
        self.assertEqual(second, ["u3", "u4"])

    def test_overlapping_intervals_force_other_users(self):
        pool = UserPool(["u1", "u2", "u3"])
        # First job 9:00-11:00 picks u1
        first = pool.assign(self.start, self.end, 1)
        self.assertEqual(first, ["u1"])
        # Second job overlaps; cursor now at u2 but u1 is busy — still picks u2
        # (next two free in cursor order from u2)
        second = pool.assign(self.start, self.end, 2)
        self.assertEqual(set(second), {"u2", "u3"})

    def test_returns_fewer_when_pool_exhausted_and_warns_once(self):
        pool = UserPool(["u1", "u2"])
        first = pool.assign(self.start, self.end, 2)
        self.assertEqual(set(first), {"u1", "u2"})
        # Overlapping job needs 2 more — none free
        with self.assertLogs("simulation.jobber_user_pool", level="WARNING") as ctx:
            second = pool.assign(self.start, self.end, 2)
            # A second insufficient call on the same day must not double-warn
            third = pool.assign(self.start, self.end, 2)
        self.assertEqual(second, [])
        self.assertEqual(third, [])
        warning_lines = [r for r in ctx.output if "UserPool" in r]
        self.assertEqual(len(warning_lines), 1)

    def test_empty_pool_returns_empty_list(self):
        pool = UserPool([])
        self.assertEqual(pool.assign(self.start, self.end, 3), [])
        self.assertEqual(pool.size, 0)

    def test_zero_count_returns_empty_list(self):
        pool = UserPool(["u1", "u2"])
        self.assertEqual(pool.assign(self.start, self.end, 0), [])

    def test_date_rollover_isolates_busy_map(self):
        pool = UserPool(["u1", "u2"])
        pool.assign(self.start, self.end, 2)
        # Day +1 — u1 and u2 should both be free again
        tomorrow = self.start + timedelta(days=1)
        tomorrow_end = self.end + timedelta(days=1)
        picked = pool.assign(tomorrow, tomorrow_end, 2)
        self.assertEqual(set(picked), {"u1", "u2"})

    def test_clear_day_resets_only_that_day(self):
        pool = UserPool(["u1", "u2"])
        pool.assign(self.start, self.end, 2)
        tomorrow = self.start + timedelta(days=1)
        pool.assign(tomorrow, tomorrow + timedelta(hours=1), 1)

        pool.clear_day(self.start.date())

        # Today: u1 and u2 free again
        again = pool.assign(self.start, self.end, 2)
        self.assertEqual(set(again), {"u1", "u2"})
        # Tomorrow: u1 still busy (1 picked there)
        tom_picks = pool.assign(tomorrow, tomorrow + timedelta(hours=1), 2)
        # Should pick u2 (the one not busy) — order may depend on cursor
        self.assertEqual(len(tom_picks), 1)


class TestLoadFromConfig(unittest.TestCase):
    def test_missing_jobber_block_returns_none(self):
        self.assertIsNone(load_user_pool_from_config({}))

    def test_empty_user_pool_returns_none(self):
        self.assertIsNone(load_user_pool_from_config({"jobber": {"user_pool": []}}))

    def test_populated_pool_returns_user_pool(self):
        pool = load_user_pool_from_config({"jobber": {"user_pool": ["a", "b"]}})
        self.assertIsNotNone(pool)
        self.assertEqual(pool.size, 2)


if __name__ == "__main__":
    unittest.main()
