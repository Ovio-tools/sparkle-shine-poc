"""tests/test_phase5_operations.py

NOTE: These tests will be consolidated into tests/test_simulation.py during
Step 10 (Phase 5 polish). Whoever builds that step should look here first.
"""
import heapq
import json
import os
import random
import sqlite3
import unittest
from collections import namedtuple
from datetime import date, datetime, time, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch, call

# ── Engine tests ──────────────────────────────────────────────────────────────

class TestTimedEventQueue(unittest.TestCase):
    def setUp(self):
        from simulation.engine import SimulationEngine, TimedEvent
        self.engine = SimulationEngine(dry_run=True)
        self.TimedEvent = TimedEvent

    def test_timed_queue_initialized_empty(self):
        self.assertEqual(self.engine._timed_queue, [])

    def test_queue_timed_event_adds_to_heap(self):
        fire_at = datetime(2026, 3, 28, 10, 0)
        self.engine.queue_timed_event(fire_at, "job_completion", {"job_id": "SS-JOB-0001"})
        self.assertEqual(len(self.engine._timed_queue), 1)
        event = self.engine._timed_queue[0]
        self.assertEqual(event.fire_at, fire_at)
        self.assertEqual(event.generator_name, "job_completion")
        self.assertEqual(event.kwargs, {"job_id": "SS-JOB-0001"})

    def test_queue_maintains_heap_order(self):
        t1 = datetime(2026, 3, 28, 14, 0)
        t2 = datetime(2026, 3, 28, 9, 0)
        t3 = datetime(2026, 3, 28, 11, 30)
        self.engine.queue_timed_event(t1, "job_completion", {})
        self.engine.queue_timed_event(t2, "job_completion", {})
        self.engine.queue_timed_event(t3, "job_completion", {})
        # heapq smallest at index 0
        self.assertEqual(self.engine._timed_queue[0].fire_at, t2)


class TestTimedQueueCheckpoint(unittest.TestCase):
    def setUp(self):
        from simulation.engine import SimulationEngine
        self._tmp = Path("/tmp/test_checkpoint_ops.json")
        self.engine = SimulationEngine(dry_run=False, target_date=date(2026, 3, 28))
        self.engine._checkpoint_file = self._tmp

    def tearDown(self):
        if self._tmp.exists():
            self._tmp.unlink()

    def test_save_checkpoint_includes_timed_queue(self):
        fire_at = datetime(2026, 3, 28, 14, 30)
        self.engine.queue_timed_event(fire_at, "job_completion", {"job_id": "SS-JOB-0001"})
        self.engine.save_checkpoint()
        state = json.loads(self._tmp.read_text())
        self.assertIn("timed_queue", state)
        self.assertEqual(len(state["timed_queue"]), 1)
        entry = state["timed_queue"][0]
        self.assertEqual(entry[0], fire_at.isoformat())
        self.assertEqual(entry[1], "job_completion")
        self.assertEqual(entry[2], {"job_id": "SS-JOB-0001"})

    def test_load_checkpoint_restores_timed_queue(self):
        from simulation.engine import SimulationEngine
        fire_at = datetime(2026, 3, 28, 14, 30)
        state = {
            "date": "2026-03-28",
            "counters": {},
            "event_count": 0,
            "error_count": 0,
            "timed_queue": [[fire_at.isoformat(), "job_completion", {"job_id": "SS-JOB-0001"}]],
        }
        self._tmp.write_text(json.dumps(state))
        engine2 = SimulationEngine(dry_run=True)
        engine2._checkpoint_file = self._tmp
        engine2.load_checkpoint()
        self.assertEqual(len(engine2._timed_queue), 1)
        self.assertEqual(engine2._timed_queue[0].generator_name, "job_completion")
        self.assertEqual(engine2._timed_queue[0].fire_at, fire_at)


class TestPlanDayOrdering(unittest.TestCase):
    def test_new_client_setup_is_first(self):
        from simulation.engine import SimulationEngine, GeneratorCall
        engine = SimulationEngine(dry_run=True, target_date=date(2026, 3, 28))
        plan = engine.plan_day(date(2026, 3, 28))
        self.assertEqual(plan[0], GeneratorCall("new_client_setup", {}))

    def test_job_scheduling_is_second(self):
        from simulation.engine import SimulationEngine, GeneratorCall
        engine = SimulationEngine(dry_run=True, target_date=date(2026, 3, 28))
        plan = engine.plan_day(date(2026, 3, 28))
        self.assertEqual(plan[1], GeneratorCall("job_scheduling", {}))


class TestRunOnceDrainsTimed(unittest.TestCase):
    def test_timed_event_fired_when_past_due(self):
        from simulation.engine import SimulationEngine
        engine = SimulationEngine(dry_run=True, target_date=date(2026, 3, 28))
        engine.speed = 9999  # skip sleep time
        fired = []

        class FakeGen:
            def execute(self, dry_run=False, **kwargs):
                fired.append(kwargs)

        engine.register("job_completion", FakeGen())
        past = datetime.utcnow() - timedelta(seconds=1)
        engine.queue_timed_event(past, "job_completion", {"job_id": "SS-JOB-TEST"})
        engine.run_once(date(2026, 3, 28))
        self.assertTrue(any(k.get("job_id") == "SS-JOB-TEST" for k in fired))


# ── Pure unit tests (no mocks, no DB) ────────────────────────────────────────

from simulation.generators.operations import (
    _adjusted_rating_distribution,
    _add_business_days,
    _assign_scheduled_time,
    _expected_duration,
    _is_due_today,
    _pick_job_type,
)


class TestAdjustedRatingDistribution(unittest.TestCase):
    def _weights(self, crew, dow):
        return dict(_adjusted_rating_distribution(crew, dow))

    def test_base_distribution(self):
        w = self._weights("Crew B", 0)  # Monday, not Crew A
        self.assertAlmostEqual(w[5], 0.60)
        self.assertAlmostEqual(w[4], 0.25)
        self.assertAlmostEqual(w[3], 0.10)

    def test_crew_a_distribution(self):
        w = self._weights("Crew A", 0)  # Monday
        self.assertAlmostEqual(w[5], 0.70)
        self.assertAlmostEqual(w[4], 0.20)

    def test_tue_wed_distribution(self):
        w = self._weights("Crew B", 1)  # Tuesday
        self.assertAlmostEqual(w[5], 0.65)
        self.assertAlmostEqual(w[4], 0.23)

    def test_crew_a_tue_wed_distribution(self):
        w = self._weights("Crew A", 2)  # Wednesday
        self.assertAlmostEqual(w[5], 0.75)
        self.assertAlmostEqual(w[4], 0.17)

    def test_five_star_never_exceeds_80_percent(self):
        for crew in ["Crew A", "Crew B", "Crew C", "Crew D"]:
            for dow in range(7):
                w = self._weights(crew, dow)
                self.assertLessEqual(w[5], 0.80 + 1e-9,
                    msg=f"5★ exceeded 80% for {crew}, dow={dow}")

    def test_weights_sum_to_one(self):
        for crew in ["Crew A", "Crew B"]:
            for dow in (0, 1, 2):
                w = self._weights(crew, dow)
                self.assertAlmostEqual(sum(w.values()), 1.0, places=5)


class TestIsDueToday(unittest.TestCase):
    def _agreement(self, start, dow, freq):
        return {"start_date": start, "day_of_week": dow, "frequency": freq}

    def test_weekly_fires_on_correct_day(self):
        a = self._agreement("2025-01-06", "monday", "weekly")  # Jan 6 = Monday
        self.assertTrue(_is_due_today(a, date(2025, 1, 13)))   # next Monday
        self.assertFalse(_is_due_today(a, date(2025, 1, 14)))  # Tuesday

    def test_biweekly_fires_weeks_0_2_4(self):
        a = self._agreement("2025-01-06", "monday", "biweekly")
        self.assertTrue(_is_due_today(a, date(2025, 1, 6)))    # week 0: due
        self.assertFalse(_is_due_today(a, date(2025, 1, 13)))  # week 1: skip
        self.assertTrue(_is_due_today(a, date(2025, 1, 20)))   # week 2: due
        self.assertFalse(_is_due_today(a, date(2025, 1, 27)))  # week 3: skip
        self.assertTrue(_is_due_today(a, date(2025, 2, 3)))    # week 4: due

    def test_monthly_fires_on_correct_day(self):
        a = self._agreement("2025-01-15", None, "monthly")
        self.assertTrue(_is_due_today(a, date(2025, 2, 15)))
        self.assertFalse(_is_due_today(a, date(2025, 2, 14)))

    def test_monthly_clamps_day_31_to_feb_28(self):
        a = self._agreement("2025-01-31", None, "monthly")
        self.assertTrue(_is_due_today(a, date(2025, 2, 28)))   # Feb has 28 days
        self.assertFalse(_is_due_today(a, date(2025, 2, 27)))


class TestPickJobType(unittest.TestCase):
    def _res_agreement(self):
        return {
            "service_type_id": "recurring-biweekly",
            "client_type": "residential",
            "start_date": "2025-01-06",
            "day_of_week": "monday",
            "frequency": "biweekly",
        }

    def test_residential_distribution_within_tolerance(self):
        random.seed(42)
        today = date(2025, 8, 1)  # August — no seasonal boost
        types = [_pick_job_type(self._res_agreement(), today) for _ in range(200)]
        regular_rate = types.count("regular") / 200
        deep_rate = types.count("deep_clean") / 200
        add_on_rate = types.count("add_on") / 200
        self.assertGreaterEqual(regular_rate, 0.75)
        self.assertLessEqual(regular_rate, 0.95)
        self.assertGreaterEqual(deep_rate, 0.04)
        self.assertLessEqual(deep_rate, 0.20)
        self.assertGreaterEqual(add_on_rate, 0.00)
        self.assertLessEqual(add_on_rate, 0.12)

    def test_seasonal_deep_clean_boost_march_vs_august(self):
        random.seed(0)
        march = date(2025, 3, 1)
        august = date(2025, 8, 1)
        a = self._res_agreement()
        march_deep = sum(
            1 for _ in range(500) if _pick_job_type(a, march) == "deep_clean"
        ) / 500
        august_deep = sum(
            1 for _ in range(500) if _pick_job_type(a, august) == "deep_clean"
        ) / 500
        self.assertGreater(march_deep, august_deep,
            msg="March deep-clean rate should exceed August (seasonal boost)")

    def test_commercial_returns_standard_or_extra(self):
        a = {
            "service_type_id": "commercial-nightly",
            "client_type": "commercial",
            "start_date": "2025-01-01",
            "day_of_week": "monday",
            "frequency": "weekly",
        }
        today = date(2025, 8, 1)
        random.seed(1)
        types = {_pick_job_type(a, today) for _ in range(50)}
        self.assertTrue(types.issubset({"standard", "extra_service"}))


class TestAssignScheduledTime(unittest.TestCase):
    def test_first_job_gets_window_start(self):
        result = _assign_scheduled_time("crew-a", [], date(2026, 3, 28))
        self.assertEqual(result, datetime(2026, 3, 28, 7, 0))

    def test_crew_d_starts_at_5pm(self):
        result = _assign_scheduled_time("crew-d", [], date(2026, 3, 28))
        self.assertEqual(result, datetime(2026, 3, 28, 17, 0))

    def test_sequential_jobs_are_ascending(self):
        random.seed(42)
        prior = []
        times = []
        for _ in range(4):
            t = _assign_scheduled_time("crew-a", prior, date(2026, 3, 28))
            times.append(t)
            prior.append({"expected_duration": 120})
        for i in range(1, len(times)):
            self.assertGreater(times[i], times[i - 1],
                msg=f"Job {i} start not after job {i-1}")

    def test_gap_between_jobs_at_least_15_minutes(self):
        random.seed(7)
        prior = [{"expected_duration": 120}]
        t0 = _assign_scheduled_time("crew-b", [], date(2026, 3, 28))
        t1 = _assign_scheduled_time("crew-b", prior, date(2026, 3, 28))
        gap = (t1 - t0).total_seconds() / 60
        # gap = 120 (duration) + travel buffer (15-30)
        self.assertGreaterEqual(gap, 135)  # 120 + 15 minimum
        self.assertLessEqual(gap, 152)     # 120 + 30 + small rounding


class TestAddBusinessDays(unittest.TestCase):
    def test_friday_plus_1_is_monday(self):
        friday = date(2026, 3, 27)
        result = _add_business_days(friday, 1)
        self.assertEqual(result, date(2026, 3, 30))  # Monday

    def test_thursday_plus_1_is_friday(self):
        result = _add_business_days(date(2026, 3, 26), 1)
        self.assertEqual(result, date(2026, 3, 27))
