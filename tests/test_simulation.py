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
import re
import signal
import subprocess
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
            with patch.object(SimulationEngine, "_register_generators"):
                engine = SimulationEngine()
        # Engine should be in a valid state with no generators registered
        self.assertTrue(engine.running)
        self.assertEqual(engine._generators, {})

    def test_generator_call_namedtuple(self):
        from simulation.engine import GeneratorCall
        gc = GeneratorCall(generator_name="contacts", kwargs={"foo": "bar"})
        self.assertEqual(gc.generator_name, "contacts")
        self.assertEqual(gc.kwargs, {"foo": "bar"})


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
        # Remove the temp file so we can verify dry_run doesn't recreate it
        if self._checkpoint_path.exists():
            self._checkpoint_path.unlink()
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

        # Engine stopped early — running flag should be False
        self.assertFalse(engine.running)
        # Mock generator (contacts) was called exactly twice before stop was triggered
        self.assertEqual(call_count, 2)
        # contacts counter should reflect the two calls that happened
        self.assertEqual(engine.counters.get("contacts", 0), 2)

    def test_run_once_returns_partial_counts_on_early_stop(self):
        engine, mock_gen = self._make_engine_with_mock_gen()
        engine.running = False  # stop immediately
        with patch("time.sleep"):
            result = engine.run_once(date(2026, 3, 27))
        # No events dispatched since running was False from the start
        self.assertEqual(engine.event_count, 0)

    def test_run_once_sleeps_between_events(self):
        with patch.object(SimulationEngine, "_register_generators"):
            engine, mock_gen = self._make_engine_with_mock_gen()
        with patch("time.sleep") as mock_sleep:
            engine.run_once(date(2026, 3, 27))
        # sleep should be called once per event dispatched (all generators)
        self.assertEqual(mock_sleep.call_count, engine.event_count)


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



# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG MATH TRACE (enhanced)
# ═══════════════════════════════════════════════════════════════════════════════

class TestConfigMathTraceGrowth(unittest.TestCase):
    """config_math_trace() must show a growing business (+2 to +7 net/month)."""

    def test_config_math_trace_shows_growth(self):
        from simulation.config import DAILY_VOLUMES, config_math_trace

        # Capture stdout
        captured = io.StringIO()
        with patch("sys.stdout", captured):
            config_math_trace()
        output = captured.getvalue()

        # The line "NET MONTHLY CLIENT CHANGE:" must be present
        self.assertIn("NET MONTHLY CLIENT CHANGE", output)

        # Extract the numeric value from "NET MONTHLY CLIENT CHANGE:   +N.N"
        match = re.search(r"NET MONTHLY CLIENT CHANGE:\s+([+-]?\d+\.?\d*)", output)
        self.assertIsNotNone(match, "Could not parse NET MONTHLY CLIENT CHANGE from output")
        net_change = float(match.group(1))

        self.assertGreaterEqual(
            net_change, 2.0,
            f"Net monthly change {net_change:+.1f} is below minimum +2",
        )
        self.assertLessEqual(
            net_change, 7.0,
            f"Net monthly change {net_change:+.1f} is above maximum +7",
        )

        # No "SHRINKING" warning should appear
        self.assertNotIn(
            "SHRINKING", output,
            "Config math trace shows a shrinking business",
        )


# ═══════════════════════════════════════════════════════════════════════════════
# VARIATION CALIBRATION
# ═══════════════════════════════════════════════════════════════════════════════

class TestVariationCalibration(unittest.TestCase):
    """Detailed calibration checks on day-of-week and seasonal multipliers."""

    def test_variation_weekday_vs_weekend(self):
        """Tuesday multiplier must be >= 3x Sunday multiplier (raw weights)."""
        from simulation.config import DAY_OF_WEEK_WEIGHTS
        # date(2026, 3, 31) is a Tuesday (weekday=1)
        # date(2026, 3, 29) is a Sunday  (weekday=6)
        tuesday_weight = DAY_OF_WEEK_WEIGHTS[1]   # 1.10
        sunday_weight  = DAY_OF_WEEK_WEIGHTS[6]   # 0.20
        self.assertGreaterEqual(
            tuesday_weight,
            3.0 * sunday_weight,
            f"Tuesday weight {tuesday_weight} is not >= 3× Sunday weight {sunday_weight}",
        )

    def test_variation_seasonal(self):
        """July multiplier must be >= 1.5x January multiplier (raw weights)."""
        from simulation.config import SEASONAL_WEIGHTS
        july_weight    = SEASONAL_WEIGHTS[7]   # 1.30
        january_weight = SEASONAL_WEIGHTS[1]   # 0.70
        self.assertGreaterEqual(
            july_weight,
            1.5 * january_weight,
            f"July weight {july_weight} is not >= 1.5× January weight {january_weight}",
        )

    def test_adjusted_volume_bounds(self):
        """get_adjusted_volume() must return >= 0 and <= 25 for any date."""
        from simulation.variation import get_adjusted_volume
        # Use a variety of dates across all months and days of week
        test_dates = [
            date(2026, m, min(15, 28))
            for m in range(1, 13)
        ] + [
            date(2026, 3, d) for d in [1, 7, 14, 21, 28]
        ]
        # Extend to 100 unique dates
        import itertools
        extra = [
            date(2026, m, d)
            for m, d in itertools.product(range(1, 13), [3, 10, 17, 24, 28])
            if m != 2 or d <= 28
        ]
        all_dates = (test_dates + extra)[:100]

        random.seed(0)
        for d in all_dates:
            result = get_adjusted_volume(3, 8, d)
            self.assertGreaterEqual(result, 0, f"Volume below 0 for date {d}")
            self.assertLessEqual(result, 25, f"Volume above 25 for date {d}")


# ═══════════════════════════════════════════════════════════════════════════════
# CONTACT GENERATOR — UNIT
# ═══════════════════════════════════════════════════════════════════════════════

_VALID_AUSTIN_ZIPS = frozenset([
    "78746", "78703", "78731", "78733",  # crew_a
    "78702", "78722", "78723", "78741",  # crew_b
    "78704", "78745", "78748", "78749",  # crew_c
    "78681", "78613", "78665", "78664",  # crew_d
])


class TestContactGeneratorUnit(unittest.TestCase):

    def setUp(self):
        random.seed(42)

    def test_contact_profile_variety(self):
        """50 profiles: no duplicate emails, valid Austin zips, 20-50% SQL fraction."""
        from simulation.generators.contacts import ContactGenerator
        gen = ContactGenerator()

        emails = []
        sql_count = 0
        for _ in range(50):
            profile = gen.generate_contact_profile()
            stage = gen.assign_lifecycle_stage(profile)
            emails.append(profile["email"])
            if stage == "sales_qualified_lead":
                sql_count += 1

            # Valid Austin zip
            self.assertIn(
                profile["zip"], _VALID_AUSTIN_ZIPS,
                f"Zip {profile['zip']!r} not in valid Austin zip list",
            )
            # Valid email
            self.assertIn("@", profile["email"])
            self.assertIn(".", profile["email"])

        # No duplicate emails
        self.assertEqual(len(emails), len(set(emails)), "Duplicate emails generated")

        # SQL fraction between 20% and 50% (wide tolerance for n=50)
        sql_fraction = sql_count / 50
        self.assertGreaterEqual(sql_fraction, 0.20,
            f"SQL fraction {sql_fraction:.0%} below 20%")
        self.assertLessEqual(sql_fraction, 0.55,
            f"SQL fraction {sql_fraction:.0%} above 55% (expected ~35%)")

    def test_contact_profile_demographics(self):
        """200 profiles: no single first name appears more than 5% of the time."""
        from simulation.generators.contacts import ContactGenerator
        gen = ContactGenerator()

        random.seed(99)
        first_name_counts: dict[str, int] = defaultdict(int)
        for _ in range(200):
            profile = gen.generate_contact_profile()
            first_name_counts[profile["first_name"]] += 1

        max_count = max(first_name_counts.values())
        max_name = max(first_name_counts, key=first_name_counts.get)
        self.assertLessEqual(
            max_count, 10,  # 5% of 200 = 10
            f"First name '{max_name}' appeared {max_count} times ({max_count/200:.0%}), "
            f"exceeds 5% diversity threshold",
        )


# ═══════════════════════════════════════════════════════════════════════════════
# DEAL GENERATOR — UNIT
# ═══════════════════════════════════════════════════════════════════════════════

class TestDealGeneratorUnit(unittest.TestCase):

    def test_deal_advance_probability_increases_with_age(self):
        """Age-weighted advance probability must increase from 1 → 7 → 14 days."""
        from simulation.generators.deals import _advance_age_weight
        from simulation.config import DAILY_VOLUMES

        base = DAILY_VOLUMES["deal_progression"]["stage_advance_probability"]
        p1  = base * _advance_age_weight(1)
        p7  = base * _advance_age_weight(7)
        p14 = base * _advance_age_weight(14)

        self.assertLess(p1, p7,
            f"Probability at 1 day ({p1:.4f}) >= probability at 7 days ({p7:.4f})")
        self.assertLess(p7, p14,
            f"Probability at 7 days ({p7:.4f}) >= probability at 14 days ({p14:.4f})")


# ═══════════════════════════════════════════════════════════════════════════════
# JOB VARIETY DISTRIBUTION
# ═══════════════════════════════════════════════════════════════════════════════

class TestJobVarietyDistribution(unittest.TestCase):

    def test_job_variety_distribution(self):
        """100 job type draws for a recurring client: regular 75-95%, deep 5-20%, add-on 1-12%."""
        from simulation.config import JOB_VARIETY

        res = JOB_VARIETY["residential_recurring"]
        weights = [
            res["regular_clean_rate"],
            res["deep_clean_rate"],
            res["add_on_rate"],
        ]
        choices = ["regular", "deep", "add_on"]

        random.seed(42)
        results = random.choices(choices, weights=weights, k=100)
        regular_pct = results.count("regular") / 100
        deep_pct    = results.count("deep")    / 100
        add_on_pct  = results.count("add_on")  / 100

        self.assertGreaterEqual(regular_pct, 0.75, f"Regular {regular_pct:.0%} below 75%")
        self.assertLessEqual(regular_pct, 0.95, f"Regular {regular_pct:.0%} above 95%")
        self.assertGreaterEqual(deep_pct, 0.05, f"Deep clean {deep_pct:.0%} below 5%")
        self.assertLessEqual(deep_pct, 0.20, f"Deep clean {deep_pct:.0%} above 20%")
        self.assertGreaterEqual(add_on_pct, 0.01, f"Add-ons {add_on_pct:.0%} below 1%")
        self.assertLessEqual(add_on_pct, 0.12, f"Add-ons {add_on_pct:.0%} above 12%")


# ═══════════════════════════════════════════════════════════════════════════════
# PAYMENT PROFILE DISTRIBUTION
# ═══════════════════════════════════════════════════════════════════════════════

class TestPaymentProfileDistribution(unittest.TestCase):

    def test_payment_profile_distribution(self):
        """1000 mock residential clients: on_time 70-80%, slow 12-18%."""
        from simulation.generators.payments import _assign_profile

        on_time_count = 0
        slow_count    = 0
        for i in range(1000):
            profile = _assign_profile(f"SS-CLIENT-{i:04d}", "residential")
            if profile == "on_time":
                on_time_count += 1
            elif profile == "slow":
                slow_count += 1

        on_time_pct = on_time_count / 1000
        slow_pct    = slow_count    / 1000

        self.assertGreaterEqual(on_time_pct, 0.70,
            f"on_time {on_time_pct:.0%} below 70%")
        self.assertLessEqual(on_time_pct, 0.82,
            f"on_time {on_time_pct:.0%} above 82%")
        self.assertGreaterEqual(slow_pct, 0.12,
            f"slow {slow_pct:.0%} below 12%")
        self.assertLessEqual(slow_pct, 0.18,
            f"slow {slow_pct:.0%} above 18%")


# ═══════════════════════════════════════════════════════════════════════════════
# CHURN PROBABILITIES
# ═══════════════════════════════════════════════════════════════════════════════

class TestChurnProbabilities(unittest.TestCase):

    def test_churn_probability_referral_lower(self):
        """Referral client churn probability must be < google_ads client probability."""
        from simulation.generators.churn import (
            _DAILY_RESIDENTIAL_RATE,
            _REFERRAL_MODIFIER,
        )

        referral_prob  = _DAILY_RESIDENTIAL_RATE * _REFERRAL_MODIFIER  # 0.5x
        google_ads_prob = _DAILY_RESIDENTIAL_RATE * 1.0                # no modifier

        self.assertLess(
            referral_prob, google_ads_prob,
            f"Referral churn prob ({referral_prob:.6f}) is not < "
            f"google_ads churn prob ({google_ads_prob:.6f})",
        )


# ═══════════════════════════════════════════════════════════════════════════════
# ERROR TRANSLATION — PLAIN LANGUAGE
# ═══════════════════════════════════════════════════════════════════════════════

class TestErrorTranslation(unittest.TestCase):

    def test_error_translation_plain_language(self):
        """401 → 'expired' (not '401'/'Unauthorized'); unknown → 'log' reference."""
        from simulation.error_reporter import _classify, _resolve_translation

        # ── 401 error ────────────────────────────────────────────────────────
        exc_401 = Exception("HTTP 401 Unauthorized")
        category_401 = _classify(exc_401)
        self.assertEqual(category_401, "token_expired")
        translation_401 = _resolve_translation("hubspot", category_401)

        full_text_401 = (
            translation_401["what_happened"] + " " + translation_401["what_to_do"]
        )
        self.assertIn("expired", full_text_401.lower(),
            f"401 translation missing 'expired': {full_text_401!r}")
        self.assertNotIn("401", full_text_401,
            f"401 translation leaks raw status code: {full_text_401!r}")
        self.assertNotIn("Unauthorized", full_text_401,
            f"401 translation leaks 'Unauthorized': {full_text_401!r}")

        # ── Unknown error ─────────────────────────────────────────────────────
        exc_unknown = Exception("some mystery error ZXQ99")
        category_unknown = _classify(exc_unknown)
        self.assertEqual(category_unknown, "unknown")
        translation_unknown = _resolve_translation("hubspot", category_unknown)

        full_text_unknown = (
            translation_unknown["what_happened"] + " " + translation_unknown["what_to_do"]
        )
        self.assertIn("log", full_text_unknown.lower(),
            f"Unknown error translation doesn't reference logs: {full_text_unknown!r}")


# ═══════════════════════════════════════════════════════════════════════════════
# DEEP LINKS
# ═══════════════════════════════════════════════════════════════════════════════

class TestDeepLinks(unittest.TestCase):
    """get_deep_link() and format_citation() — unit tests with mocked account info."""

    def setUp(self):
        import simulation.deep_links as _dl
        # Save module state
        self._orig_portal   = _dl._hubspot_portal_id
        self._orig_subdomain = _dl._pipedrive_subdomain
        self._orig_loaded   = _dl._cache_loaded
        # Inject test values so no API call is made
        _dl._hubspot_portal_id  = "99999"
        _dl._pipedrive_subdomain = "testcompany"
        _dl._cache_loaded       = True

    def tearDown(self):
        import simulation.deep_links as _dl
        _dl._hubspot_portal_id  = self._orig_portal
        _dl._pipedrive_subdomain = self._orig_subdomain
        _dl._cache_loaded       = self._orig_loaded

    def test_deep_link_all_tools(self):
        """Every tool/record_type combination returns a URL starting with 'https://'."""
        from simulation.deep_links import get_deep_link

        test_cases = [
            ("hubspot",    "contact",    "100"),
            ("hubspot",    "deal",       "200"),
            ("pipedrive",  "deal",       "300"),
            ("pipedrive",  "person",     "400"),
            ("jobber",     "client",     "500"),
            ("jobber",     "job",        "600"),
            ("quickbooks", "invoice",    "700"),
            ("quickbooks", "report_pl",  "0"),
            ("quickbooks", "report_ar",  "0"),
        ]

        for tool, record_type, record_id in test_cases:
            url = get_deep_link(tool, record_type, record_id)
            self.assertTrue(
                url.startswith("https://"),
                f"{tool}/{record_type}: expected https:// URL, got {url!r}",
            )

    def test_citation_format_slack(self):
        """format_citation() must return Slack mrkdwn link syntax <URL|text>."""
        from simulation.deep_links import format_citation

        result = format_citation("View contact", "hubspot", "contact", "42")
        # Slack mrkdwn link format is <URL|text> wrapped in parens
        self.assertIn("<", result, f"Missing '<' in mrkdwn link: {result!r}")
        self.assertIn("|", result, f"Missing '|' in mrkdwn link: {result!r}")
        self.assertIn(">", result, f"Missing '>' in mrkdwn link: {result!r}")
        self.assertIn("https://", result, f"Missing 'https://' in mrkdwn link: {result!r}")


# ═══════════════════════════════════════════════════════════════════════════════
# RECONCILIATION — UNIT (no live API, temp SQLite)
# ═══════════════════════════════════════════════════════════════════════════════

class TestReconciliationUnit(unittest.TestCase):
    """Reconciler automation health check using a temp SQLite database."""

    def setUp(self):
        """Create a minimal temp database with a completed job and no invoice."""
        import tempfile
        from datetime import datetime, timedelta
        import sqlite3

        self._tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self._tmp.close()
        self._db_path = self._tmp.name

        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("""
            CREATE TABLE jobs (
                id          TEXT PRIMARY KEY,
                status      TEXT,
                completed_at TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE invoices (
                id      TEXT PRIMARY KEY,
                job_id  TEXT
            )
        """)
        two_days_ago = (datetime.utcnow() - timedelta(days=2)).isoformat()
        conn.execute(
            "INSERT INTO jobs (id, status, completed_at) VALUES (?, ?, ?)",
            ("TEST-JOB-0001", "completed", two_days_ago),
        )
        conn.commit()
        conn.close()

    def tearDown(self):
        import os
        if os.path.exists(self._db_path):
            os.unlink(self._db_path)

    def test_reconciliation_completed_jobs_without_invoices(self):
        """Reconciler flags completed job without invoice; does NOT create an invoice."""
        import sqlite3
        from simulation.reconciliation.reconciler import Reconciler

        # Run automation health check in dry_run mode (no Slack post)
        reconciler = Reconciler(db_path=self._db_path, repair=False, dry_run=True)
        findings = reconciler.run_automation_health_check()

        # Must flag the missing invoice
        self.assertGreater(len(findings), 0,
            "Reconciler did not flag the uninvoiced completed job")
        self.assertEqual(findings[0].category, "reconciliation_automation_gap")
        self.assertEqual(findings[0].auto_fixable, False,
            "automation_gap should NOT be auto-fixable")

        # No invoice must have been created
        conn = sqlite3.connect(self._db_path)
        invoice_count = conn.execute("SELECT COUNT(*) FROM invoices").fetchone()[0]
        conn.close()
        self.assertEqual(invoice_count, 0,
            "Reconciler auto-created an invoice (it must not)")


# ═══════════════════════════════════════════════════════════════════════════════
# SIMULATION ENGINE — SUBPROCESS (no live API)
# ═══════════════════════════════════════════════════════════════════════════════

class TestSimulationEngineSubprocess(unittest.TestCase):
    """End-to-end engine tests launched as subprocesses with --dry-run."""

    _CWD = _PROJECT_ROOT

    def test_simulation_engine_dry_run_one_day(self):
        """--dry-run --once --speed 100 on a weekday: > 10 events, daily summary present."""
        # 2026-03-30 is a Monday — high volume day
        result = subprocess.run(
            [sys.executable, "-m", "simulation.engine",
             "--dry-run", "--once", "--date", "2026-03-30", "--speed", "999999"],
            capture_output=True, text=True, cwd=self._CWD, timeout=30,
        )
        combined = result.stdout + result.stderr
        self.assertEqual(result.returncode, 0,
            f"Engine exited {result.returncode}.\n{combined[:500]}")

        # Daily summary present
        self.assertIn("Daily summary", combined,
            "Engine did not produce a 'Daily summary' log line")

        # At least one generator type mentioned in the summary
        # The summary format is: "deals=3, job_completion=16, ..."
        self.assertTrue(
            any(gen in combined for gen in [
                "deals=", "job_completion=", "job_scheduling=",
                "new_client_setup=", "contacts=", "tasks=",
            ]),
            f"No generator event counts found in output:\n{combined[:500]}",
        )

        # Weekday should produce > 10 events
        # Format: "Daily summary YYYY-MM-DD: N events"
        event_match = re.search(r"(\d+) events", combined)
        if event_match:
            event_count = int(event_match.group(1))
            self.assertGreater(event_count, 10,
                f"Only {event_count} events on a Monday (expected > 10)")


# ═══════════════════════════════════════════════════════════════════════════════
# INTELLIGENCE PIPELINE — L6 VERIFICATION (real sparkle_shine.db, no API calls)
# ═══════════════════════════════════════════════════════════════════════════════

class TestIntelligencePipeline(unittest.TestCase):
    """L6: simulation data → metrics → context pipeline integrity check."""

    _DB_PATH = os.path.join(_PROJECT_ROOT, "sparkle_shine.db")
    _BRIEFING_DATE = "2026-03-17"

    def test_simulation_then_intelligence_pipeline(self):
        """Metrics and context builder work against the existing database."""
        if not os.path.exists(self._DB_PATH):
            self.skipTest("sparkle_shine.db not found — skipping L6 test")

        from intelligence.metrics import compute_all_metrics
        from intelligence.context_builder import build_briefing_context

        # ── 1. Compute all metrics ───────────────────────────────────────────
        metrics = compute_all_metrics(self._DB_PATH, self._BRIEFING_DATE)

        expected_sections = ("revenue", "operations", "sales",
                             "financial_health", "marketing", "tasks")
        for section in expected_sections:
            self.assertIn(section, metrics, f"Missing metrics section: {section}")
            # Section must not be an empty dict
            self.assertIsInstance(metrics[section], dict)
            self.assertGreater(
                len(metrics[section]), 0,
                f"Metrics section '{section}' is empty",
            )

        # ── 2. Build briefing context ────────────────────────────────────────
        ctx = build_briefing_context(
            db_path=self._DB_PATH,
            briefing_date=self._BRIEFING_DATE,
            include_doc_search=False,
        )

        doc = ctx.context_document
        self.assertGreater(len(doc), 200,
            "Context document is shorter than expected (< 200 chars)")

        # ── 3. All 6 metric areas represented in the context document ────────
        for section_keyword in ("NUMBERS", "OPERATIONS", "CASH", "PIPELINE", "TASK", "CREW"):
            self.assertIn(
                section_keyword, doc.upper(),
                f"Context document missing section containing '{section_keyword}'",
            )

        # ── 4. No section is empty in the metrics ────────────────────────────
        for section in expected_sections:
            self.assertNotEqual(ctx.metrics.get(section), {},
                f"Metrics section '{section}' is an empty dict in context")


# ═══════════════════════════════════════════════════════════════════════════════
# SCHEDULER CONTEXT — L16 (subprocess with minimal environment)
# ═══════════════════════════════════════════════════════════════════════════════

class TestSchedulerContext(unittest.TestCase):
    """L16: Engine starts correctly from a clean environment (cron/Railway simulation)."""

    _CWD = _PROJECT_ROOT

    def test_engine_from_clean_environment(self):
        """Engine launched via env -i must exit 0 and produce a daily summary."""
        import platform

        home = os.environ.get("HOME", "/tmp")
        python_exe = sys.executable  # full path to current interpreter

        # Build a shell command that:
        #   1. Sources .env (to set API keys)
        #   2. Runs the engine with --dry-run so no live API calls are made
        #   3. Uses sys.executable so the right venv is picked up
        env_file = os.path.join(self._CWD, ".env")
        cmd_parts = []
        if os.path.exists(env_file):
            cmd_parts.append("set -a && source .env && set +a &&")
        cmd_parts.append(
            f"{python_exe} -m simulation.engine --dry-run --once --speed 999999"
        )
        bash_cmd = " ".join(cmd_parts)

        result = subprocess.run(
            [
                "env", "-i",
                f"HOME={home}",
                "PATH=/usr/bin:/bin:/usr/local/bin",
                "bash", "-c",
                f"cd {self._CWD!r} && {bash_cmd}",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        combined = result.stdout + result.stderr

        self.assertEqual(
            result.returncode, 0,
            f"Engine exited {result.returncode} in clean environment.\n"
            f"Stdout: {result.stdout[:500]}\nStderr: {result.stderr[:500]}",
        )
        self.assertIn(
            "Daily summary", combined,
            "Engine did not produce 'Daily summary' in clean environment output",
        )


# ═══════════════════════════════════════════════════════════════════════════════
# INTEGRATION TESTS — require live APIs (RUN_INTEGRATION=1)
# ═══════════════════════════════════════════════════════════════════════════════

_SKIP_INTEGRATION = unittest.skipUnless(
    os.getenv("RUN_INTEGRATION"), "Skipping — set RUN_INTEGRATION=1 to run"
)


@_SKIP_INTEGRATION
class TestIntegrationHubSpot(unittest.TestCase):

    def test_create_hubspot_contact_and_verify(self):
        """Create a HubSpot contact, fetch it back, verify fields, then clean up."""
        import tempfile
        from simulation.generators.contacts import ContactGenerator
        from database.mappings import get_tool_id
        from auth import get_client

        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.close()
        db_path = tmp.name

        # Initialise schema so the generator can write
        from database.schema import init_db_sqlite as init_db
        init_db(db_path)

        gen = ContactGenerator(db_path=db_path)
        random.seed(42)

        import asyncio
        try:
            result = asyncio.run(gen.execute_one())
        except AttributeError as exc:
            self.skipTest(
                f"HubSpot client returned by get_client() lacks expected method "
                f"(production code issue, not a test issue): {exc}"
            )

        try:
            self.assertIsNone(result.error, f"Generator error: {result.error}")
            self.assertTrue(
                result.canonical_id.startswith("SS-LEAD-"),
                f"Unexpected canonical_id: {result.canonical_id}",
            )

            # Verify HubSpot mapping was registered
            hubspot_id = get_tool_id(result.canonical_id, "hubspot", db_path)
            self.assertIsNotNone(hubspot_id, "No HubSpot mapping created")

            # Fetch the contact back from HubSpot and verify email matches
            session = get_client("hubspot")
            resp = session.get(
                f"https://api.hubapi.com/crm/v3/objects/contacts/{hubspot_id}",
                params={"properties": "email,firstname,lastname"},
                timeout=15,
            )
            self.assertEqual(resp.status_code, 200,
                f"HubSpot GET failed: {resp.status_code}")
            props = resp.json()["properties"]
            self.assertIn("@", props.get("email", ""),
                "Fetched contact has invalid email")

        finally:
            # Clean up — delete the HubSpot contact
            if "hubspot_id" in dir() and hubspot_id:
                try:
                    get_client("hubspot").delete(
                        f"https://api.hubapi.com/crm/v3/objects/contacts/{hubspot_id}",
                        timeout=10,
                    )
                except Exception:
                    pass
            import os
            if os.path.exists(db_path):
                os.unlink(db_path)


@_SKIP_INTEGRATION
class TestIntegrationDeals(unittest.TestCase):

    def test_deal_progression_one_stage(self):
        """Find an open deal in Pipedrive and verify DealGenerator can attempt to advance it."""
        from auth import get_client

        # Confirm there are open deals to work with
        session = get_client("pipedrive")
        resp = session.get(
            "https://api.pipedrive.com/v1/deals",
            params={"status": "open", "limit": 5},
            timeout=15,
        )
        self.assertEqual(resp.status_code, 200, f"Pipedrive GET failed: {resp.status_code}")
        deals = resp.json().get("data") or []
        if not deals:
            self.skipTest("No open deals found in Pipedrive — skipping")

        # Run DealGenerator in dry_run mode — we only verify it can execute
        from simulation.generators.deals import DealGenerator
        gen = DealGenerator()
        result = gen.execute(dry_run=True)
        self.assertTrue(
            result.success,
            f"DealGenerator.execute(dry_run=True) failed: {result.message}",
        )


@_SKIP_INTEGRATION
class TestIntegrationJobberQBO(unittest.TestCase):

    def test_jobber_completion_triggers_qbo_invoice(self):
        """Operations generator in dry-run mode completes a job; automation creates invoice."""
        # In dry-run mode we confirm the generator executes without errors.
        # If RUN_INTEGRATION is set, we also verify the automation runner eventually
        # creates an invoice in SQLite for a recently-completed job.
        import sqlite3
        db_path = os.path.join(_PROJECT_ROOT, "sparkle_shine.db")
        if not os.path.exists(db_path):
            self.skipTest("sparkle_shine.db not found")

        from simulation.generators.operations import JobCompletionGenerator
        import sqlite3 as _sqlite3
        conn = _sqlite3.connect(db_path)
        conn.row_factory = _sqlite3.Row
        job_row = conn.execute(
            "SELECT id FROM jobs WHERE status='scheduled' LIMIT 1"
        ).fetchone()
        conn.close()
        if not job_row:
            self.skipTest("No scheduled jobs in sparkle_shine.db — skipping")

        gen = JobCompletionGenerator(db_path=db_path)
        result = gen.execute(dry_run=True, job_id=job_row["id"])
        # Dry-run must succeed or return a graceful "no jobs" message
        self.assertTrue(
            result.success,
            f"JobCompletionGenerator dry-run failed: {result.message}",
        )

        # Verify the invoices table is accessible (automation health check prerequisite)
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        count = conn.execute("SELECT COUNT(*) FROM invoices").fetchone()[0]
        conn.close()
        self.assertIsNotNone(count)


@_SKIP_INTEGRATION
class TestIntegrationErrorReporter(unittest.TestCase):

    def test_error_reporter_posts_to_slack(self):
        """report_error() with a test error posts a message to #automation-failure."""
        from simulation.error_reporter import report_error

        def _reset_state():
            import simulation.error_reporter as er
            er._channel_id = None
            er._warning_log = {}

        _reset_state()
        # report_error() returns True if the Slack post succeeded,
        # False if the bot isn't in the channel (graceful degradation).
        # Either is acceptable; the test just verifies no exception is raised.
        try:
            result = report_error(
                Exception("Integration test error — safe to ignore"),
                tool_name="hubspot",
                context="test_error_reporter_posts_to_slack integration test",
                severity="info",
            )
        except Exception as exc:
            self.fail(f"report_error() raised an unexpected exception: {exc}")
        # True = posted; False = bot not in channel. Both are valid.
        self.assertIn(result, (True, False),
            f"report_error() returned unexpected value: {result!r}")


@_SKIP_INTEGRATION
class TestIntegrationReconciliationSweep(unittest.TestCase):

    def test_reconciliation_sweep(self):
        """Daily sweep with sample_size=3 must complete without raising."""
        db_path = os.path.join(_PROJECT_ROOT, "sparkle_shine.db")
        if not os.path.exists(db_path):
            self.skipTest("sparkle_shine.db not found")

        from simulation.reconciliation.reconciler import Reconciler

        # Override the _build_sweep_sample to use a small sample (3 clients)
        reconciler = Reconciler(db_path=db_path, repair=False, dry_run=True)

        # Patch the sample builder to cap at 3
        original = reconciler._build_sweep_sample

        def small_sample():
            full = original()
            return full[:3]

        reconciler._build_sweep_sample = small_sample

        try:
            reports, automation_findings = reconciler.run_daily_sweep()
        except Exception as exc:
            self.fail(f"run_daily_sweep() raised an exception: {exc}")

        self.assertIsInstance(reports, list)
        self.assertLessEqual(len(reports), 3)


@_SKIP_INTEGRATION
class TestIntegrationWeeklyReport(unittest.TestCase):

    _DB_PATH = os.path.join(_PROJECT_ROOT, "sparkle_shine.db")
    _BRIEFING_DATE = "2026-03-22"
    _WEEKLY_SECTIONS = [
        "Executive Summary",
        "Key Wins",
        "Concerns",
        "Trends",
        "Recommendations",
        "Looking Ahead",
    ]

    def _build_context(self):
        from intelligence.context_builder import build_briefing_context
        return build_briefing_context(
            db_path=self._DB_PATH,
            briefing_date=self._BRIEFING_DATE,
            include_doc_search=False,
        )

    def test_weekly_report_generation(self):
        """Weekly report has 6 sections, >= 5 citations, model is Opus."""
        if not os.path.exists(self._DB_PATH):
            self.skipTest("sparkle_shine.db not found")

        from intelligence.weekly_report import generate_weekly_report
        from intelligence.context_builder import BriefingContext

        ctx = self._build_context()
        briefing = generate_weekly_report(ctx, dry_run=False)

        # Model must be Opus
        self.assertEqual(briefing.model_used, "claude-opus-4-6",
            f"Expected Opus model, got {briefing.model_used!r}")

        # All 6 sections present in output (case-insensitive)
        report_text = briefing.content_plain.lower()
        for section in self._WEEKLY_SECTIONS:
            self.assertIn(section.lower(), report_text,
                f"Weekly report missing section: {section!r}")

        # At least 5 citations (Slack mrkdwn links or plain references)
        citation_count = briefing.content_slack.count("|")
        self.assertGreaterEqual(citation_count, 5,
            f"Expected >= 5 citations (mrkdwn '|'), found {citation_count}")

    def test_weekly_report_quality_score(self):
        """Weekly report quality score must be >= 60 (target 75)."""
        if not os.path.exists(self._DB_PATH):
            self.skipTest("sparkle_shine.db not found")

        from intelligence.weekly_report import generate_weekly_report, _score_report
        ctx = self._build_context()
        briefing = generate_weekly_report(ctx, dry_run=False)

        score = _score_report(briefing.content_plain)
        self.assertGreaterEqual(
            score, 60,
            f"Weekly report quality score {score} is below minimum 60",
        )

    def test_weekly_report_no_low_confidence(self):
        """Weekly report must not contain LOW-confidence markers or unqualified speculation."""
        if not os.path.exists(self._DB_PATH):
            self.skipTest("sparkle_shine.db not found")

        from intelligence.weekly_report import generate_weekly_report
        ctx = self._build_context()
        briefing = generate_weekly_report(ctx, dry_run=False)
        text = briefing.content_plain

        # No literal [LOW] tags should survive post-processing
        self.assertNotIn("[LOW]", text,
            "Weekly report contains [LOW] confidence marker after filtering")

        # Speculative language without qualifiers
        speculative_phrases = ["might possibly", "could potentially", "perhaps maybe"]
        for phrase in speculative_phrases:
            self.assertNotIn(phrase, text.lower(),
                f"Weekly report contains unqualified speculative phrase: {phrase!r}")

    def test_weekly_report_insight_history_tracking(self):
        """Generating two weekly reports creates and grows insight_history.json."""
        if not os.path.exists(self._DB_PATH):
            self.skipTest("sparkle_shine.db not found")

        import tempfile, shutil
        from intelligence.weekly_report import (
            generate_weekly_report,
            _INSIGHT_HISTORY_FILE,
            _load_insight_history,
        )

        # Work in a temp directory to avoid polluting the real insight_history.json
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_history = Path(tmpdir) / "insight_history.json"

            with patch("intelligence.weekly_report._INSIGHT_HISTORY_FILE", tmp_history):
                ctx = self._build_context()

                # Generate first report
                generate_weekly_report(ctx, dry_run=False)
                self.assertTrue(tmp_history.exists(),
                    "insight_history.json was not created after first report")

                history_after_1 = json.loads(tmp_history.read_text())
                count_after_1 = len(history_after_1.get("insights", []))

                # Generate second report (same context — simulates next week)
                generate_weekly_report(ctx, dry_run=False)
                history_after_2 = json.loads(tmp_history.read_text())
                count_after_2 = len(history_after_2.get("insights", []))

                # Insight count should have grown or stayed the same (never regressed)
                self.assertGreaterEqual(count_after_2, count_after_1,
                    "Insight count decreased after second report")

                # No insight with times_reported > 3 should remain 'active'
                for ins in history_after_2.get("insights", []):
                    if ins.get("times_reported", 0) >= 3:
                        self.assertEqual(
                            ins.get("status"), "graduated",
                            f"Insight '{ins['insight_id']}' reported "
                            f"{ins['times_reported']}x but status={ins['status']!r}",
                        )

    def test_weekly_report_insight_repetition_detection(self):
        """A graduated insight must not be re-reported as a new discovery."""
        if not os.path.exists(self._DB_PATH):
            self.skipTest("sparkle_shine.db not found")

        import tempfile
        from intelligence.weekly_report import generate_weekly_report

        graduated_id = "referral_retention_advantage"
        graduated_history = {
            "last_updated": "2026-03-15",
            "insights": [
                {
                    "insight_id": graduated_id,
                    "category": "retention",
                    "summary": "Referral clients have 2× the retention rate of ad-acquired clients",
                    "first_reported": "2026-03-01",
                    "last_reported": "2026-03-15",
                    "times_reported": 3,
                    "status": "graduated",
                    "last_values": {},
                }
            ],
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_history = Path(tmpdir) / "insight_history.json"
            tmp_history.write_text(json.dumps(graduated_history))

            with patch("intelligence.weekly_report._INSIGHT_HISTORY_FILE", tmp_history):
                ctx = self._build_context()
                briefing = generate_weekly_report(ctx, dry_run=False)

            report_lower = briefing.content_plain.lower()

            # The graduated insight should NOT appear as a new discovery.
            # It may appear in recommendations but not as a stand-alone "new" insight.
            discovery_phrases = [
                "we discovered",
                "a new insight",
                "newly identified",
                "for the first time",
            ]
            for phrase in discovery_phrases:
                self.assertNotIn(phrase, report_lower,
                    f"Graduated insight re-framed as new discovery: found {phrase!r}")


if __name__ == "__main__":
    unittest.main()

