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


if __name__ == "__main__":
    unittest.main()
