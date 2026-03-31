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
        march = date(2025, 3, 1)
        august = date(2025, 8, 1)
        a = self._res_agreement()
        random.seed(0)
        march_deep = sum(
            1 for _ in range(500) if _pick_job_type(a, march) == "deep_clean"
        ) / 500
        random.seed(0)
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


class TestAddBusinessDays(unittest.TestCase):
    def test_friday_plus_1_is_monday(self):
        friday = date(2026, 3, 27)
        result = _add_business_days(friday, 1)
        self.assertEqual(result, date(2026, 3, 30))  # Monday

    def test_thursday_plus_1_is_friday(self):
        result = _add_business_days(date(2026, 3, 26), 1)
        self.assertEqual(result, date(2026, 3, 27))


# ── NewClientSetupGenerator mock tests ───────────────────────────────────────

class TestNewClientSetupGeneratorRecurring(unittest.TestCase):
    """Tests for NewClientSetupGenerator recurring client path."""

    def _make_db(self):
        """Create an in-memory DB with the required tables."""
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript("""
            CREATE TABLE won_deals (
                canonical_id TEXT PRIMARY KEY,
                client_type TEXT NOT NULL,
                service_frequency TEXT NOT NULL,
                contract_value REAL,
                start_date TEXT NOT NULL,
                crew_assignment TEXT,
                pipedrive_deal_id INTEGER
            );
            CREATE TABLE leads (
                id TEXT PRIMARY KEY,
                first_name TEXT, last_name TEXT, company_name TEXT,
                email TEXT, phone TEXT, lead_type TEXT, source TEXT,
                status TEXT DEFAULT 'new', estimated_value REAL,
                created_at TEXT DEFAULT (datetime('now')),
                last_activity_at TEXT
            );
            CREATE TABLE clients (
                id TEXT PRIMARY KEY, client_type TEXT,
                first_name TEXT, last_name TEXT, company_name TEXT,
                email TEXT, phone TEXT, address TEXT,
                neighborhood TEXT, zone TEXT, status TEXT DEFAULT 'active',
                acquisition_source TEXT, first_service_date TEXT,
                last_service_date TEXT, lifetime_value REAL DEFAULT 0,
                notes TEXT, created_at TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE cross_tool_mapping (
                canonical_id TEXT, tool_name TEXT, tool_specific_id TEXT,
                tool_specific_url TEXT, synced_at TEXT,
                PRIMARY KEY (canonical_id, tool_name)
            );
            CREATE TABLE recurring_agreements (
                id TEXT PRIMARY KEY, client_id TEXT, service_type_id TEXT,
                crew_id TEXT, frequency TEXT, price_per_visit REAL,
                start_date TEXT, end_date TEXT, status TEXT DEFAULT 'active',
                day_of_week TEXT
            );
        """)
        return conn

    @patch("simulation.generators.operations.get_client")
    @patch("simulation.generators.operations.register_mapping")
    @patch("simulation.generators.operations.generate_id")
    @patch("simulation.generators.operations.get_tool_id")
    @patch("simulation.generators.operations._gql")
    def test_recurring_residential_creates_agreement_row(
        self, mock_gql, mock_get_tool_id, mock_gen_id, mock_reg_map, mock_get_client
    ):
        from simulation.generators.operations import NewClientSetupGenerator

        # Setup: one ready residential won deal
        conn = self._make_db()
        conn.execute(
            "INSERT INTO won_deals VALUES (?,?,?,?,?,?,?)",
            ("SS-LEAD-0001", "residential", "biweekly_recurring",
             150.0, "2026-03-01", "Crew A", 999),
        )
        conn.execute(
            "INSERT INTO leads VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            ("SS-LEAD-0001", "Alice", "Smith", None,
             "alice@example.com", "5125550001", "residential",
             "referral", "new", 150.0, "2025-01-01", None),
        )
        conn.commit()

        # No existing Jobber mapping → triggers setup
        mock_get_tool_id.return_value = None

        # _gql returns for: clientCreate, propertyCreate, jobCreate (recurring)
        mock_gql.side_effect = [
            {"data": {"clientCreate": {"client": {"id": "GQL-CLIENT-1"}, "userErrors": []}}},
            {"data": {"propertyCreate": {"properties": [{"id": "GQL-PROP-1"}], "userErrors": []}}},
            {"data": {"__type": {"inputFields": [{"name": "recurrences"}]}}},  # introspect
            {"data": {"jobCreate": {"job": {"id": "GQL-JOB-1"}, "userErrors": []}}},
        ]
        mock_gen_id.side_effect = ["SS-RECUR-9999"]

        # Reset recurrence field cache so introspect _gql call is made
        import simulation.generators.operations as ops_mod
        ops_mod._recurrence_field_checked = False
        ops_mod._recurrence_field_cache = None

        gen = NewClientSetupGenerator(db_path=":memory:")

        # Wrap conn so that close() is a no-op, letting us inspect the DB after execute().
        class _NoCloseConn:
            """Proxy that delegates everything to the real connection except close()."""
            def __init__(self, real):
                self._real = real
            def close(self):
                pass  # intentional no-op
            def __getattr__(self, name):
                return getattr(self._real, name)

        proxy = _NoCloseConn(conn)
        with patch("sqlite3.connect", return_value=proxy):
            result = gen.execute(dry_run=False)

        self.assertTrue(result.success)
        self.assertIn("1", result.message)  # "setup 1 new clients"

        # recurring_agreements row should exist
        row = conn.execute(
            "SELECT * FROM recurring_agreements WHERE id = ?", ("SS-RECUR-9999",)
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["frequency"], "biweekly")
        self.assertEqual(row["price_per_visit"], 150.0)
        self.assertEqual(row["day_of_week"], "monday")  # 2026-03-01 is Sunday → bumped to Monday
        self.assertEqual(row["start_date"], "2026-03-02")  # bumped from Sunday 2026-03-01


class TestNewClientSetupErrorIsolation(unittest.TestCase):
    """Second of three clients fails; first and third still succeed."""

    @patch("simulation.generators.operations.get_client")
    @patch("simulation.generators.operations.register_mapping")
    @patch("simulation.generators.operations.generate_id")
    @patch("simulation.generators.operations.get_tool_id")
    @patch("simulation.generators.operations._gql")
    def test_one_failure_does_not_abort_others(
        self, mock_gql, mock_get_tool_id, mock_gen_id, mock_reg_map, mock_get_client
    ):
        from simulation.generators.operations import NewClientSetupGenerator

        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript("""
            CREATE TABLE won_deals (
                canonical_id TEXT PRIMARY KEY,
                client_type TEXT NOT NULL,
                service_frequency TEXT NOT NULL,
                contract_value REAL,
                start_date TEXT NOT NULL,
                crew_assignment TEXT,
                pipedrive_deal_id INTEGER
            );
            CREATE TABLE leads (
                id TEXT PRIMARY KEY, first_name TEXT, last_name TEXT,
                company_name TEXT, email TEXT, phone TEXT,
                lead_type TEXT, source TEXT, status TEXT DEFAULT 'new',
                estimated_value REAL, created_at TEXT, last_activity_at TEXT
            );
            CREATE TABLE cross_tool_mapping (
                canonical_id TEXT, tool_name TEXT, tool_specific_id TEXT,
                tool_specific_url TEXT, synced_at TEXT,
                PRIMARY KEY (canonical_id, tool_name)
            );
            CREATE TABLE recurring_agreements (
                id TEXT PRIMARY KEY, client_id TEXT, service_type_id TEXT,
                crew_id TEXT, frequency TEXT, price_per_visit REAL,
                start_date TEXT, end_date TEXT, status TEXT DEFAULT 'active',
                day_of_week TEXT
            );
        """)
        for i in (1, 2, 3):
            conn.execute(
                "INSERT INTO won_deals VALUES (?,?,?,?,?,?,?)",
                (f"SS-LEAD-{i:04d}", "residential", "biweekly_recurring",
                 150.0, "2026-03-01", "Crew A", None),
            )
            conn.execute(
                "INSERT INTO leads VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (f"SS-LEAD-{i:04d}", f"Client{i}", "Test", None,
                 f"c{i}@test.com", "5125550000", "residential",
                 "test", "new", 0.0, "2025-01-01", None),
            )
        conn.commit()

        mock_get_tool_id.return_value = None
        mock_gen_id.side_effect = ["SS-RECUR-0001", "SS-RECUR-0003"]

        success_response = [
            {"data": {"clientCreate": {"client": {"id": "GQL-C-OK"}, "userErrors": []}}},
            {"data": {"propertyCreate": {"properties": [{"id": "GQL-P-OK"}], "userErrors": []}}},
            {"data": {"__type": {"inputFields": [{"name": "recurrences"}]}}},
            {"data": {"jobCreate": {"job": {"id": "GQL-J-OK"}, "userErrors": []}}},
        ]
        # Client 1: success (4 calls), Client 2: raises on clientCreate,
        # Client 3: success (3 calls — recurrence field already cached after client 1)
        client3_responses = [
            {"data": {"clientCreate": {"client": {"id": "GQL-C-OK"}, "userErrors": []}}},
            {"data": {"propertyCreate": {"properties": [{"id": "GQL-P-OK"}], "userErrors": []}}},
            {"data": {"jobCreate": {"job": {"id": "GQL-J-OK3"}, "userErrors": []}}},
        ]
        call_count = [0]
        def side_effect(*args, **kwargs):
            call_count[0] += 1
            # Calls 1-4: client 1 (success)
            if call_count[0] <= 4:
                return success_response[call_count[0] - 1]
            # Call 5: client 2 clientCreate → raises
            if call_count[0] == 5:
                raise RuntimeError("Simulated Jobber error for client 2")
            # Calls 6+: client 3 (success, 3 calls — recurrence cached)
            return client3_responses[call_count[0] - 6]

        mock_gql.side_effect = side_effect

        import simulation.generators.operations as ops_mod
        ops_mod._recurrence_field_checked = False
        ops_mod._recurrence_field_cache = None

        gen = NewClientSetupGenerator(db_path=":memory:")

        class _NoCloseConn:
            def __init__(self, c): self._c = c
            def close(self): pass
            def __getattr__(self, name): return getattr(self._c, name)

        with patch("sqlite3.connect", return_value=_NoCloseConn(conn)):
            result = gen.execute(dry_run=False)

        # 2 succeeded, 1 failed → success=False with summary message
        self.assertFalse(result.success)
        self.assertIn("2", result.message)
        self.assertIn("1", result.message)

        # Verify DB writes: clients 1 and 3 committed, client 2 rolled back
        rows = conn.execute(
            "SELECT client_id FROM recurring_agreements ORDER BY client_id"
        ).fetchall()
        committed_ids = [r[0] for r in rows]
        self.assertIn("SS-LEAD-0001", committed_ids)
        self.assertIn("SS-LEAD-0003", committed_ids)
        self.assertNotIn("SS-LEAD-0002", committed_ids)


class TestJobSchedulingGeneratorQueueFn(unittest.TestCase):
    """queue_fn is called with correct fire_at times after creating each job."""

    @patch("simulation.generators.operations.get_client")
    @patch("simulation.generators.operations.register_mapping")
    @patch("simulation.generators.operations.generate_id")
    @patch("simulation.generators.operations.get_tool_id")
    @patch("simulation.generators.operations._gql")
    def test_queue_fn_called_for_each_due_job(
        self, mock_gql, mock_get_tool_id, mock_gen_id, mock_reg_map, mock_get_client
    ):
        from simulation.generators.operations import JobSchedulingGenerator

        # Monday 2026-03-30 — weekly agreement due
        today = date(2026, 3, 30)

        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript("""
            CREATE TABLE recurring_agreements (
                id TEXT PRIMARY KEY, client_id TEXT, service_type_id TEXT,
                crew_id TEXT, frequency TEXT, price_per_visit REAL,
                start_date TEXT, end_date TEXT, status TEXT DEFAULT 'active',
                day_of_week TEXT
            );
            CREATE TABLE jobs (
                id TEXT PRIMARY KEY, client_id TEXT, crew_id TEXT,
                service_type_id TEXT, scheduled_date TEXT, scheduled_time TEXT,
                duration_minutes_actual INTEGER,
                status TEXT DEFAULT 'scheduled', address TEXT, notes TEXT,
                review_requested INTEGER DEFAULT 0, completed_at TEXT
            );
            CREATE TABLE cross_tool_mapping (
                canonical_id TEXT, tool_name TEXT, tool_specific_id TEXT,
                tool_specific_url TEXT, synced_at TEXT,
                PRIMARY KEY (canonical_id, tool_name)
            );
        """)
        # Two weekly agreements both due on Monday
        conn.execute(
            "INSERT INTO recurring_agreements VALUES (?,?,?,?,?,?,?,?,?,?)",
            ("SS-RECUR-0001", "SS-LEAD-0001", "recurring-weekly",
             "crew-a", "weekly", 135.0, "2026-03-23", None, "active", "monday"),
        )
        conn.execute(
            "INSERT INTO recurring_agreements VALUES (?,?,?,?,?,?,?,?,?,?)",
            ("SS-RECUR-0002", "SS-LEAD-0002", "recurring-biweekly",
             "crew-b", "biweekly", 150.0, "2026-03-16", None, "active", "monday"),
        )
        conn.commit()

        # get_tool_id: return property IDs for both clients
        mock_get_tool_id.side_effect = lambda cid, tool, **kw: (
            "GQL-PROP-" + cid[-4:] if tool == "jobber_property" else None
        )
        mock_gen_id.side_effect = ["SS-JOB-9001", "SS-JOB-9002"]
        mock_gql.return_value = {
            "data": {"jobCreate": {"job": {"id": "GQL-JOB-X"}, "userErrors": []}}
        }

        queue_calls = []
        def fake_queue(fire_at, generator_name, kwargs):
            queue_calls.append((fire_at, generator_name, kwargs))

        gen = JobSchedulingGenerator(db_path=":memory:", queue_fn=fake_queue)

        class _NoCloseConn:
            def __init__(self, c): self._c = c
            def close(self): pass
            def __getattr__(self, name): return getattr(self._c, name)

        with patch("sqlite3.connect", return_value=_NoCloseConn(conn)):
            with patch("simulation.generators.operations.date") as mock_date:
                mock_date.today.return_value = today
                mock_date.fromisoformat.side_effect = date.fromisoformat
                result = gen.execute(dry_run=False)

        self.assertTrue(result.success)
        # Two jobs queued (one per due agreement)
        self.assertEqual(len(queue_calls), 2)
        for fire_at, gen_name, kwargs in queue_calls:
            self.assertEqual(gen_name, "job_completion")
            self.assertIn("job_id", kwargs)
            self.assertIsInstance(fire_at, datetime)
            # fire_at should be after window start (7 AM on 2026-03-30)
            self.assertGreater(fire_at, datetime(2026, 3, 30, 7, 0))


class TestJobSchedulingGeneratorPass2(unittest.TestCase):
    """Pre-existing scheduled jobs get completion events queued (Pass 2)."""

    def test_existing_job_queued_without_creating_new(self):
        from simulation.generators.operations import JobSchedulingGenerator

        today = date(2026, 3, 30)

        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript("""
            CREATE TABLE recurring_agreements (
                id TEXT PRIMARY KEY, client_id TEXT, service_type_id TEXT,
                crew_id TEXT, frequency TEXT, price_per_visit REAL,
                start_date TEXT, end_date TEXT, status TEXT DEFAULT 'active',
                day_of_week TEXT
            );
            CREATE TABLE jobs (
                id TEXT PRIMARY KEY, client_id TEXT, crew_id TEXT,
                service_type_id TEXT, scheduled_date TEXT, scheduled_time TEXT,
                duration_minutes_actual INTEGER,
                status TEXT DEFAULT 'scheduled', address TEXT, notes TEXT,
                review_requested INTEGER DEFAULT 0, completed_at TEXT
            );
            CREATE TABLE cross_tool_mapping (
                canonical_id TEXT, tool_name TEXT, tool_specific_id TEXT,
                tool_specific_url TEXT, synced_at TEXT,
                PRIMARY KEY (canonical_id, tool_name)
            );
        """)
        # No recurring agreements — Pass 1 creates nothing
        # Pre-existing scheduled job for today
        conn.execute(
            "INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            ("SS-JOB-EXIST", "SS-CLIENT-0001", "crew-a", "recurring-weekly",
             "2026-03-30", "07:00", None, "scheduled", None, None, 0, None),
        )
        conn.commit()

        queue_calls = []
        def fake_queue(fire_at, generator_name, kwargs):
            queue_calls.append((fire_at, generator_name, kwargs))

        gen = JobSchedulingGenerator(db_path=":memory:", queue_fn=fake_queue)

        class _NoCloseConn:
            def __init__(self, c): self._c = c
            def close(self): pass
            def __getattr__(self, name): return getattr(self._c, name)

        with patch("sqlite3.connect", return_value=_NoCloseConn(conn)):
            with patch("simulation.generators.operations.date") as mock_date:
                mock_date.today.return_value = today
                mock_date.fromisoformat.side_effect = date.fromisoformat
                result = gen.execute(dry_run=True)

        self.assertTrue(result.success)
        # Pass 2 should have queued the pre-existing job
        self.assertEqual(len(queue_calls), 1)
        self.assertEqual(queue_calls[0][1], "job_completion")
        self.assertEqual(queue_calls[0][2]["job_id"], "SS-JOB-EXIST")
        self.assertGreater(queue_calls[0][0], datetime(2026, 3, 30, 7, 0))


class TestJobCompletionGeneratorCompleted(unittest.TestCase):
    """completed outcome: jobs updated, review inserted."""

    def _make_db_with_job(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript("""
            CREATE TABLE jobs (
                id TEXT PRIMARY KEY, client_id TEXT, crew_id TEXT,
                service_type_id TEXT, scheduled_date TEXT, scheduled_time TEXT,
                duration_minutes_actual INTEGER,
                status TEXT DEFAULT 'scheduled', address TEXT, notes TEXT,
                review_requested INTEGER DEFAULT 0, completed_at TEXT
            );
            CREATE TABLE clients (
                id TEXT PRIMARY KEY, client_type TEXT,
                first_name TEXT, last_name TEXT, company_name TEXT,
                email TEXT, phone TEXT, address TEXT,
                neighborhood TEXT, zone TEXT, status TEXT DEFAULT 'active',
                acquisition_source TEXT, first_service_date TEXT,
                last_service_date TEXT, lifetime_value REAL DEFAULT 0,
                notes TEXT, created_at TEXT,
                churn_risk TEXT DEFAULT 'normal'
            );
            CREATE TABLE reviews (
                id TEXT PRIMARY KEY, client_id TEXT, job_id TEXT,
                rating INTEGER, review_text TEXT, platform TEXT,
                review_date TEXT, response_text TEXT, response_date TEXT
            );
            CREATE TABLE cross_tool_mapping (
                canonical_id TEXT, tool_name TEXT, tool_specific_id TEXT,
                tool_specific_url TEXT, synced_at TEXT,
                PRIMARY KEY (canonical_id, tool_name)
            );
        """)
        conn.execute(
            "INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            ("SS-JOB-0001", "SS-LEAD-0001", "crew-a", "recurring-weekly",
             "2026-03-30", "07:00", None, "scheduled", None, None, 0, None),
        )
        conn.execute(
            "INSERT INTO clients VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("SS-LEAD-0001", "residential", "Alice", "Smith", None,
             "a@test.com", None, None, None, None, "active",
             None, None, None, 0.0, None, "2025-01-01", "normal"),
        )
        conn.execute(
            "INSERT INTO cross_tool_mapping VALUES (?,?,?,?,?)",
            ("SS-JOB-0001", "jobber", "GQL-JOB-0001", None, "2026-03-30"),
        )
        conn.commit()
        return conn

    @patch("simulation.generators.operations.get_client")
    @patch("simulation.generators.operations.register_mapping")
    @patch("simulation.generators.operations.generate_id")
    @patch("simulation.generators.operations.get_tool_id")
    @patch("simulation.generators.operations._gql")
    def test_completed_updates_job_and_inserts_review(
        self, mock_gql, mock_get_tool_id, mock_gen_id, mock_reg_map, mock_get_client
    ):
        from simulation.generators.operations import JobCompletionGenerator

        conn = self._make_db_with_job()
        mock_get_tool_id.return_value = "GQL-JOB-0001"
        mock_gen_id.return_value = "SS-REV-0001"
        mock_gql.return_value = {
            "data": {"jobClose": {"job": {"id": "GQL-JOB-0001", "jobStatus": "COMPLETED"}, "userErrors": []}}
        }

        gen = JobCompletionGenerator(db_path=":memory:")

        class _NoCloseConn:
            def __init__(self, c): self._c = c
            def close(self): pass
            def __getattr__(self, name): return getattr(self._c, name)

        with patch("sqlite3.connect", return_value=_NoCloseConn(conn)):
            with patch("random.random", return_value=0.01):  # force 'completed' outcome
                result = gen.execute(dry_run=False, job_id="SS-JOB-0001")

        self.assertTrue(result.success)

        job = dict(conn.execute("SELECT * FROM jobs WHERE id = ?", ("SS-JOB-0001",)).fetchone())
        self.assertEqual(job["status"], "completed")
        self.assertIsNotNone(job["duration_minutes_actual"])
        self.assertIsNotNone(job["completed_at"])

        review = conn.execute("SELECT * FROM reviews WHERE job_id = ?", ("SS-JOB-0001",)).fetchone()
        self.assertIsNotNone(review)
        self.assertEqual(dict(review)["platform"], "internal")
        self.assertIn(dict(review)["rating"], (1, 2, 3, 4, 5))

    def test_completed_skips_review_in_dry_run(self):
        from simulation.generators.operations import JobCompletionGenerator
        conn = self._make_db_with_job()

        gen = JobCompletionGenerator(db_path=":memory:")

        class _NoCloseConn:
            def __init__(self, c): self._c = c
            def close(self): pass
            def __getattr__(self, name): return getattr(self._c, name)

        with patch("sqlite3.connect", return_value=_NoCloseConn(conn)):
            result = gen.execute(dry_run=True, job_id="SS-JOB-0001")

        # dry_run: no API calls, no writes, no review
        review = conn.execute("SELECT * FROM reviews WHERE job_id = ?", ("SS-JOB-0001",)).fetchone()
        self.assertIsNone(review)


class TestJobCompletionGeneratorOutcomes(unittest.TestCase):
    """cancelled, no-show, rescheduled outcome paths."""

    def _make_conn(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript("""
            CREATE TABLE jobs (
                id TEXT PRIMARY KEY, client_id TEXT, crew_id TEXT,
                service_type_id TEXT, scheduled_date TEXT, scheduled_time TEXT,
                duration_minutes_actual INTEGER,
                status TEXT DEFAULT 'scheduled', address TEXT, notes TEXT,
                review_requested INTEGER DEFAULT 0, completed_at TEXT
            );
            CREATE TABLE clients (
                id TEXT PRIMARY KEY, client_type TEXT,
                first_name TEXT, last_name TEXT, company_name TEXT,
                email TEXT, phone TEXT, address TEXT,
                neighborhood TEXT, zone TEXT, status TEXT DEFAULT 'active',
                acquisition_source TEXT, first_service_date TEXT,
                last_service_date TEXT, lifetime_value REAL DEFAULT 0,
                notes TEXT, created_at TEXT,
                churn_risk TEXT DEFAULT 'normal'
            );
            CREATE TABLE reviews (
                id TEXT PRIMARY KEY, client_id TEXT, job_id TEXT,
                rating INTEGER, review_text TEXT, platform TEXT,
                review_date TEXT, response_text TEXT, response_date TEXT
            );
            CREATE TABLE cross_tool_mapping (
                canonical_id TEXT, tool_name TEXT, tool_specific_id TEXT,
                tool_specific_url TEXT, synced_at TEXT,
                PRIMARY KEY (canonical_id, tool_name)
            );
        """)
        conn.execute(
            "INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            ("SS-JOB-0001", "SS-LEAD-0001", "crew-b", "recurring-biweekly",
             "2026-03-30", "08:00", None, "scheduled", None, None, 0, None),
        )
        conn.execute(
            "INSERT INTO clients VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("SS-LEAD-0001", "residential", "Bob", "Jones", None,
             "b@test.com", None, None, None, None, "active",
             None, None, None, 0.0, None, "2025-01-01", "normal"),
        )
        conn.execute(
            "INSERT INTO cross_tool_mapping VALUES (?,?,?,?,?)",
            ("SS-JOB-0001", "jobber", "GQL-JOB-0001", None, "2026-03-30"),
        )
        conn.commit()
        return conn

    def _no_close(self, conn):
        class _NoCloseConn:
            def __init__(self, c): self._c = c
            def close(self): pass
            def __getattr__(self, name): return getattr(self._c, name)
        return _NoCloseConn(conn)

    @patch("simulation.generators.operations.get_client")
    @patch("simulation.generators.operations.get_tool_id")
    @patch("simulation.generators.operations._gql")
    def test_cancelled_sets_status(self, mock_gql, mock_get_tool_id, mock_get_client):
        from simulation.generators.operations import JobCompletionGenerator
        conn = self._make_conn()
        mock_get_tool_id.return_value = "GQL-JOB-0001"
        mock_gql.return_value = {
            "data": {"jobClose": {"job": {"id": "GQL-JOB-0001"}, "userErrors": []}}
        }
        gen = JobCompletionGenerator(db_path=":memory:")
        with patch("sqlite3.connect", return_value=self._no_close(conn)):
            # roll lands in cancelled window: 0.92 <= x < 0.95
            with patch("random.random", return_value=0.93):
                result = gen.execute(dry_run=False, job_id="SS-JOB-0001")
        self.assertTrue(result.success)
        job = dict(conn.execute("SELECT * FROM jobs WHERE id = ?", ("SS-JOB-0001",)).fetchone())
        self.assertEqual(job["status"], "cancelled")

    @patch("simulation.generators.operations.get_client")
    @patch("simulation.generators.operations.get_tool_id")
    @patch("simulation.generators.operations._gql")
    def test_noshow_sets_status(self, mock_gql, mock_get_tool_id, mock_get_client):
        from simulation.generators.operations import JobCompletionGenerator
        conn = self._make_conn()
        mock_get_tool_id.return_value = "GQL-JOB-0001"
        mock_gql.return_value = {
            "data": {"jobClose": {"job": {"id": "GQL-JOB-0001"}, "userErrors": []}}
        }
        gen = JobCompletionGenerator(db_path=":memory:")
        with patch("sqlite3.connect", return_value=self._no_close(conn)):
            # no-show window: 0.95 <= x < 0.97
            with patch("random.random", return_value=0.96):
                result = gen.execute(dry_run=False, job_id="SS-JOB-0001")
        self.assertTrue(result.success)
        job = dict(conn.execute("SELECT * FROM jobs WHERE id = ?", ("SS-JOB-0001",)).fetchone())
        self.assertEqual(job["status"], "no-show")

    @patch("simulation.generators.operations.get_client")
    @patch("simulation.generators.operations.register_mapping")
    @patch("simulation.generators.operations.generate_id")
    @patch("simulation.generators.operations.get_tool_id")
    @patch("simulation.generators.operations._gql")
    def test_rescheduled_creates_new_job_for_tomorrow(
        self, mock_gql, mock_get_tool_id, mock_gen_id, mock_reg_map, mock_get_client
    ):
        from simulation.generators.operations import JobCompletionGenerator
        conn = self._make_conn()
        mock_get_tool_id.side_effect = lambda cid, tool, **kw: (
            "GQL-JOB-0001" if tool == "jobber" else "GQL-PROP-0001"
        )
        mock_gen_id.return_value = "SS-JOB-9999"
        mock_gql.side_effect = [
            {"data": {"jobClose": {"job": {"id": "GQL-JOB-0001"}, "userErrors": []}}},
            {"data": {"jobCreate": {"job": {"id": "GQL-JOB-9999"}, "userErrors": []}}},
        ]
        gen = JobCompletionGenerator(db_path=":memory:")
        with patch("sqlite3.connect", return_value=self._no_close(conn)):
            # rescheduled window: x >= 0.97
            with patch("random.random", return_value=0.98):
                result = gen.execute(dry_run=False, job_id="SS-JOB-0001")

        self.assertTrue(result.success)
        # Original job cancelled
        orig = dict(conn.execute("SELECT * FROM jobs WHERE id = ?", ("SS-JOB-0001",)).fetchone())
        self.assertEqual(orig["status"], "cancelled")
        # New job exists with status=scheduled
        new_job = conn.execute("SELECT * FROM jobs WHERE id = ?", ("SS-JOB-9999",)).fetchone()
        self.assertIsNotNone(new_job)
        self.assertEqual(dict(new_job)["status"], "scheduled")

    @patch("simulation.generators.operations.get_client")
    @patch("simulation.generators.operations.get_tool_id")
    @patch("simulation.generators.operations._gql")
    def test_churn_risk_set_to_high_after_3_cancellations(
        self, mock_gql, mock_get_tool_id, mock_get_client
    ):
        from simulation.generators.operations import JobCompletionGenerator
        conn = self._make_conn()
        # Pre-populate 2 existing cancellations in the last 60 days
        conn.execute("""
            INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, ("SS-JOB-OLD1", "SS-LEAD-0001", "crew-b", "recurring-biweekly",
              "2026-02-15", None, None, "cancelled", None, None, 0, None))
        conn.execute("""
            INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, ("SS-JOB-OLD2", "SS-LEAD-0001", "crew-b", "recurring-biweekly",
              "2026-03-01", None, None, "cancelled", None, None, 0, None))
        conn.commit()

        mock_get_tool_id.return_value = "GQL-JOB-0001"
        mock_gql.return_value = {
            "data": {"jobClose": {"job": {"id": "GQL-JOB-0001"}, "userErrors": []}}
        }
        gen = JobCompletionGenerator(db_path=":memory:")
        with patch("sqlite3.connect", return_value=self._no_close(conn)):
            with patch("random.random", return_value=0.93):  # cancelled
                gen.execute(dry_run=False, job_id="SS-JOB-0001")

        client = dict(conn.execute(
            "SELECT churn_risk FROM clients WHERE id = ?", ("SS-LEAD-0001",)
        ).fetchone())
        self.assertEqual(client["churn_risk"], "high")

    @patch("simulation.generators.operations.get_client")
    @patch("simulation.generators.operations.get_tool_id")
    @patch("simulation.generators.operations._gql")
    def test_churn_risk_not_set_for_only_2_cancellations(
        self, mock_gql, mock_get_tool_id, mock_get_client
    ):
        from simulation.generators.operations import JobCompletionGenerator
        conn = self._make_conn()
        # Only 1 prior cancellation in 60 days (total will be 2 after this)
        conn.execute("""
            INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, ("SS-JOB-OLD1", "SS-LEAD-0001", "crew-b", "recurring-biweekly",
              "2026-03-01", None, None, "cancelled", None, None, 0, None))
        conn.commit()

        mock_get_tool_id.return_value = "GQL-JOB-0001"
        mock_gql.return_value = {
            "data": {"jobClose": {"job": {"id": "GQL-JOB-0001"}, "userErrors": []}}
        }
        gen = JobCompletionGenerator(db_path=":memory:")
        with patch("sqlite3.connect", return_value=self._no_close(conn)):
            with patch("random.random", return_value=0.93):  # cancelled
                gen.execute(dry_run=False, job_id="SS-JOB-0001")

        client = dict(conn.execute(
            "SELECT churn_risk FROM clients WHERE id = ?", ("SS-LEAD-0001",)
        ).fetchone())
        self.assertNotEqual(client["churn_risk"], "high")


# ── Integration tests (require RUN_INTEGRATION=1) ────────────────────────────

@unittest.skipUnless(os.getenv("RUN_INTEGRATION"), "Skipping integration tests")
class TestOperationsIntegration(unittest.TestCase):
    """Live API tests — require real Jobber credentials and sparkle_shine.db."""

    DB_PATH = "sparkle_shine.db"

    def test_jobber_client_create_and_job_create_roundtrip(self):
        """NewClientSetupGenerator creates a real Jobber client and job."""
        from simulation.generators.operations import NewClientSetupGenerator
        from database.mappings import get_tool_id
        import sqlite3

        # Find a ready won deal that has no Jobber mapping yet
        conn = sqlite3.connect(self.DB_PATH)
        conn.row_factory = sqlite3.Row
        ready = conn.execute("""
            SELECT canonical_id FROM won_deals
            WHERE start_date <= date('now')
              AND canonical_id NOT IN (
                  SELECT canonical_id FROM cross_tool_mapping WHERE tool_name = 'jobber'
              )
            LIMIT 1
        """).fetchone()
        conn.close()

        if not ready:
            self.skipTest("No ready won deals without Jobber mapping")

        canonical_id = ready["canonical_id"]
        gen = NewClientSetupGenerator(db_path=self.DB_PATH)
        result = gen.execute(dry_run=False)

        self.assertTrue(result.success, f"Setup failed: {result.message}")
        jobber_client_id = get_tool_id(canonical_id, "jobber", db_path=self.DB_PATH)
        self.assertIsNotNone(jobber_client_id)
        self.assertTrue(jobber_client_id.startswith("Z2lkOi"),
            "Jobber IDs are base64-encoded Global IDs starting with Z2lkOi")

    def test_rescheduled_job_picked_up_next_day(self):
        """Rescheduled job appears in JobSchedulingGenerator pass-2 on its new date."""
        from simulation.generators.operations import JobCompletionGenerator, JobSchedulingGenerator
        import sqlite3

        # Find a scheduled job today to reschedule
        conn = sqlite3.connect(self.DB_PATH)
        conn.row_factory = sqlite3.Row
        job = conn.execute("""
            SELECT j.id FROM jobs j
            JOIN cross_tool_mapping m ON j.id = m.canonical_id AND m.tool_name = 'jobber'
            WHERE j.status = 'scheduled'
              AND j.scheduled_date = date('now')
            LIMIT 1
        """).fetchone()
        conn.close()

        if not job:
            self.skipTest("No scheduled jobs today with Jobber mappings")

        job_id = job["id"]

        # Force rescheduled outcome
        with patch("random.random", return_value=0.98):
            completion_gen = JobCompletionGenerator(db_path=self.DB_PATH)
            result = completion_gen.execute(dry_run=False, job_id=job_id)

        self.assertTrue(result.success)
        self.assertIn("rescheduled", result.message)

        # Verify new job exists in SQLite for tomorrow
        from datetime import date, timedelta
        tomorrow = (date.today() + timedelta(days=1 if date.today().weekday() < 4 else 3)).isoformat()
        conn2 = sqlite3.connect(self.DB_PATH)
        conn2.row_factory = sqlite3.Row
        new_job = conn2.execute("""
            SELECT id FROM jobs
            WHERE client_id = (SELECT client_id FROM jobs WHERE id = ?)
              AND scheduled_date = ?
              AND status = 'scheduled'
              AND id != ?
        """, (job_id, tomorrow, job_id)).fetchone()
        conn2.close()
        self.assertIsNotNone(new_job, "Rescheduled job not found in SQLite for tomorrow")


# ── Bug-fix regression tests ──────────────────────────────────────────────────

class TestJobSchedulingSkipsWhenPropertyIdIsNone(unittest.TestCase):
    """Jobber rejects propertyId=''. Generator must skip — not pass empty string."""

    def setUp(self):
        import tempfile, os
        self._tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self._tmp.close()
        self._db_path = self._tmp.name
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS recurring_agreements (
                id TEXT PRIMARY KEY, client_id TEXT, service_type_id TEXT,
                crew_id TEXT, frequency TEXT, price_per_visit REAL,
                start_date TEXT, end_date TEXT, status TEXT DEFAULT 'active',
                day_of_week TEXT, client_type TEXT
            );
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY, client_id TEXT, crew_id TEXT,
                service_type_id TEXT, scheduled_date TEXT,
                scheduled_time TEXT, status TEXT
            );
            CREATE TABLE IF NOT EXISTS cross_tool_mapping (
                canonical_id TEXT, tool_name TEXT, tool_specific_id TEXT,
                tool_specific_url TEXT, entity_type TEXT, synced_at TEXT,
                PRIMARY KEY (canonical_id, tool_name)
            );
        """)
        today = date.today()
        dow_name = today.strftime("%A").lower()
        conn.execute(
            "INSERT INTO recurring_agreements VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            ("SS-RECUR-0001", "SS-CLIENT-0001", "recurring-biweekly",
             "crew-a", "weekly", 150.0, today.isoformat(), None, "active",
             dow_name, "residential"),
        )
        conn.commit()
        conn.close()

    def tearDown(self):
        import os
        os.unlink(self._db_path)

    @patch("simulation.generators.operations._get_or_fetch_property_id", return_value=None)
    @patch("simulation.generators.operations._gql")
    @patch("simulation.generators.operations.get_client")
    def test_no_gql_job_create_when_property_id_is_none(
        self, mock_get_client, mock_gql, mock_prop_id
    ):
        from simulation.generators.operations import JobSchedulingGenerator

        gen = JobSchedulingGenerator(db_path=self._db_path)
        gen.execute(dry_run=False)

        # _gql must NOT have been called with _JOB_CREATE input (job containing propertyId)
        for call_args in mock_gql.call_args_list:
            args = call_args[0]
            if len(args) >= 3 and isinstance(args[2], dict) and "input" in args[2]:
                self.fail("_gql was called with job input despite property_id being None")


class TestRegisterMappingUsesConnectionTimeout(unittest.TestCase):
    """register_mapping must open SQLite with a timeout so concurrent writes retry.

    NOTE: This test was written before the PostgreSQL migration. The production
    get_connection() now uses psycopg2 (not sqlite3.connect), so the original
    SQLite timeout assertion is no longer applicable. Skipped post-migration.
    """

    @unittest.skip("Pre-PostgreSQL migration test; get_connection() now uses psycopg2")
    def test_get_connection_uses_nonzero_timeout(self):
        import sqlite3 as real_sqlite3
        from unittest.mock import patch, MagicMock
        captured = {}

        original_connect = real_sqlite3.connect

        def mock_connect(path, **kwargs):
            captured["timeout"] = kwargs.get("timeout", 0)
            conn = original_connect(":memory:", **kwargs)
            conn.row_factory = real_sqlite3.Row
            return conn

        with patch("database.schema.sqlite3.connect", side_effect=mock_connect):
            from database.schema import get_connection
            get_connection(":memory:")

        self.assertGreater(
            captured.get("timeout", 0), 0,
            "get_connection must pass timeout > 0 to sqlite3.connect to handle concurrent writers",
        )


class TestCompleteAsanaTaskArgumentOrder(unittest.TestCase):
    """update_task(body, task_gid, opts) — the SDK's signature.
    Previous code passed (task_gid, body, opts) which caused 'Not a Long' errors.
    """

    @patch("simulation.generators.tasks.get_tool_id")
    @patch("simulation.generators.tasks.get_client")
    def test_update_task_receives_body_as_first_arg(self, mock_get_client, mock_get_tool_id):
        import asana
        from simulation.generators.tasks import TaskCompletionGenerator

        mock_tasks_api = MagicMock()
        mock_api_client = MagicMock()
        mock_get_client.return_value = mock_api_client

        with patch("simulation.generators.tasks.asana.TasksApi", return_value=mock_tasks_api):
            gen = TaskCompletionGenerator.__new__(TaskCompletionGenerator)
            gen.db_path = ":memory:"
            gen.logger = MagicMock()
            gen._complete_asana_task("1234567890123456")

        mock_tasks_api.update_task.assert_called_once()
        call_args = mock_tasks_api.update_task.call_args[0]
        body_arg = call_args[0]
        gid_arg  = call_args[1]

        self.assertIsInstance(
            body_arg, dict,
            f"First arg to update_task must be the body dict, got: {type(body_arg).__name__}",
        )
        self.assertEqual(
            gid_arg, "1234567890123456",
            f"Second arg to update_task must be the task GID string, got: {gid_arg!r}",
        )
