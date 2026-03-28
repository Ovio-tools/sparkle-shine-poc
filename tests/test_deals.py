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


class TestEnsureSchema(unittest.TestCase):

    def test_adds_missing_columns_to_commercial_proposals(self):
        from simulation.generators.deals import DealGenerator
        gen = _bare_gen()
        conn = sqlite3.connect(":memory:")
        conn.execute(
            "CREATE TABLE commercial_proposals "
            "(id TEXT PRIMARY KEY, title TEXT, status TEXT DEFAULT 'draft')"
        )
        gen._ensure_schema(conn)
        cols = {row[1] for row in conn.execute("PRAGMA table_info(commercial_proposals)")}
        self.assertIn("start_date", cols)
        self.assertIn("crew_assignment", cols)
        self.assertIn("stage_change_time", cols)
        conn.close()

    def test_idempotent_on_second_call(self):
        """Running _ensure_schema twice must not raise."""
        from simulation.generators.deals import DealGenerator
        gen = _bare_gen()
        conn = sqlite3.connect(":memory:")
        conn.execute(
            "CREATE TABLE commercial_proposals "
            "(id TEXT PRIMARY KEY, title TEXT, status TEXT DEFAULT 'draft')"
        )
        gen._ensure_schema(conn)
        gen._ensure_schema(conn)  # second call — should not raise
        conn.close()


class TestInit(unittest.TestCase):

    def setUp(self):
        self._fd, self._db_path = tempfile.mkstemp(suffix=".db")
        from database.schema import init_db
        init_db(self._db_path)

    def tearDown(self):
        os.close(self._fd)
        os.unlink(self._db_path)

    def test_loads_stage_ids_from_tool_ids_json(self):
        from simulation.generators.deals import DealGenerator
        gen = DealGenerator(db_path=self._db_path)
        # Values from config/tool_ids.json
        self.assertEqual(gen._won_stage_id, 12)
        self.assertEqual(gen._lost_stage_id, 13)
        self.assertEqual(len(gen._stage_order), 6)
        self.assertIn(7, gen._stage_order)   # New Lead
        self.assertIn(11, gen._stage_order)  # Negotiation
        self.assertEqual(gen._stage_order[-1], 12)  # Closed Won is last

    def test_schema_columns_added_on_init(self):
        from simulation.generators.deals import DealGenerator
        DealGenerator(db_path=self._db_path)
        conn = sqlite3.connect(self._db_path)
        cols = {row[1] for row in conn.execute("PRAGMA table_info(commercial_proposals)")}
        conn.close()
        self.assertIn("start_date", cols)
        self.assertIn("crew_assignment", cols)
        self.assertIn("stage_change_time", cols)


class TestPickDeal(unittest.TestCase):

    def setUp(self):
        self._fd, self._db_path = tempfile.mkstemp(suffix=".db")
        from database.schema import init_db
        init_db(self._db_path)
        from simulation.generators.deals import DealGenerator
        self.gen = DealGenerator(db_path=self._db_path)

    def tearDown(self):
        os.close(self._fd)
        os.unlink(self._db_path)

    def _mock_pipedrive(self, deals):
        mc = MagicMock()
        mc.get.return_value.raise_for_status = MagicMock()
        mc.get.return_value.json.return_value = {"data": deals}
        return mc

    def test_returns_none_when_no_open_deals(self):
        with patch("simulation.generators.deals.get_client", return_value=self._mock_pipedrive(None)):
            with patch("time.sleep"):
                result = self.gen._pick_deal()
        self.assertIsNone(result)

    def test_returns_one_deal_from_list(self):
        deals = [{"id": i, "stage_id": 8} for i in range(1, 6)]
        with patch("simulation.generators.deals.get_client", return_value=self._mock_pipedrive(deals)):
            with patch("time.sleep"):
                result = self.gen._pick_deal()
        self.assertIn(result, deals)

    def test_uniform_selection_over_500_trials(self):
        """Each of 5 deals should be picked ~100 times; assert within [50, 150]."""
        deals = [{"id": i, "stage_id": 8} for i in range(1, 6)]
        counts = {i: 0 for i in range(1, 6)}
        random.seed(0)
        with patch("simulation.generators.deals.get_client", return_value=self._mock_pipedrive(deals)):
            with patch("time.sleep"):
                for _ in range(500):
                    d = self.gen._pick_deal()
                    counts[d["id"]] += 1
        for deal_id, count in counts.items():
            self.assertGreater(count, 50, f"deal {deal_id} picked only {count} times")
            self.assertLess(count, 150, f"deal {deal_id} picked {count} times (too often)")


class TestLogActivity(unittest.TestCase):

    def setUp(self):
        self._fd, self._db_path = tempfile.mkstemp(suffix=".db")
        from database.schema import init_db
        init_db(self._db_path)
        from simulation.generators.deals import DealGenerator
        self.gen = DealGenerator(db_path=self._db_path)

    def tearDown(self):
        os.close(self._fd)
        os.unlink(self._db_path)

    def test_posts_note_activity_to_pipedrive(self):
        mc = MagicMock()
        mc.post.return_value.status_code = 201
        with patch("simulation.generators.deals.get_client", return_value=mc):
            with patch("time.sleep"):
                self.gen._log_activity(deal_id=42, note="Stage updated", dry_run=False)
        mc.post.assert_called_once_with(
            "https://api.pipedrive.com/v1/activities",
            json={
                "deal_id": 42,
                "subject": "Stage update",
                "type": "note",
                "note": "Stage updated",
                "done": 1,
            },
        )

    def test_dry_run_skips_post(self):
        mc = MagicMock()
        with patch("simulation.generators.deals.get_client", return_value=mc):
            self.gen._log_activity(deal_id=42, note="Stage updated", dry_run=True)
        mc.post.assert_not_called()


class TestAdvanceDeal(unittest.TestCase):
    """Seed random, mock Pipedrive PUT. No SQLite fixture needed for these cases."""

    def setUp(self):
        self._fd, self._db_path = tempfile.mkstemp(suffix=".db")
        from database.schema import init_db
        init_db(self._db_path)
        from simulation.generators.deals import DealGenerator
        self.gen = DealGenerator(db_path=self._db_path)
        # Deal in Qualified (stage_id=8), 5 days in stage
        now = datetime.utcnow()
        sct = (now - timedelta(days=5)).strftime("%Y-%m-%d %H:%M:%S")
        self.deal = {"id": 100, "stage_id": 8, "stage_change_time": sct, "update_time": sct}

    def tearDown(self):
        os.close(self._fd)
        os.unlink(self._db_path)

    def _mock_client(self, put_status=200, post_status=201):
        mc = MagicMock()
        mc.put.return_value.status_code = put_status
        mc.post.return_value.status_code = post_status
        return mc

    def test_advance_fires_and_puts_next_stage(self):
        """random.random()=0.01 → advance fires → PUT with stage_id=9 (Site Visit)."""
        mc = self._mock_client()
        with patch("simulation.generators.deals.get_client", return_value=mc):
            with patch("time.sleep"):
                with patch("random.random", return_value=0.01):
                    result = self.gen._advance_deal(self.deal, dry_run=False)

        self.assertTrue(result.success)
        mc.put.assert_called_once_with(
            "https://api.pipedrive.com/v1/deals/100",
            json={"stage_id": 9},  # Site Visit Scheduled
        )

    def test_no_advance_no_loss_returns_no_change(self):
        """random.random()=0.99 → neither roll fires → no PUT, message='no change'."""
        mc = self._mock_client()
        with patch("simulation.generators.deals.get_client", return_value=mc):
            with patch("time.sleep"):
                with patch("random.random", return_value=0.99):
                    result = self.gen._advance_deal(self.deal, dry_run=False)

        self.assertTrue(result.success)
        self.assertEqual(result.message, "no change")
        mc.put.assert_not_called()

    def test_loss_fires_when_advance_misses(self):
        """First roll=0.99 misses advance, second roll=0.01 fires loss → PUT status=lost."""
        mc = self._mock_client()
        call_count = [0]

        def _alternating():
            call_count[0] += 1
            return 0.01 if call_count[0] == 2 else 0.99

        with patch("simulation.generators.deals.get_client", return_value=mc):
            with patch("time.sleep"):
                with patch("random.random", side_effect=_alternating):
                    result = self.gen._advance_deal(self.deal, dry_run=False)

        self.assertTrue(result.success)
        self.assertIn("lost", result.message)
        mc.put.assert_called_once_with(
            "https://api.pipedrive.com/v1/deals/100",
            json={"status": "lost", "stage_id": 13},  # Closed Lost
        )

    def test_advance_to_won_calls_complete_won_deal(self):
        """Deal in Negotiation (stage_id=11) + advance fires → _complete_won_deal called once."""
        now = datetime.utcnow()
        sct = (now - timedelta(days=5)).strftime("%Y-%m-%d %H:%M:%S")
        negotiation_deal = {
            "id": 200,
            "stage_id": 11,  # Negotiation
            "stage_change_time": sct,
            "update_time": sct,
            self.gen._client_type_field: "residential",
            self.gen._emv_field: None,
        }
        with patch.object(self.gen, "_complete_won_deal") as mock_won:
            with patch("random.random", return_value=0.01):
                result = self.gen._advance_deal(negotiation_deal, dry_run=False)

        mock_won.assert_called_once()
        self.assertTrue(result.success)
        self.assertIn("Won", result.message)


if __name__ == "__main__":
    unittest.main()
