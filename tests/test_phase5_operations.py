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
        self.engine = SimulationEngine(dry_run=False)
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
        engine2._timed_queue = []
        engine2.load_checkpoint()
        self.assertEqual(len(engine2._timed_queue), 1)
        self.assertEqual(engine2._timed_queue[0].generator_name, "job_completion")
        self.assertEqual(engine2._timed_queue[0].fire_at, fire_at)
