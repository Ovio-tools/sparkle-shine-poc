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
