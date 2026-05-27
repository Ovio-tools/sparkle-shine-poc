"""Unit tests for simulation.jobber_utils."""
from __future__ import annotations

import os
import random
import sys
import unittest
from datetime import date, datetime, timedelta
from unittest.mock import MagicMock

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from simulation import jobber_utils
from simulation.jobber_user_pool import UserPool


def _reset_field_cache() -> None:
    jobber_utils.JOBBER_FIELD_CACHE.update({
        "assigned_users": None,
        "timeframe_end": None,
        "recurrence": None,
    })
    jobber_utils._field_discovery_done = False
    jobber_utils._field_warned.clear()


def _install_field_mock(
    *,
    assigned_users: str | None = "assignedUsers",
    timeframe_end: str | None = "endAt",
    recurrence: str | None = "recurrences",
) -> None:
    jobber_utils.JOBBER_FIELD_CACHE["assigned_users"] = assigned_users
    jobber_utils.JOBBER_FIELD_CACHE["timeframe_end"] = timeframe_end
    jobber_utils.JOBBER_FIELD_CACHE["recurrence"] = recurrence
    jobber_utils._field_discovery_done = True


def _make_session_returning(*responses: dict) -> MagicMock:
    """Return a MagicMock session whose .post().json() yields each response."""
    iterator = iter(responses)

    def _post(*args, **kwargs):
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.json = MagicMock(return_value=next(iterator))
        return resp

    session = MagicMock()
    session.post.side_effect = _post
    return session


class TestDiscoverJobCreateFields(unittest.TestCase):
    def setUp(self):
        _reset_field_cache()

    def tearDown(self):
        _reset_field_cache()

    def test_happy_path_populates_all_three_keys(self):
        # First call → JobCreateAttributes; second → JobTimeframeAttributes
        session = _make_session_returning(
            {"data": {"__type": {"inputFields": [
                {"name": "assignedUsers", "type": {"kind": "LIST", "name": None,
                                                    "ofType": {"name": "ID", "kind": "SCALAR"}}},
                {"name": "recurrences",   "type": {"kind": "INPUT_OBJECT", "name": "Rec", "ofType": None}},
                {"name": "timeframe",     "type": {"kind": "INPUT_OBJECT",
                                                    "name": "JobTimeframeAttributes",
                                                    "ofType": None}},
            ]}}},
            {"data": {"__type": {"inputFields": [
                {"name": "startAt"}, {"name": "endAt"},
            ]}}},
        )
        jobber_utils.discover_job_create_fields(session)
        self.assertEqual(jobber_utils.JOBBER_FIELD_CACHE["assigned_users"], "assignedUsers")
        self.assertEqual(jobber_utils.JOBBER_FIELD_CACHE["timeframe_end"], "endAt")
        self.assertEqual(jobber_utils.JOBBER_FIELD_CACHE["recurrence"], "recurrences")

    def test_unknown_candidate_logs_warn_and_leaves_none(self):
        session = _make_session_returning(
            {"data": {"__type": {"inputFields": [
                {"name": "title", "type": {"kind": "SCALAR", "name": "String", "ofType": None}},
            ]}}},
        )
        with self.assertLogs("simulation.jobber_utils", level="WARNING") as ctx:
            jobber_utils.discover_job_create_fields(session)
        self.assertIsNone(jobber_utils.JOBBER_FIELD_CACHE["assigned_users"])
        self.assertIsNone(jobber_utils.JOBBER_FIELD_CACHE["timeframe_end"])
        # One WARN per missing key, deduped on re-call
        jobber_utils.discover_job_create_fields(session)
        warns = [r for r in ctx.output if "Jobber introspection" in r]
        self.assertGreaterEqual(len(warns), 1)

    def test_discovery_is_idempotent(self):
        session = _make_session_returning(
            {"data": {"__type": {"inputFields": [
                {"name": "assignedUsers", "type": {"kind": "LIST", "name": None,
                                                    "ofType": {"name": "ID", "kind": "SCALAR"}}},
                {"name": "timeframe", "type": {"kind": "INPUT_OBJECT",
                                                "name": "JobTimeframeAttributes",
                                                "ofType": None}},
            ]}}},
            {"data": {"__type": {"inputFields": [{"name": "endAt"}]}}},
        )
        jobber_utils.discover_job_create_fields(session)
        first_calls = session.post.call_count
        jobber_utils.discover_job_create_fields(session)
        self.assertEqual(session.post.call_count, first_calls)


class TestComputeEndAt(unittest.TestCase):
    def test_datetime_input_preserves_datetime_shape(self):
        out = jobber_utils.compute_end_at(
            "2026-05-26T09:00:00", duration_minutes=120, jitter_minutes=0,
        )
        self.assertEqual(out, "2026-05-26T11:00:00")

    def test_date_input_keeps_date_when_end_same_day(self):
        out = jobber_utils.compute_end_at(
            "2026-05-26", duration_minutes=30, jitter_minutes=0,
        )
        # Both 00:00 and 00:30 fall on the same day → date-only preserved
        self.assertEqual(out, "2026-05-26")

    def test_jitter_within_bounds(self):
        random.seed(0)
        for _ in range(50):
            out = jobber_utils.compute_end_at(
                "2026-05-26T09:00:00", duration_minutes=120, jitter_minutes=5,
            )
            end = datetime.fromisoformat(out)
            delta = (end - datetime(2026, 5, 26, 11, 0)).total_seconds() / 60
            self.assertGreaterEqual(delta, -5.0001)
            self.assertLessEqual(delta, 5.0001)


class TestCrewSizeFor(unittest.TestCase):
    def test_short_job_returns_one(self):
        self.assertEqual(jobber_utils.crew_size_for(60, pool_size=8), 1)

    def test_standard_job_returns_two(self):
        self.assertEqual(jobber_utils.crew_size_for(120, pool_size=8), 2)

    def test_long_job_returns_three(self):
        self.assertEqual(jobber_utils.crew_size_for(210, pool_size=8), 3)

    def test_clamps_to_pool_size(self):
        self.assertEqual(jobber_utils.crew_size_for(210, pool_size=2), 2)
        self.assertEqual(jobber_utils.crew_size_for(60, pool_size=0), 0)

    def test_boundary_values(self):
        # 90 is the small/medium boundary — equal to small_max counts as medium
        self.assertEqual(jobber_utils.crew_size_for(90, pool_size=8), 2)
        # 150 is medium_max — still medium tier
        self.assertEqual(jobber_utils.crew_size_for(150, pool_size=8), 2)
        # 151 → large tier
        self.assertEqual(jobber_utils.crew_size_for(151, pool_size=8), 3)


class TestBuildJobCreateInput(unittest.TestCase):
    def setUp(self):
        _reset_field_cache()

    def tearDown(self):
        _reset_field_cache()

    def test_omits_optional_fields_when_cache_empty(self):
        # No introspection done, no session passed → no calls happen
        result = jobber_utils.build_job_create_input(
            property_id="PROP",
            title="Test",
            invoicing={"a": 1},
            start_iso="2026-05-26T09:00:00",
            duration_minutes=120,
            user_pool=UserPool(["u1"]),
            session=None,
        )
        self.assertEqual(result["propertyId"], "PROP")
        self.assertEqual(result["timeframe"], {"startAt": "2026-05-26T09:00:00"})
        self.assertNotIn("assignedUsers", result)

    def test_includes_endAt_and_assignedUsers_when_cache_populated(self):
        _install_field_mock()
        random.seed(0)
        result = jobber_utils.build_job_create_input(
            property_id="PROP",
            title="Test",
            invoicing={"a": 1},
            start_iso="2026-05-26T09:00:00",
            duration_minutes=120,
            user_pool=UserPool(["u1", "u2", "u3"]),
            session=None,
        )
        self.assertIn("endAt", result["timeframe"])
        self.assertEqual(result["assignedUsers"], ["u1", "u2"])

    def test_skips_timeframe_when_start_iso_none(self):
        _install_field_mock()
        result = jobber_utils.build_job_create_input(
            property_id="PROP",
            title="Test",
            invoicing={"a": 1},
            start_iso=None,
            duration_minutes=120,
            user_pool=UserPool(["u1"]),
            session=None,
        )
        self.assertNotIn("timeframe", result)
        # No timeframe → no assignment either (no start/end window)
        self.assertNotIn("assignedUsers", result)

    def test_extra_merges_last(self):
        _install_field_mock()
        result = jobber_utils.build_job_create_input(
            property_id="PROP",
            title="Test",
            invoicing={"a": 1},
            start_iso="2026-05-26T09:00:00",
            duration_minutes=120,
            user_pool=None,
            session=None,
            extra={"recurrences": {"type": "WEEKLY", "interval": 1}},
        )
        self.assertEqual(result["recurrences"], {"type": "WEEKLY", "interval": 1})


if __name__ == "__main__":
    unittest.main()
