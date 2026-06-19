"""
tests/test_automations/test_error_suppression.py

Slack alert suppression + recovery notices in simulation/error_reporter.py.
Uses the PG test database (mock_db fixture ensures error_alert_state exists
via automations/migrate.py _MIGRATIONS).
"""
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

_PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from simulation import error_reporter
from simulation.error_reporter import (
    ALERT_SUPPRESSION_WINDOW_MINUTES,
    _check_suppression,
    _suppression_decision,
    report_error,
    report_recovery,
)

TOOL = "google"
CTX = "Polling Google Sheets for negative reviews"


@pytest.fixture
def clean_state(mock_db):
    """Clear suppression rows and the in-memory escalation log between tests."""
    with mock_db:
        mock_db.execute("DELETE FROM error_alert_state")
    error_reporter._warning_log.clear()
    yield mock_db
    with mock_db:
        mock_db.execute("DELETE FROM error_alert_state")
    error_reporter._warning_log.clear()


def _state_row(db, tool=TOOL, key=CTX):
    return db.execute(
        "SELECT * FROM error_alert_state WHERE tool_name = %s AND context_key = %s",
        (tool, key),
    ).fetchone()


def _backdate_last_posted(db, minutes, tool=TOOL, key=CTX):
    with db:
        db.execute(
            "UPDATE error_alert_state "
            "SET last_posted_at = CURRENT_TIMESTAMP - make_interval(mins => %s) "
            "WHERE tool_name = %s AND context_key = %s",
            (minutes, tool, key),
        )


# ── _suppression_decision ─────────────────────────────────────────────────────

class TestSuppressionDecision:
    def test_first_alert_posts_and_creates_row(self, clean_state):
        db = clean_state
        should_post, repeats = _suppression_decision(db, TOOL, CTX, 30)
        assert should_post is True
        assert repeats == 0
        row = _state_row(db)
        assert row is not None
        assert row["episode_count"] == 1
        assert row["suppressed_count"] == 0

    def test_second_alert_within_window_is_suppressed(self, clean_state):
        db = clean_state
        _suppression_decision(db, TOOL, CTX, 30)
        should_post, repeats = _suppression_decision(db, TOOL, CTX, 30)
        assert should_post is False
        assert repeats == 1
        row = _state_row(db)
        assert row["suppressed_count"] == 1
        assert row["episode_count"] == 2

    def test_window_expiry_reposts_with_suppressed_count(self, clean_state):
        db = clean_state
        _suppression_decision(db, TOOL, CTX, 30)
        _suppression_decision(db, TOOL, CTX, 30)  # suppressed
        _backdate_last_posted(db, 31)
        should_post, repeats = _suppression_decision(db, TOOL, CTX, 30)
        assert should_post is True
        assert repeats == 1  # one alert was suppressed during the window
        row = _state_row(db)
        assert row["suppressed_count"] == 0  # reset after reposting
        assert row["episode_count"] == 3

    def test_different_contexts_are_independent(self, clean_state):
        db = clean_state
        _suppression_decision(db, TOOL, CTX, 30)
        should_post, _ = _suppression_decision(db, TOOL, "some other poll", 30)
        assert should_post is True


# ── _check_suppression fail-open ──────────────────────────────────────────────

class TestCheckSuppressionFailOpen:
    def test_db_error_posts_anyway(self, clean_state):
        with patch(
            "database.connection.get_connection",
            side_effect=RuntimeError("db down"),
        ):
            should_post, repeats = _check_suppression(TOOL, CTX)
        assert should_post is True
        assert repeats == 0


# ── report_error integration ──────────────────────────────────────────────────

class TestReportErrorSuppression:
    def _slack(self):
        client = MagicMock()
        client.chat_postMessage.return_value = {"ok": True}
        return client

    def test_duplicate_alert_posts_once(self, clean_state):
        slack = self._slack()
        with patch.object(error_reporter, "setup_channel", return_value="C-TEST"), \
             patch.object(error_reporter, "get_client", return_value=slack):
            assert report_error("boom", tool_name=TOOL, context=CTX) is True
            assert report_error("boom", tool_name=TOOL, context=CTX) is True
        assert slack.chat_postMessage.call_count == 1
        row = _state_row(clean_state)
        assert row["suppressed_count"] == 1

    def test_repost_after_window_mentions_repeats(self, clean_state):
        slack = self._slack()
        with patch.object(error_reporter, "setup_channel", return_value="C-TEST"), \
             patch.object(error_reporter, "get_client", return_value=slack):
            report_error("boom", tool_name=TOOL, context=CTX)
            report_error("boom", tool_name=TOOL, context=CTX)  # suppressed
            _backdate_last_posted(clean_state, ALERT_SUPPRESSION_WINDOW_MINUTES + 1)
            report_error("boom", tool_name=TOOL, context=CTX)
        assert slack.chat_postMessage.call_count == 2
        text = slack.chat_postMessage.call_args.kwargs["text"]
        assert "1 more time" in text

    def test_dry_run_skips_suppression_state(self, clean_state):
        report_error("boom", tool_name=TOOL, context=CTX, dry_run=True)
        assert _state_row(clean_state) is None


# ── report_recovery ───────────────────────────────────────────────────────────

class TestReportRecovery:
    def _slack(self):
        client = MagicMock()
        client.chat_postMessage.return_value = {"ok": True}
        return client

    def test_no_state_row_returns_false(self, clean_state):
        slack = self._slack()
        with patch.object(error_reporter, "setup_channel", return_value="C-TEST"), \
             patch.object(error_reporter, "get_client", return_value=slack):
            assert report_recovery(TOOL, CTX) is False
        slack.chat_postMessage.assert_not_called()

    def test_multi_failure_episode_posts_and_clears(self, clean_state):
        db = clean_state
        _suppression_decision(db, TOOL, CTX, 30)
        _suppression_decision(db, TOOL, CTX, 30)  # episode_count -> 2
        slack = self._slack()
        with patch.object(error_reporter, "setup_channel", return_value="C-TEST"), \
             patch.object(error_reporter, "get_client", return_value=slack):
            assert report_recovery(TOOL, CTX) is True
        slack.chat_postMessage.assert_called_once()
        text = slack.chat_postMessage.call_args.kwargs["text"]
        assert "Recovered" in text
        assert _state_row(db) is None

    def test_single_blip_clears_silently(self, clean_state):
        db = clean_state
        _suppression_decision(db, TOOL, CTX, 30)  # episode_count == 1
        slack = self._slack()
        with patch.object(error_reporter, "setup_channel", return_value="C-TEST"), \
             patch.object(error_reporter, "get_client", return_value=slack):
            assert report_recovery(TOOL, CTX) is False
        slack.chat_postMessage.assert_not_called()
        assert _state_row(db) is None
