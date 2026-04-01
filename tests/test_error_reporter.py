import time
import unittest
from unittest.mock import MagicMock, patch

import requests


def _make_slack_mock():
    """Return a MagicMock Slack WebClient with sensible defaults."""
    client = MagicMock()
    client.conversations_list.return_value = {
        "ok": True,
        "channels": [{"id": "C12345", "name": "automation-failure"}],
    }
    client.conversations_create.return_value = {
        "ok": True,
        "channel": {"id": "C99999"},
    }
    client.conversations_setTopic.return_value = {"ok": True}
    client.chat_postMessage.return_value = {"ok": True, "ts": "1234567890.000001"}
    return client


def _reset_module_state():
    """Reset module-level singletons between tests."""
    import simulation.error_reporter as er
    er._channel_id = None
    er._warning_log = {}


class TestClassify(unittest.TestCase):
    def setUp(self):
        _reset_module_state()

    def test_str_is_manual(self):
        from simulation.error_reporter import _classify
        assert _classify("some plain-string finding") == "manual"

    def test_token_expired_error_class(self):
        from simulation.error_reporter import _classify
        from simulation.exceptions import TokenExpiredError
        assert _classify(TokenExpiredError("expired")) == "token_expired"

    def test_http_401_in_message(self):
        from simulation.error_reporter import _classify
        assert _classify(Exception("HTTP 401 Unauthorized")) == "token_expired"

    def test_http_403_in_message(self):
        from simulation.error_reporter import _classify
        assert _classify(Exception("HTTP 403 Forbidden")) == "permission_error"

    def test_rate_limit_error_class(self):
        from simulation.error_reporter import _classify
        from simulation.exceptions import RateLimitError
        assert _classify(RateLimitError()) == "rate_limited"

    def test_http_429_in_message(self):
        from simulation.error_reporter import _classify
        assert _classify(Exception("HTTP 429 Too Many Requests")) == "rate_limited"

    def test_tool_unavailable_error_class(self):
        from simulation.error_reporter import _classify
        from simulation.exceptions import ToolUnavailableError
        assert _classify(ToolUnavailableError()) == "server_error"

    def test_http_500_in_message(self):
        from simulation.error_reporter import _classify
        assert _classify(Exception("HTTP 500 Internal Server Error")) == "server_error"

    def test_http_503_in_message(self):
        from simulation.error_reporter import _classify
        assert _classify(Exception("503 Service Unavailable")) == "server_error"

    def test_connection_error(self):
        from simulation.error_reporter import _classify
        assert _classify(requests.ConnectionError()) == "connection_error"

    def test_timeout(self):
        from simulation.error_reporter import _classify
        assert _classify(requests.Timeout()) == "timeout"

    def test_tool_api_error_non_404(self):
        from simulation.error_reporter import _classify
        from simulation.exceptions import ToolAPIError
        assert _classify(ToolAPIError("400 Bad Request")) == "client_error"

    def test_tool_api_error_404(self):
        from simulation.error_reporter import _classify
        from simulation.exceptions import ToolAPIError
        assert _classify(ToolAPIError("404 Not Found")) == "not_found"

    def test_http_404_in_message(self):
        from simulation.error_reporter import _classify
        assert _classify(Exception("HTTP 404 Not Found")) == "not_found"

    def test_http_400_in_message(self):
        from simulation.error_reporter import _classify
        assert _classify(Exception("HTTP 400 Bad Request")) == "client_error"

    def test_unknown_exception(self):
        from simulation.error_reporter import _classify
        assert _classify(ValueError("something completely unexpected")) == "unknown"


class TestResolveTranslation(unittest.TestCase):
    def setUp(self):
        _reset_module_state()

    def test_quickbooks_token_expired_uses_tool_override(self):
        from simulation.error_reporter import _resolve_translation
        result = _resolve_translation("quickbooks", "token_expired")
        assert "quickbooks_auth" in result["what_to_do"]

    def test_jobber_token_expired_uses_tool_override(self):
        from simulation.error_reporter import _resolve_translation
        result = _resolve_translation("jobber", "token_expired")
        assert "jobber_auth" in result["what_to_do"]

    def test_google_token_expired_uses_tool_override(self):
        from simulation.error_reporter import _resolve_translation
        result = _resolve_translation("google", "token_expired")
        assert "google_auth" in result["what_to_do"]

    def test_asana_permission_error_appends_note(self):
        from simulation.error_reporter import _resolve_translation
        result = _resolve_translation("asana", "permission_error")
        # Base text still present
        assert "Check that the" in result["what_to_do"]
        # Appended text also present
        assert "Asana occasionally" in result["what_to_do"]

    def test_generic_tool_token_expired_uses_default(self):
        from simulation.error_reporter import _resolve_translation
        result = _resolve_translation("pipedrive", "token_expired")
        assert "token_preflight" in result["what_to_do"]

    def test_tool_name_interpolated_in_what_happened(self):
        from simulation.error_reporter import _resolve_translation
        result = _resolve_translation("quickbooks", "server_error")
        assert "Quickbooks" in result["what_happened"]
        assert "{tool}" not in result["what_happened"]

    def test_tool_name_interpolated_in_what_to_do(self):
        from simulation.error_reporter import _resolve_translation
        result = _resolve_translation("jobber", "server_error")
        assert "Jobber" in result["what_to_do"]
        assert "{tool}" not in result["what_to_do"]

    def test_manual_category_uses_exc_string(self):
        from simulation.error_reporter import _resolve_translation
        result = _resolve_translation("jobber", "manual", exc_str="3 invoices are missing")
        assert result["what_happened"] == "3 invoices are missing"

    def test_not_found_severity_is_warning(self):
        from simulation.error_reporter import _resolve_translation
        result = _resolve_translation("quickbooks", "not_found")
        assert result["severity"] == "warning"

    def test_token_expired_severity_is_critical(self):
        from simulation.error_reporter import _resolve_translation
        result = _resolve_translation("jobber", "token_expired")
        assert result["severity"] == "critical"

    def test_client_error_severity_is_info(self):
        from simulation.error_reporter import _resolve_translation
        result = _resolve_translation("quickbooks", "client_error")
        assert result["severity"] == "info"


class TestSetupChannel(unittest.TestCase):
    def setUp(self):
        _reset_module_state()

    @patch("simulation.error_reporter.get_client")
    def test_returns_existing_channel_id(self, mock_get_client):
        mock_get_client.return_value = _make_slack_mock()
        from simulation.error_reporter import setup_channel
        result = setup_channel()
        assert result == "C12345"

    @patch("simulation.error_reporter.get_client")
    def test_idempotent_second_call_skips_api(self, mock_get_client):
        mock_client = _make_slack_mock()
        mock_get_client.return_value = mock_client
        from simulation.error_reporter import setup_channel
        setup_channel()
        setup_channel()
        assert mock_client.conversations_list.call_count == 1

    @patch("simulation.error_reporter.get_client")
    def test_creates_channel_when_not_found(self, mock_get_client):
        mock_client = _make_slack_mock()
        mock_client.conversations_list.return_value = {"ok": True, "channels": []}
        mock_get_client.return_value = mock_client
        from simulation.error_reporter import setup_channel
        result = setup_channel()
        assert result == "C99999"
        mock_client.conversations_create.assert_called_once_with(name="automation-failure")

    @patch("simulation.error_reporter.get_client")
    def test_sets_topic_after_creating_channel(self, mock_get_client):
        mock_client = _make_slack_mock()
        mock_client.conversations_list.return_value = {"ok": True, "channels": []}
        mock_get_client.return_value = mock_client
        from simulation.error_reporter import setup_channel
        setup_channel()
        mock_client.conversations_setTopic.assert_called_once()
        topic_arg = mock_client.conversations_setTopic.call_args.kwargs.get("topic", "")
        assert "plain language" in topic_arg

    @patch("simulation.error_reporter.get_client")
    def test_sets_topic_on_existing_channel(self, mock_get_client):
        mock_get_client.return_value = _make_slack_mock()
        from simulation.error_reporter import setup_channel
        setup_channel()
        # Should also set topic when channel already exists
        mock_get_client.return_value.conversations_setTopic.assert_called_once()

    @patch("simulation.error_reporter.get_client")
    def test_returns_none_on_exception(self, mock_get_client):
        mock_get_client.side_effect = Exception("Slack unreachable")
        from simulation.error_reporter import setup_channel
        result = setup_channel()
        assert result is None

    def test_dry_run_returns_constant_no_api(self):
        from simulation.error_reporter import setup_channel
        result = setup_channel(dry_run=True)
        assert result == "DRY-RUN-CHANNEL-ID"

    @patch("simulation.error_reporter.get_client")
    def test_uses_limit_200_on_conversations_list(self, mock_get_client):
        mock_get_client.return_value = _make_slack_mock()
        from simulation.error_reporter import setup_channel
        setup_channel()
        call_kwargs = mock_get_client.return_value.conversations_list.call_args.kwargs
        assert call_kwargs.get("limit") == 200

    @patch("simulation.error_reporter.get_client")
    def test_returns_channel_id_when_set_topic_raises(self, mock_get_client):
        """setTopic missing scope must not block error reporting — channel ID still returned."""
        mock_client = _make_slack_mock()
        mock_client.conversations_setTopic.side_effect = Exception(
            "missing_scope: channels:write.topic"
        )
        mock_get_client.return_value = mock_client
        from simulation.error_reporter import setup_channel
        result = setup_channel()
        assert result == "C12345"

    @patch("simulation.error_reporter.get_client")
    def test_report_error_posts_successfully_when_set_topic_previously_failed(self, mock_get_client):
        """End-to-end: setTopic failure must not silently disable chat.postMessage."""
        mock_client = _make_slack_mock()
        mock_client.conversations_setTopic.side_effect = Exception(
            "missing_scope: channels:write.topic"
        )
        mock_get_client.return_value = mock_client
        from simulation.error_reporter import report_error
        result = report_error(Exception("HTTP 500"), tool_name="jobber", context="ctx")
        assert result is True
        mock_client.chat_postMessage.assert_called_once()


class TestBuildErrorBlocks(unittest.TestCase):
    def test_block_types_in_correct_order(self):
        from simulation.error_reporter import _build_error_blocks
        blocks = _build_error_blocks(
            what_happened="The connection to Quickbooks has expired.",
            what_was_affected="Invoice for Sarah Chen was skipped.",
            what_to_do="Refresh the token: python -m auth.quickbooks_auth",
            severity="warning",
            tool_name="quickbooks",
        )
        types = [b["type"] for b in blocks]
        assert types == ["header", "divider", "section", "section", "section", "divider", "context"]

    def test_warning_header_contains_warning_emoji(self):
        from simulation.error_reporter import _build_error_blocks
        blocks = _build_error_blocks(
            what_happened="msg", what_was_affected="ctx", what_to_do="do",
            severity="warning", tool_name="jobber",
        )
        header_text = blocks[0]["text"]["text"]
        assert ":warning:" in header_text

    def test_critical_header_contains_rotating_light(self):
        from simulation.error_reporter import _build_error_blocks
        blocks = _build_error_blocks(
            what_happened="msg", what_was_affected="ctx", what_to_do="do",
            severity="critical", tool_name="jobber",
            header_text="Automation Issue — Repeated Failures",
        )
        header_text = blocks[0]["text"]["text"]
        assert ":rotating_light:" in header_text

    def test_info_header_has_no_severity_emoji(self):
        from simulation.error_reporter import _build_error_blocks
        blocks = _build_error_blocks(
            what_happened="msg", what_was_affected="ctx", what_to_do="do",
            severity="info", tool_name="quickbooks",
        )
        header_text = blocks[0]["text"]["text"]
        assert ":warning:" not in header_text
        assert ":rotating_light:" not in header_text

    def test_what_happened_appears_in_section(self):
        from simulation.error_reporter import _build_error_blocks
        blocks = _build_error_blocks(
            what_happened="Connection timed out.",
            what_was_affected="Job sync",
            what_to_do="Retry",
            severity="warning",
            tool_name="jobber",
        )
        section_texts = [b["text"]["text"] for b in blocks if b["type"] == "section"]
        assert any("Connection timed out." in t for t in section_texts)

    def test_what_was_affected_appears_in_section(self):
        from simulation.error_reporter import _build_error_blocks
        blocks = _build_error_blocks(
            what_happened="msg",
            what_was_affected="Invoice for Sarah Chen was skipped.",
            what_to_do="Retry",
            severity="warning",
            tool_name="quickbooks",
        )
        section_texts = [b["text"]["text"] for b in blocks if b["type"] == "section"]
        assert any("Invoice for Sarah Chen was skipped." in t for t in section_texts)

    def test_context_footer_contains_tool_name(self):
        from simulation.error_reporter import _build_error_blocks
        blocks = _build_error_blocks(
            what_happened="msg", what_was_affected="ctx", what_to_do="do",
            severity="info", tool_name="asana",
        )
        context_block = blocks[-1]
        assert context_block["type"] == "context"
        context_text = context_block["elements"][0]["text"]
        assert "asana" in context_text

    def test_header_uses_plain_text_with_emoji_flag(self):
        from simulation.error_reporter import _build_error_blocks
        blocks = _build_error_blocks(
            what_happened="msg", what_was_affected="ctx", what_to_do="do",
            severity="warning", tool_name="jobber",
        )
        header_block = blocks[0]
        assert header_block["text"]["type"] == "plain_text"
        assert header_block["text"].get("emoji") is True


class TestReportError(unittest.TestCase):
    def setUp(self):
        _reset_module_state()

    @patch("simulation.error_reporter.get_client")
    def test_returns_true_on_successful_post(self, mock_get_client):
        mock_get_client.return_value = _make_slack_mock()
        from simulation.error_reporter import report_error
        result = report_error(
            Exception("HTTP 401"), tool_name="quickbooks",
            context="Creating invoice for Sarah Chen",
        )
        assert result is True

    @patch("simulation.error_reporter.get_client")
    def test_posts_to_correct_channel(self, mock_get_client):
        mock_client = _make_slack_mock()
        mock_get_client.return_value = mock_client
        from simulation.error_reporter import report_error
        report_error(Exception("HTTP 500"), tool_name="jobber", context="Sync run")
        call_kwargs = mock_client.chat_postMessage.call_args.kwargs
        assert call_kwargs["channel"] == "C12345"

    @patch("simulation.error_reporter.get_client")
    def test_message_includes_top_level_blocks_and_color_attachment(self, mock_get_client):
        mock_client = _make_slack_mock()
        mock_get_client.return_value = mock_client
        from simulation.error_reporter import report_error
        report_error(Exception("HTTP 500"), tool_name="jobber", context="ctx")
        call_kwargs = mock_client.chat_postMessage.call_args.kwargs
        assert "blocks" in call_kwargs
        assert "attachments" in call_kwargs
        assert len(call_kwargs["attachments"]) == 1
        assert "color" in call_kwargs["attachments"][0]
        # Content must NOT be inside attachments
        assert "blocks" not in call_kwargs["attachments"][0] or call_kwargs["attachments"][0]["blocks"] == []

    @patch("simulation.error_reporter.get_client")
    def test_severity_override_sets_critical_color(self, mock_get_client):
        mock_client = _make_slack_mock()
        mock_get_client.return_value = mock_client
        from simulation.error_reporter import report_error, _SEVERITY_COLORS
        report_error(Exception("HTTP 500"), tool_name="jobber", context="ctx", severity="critical")
        call_kwargs = mock_client.chat_postMessage.call_args.kwargs
        assert call_kwargs["attachments"][0]["color"] == _SEVERITY_COLORS["critical"]

    @patch("simulation.error_reporter.get_client")
    def test_str_exc_appears_as_what_happened(self, mock_get_client):
        mock_client = _make_slack_mock()
        mock_get_client.return_value = mock_client
        from simulation.error_reporter import report_error
        report_error(
            "3 completed jobs don't have invoices",
            tool_name="jobber",
            context="daily reconciliation check",
        )
        call_kwargs = mock_client.chat_postMessage.call_args.kwargs
        blocks_text = str(call_kwargs["blocks"])
        assert "3 completed jobs don't have invoices" in blocks_text

    @patch("simulation.error_reporter.get_client")
    def test_context_param_appears_as_what_was_affected(self, mock_get_client):
        mock_client = _make_slack_mock()
        mock_get_client.return_value = mock_client
        from simulation.error_reporter import report_error
        report_error(Exception("HTTP 500"), tool_name="jobber", context="Invoice for Sarah Chen was skipped")
        call_kwargs = mock_client.chat_postMessage.call_args.kwargs
        blocks_text = str(call_kwargs["blocks"])
        assert "Invoice for Sarah Chen was skipped" in blocks_text

    @patch("simulation.error_reporter.get_client")
    def test_chat_post_exception_returns_false_never_raises(self, mock_get_client):
        mock_client = _make_slack_mock()
        mock_client.chat_postMessage.side_effect = Exception("Network error")
        mock_get_client.return_value = mock_client
        from simulation.error_reporter import report_error
        result = report_error(Exception("HTTP 500"), tool_name="jobber", context="ctx")
        assert result is False

    @patch("simulation.error_reporter.get_client")
    def test_channel_none_returns_false_never_raises(self, mock_get_client):
        mock_get_client.side_effect = Exception("Slack down")
        from simulation.error_reporter import report_error
        result = report_error(Exception("HTTP 500"), tool_name="jobber", context="ctx")
        assert result is False

    def test_dry_run_returns_true_without_api_call(self):
        from simulation.error_reporter import report_error
        result = report_error(
            Exception("HTTP 500"), tool_name="jobber", context="ctx", dry_run=True
        )
        assert result is True


class TestEscalation(unittest.TestCase):
    def setUp(self):
        _reset_module_state()

    @patch("simulation.error_reporter.get_client")
    def test_escalates_to_critical_after_threshold(self, mock_get_client):
        mock_client = _make_slack_mock()
        mock_get_client.return_value = mock_client
        from simulation.error_reporter import report_error, ESCALATION_THRESHOLD, _SEVERITY_COLORS
        for i in range(ESCALATION_THRESHOLD):
            report_error(Exception("HTTP 500"), tool_name="jobber", context=f"attempt {i}")
        last_kwargs = mock_client.chat_postMessage.call_args.kwargs
        assert last_kwargs["attachments"][0]["color"] == _SEVERITY_COLORS["critical"]

    @patch("simulation.error_reporter.get_client")
    def test_escalated_message_has_repeated_failures_header(self, mock_get_client):
        mock_client = _make_slack_mock()
        mock_get_client.return_value = mock_client
        from simulation.error_reporter import report_error, ESCALATION_THRESHOLD
        for i in range(ESCALATION_THRESHOLD):
            report_error(Exception("HTTP 500"), tool_name="jobber", context=f"attempt {i}")
        last_kwargs = mock_client.chat_postMessage.call_args.kwargs
        assert "Repeated Failures" in last_kwargs["text"]

    @patch("simulation.error_reporter.get_client")
    def test_severity_override_does_not_add_to_warning_log(self, mock_get_client):
        mock_client = _make_slack_mock()
        mock_get_client.return_value = mock_client
        import simulation.error_reporter as er
        from simulation.error_reporter import report_error, ESCALATION_THRESHOLD
        for i in range(ESCALATION_THRESHOLD + 5):
            report_error(
                Exception("HTTP 500"), tool_name="jobber", context="ctx", severity="critical"
            )
        assert er._warning_log.get("jobber", []) == []

    @patch("simulation.error_reporter.get_client")
    def test_expired_warnings_pruned_before_count(self, mock_get_client):
        mock_client = _make_slack_mock()
        mock_get_client.return_value = mock_client
        import simulation.error_reporter as er
        from simulation.error_reporter import (
            report_error, ESCALATION_THRESHOLD, ESCALATION_WINDOW_MINUTES, _SEVERITY_COLORS,
        )
        # Inject stale timestamps directly — just below threshold
        stale_time = time.time() - (ESCALATION_WINDOW_MINUTES * 60 + 10)
        er._warning_log["jobber"] = [stale_time] * (ESCALATION_THRESHOLD - 1)
        # One fresh warning — after pruning, total is 1 (below threshold)
        report_error(Exception("HTTP 500"), tool_name="jobber", context="ctx")
        last_kwargs = mock_client.chat_postMessage.call_args.kwargs
        assert last_kwargs["attachments"][0]["color"] != _SEVERITY_COLORS["critical"]

    @patch("simulation.error_reporter.get_client")
    def test_escalation_is_per_tool_independent(self, mock_get_client):
        mock_client = _make_slack_mock()
        mock_get_client.return_value = mock_client
        from simulation.error_reporter import report_error, ESCALATION_THRESHOLD, _SEVERITY_COLORS
        for i in range(ESCALATION_THRESHOLD):
            report_error(Exception("HTTP 500"), tool_name="jobber", context="ctx")
        # Different tool — must NOT be escalated
        report_error(Exception("HTTP 500"), tool_name="quickbooks", context="ctx")
        last_kwargs = mock_client.chat_postMessage.call_args.kwargs
        assert last_kwargs["attachments"][0]["color"] != _SEVERITY_COLORS["critical"]

    @patch("simulation.error_reporter.get_client")
    def test_cold_start_lazy_init_populates_channel_id(self, mock_get_client):
        mock_client = _make_slack_mock()
        mock_get_client.return_value = mock_client
        import simulation.error_reporter as er
        assert er._channel_id is None  # confirm cold start
        from simulation.error_reporter import report_error
        result = report_error(Exception("HTTP 500"), tool_name="jobber", context="ctx")
        assert result is True
        assert er._channel_id == "C12345"


class TestReportReconciliationIssue(unittest.TestCase):
    def setUp(self):
        _reset_module_state()

    @patch("simulation.error_reporter.get_client")
    def test_mismatch_posts_info_color(self, mock_get_client):
        mock_client = _make_slack_mock()
        mock_get_client.return_value = mock_client
        from simulation.error_reporter import report_reconciliation_issue, _SEVERITY_COLORS
        report_reconciliation_issue({
            "category": "reconciliation_mismatch",
            "tool": "quickbooks",
            "entity": "SS-CLIENT-0047",
        })
        call_kwargs = mock_client.chat_postMessage.call_args.kwargs
        assert call_kwargs["attachments"][0]["color"] == _SEVERITY_COLORS["info"]

    @patch("simulation.error_reporter.get_client")
    def test_missing_posts_warning_color(self, mock_get_client):
        mock_client = _make_slack_mock()
        mock_get_client.return_value = mock_client
        from simulation.error_reporter import report_reconciliation_issue, _SEVERITY_COLORS
        report_reconciliation_issue({
            "category": "reconciliation_missing",
            "tool": "quickbooks",
            "entity": "SS-CLIENT-0047",
        })
        call_kwargs = mock_client.chat_postMessage.call_args.kwargs
        assert call_kwargs["attachments"][0]["color"] == _SEVERITY_COLORS["warning"]

    @patch("simulation.error_reporter.get_client")
    def test_automation_gap_posts_critical_color(self, mock_get_client):
        mock_client = _make_slack_mock()
        mock_get_client.return_value = mock_client
        from simulation.error_reporter import report_reconciliation_issue, _SEVERITY_COLORS
        report_reconciliation_issue({
            "category": "reconciliation_automation_gap",
            "tool": "jobber",
            "entity": "batch",
            "count": 5,
        })
        call_kwargs = mock_client.chat_postMessage.call_args.kwargs
        assert call_kwargs["attachments"][0]["color"] == _SEVERITY_COLORS["critical"]

    @patch("simulation.error_reporter.get_client")
    def test_header_uses_mag_and_data_mismatch(self, mock_get_client):
        mock_client = _make_slack_mock()
        mock_get_client.return_value = mock_client
        from simulation.error_reporter import report_reconciliation_issue
        report_reconciliation_issue({
            "category": "reconciliation_missing",
            "tool": "quickbooks",
            "entity": "SS-CLIENT-0047",
        })
        call_kwargs = mock_client.chat_postMessage.call_args.kwargs
        header_text = call_kwargs["blocks"][0]["text"]["text"]
        assert ":mag:" in header_text
        assert "Data Mismatch" in header_text

    @patch("simulation.error_reporter.get_client")
    def test_details_appended_as_extra_section(self, mock_get_client):
        mock_client = _make_slack_mock()
        mock_get_client.return_value = mock_client
        from simulation.error_reporter import report_reconciliation_issue
        report_reconciliation_issue({
            "category": "reconciliation_missing",
            "tool": "quickbooks",
            "entity": "SS-CLIENT-0047",
            "details": "Job SS-JOB-8201 completed 2026-03-26, no invoice found.",
        })
        call_kwargs = mock_client.chat_postMessage.call_args.kwargs
        blocks_text = str(call_kwargs["blocks"])
        assert "SS-JOB-8201" in blocks_text

    @patch("simulation.error_reporter.get_client")
    def test_automation_gap_interpolates_count(self, mock_get_client):
        mock_client = _make_slack_mock()
        mock_get_client.return_value = mock_client
        from simulation.error_reporter import report_reconciliation_issue
        report_reconciliation_issue({
            "category": "reconciliation_automation_gap",
            "tool": "jobber",
            "entity": "batch",
            "count": 7,
        })
        call_kwargs = mock_client.chat_postMessage.call_args.kwargs
        blocks_text = str(call_kwargs["blocks"])
        assert "7" in blocks_text

    @patch("simulation.error_reporter.get_client")
    def test_context_footer_includes_category(self, mock_get_client):
        mock_client = _make_slack_mock()
        mock_get_client.return_value = mock_client
        from simulation.error_reporter import report_reconciliation_issue
        report_reconciliation_issue({
            "category": "reconciliation_missing",
            "tool": "quickbooks",
            "entity": "SS-CLIENT-0047",
        })
        call_kwargs = mock_client.chat_postMessage.call_args.kwargs
        all_context_text = " ".join(
            el["text"]
            for block in call_kwargs["blocks"]
            if block.get("type") == "context"
            for el in block.get("elements", [])
        )
        assert "reconciliation_missing" in all_context_text

    def test_dry_run_returns_true_without_api_call(self):
        from simulation.error_reporter import report_reconciliation_issue
        result = report_reconciliation_issue(
            {
                "category": "reconciliation_mismatch",
                "tool": "quickbooks",
                "entity": "SS-CLIENT-0001",
            },
            dry_run=True,
        )
        assert result is True


if __name__ == "__main__":
    unittest.main()
