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


if __name__ == "__main__":
    unittest.main()
