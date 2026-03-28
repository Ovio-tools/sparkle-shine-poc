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


if __name__ == "__main__":
    unittest.main()
