# Error Reporter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `simulation/error_reporter.py` — the single integration point that translates all simulation/automation errors into plain-language Slack messages posted to `#automation-failure`.

**Architecture:** Three public functions (`setup_channel`, `report_error`, `report_reconciliation_issue`) backed by private classification (`_classify`), translation (`_resolve_translation`), and Block Kit building helpers. Module-level state holds a cached channel ID and a per-tool warning timestamp dict for sliding-window escalation.

**Tech Stack:** Slack SDK `WebClient` via `get_client("slack")`, `requests` (for exception isinstance checks), `unittest.mock` for tests.

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `simulation/__init__.py` | Create | Empty package marker |
| `simulation/exceptions.py` | Create | Custom exception classes (`TokenExpiredError`, `RateLimitError`, `ToolUnavailableError`, `ToolAPIError`) |
| `simulation/error_reporter.py` | Create | All public API, private helpers, module-level state |
| `tests/test_error_reporter.py` | Create | 15 tests covering all spec requirements |

---

## Task 1: Package Scaffolding

**Files:**
- Create: `simulation/__init__.py`
- Create: `simulation/exceptions.py`
- Create: `tests/test_error_reporter.py` (skeleton only)

- [ ] **Step 1: Create the simulation package marker**

```python
# simulation/__init__.py
# (empty — package marker only)
```

- [ ] **Step 2: Create the custom exception classes**

```python
# simulation/exceptions.py


class TokenExpiredError(Exception):
    """Raised when an API token has expired (HTTP 401)."""


class RateLimitError(Exception):
    """Raised when an API rate limit is hit (HTTP 429)."""


class ToolUnavailableError(Exception):
    """Raised when a tool returns a server error (HTTP 500–504)."""


class ToolAPIError(Exception):
    """Raised for API-level errors from a tool (HTTP 400, 403, 404, etc.)."""
```

- [ ] **Step 3: Create the test file skeleton**

```python
# tests/test_error_reporter.py
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
```

- [ ] **Step 4: Create the error_reporter module skeleton (no logic yet)**

```python
# simulation/error_reporter.py
import logging
import time
from datetime import datetime

import requests

from auth import get_client
from intelligence.logging_config import setup_logging
from simulation.exceptions import (
    RateLimitError,
    ToolAPIError,
    ToolUnavailableError,
    TokenExpiredError,
)

logger = setup_logging(__name__)

# ---------------------------------------------------------------------------
# Configurable escalation thresholds (referenced by Step 10 tests)
# ---------------------------------------------------------------------------
ESCALATION_THRESHOLD = 3        # warnings from same tool within window → critical
ESCALATION_WINDOW_MINUTES = 30  # rolling window in minutes
                                # 30 min covers 2-3 automation poll cycles and accounts for
                                # off-peak event spacing where events can be 15-30 min apart.
                                # A 10-min window would miss repeated failures during slow periods.

# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------
_channel_id: str | None = None
# Cached channel ID for #automation-failure.
# None until setup_channel() succeeds.

_warning_log: dict[str, list[float]] = {}
# Sliding-window escalation tracker.
# Key: tool_name. Value: list of unix timestamps for warning-level errors from that tool.
# On each report_error() call, only _warning_log[tool_name] is pruned (entries older than
# ESCALATION_WINDOW_MINUTES are removed). Then len(_warning_log[tool_name]) is checked
# against ESCALATION_THRESHOLD.


def setup_channel(dry_run: bool = False) -> str | None:
    """Create #automation-failure if it doesn't exist, set its topic, cache and return channel ID."""
    raise NotImplementedError


def report_error(
    exc: Exception | str,
    tool_name: str,
    context: str,
    severity: str | None = None,
    dry_run: bool = False,
) -> bool:
    """Translate exc to plain language and post to #automation-failure."""
    raise NotImplementedError


def report_reconciliation_issue(
    finding: dict,
    dry_run: bool = False,
) -> bool:
    """Post a reconciliation finding to #automation-failure."""
    raise NotImplementedError
```

- [ ] **Step 5: Commit scaffolding**

```bash
git add simulation/__init__.py simulation/exceptions.py simulation/error_reporter.py tests/test_error_reporter.py
git commit -m "Add simulation package scaffolding and exception classes (Step 8)"
```

---

## Task 2: `_classify()` Function

**Files:**
- Modify: `simulation/error_reporter.py` — add `_classify()`
- Modify: `tests/test_error_reporter.py` — add `TestClassify`

- [ ] **Step 1: Write the failing tests**

Add this class to `tests/test_error_reporter.py` (before `if __name__ == "__main__"`):

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_error_reporter.py::TestClassify -v
```

Expected: all 16 tests FAIL with `NotImplementedError` or `ImportError` on `_classify`.

- [ ] **Step 3: Implement `_classify()`**

Add this function to `simulation/error_reporter.py`, above the `setup_channel` stub:

```python
def _classify(exc: Exception | str) -> str:
    """Map an exception or HTTP status string to a category name."""
    if isinstance(exc, str):
        return "manual"

    if isinstance(exc, TokenExpiredError):
        return "token_expired"
    if isinstance(exc, RateLimitError):
        return "rate_limited"
    if isinstance(exc, ToolUnavailableError):
        return "server_error"
    if isinstance(exc, requests.ConnectionError):
        return "connection_error"
    if isinstance(exc, requests.Timeout):
        return "timeout"
    if isinstance(exc, ToolAPIError):
        return "not_found" if "404" in str(exc) else "client_error"

    msg = str(exc)
    if "401" in msg:
        return "token_expired"
    if "403" in msg:
        return "permission_error"
    if "429" in msg:
        return "rate_limited"
    if any(code in msg for code in ["500", "501", "502", "503", "504"]):
        return "server_error"
    if "404" in msg:
        return "not_found"
    if "400" in msg:
        return "client_error"

    return "unknown"
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_error_reporter.py::TestClassify -v
```

Expected: all 16 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add simulation/error_reporter.py tests/test_error_reporter.py
git commit -m "Add _classify() with full HTTP status and exception type coverage"
```

---

## Task 3: Translation Dicts + `_resolve_translation()`

**Files:**
- Modify: `simulation/error_reporter.py` — add `_CATEGORY_DEFAULTS`, `_TOOL_OVERRIDES`, `_RECONCILIATION_DEFAULTS`, `_resolve_translation()`
- Modify: `tests/test_error_reporter.py` — add `TestResolveTranslation`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_error_reporter.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_error_reporter.py::TestResolveTranslation -v
```

Expected: all 11 tests FAIL with `ImportError` on `_resolve_translation`.

- [ ] **Step 3: Add translation dicts and `_resolve_translation()` to `simulation/error_reporter.py`**

Add these after the imports/constants block, before `_classify()`:

```python
# ---------------------------------------------------------------------------
# Translation tables
# ---------------------------------------------------------------------------

_CATEGORY_DEFAULTS: dict[str, dict] = {
    "token_expired": {
        "what_happened": "The connection to {tool} has expired.",
        "what_to_do": "Run: `python -m demo.hardening.token_preflight`",
        "severity": "critical",
    },
    "permission_error": {
        "what_happened": "{tool} rejected the request — it may have lost a required permission.",
        "what_to_do": "Check that the {tool} token still has all required scopes.",
        "severity": "warning",
    },
    "rate_limited": {
        "what_happened": "{tool} asked us to slow down.",
        "what_to_do": "The engine will retry automatically. No action needed.",
        "severity": "warning",
    },
    "server_error": {
        "what_happened": "{tool} returned a server error.",
        "what_to_do": "The engine will retry. If this persists, check {tool}'s status page.",
        "severity": "warning",
    },
    "connection_error": {
        "what_happened": "Could not reach {tool}.",
        "what_to_do": "Check network connectivity. The engine will retry.",
        "severity": "warning",
    },
    "timeout": {
        "what_happened": "The request to {tool} timed out.",
        "what_to_do": "The engine will retry. If this persists, check {tool}'s status page.",
        "severity": "warning",
    },
    "client_error": {
        "what_happened": "A data error occurred sending a record to {tool}.",
        "what_to_do": "Check the log file for the rejected record's details.",
        "severity": "info",
    },
    "not_found": {
        "what_happened": "A record expected in {tool} was not found.",
        "what_to_do": "Check the log file for the missing record's ID.",
        "severity": "warning",
    },
    "manual": {
        "what_happened": "",  # replaced with the exc string at resolution time
        "what_to_do": "Review the log file for details.",
        "severity": "info",
    },
    "unknown": {
        "what_happened": "An unexpected error occurred with {tool}.",
        "what_to_do": "Check the log file for the full stack trace.",
        "severity": "warning",
    },
}

_TOOL_OVERRIDES: dict[str, dict[str, dict]] = {
    "quickbooks": {
        "token_expired": {"what_to_do": "Refresh the token: `python -m auth.quickbooks_auth`"},
    },
    "jobber": {
        "token_expired": {"what_to_do": "Refresh the token: `python -m auth.jobber_auth`"},
    },
    "google": {
        "token_expired": {"what_to_do": "Re-authenticate: `python -m auth.google_auth`"},
    },
    "asana": {
        "permission_error": {
            "what_to_do_append": (
                "Asana occasionally returns 403 for tasks in restricted projects"
                " — check if this is a one-off before escalating."
            ),
        },
    },
}

_RECONCILIATION_DEFAULTS: dict[str, dict] = {
    "reconciliation_mismatch": {
        "what_happened": "{tool} record for {entity} doesn't match the canonical database.",
        "what_to_do": "Review the mismatch details below. Auto-repaired mismatches need no action.",
        "severity": "info",
    },
    "reconciliation_missing": {
        "what_happened": "Expected record in {tool} for {entity} was not found.",
        "what_to_do": "The record may need to be recreated. Check the log for the canonical ID.",
        "severity": "warning",
    },
    "reconciliation_automation_gap": {
        "what_happened": "{count} completed jobs have no invoices after 24 hours.",
        "what_to_do": (
            "The Jobber-to-QuickBooks automation may have missed them."
            " Check poll_state and QuickBooks auth."
        ),
        "severity": "critical",
    },
}

_SEVERITY_COLORS: dict[str, str] = {
    "info": "#2196F3",
    "warning": "#FFC107",
    "critical": "#D32F2F",
}

_SEVERITY_EMOJIS: dict[str, str] = {
    "info": "",
    "warning": ":warning: ",
    "critical": ":rotating_light: ",
}


def _resolve_translation(
    tool_name: str,
    category: str,
    exc_str: str = "",
) -> dict:
    """Return {what_happened, what_to_do, severity} with tool overrides and {tool} interpolated."""
    entry = _CATEGORY_DEFAULTS[category].copy()

    # manual category: the exc string IS the what_happened
    if category == "manual":
        entry["what_happened"] = exc_str

    # Apply tool-specific overrides
    override = _TOOL_OVERRIDES.get(tool_name, {}).get(category, {})
    if "what_to_do" in override:
        entry["what_to_do"] = override["what_to_do"]
    if "what_to_do_append" in override:
        entry["what_to_do"] = entry["what_to_do"] + " " + override["what_to_do_append"]

    # Interpolate {tool} placeholder
    tool_title = tool_name.title()
    entry["what_happened"] = entry["what_happened"].replace("{tool}", tool_title)
    entry["what_to_do"] = entry["what_to_do"].replace("{tool}", tool_title)

    return entry
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_error_reporter.py::TestResolveTranslation -v
```

Expected: all 11 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add simulation/error_reporter.py tests/test_error_reporter.py
git commit -m "Add translation dicts and _resolve_translation() with tool overrides"
```

---

## Task 4: `setup_channel()`

**Files:**
- Modify: `simulation/error_reporter.py` — implement `setup_channel()`
- Modify: `tests/test_error_reporter.py` — add `TestSetupChannel`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_error_reporter.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_error_reporter.py::TestSetupChannel -v
```

Expected: all 8 tests FAIL with `NotImplementedError`.

- [ ] **Step 3: Implement `setup_channel()` in `simulation/error_reporter.py`**

Replace the `setup_channel` stub:

```python
def setup_channel(dry_run: bool = False) -> str | None:
    """Create #automation-failure if it doesn't exist, set its topic, cache and return channel ID.

    Idempotent: subsequent calls return the cached ID immediately.
    Returns None if Slack is unreachable — callers must handle None gracefully.
    """
    global _channel_id

    if _channel_id is not None:
        return _channel_id

    if dry_run:
        logger.info("[DRY RUN] Would create/find #automation-failure and set its topic")
        return "DRY-RUN-CHANNEL-ID"

    try:
        client = get_client("slack")

        # Search for existing channel.
        # Most workspaces have fewer than 200 channels. Exhaustive pagination on a
        # large workspace would slow engine startup for no benefit. If not found in
        # first 200 results, skip further pages and go straight to conversations_create.
        response = client.conversations_list(types="public_channel", limit=200)
        for ch in response["channels"]:
            if ch["name"] == "automation-failure":
                _channel_id = ch["id"]
                client.conversations_setTopic(
                    channel=_channel_id,
                    topic="Simulation and automation errors — plain language only, no stack traces",
                )
                return _channel_id

        # Not found — create it
        create_response = client.conversations_create(name="automation-failure")
        _channel_id = create_response["channel"]["id"]
        client.conversations_setTopic(
            channel=_channel_id,
            topic="Simulation and automation errors — plain language only, no stack traces",
        )
        return _channel_id

    except Exception as exc:
        logger.warning("Could not set up #automation-failure: %s", exc)
        return None
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_error_reporter.py::TestSetupChannel -v
```

Expected: all 8 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add simulation/error_reporter.py tests/test_error_reporter.py
git commit -m "Implement setup_channel() with idempotent caching and dry-run support"
```

---

## Task 5: Block Kit Builders

**Files:**
- Modify: `simulation/error_reporter.py` — add `_build_error_blocks()` and `_build_reconciliation_blocks()`
- Modify: `tests/test_error_reporter.py` — add `TestBuildErrorBlocks`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_error_reporter.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_error_reporter.py::TestBuildErrorBlocks -v
```

Expected: all 8 tests FAIL with `ImportError` on `_build_error_blocks`.

- [ ] **Step 3: Implement `_build_error_blocks()` and `_build_reconciliation_blocks()` in `simulation/error_reporter.py`**

Add these before the `setup_channel` function:

```python
def _build_error_blocks(
    what_happened: str,
    what_was_affected: str,
    what_to_do: str,
    severity: str,
    tool_name: str,
    header_text: str = "Automation Issue",
) -> list[dict]:
    """Build Block Kit blocks for report_error() messages."""
    emoji = _SEVERITY_EMOJIS[severity]
    now_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    return [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{emoji}{header_text}",
                "emoji": True,
            },
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*What happened:* {what_happened}"},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*What was affected:* {what_was_affected}"},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*What to do:* {what_to_do}"},
        },
        {"type": "divider"},
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": f"_Tool: {tool_name} | {now_utc}_"},
            ],
        },
    ]


def _build_reconciliation_blocks(
    what_happened: str,
    what_was_affected: str,
    what_to_do: str,
    severity: str,
    tool_name: str,
    category: str,
    details: str | None = None,
) -> list[dict]:
    """Build Block Kit blocks for report_reconciliation_issue() messages."""
    now_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    blocks: list[dict] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": ":mag: Data Mismatch Detected",
                "emoji": True,
            },
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*What happened:* {what_happened}"},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*What was affected:* {what_was_affected}"},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*What to do:* {what_to_do}"},
        },
    ]

    if details:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": details},
        })

    blocks.extend([
        {"type": "divider"},
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"_Tool: {tool_name} | Category: {category} | {now_utc}_",
                }
            ],
        },
    ])

    return blocks
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_error_reporter.py::TestBuildErrorBlocks -v
```

Expected: all 8 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add simulation/error_reporter.py tests/test_error_reporter.py
git commit -m "Add Block Kit builders for error and reconciliation messages"
```

---

## Task 6: `report_error()` — Core Path

**Files:**
- Modify: `simulation/error_reporter.py` — implement `report_error()` (without escalation)
- Modify: `tests/test_error_reporter.py` — add `TestReportError`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_error_reporter.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_error_reporter.py::TestReportError -v
```

Expected: all 9 tests FAIL with `NotImplementedError`.

- [ ] **Step 3: Implement `report_error()` in `simulation/error_reporter.py`**

Replace the `report_error` stub:

```python
def report_error(
    exc: Exception | str,
    tool_name: str,
    context: str,
    severity: str | None = None,
    dry_run: bool = False,
) -> bool:
    """Translate exc to plain language and post to #automation-failure.

    exc may be a caught Exception or a plain string (for findings that aren't exceptions).
    severity override bypasses escalation logic entirely.
    Returns True if posted (or dry_run=True). Never raises.
    """
    try:
        channel_id = setup_channel(dry_run=dry_run)
        if channel_id is None:
            logger.warning(
                "Slack #automation-failure unavailable — skipping error report for %s", tool_name
            )
            return False

        category = _classify(exc)
        translation = _resolve_translation(
            tool_name=tool_name,
            category=category,
            exc_str=str(exc) if isinstance(exc, str) else "",
        )

        what_happened = translation["what_happened"]
        what_to_do = translation["what_to_do"]
        base_severity = translation["severity"]

        # Determine final severity and header text
        header_text = "Automation Issue"
        if severity is not None:
            final_severity = severity
        else:
            final_severity = base_severity
            if base_severity == "warning":
                now = time.time()
                _warning_log.setdefault(tool_name, [])
                _warning_log[tool_name].append(now)
                cutoff = now - ESCALATION_WINDOW_MINUTES * 60
                _warning_log[tool_name] = [
                    t for t in _warning_log[tool_name] if t >= cutoff
                ]
                if len(_warning_log[tool_name]) >= ESCALATION_THRESHOLD:
                    final_severity = "critical"
                    header_text = "Automation Issue — Repeated Failures"

        blocks = _build_error_blocks(
            what_happened=what_happened,
            what_was_affected=context,
            what_to_do=what_to_do,
            severity=final_severity,
            tool_name=tool_name,
            header_text=header_text,
        )

        if dry_run:
            logger.info(
                "[DRY RUN] Would post to #automation-failure: %s — %s",
                header_text,
                what_happened,
            )
            return True

        client = get_client("slack")
        response = client.chat_postMessage(
            channel=channel_id,
            text=f"{header_text} — {what_happened}",
            blocks=blocks,
            attachments=[{"color": _SEVERITY_COLORS[final_severity], "blocks": []}],
        )
        if response["ok"]:
            return True
        logger.error("chat_postMessage returned ok=False: %s", response)
        return False

    except Exception as exc_inner:
        logger.error("Unexpected error in report_error: %s", exc_inner)
        return False
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_error_reporter.py::TestReportError -v
```

Expected: all 9 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add simulation/error_reporter.py tests/test_error_reporter.py
git commit -m "Implement report_error() core path with classification and Block Kit posting"
```

---

## Task 7: `report_error()` — Escalation Logic

**Files:**
- Modify: `tests/test_error_reporter.py` — add `TestEscalation`

(The escalation code is already in `report_error()` from Task 6. These tests verify it behaves correctly.)

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_error_reporter.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they pass** (escalation is already implemented)

```bash
python -m pytest tests/test_error_reporter.py::TestEscalation -v
```

Expected: all 6 tests PASS. If any fail, investigate and fix `report_error()` before continuing.

- [ ] **Step 3: Run full test suite to confirm no regressions**

```bash
python -m pytest tests/test_error_reporter.py -v
```

Expected: all tests PASS.

- [ ] **Step 4: Commit**

```bash
git add tests/test_error_reporter.py
git commit -m "Add escalation tests — verify per-tool window, pruning, and cold-start lazy init"
```

---

## Task 8: `report_reconciliation_issue()`

**Files:**
- Modify: `simulation/error_reporter.py` — implement `report_reconciliation_issue()`
- Modify: `tests/test_error_reporter.py` — add `TestReportReconciliationIssue`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_error_reporter.py`:

```python
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
        context_text = call_kwargs["blocks"][-1]["elements"][0]["text"]
        assert "reconciliation_missing" in context_text

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
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_error_reporter.py::TestReportReconciliationIssue -v
```

Expected: all 8 tests FAIL with `NotImplementedError`.

- [ ] **Step 3: Implement `report_reconciliation_issue()` in `simulation/error_reporter.py`**

Replace the `report_reconciliation_issue` stub:

```python
def report_reconciliation_issue(
    finding: dict,
    dry_run: bool = False,
) -> bool:
    """Post a reconciliation finding to #automation-failure.

    Uses :mag: *Data Mismatch Detected* header.
    Returns True if posted (or dry_run=True). Never raises.
    """
    try:
        channel_id = setup_channel(dry_run=dry_run)
        if channel_id is None:
            logger.warning(
                "Slack #automation-failure unavailable — skipping reconciliation report"
            )
            return False

        category = finding["category"]
        tool = finding["tool"]
        entity = finding.get("entity", "")
        count = finding.get("count", 0)
        details = finding.get("details")

        defaults = _RECONCILIATION_DEFAULTS[category]
        what_happened = (
            defaults["what_happened"]
            .replace("{tool}", tool.title())
            .replace("{entity}", entity)
            .replace("{count}", str(count))
        )
        what_to_do = defaults["what_to_do"]
        severity = defaults["severity"]

        blocks = _build_reconciliation_blocks(
            what_happened=what_happened,
            what_was_affected=entity,
            what_to_do=what_to_do,
            severity=severity,
            tool_name=tool,
            category=category,
            details=details,
        )

        if dry_run:
            logger.info(
                "[DRY RUN] Would post reconciliation finding to #automation-failure: %s",
                what_happened,
            )
            return True

        client = get_client("slack")
        response = client.chat_postMessage(
            channel=channel_id,
            text=f"Data Mismatch Detected — {what_happened}",
            blocks=blocks,
            attachments=[{"color": _SEVERITY_COLORS[severity], "blocks": []}],
        )
        if response["ok"]:
            return True
        logger.error("chat_postMessage returned ok=False for reconciliation: %s", response)
        return False

    except Exception as exc:
        logger.error("Unexpected error in report_reconciliation_issue: %s", exc)
        return False
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_error_reporter.py::TestReportReconciliationIssue -v
```

Expected: all 8 tests PASS.

- [ ] **Step 5: Run full test suite**

```bash
python -m pytest tests/test_error_reporter.py -v
```

Expected: all tests PASS. Count should be 16 + 11 + 8 + 8 + 9 + 6 + 8 = **66 tests**.

- [ ] **Step 6: Commit**

```bash
git add simulation/error_reporter.py tests/test_error_reporter.py
git commit -m "Implement report_reconciliation_issue() with three reconciliation categories"
```

---

## Self-Review Checklist

- **Spec coverage:**
  - Classification (16 cases) — Task 2 ✓
  - Translation + interpolation (11 cases) — Task 3 ✓
  - Tool-specific overrides + asana append — Task 3 ✓
  - `setup_channel()` idempotency, creates channel, sets topic, dry-run, failure-safe — Task 4 ✓
  - Block Kit structure (header/divider/sections/context), emoji per severity — Task 5 ✓
  - `blocks` for content, `attachments` color-only sidebar — Task 6 ✓
  - `context` param → "What was affected" section — Task 6 ✓
  - Severity override bypasses escalation — Tasks 6 & 7 ✓
  - Escalation threshold, window pruning, per-tool isolation — Task 7 ✓
  - Cold-start lazy init (test 15) — Task 7 ✓
  - All three reconciliation categories with correct severities — Task 8 ✓
  - `automation_gap` critical + red sidebar — Task 8 ✓
  - `details` field → extra Block Kit section — Task 8 ✓
  - `str` exc path — Task 6 ✓
  - Dry-run on all three functions — Tasks 4, 6, 8 ✓
  - Never raises (exception swallowed) — Task 6 ✓
  - `setup_channel()` failure is non-fatal — Task 6 ✓
- **Placeholders:** None.
- **Type consistency:** `_warning_log: dict[str, list[float]]` set up in scaffold, used in `report_error()`. `_channel_id: str | None` consistent throughout. `_build_error_blocks` / `_build_reconciliation_blocks` signatures match all call sites.
