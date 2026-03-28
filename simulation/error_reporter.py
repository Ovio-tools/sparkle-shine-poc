import logging
import time
from datetime import datetime
from typing import Union, Optional

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
_channel_id: Optional[str] = None
# Cached channel ID for #automation-failure.
# None until setup_channel() succeeds.

_warning_log: dict[str, list[float]] = {}
# Sliding-window escalation tracker.
# Key: tool_name. Value: list of unix timestamps for warning-level errors from that tool.
# On each report_error() call, only _warning_log[tool_name] is pruned (entries older than
# ESCALATION_WINDOW_MINUTES are removed). Then len(_warning_log[tool_name]) is checked
# against ESCALATION_THRESHOLD.


def _classify(exc: Union[Exception, str]) -> str:
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


def setup_channel(dry_run: bool = False) -> Optional[str]:
    """Create #automation-failure if it doesn't exist, set its topic, cache and return channel ID."""
    raise NotImplementedError


def report_error(
    exc: Union[Exception, str],
    tool_name: str,
    context: str,
    severity: Optional[str] = None,
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
