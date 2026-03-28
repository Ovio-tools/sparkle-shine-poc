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
    details: Optional[str] = None,
) -> list[dict]:
    """Build Block Kit blocks for report_reconciliation_issue() messages."""
    now_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    blocks: list = [
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


def setup_channel(dry_run: bool = False) -> Optional[str]:
    """Create #automation-failure if it doesn't exist, set its topic, cache and return channel ID.

    Idempotent: subsequent calls return the cached ID immediately.
    Returns None if Slack is unreachable — callers must handle None gracefully.
    """
    global _channel_id

    if dry_run:
        logger.info("[DRY RUN] Would create/find #automation-failure and set its topic")
        return "DRY-RUN-CHANNEL-ID"

    if _channel_id is not None:
        return _channel_id

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


def report_error(
    exc: Union[Exception, str],
    tool_name: str,
    context: str,
    severity: Optional[str] = None,
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
            if base_severity == "warning" and not dry_run:
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
