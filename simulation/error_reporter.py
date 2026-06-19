import logging
import os
import re
import time
import traceback
from datetime import datetime, date
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

ALERT_SUPPRESSION_WINDOW_MINUTES = 30
# At most one Slack post per identical (tool, context) failure per window.
# Source: 2026-06-11 incident — a 50-minute Google Sheets slowness window
# produced 10 identical alerts (one per 5-min runner cron cycle). A 30-min
# window matches ESCALATION_WINDOW_MINUTES, covers 6 cron cycles, and would
# have reduced that incident to 2 posts. State lives in the error_alert_state
# table because the runner is a fresh container every cycle.

_SUPPRESSION_KEY_MAX_CHARS = 500
# context strings are short human sentences; 500 chars caps pathological keys
# without ever truncating real ones.

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
# Location helpers
# ---------------------------------------------------------------------------

def _get_log_file_name() -> str:
    """Return the current daily log file name."""
    return f"logs/intelligence_{date.today().strftime('%Y-%m-%d')}.log"


def _extract_location(exc: Exception) -> Optional[str]:
    """Return 'filename:lineno' of the innermost traceback frame, or None."""
    if not isinstance(exc, Exception) or exc.__traceback__ is None:
        return None
    frames = traceback.extract_tb(exc.__traceback__)
    if not frames:
        return None
    last = frames[-1]
    filename = last.filename
    for marker in ("sparkle-shine-poc/", "sparkle_shine_poc/"):
        idx = filename.find(marker)
        if idx != -1:
            filename = filename[idx + len(marker):]
            break
    else:
        filename = os.path.basename(filename)
    return f"{filename}:{last.lineno}"


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
    "payment_required": {
        "what_happened": "{tool} rejected the request with a plan or billing limit (HTTP 402).",
        "what_to_do": (
            "This will not clear on its own — check {tool}'s plan, billing, and"
            " contact/record limits. The simulation will keep failing until the"
            " account limit is resolved."
        ),
        "severity": "critical",
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

_SLACK_SECTION_TEXT_LIMIT = 3000
_SLACK_TOP_LEVEL_TEXT_LIMIT = 3000
_SLACK_TRUNCATION_SUFFIX = " ...[truncated]"


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


def _truncate_slack_text(text: str, limit: int) -> str:
    """Trim oversized Slack mrkdwn/plain-text payloads without raising."""
    if len(text) <= limit:
        return text
    return text[: limit - len(_SLACK_TRUNCATION_SUFFIX)].rstrip() + _SLACK_TRUNCATION_SUFFIX


def _summarize_exception(exc: Exception) -> str:
    """Return a compact exception summary suitable for human-facing alerts."""
    name = exc.__class__.__name__
    message = " ".join(str(exc).split())
    return f"{name}: {message}" if message else name


def _build_slack_section(label: str, body: str) -> dict:
    return {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": _truncate_slack_text(f"*{label}:* {body}", _SLACK_SECTION_TEXT_LIMIT),
        },
    }


# Map a known HTTP status code to a category. 5xx is handled by range below.
_STATUS_CATEGORY: dict[int, str] = {
    400: "client_error",
    401: "token_expired",
    402: "payment_required",
    403: "permission_error",
    404: "not_found",
    429: "rate_limited",
}


def _extract_status_code(exc: Exception) -> Optional[int]:
    """Return the HTTP status code for an exception, or None.

    Prefer structured attributes (SDK exceptions expose `.status`; requests-style
    exceptions expose `.response.status_code`) over parsing the rendered message.
    Falls back to the FIRST standalone 3-digit status (100–599) in the message.

    The \\b…\\b anchors are critical: they stop a 3-digit run *inside* a longer
    number — e.g. the '500' inside a 'x-hubspot-ratelimit-daily: 250000' header —
    from being mistaken for a status code. That bug is exactly what mislabeled a
    HubSpot 402 as a server error in the 2026-06 incident.
    """
    status = getattr(exc, "status", None)
    if status is None:
        response = getattr(exc, "response", None)
        status = getattr(response, "status_code", None)
    if isinstance(status, int):
        return status

    match = re.search(r"\b([1-5]\d{2})\b", str(exc))
    return int(match.group(1)) if match else None


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

    code = _extract_status_code(exc)
    if code is not None:
        if code in _STATUS_CATEGORY:
            return _STATUS_CATEGORY[code]
        if 500 <= code <= 599:
            return "server_error"

    return "unknown"


def _build_error_blocks(
    what_happened: str,
    what_was_affected: str,
    what_to_do: str,
    severity: str,
    tool_name: str,
    header_text: str = "Automation Issue",
    error_location: Optional[str] = None,
    log_file: Optional[str] = None,
) -> list[dict]:
    """Build Block Kit blocks for report_error() messages."""
    emoji = _SEVERITY_EMOJIS[severity]
    now_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{emoji}{header_text}",
                "emoji": True,
            },
        },
        {"type": "divider"},
        _build_slack_section("What happened", what_happened),
        _build_slack_section("What was affected", what_was_affected),
        _build_slack_section("What to do", what_to_do),
        {"type": "divider"},
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": f"_Tool: {tool_name} | {now_utc}_"},
            ],
        },
    ]

    if log_file or error_location:
        parts = []
        if log_file:
            parts.append(f":page_facing_up: Log: `{log_file}`")
        if error_location:
            parts.append(f"`{error_location}`")
        blocks.append({
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": " · ".join(parts)},
            ],
        })

    return blocks


def _build_reconciliation_blocks(
    what_happened: str,
    what_was_affected: str,
    what_to_do: str,
    severity: str,
    tool_name: str,
    category: str,
    details: Optional[str] = None,
    log_file: Optional[str] = None,
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
        _build_slack_section("What happened", what_happened),
        _build_slack_section("What was affected", what_was_affected),
        _build_slack_section("What to do", what_to_do),
    ]

    if details:
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": _truncate_slack_text(details, _SLACK_SECTION_TEXT_LIMIT),
                },
            }
        )

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

    if log_file:
        blocks.append({
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": f":page_facing_up: Log: `{log_file}`"},
            ],
        })

    return blocks


# ---------------------------------------------------------------------------
# Duplicate-alert suppression (state in PostgreSQL: error_alert_state)
# ---------------------------------------------------------------------------

def _suppression_decision(conn, tool_name: str, context_key: str, window_minutes: int):
    """Decide whether to post an alert for (tool_name, context_key) and update state.

    Returns (should_post, suppressed_count): suppressed_count is how many
    identical alerts were swallowed since the last post — non-zero only when
    should_post is True after a window expiry, so the caller can say
    "occurred N more times".
    """
    row = conn.execute(
        """
        SELECT suppressed_count,
               (CURRENT_TIMESTAMP - last_posted_at)
                   >= make_interval(mins => %s) AS window_elapsed
        FROM error_alert_state
        WHERE tool_name = %s AND context_key = %s
        """,
        (window_minutes, tool_name, context_key),
    ).fetchone()

    if row is None:
        with conn:
            conn.execute(
                """
                INSERT INTO error_alert_state (tool_name, context_key)
                VALUES (%s, %s)
                ON CONFLICT (tool_name, context_key) DO NOTHING
                """,
                (tool_name, context_key),
            )
        return True, 0

    if row["window_elapsed"]:
        with conn:
            conn.execute(
                """
                UPDATE error_alert_state
                SET last_posted_at   = CURRENT_TIMESTAMP,
                    last_seen        = CURRENT_TIMESTAMP,
                    suppressed_count = 0,
                    episode_count    = episode_count + 1
                WHERE tool_name = %s AND context_key = %s
                """,
                (tool_name, context_key),
            )
        return True, row["suppressed_count"]

    with conn:
        conn.execute(
            """
            UPDATE error_alert_state
            SET last_seen        = CURRENT_TIMESTAMP,
                suppressed_count = suppressed_count + 1,
                episode_count    = episode_count + 1
            WHERE tool_name = %s AND context_key = %s
            """,
            (tool_name, context_key),
        )
    return False, row["suppressed_count"] + 1


def _check_suppression(tool_name: str, context: str):
    """DB-backed wrapper around _suppression_decision. Fail-open: any DB
    problem (no DATABASE_URL, missing table, connection refused) means the
    alert posts exactly as it did before suppression existed."""
    context_key = (context or "")[:_SUPPRESSION_KEY_MAX_CHARS]
    try:
        from database.connection import get_connection
        conn = get_connection()
        try:
            return _suppression_decision(
                conn, tool_name, context_key, ALERT_SUPPRESSION_WINDOW_MINUTES
            )
        finally:
            conn.close()
    except Exception as exc:
        logger.debug(
            "Alert suppression unavailable (%s) — posting without suppression", exc
        )
        return True, 0


def report_recovery(tool_name: str, context: str, dry_run: bool = False) -> bool:
    """Clear suppression state for (tool_name, context); post a one-line
    recovery notice when the episode had 2+ failures.

    Call after an operation that previously hit report_error() succeeds.
    A single failed cycle that already alerted gets no second message — the
    notice is only worth the channel noise for sustained incidents.
    Returns True only when a recovery message was posted. Never raises.
    """
    try:
        context_key = (context or "")[:_SUPPRESSION_KEY_MAX_CHARS]
        from database.connection import get_connection
        conn = get_connection()
        try:
            row = conn.execute(
                "SELECT episode_count, first_seen FROM error_alert_state "
                "WHERE tool_name = %s AND context_key = %s",
                (tool_name, context_key),
            ).fetchone()
            if row is None:
                return False
            with conn:
                conn.execute(
                    "DELETE FROM error_alert_state "
                    "WHERE tool_name = %s AND context_key = %s",
                    (tool_name, context_key),
                )
        finally:
            conn.close()

        if row["episode_count"] < 2:
            return False

        if dry_run:
            logger.info(
                "[DRY RUN] Would post recovery notice for %s — %s", tool_name, context
            )
            return True

        channel_id = setup_channel(dry_run=dry_run)
        if channel_id is None:
            return False

        first_seen = row["first_seen"]
        since = f" since {first_seen:%Y-%m-%d %H:%M} UTC" if first_seen else ""
        text = (
            f":white_check_mark: Recovered: {tool_name.title()} — {context} "
            f"(failed {row['episode_count']} times{since})"
        )
        client = get_client("slack")
        response = client.chat_postMessage(channel=channel_id, text=text)
        return bool(response["ok"])
    except Exception as exc:
        logger.error("Unexpected error in report_recovery: %s", exc)
        return False


_topic_warning_logged = False

_DESIRED_TOPIC = "Simulation and automation errors — plain language only, no stack traces"
# The topic we want #automation-failure to carry. Exported for tests.
# Slack emits a `channel_topic` system message on every setTopic call regardless
# of whether the value changed, so we compare against this before calling the API
# to avoid spamming the channel on every fresh-process startup.


def _try_set_topic(client, channel_id: str, current_topic: Optional[str] = None) -> None:
    """Attempt to set the #automation-failure topic. Log once on scope failure.

    If ``current_topic`` already matches ``_DESIRED_TOPIC``, skip the API call —
    Slack posts a ``channel_topic`` system message on every ``conversations.setTopic``
    call, so re-setting an identical topic creates noise in the channel.
    """
    global _topic_warning_logged

    if current_topic == _DESIRED_TOPIC:
        logger.debug(
            "Topic on #automation-failure already set correctly, skipping setTopic call"
        )
        return

    try:
        client.conversations_setTopic(
            channel=channel_id,
            topic=_DESIRED_TOPIC,
        )
    except Exception as topic_exc:
        if not _topic_warning_logged:
            logger.warning(
                "Could not set topic on #automation-failure (missing scope — "
                "add channels:write.topic to the Slack bot at https://api.slack.com/apps): %s",
                topic_exc,
            )
            _topic_warning_logged = True
        else:
            logger.debug("Could not set topic on #automation-failure: %s", topic_exc)


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
                candidate_id = ch["id"]
                client.conversations_join(channel=candidate_id)
                # setTopic requires channels:write.topic scope — non-fatal if absent.
                # Pass the existing topic so we can skip the API call when it already
                # matches _DESIRED_TOPIC (avoids `channel_topic` sys-msg spam).
                current_topic = ch.get("topic", {}).get("value", "")
                _try_set_topic(client, candidate_id, current_topic=current_topic)
                _channel_id = candidate_id
                return _channel_id

        # Not found — create it
        create_response = client.conversations_create(name="automation-failure")
        candidate_id = create_response["channel"]["id"]
        _try_set_topic(client, candidate_id)
        _channel_id = candidate_id
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

        if category == "unknown" and isinstance(exc, Exception):
            what_happened = (
                f"{what_happened} ({_summarize_exception(exc)})"
            )

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

        # Duplicate suppression: at most one post per identical (tool, context)
        # per ALERT_SUPPRESSION_WINDOW_MINUTES. Suppressed alerts still count
        # toward the episode (see report_recovery) and return True — the error
        # was handled, there is just nothing new to tell the channel.
        suppressed_repeats = 0
        if not dry_run:
            should_post, suppressed_repeats = _check_suppression(tool_name, context)
            if not should_post:
                logger.info(
                    "Suppressed duplicate #automation-failure alert for %s — %s "
                    "(%d suppressed this window)",
                    tool_name, context, suppressed_repeats,
                )
                return True
        if suppressed_repeats:
            plural = "s" if suppressed_repeats != 1 else ""
            what_happened += (
                f" (occurred {suppressed_repeats} more time{plural} "
                "since the last alert)"
            )

        error_location = _extract_location(exc) if isinstance(exc, Exception) else None
        log_file = _get_log_file_name()

        blocks = _build_error_blocks(
            what_happened=what_happened,
            what_was_affected=context,
            what_to_do=what_to_do,
            severity=final_severity,
            tool_name=tool_name,
            header_text=header_text,
            error_location=error_location,
            log_file=log_file,
        )

        if dry_run:
            logger.info(
                "[DRY RUN] Would post to #automation-failure: %s — %s (log: %s%s)",
                header_text,
                what_happened,
                log_file,
                f", location: {error_location}" if error_location else "",
            )
            return True

        client = get_client("slack")
        top_level_text = _truncate_slack_text(
            f"{header_text} — {what_happened}",
            _SLACK_TOP_LEVEL_TEXT_LIMIT,
        )
        response = client.chat_postMessage(
            channel=channel_id,
            text=top_level_text,
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
            log_file=_get_log_file_name(),
        )

        if dry_run:
            logger.info(
                "[DRY RUN] Would post reconciliation finding to #automation-failure: %s",
                what_happened,
            )
            return True

        client = get_client("slack")
        top_level_text = _truncate_slack_text(
            f"Data Mismatch Detected — {what_happened}",
            _SLACK_TOP_LEVEL_TEXT_LIMIT,
        )
        response = client.chat_postMessage(
            channel=channel_id,
            text=top_level_text,
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
