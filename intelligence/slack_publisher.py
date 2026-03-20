"""
intelligence/slack_publisher.py

Posts the daily briefing and alert messages to Slack using the Block Kit API.

Usage:
    python -m intelligence.slack_publisher --test
    python -m intelligence.slack_publisher --test-alert
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))

from auth import get_client
from intelligence.briefing_generator import Briefing
from intelligence.config import SLACK_CONFIG
from intelligence.logging_config import setup_logging

logger = setup_logging(__name__)

# ---------------------------------------------------------------------------
# Module-level channel ID cache: { channel_name_without_hash: channel_id }
# ---------------------------------------------------------------------------

_channel_id_cache: dict[str, str] = {}

# ---------------------------------------------------------------------------
# Section headers used to split the briefing content into Block Kit sections
# ---------------------------------------------------------------------------

_SECTION_HEADERS = [
    "Yesterday's Performance",
    "Cash Position",
    "Today's Schedule",
    "Sales Pipeline",
    "Action Items",
    "One Opportunity",
]


# ---------------------------------------------------------------------------
# Channel resolution
# ---------------------------------------------------------------------------

def resolve_channel_id(channel_name: str) -> str:
    """Convert #channel-name (or channel-name) to a Slack channel ID.

    Resolution order:
      1. In-process cache (avoids repeated API calls within the same run)
      2. SLACK_CHANNEL_<NAME> environment variable (e.g. for private channels)
      3. conversations.list API (public channels, paginated)

    Raises ValueError if the channel cannot be resolved.
    """
    name = channel_name.lstrip("#").strip()

    # 1. In-process cache
    if name in _channel_id_cache:
        return _channel_id_cache[name]

    # 2. Environment variable SLACK_CHANNEL_<UPPER_NAME>
    env_key = "SLACK_CHANNEL_" + name.upper().replace("-", "_")
    env_id = os.environ.get(env_key, "").strip()
    if env_id:
        _channel_id_cache[name] = env_id
        logger.debug("Resolved #%s from env var %s -> %s", name, env_key, env_id)
        return env_id

    # 3. conversations.list (public channels, paginated)
    client = get_client("slack")
    cursor = None
    while True:
        kwargs: dict = {"limit": 200, "types": "public_channel"}
        if cursor:
            kwargs["cursor"] = cursor

        response = client.conversations_list(**kwargs)
        for ch in response.get("channels", []):
            _channel_id_cache[ch["name"]] = ch["id"]

        next_cursor = response.get("response_metadata", {}).get("next_cursor") or ""
        if not next_cursor:
            break
        cursor = next_cursor

    if name not in _channel_id_cache:
        raise ValueError(
            f"Slack channel '#{name}' not found. "
            "Add the bot to the channel or set SLACK_CHANNEL_{NAME_UPPER} in .env."
        )

    logger.debug("Resolved #%s via conversations.list -> %s", name, _channel_id_cache[name])
    return _channel_id_cache[name]


# ---------------------------------------------------------------------------
# Briefing Block Kit builder
# ---------------------------------------------------------------------------

def _split_briefing_into_sections(content: str) -> list[str]:
    """
    Split the briefing content into up to 6 named sections.

    The content may use any of: *HEADING*, **Heading**, or plain text headers
    matching the known section names.  We find split points by scanning for
    lines that contain a known section header (case-insensitive) and slice.
    """
    lines = content.splitlines()

    # Build a list of (line_index, header_label) for each detected section start.
    split_points: list[tuple[int, str]] = []
    for i, line in enumerate(lines):
        stripped = line.strip().strip("*_").strip()
        for header in _SECTION_HEADERS:
            if header.lower() in stripped.lower():
                split_points.append((i, header))
                break

    if not split_points:
        # No headers found — return the whole content as one block (safety fallback)
        return [content.strip()]

    sections: list[str] = []
    for idx, (start, _) in enumerate(split_points):
        end = split_points[idx + 1][0] if idx + 1 < len(split_points) else len(lines)
        section_lines = lines[start:end]
        sections.append("\n".join(section_lines).strip())

    return [s for s in sections if s]


def _build_briefing_blocks(briefing: Briefing) -> list[dict]:
    """Assemble the Block Kit payload for a daily briefing."""
    date_str = briefing.date
    generated_time = time.strftime("%I:%M %p")
    token_count = briefing.input_tokens + briefing.output_tokens

    blocks: list[dict] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f":sunrise: Daily Briefing \u2014 {date_str}",
                "emoji": True,
            },
        },
        {"type": "divider"},
    ]

    sections = _split_briefing_into_sections(briefing.content_slack)
    for i, section_text in enumerate(sections):
        # Slack section text cap is 3000 chars; truncate gracefully
        if len(section_text) > 2990:
            section_text = section_text[:2987] + "\u2026"
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": section_text},
            }
        )
        if i < len(sections) - 1:
            blocks.append({"type": "divider"})

    blocks.append({"type": "divider"})
    blocks.append(
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": (
                        f"_Generated at {generated_time} | "
                        f"Model: {briefing.model_used} | "
                        f"Tokens: {token_count}_"
                    ),
                }
            ],
        }
    )

    return blocks


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def post_briefing(briefing: Briefing, channel: str = None) -> bool:
    """Post the daily briefing to the #daily-briefing channel.

    Returns True if successful, False if failed.
    """
    channel = channel or SLACK_CONFIG["briefing_channel"]

    try:
        channel_id = resolve_channel_id(channel)
    except ValueError:
        # Try without the leading # as a fallback
        fallback = channel.lstrip("#")
        logger.error(
            "Channel '%s' not found; retrying as '%s'", channel, fallback
        )
        try:
            channel_id = resolve_channel_id(fallback)
        except ValueError:
            logger.error("Could not resolve Slack channel '%s'", channel)
            return False

    blocks = _build_briefing_blocks(briefing)

    # Fallback plain-text for clients that don't render blocks
    fallback_text = f":sunrise: Daily Briefing \u2014 {briefing.date}"

    slack_client = get_client("slack")

    for attempt in range(2):
        try:
            response = slack_client.chat_postMessage(
                channel=channel_id,
                text=fallback_text,
                blocks=blocks,
            )
            if response.get("ok"):
                logger.info(
                    "Briefing posted to #%s (ts=%s)",
                    channel.lstrip("#"),
                    response.get("ts"),
                )
                return True
            else:
                logger.error(
                    "Slack API returned ok=False: %s", response.get("error")
                )
                return False

        except Exception as exc:
            error_msg = str(exc)

            # Rate limited
            if "ratelimited" in error_msg.lower() or "429" in error_msg:
                if attempt == 0:
                    logger.warning("Rate limited by Slack; waiting 60 s then retrying")
                    time.sleep(60)
                    continue
                logger.error("Still rate limited after retry; giving up")
                return False

            # Invalid / revoked token
            if "invalid_auth" in error_msg or "token_revoked" in error_msg:
                logger.critical(
                    "Slack token is invalid or revoked. "
                    "Regenerate SLACK_BOT_TOKEN in .env. Error: %s",
                    error_msg,
                )
                return False

            logger.error("Unexpected error posting briefing to Slack: %s", error_msg)
            return False

    return False


def post_alert(message: str, channel: str, urgency: str = "warning") -> bool:
    """Post a single alert message to a specific Slack channel.

    urgency levels:
      - "info"     : no emoji prefix
      - "warning"  : :warning: prefix, yellow sidebar (attachment color #FFC107)
      - "critical" : :rotating_light: prefix, red sidebar (attachment color #D32F2F)

    Returns True if successful, False if failed.
    """
    emoji_prefix = {
        "info": "",
        "warning": ":warning: ",
        "critical": ":rotating_light: ",
    }.get(urgency, ":warning: ")

    attachment_color = {
        "info": "#2196F3",
        "warning": "#FFC107",
        "critical": "#D32F2F",
    }.get(urgency, "#FFC107")

    full_message = f"{emoji_prefix}{message}"

    # Block Kit: single section block
    blocks: list[dict] = [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": full_message},
        }
    ]

    # Colored sidebar via legacy attachments (Block Kit has no native color bar)
    attachments: list[dict] = [
        {
            "color": attachment_color,
            "fallback": full_message,
            "blocks": blocks,
        }
    ]

    try:
        channel_id = resolve_channel_id(channel)
    except ValueError:
        logger.error("Could not resolve Slack channel '%s' for alert", channel)
        return False

    slack_client = get_client("slack")

    for attempt in range(2):
        try:
            response = slack_client.chat_postMessage(
                channel=channel_id,
                text=full_message,
                attachments=attachments,
            )
            if response.get("ok"):
                logger.info(
                    "Alert posted to #%s (urgency=%s, ts=%s)",
                    channel.lstrip("#"),
                    urgency,
                    response.get("ts"),
                )
                return True
            else:
                logger.error(
                    "Slack API returned ok=False for alert: %s",
                    response.get("error"),
                )
                return False

        except Exception as exc:
            error_msg = str(exc)

            if "ratelimited" in error_msg.lower() or "429" in error_msg:
                if attempt == 0:
                    logger.warning("Rate limited posting alert; waiting 60 s")
                    time.sleep(60)
                    continue
                logger.error("Still rate limited for alert; giving up")
                return False

            if "invalid_auth" in error_msg or "token_revoked" in error_msg:
                logger.critical(
                    "Slack token invalid/revoked. Error: %s", error_msg
                )
                return False

            logger.error("Unexpected error posting alert to Slack: %s", error_msg)
            return False

    return False


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _run_test() -> None:
    """Post a test message to #daily-briefing."""
    from intelligence.briefing_generator import Briefing

    test_briefing = Briefing(
        date="TEST",
        content_slack=(
            "Test briefing post from Sparkle & Shine Intelligence Layer. "
            "If you see this, Slack publishing is working correctly."
        ),
        content_plain=(
            "Test briefing post from Sparkle & Shine Intelligence Layer. "
            "If you see this, Slack publishing is working correctly."
        ),
        model_used="test",
        input_tokens=0,
        output_tokens=0,
        generation_time_seconds=0.0,
        retry_count=0,
    )

    channel = SLACK_CONFIG["briefing_channel"]
    print(f"Posting test briefing to {channel} ...")
    ok = post_briefing(test_briefing, channel=channel)
    if ok:
        print("Test briefing posted successfully.")
    else:
        print("Test briefing FAILED. Check logs for details.")
        sys.exit(1)


def _run_test_alert() -> None:
    """Post a test alert to #operations."""
    channel = SLACK_CONFIG["alert_channel"]
    print(f"Posting test alert to {channel} ...")
    ok = post_alert(
        message="Test alert from Sparkle & Shine Intelligence Layer. Slack alert publishing is working correctly.",
        channel=channel,
        urgency="warning",
    )
    if ok:
        print("Test alert posted successfully.")
    else:
        print("Test alert FAILED. Check logs for details.")
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Slack publisher for the Sparkle & Shine Intelligence Layer."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--test",
        action="store_true",
        help="Post a test message to #daily-briefing.",
    )
    group.add_argument(
        "--test-alert",
        action="store_true",
        help="Post a test alert to #operations.",
    )
    args = parser.parse_args()

    from intelligence.logging_config import setup_logging as _setup
    _setup("intelligence")

    if args.test:
        _run_test()
    elif args.test_alert:
        _run_test_alert()


if __name__ == "__main__":
    main()
