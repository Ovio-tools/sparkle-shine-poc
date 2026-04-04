"""
automations/helpers/slack_sales_notify.py

Posts Block Kit notifications to #sales about new lead drafts.
Used by Automation #7: Sales Research & Outreach Agent Chain.
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path

from auth import get_client
from intelligence.slack_publisher import resolve_channel_id

logger = logging.getLogger("automation_07")

_CHANNEL = "sales"
_DM_USER = "tools"  # Slack username of the team member to DM

# ---------------------------------------------------------------------------
# DM channel resolver (username -> DM channel ID)
# ---------------------------------------------------------------------------

_dm_channel_id_cache: dict[str, str] = {}


def _resolve_dm_channel(username: str) -> str | None:
    """
    Resolve a Slack username to a DM channel ID.

    Resolution order:
      1. Module-level cache
      2. SLACK_DM_{UPPER_NAME} environment variable (holds the member user ID)
      3. users.list API — find the member whose name or display_name matches
    Opens the DM via conversations.open and caches the resulting channel ID.
    Returns None if the user cannot be found.
    """
    if username in _dm_channel_id_cache:
        return _dm_channel_id_cache[username]

    slack_client = get_client("slack")

    # Env-var shortcut: SLACK_DM_TOOLS=U01234ABCDE skips users.list entirely
    env_key = "SLACK_DM_" + username.upper()
    user_id = os.environ.get(env_key, "").strip()

    if not user_id:
        cursor = None
        while True:
            kwargs: dict = {"limit": 200}
            if cursor:
                kwargs["cursor"] = cursor
            resp = slack_client.users_list(**kwargs)
            for member in resp.get("members", []):
                profile = member.get("profile", {})
                if (
                    member.get("name") == username
                    or profile.get("display_name") == username
                ):
                    user_id = member["id"]
                    break
            if user_id:
                break
            next_cursor = resp.get("response_metadata", {}).get("next_cursor") or ""
            if not next_cursor:
                break
            cursor = next_cursor

    if not user_id:
        logger.warning("Could not find Slack user '%s' for DM", username)
        return None

    resp = slack_client.conversations_open(users=[user_id])
    channel_id = resp.get("channel", {}).get("id")
    if channel_id:
        _dm_channel_id_cache[username] = channel_id
    return channel_id


# ---------------------------------------------------------------------------
# portal_id loader (config/tool_ids.json -> env var fallback)
# ---------------------------------------------------------------------------

_portal_id_cache: str | None = None
_portal_id_loaded: bool = False


def _get_portal_id() -> str:
    global _portal_id_cache, _portal_id_loaded
    if _portal_id_loaded:
        return _portal_id_cache or ""

    _portal_id_loaded = True

    tool_ids_path = Path(__file__).parent.parent.parent / "config" / "tool_ids.json"
    try:
        with open(tool_ids_path) as f:
            data = json.load(f)
        pid = data.get("hubspot", {}).get("portal_id")
        if pid:
            _portal_id_cache = str(pid)
            return _portal_id_cache
    except Exception:
        pass

    _portal_id_cache = os.environ.get("HUBSPOT_PORTAL_ID", "")
    return _portal_id_cache


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

_CONTACT_TYPE_DISPLAY = {
    "residential": "Residential",
    "commercial": "Commercial",
}

_LEAD_SOURCE_DISPLAY = {
    "ORGANIC_SEARCH": "Organic Search",
    "PAID_SEARCH": "Google Ads",
    "REFERRAL": "Referral",
    "DIRECT_TRAFFIC": "Website",
    "EMAIL_MARKETING": "Email Campaign",
    "SOCIAL_MEDIA": "Social Media",
    "OFFLINE": "Offline",
    "OTHER": "Other",
}

def _contact_type_display(contact: dict) -> str:
    raw = (contact.get("contact_type") or contact.get("client_type") or "").lower()
    return _CONTACT_TYPE_DISPLAY.get(raw, "Unknown (hybrid draft)")


def _lead_source_display(contact: dict) -> str:
    raw = (
        contact.get("hs_analytics_source")
        or contact.get("lead_source")
        or ""
    ).upper()
    return _LEAD_SOURCE_DISPLAY.get(raw, raw.replace("_", " ").title() if raw else "Unknown")


def _format_estimated_value(jobs_output: dict) -> str:
    val = jobs_output.get("estimated_annual_value")
    if val is None:
        return "N/A"
    try:
        return f"~${int(val):,}/year"
    except (TypeError, ValueError):
        return "N/A"


def _match_summary(jobs_output: dict) -> str:
    matches = jobs_output.get("matches") or []
    if not matches:
        return "No strong match"
    m = matches[0]
    desc = m.get("description", "")
    price = m.get("price")
    if price:
        try:
            desc = f"{desc} (${int(price):,}/visit)"
        except (TypeError, ValueError):
            pass
    return desc or "No strong match"


def _hubspot_contact_url(contact: dict) -> str:
    portal_id = _get_portal_id()
    contact_id = contact.get("id") or contact.get("hubspot_id") or ""
    if portal_id and contact_id:
        return f"https://app.hubspot.com/contacts/{portal_id}/contact/{contact_id}"
    return "https://app.hubspot.com/"


# ---------------------------------------------------------------------------
# Block Kit builders
# ---------------------------------------------------------------------------

def _build_draft_ready_blocks(
    contact: dict,
    research_output: dict,
    jobs_output: dict,
    gmail_result: dict,
    template_info: dict,
) -> list[dict]:
    name = f"{contact.get('firstname', '')} {contact.get('lastname', '')}".strip() or "Unknown"
    gmail_link = gmail_result.get("gmail_link", "")
    match_desc = _match_summary(jobs_output)

    return [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "Email Draft for a New Lead is Ready", "emoji": True},
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Contact:*\n{name}"},
                {"type": "mrkdwn", "text": f"*Type:*\n{_contact_type_display(contact)}"},
            ],
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    "*Similar jobs we have done:*\n"
                    "_Here's what I pulled from our past jobs and used in drafting a personalized response_\n\n"
                    f">{match_desc}"
                ),
            },
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Open Gmail Draft"},
                    "url": gmail_link,
                    "style": "primary",
                },
            ],
        },
    ]


def _build_draft_failed_blocks(
    contact: dict,
    error_message: str | None,
) -> list[dict]:
    name = f"{contact.get('firstname', '')} {contact.get('lastname', '')}".strip() or "Unknown"
    email = contact.get("email", "")
    hubspot_url = _hubspot_contact_url(contact)

    blocks: list[dict] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "Lead Draft Failed \u2014 Manual Follow-Up Needed",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Contact:*\n{name}"},
                {"type": "mrkdwn", "text": f"*Email:*\n{email or 'Unknown'}"},
                {"type": "mrkdwn", "text": f"*Type:*\n{_contact_type_display(contact)}"},
                {"type": "mrkdwn", "text": f"*Source:*\n{_lead_source_display(contact)}"},
            ],
        },
    ]

    if error_message:
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f":warning: {error_message}"},
            }
        )

    blocks.append(
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "View in HubSpot"},
                    "url": hubspot_url,
                }
            ],
        }
    )

    return blocks


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def notify_sales_channel(
    contact: dict,
    research_output: dict,
    jobs_output: dict,
    gmail_result: dict | None,
    template_info: dict,
    error_message: str | None = None,
) -> str | None:
    """
    Post a Block Kit message to #sales about a new lead draft.

    Returns the Slack message timestamp (ts) on success, None on failure.
    If gmail_result is None, posts an error notification instead of the
    standard draft-ready message.
    """
    if gmail_result is not None:
        blocks = _build_draft_ready_blocks(
            contact, research_output, jobs_output, gmail_result, template_info
        )
        fallback = (
            f"New lead draft ready: "
            f"{contact.get('firstname', '')} {contact.get('lastname', '')}".strip()
        )
    else:
        blocks = _build_draft_failed_blocks(contact, error_message)
        fallback = (
            f"Lead draft failed: "
            f"{contact.get('firstname', '')} {contact.get('lastname', '')}".strip()
        )

    try:
        channel_id = resolve_channel_id(_CHANNEL)
    except ValueError:
        logger.error("Could not resolve Slack channel #%s", _CHANNEL)
        return None

    slack_client = get_client("slack")
    primary_ts: str | None = None

    try:
        response = slack_client.chat_postMessage(
            channel=channel_id,
            text=fallback,
            blocks=blocks,
        )
        if response.get("ok"):
            primary_ts = response.get("ts")
            logger.info(
                "Posted sales notification to #%s (ts=%s, mode=%s)",
                _CHANNEL,
                primary_ts,
                "draft_ready" if gmail_result is not None else "draft_failed",
            )
        else:
            logger.error("Slack API returned ok=False: %s", response.get("error"))

    except Exception:
        logger.exception("Failed to post sales notification to #%s", _CHANNEL)

    # Also DM the 'tools' user with the same notification
    dm_channel_id = _resolve_dm_channel(_DM_USER)
    if dm_channel_id:
        try:
            dm_response = slack_client.chat_postMessage(
                channel=dm_channel_id,
                text=fallback,
                blocks=blocks,
            )
            if not dm_response.get("ok"):
                logger.error(
                    "Slack API returned ok=False for DM to '%s': %s",
                    _DM_USER,
                    dm_response.get("error"),
                )
        except Exception:
            logger.exception("Failed to DM '%s' on Slack", _DM_USER)

    return primary_ts
