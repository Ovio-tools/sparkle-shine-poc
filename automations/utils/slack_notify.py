"""
automations/utils/slack_notify.py

Posts messages to Slack channels by name.
Caches the channel-name → channel-ID mapping within each process session
so we only call conversations.list once.

Resolution order for a channel ID:
  1. In-process cache (already resolved this session)
  2. Environment variables  SLACK_CHANNEL_<UPPER_NAME>  (e.g. SLACK_CHANNEL_OPERATIONS)
     — avoids conversations.list entirely for known private channels; the bot
       only needs channels:read + chat:write, not groups:read.
  3. conversations.list fallback (public channels only to avoid scope errors)
"""
import os
from typing import Optional

# Module-level cache: { channel_name: channel_id }
_channel_id_cache: dict = {}

# Env-var channel IDs: populated once at import time.
# Key = channel name (no #), value = channel ID string.
def _build_env_channel_map() -> dict:
    mapping: dict = {}
    prefix = "SLACK_CHANNEL_"
    for key, value in os.environ.items():
        if key.startswith(prefix) and value:
            channel_name = key[len(prefix):].lower().replace("_", "-")
            mapping[channel_name] = value
    return mapping

_ENV_CHANNEL_IDS: dict = _build_env_channel_map()


def _resolve_channel_id(client, channel_name: str) -> str:
    """
    Return the Slack channel ID for the given channel name.

    Checks env-var IDs before calling conversations.list so that private
    channels (which require groups:read) can be resolved without that scope.
    Raises ValueError if the channel cannot be resolved by any method.
    """
    name = channel_name.lstrip("#")

    # 1. In-process cache
    if name in _channel_id_cache:
        return _channel_id_cache[name]

    # 2. Environment variable  SLACK_CHANNEL_<NAME>
    # Also check os.environ live in case load_dotenv() was called after this module was imported
    live_env_key = f"SLACK_CHANNEL_{name.upper().replace('-', '_')}"
    env_id = os.environ.get(live_env_key) or _ENV_CHANNEL_IDS.get(name, "")
    if env_id:
        _channel_id_cache[name] = env_id
        return env_id

    # 3. conversations.list — public channels only (channels:read is sufficient)
    cursor = None
    while True:
        kwargs: dict = {"limit": 200, "types": "public_channel"}
        if cursor:
            kwargs["cursor"] = cursor

        response = client.conversations_list(**kwargs)
        for channel in response.get("channels", []):
            _channel_id_cache[channel["name"]] = channel["id"]

        next_cursor = (
            response.get("response_metadata", {}).get("next_cursor") or ""
        )
        if not next_cursor:
            break
        cursor = next_cursor

    if name not in _channel_id_cache:
        raise ValueError(
            f"Slack channel '#{name}' not found. "
            "Set SLACK_CHANNEL_{NAME_UPPER} in .env or add the bot to the channel."
        )
    return _channel_id_cache[name]


def post_slack_message(
    client,
    channel_name: str,
    text: str,
    blocks: Optional[list] = None,
) -> dict:
    """
    Post a message to a Slack channel identified by name (e.g. 'operations').

    If the bot is not yet a member of the channel, joins it automatically
    and retries the post once.

    Returns the Slack API response dict.
    Raises ValueError if the channel cannot be resolved.
    """
    from slack_sdk.errors import SlackApiError

    channel_id = _resolve_channel_id(client, channel_name)

    kwargs: dict = {"channel": channel_id, "text": text}
    if blocks is not None:
        kwargs["blocks"] = blocks

    try:
        return client.chat_postMessage(**kwargs)
    except SlackApiError as exc:
        if exc.response.get("error") == "not_in_channel":
            client.conversations_join(channel=channel_id)
            return client.chat_postMessage(**kwargs)
        raise
