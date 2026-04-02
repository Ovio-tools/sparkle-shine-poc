"""
Four-tier token storage: DB -> JSON file -> env vars -> empty dict.

load_tokens(tool_name, json_path=None) -> dict
save_tokens(tool_name, token_data, json_path=None) -> None

Uses %s placeholders (PostgreSQL). All SQL is PostgreSQL-native.
"""
from __future__ import annotations

import json
import logging
import os

logger = logging.getLogger(__name__)

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS oauth_tokens (
    tool_name   TEXT PRIMARY KEY,
    token_data  JSONB NOT NULL,
    updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""

_SELECT_SQL = "SELECT token_data FROM oauth_tokens WHERE tool_name = %s"

_UPSERT_SQL = """
INSERT INTO oauth_tokens (tool_name, token_data, updated_at)
VALUES (%s, %s::jsonb, CURRENT_TIMESTAMP)
ON CONFLICT (tool_name) DO UPDATE SET
    token_data = EXCLUDED.token_data,
    updated_at = CURRENT_TIMESTAMP
"""


def _load_from_db(tool_name: str) -> dict | None:
    try:
        from database.connection import get_connection
        with get_connection() as conn:
            cursor = conn.execute(_SELECT_SQL, (tool_name,))
            row = cursor.fetchone()
            if row:
                data = row["token_data"]
                return data if isinstance(data, dict) else json.loads(data)
    except Exception as exc:
        logger.debug("[token_store] DB load failed for %s: %s", tool_name, exc)
    return None


def _save_to_db(tool_name: str, token_data: dict) -> None:
    try:
        from database.connection import get_connection
        with get_connection() as conn:
            conn.execute(_CREATE_TABLE_SQL)
            conn.execute(_UPSERT_SQL, (tool_name, json.dumps(token_data)))
    except Exception as exc:
        logger.warning("[token_store] DB save failed for %s: %s", tool_name, exc)


_ENV_PREFIX_MAP = {
    "quickbooks": "QBO",
}


def _load_from_env(tool_name: str) -> dict | None:
    """Load tokens from environment variables (bootstrap-only fallback)."""
    prefix = _ENV_PREFIX_MAP.get(tool_name, tool_name.upper())
    refresh = os.getenv(f"{prefix}_REFRESH_TOKEN")
    if not refresh:
        return None
    result = {"refresh_token": refresh}
    access = os.getenv(f"{prefix}_ACCESS_TOKEN")
    if access:
        result["access_token"] = access
    logger.debug("[token_store] Bootstrapped %s tokens from env vars", tool_name)
    return result


def load_tokens(tool_name: str, json_path: str | None = None) -> dict:
    """Load tokens using four-tier fallback: DB -> JSON file -> env vars -> empty dict."""
    data = _load_from_db(tool_name)
    if data:
        return data

    if json_path and os.path.exists(json_path):
        try:
            with open(json_path) as f:
                return json.load(f)
        except Exception as exc:
            logger.debug("[token_store] JSON load failed for %s: %s", json_path, exc)

    data = _load_from_env(tool_name)
    if data:
        return data

    return {}


def save_tokens(tool_name: str, token_data: dict, json_path: str | None = None) -> None:
    """Save tokens to DB (primary) and JSON file (backwards-compat fallback)."""
    _save_to_db(tool_name, token_data)

    if json_path:
        try:
            with open(json_path, "w") as f:
                json.dump(token_data, f, indent=2)
        except Exception as exc:
            logger.warning("[token_store] JSON save failed for %s: %s", json_path, exc)
