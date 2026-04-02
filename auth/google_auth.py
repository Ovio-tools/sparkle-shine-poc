"""
Google Workspace authentication via OAuth 2.0 (google-auth-oauthlib).

Scopes covered: Drive, Docs, Sheets, Calendar, Gmail (metadata).
Token file: token.json (path from GOOGLE_TOKEN_FILE env var, default token.json).
Credentials file: credentials.json (path from GOOGLE_CREDENTIALS_FILE env var).

get_google_credentials() auto-refreshes when the token is expired.
If no valid token file exists it launches the browser consent flow.
"""
from __future__ import annotations

import os
import sys
import json
import logging

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from credentials import get_credential
from auth import token_store

logger = logging.getLogger(__name__)

_SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/documents",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/calendar",
    "https://www.googleapis.com/auth/gmail.readonly",
]

_PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))


def _credentials_file() -> str:
    fname = get_credential("GOOGLE_CREDENTIALS_FILE")
    if os.path.isabs(fname):
        candidates = [fname]
    else:
        # Search: project root, then one level up (parent workspace dir)
        candidates = [
            os.path.join(_PROJECT_ROOT, fname),
            os.path.join(os.path.dirname(_PROJECT_ROOT), fname),
        ]
    for path in candidates:
        if os.path.exists(path):
            return path
    raise FileNotFoundError(
        f"Google credentials file '{fname}' not found in:\n"
        + "\n".join(f"  {p}" for p in candidates)
        + "\nDownload it from the Google Cloud Console and place it at one of these paths."
    )


def _token_file() -> str:
    fname = os.getenv("GOOGLE_TOKEN_FILE", "token.json")
    if os.path.isabs(fname):
        return fname
    # Prefer an existing token in parent dir; fall back to project root
    parent_path = os.path.join(os.path.dirname(_PROJECT_ROOT), fname)
    if os.path.exists(parent_path):
        return parent_path
    return os.path.join(_PROJECT_ROOT, fname)


# ------------------------------------------------------------------ #
# Core credential loader
# ------------------------------------------------------------------ #

def _build_creds_from_dict(data: dict) -> Credentials | None:
    """Construct a Credentials object from a token dict (e.g. from DB or env vars)."""
    try:
        return Credentials(
            token=data.get("token"),
            refresh_token=data.get("refresh_token"),
            token_uri=data.get("token_uri", "https://oauth2.googleapis.com/token"),
            client_id=data.get("client_id") or os.getenv("GOOGLE_CLIENT_ID") or get_credential("GOOGLE_CLIENT_ID"),
            client_secret=data.get("client_secret") or os.getenv("GOOGLE_CLIENT_SECRET") or get_credential("GOOGLE_CLIENT_SECRET"),
            scopes=data.get("scopes", _SCOPES),
        )
    except Exception:
        return None


def get_google_credentials() -> Credentials:
    """
    Return valid Google Credentials.
    Order: DB -> JSON file -> env vars (all via token_store) -> browser consent flow.
    After any refresh, updated credentials are saved to DB.
    """
    token_path = _token_file()
    creds = None

    # Load tokens via token_store (DB -> JSON -> env vars)
    token_data = token_store.load_tokens(
        "google", token_path if os.path.exists(token_path) else None
    )

    # Scope check: only applies when data came from token.json (includes "scopes" key).
    # DB and env var dicts don't have a "scopes" key, so this is skipped for them.
    if token_data.get("refresh_token") and "scopes" in token_data:
        _stored_raw = token_data["scopes"]
        if isinstance(_stored_raw, list):
            _stored_scopes = set(_stored_raw)
        else:
            _stored_scopes = set(str(_stored_raw).split())
        if not all(s in _stored_scopes for s in _SCOPES):
            # Scopes insufficient — delete stale token.json, fall through to browser flow
            if os.path.exists(token_path):
                os.remove(token_path)
            token_data = {}

    if token_data.get("refresh_token"):
        creds = _build_creds_from_dict(token_data)

    if not creds or not creds.valid:
        if creds and creds.refresh_token and (not creds.token or creds.expired):
            try:
                creds.refresh(Request())
            except Exception as exc:
                raise RuntimeError(
                    f"Google token refresh failed: {exc}. "
                    "Check that GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, and "
                    "GOOGLE_REFRESH_TOKEN are correct."
                ) from exc

            # Save refreshed credentials to token.json (local dev convenience)
            try:
                with open(token_path, "w") as f:
                    f.write(creds.to_json())
            except (OSError, PermissionError):
                logger.debug("[google] Could not write token.json (read-only filesystem?)")

            # Save refreshed credentials to DB (primary persistence)
            token_store.save_tokens("google", json.loads(creds.to_json()))
        else:
            # Browser consent flow — only works locally with credentials.json
            try:
                creds_file = _credentials_file()
            except FileNotFoundError:
                raise RuntimeError(
                    "No valid Google credentials available. "
                    "Set GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, and GOOGLE_REFRESH_TOKEN "
                    "as environment variables (for Railway/CI), or provide credentials.json "
                    "for the browser consent flow (local dev)."
                )
            flow = InstalledAppFlow.from_client_secrets_file(creds_file, _SCOPES)
            creds = flow.run_local_server(port=8025, open_browser=True)

            try:
                with open(token_path, "w") as f:
                    f.write(creds.to_json())
            except (OSError, PermissionError):
                logger.debug("[google] Could not write token.json (read-only filesystem?)")

            token_store.save_tokens("google", json.loads(creds.to_json()))

    return creds


# ------------------------------------------------------------------ #
# Service factories
# ------------------------------------------------------------------ #

def get_drive_service():
    """Return a Google Drive v3 service."""
    return build("drive", "v3", credentials=get_google_credentials())


def get_docs_service():
    """Return a Google Docs v1 service."""
    return build("docs", "v1", credentials=get_google_credentials())


def get_sheets_service():
    """Return a Google Sheets v4 service."""
    return build("sheets", "v4", credentials=get_google_credentials())


def get_calendar_service():
    """Return a Google Calendar v3 service."""
    return build("calendar", "v3", credentials=get_google_credentials())


def get_gmail_service():
    """Return a Gmail v1 service (metadata scope only)."""
    return build("gmail", "v1", credentials=get_google_credentials())
