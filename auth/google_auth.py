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
    "https://www.googleapis.com/auth/calendar",
    "https://www.googleapis.com/auth/documents",
    "https://www.googleapis.com/auth/drive",
    # gmail.compose is required for creating drafts via users.drafts.create
    # (Automation #7: Sales Research & Outreach Agent Chain).
    # Adding this scope invalidates the existing token.json on next run;
    # the browser consent flow will re-trigger automatically.
    "https://www.googleapis.com/auth/gmail.compose",
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
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


def _client_credentials() -> tuple[str | None, str | None]:
    """Return Google OAuth client credentials from env vars or credentials.json."""
    client_id = os.getenv("GOOGLE_CLIENT_ID")
    client_secret = os.getenv("GOOGLE_CLIENT_SECRET")
    if client_id and client_secret:
        return client_id, client_secret

    try:
        with open(_credentials_file()) as f:
            payload = json.load(f)
    except Exception:
        return None, None

    client_block = payload.get("installed") or payload.get("web") or {}
    return client_block.get("client_id"), client_block.get("client_secret")


# ------------------------------------------------------------------ #
# Core credential loader
# ------------------------------------------------------------------ #

def _build_creds_from_dict(data: dict) -> Credentials | None:
    """Construct a Credentials object from a token dict (e.g. from DB or env vars)."""
    try:
        client_id, client_secret = _client_credentials()
        return Credentials(
            token=data.get("token"),
            refresh_token=data.get("refresh_token"),
            token_uri=data.get("token_uri", "https://oauth2.googleapis.com/token"),
            client_id=data.get("client_id") or client_id,
            client_secret=data.get("client_secret") or client_secret,
            scopes=data.get("scopes", _SCOPES),
        )
    except Exception as exc:
        logger.warning("[google] Failed to build credentials from stored token: %s", exc)
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

    # Scope check: when stored scopes are present, verify they cover all required scopes.
    # If scopes are insufficient, try refreshing with the existing refresh token first
    # (the original consent may have included the scope even if the stored metadata is stale).
    # Only fall through to browser flow if refresh also fails.
    _scopes_insufficient = False
    _missing_scopes: list[str] = []
    if token_data.get("refresh_token") and "scopes" in token_data:
        _stored_raw = token_data["scopes"]
        if isinstance(_stored_raw, list):
            _stored_scopes = set(_stored_raw)
        else:
            _stored_scopes = set(str(_stored_raw).split())
        _missing_scopes = [s for s in _SCOPES if s not in _stored_scopes]
        if _missing_scopes:
            _scopes_insufficient = True
            logger.warning(
                "[google] Stored token missing scopes: %s. "
                "Attempting refresh with existing refresh token.",
                ", ".join(_missing_scopes),
            )
            # Delete stale token.json but keep token_data — we still need the refresh token
            if os.path.exists(token_path):
                os.remove(token_path)

    if token_data.get("refresh_token"):
        creds = _build_creds_from_dict(token_data)

    if not creds or not creds.valid or _scopes_insufficient:
        if creds and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as exc:
                if _scopes_insufficient:
                    # Refresh failed AND scopes are insufficient — need re-authorization.
                    # On headless (Railway/CI), raise a clear error naming the missing scopes.
                    try:
                        _credentials_file()
                        _has_creds_file = True
                    except FileNotFoundError:
                        _has_creds_file = False

                    if not _has_creds_file:
                        raise RuntimeError(
                            f"Google token is missing required scopes: {', '.join(_missing_scopes)}. "
                            "Token refresh failed, and browser re-authorization is unavailable "
                            "(no credentials.json). Re-authorize locally with the updated scopes, "
                            "then push the new token to the Railway DB."
                        ) from exc
                    # Fall through to browser flow below
                else:
                    raise RuntimeError(
                        f"Google token refresh failed: {exc}. "
                        "Check that GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, and "
                        "GOOGLE_REFRESH_TOKEN are correct."
                    ) from exc
            else:
                # Refresh succeeded — save updated credentials (now with correct scopes)
                try:
                    with open(token_path, "w") as f:
                        f.write(creds.to_json())
                except (OSError, PermissionError):
                    logger.debug("[google] Could not write token.json (read-only filesystem?)")

                token_store.save_tokens("google", json.loads(creds.to_json()))
                if _scopes_insufficient:
                    logger.info(
                        "[google] Token refresh succeeded — scope mismatch self-healed. "
                        "Updated token saved to DB."
                    )
                return creds

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
    """Return a Gmail v1 service."""
    return build("gmail", "v1", credentials=get_google_credentials())


def get_google_service(service_name: str = "drive", version: str | None = None):
    """Return an arbitrary Google API service object.

    Defaults match the most common services. Callers can request any
    service supported by googleapiclient.discovery.build().

    Examples:
        get_google_service("gmail", "v1")
        get_google_service("drive", "v3")
    """
    _DEFAULT_VERSIONS = {
        "drive": "v3",
        "docs": "v1",
        "sheets": "v4",
        "calendar": "v3",
        "gmail": "v1",
    }
    if version is None:
        version = _DEFAULT_VERSIONS.get(service_name, "v1")
    return build(service_name, version, credentials=get_google_credentials())
