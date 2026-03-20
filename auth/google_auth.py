"""
Google Workspace authentication via OAuth 2.0 (google-auth-oauthlib).

Scopes covered: Drive, Docs, Sheets, Calendar, Gmail (metadata).
Token file: token.json (path from GOOGLE_TOKEN_FILE env var, default token.json).
Credentials file: credentials.json (path from GOOGLE_CREDENTIALS_FILE env var).

get_google_credentials() auto-refreshes when the token is expired.
If no valid token file exists it launches the browser consent flow.
"""
import os
import sys
import json

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from credentials import get_credential

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

def get_google_credentials() -> Credentials:
    """
    Return valid Google Credentials.
    - Loads token.json if present and refreshes if expired.
    - Falls back to browser consent flow if no valid token exists.
    """
    token_path = _token_file()
    creds = None

    if os.path.exists(token_path):
        # Read the raw token to check what scopes were actually granted
        with open(token_path) as _f:
            _raw = json.load(_f)
        _stored_raw = _raw.get("scopes", "")
        if isinstance(_stored_raw, list):
            _stored_scopes = set(_stored_raw)
        else:
            _stored_scopes = set(str(_stored_raw).split())

        if all(s in _stored_scopes for s in _SCOPES):
            creds = Credentials.from_authorized_user_file(token_path, _SCOPES)
        else:
            # Scopes insufficient — delete stale token to force re-auth
            os.remove(token_path)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                _credentials_file(), _SCOPES
            )
            creds = flow.run_local_server(port=8025, open_browser=True)

        with open(token_path, "w") as f:
            f.write(creds.to_json())

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
