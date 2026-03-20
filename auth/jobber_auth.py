"""
Jobber authentication.

Token precedence:
  1. .jobber_tokens.json  (written by run_initial_auth or a prior refresh)
  2. JOBBER_ACCESS_TOKEN in .env  (pre-authorised token supplied by the sandbox)

Auto-refresh fires when the stored token is within 5 minutes of expiry.
If no refresh_token is available the env-var token is returned as-is.
"""
import json
import os
import sys
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlencode, urlparse, parse_qs

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from credentials import get_credential

_TOKEN_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".jobber_tokens.json")
_AUTHORIZE_URL = "https://api.getjobber.com/api/oauth/authorize"
_TOKEN_URL = "https://api.getjobber.com/api/oauth/token"
_GRAPHQL_URL = "https://api.getjobber.com/api/graphql"
_REDIRECT_URI = "http://localhost:8019/callback"
_EXPIRY_BUFFER = 300  # seconds — refresh if within 5 min of expiry


# ------------------------------------------------------------------ #
# Token file helpers
# ------------------------------------------------------------------ #

def _load_tokens() -> dict:
    if os.path.exists(_TOKEN_FILE):
        with open(_TOKEN_FILE) as f:
            return json.load(f)
    return {}


def _save_tokens(data: dict) -> None:
    with open(_TOKEN_FILE, "w") as f:
        json.dump(data, f, indent=2)


# ------------------------------------------------------------------ #
# Refresh
# ------------------------------------------------------------------ #

def _refresh_token(refresh_token: str) -> dict:
    """Exchange a refresh token for a new access token. Returns updated token dict."""
    client_id = get_credential("JOBBER_CLIENT_ID") if os.getenv("JOBBER_CLIENT_ID") else None
    client_secret = get_credential("JOBBER_CLIENT_SECRET") if os.getenv("JOBBER_CLIENT_SECRET") else None

    if not client_id or not client_secret:
        raise RuntimeError(
            "Cannot refresh Jobber token: JOBBER_CLIENT_ID / JOBBER_CLIENT_SECRET not set. "
            "Re-run run_initial_auth() or supply a fresh JOBBER_ACCESS_TOKEN in .env."
        )

    resp = requests.post(
        _TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    data["expires_at"] = time.time() + data.get("expires_in", 3600)
    return data


# ------------------------------------------------------------------ #
# Public API
# ------------------------------------------------------------------ #

def get_jobber_token() -> str:
    """
    Return a valid Jobber access token.
    Order: token file → auto-refresh → env-var fallback.
    """
    tokens = _load_tokens()

    if tokens.get("access_token"):
        expires_at = tokens.get("expires_at", 0)
        if time.time() < expires_at - _EXPIRY_BUFFER:
            return tokens["access_token"]

        # Try to refresh
        if tokens.get("refresh_token"):
            try:
                new_tokens = _refresh_token(tokens["refresh_token"])
                _save_tokens(new_tokens)
                return new_tokens["access_token"]
            except Exception as exc:
                print(f"[jobber] Token refresh failed ({exc}), falling back to env token.")

    # Fall back to the env-var token (e.g. sandbox pre-auth token)
    return get_credential("JOBBER_ACCESS_TOKEN")


def get_jobber_session() -> requests.Session:
    """Return a requests.Session with Jobber Bearer auth and version header."""
    token = get_jobber_token()
    api_version = os.getenv("JOBBER_API_VERSION", "2026-03-10")

    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {token}",
        "X-JOBBER-GRAPHQL-VERSION": api_version,
        "Content-Type": "application/json",
    })
    return session


def run_initial_auth() -> None:
    """
    Full OAuth 2.0 authorisation code flow.
    Opens a browser, spins up a local callback server, exchanges the code,
    and saves tokens to .jobber_tokens.json.
    """
    client_id = get_credential("JOBBER_CLIENT_ID")
    client_secret = get_credential("JOBBER_CLIENT_SECRET")

    params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": _REDIRECT_URI,
    }
    auth_url = f"{_AUTHORIZE_URL}?{urlencode(params)}"

    received_code: dict = {}

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            qs = parse_qs(urlparse(self.path).query)
            if "code" in qs:
                received_code["value"] = qs["code"][0]
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"<h2>Jobber auth complete. You can close this tab.</h2>")
            else:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"Missing code parameter.")

        def log_message(self, *args):
            pass  # silence server logs

    server = HTTPServer(("localhost", 8019), _Handler)
    print(f"Opening browser for Jobber OAuth...\n  {auth_url}\n")
    webbrowser.open(auth_url)
    server.handle_request()

    code = received_code.get("value")
    if not code:
        raise RuntimeError("No authorisation code received from Jobber.")

    resp = requests.post(
        _TOKEN_URL,
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": _REDIRECT_URI,
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    data["expires_at"] = time.time() + data.get("expires_in", 3600)
    _save_tokens(data)
    print(f"Jobber tokens saved to {_TOKEN_FILE}")
