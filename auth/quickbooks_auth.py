"""
QuickBooks Online (QBO) authentication.

Token precedence:
  1. .quickbooks_tokens.json  (written by run_initial_auth or a prior refresh)
  2. QBO_ACCESS_TOKEN in .env  (pre-authorised sandbox token)

Auto-refresh fires when the stored token is within 5 minutes of expiry.
"""
import json
import os
import sys
import time
import webbrowser
from base64 import b64encode
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlencode, urlparse, parse_qs

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from credentials import get_credential

_TOKEN_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".quickbooks_tokens.json")
_AUTHORIZE_URL = "https://appcenter.intuit.com/connect/oauth2"
_TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
_REDIRECT_URI = "http://localhost:8020/callback"
_SCOPES = "com.intuit.quickbooks.accounting"
_EXPIRY_BUFFER = 300  # seconds


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


def _basic_auth_header() -> str:
    client_id = get_credential("QUICKBOOKS_CLIENT_ID") if os.getenv("QUICKBOOKS_CLIENT_ID") else None
    client_secret = get_credential("QUICKBOOKS_CLIENT_SECRET") if os.getenv("QUICKBOOKS_CLIENT_SECRET") else None
    if not client_id or not client_secret:
        raise RuntimeError(
            "QUICKBOOKS_CLIENT_ID / QUICKBOOKS_CLIENT_SECRET not set. "
            "Cannot refresh token. Supply a fresh QBO_ACCESS_TOKEN in .env."
        )
    creds = b64encode(f"{client_id}:{client_secret}".encode()).decode()
    return f"Basic {creds}"


# ------------------------------------------------------------------ #
# Refresh
# ------------------------------------------------------------------ #

def _refresh_token(refresh_token: str) -> dict:
    resp = requests.post(
        _TOKEN_URL,
        headers={
            "Authorization": _basic_auth_header(),
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
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

def get_quickbooks_token() -> str:
    """
    Return a valid QBO access token.
    Order: token file → auto-refresh → env-var fallback.
    """
    tokens = _load_tokens()

    if tokens.get("access_token"):
        expires_at = tokens.get("expires_at", 0)
        if time.time() < expires_at - _EXPIRY_BUFFER:
            return tokens["access_token"]

        if tokens.get("refresh_token"):
            try:
                new_tokens = _refresh_token(tokens["refresh_token"])
                _save_tokens(new_tokens)
                return new_tokens["access_token"]
            except Exception as exc:
                print(f"[quickbooks] Token refresh failed ({exc}), falling back to env token.")

    return get_credential("QBO_ACCESS_TOKEN")


def get_quickbooks_headers() -> dict:
    """Return headers dict ready for QBO REST API calls."""
    return {
        "Authorization": f"Bearer {get_quickbooks_token()}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def get_company_id() -> str:
    return get_credential("QBO_COMPANY_ID")


def get_base_url() -> str:
    base = os.getenv("QBO_BASE_URL", "https://sandbox-quickbooks.api.intuit.com/v3/company")
    return f"{base}/{get_company_id()}"


def run_initial_auth() -> None:
    """
    Full OAuth 2.0 authorisation code flow for QBO.
    Opens browser, handles callback, exchanges code, saves tokens.
    """
    client_id = get_credential("QUICKBOOKS_CLIENT_ID")
    client_secret = get_credential("QUICKBOOKS_CLIENT_SECRET")

    params = {
        "client_id": client_id,
        "response_type": "code",
        "scope": _SCOPES,
        "redirect_uri": _REDIRECT_URI,
        "state": "sparkle_shine_qbo",
    }
    auth_url = f"{_AUTHORIZE_URL}?{urlencode(params)}"

    received: dict = {}

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            qs = parse_qs(urlparse(self.path).query)
            if "code" in qs:
                received["code"] = qs["code"][0]
                received["realm_id"] = qs.get("realmId", [None])[0]
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"<h2>QuickBooks auth complete. You can close this tab.</h2>")
            else:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"Missing code parameter.")

        def log_message(self, *args):
            pass

    server = HTTPServer(("localhost", 8020), _Handler)
    print(f"Opening browser for QuickBooks OAuth...\n  {auth_url}\n")
    webbrowser.open(auth_url)
    server.handle_request()

    code = received.get("code")
    if not code:
        raise RuntimeError("No authorisation code received from QuickBooks.")

    resp = requests.post(
        _TOKEN_URL,
        headers={
            "Authorization": _basic_auth_header(),
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": _REDIRECT_URI,
        },
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    data["expires_at"] = time.time() + data.get("expires_in", 3600)
    if received.get("realm_id"):
        data["realm_id"] = received["realm_id"]
    _save_tokens(data)
    print(f"QuickBooks tokens saved to {_TOKEN_FILE}")
