"""
Jobber authentication.

Token precedence (via token_store four-tier fallback):
  1. PostgreSQL DB (primary, written by the Token Keeper service)
  2. .jobber_tokens.json (local dev fallback)
  3. JOBBER_REFRESH_TOKEN env var (Railway bootstrap)
  4. JOBBER_ACCESS_TOKEN env var (stale last resort after refresh failure)

IMPORTANT: In production (Railway), get_jobber_token() is READ-ONLY.
It reads tokens from the DB but never refreshes them. The dedicated
Token Keeper service (services/token_keeper.py) is the sole owner of
the refresh flow, preventing race conditions when multiple Railway
services share the same rotating refresh token.

Local dev: set JOBBER_TOKEN_KEEPER_ENABLED=0 (or leave unset) to keep
the legacy self-refresh behaviour for convenience.
"""
import json
import logging
import os
import sys
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlencode, urlparse, parse_qs

import requests

logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from credentials import get_credential
from auth import token_store

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
    return token_store.load_tokens("jobber", _TOKEN_FILE)


def _save_tokens(data: dict) -> None:
    token_store.save_tokens("jobber", data, _TOKEN_FILE)


# ------------------------------------------------------------------ #
# Refresh
# ------------------------------------------------------------------ #

def _refresh_token(refresh_token: str) -> dict:
    """Exchange a refresh token for a new access token. Returns updated token dict.

    Refuses to run when JOBBER_TOKEN_KEEPER_ENABLED=1. In that mode the
    services/token_keeper.py worker is the sole owner of the rotating refresh
    token; any other call here would race against it and break the chain on
    Jobber's side. The guard catches accidental imports from preflight scripts,
    one-off remediation scripts, or future code that doesn't know the contract.
    """
    if _is_token_keeper_mode():
        raise RuntimeError(
            "auth.jobber_auth._refresh_token called while JOBBER_TOKEN_KEEPER_ENABLED=1. "
            "Refresh is owned by services/token_keeper.py — calling it here would break "
            "the rotating refresh-token chain. If you need a one-off bootstrap, unset "
            "the env var temporarily."
        )

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

def _is_token_keeper_mode() -> bool:
    """Check if the Token Keeper service owns refresh (production mode).

    Returns True when JOBBER_TOKEN_KEEPER_ENABLED is set to a truthy value.
    On Railway this should always be '1'. Local dev defaults to False so
    the legacy self-refresh behaviour works out of the box.
    """
    return os.getenv("JOBBER_TOKEN_KEEPER_ENABLED", "").strip().lower() in ("1", "true", "yes")


def get_jobber_token() -> str:
    """
    Return a valid Jobber access token.

    Two modes:
      - Token Keeper mode (JOBBER_TOKEN_KEEPER_ENABLED=1): read-only. Reads
        from DB and trusts the Token Keeper service to keep tokens fresh.
        Never calls the refresh endpoint.
      - Legacy mode (default / local dev): self-refreshes when the stored
        token is about to expire.
    """
    tokens = _load_tokens()

    # ── Check if current token is still valid ──
    if tokens.get("access_token"):
        expires_at = tokens.get("expires_at", 0)
        if time.time() < expires_at - _EXPIRY_BUFFER:
            return tokens["access_token"]

    # ── Token Keeper mode: read-only, never refresh here ──
    if _is_token_keeper_mode():
        # The token in DB is expired or missing. The Token Keeper should
        # have refreshed it. Return it anyway (may be slightly stale but
        # still within the server-side grace window) or fall through.
        if tokens.get("access_token"):
            logger.warning(
                "[jobber] Token appears expired but Token Keeper mode is active. "
                "Returning current token (Token Keeper may be mid-refresh)."
            )
            return tokens["access_token"]

        # No access token at all — fall through to env var / error
        logger.error(
            "[jobber] No access token in DB and Token Keeper mode is active. "
            "Check that the token-keeper service is running."
        )
        try:
            from simulation.error_reporter import report_error
            report_error(
                RuntimeError("No Jobber access token in DB"),
                tool_name="Jobber",
                context="Token Keeper mode is active but no token found in DB. "
                        "Is the token-keeper service running?",
            )
        except Exception:
            pass
    else:
        # ── Legacy mode: self-refresh ──
        if tokens.get("refresh_token"):
            try:
                new_tokens = _refresh_token(tokens["refresh_token"])
                _save_tokens(new_tokens)
                return new_tokens["access_token"]
            except requests.HTTPError as exc:
                msg = (
                    f"Token refresh failed: HTTP "
                    f"{exc.response.status_code if exc.response is not None else '?'} — "
                    f"{exc.response.text[:300] if exc.response is not None else str(exc)}"
                )
                logger.warning("[jobber] %s", msg)
                try:
                    from simulation.error_reporter import report_error
                    report_error(exc, tool_name="Jobber", context=msg + " — run: python -m auth.jobber_auth")
                except Exception:
                    pass
            except Exception as exc:
                logger.warning("[jobber] Token refresh failed: %s", exc)
                try:
                    from simulation.error_reporter import report_error
                    report_error(exc, tool_name="Jobber", context=f"Token refresh failed: {exc} — run: python -m auth.jobber_auth")
                except Exception:
                    pass

    # Last resort: stale access token from env var
    stale_token = os.getenv("JOBBER_ACCESS_TOKEN")
    if stale_token:
        logger.warning(
            "[jobber] Using JOBBER_ACCESS_TOKEN env var as last resort (may be stale)"
        )
        return stale_token

    raise RuntimeError(
        "No valid Jobber token available. "
        "If Token Keeper mode is active, ensure the token-keeper service is running. "
        "Otherwise, set JOBBER_REFRESH_TOKEN (plus JOBBER_CLIENT_ID and JOBBER_CLIENT_SECRET) "
        "or provide JOBBER_ACCESS_TOKEN as a temporary fallback."
    )


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


if __name__ == "__main__":
    run_initial_auth()
