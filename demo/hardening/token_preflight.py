"""
demo/hardening/token_preflight.py

Pre-check all OAuth and API tokens before running the intelligence pipeline.
Each check makes one lightweight API call to verify the token is valid RIGHT NOW.

Usage:
    python -m demo.hardening.token_preflight

Output:
    ┌─────────────┬────────┬─────────────────────────────┐
    │ Tool        │ Status │ Notes                       │
    ├─────────────┼────────┼─────────────────────────────┤
    │ Jobber      │ OK     │ Token refreshed proactively │
    │ QuickBooks  │ OK     │ Expires in 42 min           │
    │ Google      │ WARN   │ App in Testing mode         │
    │ HubSpot     │ OK     │                             │
    │ Pipedrive   │ OK     │                             │
    │ Asana       │ OK     │                             │
    │ Mailchimp   │ OK     │                             │
    │ Slack       │ OK     │                             │
    └─────────────┴────────┴─────────────────────────────┘
    Result: 8/8 tools reachable (1 warning)
"""

from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Literal

import requests
from dotenv import load_dotenv

# Resolve project root and load .env
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv(os.path.join(_PROJECT_ROOT, ".env"))

# Add project root to sys.path so auth modules are importable
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from auth.token_store import load_tokens
from intelligence.logging_config import setup_logging

logger = setup_logging("hardening.token_preflight")

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

TokenStatus = Literal["ok", "expired", "expiring_soon", "error"]

_TIMEOUT = 5  # seconds per check call
_QBO_EXPIRY_WARN_SECONDS = 5 * 60      # < 5 min remaining → refresh proactively
_GOOGLE_TESTING_WARN_DAYS = 6          # warn if token.json is older than 6 days
_JOBBER_TOKEN_FILE  = os.path.join(_PROJECT_ROOT, ".jobber_tokens.json")
_QBO_TOKEN_FILE     = os.path.join(_PROJECT_ROOT, ".quickbooks_tokens.json")
_GOOGLE_TOKEN_FILE  = os.path.join(_PROJECT_ROOT, "token.json")
_JOBBER_GRAPHQL_URL = "https://api.getjobber.com/api/graphql"
_JOBBER_API_VERSION = os.getenv("JOBBER_API_VERSION", "2026-03-10")


@dataclass
class TokenCheck:
    tool_name: str
    status: TokenStatus
    message: str
    action: str | None = None


@dataclass
class PreflightResult:
    checks: list[TokenCheck] = field(default_factory=list)

    @property
    def all_passed(self) -> bool:
        return all(c.status in ("ok", "expiring_soon") for c in self.checks)

    @property
    def failed_tools(self) -> list[str]:
        return [c.tool_name for c in self.checks if c.status in ("expired", "error")]

    @property
    def warnings(self) -> list[str]:
        return [c.message for c in self.checks if c.status == "expiring_soon"]


# ---------------------------------------------------------------------------
# Individual check functions
# ---------------------------------------------------------------------------

def check_jobber_token() -> TokenCheck:
    """
    Load Jobber tokens via token_store (DB -> JSON file -> env vars).
    Make a minimal GraphQL query: { account { name } }
    If 401: try refresh. If refresh works, return "ok" with warning.
    If refresh fails: return "expired" with action instructions.
    """
    tokens = load_tokens("jobber", _JOBBER_TOKEN_FILE)
    if not tokens:
        return TokenCheck(
            "Jobber", "error",
            "No Jobber tokens found (checked DB, file, env vars)",
            action="Run: python -m auth.jobber_auth",
        )

    access_token = tokens.get("access_token")
    if not access_token:
        return TokenCheck(
            "Jobber", "error",
            "No access_token in .jobber_tokens.json",
            action="Run: python -m auth.jobber_auth",
        )

    headers = {
        "Authorization": f"Bearer {access_token}",
        "X-JOBBER-GRAPHQL-VERSION": _JOBBER_API_VERSION,
        "Content-Type": "application/json",
    }
    try:
        resp = requests.post(
            _JOBBER_GRAPHQL_URL,
            json={"query": "{ account { name } }"},
            headers=headers,
            timeout=_TIMEOUT,
        )
    except Exception as exc:
        return TokenCheck("Jobber", "error", f"Connection error: {exc}")

    if resp.status_code == 401:
        return _jobber_try_refresh(tokens)

    if resp.status_code != 200:
        return TokenCheck("Jobber", "error", f"Unexpected HTTP {resp.status_code}")

    return TokenCheck("Jobber", "ok", "")


def _jobber_try_refresh(tokens: dict) -> TokenCheck:
    """Attempt a Jobber token refresh. Returns a TokenCheck reflecting the result."""
    refresh_token = tokens.get("refresh_token")
    if not refresh_token:
        return TokenCheck(
            "Jobber", "expired",
            "Token expired, no refresh token stored",
            action="Run: python -m auth.jobber_auth",
        )
    try:
        from auth.jobber_auth import _refresh_token as jobber_refresh, _save_tokens as jobber_save
        new_tokens = jobber_refresh(refresh_token)
        jobber_save(new_tokens)
        logger.info("Jobber token refreshed proactively during preflight")
        return TokenCheck("Jobber", "ok", "Token refreshed proactively")
    except Exception as exc:
        return TokenCheck(
            "Jobber", "expired",
            f"Token expired, refresh failed: {exc}",
            action="Run: python -m auth.jobber_auth",
        )


def check_quickbooks_token() -> TokenCheck:
    """
    Load QuickBooks tokens via token_store (DB -> JSON file -> env vars).
    If access token has < 5 min remaining: refresh proactively.
    Make a minimal query: GET /v3/company/{id}/companyinfo/{id}
    Handle 401 same as Jobber.
    """
    tokens = load_tokens("quickbooks", _QBO_TOKEN_FILE)
    if not tokens:
        return TokenCheck(
            "QuickBooks", "error",
            "No QuickBooks tokens found (checked DB, file, env vars)",
            action="Run: python -m auth.quickbooks_auth",
        )

    access_token = tokens.get("access_token")
    if not access_token:
        return TokenCheck(
            "QuickBooks", "error",
            "No access_token in .quickbooks_tokens.json",
            action="Run: python -m auth.quickbooks_auth",
        )

    # Proactive refresh if < 5 minutes remaining
    expires_at = tokens.get("expires_at", 0)
    remaining_seconds = expires_at - time.time()

    if 0 < remaining_seconds < _QBO_EXPIRY_WARN_SECONDS:
        refresh_result = _qbo_try_refresh(tokens)
        if refresh_result is not None:
            return refresh_result
        # Refresh updated tokens; reload
        with open(_QBO_TOKEN_FILE) as f:
            tokens = json.load(f)
        access_token = tokens["access_token"]
        expires_at   = tokens.get("expires_at", 0)
        remaining_seconds = expires_at - time.time()

    # Make a minimal API call
    company_id = os.getenv("QBO_COMPANY_ID", "")
    base_url   = os.getenv("QBO_BASE_URL", "https://sandbox-quickbooks.api.intuit.com/v3/company")
    url = f"{base_url}/{company_id}/companyinfo/{company_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=_TIMEOUT)
    except Exception as exc:
        return TokenCheck("QuickBooks", "error", f"Connection error: {exc}")

    if resp.status_code == 401:
        result = _qbo_try_refresh(tokens)
        return result if result else TokenCheck("QuickBooks", "ok", "Token refreshed proactively")

    if resp.status_code != 200:
        return TokenCheck("QuickBooks", "error", f"Unexpected HTTP {resp.status_code}")

    mins_remaining = int(remaining_seconds / 60) if remaining_seconds > 0 else 0
    msg = f"Expires in {mins_remaining} min" if mins_remaining > 0 else ""
    return TokenCheck("QuickBooks", "ok", msg)


def _qbo_try_refresh(tokens: dict) -> TokenCheck | None:
    """
    Attempt a QuickBooks token refresh.
    Returns a TokenCheck on failure, or None on success (tokens saved to file).
    """
    refresh_token = tokens.get("refresh_token")
    if not refresh_token:
        return TokenCheck(
            "QuickBooks", "expired",
            "Token expired, no refresh token stored",
            action="Run: python -m auth.quickbooks_auth",
        )
    try:
        from auth.quickbooks_auth import _refresh_token as qbo_refresh, _save_tokens as qbo_save
        new_tokens = qbo_refresh(refresh_token)
        qbo_save(new_tokens)
        logger.info("QuickBooks token refreshed proactively during preflight")
        return None  # success — caller should reload tokens
    except Exception as exc:
        return TokenCheck(
            "QuickBooks", "expired",
            f"Token expired, refresh failed: {exc}",
            action="Run: python -m auth.quickbooks_auth",
        )


def check_google_token() -> TokenCheck:
    """
    Load Google tokens via token_store (DB -> JSON file -> env vars).
    Make a minimal call: GET /drive/v3/about?fields=user
    If 401/expired: attempt refresh via google.auth.transport.requests.Request().
    IMPORTANT: Warn if the Google Cloud app is still in "Testing" mode
    (tokens expire after 7 days). Check by looking at token file age.
    """
    try:
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
        from google.auth.exceptions import RefreshError
    except ImportError as exc:
        return TokenCheck("Google", "error", f"google-auth library not installed: {exc}")

    raw = load_tokens("google", _GOOGLE_TOKEN_FILE)
    if not raw:
        return TokenCheck(
            "Google", "error",
            "No Google tokens found (checked DB, file, env vars)",
            action="Run: python -m auth.google_auth",
        )

    if not raw.get("refresh_token"):
        return TokenCheck(
            "Google", "expired",
            "No refresh token in stored Google tokens",
            action="Run: python -m auth.google_auth",
        )

    # Check file age for Testing-mode warning (only if file exists)
    testing_mode_warn = False
    token_path = _GOOGLE_TOKEN_FILE
    if os.path.exists(token_path):
        file_age_days = (time.time() - os.path.getmtime(token_path)) / (24 * 3600)
        testing_mode_warn = file_age_days >= _GOOGLE_TESTING_WARN_DAYS

    # Load credentials and refresh if needed
    scopes = raw.get("scopes", [])
    if isinstance(scopes, str):
        scopes = scopes.split()
    try:
        creds = Credentials.from_authorized_user_info(raw, scopes)
    except Exception as exc:
        return TokenCheck("Google", "error", f"Cannot parse Google token data: {exc}")

    # If expired, try to refresh
    if not creds.valid:
        if creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                # Save refreshed token to DB and file
                from auth.token_store import save_tokens
                save_tokens("google", json.loads(creds.to_json()), _GOOGLE_TOKEN_FILE)
                logger.info("Google token refreshed during preflight")
            except RefreshError as exc:
                return TokenCheck(
                    "Google", "expired",
                    f"Token expired, refresh failed: {exc}",
                    action="Run: python -m auth.google_auth",
                )
        else:
            return TokenCheck(
                "Google", "expired",
                "Credentials invalid and cannot refresh",
                action="Run: python -m auth.google_auth",
            )

    # Make a minimal Drive API call
    try:
        drive_url = "https://www.googleapis.com/drive/v3/about?fields=user"
        auth_header = {"Authorization": f"Bearer {creds.token}"}
        resp = requests.get(drive_url, headers=auth_header, timeout=_TIMEOUT)
    except Exception as exc:
        return TokenCheck("Google", "error", f"Connection error: {exc}")

    if resp.status_code == 401:
        return TokenCheck(
            "Google", "expired",
            "Token rejected (401)",
            action="Run: python -m auth.google_auth",
        )
    if resp.status_code != 200:
        return TokenCheck("Google", "error", f"Unexpected HTTP {resp.status_code}")

    if testing_mode_warn:
        return TokenCheck(
            "Google", "expiring_soon",
            f"App may be in Testing mode (token.json is {file_age_days:.0f} days old — "
            "refresh tokens expire after 7 days in Testing mode)",
        )

    return TokenCheck("Google", "ok", "")


def check_hubspot_token() -> TokenCheck:
    """
    Private App Token -- never expires, but verify it works.
    GET /crm/v3/objects/contacts?limit=1
    If 401: token was revoked. Return "error".
    """
    token = os.getenv("HUBSPOT_ACCESS_TOKEN", "")
    if not token:
        return TokenCheck("HubSpot", "error", "HUBSPOT_ACCESS_TOKEN not set")

    url = "https://api.hubapi.com/crm/v3/objects/contacts?limit=1"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        resp = requests.get(url, headers=headers, timeout=_TIMEOUT)
    except Exception as exc:
        return TokenCheck("HubSpot", "error", f"Connection error: {exc}")

    if resp.status_code == 401:
        return TokenCheck(
            "HubSpot", "error",
            "Token rejected (401) — token may have been revoked",
            action="Generate a new Private App Token in HubSpot settings",
        )
    if resp.status_code not in (200, 201):
        return TokenCheck("HubSpot", "error", f"Unexpected HTTP {resp.status_code}")

    return TokenCheck("HubSpot", "ok", "")


def check_pipedrive_token() -> TokenCheck:
    """
    API token -- never expires, but verify.
    GET /v1/users/me
    If 401: return "error".
    """
    token = os.getenv("PIPEDRIVE_API_TOKEN", "")
    if not token:
        return TokenCheck("Pipedrive", "error", "PIPEDRIVE_API_TOKEN not set")

    base = os.getenv("PIPEDRIVE_BASE_URL", "https://api.pipedrive.com/v1").rstrip("/")
    if not any(seg in base for seg in ("/v1", "/v2")):
        base = f"{base}/v1"
    url = f"{base}/users/me"
    headers = {"x-api-token": token}
    try:
        resp = requests.get(url, headers=headers, timeout=_TIMEOUT)
    except Exception as exc:
        return TokenCheck("Pipedrive", "error", f"Connection error: {exc}")

    if resp.status_code == 401:
        return TokenCheck(
            "Pipedrive", "error",
            "Token rejected (401)",
            action="Check PIPEDRIVE_API_TOKEN in .env",
        )
    if resp.status_code != 200:
        return TokenCheck("Pipedrive", "error", f"Unexpected HTTP {resp.status_code}")

    return TokenCheck("Pipedrive", "ok", "")


def check_asana_token() -> TokenCheck:
    """
    PAT -- never expires, but verify.
    GET /api/1.0/users/me
    If 401: return "error".
    """
    token = os.getenv("ASANA_ACCESS_TOKEN", "")
    if not token:
        return TokenCheck("Asana", "error", "ASANA_ACCESS_TOKEN not set")

    url = "https://app.asana.com/api/1.0/users/me"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        resp = requests.get(url, headers=headers, timeout=_TIMEOUT)
    except Exception as exc:
        return TokenCheck("Asana", "error", f"Connection error: {exc}")

    if resp.status_code == 401:
        return TokenCheck(
            "Asana", "error",
            "Token rejected (401)",
            action="Check ASANA_ACCESS_TOKEN in .env",
        )
    if resp.status_code != 200:
        return TokenCheck("Asana", "error", f"Unexpected HTTP {resp.status_code}")

    return TokenCheck("Asana", "ok", "")


def check_mailchimp_token() -> TokenCheck:
    """
    API key -- never expires, but verify.
    GET /3.0/ping
    If 401: return "error".
    """
    api_key = os.getenv("MAILCHIMP_API_KEY", "")
    server  = os.getenv("MAILCHIMP_SERVER_PREFIX", "")
    if not api_key:
        return TokenCheck("Mailchimp", "error", "MAILCHIMP_API_KEY not set")
    if not server:
        return TokenCheck("Mailchimp", "error", "MAILCHIMP_SERVER_PREFIX not set")

    url = f"https://{server}.api.mailchimp.com/3.0/ping"
    try:
        resp = requests.get(url, auth=("anystring", api_key), timeout=_TIMEOUT)
    except Exception as exc:
        return TokenCheck("Mailchimp", "error", f"Connection error: {exc}")

    if resp.status_code == 401:
        return TokenCheck(
            "Mailchimp", "error",
            "Token rejected (401)",
            action="Check MAILCHIMP_API_KEY in .env",
        )
    if resp.status_code != 200:
        return TokenCheck("Mailchimp", "error", f"Unexpected HTTP {resp.status_code}")

    return TokenCheck("Mailchimp", "ok", "")


def check_slack_token() -> TokenCheck:
    """
    Bot token -- never expires, but verify.
    POST /api/auth.test
    If invalid: return "error".
    """
    token = os.getenv("SLACK_BOT_TOKEN", "")
    if not token:
        return TokenCheck("Slack", "error", "SLACK_BOT_TOKEN not set")

    url = "https://slack.com/api/auth.test"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        resp = requests.post(url, headers=headers, timeout=_TIMEOUT)
    except Exception as exc:
        return TokenCheck("Slack", "error", f"Connection error: {exc}")

    if resp.status_code != 200:
        return TokenCheck("Slack", "error", f"Unexpected HTTP {resp.status_code}")

    data = resp.json()
    if not data.get("ok"):
        error = data.get("error", "unknown")
        return TokenCheck(
            "Slack", "error",
            f"auth.test failed: {error}",
            action="Check SLACK_BOT_TOKEN in .env",
        )

    return TokenCheck("Slack", "ok", "")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

_PREFLIGHT_CHECKS = [
    check_jobber_token,
    check_quickbooks_token,
    check_google_token,
    check_hubspot_token,
    check_pipedrive_token,
    check_asana_token,
    check_mailchimp_token,
    check_slack_token,
]


def check_all_tokens() -> PreflightResult:
    """
    Verify every tool's authentication is valid RIGHT NOW.
    Returns a PreflightResult with pass/fail per tool and
    recommended actions for failures.
    """
    result = PreflightResult()
    for check_fn in _PREFLIGHT_CHECKS:
        tool_label = check_fn.__name__.replace("check_", "").replace("_token", "").title()
        logger.debug("Running preflight check for %s", tool_label)
        try:
            check = check_fn()
        except Exception as exc:
            check = TokenCheck(tool_label, "error", f"Unexpected error: {exc}")
        result.checks.append(check)
        logger.debug(
            "Preflight %s: %s (%s)", check.tool_name, check.status, check.message
        )
    return result


# ---------------------------------------------------------------------------
# CLI table renderer
# ---------------------------------------------------------------------------

_STATUS_DISPLAY = {
    "ok":            "OK  ",
    "expiring_soon": "WARN",
    "expired":       "FAIL",
    "error":         "ERR ",
}


def print_preflight_table(result: PreflightResult) -> None:
    """Print the preflight results as a Unicode box-drawing table."""
    col_tool   = max(len(c.tool_name) for c in result.checks) + 2
    col_status = 6
    col_notes  = max((len(c.message) for c in result.checks), default=20) + 2
    col_notes  = max(col_notes, len("Notes") + 2)

    top    = f"┌{'─' * col_tool}┬{'─' * col_status}┬{'─' * col_notes}┐"
    header = f"│{'Tool':^{col_tool}}│{'Status':^{col_status}}│{'Notes':<{col_notes}}│"
    sep    = f"├{'─' * col_tool}┼{'─' * col_status}┼{'─' * col_notes}┤"
    bottom = f"└{'─' * col_tool}┴{'─' * col_status}┴{'─' * col_notes}┘"

    print(top)
    print(header)
    print(sep)
    for check in result.checks:
        status_str = _STATUS_DISPLAY.get(check.status, check.status.upper()[:4])
        row = (
            f"│ {check.tool_name:<{col_tool - 2}} "
            f"│ {status_str:<{col_status - 2}} "
            f"│ {check.message:<{col_notes - 2}} │"
        )
        print(row)
    print(bottom)

    total   = len(result.checks)
    passed  = sum(1 for c in result.checks if c.status in ("ok", "expiring_soon"))
    warns   = sum(1 for c in result.checks if c.status == "expiring_soon")
    failed  = total - passed

    status_line = f"Result: {passed}/{total} tools reachable"
    if warns:
        status_line += f" ({warns} warning{'s' if warns != 1 else ''})"
    if failed:
        status_line += f" -- {failed} FAILED: {', '.join(result.failed_tools)}"
    print(status_line)
    print()

    # Print action items for failed checks
    action_checks = [c for c in result.checks if c.action]
    if action_checks:
        print("Action items:")
        for check in action_checks:
            print(f"  [{check.tool_name}] {check.action}")
        print()


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("Sparkle & Shine -- Token Preflight Check")
    print("=" * 45)
    print()
    result = check_all_tokens()
    print_preflight_table(result)
    sys.exit(0 if result.all_passed else 1)
