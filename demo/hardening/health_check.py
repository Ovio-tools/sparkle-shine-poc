"""
demo/hardening/health_check.py

Quick "is everything reachable" check for the intelligence pipeline.

Lighter than the token preflight check:
  - Does NOT attempt token refresh on failure.
  - Timeout: 5 seconds per tool.
  - Runs all 8 checks concurrently (ThreadPoolExecutor).
  - Designed to finish under 10 seconds total.

Usage:
    from demo.hardening.health_check import quick_health_check

    reachable = quick_health_check()
    # {"jobber": True, "quickbooks": True, "google": False, ...}

    # Skip unhealthy tools in the pipeline:
    skip = {tool for tool, ok in reachable.items() if not ok}
"""

from __future__ import annotations

import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from dotenv import load_dotenv

# Resolve project root and load .env
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv(os.path.join(_PROJECT_ROOT, ".env"))

if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from intelligence.logging_config import setup_logging

logger = setup_logging("hardening.health_check")

_TIMEOUT = 5  # seconds per tool
_JOBBER_TOKEN_FILE  = os.path.join(_PROJECT_ROOT, ".jobber_tokens.json")
_QBO_TOKEN_FILE     = os.path.join(_PROJECT_ROOT, ".quickbooks_tokens.json")
_GOOGLE_TOKEN_FILE  = os.path.join(_PROJECT_ROOT, "token.json")
_JOBBER_GRAPHQL_URL = "https://api.getjobber.com/api/graphql"
_JOBBER_API_VERSION = os.getenv("JOBBER_API_VERSION", "2026-03-10")


# ---------------------------------------------------------------------------
# Individual ping functions (lightweight, no refresh)
# ---------------------------------------------------------------------------

def _ping_jobber() -> bool:
    if not os.path.exists(_JOBBER_TOKEN_FILE):
        logger.debug("health_check: Jobber token file missing")
        return False
    try:
        with open(_JOBBER_TOKEN_FILE) as f:
            tokens = json.load(f)
        access_token = tokens.get("access_token", "")
        if not access_token:
            return False
        resp = requests.post(
            _JOBBER_GRAPHQL_URL,
            json={"query": "{ account { name } }"},
            headers={
                "Authorization": f"Bearer {access_token}",
                "X-JOBBER-GRAPHQL-VERSION": _JOBBER_API_VERSION,
                "Content-Type": "application/json",
            },
            timeout=_TIMEOUT,
        )
        return resp.status_code == 200
    except Exception as exc:
        logger.debug("health_check: Jobber ping failed: %s", exc)
        return False


def _ping_quickbooks() -> bool:
    if not os.path.exists(_QBO_TOKEN_FILE):
        logger.debug("health_check: QuickBooks token file missing")
        return False
    try:
        with open(_QBO_TOKEN_FILE) as f:
            tokens = json.load(f)
        access_token = tokens.get("access_token", "")
        if not access_token:
            return False
        company_id = os.getenv("QBO_COMPANY_ID", "")
        base_url   = os.getenv("QBO_BASE_URL", "https://sandbox-quickbooks.api.intuit.com/v3/company")
        url = f"{base_url}/{company_id}/companyinfo/{company_id}"
        resp = requests.get(
            url,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/json",
            },
            timeout=_TIMEOUT,
        )
        return resp.status_code == 200
    except Exception as exc:
        logger.debug("health_check: QuickBooks ping failed: %s", exc)
        return False


def _ping_google() -> bool:
    token_path = _GOOGLE_TOKEN_FILE
    alt_path = os.path.join(os.path.dirname(_PROJECT_ROOT), "token.json")
    if not os.path.exists(token_path) and os.path.exists(alt_path):
        token_path = alt_path
    if not os.path.exists(token_path):
        logger.debug("health_check: Google token.json missing")
        return False
    try:
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request

        with open(token_path) as f:
            raw = json.load(f)
        scopes = raw.get("scopes", [])
        if isinstance(scopes, str):
            scopes = scopes.split()
        creds = Credentials.from_authorized_user_info(raw, scopes)

        # Only refresh if expired — health check, not preflight
        if not creds.valid and creds.expired and creds.refresh_token:
            creds.refresh(Request())

        if not creds.token:
            return False

        resp = requests.get(
            "https://www.googleapis.com/drive/v3/about?fields=user",
            headers={"Authorization": f"Bearer {creds.token}"},
            timeout=_TIMEOUT,
        )
        return resp.status_code == 200
    except Exception as exc:
        logger.debug("health_check: Google ping failed: %s", exc)
        return False


def _ping_hubspot() -> bool:
    token = os.getenv("HUBSPOT_ACCESS_TOKEN", "")
    if not token:
        return False
    try:
        resp = requests.get(
            "https://api.hubapi.com/crm/v3/objects/contacts?limit=1",
            headers={"Authorization": f"Bearer {token}"},
            timeout=_TIMEOUT,
        )
        return resp.status_code in (200, 201)
    except Exception as exc:
        logger.debug("health_check: HubSpot ping failed: %s", exc)
        return False


def _ping_pipedrive() -> bool:
    token = os.getenv("PIPEDRIVE_API_TOKEN", "")
    if not token:
        return False
    try:
        base = os.getenv("PIPEDRIVE_BASE_URL", "https://api.pipedrive.com/v1").rstrip("/")
        if not any(seg in base for seg in ("/v1", "/v2")):
            base = f"{base}/v1"
        resp = requests.get(
            f"{base}/users/me",
            headers={"x-api-token": token},
            timeout=_TIMEOUT,
        )
        return resp.status_code == 200
    except Exception as exc:
        logger.debug("health_check: Pipedrive ping failed: %s", exc)
        return False


def _ping_asana() -> bool:
    token = os.getenv("ASANA_ACCESS_TOKEN", "")
    if not token:
        return False
    try:
        resp = requests.get(
            "https://app.asana.com/api/1.0/users/me",
            headers={"Authorization": f"Bearer {token}"},
            timeout=_TIMEOUT,
        )
        return resp.status_code == 200
    except Exception as exc:
        logger.debug("health_check: Asana ping failed: %s", exc)
        return False


def _ping_mailchimp() -> bool:
    api_key = os.getenv("MAILCHIMP_API_KEY", "")
    server  = os.getenv("MAILCHIMP_SERVER_PREFIX", "")
    if not api_key or not server:
        return False
    try:
        resp = requests.get(
            f"https://{server}.api.mailchimp.com/3.0/ping",
            auth=("anystring", api_key),
            timeout=_TIMEOUT,
        )
        return resp.status_code == 200
    except Exception as exc:
        logger.debug("health_check: Mailchimp ping failed: %s", exc)
        return False


def _ping_slack() -> bool:
    token = os.getenv("SLACK_BOT_TOKEN", "")
    if not token:
        return False
    try:
        resp = requests.post(
            "https://slack.com/api/auth.test",
            headers={"Authorization": f"Bearer {token}"},
            timeout=_TIMEOUT,
        )
        if resp.status_code != 200:
            return False
        return resp.json().get("ok", False)
    except Exception as exc:
        logger.debug("health_check: Slack ping failed: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Map of tool name -> ping function
# ---------------------------------------------------------------------------

_PING_FUNCTIONS: dict[str, callable] = {
    "jobber":      _ping_jobber,
    "quickbooks":  _ping_quickbooks,
    "google":      _ping_google,
    "hubspot":     _ping_hubspot,
    "pipedrive":   _ping_pipedrive,
    "asana":       _ping_asana,
    "mailchimp":   _ping_mailchimp,
    "slack":       _ping_slack,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def quick_health_check() -> dict[str, bool]:
    """
    Ping each tool with a minimal API call.
    Timeout: 5 seconds per tool.
    Runs all 8 checks concurrently using ThreadPoolExecutor.
    Total time target: under 10 seconds.

    Returns a dict mapping tool_name -> bool (True = reachable).
    """
    start = time.monotonic()
    results: dict[str, bool] = {}

    with ThreadPoolExecutor(max_workers=len(_PING_FUNCTIONS)) as executor:
        future_to_tool = {
            executor.submit(ping_fn): tool_name
            for tool_name, ping_fn in _PING_FUNCTIONS.items()
        }
        for future in as_completed(future_to_tool, timeout=_TIMEOUT + 2):
            tool_name = future_to_tool[future]
            try:
                results[tool_name] = future.result()
            except Exception as exc:
                logger.debug("health_check: %s raised: %s", tool_name, exc)
                results[tool_name] = False

    # Any tool whose future didn't complete: mark False
    for tool_name in _PING_FUNCTIONS:
        if tool_name not in results:
            results[tool_name] = False

    duration = time.monotonic() - start
    reachable_count = sum(1 for ok in results.values() if ok)
    logger.info(
        "Health check complete: %d/%d tools reachable in %.1fs",
        reachable_count, len(results), duration,
    )
    return results
