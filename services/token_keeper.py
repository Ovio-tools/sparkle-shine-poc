"""
services/token_keeper.py

Dedicated Jobber OAuth token refresh service.

Problem: Jobber uses single-use rotating refresh tokens. When multiple Railway
services (simulation-engine, automation-runner, intelligence-weekly, sales-outreach)
independently try to refresh, the first one invalidates the refresh token and all
subsequent attempts break the token chain permanently.

Solution: This service is the SOLE owner of the Jobber refresh flow. It runs as
an always-on Railway worker that refreshes proactively every 45 minutes (before
the 60-minute access token expiry). All other services read tokens from the
shared PostgreSQL oauth_tokens table and never call the refresh endpoint.

Railway config:
  - Type: Worker (always-on)
  - Start command: python -m services.token_keeper
"""
from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from intelligence.logging_config import setup_logging

logger = setup_logging("services.token_keeper")

# ------------------------------------------------------------------ #
# Config
# ------------------------------------------------------------------ #

_TOKEN_URL = "https://api.getjobber.com/api/oauth/token"

# Refresh every 45 minutes (access tokens expire at 60 min).
# Source: Jobber OAuth docs — access_token lifetime is 3600s.
REFRESH_INTERVAL_SECONDS = 45 * 60

# How long before expiry to trigger a refresh (safety margin).
# If the token expires in < 5 min and we haven't refreshed yet, force it.
EXPIRY_BUFFER_SECONDS = 300  # 5 minutes

# Sleep between health-check ticks (how often we check if a refresh is needed).
TICK_INTERVAL_SECONDS = 60  # 1 minute

# Failure-count staircase for Slack alerts. At the 60s tick interval these
# correspond roughly to ~3 min, ~15 min, ~1 h, ~4 h, and ~24 h of sustained
# failure. We alert exactly once at each step instead of on every tick, so a
# multi-hour Jobber outage produces ~5 messages in #automation-failure rather
# than dozens.
_ALERT_AT_FAILURE_COUNTS: frozenset[int] = frozenset({3, 15, 60, 240, 1440})

# Body length captured in error messages and logs. Long enough to read a
# maintenance page or an OAuth error JSON, short enough not to flood Slack.
_BODY_EXCERPT_CHARS = 300


# ------------------------------------------------------------------ #
# Errors
# ------------------------------------------------------------------ #


class JobberRefreshFailure(RuntimeError):
    """Refresh failed. Carries enough context to triage from Slack/logs alone.

    `transient=True` means "Jobber's side, retry naturally" — 2xx with non-JSON
    body (maintenance / CDN error page), 5xx server errors, or network errors.
    `transient=False` means "the refresh-token chain is likely broken" — 4xx
    auth/grant errors. Operators should re-authenticate only for the latter.
    """

    def __init__(
        self,
        message: str,
        *,
        transient: bool,
        status: int | None = None,
        content_type: str | None = None,
        body_excerpt: str = "",
    ):
        super().__init__(message)
        self.transient = transient
        self.status = status
        self.content_type = content_type
        self.body_excerpt = body_excerpt


# ------------------------------------------------------------------ #
# Token DB operations (direct, no import cycle with auth/)
# ------------------------------------------------------------------ #

def _load_jobber_tokens() -> dict:
    """Load Jobber tokens directly from PostgreSQL."""
    try:
        from database.connection import get_connection
        with get_connection() as conn:
            cursor = conn.execute(
                "SELECT token_data FROM oauth_tokens WHERE tool_name = %s",
                ("jobber",),
            )
            row = cursor.fetchone()
            if row:
                data = row["token_data"]
                return data if isinstance(data, dict) else json.loads(data)
    except Exception as exc:
        logger.error("[token_keeper] Failed to load tokens from DB: %s", exc)
    return {}


def _save_jobber_tokens(token_data: dict) -> None:
    """Save Jobber tokens directly to PostgreSQL."""
    from database.connection import get_connection
    with get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS oauth_tokens (
                tool_name   TEXT PRIMARY KEY,
                token_data  JSONB NOT NULL,
                updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """,
        )
        conn.execute(
            """
            INSERT INTO oauth_tokens (tool_name, token_data, updated_at)
            VALUES (%s, %s::jsonb, CURRENT_TIMESTAMP)
            ON CONFLICT (tool_name) DO UPDATE SET
                token_data = EXCLUDED.token_data,
                updated_at = CURRENT_TIMESTAMP
            """,
            ("jobber", json.dumps(token_data)),
        )


# ------------------------------------------------------------------ #
# Refresh logic
# ------------------------------------------------------------------ #

def _refresh_token(refresh_token: str) -> dict:
    """Exchange a refresh token for a new access + refresh token pair.

    Captures HTTP status, Content-Type, and a body excerpt before parsing JSON,
    so any failure surfaces enough context in Slack/logs to triage without
    rerunning the call. Raises JobberRefreshFailure with classification.
    """
    client_id = os.getenv("JOBBER_CLIENT_ID")
    client_secret = os.getenv("JOBBER_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise JobberRefreshFailure(
            "JOBBER_CLIENT_ID / JOBBER_CLIENT_SECRET not set. "
            "Cannot refresh Jobber token.",
            transient=False,
        )

    try:
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
    except (requests.ConnectionError, requests.Timeout) as exc:
        raise JobberRefreshFailure(
            f"Network error contacting Jobber OAuth endpoint: {exc}",
            transient=True,
        ) from exc

    status = resp.status_code
    content_type = resp.headers.get("Content-Type", "")
    body = resp.text or ""
    body_excerpt = body[:_BODY_EXCERPT_CHARS]

    logger.info(
        "[token_keeper] Jobber refresh response: HTTP %d, Content-Type=%r, body_len=%d",
        status, content_type, len(body),
    )

    if 200 <= status < 300:
        if "json" not in content_type.lower() or not body.strip():
            # Most common during Jobber maintenance windows: a load balancer
            # returns 200 with an HTML error page or empty body.
            raise JobberRefreshFailure(
                f"Jobber refresh returned HTTP {status} with non-JSON body "
                f"(Content-Type={content_type!r}, body={body_excerpt!r}). "
                "Most likely Jobber API is in scheduled maintenance — "
                "check https://www.jobberstatus.net/.",
                transient=True,
                status=status,
                content_type=content_type,
                body_excerpt=body_excerpt,
            )
        try:
            data = resp.json()
        except ValueError as exc:
            raise JobberRefreshFailure(
                f"Jobber refresh returned HTTP {status} but JSON decode failed: {exc}. "
                f"Content-Type={content_type!r}, body={body_excerpt!r}",
                transient=True,
                status=status,
                content_type=content_type,
                body_excerpt=body_excerpt,
            ) from exc
        data["expires_at"] = time.time() + data.get("expires_in", 3600)
        return data

    if 500 <= status < 600:
        raise JobberRefreshFailure(
            f"Jobber refresh returned HTTP {status} (server error): {body_excerpt}",
            transient=True,
            status=status,
            content_type=content_type,
            body_excerpt=body_excerpt,
        )

    # 4xx: refresh token rejected, app deauthorized, or bad client credentials.
    # The chain is broken on Jobber's side; retrying without re-auth is futile.
    raise JobberRefreshFailure(
        f"Jobber refresh returned HTTP {status}: {body_excerpt}",
        transient=False,
        status=status,
        content_type=content_type,
        body_excerpt=body_excerpt,
    )


def _needs_refresh(tokens: dict) -> bool:
    """Check if the current access token needs refreshing."""
    if not tokens.get("access_token"):
        return True
    expires_at = tokens.get("expires_at", 0)
    return time.time() >= expires_at - EXPIRY_BUFFER_SECONDS


def _alert_slack(message: str, severity: str = "warning") -> None:
    """Post a token-keeper alert to #automation-failure."""
    try:
        from simulation.error_reporter import report_error
        exc = RuntimeError(message)
        report_error(exc, tool_name="Jobber", context=message)
    except Exception as exc:
        logger.warning("[token_keeper] Could not alert Slack: %s", exc)


# ------------------------------------------------------------------ #
# Main loop
# ------------------------------------------------------------------ #

class TokenKeeper:
    """Always-on service that sole-owns Jobber token refresh."""

    def __init__(self):
        self._running = True
        self._consecutive_failures = 0
        self._last_refresh_time = 0.0

    def handle_shutdown(self, signum, frame):
        sig_name = signal.Signals(signum).name
        logger.info("[token_keeper] Received %s, shutting down gracefully", sig_name)
        self._running = False

    def _do_refresh(self) -> bool:
        """Attempt a token refresh. Returns True on success."""
        tokens = _load_jobber_tokens()

        if not tokens.get("refresh_token"):
            logger.error(
                "[token_keeper] No refresh token in DB. "
                "Run `python -m auth.jobber_auth` to bootstrap."
            )
            if self._consecutive_failures + 1 in _ALERT_AT_FAILURE_COUNTS:
                _alert_slack(
                    "No Jobber refresh token in DB. "
                    "Run `python -m auth.jobber_auth` to re-authenticate.",
                    severity="critical",
                )
            self._consecutive_failures += 1
            return False

        try:
            new_tokens = _refresh_token(tokens["refresh_token"])
        except Exception as exc:
            logger.error("[token_keeper] Jobber token refresh failed: %s", exc)
            self._consecutive_failures += 1
            if self._consecutive_failures in _ALERT_AT_FAILURE_COUNTS:
                self._post_failure_alert(exc)
            return False

        _save_jobber_tokens(new_tokens)
        prior_failures = self._consecutive_failures
        self._consecutive_failures = 0
        self._last_refresh_time = time.time()

        expires_in = new_tokens.get("expires_in", 3600)
        logger.info(
            "[token_keeper] Refreshed Jobber token successfully. "
            "New token expires in %d seconds.",
            expires_in,
        )

        # Only post a recovery message if we'd previously alerted on this
        # streak. Below the staircase floor, recovery is silent so brief
        # blips don't generate noise.
        if prior_failures >= min(_ALERT_AT_FAILURE_COUNTS):
            _alert_slack(
                f"Jobber token refresh recovered after {prior_failures} consecutive failures. "
                "The token chain is healthy again.",
                severity="info",
            )
        return True

    def _post_failure_alert(self, exc: Exception) -> None:
        """Post a Slack alert with severity / wording derived from the failure type."""
        transient = isinstance(exc, JobberRefreshFailure) and exc.transient
        if transient:
            label = "WARNING"
            severity = "warning"
            kind = (
                "Jobber API may be in maintenance or returning a non-JSON error page"
            )
            action = (
                "Token-keeper will keep retrying. "
                "Check Jobber's status page at https://www.jobberstatus.net/ before re-authenticating"
            )
        else:
            label = "CRITICAL"
            severity = "critical"
            kind = "Token chain likely broken (refresh token rejected by Jobber)"
            action = (
                "Run `python -m auth.jobber_auth` to re-authenticate "
                "after confirming Jobber is operational"
            )
        _alert_slack(
            f"{label}: Jobber token refresh has failed {self._consecutive_failures} "
            f"times in a row. {kind}. Last error: {exc}. {action}.",
            severity=severity,
        )

    def _should_refresh_now(self, tokens: dict) -> bool:
        """Determine if we should refresh right now."""
        # Always refresh if token is expired or about to expire
        if _needs_refresh(tokens):
            return True

        # Proactive refresh: if we haven't refreshed in REFRESH_INTERVAL_SECONDS
        if self._last_refresh_time == 0:
            # First tick — check the DB updated_at to avoid unnecessary refresh
            return _needs_refresh(tokens)

        elapsed = time.time() - self._last_refresh_time
        return elapsed >= REFRESH_INTERVAL_SECONDS

    def run(self):
        """Main loop: check token health every TICK_INTERVAL_SECONDS, refresh as needed."""
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        signal.signal(signal.SIGINT, self.handle_shutdown)

        logger.info(
            "[token_keeper] Starting Jobber Token Keeper. "
            "Refresh interval: %d min, tick interval: %d sec.",
            REFRESH_INTERVAL_SECONDS // 60,
            TICK_INTERVAL_SECONDS,
        )

        # Initial refresh on startup to ensure we have a valid token
        tokens = _load_jobber_tokens()
        if _needs_refresh(tokens):
            logger.info("[token_keeper] Token needs refresh on startup, refreshing now.")
            self._do_refresh()
        else:
            expires_at = tokens.get("expires_at", 0)
            remaining = max(0, expires_at - time.time())
            logger.info(
                "[token_keeper] Current token valid for %.0f more seconds. "
                "Will refresh proactively at %d-minute intervals.",
                remaining,
                REFRESH_INTERVAL_SECONDS // 60,
            )
            self._last_refresh_time = time.time()

        while self._running:
            time.sleep(TICK_INTERVAL_SECONDS)

            if not self._running:
                break

            tokens = _load_jobber_tokens()
            if self._should_refresh_now(tokens):
                self._do_refresh()

        logger.info("[token_keeper] Shut down cleanly.")


# ------------------------------------------------------------------ #
# Entry point
# ------------------------------------------------------------------ #

if __name__ == "__main__":
    keeper = TokenKeeper()
    keeper.run()
