"""
demo/hardening/retry.py

A decorator and utility for resilient API calls with exponential backoff.

Usage:
    from demo.hardening.retry import with_retry, TokenExpiredError, ToolUnavailableError

    @with_retry(max_retries=3)
    def call_jobber_api(endpoint, params):
        ...
"""

from __future__ import annotations

import functools
import time

import requests

from intelligence.logging_config import setup_logging

logger = setup_logging("hardening.retry")

DEFAULT_RETRY_CONFIG = {
    "max_retries": 3,
    "base_delay_seconds": 1.0,
    "max_delay_seconds": 30.0,
    "backoff_factor": 2.0,        # exponential: 1s, 2s, 4s
    "retryable_status_codes": [429, 500, 502, 503, 504],
    "retryable_exceptions": [
        "ConnectionError", "Timeout", "ConnectionResetError"
    ],
}

_RETRYABLE_STATUS_CODES = set(DEFAULT_RETRY_CONFIG["retryable_status_codes"])
_NO_RETRY_STATUS_CODES = {400, 403, 404}


# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------

class TokenExpiredError(Exception):
    """Raised when an API returns 401, indicating the token needs refresh."""
    def __init__(self, tool_name: str, message: str = ""):
        self.tool_name = tool_name
        super().__init__(f"{tool_name} token expired: {message}")


class ToolUnavailableError(Exception):
    """Raised after max retries exhausted."""
    def __init__(self, tool_name: str, last_error: str = ""):
        self.tool_name = tool_name
        super().__init__(f"{tool_name} unavailable after retries: {last_error}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _calc_delay(attempt: int, base_delay: float, backoff_factor: float, max_delay: float) -> float:
    """Compute exponential backoff delay for the given attempt (0-indexed), capped at max_delay."""
    delay = base_delay * (backoff_factor ** attempt)
    return min(delay, max_delay)


def _parse_retry_after(header_value: str | None) -> float | None:
    """
    Parse the Retry-After header value.
    Returns seconds as float, or None if unparseable.
    Supports both integer seconds and HTTP-date formats.
    """
    if not header_value:
        return None
    try:
        return float(header_value)
    except (ValueError, TypeError):
        pass
    # Try HTTP-date parsing (e.g. "Wed, 21 Oct 2015 07:28:00 GMT")
    import email.utils
    try:
        import calendar
        parsed = email.utils.parsedate(header_value)
        if parsed:
            retry_time = calendar.timegm(parsed)
            wait = retry_time - time.time()
            return max(wait, 0.0)
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Decorator
# ---------------------------------------------------------------------------

def with_retry(max_retries: int = 3, base_delay: float = 1.0,
               backoff_factor: float = 2.0, max_delay: float = 30.0):
    """
    Decorator factory for API calls that should be retried on transient failures.

    Behavior:
    - On retryable HTTP status codes (429, 500-504): wait and retry.
    - On 429 specifically: respect Retry-After header if present.
    - On retryable exceptions (ConnectionError, Timeout, ConnectionResetError): wait and retry.
    - On 401 Unauthorized: do NOT retry. Raises TokenExpiredError with the tool name.
    - On 400, 403, 404: do NOT retry (client error, not transient). Re-raises original.
    - After max_retries exhausted: raises ToolUnavailableError.
    - Logs each retry attempt with attempt number and wait time.

    The wrapped function must call response.raise_for_status() so that HTTP errors
    surface as requests.exceptions.HTTPError for the decorator to catch.

    Example:
        @with_retry(max_retries=3)
        def call_jobber_api(endpoint, params):
            resp = session.post(endpoint, json=params, timeout=15)
            resp.raise_for_status()
            return resp.json()
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            tool_name = func.__name__
            last_exc: Exception | None = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)

                except requests.exceptions.HTTPError as exc:
                    status_code = exc.response.status_code if exc.response is not None else None

                    # 401: token expired — never retry
                    if status_code == 401:
                        raise TokenExpiredError(tool_name, str(exc)) from exc

                    # 400, 403, 404: client error — never retry
                    if status_code in _NO_RETRY_STATUS_CODES:
                        raise

                    # 429, 500-504: transient — retry with backoff
                    if status_code in _RETRYABLE_STATUS_CODES:
                        if attempt >= max_retries:
                            raise ToolUnavailableError(tool_name, str(exc)) from exc

                        # For 429: honour Retry-After header if present
                        if status_code == 429 and exc.response is not None:
                            wait = _parse_retry_after(
                                exc.response.headers.get("Retry-After")
                            )
                            if wait is None:
                                wait = _calc_delay(attempt, base_delay, backoff_factor, max_delay)
                            else:
                                wait = min(wait, max_delay)
                        else:
                            wait = _calc_delay(attempt, base_delay, backoff_factor, max_delay)

                        logger.warning(
                            "%s: HTTP %s, retry %d/%d in %.1fs",
                            tool_name, status_code, attempt + 1, max_retries, wait,
                        )
                        time.sleep(wait)
                        last_exc = exc
                        continue

                    # All other HTTP errors: don't retry
                    raise

                except (
                    requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout,
                    ConnectionResetError,
                    ConnectionError,
                ) as exc:
                    if attempt >= max_retries:
                        raise ToolUnavailableError(tool_name, str(exc)) from exc

                    wait = _calc_delay(attempt, base_delay, backoff_factor, max_delay)
                    logger.warning(
                        "%s: %s, retry %d/%d in %.1fs",
                        tool_name, type(exc).__name__, attempt + 1, max_retries, wait,
                    )
                    time.sleep(wait)
                    last_exc = exc
                    continue

            # Unreachable in normal operation, but satisfies the type checker
            raise ToolUnavailableError(tool_name, str(last_exc))

        return wrapper
    return decorator
