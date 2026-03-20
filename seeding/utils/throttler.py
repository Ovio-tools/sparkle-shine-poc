"""
Rate-limiting throttler for API calls.

Usage:
    from seeding.utils.throttler import JOBBER

    with JOBBER:
        JOBBER.track_call("clients.list")
        # make API call

    # or just use wait():
    JOBBER.wait()
    result = api_call()
"""

import time
from collections import defaultdict
from typing import Dict


class Throttler:
    """
    Enforces a minimum interval between calls to stay under a rate limit.

    Args:
        requests_per_second: Maximum calls per second (e.g. 6.5 allows ~6-7 calls/sec).
    """

    def __init__(self, requests_per_second: float):
        self._min_interval = 1.0 / requests_per_second
        self._last_call: float = 0.0
        self._call_counts: Dict[str, int] = defaultdict(int)

    def wait(self) -> None:
        """Sleep for any remaining time since the last call, then record the call time."""
        now = time.monotonic()
        elapsed = now - self._last_call
        remaining = self._min_interval - elapsed
        if remaining > 0:
            time.sleep(remaining)
        self._last_call = time.monotonic()

    def __enter__(self) -> "Throttler":
        self.wait()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        # No cleanup needed; timing is handled in __enter__ via wait()
        return None

    def track_call(self, tool_name: str) -> None:
        """Increment the call counter for tool_name."""
        self._call_counts[tool_name] += 1

    def stats(self) -> Dict[str, int]:
        """Return a copy of the per-tool call counts for this session."""
        return dict(self._call_counts)


# ---------------------------------------------------------------------------
# Pre-configured instances
# ---------------------------------------------------------------------------

JOBBER = Throttler(6.5)
QUICKBOOKS = Throttler(6.5)
HUBSPOT = Throttler(8.0)
MAILCHIMP = Throttler(6.5)
PIPEDRIVE = Throttler(6.5)
ASANA = Throttler(2.2)
SLACK = Throttler(0.9)
