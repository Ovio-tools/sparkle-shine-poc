"""
database/health.py

Shared health check primitives for Sparkle & Shine service runners.

Provides:
  - HealthCheck dataclass
  - check_connection()         -- can we reach the DB?
  - check_table_inventory()    -- are all expected tables present?
  - check_sequences()          -- are SERIAL sequences in sync with max(id)?
  - check_oauth_tokens()       -- are OAuth tokens present and not expired?
  - render_table()             -- print a PASS/WARN/FAIL table to stdout
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class HealthCheck:
    name: str
    status: str    # "PASS" | "WARN" | "FAIL" | "SKIP"
    message: str


_MARKER = {"PASS": "✓", "WARN": "!", "FAIL": "✗", "SKIP": "-"}


def render_table(title: str, checks: list[HealthCheck]) -> None:
    """Print a bordered results table to stdout.

    Uses print() — not logger — so output is clean stdout without
    log timestamps, suitable for terminal or Railway log tailing.
    """
    line = "=" * 48
    print(f"\n{title}")
    print(line)
    for c in checks:
        sym = _MARKER.get(c.status, "?")
        msg = f"  {c.message}" if c.message else ""
        print(f"  {sym} {c.status:<4}  {c.name}{msg}")
    print(line)

    fail_count = sum(1 for c in checks if c.status == "FAIL")
    warn_count = sum(1 for c in checks if c.status == "WARN")

    if fail_count:
        print(f"  Result: FAIL ({fail_count} failure(s), {warn_count} warning(s))")
    elif warn_count:
        print(f"  Result: WARN ({warn_count} warning(s))")
    else:
        print("  Result: PASS")
    print()
