"""
scripts/pg_health_check.py

Standalone PostgreSQL diagnostic for Sparkle & Shine.

Checks (in order):
  1. DATABASE_URL set in environment
  2. DB connection (SELECT 1)
  3. Full table inventory — all tables in _TABLE_NAMES from database.schema
  4. Sequence health — all tables with SERIAL PKs
  5. OAuth token rows for jobber, quickbooks, google

Exit 0 — all checks are PASS or WARN
Exit 1 — one or more checks FAIL

Usage:
    python scripts/pg_health_check.py

Works locally and on Railway without modification.
"""

from __future__ import annotations

import os
import sys

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(_PROJECT_ROOT, ".env"))

from database.health import (
    HealthCheck,
    check_connection,
    check_table_inventory,
    check_sequences,
    check_oauth_tokens,
    render_table,
)
from database.schema import _TABLE_NAMES

_TITLE = "Sparkle & Shine — PostgreSQL Health Check"


def main() -> None:
    checks: list[HealthCheck] = []

    # 1. DATABASE_URL set
    if os.environ.get("DATABASE_URL"):
        checks.append(HealthCheck("DATABASE_URL set", "PASS", ""))
    else:
        checks.append(HealthCheck(
            "DATABASE_URL set", "FAIL",
            "not set — export DATABASE_URL=postgresql://... or add to .env"
        ))
        render_table(_TITLE, checks)
        sys.exit(1)

    # 2. DB connection
    conn_check, conn = check_connection()
    checks.append(conn_check)

    if conn is None:
        for name in ("Table inventory", "Sequence health", "OAuth tokens"):
            checks.append(HealthCheck(name, "SKIP", "DB unreachable"))
        render_table(_TITLE, checks)
        sys.exit(1)

    try:
        # 3. Table inventory (all 24 tables from _TABLE_NAMES)
        checks.extend(check_table_inventory(conn, _TABLE_NAMES))

        # 4. Sequence health (discovers serial PKs via information_schema)
        checks.extend(check_sequences(conn, _TABLE_NAMES))

        # 5. OAuth token rows
        checks.extend(check_oauth_tokens(conn))
    finally:
        conn.close()

    render_table(_TITLE, checks)
    sys.exit(1 if any(c.status == "FAIL" for c in checks) else 0)


if __name__ == "__main__":
    main()
