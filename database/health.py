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
from datetime import datetime, timezone

from database.connection import get_connection, table_exists


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


def check_connection() -> tuple[HealthCheck, object]:
    """Open a DB connection and run SELECT 1.

    Returns (HealthCheck, conn) on success, (HealthCheck, None) on failure.
    Caller is responsible for closing the returned conn.
    Connection-dependent checks should be skipped if conn is None.
    """
    try:
        conn = get_connection()
        conn.execute("SELECT 1")
        return HealthCheck("DB connection", "PASS", ""), conn
    except Exception as exc:
        return HealthCheck("DB connection", "FAIL", str(exc)), None


def check_table_inventory(conn, tables: list[str]) -> list[HealthCheck]:
    """Check that every table in `tables` exists in the public schema.

    Uses table_exists() from database.connection.
    Pass _TABLE_NAMES from database.schema for a full inventory,
    or a subset for a service-scoped check.
    """
    results = []
    for table in tables:
        if table_exists(conn, table):
            results.append(HealthCheck(f"Table: {table}", "PASS", ""))
        else:
            results.append(HealthCheck(
                f"Table: {table}", "FAIL", "missing — run migrations"
            ))
    return results


def check_sequences(conn, table_names: list[str]) -> list[HealthCheck]:
    """Verify SERIAL sequences are not behind their table's max(id).

    A sequence falls behind when rows are inserted with explicit IDs
    (bypassing nextval), typically during data migrations. If the
    sequence is behind, the next INSERT will fail with a unique-
    constraint violation.

    Tables without a SERIAL PK are silently skipped.
    """
    results = []
    for table in table_names:
        seq_name = f"{table}_id_seq"

        # Check if this sequence exists in the public schema
        cursor = conn.execute(
            "SELECT 1 FROM information_schema.sequences "
            "WHERE sequence_schema = 'public' AND sequence_name = %s",
            (seq_name,),
        )
        if not cursor.fetchone():
            continue  # TEXT PK or no sequence — skip silently

        # Get sequence current last_value
        cursor = conn.execute(f'SELECT last_value FROM "{seq_name}"')
        last_value = cursor.fetchone()["last_value"]

        # Get max id in the table
        cursor = conn.execute(f'SELECT MAX(id) AS max_id FROM "{table}"')
        row = cursor.fetchone()
        max_id = row["max_id"] if row["max_id"] is not None else 0

        if max_id == 0:
            results.append(HealthCheck(
                f"Sequence: {seq_name}", "PASS", "table is empty"
            ))
        elif last_value < max_id:
            results.append(HealthCheck(
                f"Sequence: {seq_name}", "FAIL",
                f"behind: last={last_value}, max={max_id} — next INSERT will fail",
            ))
        else:
            results.append(HealthCheck(
                f"Sequence: {seq_name}", "PASS",
                f"last={last_value}, max={max_id}",
            ))
    return results


_OAUTH_TOOLS = ["jobber", "quickbooks", "google"]
_OAUTH_STALE_DAYS = 7  # ESTIMATED: refresh tokens typically valid 30–90 days;
                       # 7-day staleness suggests the token pipeline has stalled.


def check_oauth_tokens(conn) -> list[HealthCheck]:
    """Check that OAuth token rows exist and aren't stale or expired.

    Queries the oauth_tokens table (tool_name PK, token_data JSONB,
    updated_at TIMESTAMP). Works identically locally and on Railway
    — no file-system reads.

    FAIL  — row missing for a tool (no token stored at all)
    WARN  — updated_at > 7 days old (token pipeline may have stalled)
    WARN  — token_data['expires_at'] is in the past (access token expired;
             may auto-refresh on next use, but worth flagging)
    PASS  — token present, updated recently, not expired
    """
    cursor = conn.execute(
        "SELECT tool_name, token_data, updated_at FROM oauth_tokens "
        "WHERE tool_name = ANY(%s)",
        (_OAUTH_TOOLS,),
    )
    rows = {row["tool_name"]: row for row in cursor.fetchall()}

    results = []
    now = datetime.now(timezone.utc)

    for tool in _OAUTH_TOOLS:
        if tool not in rows:
            results.append(HealthCheck(
                f"OAuth token: {tool}", "FAIL", "no row in oauth_tokens"
            ))
            continue

        row = rows[tool]
        updated_at = row["updated_at"]

        # Staleness check
        if isinstance(updated_at, datetime):
            if updated_at.tzinfo is None:
                updated_at = updated_at.replace(tzinfo=timezone.utc)
            age_days = (now - updated_at).total_seconds() / 86400
            if age_days > _OAUTH_STALE_DAYS:
                results.append(HealthCheck(
                    f"OAuth token: {tool}", "WARN",
                    f"updated_at is {age_days:.0f} days ago — token may be stale",
                ))
                continue

        # Access token expiry check
        token_data = row["token_data"]
        if isinstance(token_data, dict):
            raw_exp = token_data.get("expires_at") or token_data.get("expiry")
            if raw_exp:
                try:
                    if isinstance(raw_exp, str):
                        exp_dt = datetime.fromisoformat(raw_exp.replace("Z", "+00:00"))
                    elif isinstance(raw_exp, (int, float)):
                        exp_dt = datetime.fromtimestamp(raw_exp, tz=timezone.utc)
                    else:
                        exp_dt = None

                    if exp_dt is not None:
                        if exp_dt.tzinfo is None:
                            exp_dt = exp_dt.replace(tzinfo=timezone.utc)
                        if exp_dt < now:
                            results.append(HealthCheck(
                                f"OAuth token: {tool}", "WARN",
                                f"expires_at in the past ({exp_dt.strftime('%Y-%m-%d %H:%M')} UTC)",
                            ))
                            continue
                except (ValueError, OSError, OverflowError):
                    pass  # unparseable expiry — don't FAIL, just skip the check

        results.append(HealthCheck(f"OAuth token: {tool}", "PASS", "present and not expired"))

    return results
