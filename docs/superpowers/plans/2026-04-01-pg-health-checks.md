# PostgreSQL Health Checks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `database/health.py` shared primitives, a standalone `scripts/pg_health_check.py` diagnostic CLI, and a `--health` flag to `simulation/engine.py`, `automations/runner.py`, and `intelligence/runner.py` that each answer "can this service do its job right now?"

**Architecture:** A shared `database/health.py` module owns the `HealthCheck` dataclass, four check functions, and the table renderer. `scripts/pg_health_check.py` is the standalone CLI entry point. Each runner imports from `database.health` and adds its own service-specific checks in a `_run_health_check()` function that short-circuits before any normal operation runs.

**Tech Stack:** Python 3, psycopg2, pytest, unittest.mock. No new dependencies.

---

## File Map

| File | Change | Responsibility |
|------|--------|----------------|
| `database/health.py` | **Create** | `HealthCheck`, `check_connection`, `check_table_inventory`, `check_sequences`, `check_oauth_tokens`, `render_table` |
| `scripts/pg_health_check.py` | **Create** | Standalone CLI: runs all 5 common checks against full `_TABLE_NAMES` list |
| `simulation/engine.py` | **Modify** | Add `--health` argparse flag + `_run_health_check()` function |
| `automations/runner.py` | **Modify** | Add `--health` argparse flag + `_run_health_check()` function |
| `intelligence/runner.py` | **Modify** | Add `--health` argparse flag + `_run_health_check()` function |
| `tests/test_health.py` | **Create** | Unit tests for all health check functions and runner `_run_health_check()` calls |

---

## Task 1: `HealthCheck` dataclass + `render_table`

**Files:**
- Create: `database/health.py`
- Create: `tests/test_health.py`

- [ ] **Step 1: Write the failing tests for `render_table`**

Create `tests/test_health.py`:

```python
"""
tests/test_health.py

Unit tests for database/health.py check functions and render_table.
All DB interactions are mocked — no live database required.
"""
import pytest
from unittest.mock import MagicMock, patch
from dataclasses import dataclass


def test_render_table_all_pass(capsys):
    from database.health import HealthCheck, render_table
    checks = [
        HealthCheck("DB connection", "PASS", ""),
        HealthCheck("Table: clients", "PASS", ""),
    ]
    render_table("Test Title", checks)
    captured = capsys.readouterr()
    assert "✓ PASS" in captured.out
    assert "Result: PASS" in captured.out
    assert "Test Title" in captured.out


def test_render_table_with_fail(capsys):
    from database.health import HealthCheck, render_table
    checks = [
        HealthCheck("DB connection", "PASS", ""),
        HealthCheck("Table: foo", "FAIL", "missing — run migrations"),
    ]
    render_table("Test Title", checks)
    captured = capsys.readouterr()
    assert "✗ FAIL" in captured.out
    assert "Result: FAIL" in captured.out
    assert "1 failure" in captured.out


def test_render_table_warn_only(capsys):
    from database.health import HealthCheck, render_table
    checks = [HealthCheck("OAuth token: jobber", "WARN", "expired")]
    render_table("Test Title", checks)
    captured = capsys.readouterr()
    assert "!" in captured.out
    assert "Result: WARN" in captured.out


def test_render_table_skip(capsys):
    from database.health import HealthCheck, render_table
    checks = [HealthCheck("Table inventory", "SKIP", "DB unreachable")]
    render_table("Test Title", checks)
    captured = capsys.readouterr()
    assert "SKIP" in captured.out
    assert "Result: PASS" in captured.out  # SKIP is neutral, not a failure
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd sparkle-shine-poc
python -m pytest tests/test_health.py -v 2>&1 | head -30
```

Expected: `ModuleNotFoundError: No module named 'database.health'`

- [ ] **Step 3: Create `database/health.py` with `HealthCheck` and `render_table`**

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_health.py::test_render_table_all_pass \
    tests/test_health.py::test_render_table_with_fail \
    tests/test_health.py::test_render_table_warn_only \
    tests/test_health.py::test_render_table_skip -v
```

Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add database/health.py tests/test_health.py
git commit -m "Add HealthCheck dataclass and render_table (health checks Task 1)"
```

---

## Task 2: `check_connection()`

**Files:**
- Modify: `database/health.py`
- Modify: `tests/test_health.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_health.py`:

```python
def test_check_connection_pass():
    from database.health import check_connection, HealthCheck
    mock_conn = MagicMock()
    with patch("database.health.get_connection", return_value=mock_conn):
        result_check, result_conn = check_connection()
    assert result_check.status == "PASS"
    assert result_conn is mock_conn


def test_check_connection_fail():
    from database.health import check_connection
    with patch("database.health.get_connection", side_effect=Exception("connection refused")):
        result_check, result_conn = check_connection()
    assert result_check.status == "FAIL"
    assert result_conn is None
    assert "connection refused" in result_check.message
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_health.py::test_check_connection_pass \
    tests/test_health.py::test_check_connection_fail -v
```

Expected: `ImportError` or `AttributeError: module 'database.health' has no attribute 'check_connection'`

- [ ] **Step 3: Add `check_connection` to `database/health.py`**

Add after the imports at the top of `database/health.py`:

```python
from database.connection import get_connection, table_exists
```

Add the function after `render_table`:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_health.py::test_check_connection_pass \
    tests/test_health.py::test_check_connection_fail -v
```

Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add database/health.py tests/test_health.py
git commit -m "Add check_connection to database/health (health checks Task 2)"
```

---

## Task 3: `check_table_inventory()`

**Files:**
- Modify: `database/health.py`
- Modify: `tests/test_health.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_health.py`:

```python
def test_check_table_inventory_all_present():
    from database.health import check_table_inventory
    mock_conn = MagicMock()
    with patch("database.health.table_exists", return_value=True):
        results = check_table_inventory(mock_conn, ["clients", "jobs"])
    assert len(results) == 2
    assert all(c.status == "PASS" for c in results)


def test_check_table_inventory_one_missing():
    from database.health import check_table_inventory
    mock_conn = MagicMock()

    def _exists(conn, table):
        return table != "jobs"

    with patch("database.health.table_exists", side_effect=_exists):
        results = check_table_inventory(mock_conn, ["clients", "jobs"])

    by_name = {c.name: c for c in results}
    assert by_name["Table: clients"].status == "PASS"
    assert by_name["Table: jobs"].status == "FAIL"
    assert "missing" in by_name["Table: jobs"].message


def test_check_table_inventory_empty_list():
    from database.health import check_table_inventory
    mock_conn = MagicMock()
    results = check_table_inventory(mock_conn, [])
    assert results == []
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_health.py::test_check_table_inventory_all_present \
    tests/test_health.py::test_check_table_inventory_one_missing \
    tests/test_health.py::test_check_table_inventory_empty_list -v
```

Expected: `AttributeError: module 'database.health' has no attribute 'check_table_inventory'`

- [ ] **Step 3: Add `check_table_inventory` to `database/health.py`**

Add after `check_connection`:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_health.py::test_check_table_inventory_all_present \
    tests/test_health.py::test_check_table_inventory_one_missing \
    tests/test_health.py::test_check_table_inventory_empty_list -v
```

Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add database/health.py tests/test_health.py
git commit -m "Add check_table_inventory to database/health (health checks Task 3)"
```

---

## Task 4: `check_sequences()`

**Files:**
- Modify: `database/health.py`
- Modify: `tests/test_health.py`

**Background:** Only 5 of the 24 tables have `SERIAL PRIMARY KEY` and thus PostgreSQL sequences:
`marketing_interactions`, `cross_tool_mapping`, `document_index`, `automation_log`, `pending_actions`.
Tables with TEXT PKs (clients, jobs, etc.) have no sequence. The check discovers sequences
via `information_schema.sequences` rather than hardcoding names, so TEXT-PK tables are
silently skipped.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_health.py`:

```python
def test_check_sequences_skips_text_pk_tables():
    """Tables without a SERIAL PK produce no HealthCheck entries."""
    from database.health import check_sequences
    mock_conn = MagicMock()
    # information_schema.sequences returns nothing for 'clients'
    mock_conn.execute.return_value.fetchone.return_value = None
    results = check_sequences(mock_conn, ["clients"])
    assert results == []


def _make_seq_conn(seq_exists: bool, last_value: int, max_id: int | None):
    """Helper: build a mock conn that simulates sequence queries."""
    mock_conn = MagicMock()

    def _execute(sql, params=None):
        cursor = MagicMock()
        if "information_schema.sequences" in sql:
            cursor.fetchone.return_value = (
                {"sequence_name": "pending_actions_id_seq"} if seq_exists else None
            )
        elif "last_value" in sql:
            cursor.fetchone.return_value = {"last_value": last_value}
        elif "MAX(id)" in sql:
            cursor.fetchone.return_value = {"max_id": max_id}
        return cursor

    mock_conn.execute.side_effect = _execute
    return mock_conn


def test_check_sequences_pass_in_sync():
    from database.health import check_sequences
    conn = _make_seq_conn(seq_exists=True, last_value=50, max_id=50)
    results = check_sequences(conn, ["pending_actions"])
    assert len(results) == 1
    assert results[0].status == "PASS"
    assert "last=50" in results[0].message


def test_check_sequences_fail_behind():
    from database.health import check_sequences
    conn = _make_seq_conn(seq_exists=True, last_value=45, max_id=50)
    results = check_sequences(conn, ["pending_actions"])
    assert results[0].status == "FAIL"
    assert "behind" in results[0].message
    assert "last=45" in results[0].message
    assert "max=50" in results[0].message


def test_check_sequences_pass_empty_table():
    """Sequence exists but table has no rows — nothing can drift."""
    from database.health import check_sequences
    conn = _make_seq_conn(seq_exists=True, last_value=1, max_id=None)
    results = check_sequences(conn, ["pending_actions"])
    assert results[0].status == "PASS"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_health.py -k "test_check_sequences" -v
```

Expected: `AttributeError: module 'database.health' has no attribute 'check_sequences'`

- [ ] **Step 3: Add `check_sequences` to `database/health.py`**

Add after `check_table_inventory`:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_health.py -k "test_check_sequences" -v
```

Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add database/health.py tests/test_health.py
git commit -m "Add check_sequences to database/health (health checks Task 4)"
```

---

## Task 5: `check_oauth_tokens()`

**Files:**
- Modify: `database/health.py`
- Modify: `tests/test_health.py`

**Background:** OAuth tokens live in the `oauth_tokens` table (tool_name TEXT PK, token_data JSONB, updated_at TIMESTAMP). psycopg2 returns `updated_at` as a Python `datetime` object. `token_data` arrives as a Python `dict` (psycopg2 deserialises JSONB automatically).

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_health.py`:

```python
from datetime import datetime, timezone, timedelta


def _make_oauth_conn(rows: list[dict]):
    """Helper: conn whose execute().fetchall() returns rows."""
    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetchall.return_value = rows
    return mock_conn


def test_check_oauth_tokens_all_present_and_valid():
    from database.health import check_oauth_tokens
    now = datetime.now(timezone.utc)
    future = (now + timedelta(hours=2)).isoformat()
    rows = [
        {"tool_name": "jobber",     "token_data": {"expires_at": future}, "updated_at": now},
        {"tool_name": "quickbooks", "token_data": {"expires_at": future}, "updated_at": now},
        {"tool_name": "google",     "token_data": {},                     "updated_at": now},
    ]
    results = check_oauth_tokens(_make_oauth_conn(rows))
    assert all(c.status == "PASS" for c in results), [c for c in results if c.status != "PASS"]


def test_check_oauth_tokens_missing_tool():
    from database.health import check_oauth_tokens
    now = datetime.now(timezone.utc)
    rows = [{"tool_name": "jobber", "token_data": {}, "updated_at": now}]
    results = check_oauth_tokens(_make_oauth_conn(rows))
    by_name = {c.name: c for c in results}
    assert by_name["OAuth token: jobber"].status == "PASS"
    assert by_name["OAuth token: quickbooks"].status == "FAIL"
    assert by_name["OAuth token: google"].status == "FAIL"


def test_check_oauth_tokens_stale_updated_at():
    from database.health import check_oauth_tokens
    stale = datetime.now(timezone.utc) - timedelta(days=10)
    rows = [
        {"tool_name": "jobber",     "token_data": {}, "updated_at": stale},
        {"tool_name": "quickbooks", "token_data": {}, "updated_at": stale},
        {"tool_name": "google",     "token_data": {}, "updated_at": stale},
    ]
    results = check_oauth_tokens(_make_oauth_conn(rows))
    assert all(c.status == "WARN" for c in results)
    assert all("days ago" in c.message for c in results)


def test_check_oauth_tokens_expired_access_token():
    from database.health import check_oauth_tokens
    now = datetime.now(timezone.utc)
    past = (now - timedelta(hours=1)).isoformat()
    rows = [
        {"tool_name": "jobber",     "token_data": {"expires_at": past}, "updated_at": now},
        {"tool_name": "quickbooks", "token_data": {"expires_at": past}, "updated_at": now},
        {"tool_name": "google",     "token_data": {"expires_at": past}, "updated_at": now},
    ]
    results = check_oauth_tokens(_make_oauth_conn(rows))
    assert all(c.status == "WARN" for c in results)
    assert all("expires_at" in c.message for c in results)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_health.py -k "test_check_oauth_tokens" -v
```

Expected: `AttributeError: module 'database.health' has no attribute 'check_oauth_tokens'`

- [ ] **Step 3: Add `check_oauth_tokens` to `database/health.py`**

Add at the top of the file, after the existing imports:

```python
from datetime import datetime, timezone
```

Add the function after `check_sequences`:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_health.py -k "test_check_oauth_tokens" -v
```

Expected: 4 passed

- [ ] **Step 5: Run the full test suite for `test_health.py`**

```bash
python -m pytest tests/test_health.py -v
```

Expected: All tests pass (currently 16 tests)

- [ ] **Step 6: Commit**

```bash
git add database/health.py tests/test_health.py
git commit -m "Add check_oauth_tokens to database/health (health checks Task 5)"
```

---

## Task 6: `scripts/pg_health_check.py`

**Files:**
- Create: `scripts/pg_health_check.py`
- Modify: `tests/test_health.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_health.py`:

```python
def test_pg_health_check_exits_0_when_all_pass():
    """main() exits 0 when all checks pass."""
    import sys
    from unittest.mock import patch, MagicMock
    from database.health import HealthCheck

    mock_conn = MagicMock()
    pass_check = HealthCheck("DB connection", "PASS", "")

    with patch("scripts.pg_health_check.os.environ.get", return_value="postgresql://fake"), \
         patch("database.health.get_connection", return_value=mock_conn), \
         patch("database.health.table_exists", return_value=True), \
         patch("database.health.check_sequences", return_value=[HealthCheck("Sequence: x", "PASS", "")]), \
         patch("database.health.check_oauth_tokens", return_value=[HealthCheck("OAuth token: jobber", "PASS", "")]), \
         patch("database.health.render_table"), \
         pytest.raises(SystemExit) as exc_info:
        import importlib
        import scripts.pg_health_check as m
        importlib.reload(m)
        m.main()

    assert exc_info.value.code == 0


def test_pg_health_check_exits_1_on_fail():
    """main() exits 1 when DATABASE_URL is not set."""
    with patch("os.environ.get", return_value=None), \
         patch("database.health.render_table"), \
         pytest.raises(SystemExit) as exc_info:
        import importlib
        import scripts.pg_health_check as m
        importlib.reload(m)
        m.main()

    assert exc_info.value.code == 1
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_health.py -k "test_pg_health_check" -v
```

Expected: `ModuleNotFoundError: No module named 'scripts.pg_health_check'`

- [ ] **Step 3: Create `scripts/pg_health_check.py`**

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_health.py -k "test_pg_health_check" -v
```

Expected: 2 passed

- [ ] **Step 5: Smoke-test locally (requires DATABASE_URL)**

```bash
python scripts/pg_health_check.py
```

Expected: table printed to stdout, exit 0 if DB healthy.
If `DATABASE_URL` is not set: FAIL on check 1, exit 1.

- [ ] **Step 6: Commit**

```bash
git add scripts/pg_health_check.py tests/test_health.py
git commit -m "Add scripts/pg_health_check.py standalone diagnostic (health checks Task 6)"
```

---

## Task 7: `simulation/engine.py --health`

**Files:**
- Modify: `simulation/engine.py`
- Modify: `tests/test_health.py`

The `--health` flag is handled before the engine is constructed. `_run_health_check()` is a module-level function (not a method) that imports from `database.health` inside the function body and calls `sys.exit()` after rendering.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_health.py`:

```python
def test_simulation_engine_health_exits_0_all_pass():
    """--health exits 0 when connection and tables are healthy."""
    from database.health import HealthCheck

    pass_hc = HealthCheck("DB connection", "PASS", "")
    mock_conn = MagicMock()

    with patch("database.health.get_connection", return_value=mock_conn), \
         patch("database.health.table_exists", return_value=True), \
         patch("database.health.check_sequences", return_value=[]), \
         patch("database.health.render_table"), \
         patch("simulation.engine.CHECKPOINT_FILE") as mock_cp:

        mock_cp.exists.return_value = False  # no checkpoint — first run

        from simulation.engine import _run_health_check
        with pytest.raises(SystemExit) as exc_info:
            _run_health_check()

    assert exc_info.value.code == 0


def test_simulation_engine_health_exits_1_on_db_fail():
    """--health exits 1 when DB connection fails."""
    with patch("database.health.get_connection", side_effect=Exception("timeout")), \
         patch("database.health.render_table"):

        from simulation.engine import _run_health_check
        with pytest.raises(SystemExit) as exc_info:
            _run_health_check()

    assert exc_info.value.code == 1


def test_simulation_engine_health_warns_stale_checkpoint(tmp_path):
    """--health emits WARN when checkpoint date is >1 day old."""
    import json
    from datetime import date, timedelta
    from database.health import HealthCheck

    old_date = (date.today() - timedelta(days=3)).isoformat()
    cp_file = tmp_path / "checkpoint.json"
    cp_file.write_text(json.dumps({"date": old_date, "counters": {}}))

    mock_conn = MagicMock()
    rendered_checks = []

    def _capture(title, checks):
        rendered_checks.extend(checks)

    with patch("database.health.get_connection", return_value=mock_conn), \
         patch("database.health.table_exists", return_value=True), \
         patch("database.health.check_sequences", return_value=[]), \
         patch("database.health.render_table", side_effect=_capture), \
         patch("simulation.engine.CHECKPOINT_FILE", cp_file):

        from simulation.engine import _run_health_check
        with pytest.raises(SystemExit):
            _run_health_check()

    warn_checks = [c for c in rendered_checks if c.status == "WARN" and "Checkpoint" in c.name]
    assert len(warn_checks) == 1
    assert "days old" in warn_checks[0].message
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_health.py -k "test_simulation_engine_health" -v
```

Expected: `ImportError: cannot import name '_run_health_check' from 'simulation.engine'`

- [ ] **Step 3: Add `_run_health_check` to `simulation/engine.py`**

Add this function before the `if __name__ == "__main__":` block (after `SimulationEngine` class ends, around line 430):

```python
def _run_health_check() -> None:
    """Run simulation engine health checks and exit.

    Answers: 'Can the simulation engine generate events right now?'
    Called by --health before the engine is constructed.
    Exits 0 if all checks PASS or WARN, exits 1 if any FAIL.
    """
    import importlib

    from database.health import (
        HealthCheck,
        check_connection,
        check_table_inventory,
        check_sequences,
        render_table,
    )

    checks: list[HealthCheck] = []

    # 1. DB connection
    conn_check, conn = check_connection()
    checks.append(conn_check)

    _TABLES = ["clients", "jobs", "invoices", "payments", "cross_tool_mapping"]

    if conn is None:
        for name in ("Table inventory", "Sequence health"):
            checks.append(HealthCheck(name, "SKIP", "DB unreachable"))
    else:
        try:
            # 2. Table inventory
            checks.extend(check_table_inventory(conn, _TABLES))
            # 3. Sequence health (cross_tool_mapping has SERIAL PK; others use TEXT)
            checks.extend(check_sequences(conn, _TABLES))
        finally:
            conn.close()

    # 4. Checkpoint freshness
    if CHECKPOINT_FILE.exists():
        try:
            import json
            from datetime import date as _date
            state = json.loads(CHECKPOINT_FILE.read_text())
            cp_date = _date.fromisoformat(state["date"])
            delta = (_date.today() - cp_date).days
            if delta > 1:
                checks.append(HealthCheck(
                    "Checkpoint freshness", "WARN",
                    f"checkpoint is {delta} day(s) old — engine may have stopped",
                ))
            else:
                checks.append(HealthCheck(
                    "Checkpoint freshness", "PASS", f"date={cp_date}",
                ))
        except Exception as exc:
            checks.append(HealthCheck(
                "Checkpoint freshness", "WARN", f"could not parse checkpoint: {exc}",
            ))
    else:
        checks.append(HealthCheck(
            "Checkpoint freshness", "PASS", "no checkpoint file (first run)",
        ))

    # 5. Generator imports
    _GENERATOR_IMPORTS = [
        ("simulation.generators.operations", "NewClientSetupGenerator"),
        ("simulation.generators.operations", "JobSchedulingGenerator"),
        ("simulation.generators.operations", "JobCompletionGenerator"),
        ("simulation.generators.contacts",   "ContactGenerator"),
        ("simulation.generators.deals",      "DealGenerator"),
        ("simulation.generators.churn",      "ChurnGenerator"),
        ("simulation.generators.payments",   "PaymentGenerator"),
        ("simulation.generators.tasks",      "TaskCompletionGenerator"),
    ]
    for module_path, class_name in _GENERATOR_IMPORTS:
        try:
            mod = importlib.import_module(module_path)
            getattr(mod, class_name)
            checks.append(HealthCheck(f"Import: {class_name}", "PASS", ""))
        except (ImportError, AttributeError) as exc:
            checks.append(HealthCheck(f"Import: {class_name}", "WARN", str(exc)))

    render_table("Simulation Engine — Health Check", checks)
    sys.exit(1 if any(c.status == "FAIL" for c in checks) else 0)
```

- [ ] **Step 4: Add `--health` to the argparse block in `simulation/engine.py`**

In the `if __name__ == "__main__":` block, add `--health` after the existing `--verbose` argument (around line 455):

```python
    parser.add_argument(
        "--health",
        action="store_true",
        help="Run service health checks and exit. Does not start the engine.",
    )
```

And add the early-exit call right after `args = parser.parse_args()`:

```python
    args = parser.parse_args()

    if args.health:
        _run_health_check()  # exits internally
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
python -m pytest tests/test_health.py -k "test_simulation_engine_health" -v
```

Expected: 3 passed

- [ ] **Step 6: Smoke-test the flag**

```bash
python -m simulation.engine --health
```

Expected: health table printed to stdout, process exits (0 if DB up, 1 if not).

- [ ] **Step 7: Commit**

```bash
git add simulation/engine.py tests/test_health.py
git commit -m "Add --health flag to simulation/engine.py (health checks Task 7)"
```

---

## Task 8: `automations/runner.py --health`

**Files:**
- Modify: `automations/runner.py`
- Modify: `tests/test_health.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_health.py`:

```python
def test_automations_runner_health_exits_0_all_pass():
    """--health exits 0 when tables present and no stale sentinels."""
    mock_conn = MagicMock()

    with patch("database.health.get_connection", return_value=mock_conn), \
         patch("database.health.table_exists", return_value=True), \
         patch("database.health.check_sequences", return_value=[]), \
         patch("database.health.render_table"), \
         patch("os.path.exists", return_value=False):  # sentinels missing = first run

        from automations.runner import _run_health_check
        with pytest.raises(SystemExit) as exc_info:
            _run_health_check()

    assert exc_info.value.code == 0


def test_automations_runner_health_warns_stale_lead_leak(tmp_path):
    """--health emits WARN when lead_leak sentinel is >48h old."""
    import time

    sentinel = tmp_path / ".lead_leak_last_run"
    sentinel.write_text("")
    # backdate mtime to 3 days ago
    old_mtime = time.time() - (3 * 86400)
    os.utime(str(sentinel), (old_mtime, old_mtime))

    mock_conn = MagicMock()
    rendered_checks = []

    def _capture(title, checks):
        rendered_checks.extend(checks)

    with patch("database.health.get_connection", return_value=mock_conn), \
         patch("database.health.table_exists", return_value=True), \
         patch("database.health.check_sequences", return_value=[]), \
         patch("database.health.render_table", side_effect=_capture), \
         patch("automations.runner._LEAD_LEAK_SENTINEL", str(sentinel)), \
         patch("automations.runner._OVERDUE_INVOICE_SENTINEL", str(tmp_path / ".overdue_invoice_last_run")):

        from automations.runner import _run_health_check
        with pytest.raises(SystemExit):
            _run_health_check()

    warn_checks = [c for c in rendered_checks if c.status == "WARN" and "lead" in c.name.lower()]
    assert len(warn_checks) == 1
    assert "h ago" in warn_checks[0].message
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_health.py -k "test_automations_runner_health" -v
```

Expected: `ImportError: cannot import name '_run_health_check' from 'automations.runner'`

- [ ] **Step 3: Add `_run_health_check` to `automations/runner.py`**

Add this function after `_merge` (around line 398) and before the `main()` function:

```python
def _run_health_check() -> None:
    """Run automation runner health checks and exit.

    Answers: 'Can the automation runner process triggers right now?'
    Called by --health before migrations or polling run.
    Exits 0 if all checks PASS or WARN, exits 1 if any FAIL.
    """
    import importlib

    from database.health import (
        HealthCheck,
        check_connection,
        check_table_inventory,
        check_sequences,
        render_table,
    )

    checks: list[HealthCheck] = []

    # 1. DB connection
    conn_check, conn = check_connection()
    checks.append(conn_check)

    _TABLES     = ["pending_actions", "poll_state"]
    _SEQ_TABLES = ["pending_actions", "automation_log"]

    if conn is None:
        for name in ("Table inventory", "Sequence health"):
            checks.append(HealthCheck(name, "SKIP", "DB unreachable"))
    else:
        try:
            # 2. Table inventory
            checks.extend(check_table_inventory(conn, _TABLES))
            # 3. Sequence health for the two SERIAL-PK automation tables
            checks.extend(check_sequences(conn, _SEQ_TABLES))
        finally:
            conn.close()

    # 4. Sentinel age
    _sentinels = [
        (_LEAD_LEAK_SENTINEL,       "Lead leak sentinel",       48 * 3600),
        (_OVERDUE_INVOICE_SENTINEL, "Overdue invoice sentinel", 14 * 86400),
    ]
    for sentinel_path, name, max_age_seconds in _sentinels:
        if not os.path.exists(sentinel_path):
            checks.append(HealthCheck(name, "PASS", "missing (first run)"))
        else:
            age_seconds = time.time() - os.path.getmtime(sentinel_path)
            if age_seconds > max_age_seconds:
                checks.append(HealthCheck(
                    name, "WARN",
                    f"last ran {age_seconds / 3600:.0f}h ago",
                ))
            else:
                checks.append(HealthCheck(name, "PASS", ""))

    # 5. Automation module imports
    _AUTOMATION_IMPORTS = [
        ("automations.new_client_onboarding",  "NewClientOnboarding"),
        ("automations.job_completion_flow",     "JobCompletionFlow"),
        ("automations.payment_received",        "PaymentReceived"),
        ("automations.negative_review",         "NegativeReviewResponse"),
        ("automations.lead_leak_detection",     "LeadLeakDetection"),
        ("automations.overdue_invoice",         "OverdueInvoiceEscalation"),
        ("automations.hubspot_qualified_sync",  "HubSpotQualifiedSync"),
    ]
    for module_path, class_name in _AUTOMATION_IMPORTS:
        try:
            mod = importlib.import_module(module_path)
            getattr(mod, class_name)
            checks.append(HealthCheck(f"Import: {class_name}", "PASS", ""))
        except (ImportError, AttributeError) as exc:
            checks.append(HealthCheck(f"Import: {class_name}", "WARN", str(exc)))

    render_table("Automation Runner — Health Check", checks)
    sys.exit(1 if any(c.status == "FAIL" for c in checks) else 0)
```

- [ ] **Step 4: Add `--health` to the argparse block in `automations/runner.py`**

In `main()`, add `--health` to the parser (after the existing `--dry-run` argument):

```python
    parser.add_argument(
        "--health",
        action="store_true",
        help="Run service health checks and exit. Does not process any triggers.",
    )
```

Add the early-exit call right after `args = parser.parse_args()` and before the `if not (args.poll ...)` default logic:

```python
    args = parser.parse_args()

    if args.health:
        _run_health_check()  # exits internally
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
python -m pytest tests/test_health.py -k "test_automations_runner_health" -v
```

Expected: 2 passed

- [ ] **Step 6: Smoke-test the flag**

```bash
python -m automations.runner --health
```

Expected: health table printed to stdout, exits 0 or 1.

- [ ] **Step 7: Commit**

```bash
git add automations/runner.py tests/test_health.py
git commit -m "Add --health flag to automations/runner.py (health checks Task 8)"
```

---

## Task 9: `intelligence/runner.py --health`

**Files:**
- Modify: `intelligence/runner.py`
- Modify: `tests/test_health.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_health.py`:

```python
def test_intelligence_runner_health_exits_0_all_pass():
    """--health exits 0 when DB, ANTHROPIC_API_KEY, and briefings/ are all good."""
    mock_conn = MagicMock()

    with patch("database.health.get_connection", return_value=mock_conn), \
         patch("database.health.table_exists", return_value=True), \
         patch("database.health.check_sequences", return_value=[]), \
         patch("database.health.check_oauth_tokens", return_value=[]), \
         patch("database.health.render_table"), \
         patch("os.environ.get", return_value="sk-fake-key"), \
         patch("os.path.isdir", return_value=True):

        from intelligence.runner import _run_health_check
        with pytest.raises(SystemExit) as exc_info:
            _run_health_check()

    assert exc_info.value.code == 0


def test_intelligence_runner_health_fails_without_api_key():
    """--health exits 1 when ANTHROPIC_API_KEY is not set."""
    mock_conn = MagicMock()
    rendered_checks = []

    def _capture(title, checks):
        rendered_checks.extend(checks)

    with patch("database.health.get_connection", return_value=mock_conn), \
         patch("database.health.table_exists", return_value=True), \
         patch("database.health.check_sequences", return_value=[]), \
         patch("database.health.check_oauth_tokens", return_value=[]), \
         patch("database.health.render_table", side_effect=_capture), \
         patch("os.path.isdir", return_value=True):

        # Simulate ANTHROPIC_API_KEY missing: os.environ.get returns None
        original_get = os.environ.get
        def _fake_get(key, default=None):
            if key == "ANTHROPIC_API_KEY":
                return None
            return original_get(key, default)

        with patch("os.environ.get", side_effect=_fake_get):
            from intelligence.runner import _run_health_check
            with pytest.raises(SystemExit) as exc_info:
                _run_health_check()

    assert exc_info.value.code == 1
    fail_checks = [c for c in rendered_checks if c.status == "FAIL" and "ANTHROPIC" in c.name]
    assert len(fail_checks) == 1
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_health.py -k "test_intelligence_runner_health" -v
```

Expected: `ImportError: cannot import name '_run_health_check' from 'intelligence.runner'`

- [ ] **Step 3: Add `_run_health_check` to `intelligence/runner.py`**

Add this function after `_print_summary` (around line 415) and before `_build_parser`:

```python
def _run_health_check() -> None:
    """Run intelligence pipeline health checks and exit.

    Answers: 'Can the intelligence pipeline produce a briefing right now?'
    Called by --health before any pipeline stage runs.
    Exits 0 if all checks PASS or WARN, exits 1 if any FAIL.
    """
    import importlib

    from database.health import (
        HealthCheck,
        check_connection,
        check_table_inventory,
        check_sequences,
        check_oauth_tokens,
        render_table,
    )

    checks: list[HealthCheck] = []

    _TABLES = [
        "daily_metrics_snapshot", "document_index",
        "jobs", "clients", "invoices",
    ]

    # 1. DB connection
    conn_check, conn = check_connection()
    checks.append(conn_check)

    if conn is None:
        for name in ("Table inventory", "Sequence health", "OAuth tokens"):
            checks.append(HealthCheck(name, "SKIP", "DB unreachable"))
    else:
        try:
            # 2. Table inventory
            checks.extend(check_table_inventory(conn, _TABLES))
            # 3. Sequence health (document_index has SERIAL PK; others use TEXT)
            checks.extend(check_sequences(conn, _TABLES))
            # 5. OAuth tokens (jobber, quickbooks, google)
            checks.extend(check_oauth_tokens(conn))
        finally:
            conn.close()

    # 4. ANTHROPIC_API_KEY
    if os.environ.get("ANTHROPIC_API_KEY"):
        checks.append(HealthCheck("ANTHROPIC_API_KEY", "PASS", "set"))
    else:
        checks.append(HealthCheck(
            "ANTHROPIC_API_KEY", "FAIL",
            "not set — add to .env or Railway environment",
        ))

    # 6. briefings/ directory
    if os.path.isdir(BRIEFINGS_DIR):
        checks.append(HealthCheck("briefings/ directory", "PASS", ""))
    else:
        checks.append(HealthCheck(
            "briefings/ directory", "WARN",
            f"{BRIEFINGS_DIR} does not exist (will be created on first run)",
        ))

    # 7. Syncer imports
    _SYNCER_IMPORTS = [
        ("intelligence.syncers.sync_google",     "GoogleSyncer"),
        ("intelligence.syncers.sync_jobber",      "JobberSyncer"),
        ("intelligence.syncers.sync_quickbooks",  "QuickBooksSyncer"),
        ("intelligence.syncers.sync_hubspot",     "HubSpotSyncer"),
        ("intelligence.syncers.sync_pipedrive",   "PipedriveSyncer"),
        ("intelligence.syncers.sync_mailchimp",   "MailchimpSyncer"),
        ("intelligence.syncers.sync_asana",       "AsanaSyncer"),
        ("intelligence.syncers.sync_slack",       "SlackSyncer"),
    ]
    for module_path, class_name in _SYNCER_IMPORTS:
        try:
            mod = importlib.import_module(module_path)
            getattr(mod, class_name)
            checks.append(HealthCheck(f"Import: {class_name}", "PASS", ""))
        except (ImportError, AttributeError) as exc:
            checks.append(HealthCheck(f"Import: {class_name}", "WARN", str(exc)))

    render_table("Intelligence Runner — Health Check", checks)
    sys.exit(1 if any(c.status == "FAIL" for c in checks) else 0)
```

- [ ] **Step 4: Add `--health` to `_build_parser()` in `intelligence/runner.py`**

Inside `_build_parser()`, add `--health` as the last argument before `return parser`:

```python
    parser.add_argument(
        "--health",
        action="store_true",
        help="Run service health checks and exit. Does not run the pipeline.",
    )
```

- [ ] **Step 5: Handle `--health` early in `main()` in `intelligence/runner.py`**

In `main()`, add the early-exit right after `args = parser.parse_args()`, before the `--preflight` check:

```python
    args = parser.parse_args()

    if args.health:
        _run_health_check()  # exits internally
```

- [ ] **Step 6: Run tests to verify they pass**

```bash
python -m pytest tests/test_health.py -k "test_intelligence_runner_health" -v
```

Expected: 2 passed

- [ ] **Step 7: Run the full test suite**

```bash
python -m pytest tests/test_health.py -v
```

Expected: All tests pass.

- [ ] **Step 8: Smoke-test the flag**

```bash
python -m intelligence.runner --health
```

Expected: health table printed to stdout, exits 0 or 1.

- [ ] **Step 9: Commit**

```bash
git add intelligence/runner.py tests/test_health.py
git commit -m "Add --health flag to intelligence/runner.py (health checks Task 9)"
```

---

## Self-Review Notes

**Spec coverage:**
- `database/health.py` — Tasks 1–5 ✓
- `scripts/pg_health_check.py` — Task 6 ✓
- `simulation/engine.py --health` (DB + tables + sequences + checkpoint + generators) — Task 7 ✓
- `automations/runner.py --health` (DB + tables + sequences for both SERIAL tables + sentinels + automations) — Task 8 ✓
- `intelligence/runner.py --health` (DB + tables + sequences + ANTHROPIC_API_KEY + OAuth + briefings/ + syncers) — Task 9 ✓
- `check_table_inventory` uses `_TABLE_NAMES` from `database.schema`, not a hardcoded list — Task 6 ✓
- `check_sequences` discovers sequences via `information_schema.sequences` — Task 4 ✓
- `check_oauth_tokens` queries `oauth_tokens` table, not file system — Task 5 ✓
- WARN exits 0; FAIL exits 1; SKIP is neutral — Tasks 1, 6–9 ✓
- `--health` short-circuits before any normal operation — Tasks 7–9 ✓

**Type consistency:** `check_connection()` returns `tuple[HealthCheck, object]` throughout. All runner `_run_health_check()` functions use the same destructuring `conn_check, conn = check_connection()` pattern. All `_TABLES` lists are `list[str]`.

**Connection pattern:** `check_connection()` returns the open connection so callers don't open two connections. Callers wrap subsequent DB checks in `try/finally: conn.close()`.
