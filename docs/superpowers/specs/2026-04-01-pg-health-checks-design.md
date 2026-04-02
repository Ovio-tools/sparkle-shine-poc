# Design: PostgreSQL Health Checks + Runner `--health` Flags

**Date:** 2026-04-01
**Status:** Approved

---

## Goal

Add a standalone PostgreSQL diagnostics script (`scripts/pg_health_check.py`) and a `--health` flag to the three service runners (`simulation/engine.py`, `automations/runner.py`, `intelligence/runner.py`).

Each health check answers the question: *"Can this specific service do its job right now?"* — not just "is the database up", and not "is every external API reachable."

Works identically locally and on Railway (no file-system assumptions for credentials).

---

## Architecture

### Approach: Shared `database/health.py` module

Common primitives live in `database/health.py`. All four callers import from it. No duplication of the renderer or the connection/table/sequence check logic.

**New files:**
- `database/health.py` — shared primitives
- `scripts/pg_health_check.py` — standalone CLI entry point

**Modified files:**
- `simulation/engine.py` — add `--health` flag
- `automations/runner.py` — add `--health` flag
- `intelligence/runner.py` — add `--health` flag

---

## `database/health.py`

### `HealthCheck` dataclass

```python
@dataclass
class HealthCheck:
    name: str
    status: str    # "PASS" | "WARN" | "FAIL" | "SKIP"
    message: str
```

### Check functions

**`check_connection() -> HealthCheck`**
Opens a connection via `DATABASE_URL`, runs `SELECT 1`. Returns PASS or FAIL with the error message on failure. All subsequent DB-dependent checks should be skipped if this returns FAIL.

**`check_table_inventory(conn, tables: list[str]) -> list[HealthCheck]`**
Calls `table_exists(conn, name)` (from `database.connection`) for each name in `tables`. FAIL for any missing table. Callers pass their own list; `scripts/pg_health_check.py` passes `_TABLE_NAMES` imported from `database.schema` — no hardcoded count or list.

**`check_sequences(conn, table_names: list[str]) -> list[HealthCheck]`**
Uses `information_schema.sequences` to discover which sequences exist for the given table names before querying them — avoids hardcoding sequence names and gracefully handles tables without a serial PK. For each discovered sequence, compares `SELECT last_value FROM {seq}` against `SELECT MAX(id) FROM {table}`. If sequence is behind max(id), the next INSERT will fail with a unique-constraint error → FAIL. If table has no rows → PASS (nothing to drift from).

**`check_oauth_tokens(conn) -> list[HealthCheck]`**
Queries `oauth_tokens` table for rows where `tool_name IN ('jobber', 'quickbooks', 'google')`.
- FAIL if a row is missing entirely (no token stored for that tool).
- WARN if `updated_at` is stale (> 7 days — suggests the token hasn't refreshed).
- WARN if `token_data->>'expires_at'` parses to a timestamp in the past (access token expired; may auto-refresh).
- PASS otherwise.

No file-system reads. Works identically locally and on Railway.

### `render_table(title: str, checks: list[HealthCheck]) -> None`

Prints a bordered results table to stdout using `print()` (not logger — health output must be clean stdout, not mixed with log timestamps).

Markers: `✓ PASS`, `!  WARN`, `✗ FAIL`, `-- SKIP`

Example output:
```
Sparkle & Shine — PostgreSQL Health Check
==========================================
  ✓ PASS  DATABASE_URL set
  ✓ PASS  DB connection
  ✓ PASS  Table: clients
  ✗ FAIL  Table: oauth_tokens  (missing — run migrations)
  !  WARN  Sequence: jobs_id_seq  (behind: last=8180, max=8201)
  ✓ PASS  OAuth token: jobber
  !  WARN  OAuth token: quickbooks  (expires_at in the past)
==========================================
  Result: FAIL (2 issue(s))
```

### Exit logic

Used by all callers:
```python
exit_code = 1 if any(c.status == "FAIL" for c in checks) else 0
sys.exit(exit_code)
```

WARN alone does not cause a non-zero exit. SKIP is neutral.

---

## `scripts/pg_health_check.py`

Standalone CLI. Comprehensive diagnostic across the full database — not scoped to any one service.

```
python scripts/pg_health_check.py
```

### Checks (in order)

| # | Check | Function |
|---|-------|----------|
| 1 | `DATABASE_URL` set in environment | inline |
| 2 | DB connection (`SELECT 1`) | `check_connection()` |
| 3 | Full table inventory — all tables in `_TABLE_NAMES` from `database.schema` | `check_table_inventory()` |
| 4 | Sequence health — all tables with a serial PK | `check_sequences()` |
| 5 | OAuth token rows for jobber, quickbooks, google | `check_oauth_tokens()` |

If check 2 fails, checks 3–5 are marked SKIP with note "DB unreachable".

Exit 0 if all PASS or WARN. Exit 1 if any FAIL.

---

## Runner `--health` Flags

In each runner, `--health` is checked before any other logic — before DB connections, migrations, or pipeline stages. It short-circuits and exits after printing results.

---

### `simulation/engine.py --health`

*"Can the engine generate events right now?"*

| # | Check | Status on failure |
|---|-------|-------------------|
| 1 | DB connection | FAIL |
| 2 | Tables: `clients`, `jobs`, `invoices`, `payments`, `cross_tool_mapping` | FAIL if any missing |
| 3 | Sequence health for those 5 tables | FAIL if any behind |
| 4 | Checkpoint freshness: if `simulation/checkpoint.json` exists, parse its `date` field. >1 day behind today → WARN (engine may have stopped). Missing file → PASS (first run). | WARN |
| 5 | Generator imports: try importing all 6 generator classes. Each `ImportError` → WARN with the module name. | WARN per failure |

---

### `automations/runner.py --health`

*"Can the automation runner process triggers right now?"*

| # | Check | Status on failure |
|---|-------|-------------------|
| 1 | DB connection | FAIL |
| 2 | Tables: `pending_actions`, `poll_state` | FAIL if any missing |
| 3 | Sequence health for `pending_actions` and `automation_log` | FAIL if either behind |
| 4 | Sentinel age: `.lead_leak_last_run` >48h old → WARN; `.overdue_invoice_last_run` >14 days old → WARN. Missing sentinel files → PASS (first run). | WARN |
| 5 | Automation module imports: try importing all 6 automation classes. Each `ImportError` → WARN. | WARN per failure |

---

### `intelligence/runner.py --health`

*"Can the intelligence pipeline produce a briefing right now?"*

| # | Check | Status on failure |
|---|-------|-------------------|
| 1 | DB connection | FAIL |
| 2 | Tables: `daily_metrics_snapshot`, `document_index`, `jobs`, `clients`, `invoices` | FAIL if any missing |
| 3 | Sequence health for those 5 tables | FAIL if any behind |
| 4 | `ANTHROPIC_API_KEY` set in environment | FAIL |
| 5 | OAuth tokens: jobber, quickbooks, google (via `check_oauth_tokens()`) | WARN on expiry |
| 6 | `briefings/` directory exists | WARN if not |
| 7 | Syncer module imports: try importing all 8 syncer classes. Each `ImportError` → WARN. | WARN per failure |

---

## Conventions Followed

- **Import paths:** `from database.connection import get_connection`, `from database.health import ...` — consistent with project conventions.
- **Logging:** `render_table()` uses `print()` only. No log timestamps in health output. Logger used only for debug-level internal details.
- **Dry-run:** `--health` does not interact with `--dry-run`. Health checks are always read-only.
- **Error isolation:** Each check is wrapped independently. One failing check never prevents the others from running.
- **No hardcoded table lists:** Table inventory uses `_TABLE_NAMES` from `database.schema`. Sequence discovery uses `information_schema.sequences`.
- **Import pattern for generator/syncer/automation checks:** `try/except ImportError` — same pattern as `simulation/engine.py`'s existing `_register_generators()`.

---

## Files Changed Summary

| File | Change |
|------|--------|
| `database/health.py` | **New** — `HealthCheck`, 4 check functions, `render_table` |
| `scripts/pg_health_check.py` | **New** — standalone CLI diagnostic |
| `simulation/engine.py` | **Modified** — add `--health` to argparse + `_run_health_check()` |
| `automations/runner.py` | **Modified** — add `--health` to argparse + `_run_health_check()` |
| `intelligence/runner.py` | **Modified** — add `--health` to argparse + `_run_health_check()` |
