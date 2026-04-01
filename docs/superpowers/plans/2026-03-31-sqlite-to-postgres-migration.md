# SQLite → PostgreSQL Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace all SQLite-specific constructs in `database/` and `automations/` with PostgreSQL-compatible equivalents.

**Architecture:** Seven mechanical patterns (P1–P7) applied in-place across 15 files. No new files created. `database.connection.Connection` already wraps psycopg2 with `RealDictCursor` (dict-like row access), so once `?` placeholders, `datetime('now')`, `hasattr` fallbacks, and `sqlite3` imports are removed, all calls work transparently. `seeding/`, `demo/`, `setup/`, `scripts/`, `tests/`, and root-level `create_*.py` are untouched.

**Tech Stack:** Python 3, psycopg2, `database.connection.get_connection`, PostgreSQL

### Pattern Reference

| ID | Old (SQLite) | New (PostgreSQL) |
|----|-------------|-----------------|
| P1 | `?` in SQL string | `%s` |
| P2 | `datetime('now')` in SQL | `CURRENT_TIMESTAMP` |
| P3 | `INSERT OR IGNORE INTO ...` | `INSERT INTO ... ON CONFLICT DO NOTHING` |
| P4 | `sqlite3.connect()` | `get_connection()` from `database.connection` |
| P5 | `row[0]` / `fetchone()[0]` | `row["col_name"]` with alias if needed |
| P6 | `hasattr(row, "keys")` fallback | direct dict access: `row["col_name"]` |
| P7 | `sqlite3.Connection` type hint | remove annotation |

**Additional removals per file:**
- `import sqlite3` when no longer needed
- `PRAGMA foreign_keys = ON` lines
- `row_factory = sqlite3.Row` lines
- `AUTOINCREMENT` → `SERIAL` in DDL

---

### Task 1: database/mappings.py — P1, P2, P6 + remove import sqlite3

**Files:**
- Modify: `database/mappings.py`

- [ ] **Step 1: Pre-flight — confirm patterns exist**

```bash
cd sparkle-shine-poc
grep -n "import sqlite3\|datetime('now')\| = ?\|, ?\|hasattr(row" database/mappings.py
```

Expected: at least 10 matches across `import sqlite3` (line 1), `datetime('now')` (lines 89, 93, 241, 244), `?` placeholders (lines 73–75, 89, 110, 129, 148, 165, 195, 210, 241), and `hasattr(row` (line 77).

- [ ] **Step 2: Remove `import sqlite3`**

In `database/mappings.py`, delete line 1:

```python
# DELETE this line:
import sqlite3
```

- [ ] **Step 3: Fix `register_mapping` — collision guard (P1) and hasattr fallback (P6)**

Find this block (around lines 71–83):

```python
    existing = conn.execute(
        "SELECT canonical_id FROM cross_tool_mapping "
        "WHERE tool_name = ? AND tool_specific_id = ?",
        (tool_name, tool_specific_id),
    ).fetchone()
    if existing is not None:
        existing_cid = existing["canonical_id"] if hasattr(existing, "keys") else existing[0]
```

Replace with:

```python
    existing = conn.execute(
        "SELECT canonical_id FROM cross_tool_mapping "
        "WHERE tool_name = %s AND tool_specific_id = %s",
        (tool_name, tool_specific_id),
    ).fetchone()
    if existing is not None:
        existing_cid = existing["canonical_id"]
```

- [ ] **Step 4: Fix `register_mapping` — INSERT SQL (P1, P2)**

Find this block (around lines 85–96):

```python
        conn.execute(
            """
            INSERT INTO cross_tool_mapping
                (canonical_id, entity_type, tool_name, tool_specific_id, tool_specific_url, synced_at)
            VALUES (?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(canonical_id, tool_name) DO UPDATE SET
                tool_specific_id  = excluded.tool_specific_id,
                tool_specific_url = excluded.tool_specific_url,
                synced_at         = datetime('now')
            """,
            (canonical_id, entity_type, tool_name, tool_specific_id, tool_specific_url),
        )
```

Replace with:

```python
        conn.execute(
            """
            INSERT INTO cross_tool_mapping
                (canonical_id, entity_type, tool_name, tool_specific_id, tool_specific_url, synced_at)
            VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT(canonical_id, tool_name) DO UPDATE SET
                tool_specific_id  = excluded.tool_specific_id,
                tool_specific_url = excluded.tool_specific_url,
                synced_at         = CURRENT_TIMESTAMP
            """,
            (canonical_id, entity_type, tool_name, tool_specific_id, tool_specific_url),
        )
```

- [ ] **Step 5: Fix `get_tool_id` — SELECT (P1)**

Find (around line 108–112):

```python
            "SELECT tool_specific_id FROM cross_tool_mapping "
            "WHERE canonical_id = ? AND tool_name = ?",
            (canonical_id, tool_name),
```

Replace with:

```python
            "SELECT tool_specific_id FROM cross_tool_mapping "
            "WHERE canonical_id = %s AND tool_name = %s",
            (canonical_id, tool_name),
```

- [ ] **Step 6: Fix `get_tool_url` — SELECT (P1)**

Find (around line 127–131):

```python
            "SELECT tool_specific_url FROM cross_tool_mapping "
            "WHERE canonical_id = ? AND tool_name = ?",
            (canonical_id, tool_name),
```

Replace with:

```python
            "SELECT tool_specific_url FROM cross_tool_mapping "
            "WHERE canonical_id = %s AND tool_name = %s",
            (canonical_id, tool_name),
```

- [ ] **Step 7: Fix `get_canonical_id` — SELECT (P1)**

Find (around line 146–150):

```python
            "SELECT canonical_id FROM cross_tool_mapping "
            "WHERE tool_name = ? AND tool_specific_id = ?",
            (tool_name, tool_specific_id),
```

Replace with:

```python
            "SELECT canonical_id FROM cross_tool_mapping "
            "WHERE tool_name = %s AND tool_specific_id = %s",
            (tool_name, tool_specific_id),
```

- [ ] **Step 8: Fix `get_all_mappings` — SELECT (P1)**

Find (around line 163–165):

```python
            "SELECT tool_name, tool_specific_id FROM cross_tool_mapping "
            "WHERE canonical_id = ?",
            (canonical_id,),
```

Replace with:

```python
            "SELECT tool_name, tool_specific_id FROM cross_tool_mapping "
            "WHERE canonical_id = %s",
            (canonical_id,),
```

- [ ] **Step 9: Fix `find_unmapped` — SELECT (P1)**

Find (around line 187–196):

```python
            f"""
            SELECT e.id FROM {table} e
            WHERE e.id NOT IN (
                SELECT canonical_id FROM cross_tool_mapping
                WHERE tool_name = ?
            )
            """,
            (tool_name,),
```

Replace with:

```python
            f"""
            SELECT e.id FROM {table} e
            WHERE e.id NOT IN (
                SELECT canonical_id FROM cross_tool_mapping
                WHERE tool_name = %s
            )
            """,
            (tool_name,),
```

- [ ] **Step 10: Fix `list_mapped_tools` — SELECT (P1)**

Find (around line 208–210):

```python
            "SELECT tool_name FROM cross_tool_mapping WHERE canonical_id = ?",
            (canonical_id,),
```

Replace with:

```python
            "SELECT tool_name FROM cross_tool_mapping WHERE canonical_id = %s",
            (canonical_id,),
```

- [ ] **Step 11: Fix `bulk_register` — INSERT SQL (P1, P2)**

Find (around lines 237–247):

```python
        conn.executemany(
            """
            INSERT INTO cross_tool_mapping
                (canonical_id, entity_type, tool_name, tool_specific_id, tool_specific_url, synced_at)
            VALUES (?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(canonical_id, tool_name) DO UPDATE SET
                tool_specific_id = excluded.tool_specific_id,
                synced_at        = datetime('now')
            """,
            rows,
        )
```

Replace with:

```python
        conn.executemany(
            """
            INSERT INTO cross_tool_mapping
                (canonical_id, entity_type, tool_name, tool_specific_id, tool_specific_url, synced_at)
            VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT(canonical_id, tool_name) DO UPDATE SET
                tool_specific_id = excluded.tool_specific_id,
                synced_at        = CURRENT_TIMESTAMP
            """,
            rows,
        )
```

- [ ] **Step 12: Verify — no SQLite patterns remain**

```bash
grep -n "import sqlite3\|datetime('now')\|hasattr(row\| = ?\b\|, ?)" database/mappings.py
```

Expected: 0 matches.

- [ ] **Step 13: Syntax check**

```bash
python -m py_compile database/mappings.py && echo "OK"
```

Expected: `OK`

- [ ] **Step 14: Commit**

```bash
git add database/mappings.py
git commit -m "migrate(mappings): SQLite → PostgreSQL — P1 %s, P2 CURRENT_TIMESTAMP, P6 remove hasattr"
```

---

### Task 2: automations/state.py — P1, P2, P6, P7 + remove import sqlite3

**Files:**
- Modify: `automations/state.py`

- [ ] **Step 1: Pre-flight**

```bash
grep -n "import sqlite3\|sqlite3\.Connection\|datetime('now')\| = ?\|, ?)\|hasattr(row" automations/state.py
```

Expected: `import sqlite3` (line 8), `sqlite3.Connection` (lines 13, 42), `? ` (lines 26–27, 57), `datetime('now')` (lines 57, 61), `hasattr(row` (line 34).

- [ ] **Step 2: Remove `import sqlite3` and fix type hints (P7)**

Delete line 8 (`import sqlite3`) entirely.

Change the function signatures. Find:

```python
def get_last_poll(
    db: sqlite3.Connection,
    tool_name: str,
    entity_type: str,
) -> Optional[dict]:
```

Replace with:

```python
def get_last_poll(
    db,
    tool_name: str,
    entity_type: str,
) -> Optional[dict]:
```

Find:

```python
def update_last_poll(
    db: sqlite3.Connection,
    tool_name: str,
    entity_type: str,
    last_id: Optional[str],
    last_timestamp: Optional[str],
) -> None:
```

Replace with:

```python
def update_last_poll(
    db,
    tool_name: str,
    entity_type: str,
    last_id: Optional[str],
    last_timestamp: Optional[str],
) -> None:
```

- [ ] **Step 3: Fix `get_last_poll` — SELECT placeholder (P1) and remove hasattr fallback (P6)**

Find this block (around lines 21–38):

```python
    cursor = db.execute(
        """
        SELECT tool_name, entity_type, last_processed_id,
               last_processed_timestamp, last_poll_at
        FROM poll_state
        WHERE tool_name = ? AND entity_type = ?
        """,
        (tool_name, entity_type),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    # Support both sqlite3.Row and plain tuple
    if hasattr(row, "keys"):
        return dict(row)
    keys = ["tool_name", "entity_type", "last_processed_id",
            "last_processed_timestamp", "last_poll_at"]
    return dict(zip(keys, row))
```

Replace with:

```python
    cursor = db.execute(
        """
        SELECT tool_name, entity_type, last_processed_id,
               last_processed_timestamp, last_poll_at
        FROM poll_state
        WHERE tool_name = %s AND entity_type = %s
        """,
        (tool_name, entity_type),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return dict(row)
```

- [ ] **Step 4: Fix `update_last_poll` — INSERT placeholders and datetime (P1, P2)**

Find this block (around lines 51–64):

```python
        db.execute(
            """
            INSERT INTO poll_state
                (tool_name, entity_type, last_processed_id,
                 last_processed_timestamp, last_poll_at)
            VALUES (?, ?, ?, ?, datetime('now'))
            ON CONFLICT(tool_name, entity_type) DO UPDATE SET
                last_processed_id        = excluded.last_processed_id,
                last_processed_timestamp = excluded.last_processed_timestamp,
                last_poll_at             = datetime('now')
            """,
            (tool_name, entity_type, last_id, last_timestamp),
        )
```

Replace with:

```python
        db.execute(
            """
            INSERT INTO poll_state
                (tool_name, entity_type, last_processed_id,
                 last_processed_timestamp, last_poll_at)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT(tool_name, entity_type) DO UPDATE SET
                last_processed_id        = excluded.last_processed_id,
                last_processed_timestamp = excluded.last_processed_timestamp,
                last_poll_at             = CURRENT_TIMESTAMP
            """,
            (tool_name, entity_type, last_id, last_timestamp),
        )
```

- [ ] **Step 5: Verify**

```bash
grep -n "import sqlite3\|sqlite3\.\|datetime('now')\|hasattr(row\| = ?\b\|, ?)" automations/state.py
```

Expected: 0 matches.

- [ ] **Step 6: Syntax check**

```bash
python -m py_compile automations/state.py && echo "OK"
```

Expected: `OK`

- [ ] **Step 7: Commit**

```bash
git add automations/state.py
git commit -m "migrate(state): SQLite → PostgreSQL — P1 %s, P2 CURRENT_TIMESTAMP, P6 remove hasattr, P7 remove type hints"
```

---

### Task 3: automations/migrate.py — P2, P4 + SERIAL + remove PRAGMA + remove import sqlite3

**Files:**
- Modify: `automations/migrate.py`

- [ ] **Step 1: Pre-flight**

```bash
grep -n "import sqlite3\|sqlite3\.connect\|PRAGMA\|AUTOINCREMENT\|datetime('now')" automations/migrate.py
```

Expected: `import sqlite3` (line 11), `sqlite3.connect` (line 78), `PRAGMA foreign_keys` (line 79), `AUTOINCREMENT` (lines 39, 57), `datetime('now')` (lines 48, 64).

- [ ] **Step 2: Remove `import sqlite3` and add `get_connection` import**

Delete line 11 (`import sqlite3`).

Add the following import after `import os` and `import sys` (around line 10):

```python
from database.connection import get_connection
```

- [ ] **Step 3: Remove the `DB_PATH` constant and fix `run_migration` (P4)**

Delete this line (around line 17):

```python
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "sparkle_shine.db")
```

Find the `run_migration` function (around lines 77–84):

```python
def run_migration(db_path: str = DB_PATH) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    with conn:
        for stmt in _MIGRATIONS:
            conn.execute(stmt)
    conn.close()
    print(f"Migration complete: {os.path.abspath(db_path)}")
    print("Tables ensured: poll_state, automation_log, pending_actions")
```

Replace with:

```python
def run_migration(db_path: str = None) -> None:
    conn = get_connection()
    with conn:
        for stmt in _MIGRATIONS:
            conn.execute(stmt)
    conn.close()
    print("Migration complete.")
    print("Tables ensured: poll_state, automation_log, pending_actions")
```

- [ ] **Step 4: Fix `automation_log` DDL — AUTOINCREMENT → SERIAL, datetime('now') → CURRENT_TIMESTAMP (P2)**

Find the `automation_log` DDL string (around lines 37–50):

```python
    """
    CREATE TABLE IF NOT EXISTS automation_log (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id          TEXT NOT NULL,
        automation_name TEXT NOT NULL,
        trigger_source  TEXT,
        trigger_detail  TEXT,
        action_name     TEXT NOT NULL,
        action_target   TEXT,
        status          TEXT NOT NULL CHECK(status IN ('success','failed','skipped')),
        error_message   TEXT,
        created_at      TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """,
```

Replace with:

```python
    """
    CREATE TABLE IF NOT EXISTS automation_log (
        id              SERIAL PRIMARY KEY,
        run_id          TEXT NOT NULL,
        automation_name TEXT NOT NULL,
        trigger_source  TEXT,
        trigger_detail  TEXT,
        action_name     TEXT NOT NULL,
        action_target   TEXT,
        status          TEXT NOT NULL CHECK(status IN ('success','failed','skipped')),
        error_message   TEXT,
        created_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
```

- [ ] **Step 5: Fix `pending_actions` DDL — AUTOINCREMENT → SERIAL, datetime('now') → CURRENT_TIMESTAMP (P2)**

Find the `pending_actions` DDL string (around lines 55–67):

```python
    """
    CREATE TABLE IF NOT EXISTS pending_actions (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        automation_name     TEXT NOT NULL,
        action_name         TEXT NOT NULL,
        trigger_context     TEXT NOT NULL,
        execute_after       TEXT NOT NULL,
        status              TEXT NOT NULL DEFAULT 'pending'
                                CHECK(status IN ('pending','executed','failed')),
        created_at          TEXT NOT NULL DEFAULT (datetime('now')),
        executed_at         TEXT
    )
    """,
```

Replace with:

```python
    """
    CREATE TABLE IF NOT EXISTS pending_actions (
        id                  SERIAL PRIMARY KEY,
        automation_name     TEXT NOT NULL,
        action_name         TEXT NOT NULL,
        trigger_context     TEXT NOT NULL,
        execute_after       TEXT NOT NULL,
        status              TEXT NOT NULL DEFAULT 'pending'
                                CHECK(status IN ('pending','executed','failed')),
        created_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        executed_at         TEXT
    )
    """,
```

- [ ] **Step 6: Verify**

```bash
grep -n "import sqlite3\|sqlite3\.\|PRAGMA\|AUTOINCREMENT\|datetime('now')" automations/migrate.py
```

Expected: 0 matches.

- [ ] **Step 7: Syntax check**

```bash
python -m py_compile automations/migrate.py && echo "OK"
```

Expected: `OK`

- [ ] **Step 8: Commit**

```bash
git add automations/migrate.py
git commit -m "migrate(migrate): SQLite → PostgreSQL — P4 get_connection, P2 CURRENT_TIMESTAMP, SERIAL, remove PRAGMA"
```

---

### Task 4: automations/runner.py — delete row_factory line + fix ? placeholder

**Files:**
- Modify: `automations/runner.py`

- [ ] **Step 1: Pre-flight**

```bash
grep -n "row_factory\|sqlite3\|= ?\b\|, ?)" automations/runner.py
```

Expected: `row_factory` at line ~473 and `?` at line ~308.

- [ ] **Step 2: Delete `db.row_factory` line**

Find this line (around line 473):

```python
    db.row_factory = __import__("sqlite3").Row
```

Delete it entirely.

- [ ] **Step 3: Fix `pending_actions` UPDATE placeholder (P1)**

Find this line (around line 308):

```python
                    "UPDATE pending_actions SET status='executed', executed_at=? WHERE id=?",
```

Replace with:

```python
                    "UPDATE pending_actions SET status='executed', executed_at=%s WHERE id=%s",
```

- [ ] **Step 4: Verify**

```bash
grep -n "row_factory\|import sqlite3\| = ?\b\|, ?)" automations/runner.py
```

Expected: 0 matches.

- [ ] **Step 5: Syntax check**

```bash
python -m py_compile automations/runner.py && echo "OK"
```

Expected: `OK`

- [ ] **Step 6: Commit**

```bash
git add automations/runner.py
git commit -m "migrate(runner): remove sqlite3 row_factory, fix ? placeholder in pending_actions UPDATE"
```

---

### Task 5: automations/base.py — P7 + remove import sqlite3

**Files:**
- Modify: `automations/base.py`

- [ ] **Step 1: Pre-flight**

```bash
grep -n "import sqlite3\|sqlite3\." automations/base.py
```

Expected: `import sqlite3` (line 7), `sqlite3.Connection` (line 28).

- [ ] **Step 2: Remove `import sqlite3`**

Delete line 7 (`import sqlite3`).

- [ ] **Step 3: Remove `sqlite3.Connection` type hint (P7)**

Find (around line 28):

```python
    def __init__(self, clients: Any, db: sqlite3.Connection, dry_run: bool = False):
```

Replace with:

```python
    def __init__(self, clients: Any, db, dry_run: bool = False):
```

- [ ] **Step 4: Verify**

```bash
grep -n "sqlite3" automations/base.py
```

Expected: 0 matches.

- [ ] **Step 5: Syntax check**

```bash
python -m py_compile automations/base.py && echo "OK"
```

Expected: `OK`

- [ ] **Step 6: Commit**

```bash
git add automations/base.py
git commit -m "migrate(base): remove sqlite3 import and Connection type hint"
```

---

### Task 6: automations/triggers.py — P7 + remove import sqlite3

**Files:**
- Modify: `automations/triggers.py`

- [ ] **Step 1: Pre-flight**

```bash
grep -n "import sqlite3\|sqlite3\." automations/triggers.py
```

Expected: `import sqlite3` (line 13), `sqlite3.Connection` on lines for all 4 poll functions.

- [ ] **Step 2: Remove `import sqlite3`**

Delete line 13 (`import sqlite3`).

- [ ] **Step 3: Remove `sqlite3.Connection` type hints from all 4 poll functions (P7)**

Find (around line 56):

```python
def poll_pipedrive_won_deals(clients: Any, db: sqlite3.Connection) -> list:
```

Replace with:

```python
def poll_pipedrive_won_deals(clients: Any, db) -> list:
```

Find (around line 185):

```python
def poll_jobber_completed_jobs(clients: Any, db: sqlite3.Connection) -> list:
```

Replace with:

```python
def poll_jobber_completed_jobs(clients: Any, db) -> list:
```

Find (around line 274):

```python
def poll_quickbooks_payments(clients: Any, db: sqlite3.Connection) -> list:
```

Replace with:

```python
def poll_quickbooks_payments(clients: Any, db) -> list:
```

Find (around line 379):

```python
def poll_sheets_negative_reviews(clients: Any, db: sqlite3.Connection) -> list:
```

Replace with:

```python
def poll_sheets_negative_reviews(clients: Any, db) -> list:
```

- [ ] **Step 4: Verify**

```bash
grep -n "sqlite3" automations/triggers.py
```

Expected: 0 matches.

- [ ] **Step 5: Syntax check**

```bash
python -m py_compile automations/triggers.py && echo "OK"
```

Expected: `OK`

- [ ] **Step 6: Commit**

```bash
git add automations/triggers.py
git commit -m "migrate(triggers): remove sqlite3 import and Connection type hints from all 4 poll functions"
```

---

### Task 7: automations/utils/id_resolver.py — P1, P2, P5, P6, P7 + remove import sqlite3

**Files:**
- Modify: `automations/utils/id_resolver.py`

- [ ] **Step 1: Pre-flight**

```bash
grep -n "import sqlite3\|sqlite3\.\|datetime('now')\| = ?\|, ?)\|row\[0\]\|hasattr(row" automations/utils/id_resolver.py
```

Expected: matches on `import sqlite3`, `sqlite3.Connection` (lines 14, 34, 56), `?` (lines 22, 43, 71), `datetime('now')` (lines 100, 103), `row[0]` (lines 30, 52), `hasattr(row` (lines 30, 52, 75).

- [ ] **Step 2: Remove `import sqlite3`**

Delete line 6 (`import sqlite3`).

- [ ] **Step 3: Remove `sqlite3.Connection` type hints (P7)**

Find (around line 14):

```python
def resolve(db: sqlite3.Connection, canonical_id: str, target_tool: str) -> str:
```

Replace with:

```python
def resolve(db, canonical_id: str, target_tool: str) -> str:
```

Find (around line 33–34):

```python
def reverse_resolve(
    db: sqlite3.Connection, tool_specific_id: str, source_tool: str
) -> str:
```

Replace with:

```python
def reverse_resolve(
    db, tool_specific_id: str, source_tool: str
) -> str:
```

Find (around line 55–56):

```python
def register_mapping(
    db: sqlite3.Connection,
```

Replace with:

```python
def register_mapping(
    db,
```

- [ ] **Step 4: Fix `resolve` — placeholder and row access (P1, P5, P6)**

Find this block (around lines 20–30):

```python
    cursor = db.execute(
        "SELECT tool_specific_id FROM cross_tool_mapping "
        "WHERE canonical_id = ? AND tool_name = ?",
        (canonical_id, target_tool),
    )
    row = cursor.fetchone()
    if row is None:
        raise MappingNotFoundError(
            f"No mapping for canonical_id='{canonical_id}' in tool='{target_tool}'"
        )
    return row[0] if not hasattr(row, "keys") else row["tool_specific_id"]
```

Replace with:

```python
    cursor = db.execute(
        "SELECT tool_specific_id FROM cross_tool_mapping "
        "WHERE canonical_id = %s AND tool_name = %s",
        (canonical_id, target_tool),
    )
    row = cursor.fetchone()
    if row is None:
        raise MappingNotFoundError(
            f"No mapping for canonical_id='{canonical_id}' in tool='{target_tool}'"
        )
    return row["tool_specific_id"]
```

- [ ] **Step 5: Fix `reverse_resolve` — placeholder and row access (P1, P5, P6)**

Find this block (around lines 41–52):

```python
    cursor = db.execute(
        "SELECT canonical_id FROM cross_tool_mapping "
        "WHERE tool_specific_id = ? AND tool_name = ?",
        (tool_specific_id, source_tool),
    )
    row = cursor.fetchone()
    if row is None:
        raise MappingNotFoundError(
            f"No mapping for tool_specific_id='{tool_specific_id}' "
            f"in tool='{source_tool}'"
        )
    return row[0] if not hasattr(row, "keys") else row["canonical_id"]
```

Replace with:

```python
    cursor = db.execute(
        "SELECT canonical_id FROM cross_tool_mapping "
        "WHERE tool_specific_id = %s AND tool_name = %s",
        (tool_specific_id, source_tool),
    )
    row = cursor.fetchone()
    if row is None:
        raise MappingNotFoundError(
            f"No mapping for tool_specific_id='{tool_specific_id}' "
            f"in tool='{source_tool}'"
        )
    return row["canonical_id"]
```

- [ ] **Step 6: Fix `register_mapping` — collision guard (P1, P6)**

Find this block (around lines 69–80):

```python
    existing = db.execute(
        "SELECT canonical_id FROM cross_tool_mapping "
        "WHERE tool_name = ? AND tool_specific_id = ?",
        (tool_name, tool_specific_id),
    ).fetchone()
    if existing is not None:
        existing_cid = existing["canonical_id"] if hasattr(existing, "keys") else existing[0]
```

Replace with:

```python
    existing = db.execute(
        "SELECT canonical_id FROM cross_tool_mapping "
        "WHERE tool_name = %s AND tool_specific_id = %s",
        (tool_name, tool_specific_id),
    ).fetchone()
    if existing is not None:
        existing_cid = existing["canonical_id"]
```

- [ ] **Step 7: Fix `register_mapping` — INSERT SQL (P1, P2)**

Find this block (around lines 95–106):

```python
    with db:
        db.execute(
            """
            INSERT INTO cross_tool_mapping
                (canonical_id, entity_type, tool_name, tool_specific_id, synced_at)
            VALUES (?, ?, ?, ?, datetime('now'))
            ON CONFLICT(canonical_id, tool_name) DO UPDATE SET
                tool_specific_id = excluded.tool_specific_id,
                synced_at        = datetime('now')
            """,
            (canonical_id, entity_type, tool_name, tool_specific_id),
        )
```

Replace with:

```python
    with db:
        db.execute(
            """
            INSERT INTO cross_tool_mapping
                (canonical_id, entity_type, tool_name, tool_specific_id, synced_at)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT(canonical_id, tool_name) DO UPDATE SET
                tool_specific_id = excluded.tool_specific_id,
                synced_at        = CURRENT_TIMESTAMP
            """,
            (canonical_id, entity_type, tool_name, tool_specific_id),
        )
```

- [ ] **Step 8: Verify**

```bash
grep -n "import sqlite3\|sqlite3\.\|datetime('now')\| = ?\b\|, ?)\|row\[0\]\|hasattr(row" automations/utils/id_resolver.py
```

Expected: 0 matches.

- [ ] **Step 9: Syntax check**

```bash
python -m py_compile automations/utils/id_resolver.py && echo "OK"
```

Expected: `OK`

- [ ] **Step 10: Commit**

```bash
git add automations/utils/id_resolver.py
git commit -m "migrate(id_resolver): P1 %s, P2 CURRENT_TIMESTAMP, P5 named cols, P6 remove hasattr, P7 remove type hints"
```

---

### Task 8: automations/new_client_onboarding.py — P1, P2, P3, P6

**Files:**
- Modify: `automations/new_client_onboarding.py`

- [ ] **Step 1: Pre-flight**

```bash
grep -n "INSERT OR IGNORE\|datetime('now')\| = ?\|, ?)\|hasattr(row\|row\[0\]" automations/new_client_onboarding.py
```

Expected: `INSERT OR IGNORE` (lines ~417, ~487), `datetime('now')` (line ~499), `?` (lines ~374, ~419, ~489, ~500, ~834), `hasattr(row` (lines ~377, ~401, ~403, ~477, ~479, ~838).

- [ ] **Step 2: Fix email lookup SELECT (P1)**

Find (around line 373–375):

```python
            row = self.db.execute(
                "SELECT id FROM clients WHERE email = ?", (email,)
            ).fetchone()
```

Replace with:

```python
            row = self.db.execute(
                "SELECT id FROM clients WHERE email = %s", (email,)
            ).fetchone()
```

- [ ] **Step 3: Fix row access after email lookup (P6)**

Find (around line 377):

```python
                cid = row["id"] if hasattr(row, "keys") else row[0]
```

Replace with:

```python
                cid = row["id"]
```

- [ ] **Step 4: Fix `row_c`/`row_m` hasattr fallbacks in `_mint_or_find_canonical_id` (P6)**

Find (around lines 400–403):

```python
        if row_c:
            candidates.append(row_c["id"] if hasattr(row_c, "keys") else row_c[0])
        if row_m:
            candidates.append(row_m["canonical_id"] if hasattr(row_m, "keys") else row_m[0])
```

Replace with:

```python
        if row_c:
            candidates.append(row_c["id"])
        if row_m:
            candidates.append(row_m["canonical_id"])
```

- [ ] **Step 5: Fix first `INSERT OR IGNORE INTO clients` (P1, P3)**

Find (around lines 414–422):

```python
                self.db.execute(
                    """
                    INSERT OR IGNORE INTO clients
                        (id, client_type, first_name, last_name, email, status)
                    VALUES (?, ?, ?, ?, ?, 'active')
                    """,
                    (canonical_id, client_type, first_name, last_name, email),
                )
```

Replace with:

```python
                self.db.execute(
                    """
                    INSERT INTO clients
                        (id, client_type, first_name, last_name, email, status)
                    VALUES (%s, %s, %s, %s, %s, 'active')
                    ON CONFLICT DO NOTHING
                    """,
                    (canonical_id, client_type, first_name, last_name, email),
                )
```

- [ ] **Step 6: Fix `row_c`/`row_m` hasattr fallbacks in `_promote_lead_to_client` (P6)**

Find (around lines 476–479):

```python
        if row_c:
            candidates.append(row_c["id"] if hasattr(row_c, "keys") else row_c[0])
        if row_m:
            candidates.append(row_m["canonical_id"] if hasattr(row_m, "keys") else row_m[0])
```

Replace with:

```python
        if row_c:
            candidates.append(row_c["id"])
        if row_m:
            candidates.append(row_m["canonical_id"])
```

- [ ] **Step 7: Fix second `INSERT OR IGNORE INTO clients` (P1, P3)**

Find (around lines 484–492):

```python
            self.db.execute(
                """
                INSERT OR IGNORE INTO clients
                    (id, client_type, first_name, last_name, email, status)
                VALUES (?, ?, ?, ?, ?, 'active')
                """,
                (client_id, client_type, first_name, last_name, email),
            )
```

Replace with:

```python
            self.db.execute(
                """
                INSERT INTO clients
                    (id, client_type, first_name, last_name, email, status)
                VALUES (%s, %s, %s, %s, %s, 'active')
                ON CONFLICT DO NOTHING
                """,
                (client_id, client_type, first_name, last_name, email),
            )
```

- [ ] **Step 8: Fix `UPDATE cross_tool_mapping` — placeholders and datetime (P1, P2)**

Find (around lines 496–503):

```python
            self.db.execute(
                """
                UPDATE cross_tool_mapping
                   SET canonical_id = ?, entity_type = 'CLIENT', synced_at = datetime('now')
                 WHERE canonical_id = ?
                   AND tool_name IN ('pipedrive', 'pipedrive_person')
                """,
                (client_id, lead_id),
            )
```

Replace with:

```python
            self.db.execute(
                """
                UPDATE cross_tool_mapping
                   SET canonical_id = %s, entity_type = 'CLIENT', synced_at = CURRENT_TIMESTAMP
                 WHERE canonical_id = %s
                   AND tool_name IN ('pipedrive', 'pipedrive_person')
                """,
                (client_id, lead_id),
            )
```

- [ ] **Step 9: Fix `_verify_mappings` SELECT and set comprehension (P1, P6)**

Find (around lines 830–839):

```python
        cursor = self.db.execute(
            """
            SELECT tool_name FROM cross_tool_mapping
            WHERE canonical_id = ?
            """,
            (canonical_id,),
        )
        registered = {row[0] if not hasattr(row, "keys") else row["tool_name"]
                      for row in cursor.fetchall()}
```

Replace with:

```python
        cursor = self.db.execute(
            """
            SELECT tool_name FROM cross_tool_mapping
            WHERE canonical_id = %s
            """,
            (canonical_id,),
        )
        registered = {row["tool_name"] for row in cursor.fetchall()}
```

- [ ] **Step 10: Verify**

```bash
grep -n "INSERT OR IGNORE\|datetime('now')\| = ?\b\|, ?)\|hasattr(row\|row\[0\]" automations/new_client_onboarding.py
```

Expected: 0 matches.

- [ ] **Step 10: Delete `db.row_factory` line in `__main__` block**

Find (around line 886):

```python
    db.row_factory = __import__("sqlite3").Row
```

Delete it.

- [ ] **Step 11: Verify**

```bash
grep -n "INSERT OR IGNORE\|datetime('now')\| = ?\b\|, ?)\|hasattr(row\|row\[0\]\|row_factory\|sqlite3" automations/new_client_onboarding.py
```

Expected: 0 matches.

- [ ] **Step 12: Syntax check**

```bash
python -m py_compile automations/new_client_onboarding.py && echo "OK"
```

Expected: `OK`

- [ ] **Step 13: Commit**

```bash
git add automations/new_client_onboarding.py
git commit -m "migrate(new_client_onboarding): P1 %s, P2 CURRENT_TIMESTAMP, P3 ON CONFLICT DO NOTHING, P6 remove hasattr, remove row_factory"
```

---

### Task 9: automations/job_completion_flow.py — P1, P5, P6

**Files:**
- Modify: `automations/job_completion_flow.py`

- [ ] **Step 1: Pre-flight**

```bash
grep -n "datetime('now')\| = ?\|, ?)\|hasattr(row\|row\[0\]" automations/job_completion_flow.py
```

Expected: `?` (lines ~160, ~277), `row[0]` (line ~147), `hasattr(row` (lines ~281–283).

- [ ] **Step 2: Fix `row[0]` invoice ID lookup (P5)**

Find (around line 143–148):

```python
                row = self.db.execute(
                    "SELECT id FROM invoices ORDER BY id DESC LIMIT 1"
                ).fetchone()
                last_n = int(
                    (row[0] if row else "SS-INV-0000").split("-")[-1]
                )
```

Replace with:

```python
                row = self.db.execute(
                    "SELECT id FROM invoices ORDER BY id DESC LIMIT 1"
                ).fetchone()
                last_n = int(
                    (row["id"] if row else "SS-INV-0000").split("-")[-1]
                )
```

- [ ] **Step 3: Fix `INSERT INTO invoices` placeholders (P1)**

Find (around lines 157–168):

```python
                    self.db.execute(
                        "INSERT INTO invoices "
                        "(id, client_id, amount, status, issue_date, due_date) "
                        "VALUES (?, ?, ?, 'sent', ?, ?)",
                        (
                            inv_canonical_id,
                            ctx["canonical_id"],
                            invoice_amount,
                            ctx["completion_date"].isoformat(),
                            inv_due_date,
                        ),
                    )
```

Replace with:

```python
                    self.db.execute(
                        "INSERT INTO invoices "
                        "(id, client_id, amount, status, issue_date, due_date) "
                        "VALUES (%s, %s, %s, 'sent', %s, %s)",
                        (
                            inv_canonical_id,
                            ctx["canonical_id"],
                            invoice_amount,
                            ctx["completion_date"].isoformat(),
                            inv_due_date,
                        ),
                    )
```

- [ ] **Step 4: Fix client lookup SELECT and remove hasattr fallback (P1, P6)**

Find (around lines 276–285):

```python
            row = self.db.execute(
                "SELECT first_name, last_name, email FROM clients WHERE id = ?",
                (canonical_id,),
            ).fetchone()
            if row:
                fn = row["first_name"] if hasattr(row, "keys") else row[0]
                ln = row["last_name"]  if hasattr(row, "keys") else row[1]
                em = row["email"]      if hasattr(row, "keys") else row[2]
                client_name  = f"{fn} {ln}".strip() or client_name
                client_email = em or ""
```

Replace with:

```python
            row = self.db.execute(
                "SELECT first_name, last_name, email FROM clients WHERE id = %s",
                (canonical_id,),
            ).fetchone()
            if row:
                client_name  = f"{row['first_name']} {row['last_name']}".strip() or client_name
                client_email = row["email"] or ""
```

- [ ] **Step 5: Delete `db.row_factory` line in `__main__` block**

Find (around line 568):

```python
    db.row_factory = __import__("sqlite3").Row
```

Delete it.

- [ ] **Step 6: Verify**

```bash
grep -n "datetime('now')\| = ?\b\|, ?)\|hasattr(row\|row\[0\]\|row\[1\]\|row\[2\]\|row_factory\|sqlite3" automations/job_completion_flow.py
```

Expected: 0 matches.

- [ ] **Step 7: Syntax check**

```bash
python -m py_compile automations/job_completion_flow.py && echo "OK"
```

Expected: `OK`

- [ ] **Step 8: Commit**

```bash
git add automations/job_completion_flow.py
git commit -m "migrate(job_completion_flow): P1 %s, P5 row['id'], P6 remove hasattr fallbacks, remove row_factory"
```

---

### Task 10: automations/payment_received.py — P1, P6

**Files:**
- Modify: `automations/payment_received.py`

- [ ] **Step 1: Pre-flight**

```bash
grep -n "datetime('now')\| = ?\|, ?)\|hasattr(row\|row\[0\]\|row\[1\]\|row\[2\]" automations/payment_received.py
```

Expected: `?` (line ~195), `hasattr(row` (lines ~182–184, ~199), `row[0]`/`row[1]`/`row[2]` (lines ~182–184), `row[0]` (line ~199).

- [ ] **Step 2: Fix client info SELECT and remove hasattr fallbacks (P1, P6)**

Find (around lines 177–186):

```python
            row = self.db.execute(
                "SELECT first_name, last_name, client_type FROM clients WHERE id = ?",
                (canonical_id,),
            ).fetchone()
            if row:
                fn = row["first_name"] if hasattr(row, "keys") else row[0]
                ln = row["last_name"]  if hasattr(row, "keys") else row[1]
                ct = row["client_type"] if hasattr(row, "keys") else row[2]
                client_name = f"{fn} {ln}".strip() or client_name
                client_type = (ct or "residential").lower()
```

Replace with:

```python
            row = self.db.execute(
                "SELECT first_name, last_name, client_type FROM clients WHERE id = %s",
                (canonical_id,),
            ).fetchone()
            if row:
                client_name = f"{row['first_name']} {row['last_name']}".strip() or client_name
                client_type = (row["client_type"] or "residential").lower()
```

- [ ] **Step 3: Fix outstanding balance SELECT and remove hasattr fallback (P1, P6)**

Find (around lines 193–199):

```python
            row = self.db.execute(
                "SELECT COALESCE(SUM(amount), 0.0) AS total FROM invoices "
                "WHERE client_id = ? AND status != 'paid'",
                (canonical_id,),
            ).fetchone()
            if row:
                db_outstanding = float(row["total"] if hasattr(row, "keys") else row[0])
```

Replace with:

```python
            row = self.db.execute(
                "SELECT COALESCE(SUM(amount), 0.0) AS total FROM invoices "
                "WHERE client_id = %s AND status != 'paid'",
                (canonical_id,),
            ).fetchone()
            if row:
                db_outstanding = float(row["total"])
```

- [ ] **Step 4: Delete `db.row_factory` line in `__main__` block**

Find (around line 422):

```python
    db.row_factory = __import__("sqlite3").Row
```

Delete it.

- [ ] **Step 5: Verify**

```bash
grep -n "datetime('now')\| = ?\b\|, ?)\|hasattr(row\|row\[0\]\|row\[1\]\|row\[2\]\|row_factory\|sqlite3" automations/payment_received.py
```

Expected: 0 matches.

- [ ] **Step 6: Syntax check**

```bash
python -m py_compile automations/payment_received.py && echo "OK"
```

Expected: `OK`

- [ ] **Step 7: Commit**

```bash
git add automations/payment_received.py
git commit -m "migrate(payment_received): P1 %s, P6 remove hasattr fallbacks, remove row_factory"
```

---

### Task 11: automations/negative_review.py — remove row_factory

**Files:**
- Modify: `automations/negative_review.py`

- [ ] **Step 1: Pre-flight**

```bash
grep -n "sqlite3\|row_factory" automations/negative_review.py
```

Expected: `row_factory` at line ~350.

- [ ] **Step 2: Delete `db.row_factory` line**

Find (around line 350):

```python
    db.row_factory = __import__("sqlite3").Row
```

Delete it entirely.

- [ ] **Step 3: Verify**

```bash
grep -n "sqlite3\|row_factory" automations/negative_review.py
```

Expected: 0 matches.

- [ ] **Step 4: Syntax check**

```bash
python -m py_compile automations/negative_review.py && echo "OK"
```

Expected: `OK`

- [ ] **Step 5: Commit**

```bash
git add automations/negative_review.py
git commit -m "migrate(negative_review): remove sqlite3 row_factory"
```

---

### Task 12: automations/hubspot_qualified_sync.py — P1, P2, P6

**Files:**
- Modify: `automations/hubspot_qualified_sync.py`

- [ ] **Step 1: Pre-flight**

```bash
grep -n "datetime('now')\| = ?\|, ?)\|hasattr(row\|row\[0\]" automations/hubspot_qualified_sync.py
```

Expected: `datetime('now')` (lines ~146, ~149, ~591, ~594), `?` (lines ~357, ~374, ~571), `hasattr(row` (lines ~360, ~575), `row[0]` (lines ~360, ~575).

- [ ] **Step 2: Fix `_classify_contacts` — cross_tool_mapping INSERT (P1, P2)**

Find (around lines 141–152):

```python
                    self.db.execute(
                        """
                        INSERT INTO cross_tool_mapping
                            (canonical_id, entity_type, tool_name,
                             tool_specific_id, synced_at)
                        VALUES (?, 'LEAD', 'hubspot', ?, datetime('now'))
                        ON CONFLICT(canonical_id, tool_name) DO UPDATE SET
                            tool_specific_id = excluded.tool_specific_id,
                            synced_at        = datetime('now')
                        """,
                        (f"SS-LEAD-{next_id_n + i:04d}", contact["hubspot_id"]),
                    )
```

Replace with:

```python
                    self.db.execute(
                        """
                        INSERT INTO cross_tool_mapping
                            (canonical_id, entity_type, tool_name,
                             tool_specific_id, synced_at)
                        VALUES (%s, 'LEAD', 'hubspot', %s, CURRENT_TIMESTAMP)
                        ON CONFLICT(canonical_id, tool_name) DO UPDATE SET
                            tool_specific_id = excluded.tool_specific_id,
                            synced_at        = CURRENT_TIMESTAMP
                        """,
                        (f"SS-LEAD-{next_id_n + i:04d}", contact["hubspot_id"]),
                    )
```

- [ ] **Step 3: Fix `_classify_contacts` — hubspot lookup SELECT and hasattr fallback (P1, P6)**

Find (around lines 355–360):

```python
            rows = self.db.execute(
                "SELECT DISTINCT canonical_id FROM cross_tool_mapping "
                "WHERE tool_specific_id = ? AND tool_name = 'hubspot'",
                (contact["hubspot_id"],),
            ).fetchall()
            canonical_ids = [r[0] if not hasattr(r, "keys") else r["canonical_id"] for r in rows]
```

Replace with:

```python
            rows = self.db.execute(
                "SELECT DISTINCT canonical_id FROM cross_tool_mapping "
                "WHERE tool_specific_id = %s AND tool_name = 'hubspot'",
                (contact["hubspot_id"],),
            ).fetchall()
            canonical_ids = [r["canonical_id"] for r in rows]
```

- [ ] **Step 4: Fix Pipedrive existence check SELECT (P1)**

Find (around lines 372–376):

```python
                row = self.db.execute(
                    "SELECT 1 FROM cross_tool_mapping "
                    "WHERE canonical_id = ? AND tool_name LIKE 'pipedrive%' LIMIT 1",
                    (cid,),
                ).fetchone()
```

Replace with:

```python
                row = self.db.execute(
                    "SELECT 1 FROM cross_tool_mapping "
                    "WHERE canonical_id = %s AND tool_name LIKE 'pipedrive%' LIMIT 1",
                    (cid,),
                ).fetchone()
```

- [ ] **Step 5: Fix `_register_mappings` — collision guard SELECT and hasattr fallback (P1, P6)**

Find (around lines 569–575):

```python
            existing = self.db.execute(
                "SELECT canonical_id FROM cross_tool_mapping "
                "WHERE tool_name = ? AND tool_specific_id = ?",
                (tool_name, tool_id),
            ).fetchone()
            if existing is not None:
                ecid = existing[0] if not hasattr(existing, "keys") else existing["canonical_id"]
```

Replace with:

```python
            existing = self.db.execute(
                "SELECT canonical_id FROM cross_tool_mapping "
                "WHERE tool_name = %s AND tool_specific_id = %s",
                (tool_name, tool_id),
            ).fetchone()
            if existing is not None:
                ecid = existing["canonical_id"]
```

- [ ] **Step 6: Fix `_register_mappings` — INSERT SQL (P1, P2)**

Find (around lines 587–597):

```python
                self.db.execute(
                    """
                    INSERT INTO cross_tool_mapping
                        (canonical_id, entity_type, tool_name, tool_specific_id, synced_at)
                    VALUES (?, ?, ?, ?, datetime('now'))
                    ON CONFLICT(canonical_id, tool_name) DO UPDATE SET
                        tool_specific_id = excluded.tool_specific_id,
                        synced_at        = datetime('now')
                    """,
                    (canonical_id, entity_type, tool_name, tool_id),
                )
```

Replace with:

```python
                self.db.execute(
                    """
                    INSERT INTO cross_tool_mapping
                        (canonical_id, entity_type, tool_name, tool_specific_id, synced_at)
                    VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT(canonical_id, tool_name) DO UPDATE SET
                        tool_specific_id = excluded.tool_specific_id,
                        synced_at        = CURRENT_TIMESTAMP
                    """,
                    (canonical_id, entity_type, tool_name, tool_id),
                )
```

- [ ] **Step 7: Delete `db.row_factory` line in `__main__` block**

Find (around line 694):

```python
    db.row_factory = __import__("sqlite3").Row
```

Delete it.

- [ ] **Step 8: Verify**

```bash
grep -n "datetime('now')\| = ?\b\|, ?)\|hasattr(row\|row\[0\]\|row_factory\|sqlite3" automations/hubspot_qualified_sync.py
```

Expected: 0 matches.

- [ ] **Step 9: Syntax check**

```bash
python -m py_compile automations/hubspot_qualified_sync.py && echo "OK"
```

Expected: `OK`

- [ ] **Step 10: Commit**

```bash
git add automations/hubspot_qualified_sync.py
git commit -m "migrate(hubspot_qualified_sync): P1 %s, P2 CURRENT_TIMESTAMP, P6 remove hasattr fallbacks, remove row_factory"
```

---

### Task 13: automations/create_sql_won_deal.py — P7 + remove sqlite3 import and row_factory

**Files:**
- Modify: `automations/create_sql_won_deal.py`

- [ ] **Step 1: Pre-flight**

```bash
grep -n "sqlite3\|row_factory" automations/create_sql_won_deal.py
```

Expected: `import sqlite3 as _sqlite3` (line ~464) and `db.row_factory = _sqlite3.Row` (line ~511), both in `if __name__ == "__main__"` block.

- [ ] **Step 2: Delete `import sqlite3 as _sqlite3` line**

Find (around line 464, inside `if __name__ == "__main__":`):

```python
    import sqlite3 as _sqlite3
```

Delete it.

- [ ] **Step 3: Delete `db.row_factory = _sqlite3.Row` line**

Find (around line 511):

```python
    db.row_factory = _sqlite3.Row
```

Delete it.

- [ ] **Step 4: Verify**

```bash
grep -n "sqlite3\|row_factory" automations/create_sql_won_deal.py
```

Expected: 0 matches.

- [ ] **Step 5: Syntax check**

```bash
python -m py_compile automations/create_sql_won_deal.py && echo "OK"
```

Expected: `OK`

- [ ] **Step 6: Commit**

```bash
git add automations/create_sql_won_deal.py
git commit -m "migrate(create_sql_won_deal): remove sqlite3 import and row_factory from __main__ block"
```

---

### Task 14: automations/overdue_invoice.py — P1, P6

**Files:**
- Modify: `automations/overdue_invoice.py`

- [ ] **Step 1: Pre-flight**

```bash
grep -n "datetime('now')\| = ?\|, ?)\|hasattr(row\|row\[0\]\|row\[1\]\|row\[2\]\|row\[3\]\|sqlite3\|row_factory" automations/overdue_invoice.py
```

Expected: `?` (line ~361), `hasattr(row` (lines ~365–368), `row[0]`–`row[3]` (lines ~365–368), `row_factory` (line ~604).

- [ ] **Step 2: Fix client details SELECT and remove hasattr fallbacks (P1, P6)**

Find (around lines 360–369):

```python
            row = self.db.execute(
                "SELECT first_name, last_name, email, phone FROM clients WHERE id = ?",
                (canonical_id,),
            ).fetchone()
            if row:
                fn    = row["first_name"] if hasattr(row, "keys") else row[0]
                ln    = row["last_name"]  if hasattr(row, "keys") else row[1]
                email = row["email"]      if hasattr(row, "keys") else row[2]
                phone = row["phone"]      if hasattr(row, "keys") else row[3]
                client_name = f"{fn} {ln}".strip() or client_name
```

Replace with:

```python
            row = self.db.execute(
                "SELECT first_name, last_name, email, phone FROM clients WHERE id = %s",
                (canonical_id,),
            ).fetchone()
            if row:
                email = row["email"]
                phone = row["phone"]
                client_name = f"{row['first_name']} {row['last_name']}".strip() or client_name
```

- [ ] **Step 3: Delete `db.row_factory` line in `__main__` block**

Find (around line 604):

```python
    db.row_factory = __import__("sqlite3").Row
```

Delete it.

- [ ] **Step 4: Verify**

```bash
grep -n "datetime('now')\| = ?\b\|, ?)\|hasattr(row\|row\[0\]\|row\[1\]\|row\[2\]\|row\[3\]\|sqlite3\|row_factory" automations/overdue_invoice.py
```

Expected: 0 matches.

- [ ] **Step 5: Syntax check**

```bash
python -m py_compile automations/overdue_invoice.py && echo "OK"
```

Expected: `OK`

- [ ] **Step 6: Commit**

```bash
git add automations/overdue_invoice.py
git commit -m "migrate(overdue_invoice): P1 %s, P6 remove hasattr fallbacks, remove row_factory"
```

---

### Task 15: automations/lead_leak_detection.py — remove row_factory

**Files:**
- Modify: `automations/lead_leak_detection.py`

- [ ] **Step 1: Pre-flight**

```bash
grep -n "sqlite3\|row_factory" automations/lead_leak_detection.py
```

Expected: `row_factory` at line ~474.

- [ ] **Step 2: Delete `db.row_factory` line**

Find (around line 474):

```python
    db.row_factory = __import__("sqlite3").Row
```

Delete it.

- [ ] **Step 3: Verify**

```bash
grep -n "sqlite3\|row_factory" automations/lead_leak_detection.py
```

Expected: 0 matches.

- [ ] **Step 4: Syntax check**

```bash
python -m py_compile automations/lead_leak_detection.py && echo "OK"
```

Expected: `OK`

- [ ] **Step 5: Commit**

```bash
git add automations/lead_leak_detection.py
git commit -m "migrate(lead_leak_detection): remove sqlite3 row_factory"
```

---

### Task 16: Final sweep verification

**Files:** All 15 modified files

- [ ] **Step 1: Global sweep — confirm no SQLite patterns remain in target directories**

```bash
grep -rn "import sqlite3\|sqlite3\.connect\|sqlite3\.Row\|row_factory\|datetime('now')\|PRAGMA foreign_keys\|AUTOINCREMENT\|INSERT OR IGNORE\|INSERT OR REPLACE" \
  database/ automations/
```

Expected: 0 matches. If any appear, note the file and apply the relevant pattern (P1–P7).

- [ ] **Step 2: Global sweep — confirm no `?` placeholders remain in SQL strings**

```bash
grep -rn "execute.*\".*?.*\"\\|execute.*'.*?.*'" database/ automations/ | grep -v "test\|seeding\|demo\|setup\|scripts"
```

Expected: 0 matches. Any remaining `?` in SQL strings is a P1 miss.

- [ ] **Step 3: Global sweep — confirm no `row[0]`/`row[1]` integer access remains**

```bash
grep -rn "row\[0\]\|row\[1\]\|row\[2\]\|row\[3\]\|hasattr(row" database/ automations/
```

Expected: 0 matches.

- [ ] **Step 4: Compile all modified files**

```bash
python -m py_compile database/mappings.py automations/state.py automations/migrate.py \
  automations/runner.py automations/base.py automations/triggers.py \
  automations/utils/id_resolver.py automations/new_client_onboarding.py \
  automations/job_completion_flow.py automations/payment_received.py \
  automations/negative_review.py automations/hubspot_qualified_sync.py \
  automations/create_sql_won_deal.py automations/overdue_invoice.py \
  automations/lead_leak_detection.py && echo "All OK"
```

Expected: `All OK`

- [ ] **Step 5: Commit sweep result**

```bash
git add -p  # review any remaining changes, then:
git commit -m "migrate: final sweep — confirm SQLite constructs fully removed from database/ and automations/"
```
