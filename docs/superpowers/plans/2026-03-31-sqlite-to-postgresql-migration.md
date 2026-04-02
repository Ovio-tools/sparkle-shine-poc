# SQLite → PostgreSQL Migration Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace SQLite with PostgreSQL as the database backend for the Sparkle & Shine POC, preserving all existing functionality across 53 files.

**Architecture:** Introduce a thin `PgConnectionWrapper` class in `database/schema.py` that adapts the psycopg2 API to match the sqlite3 interface already used across the codebase (`conn.execute()`, `conn.executemany()`, `with conn:`, `row["col"]` access). This minimises blast radius: most of the 53 callers only need SQL parameter placeholder changes (`?` → `%s`).

**Tech Stack:** Python 3, psycopg2-binary, PostgreSQL 14+, python-dotenv (already installed)

---

## Context

The project uses raw `sqlite3` — no ORM, no migration tool. All 53 files call `get_connection()` from `database/schema.py` and use `conn.execute()` directly. The key challenges are:
- sqlite3 exposes `connection.execute()` directly; psycopg2 requires a cursor
- SQLite uses `?` parameter placeholders; PostgreSQL uses `%s`
- SQLite SQL uses `datetime('now')`, `INTEGER PRIMARY KEY AUTOINCREMENT`, and `PRAGMA foreign_keys`; PostgreSQL needs `NOW()`, `SERIAL`/`BIGSERIAL`, and no pragma
- The test setup creates `sparkle_shine_test.db` — this needs a parallel test database strategy

---

## File Map

| File | Change |
|------|--------|
| `requirements.txt` | Add `psycopg2-binary` |
| `.env.example` | Add `DATABASE_URL` |
| `.env` | Add `DATABASE_URL` (local dev value) |
| `database/schema.py` | Full rewrite: psycopg2 + `PgConnectionWrapper` + fixed DDL |
| `database/mappings.py` | `?` → `%s`, `datetime('now')` → `NOW()` in SQL strings |
| `tests/conftest.py` | Switch to test database via `TEST_DATABASE_URL` env var |
| `scripts/migrate_sqlite_to_pg.py` | **New** — one-time data migration script |
| All 51 other `.py` files using `get_connection` | `?` → `%s` in SQL string literals only |

---

## Task 1: Install psycopg2 and add DATABASE_URL

**Files:**
- Modify: `requirements.txt`
- Modify: `.env.example`
- Modify: `.env`

- [ ] **Step 1: Add psycopg2-binary to requirements**

Edit `requirements.txt` — append after the existing packages:
```
psycopg2-binary
```

- [ ] **Step 2: Install the new dependency**

Run: `pip install psycopg2-binary`
Expected: `Successfully installed psycopg2-binary-x.x.x`

- [ ] **Step 3: Add DATABASE_URL to .env.example**

Append to `.env.example`:
```
DATABASE_URL=postgresql://USER:PASSWORD@HOST:5432/sparkle_shine
TEST_DATABASE_URL=postgresql://USER:PASSWORD@HOST:5432/sparkle_shine_test
```

- [ ] **Step 4: Add DATABASE_URL to .env**

Append to `.env` (substitute your local PostgreSQL credentials):
```
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/sparkle_shine
TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/sparkle_shine_test
```

- [ ] **Step 5: Create the databases in PostgreSQL**

Run: `psql -U postgres -c "CREATE DATABASE sparkle_shine;"` and
`psql -U postgres -c "CREATE DATABASE sparkle_shine_test;"`
Expected: `CREATE DATABASE` for each

- [ ] **Step 6: Commit**

```bash
git add requirements.txt .env.example
git commit -m "chore: add psycopg2-binary and DATABASE_URL env config"
```

---

## Task 2: Rewrite `database/schema.py`

**Files:**
- Modify: `database/schema.py`

This is the central change. We replace `sqlite3` with a `PgConnectionWrapper` that exposes the same interface (`conn.execute()`, `conn.executemany()`, `with conn:`, cursor rows as dicts).

- [ ] **Step 1: Write the failing test**

In `tests/test_db_connection.py` (new file):
```python
import os
import pytest
from database.schema import get_connection, init_db

TEST_DB_URL = os.getenv("TEST_DATABASE_URL")

@pytest.fixture(scope="module")
def pg_conn():
    conn = get_connection(TEST_DB_URL)
    yield conn
    conn.close()

def test_get_connection_returns_wrapper(pg_conn):
    assert hasattr(pg_conn, "execute")
    assert hasattr(pg_conn, "executemany")
    assert hasattr(pg_conn, "close")

def test_execute_returns_cursor_like(pg_conn):
    cursor = pg_conn.execute("SELECT 1 AS val")
    row = cursor.fetchone()
    assert row["val"] == 1

def test_init_db_creates_tables(pg_conn):
    init_db(TEST_DB_URL)
    cursor = pg_conn.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'public' AND table_name = 'clients'"
    )
    row = cursor.fetchone()
    assert row is not None
    assert row["table_name"] == "clients"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_db_connection.py -v`
Expected: FAIL — ImportError or connection error since schema.py still uses sqlite3

- [ ] **Step 3: Rewrite database/schema.py**

Replace the entire file content:
```python
import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

_DEFAULT_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/sparkle_shine")


class PgConnectionWrapper:
    """
    Thin adapter that makes a psycopg2 connection look like sqlite3.Connection.
    Supports: conn.execute(), conn.executemany(), with conn: (transaction), conn.close()
    Rows support dict-style access: row["column_name"]
    """

    def __init__(self, dsn: str):
        self._conn = psycopg2.connect(dsn)
        self._conn.autocommit = False

    def execute(self, sql: str, params=None):
        cursor = self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(sql, params)
        return cursor

    def executemany(self, sql: str, params_list):
        cursor = self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.executemany(sql, params_list)
        return cursor

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self._conn.commit()
        else:
            self._conn.rollback()
        return False  # do not suppress exceptions


CREATE_TABLES = [
    # ------------------------------------------------------------------ #
    # 1. clients
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS clients (
        id                  TEXT PRIMARY KEY,
        client_type         TEXT NOT NULL CHECK(client_type IN ('residential','commercial')),
        first_name          TEXT,
        last_name           TEXT,
        company_name        TEXT,
        email               TEXT UNIQUE NOT NULL,
        phone               TEXT,
        address             TEXT,
        neighborhood        TEXT,
        zone                TEXT,
        status              TEXT NOT NULL DEFAULT 'active'
                                CHECK(status IN ('active','churned','occasional','lead')),
        acquisition_source  TEXT,
        first_service_date  TEXT,
        last_service_date   TEXT,
        lifetime_value      REAL DEFAULT 0.0,
        notes               TEXT,
        created_at          TEXT NOT NULL DEFAULT (NOW()::TEXT)
    )
    """,

    # ------------------------------------------------------------------ #
    # 2. leads
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS leads (
        id                  TEXT PRIMARY KEY,
        first_name          TEXT,
        last_name           TEXT,
        company_name        TEXT,
        email               TEXT,
        phone               TEXT,
        lead_type           TEXT NOT NULL CHECK(lead_type IN ('residential','commercial')),
        source              TEXT,
        status              TEXT NOT NULL DEFAULT 'new'
                                CHECK(status IN ('new','contacted','qualified','lost')),
        estimated_value     REAL DEFAULT 0.0,
        created_at          TEXT NOT NULL DEFAULT (NOW()::TEXT),
        last_activity_at    TEXT,
        notes               TEXT
    )
    """,

    # ------------------------------------------------------------------ #
    # 3. employees
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS employees (
        id                  TEXT PRIMARY KEY,
        first_name          TEXT NOT NULL,
        last_name           TEXT NOT NULL,
        role                TEXT NOT NULL,
        crew_id             TEXT REFERENCES crews(id),
        hire_date           TEXT NOT NULL,
        termination_date    TEXT,
        status              TEXT NOT NULL DEFAULT 'active'
                                CHECK(status IN ('active','terminated')),
        hourly_rate         REAL NOT NULL DEFAULT 0.0,
        email               TEXT,
        notes               TEXT
    )
    """,

    # ------------------------------------------------------------------ #
    # 4. crews
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS crews (
        id                  TEXT PRIMARY KEY,
        name                TEXT NOT NULL,
        zone                TEXT,
        lead_employee_id    TEXT REFERENCES employees(id)
    )
    """,

    # ------------------------------------------------------------------ #
    # 5. jobs
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS jobs (
        id                          TEXT PRIMARY KEY,
        client_id                   TEXT NOT NULL REFERENCES clients(id),
        crew_id                     TEXT REFERENCES crews(id),
        service_type_id             TEXT NOT NULL,
        scheduled_date              TEXT NOT NULL,
        scheduled_time              TEXT,
        duration_minutes_actual     INTEGER,
        status                      TEXT NOT NULL DEFAULT 'scheduled'
                                        CHECK(status IN ('scheduled','completed','cancelled','no-show')),
        address                     TEXT,
        notes                       TEXT,
        review_requested            INTEGER NOT NULL DEFAULT 0 CHECK(review_requested IN (0,1)),
        completed_at                TEXT
    )
    """,

    # ------------------------------------------------------------------ #
    # 6. recurring_agreements
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS recurring_agreements (
        id                  TEXT PRIMARY KEY,
        client_id           TEXT NOT NULL REFERENCES clients(id),
        service_type_id     TEXT NOT NULL,
        crew_id             TEXT REFERENCES crews(id),
        frequency           TEXT NOT NULL CHECK(frequency IN ('weekly','biweekly','monthly')),
        price_per_visit     REAL NOT NULL,
        start_date          TEXT NOT NULL,
        end_date            TEXT,
        status              TEXT NOT NULL DEFAULT 'active'
                                CHECK(status IN ('active','paused','cancelled')),
        day_of_week         TEXT
    )
    """,

    # ------------------------------------------------------------------ #
    # 7. commercial_proposals
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS commercial_proposals (
        id                  TEXT PRIMARY KEY,
        lead_id             TEXT REFERENCES leads(id),
        client_id           TEXT REFERENCES clients(id),
        title               TEXT NOT NULL,
        square_footage      REAL,
        service_scope       TEXT,
        price_per_visit     REAL,
        frequency           TEXT,
        monthly_value       REAL,
        status              TEXT NOT NULL DEFAULT 'draft'
                                CHECK(status IN ('draft','sent','negotiating','won','lost','expired')),
        sent_date           TEXT,
        decision_date       TEXT,
        notes               TEXT
    )
    """,

    # ------------------------------------------------------------------ #
    # 8. invoices
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS invoices (
        id                  TEXT PRIMARY KEY,
        client_id           TEXT NOT NULL REFERENCES clients(id),
        job_id              TEXT REFERENCES jobs(id),
        amount              REAL NOT NULL,
        status              TEXT NOT NULL DEFAULT 'draft'
                                CHECK(status IN ('draft','sent','paid','overdue','written_off')),
        issue_date          TEXT NOT NULL,
        due_date            TEXT,
        paid_date           TEXT,
        days_outstanding    INTEGER
    )
    """,

    # ------------------------------------------------------------------ #
    # 9. payments
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS payments (
        id                  TEXT PRIMARY KEY,
        invoice_id          TEXT NOT NULL REFERENCES invoices(id),
        client_id           TEXT NOT NULL REFERENCES clients(id),
        amount              REAL NOT NULL,
        payment_method      TEXT,
        payment_date        TEXT NOT NULL
    )
    """,

    # ------------------------------------------------------------------ #
    # 10. marketing_campaigns
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS marketing_campaigns (
        id                  TEXT PRIMARY KEY,
        name                TEXT NOT NULL,
        platform            TEXT NOT NULL DEFAULT 'mailchimp',
        campaign_type       TEXT,
        subject_line        TEXT,
        send_date           TEXT,
        recipient_count     INTEGER DEFAULT 0,
        open_rate           REAL DEFAULT 0.0,
        click_rate          REAL DEFAULT 0.0,
        conversion_count    INTEGER DEFAULT 0
    )
    """,

    # ------------------------------------------------------------------ #
    # 11. marketing_interactions
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS marketing_interactions (
        id                  SERIAL PRIMARY KEY,
        client_id           TEXT REFERENCES clients(id),
        lead_id             TEXT REFERENCES leads(id),
        campaign_id         TEXT NOT NULL REFERENCES marketing_campaigns(id),
        interaction_type    TEXT NOT NULL CHECK(interaction_type IN ('open','click','conversion')),
        interaction_date    TEXT NOT NULL
    )
    """,

    # ------------------------------------------------------------------ #
    # 12. reviews
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS reviews (
        id                  TEXT PRIMARY KEY,
        client_id           TEXT NOT NULL REFERENCES clients(id),
        job_id              TEXT REFERENCES jobs(id),
        rating              INTEGER NOT NULL CHECK(rating BETWEEN 1 AND 5),
        review_text         TEXT,
        platform            TEXT,
        review_date         TEXT NOT NULL,
        response_text       TEXT,
        response_date       TEXT
    )
    """,

    # ------------------------------------------------------------------ #
    # 13. tasks
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS tasks (
        id                      TEXT PRIMARY KEY,
        title                   TEXT NOT NULL,
        description             TEXT,
        project_name            TEXT,
        assignee_employee_id    TEXT REFERENCES employees(id),
        client_id               TEXT REFERENCES clients(id),
        due_date                TEXT,
        completed_date          TEXT,
        status                  TEXT NOT NULL DEFAULT 'not_started'
                                    CHECK(status IN ('not_started','in_progress','completed','overdue')),
        priority                TEXT NOT NULL DEFAULT 'medium'
                                    CHECK(priority IN ('low','medium','high')),
        created_at              TEXT NOT NULL DEFAULT (NOW()::TEXT)
    )
    """,

    # ------------------------------------------------------------------ #
    # 14. calendar_events
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS calendar_events (
        id                  TEXT PRIMARY KEY,
        title               TEXT NOT NULL,
        event_type          TEXT,
        start_datetime      TEXT NOT NULL,
        end_datetime        TEXT,
        attendees           TEXT,
        related_client_id   TEXT REFERENCES clients(id),
        notes               TEXT
    )
    """,

    # ------------------------------------------------------------------ #
    # 15. documents
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS documents (
        id                  TEXT PRIMARY KEY,
        title               TEXT NOT NULL,
        doc_type            TEXT NOT NULL CHECK(doc_type IN ('sop','contract','template','spreadsheet')),
        platform            TEXT NOT NULL CHECK(platform IN ('google_docs','google_sheets')),
        google_file_id      TEXT,
        content_text        TEXT,
        keywords            TEXT,
        last_indexed_at     TEXT
    )
    """,

    # ------------------------------------------------------------------ #
    # 16. cross_tool_mapping
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS cross_tool_mapping (
        id                  SERIAL PRIMARY KEY,
        canonical_id        TEXT NOT NULL,
        entity_type         TEXT NOT NULL,
        tool_name           TEXT NOT NULL,
        tool_specific_id    TEXT NOT NULL,
        tool_specific_url   TEXT,
        synced_at           TEXT NOT NULL DEFAULT (NOW()::TEXT),
        UNIQUE(canonical_id, tool_name)
    )
    """,

    # ------------------------------------------------------------------ #
    # 17. daily_metrics_snapshot
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS daily_metrics_snapshot (
        snapshot_date           TEXT PRIMARY KEY,
        total_revenue_mtd       REAL DEFAULT 0.0,
        jobs_completed          INTEGER DEFAULT 0,
        jobs_scheduled          INTEGER DEFAULT 0,
        jobs_cancelled          INTEGER DEFAULT 0,
        active_clients          INTEGER DEFAULT 0,
        new_leads               INTEGER DEFAULT 0,
        open_invoices_value     REAL DEFAULT 0.0,
        overdue_invoices_value  REAL DEFAULT 0.0,
        pipeline_value          REAL DEFAULT 0.0,
        raw_json                TEXT
    )
    """,

    # ------------------------------------------------------------------ #
    # 18. document_index
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS document_index (
        id          SERIAL PRIMARY KEY,
        doc_id      TEXT NOT NULL REFERENCES documents(id),
        chunk_text  TEXT NOT NULL,
        keywords    TEXT,
        source_title TEXT,
        indexed_at  TEXT NOT NULL DEFAULT (NOW()::TEXT)
    )
    """,

    # ------------------------------------------------------------------ #
    # Indexes
    # ------------------------------------------------------------------ #
    "CREATE INDEX IF NOT EXISTS idx_clients_email        ON clients(email)",
    "CREATE INDEX IF NOT EXISTS idx_clients_status       ON clients(status)",
    "CREATE INDEX IF NOT EXISTS idx_leads_status         ON leads(status)",
    "CREATE INDEX IF NOT EXISTS idx_leads_email          ON leads(email)",
    "CREATE INDEX IF NOT EXISTS idx_jobs_client_id       ON jobs(client_id)",
    "CREATE INDEX IF NOT EXISTS idx_jobs_scheduled_date  ON jobs(scheduled_date)",
    "CREATE INDEX IF NOT EXISTS idx_jobs_status          ON jobs(status)",
    "CREATE INDEX IF NOT EXISTS idx_invoices_client_id   ON invoices(client_id)",
    "CREATE INDEX IF NOT EXISTS idx_invoices_status      ON invoices(status)",
    "CREATE INDEX IF NOT EXISTS idx_payments_invoice_id  ON payments(invoice_id)",
    "CREATE INDEX IF NOT EXISTS idx_recurring_client_id  ON recurring_agreements(client_id)",
    "CREATE INDEX IF NOT EXISTS idx_recurring_status     ON recurring_agreements(status)",
    "CREATE INDEX IF NOT EXISTS idx_proposals_status     ON commercial_proposals(status)",
    "CREATE INDEX IF NOT EXISTS idx_tasks_status         ON tasks(status)",
    "CREATE INDEX IF NOT EXISTS idx_tasks_assignee       ON tasks(assignee_employee_id)",
    "CREATE INDEX IF NOT EXISTS idx_mktg_inter_client    ON marketing_interactions(client_id)",
    "CREATE INDEX IF NOT EXISTS idx_mktg_inter_campaign  ON marketing_interactions(campaign_id)",
    "CREATE INDEX IF NOT EXISTS idx_cross_tool_canonical ON cross_tool_mapping(canonical_id)",
    "CREATE INDEX IF NOT EXISTS idx_doc_index_doc_id     ON document_index(doc_id)",

    # ------------------------------------------------------------------ #
    # 19. poll_state
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS poll_state (
        tool_name                TEXT NOT NULL,
        entity_type              TEXT NOT NULL,
        last_processed_id        TEXT,
        last_processed_timestamp TEXT,
        last_poll_at             TEXT NOT NULL,
        PRIMARY KEY (tool_name, entity_type)
    )
    """,

    # ------------------------------------------------------------------ #
    # 20. automation_log
    # ------------------------------------------------------------------ #
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
        created_at      TEXT NOT NULL DEFAULT (NOW()::TEXT)
    )
    """,

    # ------------------------------------------------------------------ #
    # 21. pending_actions
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS pending_actions (
        id              SERIAL PRIMARY KEY,
        automation_name TEXT NOT NULL,
        action_name     TEXT NOT NULL,
        trigger_context TEXT NOT NULL,
        execute_after   TEXT NOT NULL,
        status          TEXT NOT NULL DEFAULT 'pending'
                            CHECK(status IN ('pending','executed','failed')),
        created_at      TEXT NOT NULL DEFAULT (NOW()::TEXT),
        executed_at     TEXT
    )
    """,

    "CREATE INDEX IF NOT EXISTS idx_automation_log_run_id   ON automation_log(run_id)",
    "CREATE INDEX IF NOT EXISTS idx_automation_log_status   ON automation_log(status)",
    "CREATE INDEX IF NOT EXISTS idx_pending_actions_status  ON pending_actions(status)",
    "CREATE INDEX IF NOT EXISTS idx_pending_actions_execute ON pending_actions(execute_after)",
]

_TABLE_NAMES = [
    "clients", "leads", "employees", "crews", "jobs",
    "recurring_agreements", "commercial_proposals", "invoices", "payments",
    "marketing_campaigns", "marketing_interactions", "reviews", "tasks",
    "calendar_events", "documents", "cross_tool_mapping",
    "daily_metrics_snapshot", "document_index",
    "poll_state", "automation_log", "pending_actions",
]


def get_connection(db_url: str = None) -> PgConnectionWrapper:
    url = db_url or _DEFAULT_URL
    return PgConnectionWrapper(url)


def init_db(db_url: str = None) -> None:
    conn = get_connection(db_url)
    with conn:
        for statement in CREATE_TABLES:
            conn.execute(statement)
    conn.close()


if __name__ == "__main__":
    db_url = os.getenv("DATABASE_URL", _DEFAULT_URL)
    init_db(db_url)
    conn = get_connection(db_url)
    print(f"\nDatabase initialised: {db_url}\n")
    print(f"{'Table':<30} {'Columns':>7}")
    print("-" * 40)
    for table in _TABLE_NAMES:
        cursor = conn.execute(
            "SELECT COUNT(*) AS col_count FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = %s",
            (table,)
        )
        row = cursor.fetchone()
        cols = row["col_count"] if row else 0
        print(f"  {table:<28} {cols:>7}")
    cursor = conn.execute(
        "SELECT COUNT(*) AS idx_count FROM pg_indexes WHERE schemaname = 'public'"
    )
    index_count = cursor.fetchone()["idx_count"]
    print("-" * 40)
    print(f"  {'TOTAL TABLES':<28} {len(_TABLE_NAMES):>7}")
    print(f"  {'TOTAL INDEXES':<28} {index_count:>7}")
    conn.close()
    print()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_db_connection.py -v`
Expected: 3 tests PASS

- [ ] **Step 5: Commit**

```bash
git add database/schema.py requirements.txt tests/test_db_connection.py
git commit -m "feat: replace sqlite3 with psycopg2 in database/schema.py; add PgConnectionWrapper"
```

---

## Task 3: Migrate `database/mappings.py`

**Files:**
- Modify: `database/mappings.py`

Two changes: `?` → `%s` throughout, and `datetime('now')` → `NOW()` in the INSERT SQL strings.

- [ ] **Step 1: Write the failing test**

In `tests/test_mappings_pg.py` (new file):
```python
import os
import pytest
from database.schema import get_connection, init_db
from database.mappings import bulk_register, get_canonical_id, get_tool_id, find_unmapped

DB_URL = os.getenv("TEST_DATABASE_URL")

@pytest.fixture(scope="module", autouse=True)
def setup_db():
    init_db(DB_URL)

def test_bulk_register_and_lookup():
    bulk_register([("SS-CLIENT-0001", "jobber", "jb-001")], db_path=DB_URL)
    result = get_canonical_id("jobber", "jb-001", db_path=DB_URL)
    assert result == "SS-CLIENT-0001"

def test_get_tool_id():
    bulk_register([("SS-CLIENT-0002", "hubspot", "hs-002")], db_path=DB_URL)
    result = get_tool_id("SS-CLIENT-0002", "hubspot", db_path=DB_URL)
    assert result == "hs-002"

def test_find_unmapped_returns_list():
    result = find_unmapped("CLIENT", "pipedrive", db_path=DB_URL)
    assert isinstance(result, list)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_mappings_pg.py -v`
Expected: FAIL — psycopg2 rejects `?` placeholders with `syntax error`

- [ ] **Step 3: Update database/mappings.py**

Make these targeted edits (do not change logic, only SQL syntax):

1. Replace every `?` with `%s` in all SQL strings (there are ~15 occurrences).
2. Replace `datetime('now')` with `NOW()` in the two INSERT statements.
3. The `ON CONFLICT ... DO UPDATE SET` syntax is already PostgreSQL-compatible — no change needed.
4. The `db_path` parameter name stays the same (it now receives a DSN URL instead of a file path — the `get_connection()` signature already accepts either via the wrapper).

Key substitutions in `register_mapping()`:
```python
# Before
conn.execute(
    "SELECT canonical_id FROM cross_tool_mapping "
    "WHERE tool_name = ? AND tool_specific_id = ?",
    (tool_name, tool_specific_id),
)
# After
conn.execute(
    "SELECT canonical_id FROM cross_tool_mapping "
    "WHERE tool_name = %s AND tool_specific_id = %s",
    (tool_name, tool_specific_id),
)
```

In the INSERT in `register_mapping()`:
```python
# Before
VALUES (?, ?, ?, ?, ?, datetime('now'))
# After
VALUES (%s, %s, %s, %s, %s, NOW()::TEXT)
```

In `bulk_register()` executemany INSERT:
```python
# Before
VALUES (?, ?, ?, ?, ?, datetime('now'))
# After
VALUES (%s, %s, %s, %s, %s, NOW()::TEXT)
```

Apply the same `?` → `%s` pattern to all other `conn.execute()` calls in this file (`get_tool_id`, `get_tool_url`, `get_canonical_id`, `get_all_mappings`, `find_unmapped`, `list_mapped_tools`, `print_mapping_report`).

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_mappings_pg.py -v`
Expected: 3 tests PASS

- [ ] **Step 5: Commit**

```bash
git add database/mappings.py tests/test_mappings_pg.py
git commit -m "feat: migrate database/mappings.py SQL to PostgreSQL syntax"
```

---

## Task 4: Update `tests/conftest.py` for PostgreSQL

**Files:**
- Modify: `tests/conftest.py`

The current fixture yields a file path. Now it should yield a DSN URL, and setup/teardown should use `init_db()`.

- [ ] **Step 1: Rewrite tests/conftest.py**

```python
"""
pytest configuration for Sparkle & Shine integration tests.

Provides:
  - test_db_url fixture  (session-scoped, schema created fresh each run)
  - requires_google mark (skips tests when token.json is absent)
  - sys.path wiring so project modules are always importable
"""

import os
import sys
import pytest

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

_TEST_DB_URL = os.getenv(
    "TEST_DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/sparkle_shine_test",
)


def _google_token_exists() -> bool:
    parent = os.path.dirname(_PROJECT_ROOT)
    return os.path.exists(os.path.join(_PROJECT_ROOT, "token.json")) or \
           os.path.exists(os.path.join(parent, "token.json"))


requires_google = pytest.mark.skipif(
    not _google_token_exists(),
    reason=(
        "Google token.json not found — complete the OAuth flow first: "
        "python -m auth --verify"
    ),
)


@pytest.fixture(scope="session")
def test_db_path():
    """Yield the DSN URL for the isolated test database (named test_db_path for backwards compat)."""
    from database.schema import init_db
    init_db(_TEST_DB_URL)
    yield _TEST_DB_URL
```

Note: The old fixture was named `test_db_path` and yielded a file path string. It now yields a DSN URL string. All existing tests that pass `test_db_path` to `get_connection()` or to mapping functions will work unchanged since those functions now accept a DSN URL.

- [ ] **Step 2: Run existing phase tests to check scope**

Run: `pytest tests/test_phase1.py -v 2>&1 | head -50`
Expected: Tests run (some may fail due to `?` placeholder errors — this surfaces which files still need updating)

- [ ] **Step 3: Commit**

```bash
git add tests/conftest.py
git commit -m "feat: update tests/conftest.py to use PostgreSQL test database"
```

---

## Task 5: Replace `?` placeholders across all remaining files

**Files:** All 51 remaining `.py` files identified by grep

This is the bulk of the work. Every `?` used as a SQL parameter placeholder must become `%s`. Python's `?` used in non-SQL contexts (ternary expressions, f-strings, print statements) must NOT be changed.

- [ ] **Step 1: Find all files with SQL `?` placeholders**

Run: `grep -rn "execute.*\?" sparkle-shine-poc --include="*.py" | grep -v "test_db" | grep -v ".pyc"`

This lists every file + line where `?` appears inside an `execute()` call.

- [ ] **Step 2: Apply the replacement to each file**

For each file returned by the grep above, do a targeted find-and-replace of SQL `?` → `%s`. The safest approach is to look for the pattern inside `execute(` or `executemany(` strings.

**Key files to prioritise (highest SQL density):**
- `intelligence/metrics/revenue.py` — ~8 `conn.execute()` calls, all read-only SELECT
- `automations/job_completion_flow.py` — ~3 `self.db.execute()` calls with params
- `automations/payment_received.py`, `negative_review.py`, `lead_leak_detection.py`, `overdue_invoice.py`, `new_client_onboarding.py`, `hubspot_qualified_sync.py`, `create_sql_won_deal.py`
- `seeding/generators/gen_clients.py`, `gen_jobs.py`, `gen_financials.py`, `gen_marketing.py`, `gen_tasks_events.py`
- `seeding/pushers/push_jobber.py`, `push_quickbooks.py`, `push_hubspot.py`, `push_mailchimp.py`, `push_pipedrive.py`, `push_asana.py`
- `simulation/generators/operations.py`, `deals.py`, `churn.py`
- `simulation/reconciliation/reconciler.py`
- All `tests/` files and `demo/` files

**Pattern to apply per file** (example from `automations/job_completion_flow.py`):
```python
# Before
row = self.db.execute(
    "SELECT * FROM jobs WHERE id = ?",
    (job_id,)
).fetchone()
# After
row = self.db.execute(
    "SELECT * FROM jobs WHERE id = %s",
    (job_id,)
).fetchone()
```

Also watch for `datetime('now')` appearing in INSERT statements in these files — replace with `NOW()::TEXT`.

- [ ] **Step 3: Also fix `db_path` default arguments in callers**

Many functions have `db_path: str = "sparkle_shine.db"` as a default. These defaults are now dead code (the env var handles it) but won't cause errors. Leave them in place for now to avoid unrelated signature changes.

- [ ] **Step 4: Run all tests**

Run: `pytest tests/ -v --tb=short 2>&1 | tail -40`
Expected: Tests that were passing before should still pass. Fix any new failures caused by remaining `?` occurrences.

- [ ] **Step 5: Commit**

```bash
git add .
git commit -m "feat: replace all SQLite ? placeholders with %s for PostgreSQL compatibility"
```

---

## Task 6: Write the one-time data migration script

**Files:**
- Create: `scripts/migrate_sqlite_to_pg.py`

This script reads all 21 tables from the existing `sparkle_shine.db` SQLite file and inserts them into PostgreSQL, table by table, in dependency order (parents before children).

- [ ] **Step 1: Write the migration script**

Create `scripts/migrate_sqlite_to_pg.py`:
```python
"""
One-time migration: copy all data from sparkle_shine.db (SQLite) to PostgreSQL.
Run from project root: python scripts/migrate_sqlite_to_pg.py

Prerequisites:
  - PostgreSQL database exists and schema is initialised (run init_db() first)
  - .env contains DATABASE_URL pointing to the target PostgreSQL instance
  - sparkle_shine.db exists in the project root
"""
import os
import sys
import sqlite3
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _PROJECT_ROOT)

SQLITE_PATH = os.path.join(_PROJECT_ROOT, "sparkle_shine.db")
PG_URL = os.getenv("DATABASE_URL")

# Tables in insertion order (parents before children)
TABLES_IN_ORDER = [
    "crews",
    "employees",
    "clients",
    "leads",
    "jobs",
    "recurring_agreements",
    "commercial_proposals",
    "invoices",
    "payments",
    "marketing_campaigns",
    "marketing_interactions",
    "reviews",
    "tasks",
    "calendar_events",
    "documents",
    "cross_tool_mapping",
    "daily_metrics_snapshot",
    "document_index",
    "poll_state",
    "automation_log",
    "pending_actions",
]

# Tables with SERIAL PKs — must skip the id column in INSERT and let PostgreSQL assign it
# (But since we want to preserve IDs for referential integrity, we override the sequence instead)
SERIAL_TABLES = {
    "marketing_interactions",
    "cross_tool_mapping",
    "document_index",
    "automation_log",
    "pending_actions",
}


def migrate():
    if not PG_URL:
        print("ERROR: DATABASE_URL not set in .env")
        sys.exit(1)
    if not os.path.exists(SQLITE_PATH):
        print(f"ERROR: SQLite database not found at {SQLITE_PATH}")
        sys.exit(1)

    sqlite_conn = sqlite3.connect(SQLITE_PATH)
    sqlite_conn.row_factory = sqlite3.Row

    pg_conn = psycopg2.connect(PG_URL)
    pg_conn.autocommit = False

    print(f"Migrating from {SQLITE_PATH} → {PG_URL}\n")

    for table in TABLES_IN_ORDER:
        rows = sqlite_conn.execute(f"SELECT * FROM {table}").fetchall()
        if not rows:
            print(f"  {table}: 0 rows (skipped)")
            continue

        col_names = list(rows[0].keys())
        placeholders = ", ".join(["%s"] * len(col_names))
        cols_sql = ", ".join(col_names)
        insert_sql = (
            f"INSERT INTO {table} ({cols_sql}) VALUES ({placeholders}) "
            f"ON CONFLICT DO NOTHING"
        )

        data = [tuple(row[c] for c in col_names) for row in rows]
        with pg_conn:
            cur = pg_conn.cursor()
            cur.executemany(insert_sql, data)

        # If the table has a SERIAL PK, reset the sequence to the current max
        if table in SERIAL_TABLES:
            with pg_conn:
                cur = pg_conn.cursor()
                cur.execute(
                    f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), "
                    f"COALESCE(MAX(id), 1)) FROM {table}"
                )

        print(f"  {table}: {len(rows)} rows migrated")

    sqlite_conn.close()
    pg_conn.close()
    print("\nMigration complete.")


if __name__ == "__main__":
    migrate()
```

- [ ] **Step 2: Run the migration**

First initialise the schema, then run:
```bash
python -c "from database.schema import init_db; init_db()"
python scripts/migrate_sqlite_to_pg.py
```
Expected output: each table listed with row count, ending with "Migration complete."

- [ ] **Step 3: Verify row counts match**

Run:
```bash
python -c "
import sqlite3, psycopg2, os
from dotenv import load_dotenv; load_dotenv()
sqlite = sqlite3.connect('sparkle_shine.db')
pg = psycopg2.connect(os.getenv('DATABASE_URL'))
tables = ['clients','leads','employees','jobs','invoices','payments','reviews','cross_tool_mapping']
for t in tables:
    s = sqlite.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]
    cur = pg.cursor(); cur.execute(f'SELECT COUNT(*) FROM {t}'); p = cur.fetchone()[0]
    status = 'OK' if s == p else 'MISMATCH'
    print(f'  {t:<30} sqlite={s:>6}  pg={p:>6}  [{status}]')
"
```
Expected: All rows show `OK`

- [ ] **Step 4: Commit**

```bash
git add scripts/migrate_sqlite_to_pg.py
git commit -m "feat: add one-time SQLite→PostgreSQL data migration script"
```

---

## Task 7: End-to-end verification

- [ ] **Step 1: Run the full test suite**

Run: `pytest tests/ -v --tb=short`
Expected: All previously-passing tests pass against PostgreSQL

- [ ] **Step 2: Run the intelligence runner (dry run)**

Run: `python -m intelligence.runner --skip-sync --date 2026-03-31 --dry-run`
Expected: Context document printed without errors

- [ ] **Step 3: Run the automation runner (dry run)**

Run: `python -m automations.runner --dry-run`
Expected: Automation scan completes without SQL errors

- [ ] **Step 4: Run the mapping report**

Run: `python -c "from database.mappings import print_mapping_report; print_mapping_report()"`
Expected: Mapping coverage table printed for all 8 tools

- [ ] **Step 5: Final commit**

```bash
git add .
git commit -m "feat: complete SQLite→PostgreSQL migration; all tests passing"
```

---

## Key SQL Differences Summary

| SQLite | PostgreSQL |
|--------|-----------|
| `sqlite3.connect(path)` | `psycopg2.connect(dsn)` |
| `conn.execute(sql, ?)` | `conn.cursor().execute(sql, %s)` — handled by wrapper |
| `sqlite3.Row` (dict-like) | `RealDictCursor` — handled by wrapper |
| `PRAGMA foreign_keys = ON` | Remove — FKs always enforced |
| `datetime('now')` in DEFAULT | `NOW()::TEXT` |
| `INTEGER PRIMARY KEY AUTOINCREMENT` | `SERIAL PRIMARY KEY` |
| `sqlite_master` | `information_schema.tables` / `pg_indexes` |
| `?` parameter placeholder | `%s` parameter placeholder |
| `ON CONFLICT ... DO UPDATE` | Same — already ANSI-compatible |

## Dependencies to Update

| Dependency | Change |
|------------|--------|
| `sqlite3` (stdlib) | Replaced by `psycopg2-binary` (new in requirements.txt) |
| No change needed | `python-dotenv`, `requests`, Google/HubSpot/Mailchimp/Asana/Slack SDKs |

## Verification Plan

1. `pytest tests/test_db_connection.py` — connection wrapper works
2. `pytest tests/test_mappings_pg.py` — ID mapping system works
3. `pytest tests/ -v` — full suite green
4. `python scripts/migrate_sqlite_to_pg.py` — data migrated cleanly
5. `python -m intelligence.runner --skip-sync --dry-run` — briefing pipeline end-to-end
