import os
import logging

from dotenv import load_dotenv
load_dotenv()

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)


class Connection:
    """Thin wrapper around psycopg2.connection.

    Provides conn.execute() / conn.executemany() shortcuts that return
    RealDictCursor results (dict-like row access). Implements context
    manager: commits on clean exit, rolls back on exception.
    """

    def __init__(self, pg_conn):
        self._conn = pg_conn

    def execute(self, sql, params=None):
        cursor = self._conn.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
        )
        cursor.execute(sql, params)
        return cursor

    def executemany(self, sql, params_list):
        cursor = self._conn.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
        )
        cursor.executemany(sql, params_list)
        return cursor

    def cursor(self):
        return self._conn.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
        )

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
        return False  # don't suppress exceptions


def get_connection() -> Connection:
    """Return a PostgreSQL connection wrapped in Connection.

    Reads DATABASE_URL from environment.

    Raises EnvironmentError if DATABASE_URL is not set.
    """
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise EnvironmentError(
            "DATABASE_URL is not set. Set it in .env or environment.\n"
            "  Local:   DATABASE_URL=postgresql://localhost/sparkle_shine\n"
            "  Railway: provided automatically by the PostgreSQL plugin"
        )
    conn = psycopg2.connect(url)
    conn.autocommit = False
    return Connection(conn)


# ── Helper functions ──────────────────────────────────────────────

def column_exists(conn: Connection, table: str, column: str) -> bool:
    """Check if a column exists in the given table."""
    cursor = conn.execute(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = %s AND column_name = %s",
        (table, column),
    )
    return cursor.fetchone() is not None


def table_exists(conn: Connection, table: str) -> bool:
    """Check if a table exists."""
    cursor = conn.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = 'public' AND table_name = %s",
        (table,),
    )
    return cursor.fetchone() is not None


def get_column_names(conn: Connection, table: str) -> set[str]:
    """Return the set of column names for a table.

    Replaces all PRAGMA table_info usage.
    Used by _ensure_schema() methods to check before ALTER TABLE.
    """
    cursor = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = %s",
        (table,),
    )
    return {row["column_name"] for row in cursor.fetchall()}


def date_subtract_sql(days: int) -> str:
    """Return a SQL fragment for 'today minus N days'.

    Returns: CURRENT_DATE - INTERVAL '60 days'
    """
    return f"CURRENT_DATE - INTERVAL '{days} days'"
