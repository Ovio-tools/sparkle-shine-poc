"""Helpers for running Postgres-first code against SQLite in unit tests."""

from __future__ import annotations

import re
import sqlite3


_INTERVAL_RE = re.compile(
    r"CURRENT_DATE\s*-\s*INTERVAL\s*'(\d+)\s+days'",
    re.IGNORECASE,
)
_NULLS_FIRST_RE = re.compile(
    r"ORDER BY\s+([A-Za-z0-9_\.]+)\s+ASC\s+NULLS\s+FIRST",
    re.IGNORECASE,
)


def adapt_postgres_sql(sql: str) -> str:
    """Translate the PostgreSQL subset used by legacy tests into SQLite SQL."""
    adapted = sql.replace("%s", "?")
    adapted = adapted.replace("::date", "")
    adapted = adapted.replace("::timestamp", "")
    adapted = _INTERVAL_RE.sub(lambda m: f"date('now', '-{m.group(1)} days')", adapted)
    adapted = adapted.replace("CURRENT_DATE", "date('now')")
    adapted = _NULLS_FIRST_RE.sub(r"ORDER BY \1 IS NOT NULL, \1 ASC", adapted)
    return adapted


class SQLiteCompatConnection:
    """Minimal wrapper matching the shared DB connection API used in code."""

    def __init__(self, conn: sqlite3.Connection, *, close_underlying: bool):
        self._conn = conn
        self._close_underlying = close_underlying
        self._conn.row_factory = sqlite3.Row

    def execute(self, sql, params=None):
        return self._conn.execute(adapt_postgres_sql(sql), params or ())

    def executemany(self, sql, params_list):
        return self._conn.executemany(adapt_postgres_sql(sql), params_list)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        if self._close_underlying:
            self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self._conn.commit()
        else:
            self._conn.rollback()
        return False

    def __getattr__(self, name):
        return getattr(self._conn, name)


def open_sqlite_compat(db_path: str) -> SQLiteCompatConnection:
    """Open a file-backed SQLite DB behind the compat wrapper."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return SQLiteCompatConnection(conn, close_underlying=True)


def wrap_sqlite_connection(conn: sqlite3.Connection) -> SQLiteCompatConnection:
    """Wrap an existing SQLite connection without taking ownership of close()."""
    conn.row_factory = sqlite3.Row
    return SQLiteCompatConnection(conn, close_underlying=False)


def sqlite_get_column_names(conn, table: str) -> set[str]:
    """Return column names for a SQLite table."""
    raw_conn = getattr(conn, "_conn", conn)
    cursor = raw_conn.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cursor.fetchall()}
