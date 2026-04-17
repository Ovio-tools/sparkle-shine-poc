#!/usr/bin/env python3
"""
Ad-hoc Railway PostgreSQL operations.

Connects via DATABASE_PUBLIC_URL (works from a local machine) or
DATABASE_URL (works inside Railway).

Usage:
    python scripts/railway_db.py tokens
    python scripts/railway_db.py tokens jobber
    python scripts/railway_db.py clear-token jobber
    python scripts/railway_db.py poll-state
    python scripts/railway_db.py auto-log 20
    python scripts/railway_db.py sql "SELECT * FROM oauth_tokens"
"""
from __future__ import annotations

import argparse
import os
import sys

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

load_dotenv()


def _get_url() -> str:
    url = os.environ.get("DATABASE_PUBLIC_URL") or os.environ.get("DATABASE_URL")
    if not url:
        print("ERROR: set DATABASE_PUBLIC_URL or DATABASE_URL first.", file=sys.stderr)
        raise SystemExit(1)
    return url


def _connect():
    conn = psycopg2.connect(_get_url(), connect_timeout=10)
    conn.autocommit = True
    return conn


def _print_rows(cursor, max_col_width: int = 80) -> None:
    if cursor.description is None:
        print("(no results)")
        return

    rows = cursor.fetchall()
    if not rows:
        print("(0 rows)")
        return

    cols = [d.name for d in cursor.description]
    rendered_rows = []
    for row in rows:
        values = row.values() if hasattr(row, "values") else row
        rendered = []
        for val in values:
            text = str(val) if val is not None else "NULL"
            if len(text) > max_col_width:
                text = text[: max_col_width - 3] + "..."
            rendered.append(text)
        rendered_rows.append(rendered)

    widths = [len(c) for c in cols]
    for row in rendered_rows:
        for idx, val in enumerate(row):
            widths[idx] = max(widths[idx], len(val))

    print(" | ".join(col.ljust(widths[idx]) for idx, col in enumerate(cols)))
    print("-+-".join("-" * width for width in widths))
    for row in rendered_rows:
        print(" | ".join(val.ljust(widths[idx]) for idx, val in enumerate(row)))
    print(f"\n({len(rows)} row{'s' if len(rows) != 1 else ''})")


def cmd_tokens(args) -> None:
    conn = _connect()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if args.tool_name:
            cur.execute(
                """
                SELECT tool_name, updated_at, token_data
                FROM oauth_tokens
                WHERE tool_name = %s
                """,
                (args.tool_name,),
            )
        else:
            cur.execute(
                """
                SELECT tool_name, updated_at,
                       substring(token_data::text, 1, 120) AS token_preview
                FROM oauth_tokens
                ORDER BY tool_name
                """
            )
        _print_rows(cur)
    finally:
        conn.close()


def cmd_clear_token(args) -> None:
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM oauth_tokens WHERE tool_name = %s", (args.tool_name,))
        print(f"Deleted {cur.rowcount} row(s) for '{args.tool_name}'")
    finally:
        conn.close()


def cmd_poll_state(args) -> None:
    conn = _connect()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM poll_state ORDER BY tool_name, entity_type")
        _print_rows(cur)
    finally:
        conn.close()


def cmd_auto_log(args) -> None:
    conn = _connect()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT * FROM automation_log ORDER BY created_at DESC LIMIT %s",
            (args.limit,),
        )
        _print_rows(cur)
    finally:
        conn.close()


def cmd_sql(args) -> None:
    query = args.query.strip()
    first_word = query.split()[0].upper() if query else ""
    if not args.force and first_word in {"DELETE", "DROP", "TRUNCATE", "ALTER", "UPDATE", "INSERT"}:
        print(f"ERROR: '{first_word}' blocked without --force.", file=sys.stderr)
        raise SystemExit(1)

    conn = _connect()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(query)
        _print_rows(cur)
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Ad-hoc Railway PostgreSQL operations")
    sub = parser.add_subparsers(dest="command", required=True)

    p_tokens = sub.add_parser("tokens", help="Show oauth_tokens rows")
    p_tokens.add_argument("tool_name", nargs="?", help="Optional tool filter")
    p_tokens.set_defaults(func=cmd_tokens)

    p_clear = sub.add_parser("clear-token", help="Delete a token row")
    p_clear.add_argument("tool_name", help="Tool name to clear")
    p_clear.set_defaults(func=cmd_clear_token)

    p_poll = sub.add_parser("poll-state", help="Show poll_state watermarks")
    p_poll.set_defaults(func=cmd_poll_state)

    p_log = sub.add_parser("auto-log", help="Show recent automation_log rows")
    p_log.add_argument("limit", nargs="?", type=int, default=20, help="Number of rows")
    p_log.set_defaults(func=cmd_auto_log)

    p_sql = sub.add_parser("sql", help="Run ad-hoc SQL")
    p_sql.add_argument("query", help="SQL query to execute")
    p_sql.add_argument("--force", action="store_true", help="Allow write statements")
    p_sql.set_defaults(func=cmd_sql)

    args = parser.parse_args()
    args.func(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
