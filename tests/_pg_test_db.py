"""Per-worker Postgres test-database resolver.

Two test suites truncate and re-seed TEST_DATABASE_URL:
  - tests/test_phase4.py
  - tests/test_automations/conftest.py

Run them under pytest-xdist and they race each other on the shared DB
(deadlocks, false failures, random TRUNCATE errors). This module gives each
xdist worker its own database by suffixing the DB name with the worker id
(e.g. sparkle_shine_test_gw0) and auto-creating it on first use.

Single-process runs (PYTEST_XDIST_WORKER unset or "master") keep the
original URL unchanged, so nothing about the existing local workflow changes.
"""
from __future__ import annotations

import os
import re
from urllib.parse import urlparse, urlunparse

import psycopg2


_DEFAULT_URL = "postgresql://localhost/sparkle_shine_test"
_ensured_urls: set[str] = set()


def _worker_suffix() -> str:
    worker = os.environ.get("PYTEST_XDIST_WORKER", "")
    if not worker or worker == "master":
        return ""
    return "_" + re.sub(r"[^a-z0-9]", "_", worker.lower())


def _db_name(url: str) -> str:
    return (urlparse(url).path or "/").lstrip("/")


def _with_db_name(url: str, new_name: str) -> str:
    parsed = urlparse(url)
    return urlunparse(parsed._replace(path=f"/{new_name}"))


def _append_suffix(url: str, suffix: str) -> str:
    if not suffix:
        return url
    base = _db_name(url) or "sparkle_shine_test"
    return _with_db_name(url, base + suffix)


def _ensure_database(url: str) -> None:
    """Create the target database via a maintenance connection if missing."""
    if url in _ensured_urls:
        return
    target = _db_name(url)
    if not target:
        _ensured_urls.add(url)
        return
    maintenance_url = _with_db_name(url, "postgres")
    try:
        conn = psycopg2.connect(maintenance_url)
    except Exception:
        # If the maintenance DB is unreachable, fall through and let the
        # caller's real connection attempt produce the actionable error.
        _ensured_urls.add(url)
        return
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (target,))
        if cur.fetchone() is None:
            # target is regex-sanitized to [a-z0-9_]; safe to interpolate.
            cur.execute(f'CREATE DATABASE "{target}"')
        cur.close()
    finally:
        conn.close()
    _ensured_urls.add(url)


def resolve_test_db_url() -> str:
    """Return the pytest-worker-safe Postgres test DB URL.

    - Base URL comes from TEST_DATABASE_URL (or a localhost default).
    - Under pytest-xdist, the worker id is appended to the DB name so each
      worker has its own schema to truncate without colliding with peers.
    - The per-worker DB is auto-created on first call (idempotent).
    """
    base = os.getenv("TEST_DATABASE_URL", _DEFAULT_URL)
    url = _append_suffix(base, _worker_suffix())
    _ensure_database(url)
    return url
