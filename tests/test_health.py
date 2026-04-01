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
