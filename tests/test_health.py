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
