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


def _make_seq_conn(seq_exists: bool, last_value: int, max_id):
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


def test_check_sequences_skips_text_pk_tables():
    """Tables without a SERIAL PK produce no HealthCheck entries."""
    from database.health import check_sequences
    mock_conn = MagicMock()
    # information_schema.sequences returns nothing for 'clients'
    mock_conn.execute.return_value.fetchone.return_value = None
    results = check_sequences(mock_conn, ["clients"])
    assert results == []


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
