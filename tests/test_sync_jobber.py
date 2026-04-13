import sqlite3
from unittest.mock import patch

from intelligence.syncers.sync_jobber import (
    _quote_is_recurring,
    JobberSyncer,
)
from tests.sqlite_compat import wrap_sqlite_connection


def test_quote_is_recurring_when_any_linked_job_is_recurring():
    quote = {
        "jobs": {
            "nodes": [
                {"jobType": "ONE_OFF"},
                {"jobType": "RECURRING"},
            ]
        }
    }

    assert _quote_is_recurring(quote) is True


def test_quote_is_not_recurring_without_recurring_linked_jobs():
    quote = {"jobs": {"nodes": [{"jobType": "ONE_OFF"}]}}

    assert _quote_is_recurring(quote) is False
def test_upsert_job_does_not_downgrade_completed_rows_to_scheduled():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE sync_state (
            tool_name TEXT PRIMARY KEY,
            last_sync_at TEXT NOT NULL,
            records_synced INTEGER,
            last_error TEXT
        );
        CREATE TABLE jobs (
            id TEXT PRIMARY KEY,
            client_id TEXT NOT NULL,
            service_type_id TEXT NOT NULL,
            scheduled_date TEXT,
            scheduled_time TEXT,
            status TEXT NOT NULL DEFAULT 'scheduled',
            completed_at TEXT
        );
        """
    )
    conn.execute(
        """
        INSERT INTO jobs (id, client_id, service_type_id, scheduled_date, scheduled_time, status, completed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "SS-JOB-0001",
            "SS-CLIENT-0001",
            "residential-clean",
            "2026-04-12",
            "07:30",
            "completed",
            "2026-04-12T22:56:19.969422",
        ),
    )
    conn.commit()

    with patch("intelligence.syncers.base_syncer.get_connection", return_value=wrap_sqlite_connection(conn)):
        with patch("intelligence.syncers.sync_jobber.get_canonical_id") as mock_get_canonical_id:
            syncer = JobberSyncer(":memory:")
            mock_get_canonical_id.side_effect = lambda tool, tool_id, **kwargs: {
                "JOBBER-JOB-1": "SS-JOB-0001",
                "JOBBER-CLIENT-1": "SS-CLIENT-0001",
            }.get(tool_id)

            syncer._upsert_job(
                {
                    "id": "JOBBER-JOB-1",
                    "jobStatus": "ACTIVE",
                    "startAt": "2026-04-12T07:30:00Z",
                    "endAt": None,
                    "client": {"id": "JOBBER-CLIENT-1"},
                }
            )
            syncer.close()

    row = conn.execute(
        "SELECT status, completed_at FROM jobs WHERE id = ?",
        ("SS-JOB-0001",),
    ).fetchone()
    assert row["status"] == "completed"
    assert row["completed_at"] == "2026-04-12T22:56:19.969422"
