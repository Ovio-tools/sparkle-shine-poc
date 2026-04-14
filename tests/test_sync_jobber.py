import sqlite3
from unittest.mock import patch

from intelligence.syncers.sync_jobber import (
    _quote_is_recurring,
    _service_type_from_title,
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


def test_service_type_from_title_maps_known_titles():
    assert _service_type_from_title("Recurring Biweekly") == "recurring-biweekly"
    assert _service_type_from_title("Standard Residential Clean") == "std-residential"
    assert _service_type_from_title("Move-In/Move-Out Clean") == "move-in-out"


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
            job_title_raw TEXT,
            jobber_job_type TEXT,
            scheduled_date TEXT,
            scheduled_time TEXT,
            duration_minutes_actual INTEGER,
            status TEXT NOT NULL DEFAULT 'scheduled',
            notes TEXT,
            is_recurring_job BOOLEAN,
            jobber_updated_at TEXT,
            completed_at TEXT
        );
        """
    )
    conn.execute(
        """
        INSERT INTO jobs
            (id, client_id, service_type_id, scheduled_date, scheduled_time,
             duration_minutes_actual, status, notes, is_recurring_job, jobber_updated_at,
             completed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "SS-JOB-0001",
            "SS-CLIENT-0001",
            "residential-clean",
            "2026-04-12",
            "07:30",
            120,
            "completed",
            None,
            None,
            None,
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


def test_upsert_job_enriches_confirmed_fields():
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
            job_title_raw TEXT,
            jobber_job_type TEXT,
            scheduled_date TEXT,
            scheduled_time TEXT,
            duration_minutes_actual INTEGER,
            status TEXT NOT NULL DEFAULT 'scheduled',
            notes TEXT,
            is_recurring_job BOOLEAN,
            jobber_updated_at TEXT,
            completed_at TEXT
        );
        """
    )
    conn.execute(
        """
        INSERT INTO jobs
            (id, client_id, service_type_id, status)
        VALUES (?, ?, ?, ?)
        """,
        ("SS-JOB-0002", "SS-CLIENT-0001", "residential-clean", "scheduled"),
    )
    conn.commit()

    with patch("intelligence.syncers.base_syncer.get_connection", return_value=wrap_sqlite_connection(conn)):
        with patch("intelligence.syncers.sync_jobber.get_canonical_id") as mock_get_canonical_id:
            syncer = JobberSyncer(":memory:")
            mock_get_canonical_id.side_effect = lambda tool, tool_id, **kwargs: {
                "JOBBER-JOB-2": "SS-JOB-0002",
                "JOBBER-CLIENT-1": "SS-CLIENT-0001",
            }.get(tool_id)

            syncer._upsert_job(
                {
                    "id": "JOBBER-JOB-2",
                    "title": "Recurring Weekly",
                    "instructions": "Gate code 4455",
                    "jobType": "RECURRING",
                    "jobStatus": "ACTIVE",
                    "startAt": "2026-04-13T08:00:00Z",
                    "endAt": None,
                    "updatedAt": "2026-04-13T09:00:00Z",
                    "client": {"id": "JOBBER-CLIENT-1"},
                    "visitSchedule": {"recurrenceSchedule": {"calendarRule": "FREQ=WEEKLY"}},
                    "visits": {"nodes": [{"duration": 7200}]},
                }
            )
            syncer.close()

    row = conn.execute(
        """
        SELECT service_type_id, job_title_raw, jobber_job_type, scheduled_date, scheduled_time,
               duration_minutes_actual, notes, is_recurring_job, jobber_updated_at
        FROM jobs
        WHERE id = ?
        """,
        ("SS-JOB-0002",),
    ).fetchone()
    assert row["service_type_id"] == "recurring-weekly"
    assert row["job_title_raw"] == "Recurring Weekly"
    assert row["jobber_job_type"] == "RECURRING"
    assert row["scheduled_date"] == "2026-04-13"
    assert row["scheduled_time"] == "08:00"
    assert row["duration_minutes_actual"] == 120
    assert row["notes"] == "Gate code 4455"
    assert row["is_recurring_job"] == 1
    assert row["jobber_updated_at"] == "2026-04-13T09:00:00Z"
