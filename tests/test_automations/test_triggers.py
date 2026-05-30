from unittest.mock import MagicMock

import requests

from automations.triggers import poll_jobber_completed_jobs


def test_poll_jobber_completed_jobs_includes_assignees_and_visit_duration(mock_db):
    with mock_db:
        mock_db.execute(
            """
            INSERT INTO poll_state
                (tool_name, entity_type, last_processed_id, last_processed_timestamp, last_poll_at)
            VALUES (%s, %s, %s, %s, %s)
            """,
            ("jobber", "completed_job", None, "2026-03-14T00:00:00Z", "2026-03-14T00:00:00Z"),
        )

    session = MagicMock()
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = {
        "data": {
            "jobs": {
                "nodes": [
                    {
                        "id": "JOBBER-JOB-1",
                        "title": "Recurring Weekly",
                        "completedAt": "2026-03-15T18:00:00Z",
                        "instructions": "Gate code",
                        "jobType": "RECURRING",
                        "client": {"id": "JOBBER-CLIENT-1", "name": "Jane Smith"},
                        "visitSchedule": {"recurrenceSchedule": {"calendarRule": "FREQ=WEEKLY"}},
                        "visits": {
                            "nodes": [
                                {
                                    "id": "visit-older",
                                    "duration": 120,
                                    "startAt": "2026-03-15T08:00:00Z",
                                    "endAt": "2026-03-15T10:00:00Z",
                                    "completedAt": "2026-03-15T10:00:00Z",
                                    "assignedUsers": {"nodes": []},
                                },
                                {
                                    "id": "visit-newer",
                                    "duration": None,
                                    "startAt": "2026-03-15T12:00:00Z",
                                    "endAt": "2026-03-15T13:30:00Z",
                                    "completedAt": "2026-03-15T13:30:00Z",
                                    "assignedUsers": {
                                        "nodes": [
                                            {
                                                "id": "user-1",
                                                "name": {"full": "Claudia Ramirez"},
                                                "email": {"raw": "claudia.ramirez@oviodigital.com"},
                                            }
                                        ]
                                    },
                                },
                            ]
                        },
                    }
                ],
                "pageInfo": {"hasNextPage": False, "endCursor": None},
            }
        }
    }
    session.post.return_value = resp

    events = poll_jobber_completed_jobs(lambda tool: session, mock_db)

    assert len(events) == 1
    event = events[0]
    assert event["duration_minutes"] == 90
    assert event["jobber_assigned_users"] == [
        {
            "id": "user-1",
            "name": "Claudia Ramirez",
            "email": "claudia.ramirez@oviodigital.com",
        }
    ]
    assert event["crew"] is None


def test_poll_jobber_completed_jobs_stops_paging_once_it_hits_watermark(mock_db):
    with mock_db:
        mock_db.execute(
            """
            INSERT INTO poll_state
                (tool_name, entity_type, last_processed_id, last_processed_timestamp, last_poll_at)
            VALUES (%s, %s, %s, %s, %s)
            """,
            ("jobber", "completed_job", None, "2026-03-14T00:00:00Z", "2026-03-14T00:00:00Z"),
        )

    session = MagicMock()

    first_resp = MagicMock()
    first_resp.raise_for_status.return_value = None
    first_resp.json.return_value = {
        "data": {
            "jobs": {
                "nodes": [
                    {
                        "id": "JOBBER-JOB-NEW",
                        "title": "Recurring Weekly",
                        "completedAt": "2026-03-15T18:00:00Z",
                        "instructions": "",
                        "jobType": "RECURRING",
                        "client": {"id": "JOBBER-CLIENT-1", "name": "Jane Smith"},
                        "visitSchedule": {"recurrenceSchedule": {"calendarRule": "FREQ=WEEKLY"}},
                        "visits": {"nodes": []},
                    },
                    {
                        "id": "JOBBER-JOB-OLD",
                        "title": "Older Job",
                        "completedAt": "2026-03-14T00:00:00Z",
                        "instructions": "",
                        "jobType": "RECURRING",
                        "client": {"id": "JOBBER-CLIENT-2", "name": "John Smith"},
                        "visitSchedule": {"recurrenceSchedule": {"calendarRule": "FREQ=WEEKLY"}},
                        "visits": {"nodes": []},
                    },
                ],
                "pageInfo": {"hasNextPage": True, "endCursor": "cursor-2"},
            }
        }
    }

    session.post.return_value = first_resp

    events = poll_jobber_completed_jobs(lambda tool: session, mock_db)

    assert len(events) == 1
    assert events[0]["job_id"] == "JOBBER-JOB-NEW"
    assert session.post.call_count == 1


def test_poll_jobber_completed_jobs_retries_transient_timeout(mock_db, monkeypatch):
    with mock_db:
        mock_db.execute(
            """
            INSERT INTO poll_state
                (tool_name, entity_type, last_processed_id, last_processed_timestamp, last_poll_at)
            VALUES (%s, %s, %s, %s, %s)
            """,
            ("jobber", "completed_job", None, "2026-03-14T00:00:00Z", "2026-03-14T00:00:00Z"),
        )

    monkeypatch.setattr("automations.triggers.JOBBER.wait", lambda: None)
    monkeypatch.setattr("automations.triggers.time.sleep", lambda _: None)

    session = MagicMock()
    success_resp = MagicMock()
    success_resp.raise_for_status.return_value = None
    success_resp.json.return_value = {
        "data": {
            "jobs": {
                "nodes": [
                    {
                        "id": "JOBBER-JOB-1",
                        "title": "Recurring Weekly",
                        "completedAt": "2026-03-15T18:00:00Z",
                        "instructions": "",
                        "jobType": "RECURRING",
                        "client": {"id": "JOBBER-CLIENT-1", "name": "Jane Smith"},
                        "visitSchedule": {"recurrenceSchedule": {"calendarRule": "FREQ=WEEKLY"}},
                        "visits": {"nodes": []},
                    }
                ],
                "pageInfo": {"hasNextPage": False, "endCursor": None},
            }
        }
    }
    session.post.side_effect = [requests.Timeout("slow page"), success_resp]

    events = poll_jobber_completed_jobs(lambda tool: session, mock_db)

    assert len(events) == 1
    assert events[0]["job_id"] == "JOBBER-JOB-1"
    assert session.post.call_count == 2
