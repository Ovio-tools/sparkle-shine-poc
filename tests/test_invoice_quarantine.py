from scripts import quarantine_residual_orphan_invoices as quarantine


def _seed_client(conn, client_id: str, email: str) -> None:
    conn.execute(
        """
        INSERT INTO clients (id, client_type, email, status)
        VALUES (%s, 'residential', %s, 'active')
        """,
        (client_id, email),
    )


def test_fetch_residual_no_same_day_candidates_excludes_duplicates_and_same_day_jobs(pg_test_conn):
    _seed_client(pg_test_conn, "SS-CLIENT-0001", "quarantine-1@example.com")
    _seed_client(pg_test_conn, "SS-CLIENT-0002", "quarantine-2@example.com")
    _seed_client(pg_test_conn, "SS-CLIENT-0003", "quarantine-3@example.com")
    _seed_client(pg_test_conn, "SS-CLIENT-0004", "quarantine-4@example.com")
    _seed_client(pg_test_conn, "SS-CLIENT-0005", "quarantine-5@example.com")

    pg_test_conn.execute(
        """
        INSERT INTO jobs (
            id, client_id, service_type_id, scheduled_date, status, completed_at
        )
        VALUES
            ('SS-JOB-1003', 'SS-CLIENT-0003', 'recurring-weekly', '2026-04-15', 'completed', '2026-04-15 09:00:00'),
            ('SS-JOB-1005', 'SS-CLIENT-0005', 'recurring-biweekly', '2026-04-09', 'completed', '2026-04-09 11:00:00'),
            ('SS-JOB-2001', 'SS-CLIENT-0004', 'recurring-biweekly', '2026-04-09', 'completed', '2026-04-09 11:30:00')
        """
    )
    pg_test_conn.execute(
        """
        INSERT INTO invoices (id, client_id, job_id, amount, status, issue_date, due_date)
        VALUES
            ('SS-INV-1001', 'SS-CLIENT-0001', NULL, 150.0, 'sent', '2026-04-09', '2026-04-09'),
            ('SS-INV-1002', 'SS-CLIENT-0002', NULL, 150.0, 'sent', '2026-04-09', '2026-04-09'),
            ('SS-INV-1003', 'SS-CLIENT-0003', NULL, 135.0, 'sent', '2026-04-14', '2026-04-14'),
            ('SS-INV-1004', 'SS-CLIENT-0004', NULL, 150.0, 'sent', '2026-04-09', '2026-04-09'),
            ('SS-INV-1005', 'SS-CLIENT-0005', NULL, 150.0, 'sent', '2026-04-09', '2026-04-09'),
            ('SS-INV-2001', 'SS-CLIENT-0004', 'SS-JOB-2001', 150.0, 'sent', '2026-04-09', '2026-04-09')
        """
    )
    pg_test_conn.execute(
        """
        INSERT INTO cross_tool_mapping (canonical_id, entity_type, tool_name, tool_specific_id)
        VALUES
            ('SS-INV-1001', 'INV', 'quickbooks', '9001'),
            ('SS-INV-1003', 'INV', 'quickbooks', '9003'),
            ('SS-INV-1004', 'INV', 'quickbooks', '9004'),
            ('SS-INV-1005', 'INV', 'quickbooks', '9005')
        """
    )
    pg_test_conn.commit()

    rows = quarantine._fetch_residual_no_same_day_candidates(pg_test_conn, nearby_window_days=3)
    ids = [row["invoice_id"] for row in rows]

    assert ids == ["SS-INV-1003", "SS-INV-1001", "SS-INV-1002"]
    nearby_counts = {row["invoice_id"]: row["nearby_completed_job_count_3d"] for row in rows}
    assert nearby_counts["SS-INV-1003"] == 1
    assert nearby_counts["SS-INV-1001"] == 0
    assert nearby_counts["SS-INV-1002"] == 0


def test_run_quarantine_upserts_rows_idempotently(pg_test_conn):
    _seed_client(pg_test_conn, "SS-CLIENT-0101", "quarantine-run@example.com")
    pg_test_conn.execute(
        """
        INSERT INTO invoices (id, client_id, job_id, amount, status, issue_date, due_date)
        VALUES ('SS-INV-0101', 'SS-CLIENT-0101', NULL, 150.0, 'sent', '2026-04-09', '2026-04-09')
        """
    )
    pg_test_conn.execute(
        """
        INSERT INTO cross_tool_mapping (canonical_id, entity_type, tool_name, tool_specific_id)
        VALUES ('SS-INV-0101', 'INV', 'quickbooks', '9101')
        """
    )
    pg_test_conn.commit()

    with pg_test_conn:
        rows, summary = quarantine.run_quarantine(
            pg_test_conn,
            apply=True,
            nearby_window_days=3,
            source="test-source",
            reviewed_by="test-reviewer",
            expected_total=1,
            expected_auto=1,
            expected_manual=0,
        )
    assert summary["total"] == 1
    assert rows[0]["reason_code"] == "NO_NEARBY_COMPLETED_JOB_3D"

    with pg_test_conn:
        quarantine.run_quarantine(
            pg_test_conn,
            apply=True,
            nearby_window_days=3,
            source="test-source",
            reviewed_by="test-reviewer",
            expected_total=1,
            expected_auto=1,
            expected_manual=0,
        )

    stored = pg_test_conn.execute(
        """
        SELECT invoice_id, quarantine_lane, reason_code, reviewed_by
        FROM invoice_quarantine
        """
    ).fetchall()
    assert len(stored) == 1
    assert stored[0]["invoice_id"] == "SS-INV-0101"
    assert stored[0]["quarantine_lane"] == "auto_quarantine"
    assert stored[0]["reason_code"] == "NO_NEARBY_COMPLETED_JOB_3D"
    assert stored[0]["reviewed_by"] == "test-reviewer"
