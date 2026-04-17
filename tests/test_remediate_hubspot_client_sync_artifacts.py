from scripts import remediate_hubspot_client_sync_artifacts as remediation


def _insert_lead(conn, lead_id: str, email: str) -> None:
    conn.execute(
        """
        INSERT INTO leads (id, email, lead_type, source, status)
        VALUES (%s, %s, 'residential', 'hubspot', 'new')
        """,
        (lead_id, email),
    )


def _insert_client(conn, client_id: str, email: str) -> None:
    conn.execute(
        """
        INSERT INTO clients (id, client_type, first_name, email, status)
        VALUES (%s, 'residential', 'Test', %s, 'active')
        """,
        (client_id, email),
    )


def _insert_hubspot_mapping(conn, canonical_id: str, entity_type: str, hubspot_id: str) -> None:
    conn.execute(
        """
        INSERT INTO cross_tool_mapping (
            canonical_id, entity_type, tool_name, tool_specific_id
        ) VALUES (%s, %s, 'hubspot', %s)
        """,
        (canonical_id, entity_type, hubspot_id),
    )


def test_execute_reassigns_hubspot_mapping_when_client_has_dependencies(pg_test_conn):
    _insert_lead(pg_test_conn, "SS-LEAD-9001", "shared@example.com")
    _insert_client(pg_test_conn, "SS-CLIENT-9001", "shared@example.com")
    _insert_hubspot_mapping(pg_test_conn, "SS-LEAD-9001", "LEAD", "hs-123")
    pg_test_conn.execute(
        """
        INSERT INTO cross_tool_mapping (
            canonical_id, entity_type, tool_name, tool_specific_id
        ) VALUES (%s, 'CLIENT', 'jobber', %s)
        """,
        ("SS-CLIENT-9001", "jobber-123"),
    )
    pg_test_conn.commit()

    candidates = remediation._load_candidates()

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.action == "reassign_hubspot_mapping"
    assert candidate.has_jobber is True

    stats = remediation.remediate(candidates, execute=True)
    row = pg_test_conn.execute(
        """
        SELECT canonical_id, entity_type
        FROM cross_tool_mapping
        WHERE tool_name = 'hubspot' AND tool_specific_id = 'hs-123'
        """
    ).fetchone()

    assert stats["reassigned"] == 1
    assert stats["deleted"] == 0
    assert row["canonical_id"] == "SS-CLIENT-9001"
    assert row["entity_type"] == "CLIENT"


def test_execute_deletes_dependency_free_duplicate_client(pg_test_conn):
    _insert_lead(pg_test_conn, "SS-LEAD-9002", "delete-me@example.com")
    _insert_client(pg_test_conn, "SS-CLIENT-9002", "delete-me@example.com")
    _insert_hubspot_mapping(pg_test_conn, "SS-LEAD-9002", "LEAD", "hs-456")
    pg_test_conn.commit()

    candidates = remediation._load_candidates()

    assert len(candidates) == 1
    assert candidates[0].action == "delete_duplicate_client"

    stats = remediation.remediate(candidates, execute=True)
    client_row = pg_test_conn.execute(
        "SELECT 1 FROM clients WHERE id = %s",
        ("SS-CLIENT-9002",),
    ).fetchone()
    mapping_row = pg_test_conn.execute(
        """
        SELECT canonical_id
        FROM cross_tool_mapping
        WHERE tool_name = 'hubspot' AND tool_specific_id = 'hs-456'
        """
    ).fetchone()

    assert stats["deleted"] == 1
    assert stats["reassigned"] == 0
    assert client_row is None
    assert mapping_row["canonical_id"] == "SS-LEAD-9002"
