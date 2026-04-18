import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

from intelligence.syncers.sync_hubspot import HubSpotSyncer
from tests.sqlite_compat import wrap_sqlite_connection


def _build_conn():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.create_function("GREATEST", 2, lambda a, b: a if a >= b else b)
    conn.executescript(
        """
        CREATE TABLE sync_state (
            tool_name TEXT PRIMARY KEY,
            last_sync_at TEXT NOT NULL,
            records_synced INTEGER,
            last_error TEXT
        );
        CREATE TABLE clients (
            id TEXT PRIMARY KEY,
            client_type TEXT,
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            neighborhood TEXT,
            acquisition_source TEXT,
            lifetime_value REAL DEFAULT 0,
            last_service_date TEXT
        );
        CREATE TABLE leads (
            id TEXT PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            company_name TEXT,
            email TEXT,
            phone TEXT,
            lead_type TEXT,
            source TEXT,
            status TEXT,
            estimated_value REAL,
            created_at TEXT,
            last_activity_at TEXT,
            notes TEXT
        );
        CREATE TABLE cross_tool_mapping (
            canonical_id TEXT NOT NULL,
            entity_type TEXT,
            tool_name TEXT NOT NULL,
            tool_specific_id TEXT,
            tool_specific_url TEXT,
            synced_at TEXT
        );
        """
    )
    conn.commit()
    return conn


def _register_mapping_sqlite(conn, canonical_id, tool_name, tool_specific_id, tool_specific_url=None, db_path=None):
    entity_type = canonical_id.split("-")[1]
    conn.execute(
        """
        INSERT INTO cross_tool_mapping
            (canonical_id, entity_type, tool_name, tool_specific_id, tool_specific_url, synced_at)
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        (canonical_id, entity_type, tool_name, tool_specific_id, tool_specific_url),
    )
    conn.commit()


@patch("intelligence.syncers.base_syncer.get_connection")
def test_upsert_contact_without_customer_evidence_stays_lead(mock_get_connection):
    conn = _build_conn()
    mock_get_connection.return_value = wrap_sqlite_connection(conn)

    contact = SimpleNamespace(
        id="HS-100",
        properties={
            "email": "mia@example.com",
            "firstname": "Mia",
            "lastname": "Jefferson",
            "client_type": "commercial",
            "lifecyclestage": "customer",
            "lead_source_detail": "google_ads",
            "neighborhood": "",
            "lifetime_value": "0",
            "last_service_date": "",
        },
    )

    with patch("intelligence.syncers.sync_hubspot.get_canonical_id", return_value=None), patch(
        "intelligence.syncers.sync_hubspot.generate_id", return_value="SS-LEAD-0001"
    ), patch(
        "intelligence.syncers.sync_hubspot.register_mapping",
        side_effect=lambda canonical_id, tool_name, tool_specific_id, tool_specific_url=None, db_path=None: _register_mapping_sqlite(
            conn, canonical_id, tool_name, tool_specific_id, tool_specific_url, db_path
        ),
    ):
        syncer = HubSpotSyncer(":memory:")
        syncer._upsert_contact(contact)
        syncer.close()

    assert conn.execute("SELECT COUNT(*) FROM clients").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0] == 1
    lead = conn.execute("SELECT id, email FROM leads").fetchone()
    assert lead["id"] == "SS-LEAD-0001"
    assert lead["email"] == "mia@example.com"


@patch("intelligence.syncers.base_syncer.get_connection")
def test_upsert_contact_with_customer_evidence_creates_client(mock_get_connection):
    conn = _build_conn()
    mock_get_connection.return_value = wrap_sqlite_connection(conn)

    contact = SimpleNamespace(
        id="HS-101",
        properties={
            "email": "mueller@example.com",
            "firstname": "Mia",
            "lastname": "Jefferson",
            "client_type": "commercial",
            "lifecyclestage": "customer",
            "lead_source_detail": "google_ads",
            "neighborhood": "",
            "lifetime_value": "538.46",
            "last_service_date": "2026-04-17",
        },
    )

    with patch("intelligence.syncers.sync_hubspot.get_canonical_id", return_value=None), patch(
        "intelligence.syncers.sync_hubspot.generate_id", return_value="SS-CLIENT-0001"
    ), patch(
        "intelligence.syncers.sync_hubspot.register_mapping",
        side_effect=lambda canonical_id, tool_name, tool_specific_id, tool_specific_url=None, db_path=None: _register_mapping_sqlite(
            conn, canonical_id, tool_name, tool_specific_id, tool_specific_url, db_path
        ),
    ):
        syncer = HubSpotSyncer(":memory:")
        syncer._upsert_contact(contact)
        syncer.close()

    assert conn.execute("SELECT COUNT(*) FROM clients").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0] == 0
    client = conn.execute("SELECT id, email, lifetime_value, last_service_date FROM clients").fetchone()
    assert client["id"] == "SS-CLIENT-0001"
    assert client["email"] == "mueller@example.com"
    assert float(client["lifetime_value"]) == 538.46
    assert client["last_service_date"] == "2026-04-17"
