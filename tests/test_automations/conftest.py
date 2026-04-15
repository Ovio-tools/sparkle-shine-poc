"""
tests/test_automations/conftest.py

Shared fixtures for Phase 3 automation unit tests.
All API calls are mocked — no real HTTP requests are made.
"""
import os
import sys
import types
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

# ── Ensure project root is importable ─────────────────────────────────────────
_PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# Load .env so TEST_DATABASE_URL is available
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(_PROJECT_ROOT, ".env"))
except ImportError:
    pass

_TEST_DB_URL = os.getenv("TEST_DATABASE_URL", "postgresql://localhost/sparkle_shine_test")

# ── Minimal tool_ids used across all tests ────────────────────────────────────
TEST_TOOL_IDS = {
    "mailchimp": {"audience_id": "test-audience-abc123"},
    "asana": {
        "projects": {
            "Client Success":      "proj-cs-gid",
            "Sales Pipeline Tasks": "proj-sp-gid",
            "Admin & Operations":  "proj-ao-gid",
        },
        "sections": {
            "Client Success": {
                "Onboarding": "sec-onboard-gid",
                "At Risk":    "sec-atrisk-gid",
            },
            "Sales Pipeline Tasks": {
                "Follow-Up": "sec-followup-gid",
            },
            "Admin & Operations": {
                "To Do": "sec-todo-gid",
            },
        },
    },
    "slack": {
        "channels": {
            "new-clients":    "C001",
            "operations":     "C002",
            "sales": "C003",
        },
        "users": {"Maria Gonzalez": "U123MARIA"},
    },
    "quickbooks": {
        "items": {
            "Standard Residential Clean": "19",
            "Deep Clean": "20",
        }
    },
}


# ── Session-scoped auth module stub ───────────────────────────────────────────

@pytest.fixture(autouse=True)
def _patch_auth_modules():
    """
    Replace auth.quickbooks_auth in sys.modules with a lightweight stub so
    automations that do `from auth.quickbooks_auth import get_base_url` inside
    their methods never touch the file system or env vars.
    """
    fake = types.ModuleType("auth.quickbooks_auth")
    fake.get_base_url = lambda: "http://qbo.test/v3/company/TESTID"
    fake.get_quickbooks_headers = lambda: {
        "Authorization": "Bearer test-token",
        "Accept": "application/json",
    }

    original = sys.modules.get("auth.quickbooks_auth")
    sys.modules["auth.quickbooks_auth"] = fake
    yield
    if original is not None:
        sys.modules["auth.quickbooks_auth"] = original
    else:
        sys.modules.pop("auth.quickbooks_auth", None)


# ── PostgreSQL test database ───────────────────────────────────────────────────

def _build_test_db():
    """Create tables, truncate for isolation, and seed test data. Returns Connection."""
    os.environ["DATABASE_URL"] = _TEST_DB_URL
    from database.schema import init_db
    from database.connection import get_connection
    from automations.migrate import _MIGRATIONS

    init_db()
    conn = get_connection()

    # Truncate all tables for test isolation (CASCADE handles FK dependencies)
    try:
        with conn:
            conn.execute("""
                TRUNCATE clients, leads, cross_tool_mapping, invoices, jobs,
                automation_log, pending_actions, poll_state, payments,
                recurring_agreements, reviews, tasks, employees, crews,
                marketing_campaigns, marketing_interactions, commercial_proposals,
                calendar_events, documents, document_index, daily_metrics_snapshot,
                won_deals, gmail_metadata CASCADE
            """)
    except Exception:
        # Fallback: truncate table by table, ignoring errors
        tables = [
            "automation_log", "pending_actions", "poll_state",
            "document_index", "documents", "reviews", "tasks",
            "marketing_interactions", "marketing_campaigns",
            "cross_tool_mapping", "payments", "invoices", "jobs",
            "recurring_agreements", "commercial_proposals",
            "calendar_events", "won_deals", "gmail_metadata",
            "daily_metrics_snapshot", "leads", "employees", "crews", "clients",
        ]
        for t in tables:
            try:
                with conn:
                    conn.execute(f"TRUNCATE {t} CASCADE")
            except Exception:
                pass

    # Run migration statements (CREATE TABLE IF NOT EXISTS — safe to re-run)
    for stmt in _MIGRATIONS:
        try:
            with conn:
                conn.execute(stmt)
        except Exception:
            pass  # Already exists

    # Seed test client
    with conn:
        conn.execute(
            """
            INSERT INTO clients
                (id, client_type, first_name, last_name, email, status)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            ('SS-CLIENT-0001', 'commercial', 'Jane', 'Smith', 'jane@example.com', 'active')
        )

    # Seed cross_tool_mapping
    mappings = [
        ("SS-CLIENT-0001", "CLIENT", "pipedrive",  "201"),
        ("SS-CLIENT-0001", "CLIENT", "jobber",     "301"),
        ("SS-CLIENT-0001", "CLIENT", "quickbooks", "401"),
        ("SS-CLIENT-0001", "CLIENT", "hubspot",    "501"),
        ("SS-CLIENT-0001", "CLIENT", "mailchimp",  "test@example.com"),
        ("SS-JOB-0001",    "JOB",    "jobber",     "601"),
        ("SS-INV-0001",    "INV",    "quickbooks", "701"),
        ("SS-CLIENT-0002", "CLIENT", "hubspot",    "502"),
        ("SS-CLIENT-0002", "CLIENT", "pipedrive",  "202"),
        ("SS-CLIENT-0003", "CLIENT", "hubspot",    "503"),
        ("SS-CLIENT-0003", "CLIENT", "pipedrive",  "203"),
    ]
    with conn:
        for cid, etype, tool, tid in mappings:
            conn.execute(
                """
                INSERT INTO cross_tool_mapping
                    (canonical_id, entity_type, tool_name, tool_specific_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (cid, etype, tool, tid),
            )

    # Seed a sample completed job so invoice writes can satisfy the FK on invoices.job_id.
    with conn:
        conn.execute(
            """
            INSERT INTO jobs
                (id, client_id, service_type_id, scheduled_date, status, completed_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (
                "SS-JOB-0001",
                "SS-CLIENT-0001",
                "std-residential",
                "2026-03-15",
                "completed",
                "2026-03-15",
            ),
        )

    return conn


@pytest.fixture
def mock_db():
    conn = _build_test_db()
    yield conn
    conn.close()


# ── Per-tool mock clients ──────────────────────────────────────────────────────

def _make_jobber_mock() -> MagicMock:
    """Return a Jobber mock session pre-configured with 3 sequential GQL responses."""
    session = MagicMock()
    session.base_url = "https://api.getjobber.com"

    client_resp = MagicMock()
    client_resp.raise_for_status.return_value = None
    client_resp.json.return_value = {
        "data": {
            "clientCreate": {
                "client": {"id": "j-client-123"},
                "userErrors": [],
            }
        }
    }

    prop_resp = MagicMock()
    prop_resp.raise_for_status.return_value = None
    prop_resp.json.return_value = {
        "data": {
            "propertyCreate": {
                "properties": [{"id": "j-prop-456"}],
                "userErrors": [],
            }
        }
    }

    job_resp = MagicMock()
    job_resp.raise_for_status.return_value = None
    job_resp.json.return_value = {
        "data": {
            "jobCreate": {
                "job": {"id": "j-job-789", "title": "Clean", "jobStatus": "ACTIVE"},
                "userErrors": [],
            }
        }
    }

    session.post.side_effect = [client_resp, prop_resp, job_resp]
    return session


def _make_pipedrive_mock() -> MagicMock:
    session = MagicMock()
    session.base_url = "https://api.pipedrive.com/v1"
    session.post.return_value.raise_for_status.return_value = None
    session.post.return_value.json.return_value = {
        "success": True,
        "data": {"id": "act-001"},
    }
    session.get.return_value.raise_for_status.return_value = None
    session.get.return_value.json.return_value = {
        "success": True,
        "data": {
            "items": [
                {
                    "item": {
                        "id": 101,
                        "name": "Jane Smith",
                        "email": [{"value": "jane@example.com"}],
                    }
                }
            ]
        },
    }
    return session


def _make_hubspot_mock() -> MagicMock:
    hs = MagicMock()

    # Contacts basic API: get_by_id returns a contact with properties
    mock_contact = MagicMock()
    mock_contact.properties = {
        "total_services_completed": "3",
        "total_payments_received":  "2",
        "outstanding_balance":      "300.00",
    }
    hs.crm.contacts.basic_api.get_by_id.return_value = mock_contact

    # Contacts search API: do_search returns one result
    mock_result = MagicMock()
    mock_found = MagicMock()
    mock_found.id = "501"
    mock_found.properties = {
        "email":       "jane@example.com",
        "firstname":   "Jane",
        "lastname":    "Smith",
        "lead_source": "Website",
        "createdate":  "1672531200000",  # 2023-01-01
    }
    mock_result.results = [mock_found]
    hs.crm.contacts.search_api.do_search.return_value = mock_result

    created = MagicMock()
    created.id = "hs-created-123"
    hs.crm.contacts.basic_api.create.return_value = created

    return hs


def _make_mailchimp_mock() -> MagicMock:
    mc = MagicMock()
    mc.lists.set_list_member.return_value   = {}
    mc.lists.add_list_member.return_value   = {}
    mc.lists.update_list_member_tags.return_value = {}
    return mc


class MockClients:
    """
    Callable client factory whose per-tool mocks are accessible as attributes
    for use in test assertions.
    """

    def __init__(self):
        self.pipedrive  = _make_pipedrive_mock()
        self.jobber     = _make_jobber_mock()
        # QuickBooks: automations use self.clients("quickbooks") as a headers dict
        self.quickbooks = {
            "Authorization": "Bearer test-qbo-token",
            "Content-Type":  "application/json",
        }
        self.hubspot    = _make_hubspot_mock()
        self.mailchimp  = _make_mailchimp_mock()
        self.asana      = MagicMock()
        self.slack      = MagicMock()
        self.slack.chat_postMessage.return_value = {"ok": True}
        # conversations_list must return a real dict so _resolve_channel_id
        # doesn't iterate infinitely on a truthy MagicMock next_cursor
        self.slack.conversations_list.return_value = {
            "channels": [
                {"name": "new-clients",    "id": "C001"},
                {"name": "operations",     "id": "C002"},
                {"name": "sales", "id": "C003"},
            ],
            "response_metadata": {"next_cursor": ""},
        }

    def __call__(self, tool_name: str):
        return getattr(self, tool_name)


@pytest.fixture
def mock_clients():
    return MockClients()


@pytest.fixture(autouse=True)
def _clear_slack_channel_cache():
    """
    Clear the module-level Slack channel-ID cache between tests to prevent
    state from one test leaking into the next.
    """
    import automations.utils.slack_notify as _sn
    _sn._channel_id_cache.clear()
    yield
    _sn._channel_id_cache.clear()


# ── Realistic trigger events ──────────────────────────────────────────────────

@pytest.fixture(scope="session")
def sample_triggers():
    three_days_ago = (
        datetime.now(timezone.utc) - timedelta(days=3)
    ).strftime("%Y-%m-%dT%H:%M:%S+00:00")

    return {
        "won_deal": {
            "deal_id":           "201",
            "contact_name":      "Jane Smith",
            "contact_email":     "jane@example.com",
            "contact_phone":     "(512) 555-0101",
            "client_type":       "residential",
            "service_type":      "standard_residential",
            "service_frequency": "biweekly",
            "deal_value":        150,
            "neighborhood":      "Zilker",
            "address":           "100 Main St",
        },
        "won_deal_commercial": {
            "deal_id":       "211",
            "contact_name":  "Acme Corp",
            "contact_email": "billing@acme.com",
            "client_type":   "commercial",
            "service_type":  "Commercial Nightly Clean",
            "deal_value":    1200,
            "neighborhood":  "Downtown",
            "address":       "500 Congress Ave",
        },
        "won_deal_one_time": {
            "deal_id":       "221",
            "contact_name":  "Bob Jones",
            "contact_email": "bob.jones.onetime@example.com",
            "client_type":   "one-time",
            "service_type":  "Move-In/Move-Out Clean",
            "deal_value":    325,
            "neighborhood":  "Hyde Park",
            "address":       "200 Elm St",
        },
        "completed_job": {
            "job_id":            "601",
            "client_id":         "301",          # Jobber client → SS-CLIENT-0001
            "service_type":      "Standard Residential Clean",
            "duration_minutes":  130,
            "crew":              "Crew A",
            "completion_notes":  "All done.",
            "is_recurring":      True,
            "completed_at":      "2026-03-15",
        },
        "payment": {
            "payment_id":  "801",
            "amount":      150.00,
            "date":        "2026-03-15",
            "method":      "credit_card",
            "invoice_id":  "701",
            "customer_id": "401",          # QBO customer → SS-CLIENT-0001
        },
        "negative_review": {
            "row_index":    42,
            "date":         "2026-03-15",
            "client_name":  "John Doe",
            "client_email": "john@example.com",
            "rating":       1,
            "review_text":  "Terrible service, crew was late and left without finishing.",
            "crew":         "Crew B",
            "service_type": "deep_clean",
        },
        # ── Lead-leak test fixtures ────────────────────────────────────────────
        "hs_leads_5_total": [
            # 501, 502, 503 are mapped to Pipedrive → NOT leaked
            {
                "hubspot_id": "501",
                "email":      "jane@example.com",
                "firstname":  "Jane",
                "lastname":   "Smith",
                "lead_source": "Website",
                "createdate": three_days_ago,
            },
            {
                "hubspot_id": "502",
                "email":      "client2@example.com",
                "firstname":  "Client",
                "lastname":   "Two",
                "lead_source": "Referral",
                "createdate": three_days_ago,
            },
            {
                "hubspot_id": "503",
                "email":      "client3@example.com",
                "firstname":  "Client",
                "lastname":   "Three",
                "lead_source": "Google Ads",
                "createdate": three_days_ago,
            },
            # 504, 505 have no Pipedrive mapping → leaked
            {
                "hubspot_id": "504",
                "email":      "leaked1@example.com",
                "firstname":  "Leaked",
                "lastname":   "One",
                "lead_source": "Facebook Ads",
                "createdate": three_days_ago,
            },
            {
                "hubspot_id": "505",
                "email":      "leaked2@example.com",
                "firstname":  "Leaked",
                "lastname":   "Two",
                "lead_source": "Google Ads",
                "createdate": three_days_ago,
            },
        ],
    }
