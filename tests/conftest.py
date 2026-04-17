"""
pytest configuration for Sparkle & Shine tests.
"""
import os
import sys
import warnings
import pytest

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

warnings.filterwarnings(
    "ignore",
    message=r"You are using a Python version 3\.9 past its end of life.*",
    category=FutureWarning,
)
warnings.filterwarnings(
    "ignore",
    message=r"You are using a non-supported Python version \(3\.9\.6\).*",
    category=FutureWarning,
)
warnings.filterwarnings(
    "ignore",
    message=r"urllib3 v2 only supports OpenSSL 1\.1\.1\+.*",
    category=Warning,
)

# Load .env so TEST_DATABASE_URL is available
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(_PROJECT_ROOT, ".env"))
except ImportError:
    pass

from credentials import google_noninteractive_credentials_available
from tests._pg_test_db import resolve_test_db_url

_TEST_DB_URL = resolve_test_db_url()

# Legacy SQLite path kept only for the shrinking set of tests that still
# explicitly exercise SQLite-only helpers.
_TEST_DB_SQLITE = os.path.join(_PROJECT_ROOT, "sparkle_shine_test.db")


requires_google = pytest.mark.skipif(
    not google_noninteractive_credentials_available(),
    reason=(
        "Google auth is not configured for non-interactive use. "
        "Set GOOGLE_CLIENT_ID/GOOGLE_CLIENT_SECRET/GOOGLE_REFRESH_TOKEN, "
        "or provide GOOGLE_CREDENTIALS_FILE/GOOGLE_TOKEN_FILE with a refresh token."
    ),
)


@pytest.fixture(scope="session", autouse=True)
def _init_test_db():
    """Point DATABASE_URL at the PostgreSQL test instance once per session.

    Autouse so every test file (including simulation tests that call
    database.mappings functions) always hits the test DB, never production.
    """
    os.environ["DATABASE_URL"] = _TEST_DB_URL
    from database.schema import init_db
    init_db()


@pytest.fixture(scope="session")
def test_db_path():
    """Yield the legacy SQLite path for the few tests that still opt into SQLite."""
    yield _TEST_DB_SQLITE


@pytest.fixture(scope="session")
def test_pg_db():
    """Yield the DSN URL for the test PostgreSQL database."""
    yield _TEST_DB_URL


@pytest.fixture
def pg_test_conn():
    """Yield a PostgreSQL test connection with a clean schema.

    Truncates domain tables before yielding so tests start from an empty DB.
    Mirrors the _make_pg_test_db() helper in tests/test_phase4.py.
    """
    os.environ["DATABASE_URL"] = _TEST_DB_URL
    from database.schema import init_db
    from database.connection import get_connection as _gc
    init_db()
    conn = _gc()
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
    yield conn
    conn.close()
