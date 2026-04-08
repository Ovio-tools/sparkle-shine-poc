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

_TEST_DB_URL = os.getenv(
    "TEST_DATABASE_URL",
    "postgresql://localhost/sparkle_shine_test",
)

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
