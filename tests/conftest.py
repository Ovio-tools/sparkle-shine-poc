"""
pytest configuration for Sparkle & Shine tests.
"""
import os
import sys
import pytest

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# Load .env so TEST_DATABASE_URL is available
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(_PROJECT_ROOT, ".env"))
except ImportError:
    pass

_TEST_DB_URL = os.getenv(
    "TEST_DATABASE_URL",
    "postgresql://localhost/sparkle_shine_test",
)

# Legacy SQLite path kept for simulation tests that still use SQLite
_TEST_DB_SQLITE = os.path.join(_PROJECT_ROOT, "sparkle_shine_test.db")


def _google_token_exists() -> bool:
    parent = os.path.dirname(_PROJECT_ROOT)
    return os.path.exists(os.path.join(_PROJECT_ROOT, "token.json")) or \
           os.path.exists(os.path.join(parent, "token.json"))


requires_google = pytest.mark.skipif(
    not _google_token_exists(),
    reason=(
        "Google token.json not found — complete the OAuth flow first: "
        "python -m auth --verify"
    ),
)


@pytest.fixture(scope="session")
def test_db_path():
    """Yield the SQLite path for simulation/legacy tests that still use SQLite."""
    yield _TEST_DB_SQLITE


@pytest.fixture(scope="session")
def test_pg_db():
    """Yield the DSN URL for the test PostgreSQL database."""
    # Point get_connection() at the test database
    os.environ["DATABASE_URL"] = _TEST_DB_URL
    from database.schema import init_db
    init_db()
    yield _TEST_DB_URL
