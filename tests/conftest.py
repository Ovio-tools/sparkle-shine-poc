"""
pytest configuration for Sparkle & Shine Phase 1 integration tests.

Provides:
  - test_db_path fixture  (session-scoped, auto-cleaned up)
  - requires_google mark  (skips tests when token.json is absent)
  - sys.path wiring so project modules are always importable
"""

import os
import sys
import pytest

# ------------------------------------------------------------------ #
# Make the project root importable regardless of how pytest is invoked
# ------------------------------------------------------------------ #
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

_TEST_DB = os.path.join(_PROJECT_ROOT, "sparkle_shine_test.db")


# ------------------------------------------------------------------ #
# Google-token guard
# ------------------------------------------------------------------ #
def _google_token_exists() -> bool:
    """Return True if a Google OAuth token.json is present."""
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


# ------------------------------------------------------------------ #
# Fixtures
# ------------------------------------------------------------------ #
@pytest.fixture(scope="session")
def test_db_path():
    """Yield the path to the isolated test database."""
    yield _TEST_DB


@pytest.fixture(scope="session", autouse=True)
def cleanup_test_db(test_db_path):
    """Delete the test database after the full test session finishes."""
    yield
    if os.path.exists(test_db_path):
        os.unlink(test_db_path)
