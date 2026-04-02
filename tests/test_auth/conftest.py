"""
Conftest for test_auth package.

Overrides the root-level _init_test_db autouse fixture so these unit tests
do not require a live PostgreSQL connection. The database module is already
mocked via sys.modules in each test file before importing token_store.
"""
import pytest


@pytest.fixture(scope="session", autouse=True)
def _init_test_db():
    """No-op override: token_store unit tests mock database at the module level."""
    pass
