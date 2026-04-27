"""
Conftest for test_services package.

These tests mock all external dependencies (HTTP, DB, Slack) at the module
level, so they don't need the live PostgreSQL fixture from the root conftest.
"""
import pytest


@pytest.fixture(scope="session", autouse=True)
def _init_test_db():
    """No-op override: services unit tests mock the database layer."""
    pass
