"""Tests for token_store env var fallback tier."""
import os
import unittest
from unittest.mock import patch, MagicMock

from auth import token_store


class TestLoadFromEnv(unittest.TestCase):
    """Test the env var fallback tier in token_store."""

    def test_returns_none_when_no_env_vars(self):
        """No env vars set -> returns None."""
        with patch.dict(os.environ, {}, clear=True):
            result = token_store._load_from_env("jobber")
            self.assertIsNone(result)

    def test_returns_refresh_token_from_env(self):
        """JOBBER_REFRESH_TOKEN set -> returns dict with refresh_token."""
        with patch.dict(os.environ, {"JOBBER_REFRESH_TOKEN": "rt_abc123"}, clear=True):
            result = token_store._load_from_env("jobber")
            self.assertEqual(result, {"refresh_token": "rt_abc123"})

    def test_returns_both_tokens_from_env(self):
        """Both JOBBER_REFRESH_TOKEN and JOBBER_ACCESS_TOKEN set."""
        env = {
            "JOBBER_REFRESH_TOKEN": "rt_abc123",
            "JOBBER_ACCESS_TOKEN": "at_xyz789",
        }
        with patch.dict(os.environ, env, clear=True):
            result = token_store._load_from_env("jobber")
            self.assertEqual(result, {
                "refresh_token": "rt_abc123",
                "access_token": "at_xyz789",
            })

    def test_quickbooks_uses_qbo_prefix(self):
        """QuickBooks uses QBO_ prefix, not QUICKBOOKS_."""
        with patch.dict(os.environ, {"QBO_REFRESH_TOKEN": "rt_qbo"}, clear=True):
            result = token_store._load_from_env("quickbooks")
            self.assertEqual(result, {"refresh_token": "rt_qbo"})

    def test_quickbooks_ignores_wrong_prefix(self):
        """QUICKBOOKS_REFRESH_TOKEN should NOT be picked up."""
        with patch.dict(os.environ, {"QUICKBOOKS_REFRESH_TOKEN": "rt_wrong"}, clear=True):
            result = token_store._load_from_env("quickbooks")
            self.assertIsNone(result)

    def test_google_returns_refresh_token(self):
        """GOOGLE_REFRESH_TOKEN set."""
        with patch.dict(os.environ, {"GOOGLE_REFRESH_TOKEN": "rt_google"}, clear=True):
            result = token_store._load_from_env("google")
            self.assertEqual(result, {"refresh_token": "rt_google"})


class TestLoadTokensEnvFallback(unittest.TestCase):
    """Test that load_tokens falls through to env vars when DB and JSON fail."""

    @patch.object(token_store, "_load_from_db", return_value=(None, True))
    def test_env_var_fallback_when_db_and_json_missing(self, mock_db):
        """No DB, no JSON file -> falls through to env var."""
        with patch.dict(os.environ, {"JOBBER_REFRESH_TOKEN": "rt_fallback"}, clear=True):
            result = token_store.load_tokens("jobber", "/nonexistent/path.json")
            self.assertEqual(result, {"refresh_token": "rt_fallback"})

    @patch.object(token_store, "_load_from_db", return_value=({"refresh_token": "rt_from_db"}, True))
    def test_db_takes_priority_over_env(self, mock_db):
        """DB has tokens -> env vars are NOT consulted."""
        with patch.dict(os.environ, {"JOBBER_REFRESH_TOKEN": "rt_env"}, clear=True):
            result = token_store.load_tokens("jobber")
            self.assertEqual(result["refresh_token"], "rt_from_db")

    @patch.object(token_store, "_load_from_db", return_value=(None, True))
    def test_returns_empty_dict_when_nothing_available(self, mock_db):
        """No DB, no JSON, no env vars -> returns empty dict."""
        with patch.dict(os.environ, {}, clear=True):
            result = token_store.load_tokens("jobber", "/nonexistent/path.json")
            self.assertEqual(result, {})


if __name__ == "__main__":
    unittest.main()
