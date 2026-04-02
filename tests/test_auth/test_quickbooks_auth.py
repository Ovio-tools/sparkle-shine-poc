"""Tests for QuickBooks auth fallback behavior."""
import os
import unittest
from unittest.mock import patch, MagicMock

import sys
sys.modules.setdefault("database", MagicMock())
sys.modules.setdefault("database.connection", MagicMock())

from auth import quickbooks_auth


class TestGetQuickbooksTokenFallback(unittest.TestCase):
    """Test graceful fallback when token refresh fails."""

    @patch.object(quickbooks_auth, "_load_tokens", return_value={})
    def test_raises_runtime_error_when_no_tokens(self, mock_load):
        """No tokens anywhere -> clear RuntimeError, not EnvironmentError."""
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError) as ctx:
                quickbooks_auth.get_quickbooks_token()
            self.assertIn("QuickBooks", str(ctx.exception))
            self.assertIn("QBO_REFRESH_TOKEN", str(ctx.exception))

    @patch.object(quickbooks_auth, "_load_tokens", return_value={"refresh_token": "dead"})
    @patch.object(quickbooks_auth, "_refresh_token", side_effect=Exception("401 Unauthorized"))
    def test_falls_back_to_access_token_env_var(self, mock_refresh, mock_load):
        """Refresh fails + QBO_ACCESS_TOKEN set -> returns it with warning."""
        with patch.dict(os.environ, {"QBO_ACCESS_TOKEN": "at_stale"}, clear=True):
            token = quickbooks_auth.get_quickbooks_token()
            self.assertEqual(token, "at_stale")

    @patch.object(quickbooks_auth, "_load_tokens", return_value={"refresh_token": "dead"})
    @patch.object(quickbooks_auth, "_refresh_token", side_effect=Exception("401 Unauthorized"))
    def test_raises_clear_error_when_refresh_fails_no_fallback(self, mock_refresh, mock_load):
        """Refresh fails + no QBO_ACCESS_TOKEN -> clear RuntimeError."""
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError) as ctx:
                quickbooks_auth.get_quickbooks_token()
            self.assertIn("refresh", str(ctx.exception).lower())

    @patch.object(quickbooks_auth, "_load_tokens", return_value={
        "access_token": "at_valid", "expires_at": 9999999999, "refresh_token": "rt_ok"
    })
    def test_returns_valid_unexpired_token(self, mock_load):
        """Valid unexpired token -> returns it without refresh."""
        token = quickbooks_auth.get_quickbooks_token()
        self.assertEqual(token, "at_valid")


if __name__ == "__main__":
    unittest.main()
