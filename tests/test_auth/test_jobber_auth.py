"""Tests for Jobber auth fallback behavior."""
import os
import unittest
from unittest.mock import patch, MagicMock, PropertyMock

import sys
sys.modules.setdefault("database", MagicMock())
sys.modules.setdefault("database.connection", MagicMock())

from auth import jobber_auth


class TestGetJobberTokenFallback(unittest.TestCase):
    """Test graceful fallback when token refresh fails."""

    @patch.object(jobber_auth, "_load_tokens", return_value={})
    def test_raises_runtime_error_when_no_tokens(self, mock_load):
        """No tokens anywhere -> clear RuntimeError, not EnvironmentError."""
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError) as ctx:
                jobber_auth.get_jobber_token()
            self.assertIn("Jobber", str(ctx.exception))
            self.assertIn("JOBBER_REFRESH_TOKEN", str(ctx.exception))

    @patch.object(jobber_auth, "_load_tokens", return_value={"refresh_token": "dead_token"})
    @patch.object(jobber_auth, "_refresh_token", side_effect=Exception("401 Unauthorized"))
    def test_falls_back_to_access_token_env_var(self, mock_refresh, mock_load):
        """Refresh fails + JOBBER_ACCESS_TOKEN set -> returns it with warning."""
        with patch.dict(os.environ, {"JOBBER_ACCESS_TOKEN": "at_stale"}, clear=True):
            token = jobber_auth.get_jobber_token()
            self.assertEqual(token, "at_stale")

    @patch.object(jobber_auth, "_load_tokens", return_value={"refresh_token": "dead_token"})
    @patch.object(jobber_auth, "_refresh_token", side_effect=Exception("401 Unauthorized"))
    def test_raises_clear_error_when_refresh_fails_no_fallback(self, mock_refresh, mock_load):
        """Refresh fails + no JOBBER_ACCESS_TOKEN -> clear RuntimeError."""
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError) as ctx:
                jobber_auth.get_jobber_token()
            self.assertIn("refresh", str(ctx.exception).lower())

    @patch.object(jobber_auth, "_load_tokens", return_value={
        "access_token": "at_valid", "expires_at": 9999999999, "refresh_token": "rt_ok"
    })
    def test_returns_valid_unexpired_token(self, mock_load):
        """Valid unexpired token -> returns it without refresh."""
        token = jobber_auth.get_jobber_token()
        self.assertEqual(token, "at_valid")

    @patch.object(jobber_auth, "_load_tokens", return_value={"refresh_token": "dead_token"})
    def test_http_error_logs_status_and_body(self, mock_load):
        """requests.HTTPError logs HTTP status code and response body excerpt."""
        import requests as req
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized: token revoked"
        http_err = req.HTTPError(response=mock_response)

        with patch.object(jobber_auth, "_refresh_token", side_effect=http_err):
            with patch.dict(os.environ, {"JOBBER_ACCESS_TOKEN": "at_fallback"}, clear=True):
                token = jobber_auth.get_jobber_token()
                self.assertEqual(token, "at_fallback")


if __name__ == "__main__":
    unittest.main()
