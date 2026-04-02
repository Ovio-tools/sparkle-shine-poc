"""Tests for Google auth refresh condition and Railway fallback."""
import os
import unittest
from unittest.mock import patch, MagicMock, PropertyMock

import sys
sys.modules.setdefault("database", MagicMock())
sys.modules.setdefault("database.connection", MagicMock())

from google.oauth2.credentials import Credentials

from auth import google_auth


class TestRefreshCondition(unittest.TestCase):
    """Test the refresh condition covers both bootstrap and normal paths."""

    @patch.object(google_auth, "token_store")
    @patch("auth.google_auth.Request")
    def test_refreshes_when_no_access_token_but_has_refresh(self, mock_request, mock_store):
        """Env var bootstrap: refresh_token present, no access token -> should refresh."""
        mock_store.load_tokens.return_value = {"refresh_token": "rt_env"}
        mock_store.save_tokens = MagicMock()

        # Create a real Credentials object with just a refresh token
        mock_creds = MagicMock(spec=Credentials)
        mock_creds.token = None
        mock_creds.valid = False
        mock_creds.expired = False  # No token to be expired
        mock_creds.refresh_token = "rt_env"

        with patch.object(google_auth, "_build_creds_from_dict", return_value=mock_creds):
            with patch.object(mock_creds, "to_json", return_value='{"token": "new", "refresh_token": "rt_env"}'):
                google_auth.get_google_credentials()

        # Should have called refresh, not the browser flow
        mock_creds.refresh.assert_called_once()

    @patch.object(google_auth, "token_store")
    def test_skips_refresh_when_token_valid(self, mock_store):
        """Valid unexpired token -> no refresh, return immediately."""
        mock_store.load_tokens.return_value = {"refresh_token": "rt", "token": "at_valid"}
        mock_store.save_tokens = MagicMock()

        mock_creds = MagicMock(spec=Credentials)
        mock_creds.token = "at_valid"
        mock_creds.valid = True
        mock_creds.expired = False
        mock_creds.refresh_token = "rt"

        with patch.object(google_auth, "_build_creds_from_dict", return_value=mock_creds):
            result = google_auth.get_google_credentials()

        mock_creds.refresh.assert_not_called()
        self.assertEqual(result, mock_creds)


class TestMissingCredentialsJson(unittest.TestCase):
    """Test that missing credentials.json raises clear error, not FileNotFoundError."""

    @patch.object(google_auth, "token_store")
    @patch("auth.google_auth.Request")
    def test_clear_error_when_no_credentials_json_and_no_env(self, mock_request, mock_store):
        """No credentials.json, no env vars -> RuntimeError with guidance."""
        mock_store.load_tokens.return_value = {}

        with patch.object(google_auth, "_build_creds_from_dict", return_value=None):
            with self.assertRaises(RuntimeError) as ctx:
                google_auth.get_google_credentials()
            self.assertIn("GOOGLE_REFRESH_TOKEN", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
