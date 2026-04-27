"""
Verify the token_preflight Jobber refresh guard.

When JOBBER_TOKEN_KEEPER_ENABLED=1, the preflight must NOT call
auth.jobber_auth._refresh_token (that would race against the token-keeper
worker and break the rotating refresh-token chain). Instead it should
return a TokenCheck that signals the syncer should skip this cycle.
"""
import os
import unittest
from unittest.mock import patch

from demo.hardening import token_preflight


class TestPreflightRespectsKeeperMode(unittest.TestCase):

    def test_skips_refresh_when_keeper_mode_enabled(self):
        """JOBBER_TOKEN_KEEPER_ENABLED=1 -> no refresh attempt, returns 'expired'."""
        with patch.dict(os.environ, {"JOBBER_TOKEN_KEEPER_ENABLED": "1"}, clear=True):
            with patch("auth.jobber_auth._refresh_token") as mock_refresh:
                result = token_preflight._jobber_try_refresh(
                    {"refresh_token": "rt_present"}
                )
                mock_refresh.assert_not_called()
        self.assertEqual(result.tool_name, "Jobber")
        self.assertEqual(result.status, "expired")
        self.assertIn("token-keeper", result.message.lower())

    def test_refresh_runs_when_flag_unset(self):
        """JOBBER_TOKEN_KEEPER_ENABLED unset -> legacy mode, refresh is attempted."""
        with patch.dict(os.environ, {}, clear=True):
            with patch("auth.jobber_auth._refresh_token", return_value={
                "access_token": "at_new",
                "refresh_token": "rt_new",
                "expires_in": 3600,
                "expires_at": 9999999999,
            }) as mock_refresh:
                with patch("auth.jobber_auth._save_tokens"):
                    result = token_preflight._jobber_try_refresh(
                        {"refresh_token": "rt_present"}
                    )
                mock_refresh.assert_called_once_with("rt_present")
        self.assertEqual(result.status, "ok")


if __name__ == "__main__":
    unittest.main()
