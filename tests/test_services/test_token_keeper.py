"""
Tests for services.token_keeper diagnostic enrichment and alert throttling.

Covers the three diagnosis-blocker fixes:
  1. _refresh_token captures HTTP status, Content-Type, and body excerpt before
     parsing JSON, and includes them in the raised exception.
  2. _do_refresh throttles repeated alerts so a sustained outage does not flood
     #automation-failure with one CRITICAL message per minute.
  3. Errors are classified as transient (likely Jobber maintenance / 5xx /
     network) vs chain-broken (4xx, auth) so the operator can tell at a glance
     whether to wait or to re-bootstrap.
"""
import os
import unittest
from unittest.mock import MagicMock, patch

import requests as req

from services import token_keeper


def _http_response(status: int, body: str, content_type: str = "application/json") -> MagicMock:
    """Build a mock requests.Response that mimics real status / headers / body."""
    resp = MagicMock(spec=req.Response)
    resp.status_code = status
    resp.text = body
    resp.headers = {"Content-Type": content_type}
    if "json" in content_type.lower() and body.strip():
        import json as _json
        resp.json.return_value = _json.loads(body)
    else:
        resp.json.side_effect = ValueError("Expecting value: line 1 column 1 (char 0)")
    return resp


class TestRefreshTokenDiagnostics(unittest.TestCase):
    """_refresh_token must surface HTTP status / Content-Type / body excerpt."""

    def setUp(self):
        self._env = patch.dict(
            os.environ,
            {"JOBBER_CLIENT_ID": "cid", "JOBBER_CLIENT_SECRET": "csec"},
            clear=False,
        )
        self._env.start()

    def tearDown(self):
        self._env.stop()

    def test_2xx_with_empty_body_includes_status_and_content_type(self):
        """200 + empty body: error must mention HTTP 200 and Content-Type."""
        with patch("services.token_keeper.requests.post",
                   return_value=_http_response(200, "", "text/html")):
            with self.assertRaises(token_keeper.JobberRefreshFailure) as ctx:
                token_keeper._refresh_token("rt_ok")
        msg = str(ctx.exception)
        self.assertIn("HTTP 200", msg)
        self.assertIn("text/html", msg)
        self.assertTrue(ctx.exception.transient,
                        "200 + non-JSON should classify as transient")

    def test_2xx_with_html_body_includes_body_excerpt(self):
        """200 + HTML maintenance page: error must include body excerpt."""
        html = "<html><body>We'll be back soon - scheduled maintenance</body></html>"
        with patch("services.token_keeper.requests.post",
                   return_value=_http_response(200, html, "text/html")):
            with self.assertRaises(token_keeper.JobberRefreshFailure) as ctx:
                token_keeper._refresh_token("rt_ok")
        self.assertIn("maintenance", str(ctx.exception).lower())
        self.assertEqual(ctx.exception.status, 200)

    def test_400_invalid_grant_classified_as_chain_broken(self):
        """400 invalid_grant: error must say HTTP 400 and be non-transient."""
        body = '{"error":"invalid_grant","error_description":"Token expired"}'
        with patch("services.token_keeper.requests.post",
                   return_value=_http_response(400, body)):
            with self.assertRaises(token_keeper.JobberRefreshFailure) as ctx:
                token_keeper._refresh_token("rt_ok")
        self.assertIn("HTTP 400", str(ctx.exception))
        self.assertIn("invalid_grant", str(ctx.exception))
        self.assertFalse(ctx.exception.transient,
                         "4xx must classify as chain-broken")

    def test_5xx_server_error_classified_as_transient(self):
        """503 from Jobber: transient (their problem, not ours)."""
        with patch("services.token_keeper.requests.post",
                   return_value=_http_response(503, "Service Unavailable", "text/plain")):
            with self.assertRaises(token_keeper.JobberRefreshFailure) as ctx:
                token_keeper._refresh_token("rt_ok")
        self.assertIn("HTTP 503", str(ctx.exception))
        self.assertTrue(ctx.exception.transient)

    def test_network_error_classified_as_transient(self):
        """ConnectionError: transient."""
        with patch("services.token_keeper.requests.post",
                   side_effect=req.ConnectionError("DNS lookup failed")):
            with self.assertRaises(token_keeper.JobberRefreshFailure) as ctx:
                token_keeper._refresh_token("rt_ok")
        self.assertTrue(ctx.exception.transient)

    def test_200_valid_json_returns_data_with_expires_at(self):
        """Happy path: 200 + valid JSON returns parsed dict with expires_at set."""
        body = '{"access_token":"new_at","refresh_token":"new_rt","expires_in":3600}'
        with patch("services.token_keeper.requests.post",
                   return_value=_http_response(200, body)):
            data = token_keeper._refresh_token("rt_ok")
        self.assertEqual(data["access_token"], "new_at")
        self.assertIn("expires_at", data)


class TestDoRefreshAlertThrottling(unittest.TestCase):
    """_do_refresh must throttle alerts and surface classification."""

    def _keeper_with_failure(self, exc: Exception) -> token_keeper.TokenKeeper:
        keeper = token_keeper.TokenKeeper()
        # Patches are applied by the caller via context managers; we just build
        # the bare instance here.
        return keeper

    def test_alerts_at_failure_3_then_throttles_until_15(self):
        """Failures 1-2: silent. Failure 3: alert. Failures 4-14: silent."""
        keeper = token_keeper.TokenKeeper()
        exc = token_keeper.JobberRefreshFailure(
            "Jobber refresh returned HTTP 200 with non-JSON body",
            transient=True, status=200,
        )
        with patch("services.token_keeper._load_jobber_tokens",
                   return_value={"refresh_token": "rt"}):
            with patch("services.token_keeper._refresh_token", side_effect=exc):
                with patch("services.token_keeper._alert_slack") as mock_alert:
                    for _ in range(14):
                        keeper._do_refresh()
        self.assertEqual(mock_alert.call_count, 1,
                         "Should alert once at failure #3, not on every tick")
        self.assertEqual(keeper._consecutive_failures, 14)

    def test_alerts_at_each_staircase_step(self):
        """Across 60 failures, alerts fire at 3, 15, 60 = 3 alerts total."""
        keeper = token_keeper.TokenKeeper()
        exc = token_keeper.JobberRefreshFailure(
            "Jobber refresh returned HTTP 200 with non-JSON body",
            transient=True, status=200,
        )
        with patch("services.token_keeper._load_jobber_tokens",
                   return_value={"refresh_token": "rt"}):
            with patch("services.token_keeper._refresh_token", side_effect=exc):
                with patch("services.token_keeper._alert_slack") as mock_alert:
                    for _ in range(60):
                        keeper._do_refresh()
        self.assertEqual(mock_alert.call_count, 3,
                         "Expected alerts at staircase steps 3, 15, 60")

    def test_transient_alert_uses_warning_severity_and_mentions_status(self):
        """Transient errors should label as WARNING and mention status page."""
        keeper = token_keeper.TokenKeeper()
        exc = token_keeper.JobberRefreshFailure(
            "Jobber refresh returned HTTP 200 with non-JSON body (Content-Type='text/html')",
            transient=True, status=200, content_type="text/html",
        )
        with patch("services.token_keeper._load_jobber_tokens",
                   return_value={"refresh_token": "rt"}):
            with patch("services.token_keeper._refresh_token", side_effect=exc):
                with patch("services.token_keeper._alert_slack") as mock_alert:
                    for _ in range(3):
                        keeper._do_refresh()
        self.assertEqual(mock_alert.call_count, 1)
        message = mock_alert.call_args[0][0]
        kwargs = mock_alert.call_args.kwargs
        self.assertIn("WARNING", message)
        self.assertIn("status", message.lower())  # mentions status / status page
        self.assertEqual(kwargs.get("severity"), "warning")

    def test_chain_broken_alert_uses_critical_severity(self):
        """4xx errors should label as CRITICAL and recommend re-authenticate."""
        keeper = token_keeper.TokenKeeper()
        exc = token_keeper.JobberRefreshFailure(
            "Jobber refresh returned HTTP 400: invalid_grant",
            transient=False, status=400,
        )
        with patch("services.token_keeper._load_jobber_tokens",
                   return_value={"refresh_token": "rt"}):
            with patch("services.token_keeper._refresh_token", side_effect=exc):
                with patch("services.token_keeper._alert_slack") as mock_alert:
                    for _ in range(3):
                        keeper._do_refresh()
        message = mock_alert.call_args[0][0]
        kwargs = mock_alert.call_args.kwargs
        self.assertIn("CRITICAL", message)
        self.assertIn("re-authenticate", message.lower())
        self.assertEqual(kwargs.get("severity"), "critical")

    def test_recovery_resets_failure_count_and_alerts_recovery(self):
        """After failures, a successful refresh resets the count and alerts recovery."""
        keeper = token_keeper.TokenKeeper()
        keeper._consecutive_failures = 5

        good_tokens = {
            "access_token": "at_new",
            "refresh_token": "rt_new",
            "expires_in": 3600,
            "expires_at": 9999999999,
        }
        with patch("services.token_keeper._load_jobber_tokens",
                   return_value={"refresh_token": "rt_old"}):
            with patch("services.token_keeper._refresh_token",
                       return_value=good_tokens):
                with patch("services.token_keeper._save_jobber_tokens"):
                    with patch("services.token_keeper._alert_slack") as mock_alert:
                        result = keeper._do_refresh()

        self.assertTrue(result)
        self.assertEqual(keeper._consecutive_failures, 0)
        self.assertEqual(mock_alert.call_count, 1,
                         "Should post a recovery message after a streak of >=3 failures")
        self.assertIn("recovered", mock_alert.call_args[0][0].lower())

    def test_recovery_silent_if_no_prior_alerts(self):
        """If failures never crossed alert threshold, recovery is silent."""
        keeper = token_keeper.TokenKeeper()
        keeper._consecutive_failures = 1  # below threshold

        good_tokens = {
            "access_token": "at_new",
            "refresh_token": "rt_new",
            "expires_in": 3600,
            "expires_at": 9999999999,
        }
        with patch("services.token_keeper._load_jobber_tokens",
                   return_value={"refresh_token": "rt_old"}):
            with patch("services.token_keeper._refresh_token",
                       return_value=good_tokens):
                with patch("services.token_keeper._save_jobber_tokens"):
                    with patch("services.token_keeper._alert_slack") as mock_alert:
                        keeper._do_refresh()

        self.assertEqual(mock_alert.call_count, 0)


if __name__ == "__main__":
    unittest.main()
