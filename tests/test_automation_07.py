"""
tests/test_automation_07.py

Automation 7 — Sales Research & Outreach Agent Chain
Tests covering template logic, agent fallbacks, orchestrator flow, and
end-to-end smoke test.

Unit tests (1-12):   no API calls, no DB.
Integration (13-20): require live PostgreSQL + APIs.
                     Set RUN_INTEGRATION=1 to enable.

Run unit tests only:
    python tests/test_automation_07.py
    python -m pytest tests/test_automation_07.py -v -k "not integration"

Run with integration tests:
    RUN_INTEGRATION=1 python tests/test_automation_07.py
    RUN_INTEGRATION=1 python -m pytest tests/test_automation_07.py -v
"""

from __future__ import annotations

import io
import logging
import os
import sys
import unittest
import unittest.mock

# ── Path wiring ────────────────────────────────────────────────────────────────
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(_PROJECT_ROOT, ".env"))
except ImportError:
    pass

# ── Integration gate ───────────────────────────────────────────────────────────
_RUN_INTEGRATION = bool(os.getenv("RUN_INTEGRATION"))


def _integration(cls):
    """Class decorator: skip the entire class unless RUN_INTEGRATION is set."""
    return unittest.skipUnless(
        _RUN_INTEGRATION,
        "Skipping integration tests — set RUN_INTEGRATION=1 to enable",
    )(cls)


# ══════════════════════════════════════════════════════════════════════════════
# UNIT TESTS — lead_source_openers.py  (tests 1–5)
# ══════════════════════════════════════════════════════════════════════════════

class TestLeadSourceOpeners(unittest.TestCase):
    """Tests 1–5: get_opener() for each lead-source scenario."""

    def setUp(self):
        from automations.templates.lead_source_openers import get_opener, LEAD_SOURCE_OPENERS
        self.get_opener = get_opener
        self.default = LEAD_SOURCE_OPENERS[None]

    # 1
    def test_lead_source_opener_organic(self):
        """1. ORGANIC_SEARCH returns the expected opener line."""
        result = self.get_opener("ORGANIC_SEARCH")
        self.assertEqual(result, "Thanks for finding us online")

    # 2
    def test_lead_source_opener_referral_with_name(self):
        """2. REFERRAL + referrer_name includes the referrer's name in the opener."""
        result = self.get_opener("REFERRAL", referrer_name="John Smith")
        self.assertIn("John Smith", result)

    # 3
    def test_lead_source_opener_referral_no_name(self):
        """3. REFERRAL with no referrer_name falls back to a generic referred message."""
        result = self.get_opener("REFERRAL", referrer_name=None)
        self.assertIn("referred", result.lower())
        # The raw format-string placeholder must not leak into the output
        self.assertNotIn("{referrer_name}", result)

    # 4
    def test_lead_source_opener_unknown(self):
        """4. Unrecognised source returns the same opener as None (default)."""
        result = self.get_opener("TOTALLY_UNKNOWN_SOURCE_XYZ")
        self.assertEqual(result, self.default)

    # 5
    def test_lead_source_opener_none(self):
        """5. None as lead_source returns the default opener."""
        result = self.get_opener(None)
        self.assertEqual(result, self.default)


# ══════════════════════════════════════════════════════════════════════════════
# UNIT TESTS — template_selector.py  (tests 6–9)
# ══════════════════════════════════════════════════════════════════════════════

class TestTemplateSelector(unittest.TestCase):
    """Tests 6–9: select_template() returns correct set and variant."""

    def setUp(self):
        from automations.templates.template_selector import select_template
        self.select = select_template

    # 6
    def test_template_selector_residential_high_high(self):
        """6. residential + both high → set='residential', variant in {A, B}."""
        template_set, variant = self.select("residential", "high", "high")
        self.assertEqual(template_set, "residential")
        self.assertIn(variant, ("A", "B"),
                      f"Expected A or B for high/high; got {variant!r}")

    # 7
    def test_template_selector_commercial_high_medium(self):
        """7. commercial + high research + medium match → set='commercial', variant='B'."""
        template_set, variant = self.select("commercial", "high", "medium")
        self.assertEqual(template_set, "commercial")
        self.assertEqual(variant, "B")

    # 8
    def test_template_selector_missing_type_low_low(self):
        """8. Missing/unknown contact type + both low → set='hybrid', variant='C'."""
        template_set, variant = self.select("", "low", "low")
        self.assertEqual(template_set, "hybrid")
        self.assertEqual(variant, "C")

    # 9
    def test_template_selector_randomization(self):
        """9. With both high, 20 calls must produce at least one A and one B."""
        variants_seen: set[str] = set()
        for _ in range(20):
            _, v = self.select("residential", "high", "high")
            variants_seen.add(v)
        self.assertIn("A", variants_seen,
                      "Variant A never appeared in 20 high/high calls")
        self.assertIn("B", variants_seen,
                      "Variant B never appeared in 20 high/high calls")


# ══════════════════════════════════════════════════════════════════════════════
# UNIT TESTS — signatures.py  (tests 10–12)
# ══════════════════════════════════════════════════════════════════════════════

class TestSignatures(unittest.TestCase):
    """Tests 10–12: get_signature() returns correct block per template set."""

    def setUp(self):
        from automations.templates.signatures import (
            get_signature,
            RESIDENTIAL_SIGNATURE,
            COMMERCIAL_SIGNATURE,
        )
        self.get_signature = get_signature
        self.residential = RESIDENTIAL_SIGNATURE
        self.commercial = COMMERCIAL_SIGNATURE

    # 10
    def test_signature_residential(self):
        """10. Residential signature uses first name only; no 'Owner', no email."""
        sig = self.get_signature("residential")
        self.assertIn("Maria", sig)
        self.assertNotIn("Owner", sig,
                         "Residential signature must not include 'Owner' title")
        self.assertNotIn("@", sig,
                         "Residential signature must not include an email address")

    # 11
    def test_signature_commercial(self):
        """11. Commercial signature has full name, 'Owner' title, and email address."""
        sig = self.get_signature("commercial")
        self.assertIn("Maria Gonzalez", sig)
        self.assertIn("Owner", sig)
        self.assertIn("@", sig)

    # 12
    def test_signature_hybrid(self):
        """12. Hybrid template_set returns the commercial signature (professional close)."""
        sig = self.get_signature("hybrid")
        self.assertEqual(sig, self.commercial,
                         "Hybrid should return the COMMERCIAL_SIGNATURE verbatim")


# ══════════════════════════════════════════════════════════════════════════════
# INTEGRATION TESTS  (tests 13–20)
# Gate: @unittest.skipUnless(os.getenv("RUN_INTEGRATION"), ...)
# ══════════════════════════════════════════════════════════════════════════════

@_integration
class TestSimilarJobsIntegration(unittest.TestCase):
    """Tests 13–14: Agent 2 SQL query against live PostgreSQL."""

    # 13
    def test_similar_jobs_query_runs(self):
        """13. Agent 2 SQL executes against PostgreSQL without error and returns a list."""
        from database.connection import get_connection
        from automations.agents.similar_jobs_agent import _SIMILARITY_SQL

        contact = {
            "service_interest": "biweekly_recurring",
            "contact_type":     "residential",
            "neighborhood":     "Westlake",
            "address":          "2401 Westlake Dr",
        }
        crew_zone = "West Austin"
        params = (
            contact["service_interest"],
            contact["contact_type"],
            contact["contact_type"],
            contact["neighborhood"],
            crew_zone,
        )

        conn = get_connection()
        try:
            cursor = conn.execute(_SIMILARITY_SQL, params)
            rows = [dict(r) for r in cursor.fetchall()]
        finally:
            conn.close()

        self.assertIsInstance(rows, list,
                              "SQL query must return a list (even if empty)")

    # 14
    def test_similar_jobs_no_address_fallback(self):
        """14. Agent 2 with empty address runs without error; result has required keys."""
        from automations.agents.similar_jobs_agent import find_similar_jobs

        contact = {
            "service_interest": "biweekly_recurring",
            "contact_type":     "residential",
            "neighborhood":     "",
            "address":          "",
        }

        # Let the SQL run live; mock only the Sonnet formatting step
        mock_msg = unittest.mock.MagicMock()
        mock_msg.content = [unittest.mock.MagicMock(text="[]", type="text")]

        with unittest.mock.patch(
            "automations.agents.similar_jobs_agent.anthropic.Anthropic"
        ) as mock_cls:
            mock_client = unittest.mock.MagicMock()
            mock_cls.return_value = mock_client
            mock_client.messages.create.return_value = mock_msg

            result = find_similar_jobs(contact)

        self.assertIn("matches",          result)
        self.assertIn("match_confidence", result)
        self.assertIsInstance(result["matches"], list)


@_integration
class TestResearchAgentIntegration(unittest.TestCase):
    """Tests 15–16: Agent 1 (research_agent) with live Anthropic API."""

    # 15
    def test_research_agent_commercial(self):
        """15. Commercial contact returns valid JSON dict with research_confidence."""
        from automations.agents.research_agent import research_lead

        contact = {
            "firstname":    "Medical",
            "lastname":     "Group",
            "company":      "Barton Creek Medical Group",
            "address":      "3801 Bee Cave Rd",
            "city":         "Austin",
            "contact_type": "commercial",
        }
        result = research_lead(contact)

        self.assertIsInstance(result, dict)
        self.assertIn("research_confidence", result)
        self.assertIn(
            result["research_confidence"], ("high", "medium", "low"),
            f"research_confidence must be high/medium/low; got {result['research_confidence']!r}",
        )

    # 16
    def test_research_agent_residential(self):
        """16. Residential contact returns a dict with a neighborhood_context key."""
        from automations.agents.research_agent import research_lead

        contact = {
            "firstname":    "Jane",
            "lastname":     "Smith",
            "company":      "",
            "address":      "4800 Bull Creek Rd",
            "city":         "Austin",
            "contact_type": "residential",
        }
        result = research_lead(contact)

        self.assertIsInstance(result, dict)
        self.assertIn("neighborhood_context", result,
                      "residential research must include 'neighborhood_context'")
        self.assertIsInstance(result["neighborhood_context"], str)


@_integration
class TestEmailSynthesisIntegration(unittest.TestCase):
    """Tests 17–18: Agent 3 (email_synthesis_agent) with live Anthropic API."""

    _CONTACT = {
        "firstname":                  "Priya",
        "lastname":                   "Nair",
        "email":                      "priya.nair@example.com",
        "company":                    "",
        "contact_type":               "residential",
        "lead_source":                "REFERRAL",
        "hs_analytics_source":        "REFERRAL",
        "hs_analytics_source_data_1": "John Smith",
        "service_interest":           "biweekly_recurring",
    }
    _RESEARCH = {
        "contact_type_inferred": "residential",
        "business_type":         None,
        "business_details":      "",
        "neighborhood_context":  "Bull Creek — established residential neighbourhood.",
        "notable_details":       "",
        "research_confidence":   "medium",
        "raw_summary":           "Priya Nair appears to be a homeowner in the Bull Creek area.",
    }
    _JOBS = {
        "matches": [
            {
                "job_id":          "SS-JOB-0001",
                "description":     "Biweekly residential clean in Westlake, completed last month.",
                "neighborhood":    "Westlake",
                "service_type_id": "biweekly_recurring",
                "similarity_score": 80,
            }
        ],
        "match_confidence":       "high",
        "estimated_annual_value": 3_600.0,
    }

    @classmethod
    def setUpClass(cls):
        """Call synthesize_email once; reuse result in both tests to save API cost."""
        from automations.agents.email_synthesis_agent import synthesize_email
        cls._result = synthesize_email(cls._CONTACT, cls._RESEARCH, cls._JOBS)

    # 17
    def test_email_synthesis_returns_valid_json(self):
        """17. Agent 3 returns a dict containing all five required keys."""
        if self._result is None:
            self.skipTest("synthesize_email returned None — Anthropic API may be unavailable")

        self.assertIsInstance(self._result, dict)
        for key in ("subject", "body", "template_set_used", "variant_used", "word_count"):
            self.assertIn(key, self._result, f"Required key missing from result: {key!r}")

    # 18
    def test_email_synthesis_respects_word_limit(self):
        """18. word_count ≤ 150 for variants A/B and ≤ 100 for variant C."""
        if self._result is None:
            self.skipTest("synthesize_email returned None — Anthropic API may be unavailable")

        variant    = self._result.get("variant_used")
        word_count = self._result.get("word_count", 0)

        if variant in ("A", "B"):
            self.assertLessEqual(
                word_count, 150,
                f"Variant {variant} word_count {word_count} exceeds 150-word limit",
            )
        elif variant == "C":
            self.assertLessEqual(
                word_count, 100,
                f"Variant C word_count {word_count} exceeds 100-word limit",
            )
        else:
            self.fail(f"Unexpected variant value: {variant!r}")


@_integration
class TestGmailDraftIntegration(unittest.TestCase):
    """Test 19: create_gmail_draft() against the live Gmail API."""

    # 19
    def test_gmail_draft_creation(self):
        """19. Returns dict with draft_id and gmail_link; link contains the draft_id."""
        from automations.helpers.gmail_draft import create_gmail_draft

        result = create_gmail_draft(
            to_email="maria@sparkleshineaustin.com",
            subject="[TEST] Automation 07 draft creation smoke test",
            body_text=(
                "This is an automated test draft created by test_automation_07.py.\n"
                "Safe to delete."
            ),
        )

        self.assertIsNotNone(
            result,
            "create_gmail_draft returned None — Google OAuth may not be configured",
        )
        self.assertIn("draft_id",   result)
        self.assertIn("gmail_link", result)
        self.assertIn(
            result["draft_id"],
            result["gmail_link"],
            "gmail_link must contain the draft_id for deep-linking",
        )


@_integration
class TestFullChainDryRun(unittest.TestCase):
    """Test 20: orchestrator dry-run logs all expected steps, no real API calls."""

    # 20
    def test_full_chain_dry_run(self):
        """20. dry_run=True logs [DRY RUN] markers for each step; no API calls made."""
        from unittest.mock import MagicMock, patch
        from database.connection import get_connection
        from automations.automation_07_sales_outreach import SalesOutreachAutomation

        _FAKE_CONTACT = {
            "hubspot_id":                 "dry-test-001",
            "firstname":                  "Test",
            "lastname":                   "Lead",
            "email":                      "testlead@example.com",
            "company":                    "",
            "address":                    "123 Test St",
            "city":                       "Austin",
            "contact_type":               "residential",
            "lead_source":                "ORGANIC_SEARCH",
            "hs_analytics_source":        "ORGANIC_SEARCH",
            "hs_analytics_source_data_1": "",
            "service_interest":           "biweekly_recurring",
            "lifecyclestage":             "lead",
        }

        # Capture log output from the automation_07 logger
        log_stream = io.StringIO()
        handler = logging.StreamHandler(log_stream)
        handler.setLevel(logging.DEBUG)
        auto_logger = logging.getLogger("automation_07")
        auto_logger.addHandler(handler)
        orig_level = auto_logger.level
        auto_logger.setLevel(logging.DEBUG)

        db = get_connection()
        try:
            mock_clients = MagicMock()

            with patch.object(
                SalesOutreachAutomation,
                "_read_watermark",
                return_value=0,
            ), patch.object(
                SalesOutreachAutomation,
                "_fetch_new_contacts",
                return_value=([_FAKE_CONTACT], 1_000),
            ):
                automation = SalesOutreachAutomation(
                    clients=mock_clients,
                    db=db,
                    dry_run=True,
                )
                automation.run()
        finally:
            db.close()
            auto_logger.removeHandler(handler)
            auto_logger.setLevel(orig_level)

        output = log_stream.getvalue()

        self.assertIn("[DRY RUN]", output,
                      "[DRY RUN] marker must appear in log output")
        self.assertIn("Agent 1", output,
                      "Expected 'Agent 1' step logged in dry-run output")
        self.assertIn("Agent 3", output,
                      "Expected 'Agent 3' step logged in dry-run output")
        self.assertIn("Gmail draft", output,
                      "Expected 'Gmail draft' step logged in dry-run output")


# ══════════════════════════════════════════════════════════════════════════════
# __main__ entry point — run tests and print a numbered summary
# ══════════════════════════════════════════════════════════════════════════════

_TEST_MANIFEST = [
    # (category, number, test_method_name)
    ("Unit", " 1", "test_lead_source_opener_organic"),
    ("Unit", " 2", "test_lead_source_opener_referral_with_name"),
    ("Unit", " 3", "test_lead_source_opener_referral_no_name"),
    ("Unit", " 4", "test_lead_source_opener_unknown"),
    ("Unit", " 5", "test_lead_source_opener_none"),
    ("Unit", " 6", "test_template_selector_residential_high_high"),
    ("Unit", " 7", "test_template_selector_commercial_high_medium"),
    ("Unit", " 8", "test_template_selector_missing_type_low_low"),
    ("Unit", " 9", "test_template_selector_randomization"),
    ("Unit", "10", "test_signature_residential"),
    ("Unit", "11", "test_signature_commercial"),
    ("Unit", "12", "test_signature_hybrid"),
    ("Intg", "13", "test_similar_jobs_query_runs"),
    ("Intg", "14", "test_similar_jobs_no_address_fallback"),
    ("Intg", "15", "test_research_agent_commercial"),
    ("Intg", "16", "test_research_agent_residential"),
    ("Intg", "17", "test_email_synthesis_returns_valid_json"),
    ("Intg", "18", "test_email_synthesis_respects_word_limit"),
    ("Intg", "19", "test_gmail_draft_creation"),
    ("Intg", "20", "test_full_chain_dry_run"),
]


class _TrackingResult(unittest.TextTestResult):
    """TextTestResult that records per-test status for the summary block."""

    def __init__(self, stream, descriptions, verbosity):
        super().__init__(stream, descriptions, verbosity)
        self._status: dict[str, str] = {}

    def addSuccess(self, test):
        super().addSuccess(test)
        self._status[test._testMethodName] = "[PASS]"

    def addFailure(self, test, err):
        super().addFailure(test, err)
        self._status[test._testMethodName] = "[FAIL]"

    def addError(self, test, err):
        super().addError(test, err)
        self._status[test._testMethodName] = "[FAIL]"

    def addSkip(self, test, reason):
        super().addSkip(test, reason)
        self._status[test._testMethodName] = "[SKIP]"

    def addExpectedFailure(self, test, err):
        super().addExpectedFailure(test, err)
        self._status[test._testMethodName] = "[PASS]"

    def addUnexpectedSuccess(self, test):
        super().addUnexpectedSuccess(test)
        self._status[test._testMethodName] = "[FAIL]"


if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite  = loader.loadTestsFromModule(sys.modules[__name__])

    runner = unittest.TextTestRunner(
        resultclass=_TrackingResult,
        verbosity=2,
    )
    result = runner.run(suite)

    # ── Numbered summary ──────────────────────────────────────────────────────
    print()
    print("=" * 65)
    print("  Automation 07 Test Summary  (20 tests)")
    print("=" * 65)

    for cat, num, name in _TEST_MANIFEST:
        marker = result._status.get(name, "[ ? ]")
        print(f"  {marker} {num}. [{cat}] {name}")

    print("─" * 65)
    skipped = len(result.skipped)
    failed  = len(result.failures) + len(result.errors)
    passed  = result.testsRun - failed - skipped
    print(
        f"  Ran {result.testsRun} tests: "
        f"{passed} passed, {failed} failed, {skipped} skipped"
    )
    if not _RUN_INTEGRATION:
        intg_count = sum(1 for c, _, _ in _TEST_MANIFEST if c == "Intg")
        print(
            f"  ({intg_count} integration tests skipped — "
            "set RUN_INTEGRATION=1 to enable)"
        )
    print("=" * 65)

    sys.exit(0 if result.wasSuccessful() else 1)
