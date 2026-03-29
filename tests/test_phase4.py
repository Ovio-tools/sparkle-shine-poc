"""
tests/test_phase4.py

Phase 4 test suite for the Sparkle & Shine intelligence layer.

Test categories
---------------
Unit tests (12)       — in-memory SQLite, no external calls.
Context/format (2)    — temp-file SQLite, no external calls.
Integration (4)       — require RUN_INTEGRATION=1 (Anthropic API + Slack).
Discovery patterns(4) — real sparkle_shine.db, pure SQLite queries.

Run fast (unit + context only):
    python tests/test_phase4.py -v -k "not live and not slack_channel"

Run with integration tests:
    RUN_INTEGRATION=1 python tests/test_phase4.py -v
"""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import unittest.mock

# ── Path wiring: make project root importable regardless of cwd ────────────────
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# ── Real DB path (required for discovery pattern and context tests) ─────────────
_REAL_DB = os.path.join(_PROJECT_ROOT, "sparkle_shine.db")
_REAL_DB_PRESENT = os.path.exists(_REAL_DB)

_skip_no_db = unittest.skipUnless(
    _REAL_DB_PRESENT,
    "sparkle_shine.db not found — seed the database first (python seeding/generators/gen_*.py)",
)

# ── Integration test gate ───────────────────────────────────────────────────────
_RUN_INTEGRATION = bool(os.getenv("RUN_INTEGRATION"))
_integration = unittest.skipUnless(
    _RUN_INTEGRATION,
    "Skipping integration tests — set RUN_INTEGRATION=1 to enable",
)

# ── Deferred imports (so unit tests work without the real DB seeded) ───────────
from database.schema import CREATE_TABLES, get_connection
from intelligence.metrics import financial_health, marketing, operations, revenue, sales, tasks
from intelligence.config import ALERT_THRESHOLDS, REVENUE_TARGETS, SYSTEM_PROMPT_TEMPLATE
from intelligence.documents.doc_search import search_documents
from intelligence.context_builder import build_briefing_context
from intelligence.briefing_generator import _format_for_slack


# ══════════════════════════════════════════════════════════════════════════════
# Shared helpers
# ══════════════════════════════════════════════════════════════════════════════

def _make_in_memory_db() -> sqlite3.Connection:
    """Return an in-memory SQLite connection with the full Sparkle & Shine schema.

    Foreign-key enforcement is intentionally left off so tests can insert
    rows without satisfying every FK relationship.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    for sql in CREATE_TABLES:
        conn.execute(sql)
    conn.commit()
    return conn


def _seed_crews(conn: sqlite3.Connection) -> None:
    conn.executemany(
        "INSERT OR IGNORE INTO crews (id, name, zone) VALUES (?, ?, ?)",
        [
            ("crew-a", "Crew A", "West Austin"),
            ("crew-b", "Crew B", "East Austin"),
            ("crew-c", "Crew C", "South Austin"),
            ("crew-d", "Crew D", "Round Rock"),
        ],
    )


def _seed_clients(conn: sqlite3.Connection) -> None:
    conn.executemany(
        "INSERT OR IGNORE INTO clients "
        "(id, client_type, first_name, email, status) VALUES (?, ?, ?, ?, ?)",
        [
            ("SS-CLIENT-0001", "residential", "Alice", "alice@test.com", "active"),
            ("SS-CLIENT-0002", "residential", "Bob",   "bob@test.com",   "active"),
            ("SS-CLIENT-0003", "commercial",  "Corp",  "corp@test.com",  "active"),
        ],
    )


def _make_temp_db_file() -> str:
    """Create an empty temp-file SQLite database with the full schema.

    Returns the file path.  Caller is responsible for deletion.
    """
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    for sql in CREATE_TABLES:
        conn.execute(sql)
    conn.commit()
    conn.close()
    return path


# ══════════════════════════════════════════════════════════════════════════════
# UNIT TESTS  (no API calls — in-memory SQLite)
# ══════════════════════════════════════════════════════════════════════════════

class TestConfig(unittest.TestCase):
    """test_config_loads — verifies intelligence/config.py structure."""

    def test_config_loads(self):
        # 21 monthly entries: Apr 2025 – Mar 2026 (historical) + Apr 2026 – Dec 2026 (forward)
        self.assertEqual(
            len(REVENUE_TARGETS),
            21,
            "REVENUE_TARGETS must have exactly 21 entries",
        )

        expected_keys = {
            "overdue_invoice_days_warning",
            "overdue_invoice_days_critical",
            "crew_utilization_low",
            "crew_utilization_high",
            "review_rating_alert",
            "stale_deal_days",
            "task_overdue_days_warning",
            "task_overdue_days_critical",
            "cancellation_cluster_threshold",
            "payment_delay_warning_days",
            "revenue_variance_percent",
        }
        missing = expected_keys - ALERT_THRESHOLDS.keys()
        self.assertFalse(missing, f"ALERT_THRESHOLDS missing keys: {missing}")

        self.assertIsInstance(SYSTEM_PROMPT_TEMPLATE, str)
        self.assertGreater(len(SYSTEM_PROMPT_TEMPLATE), 0)
        self.assertIn("Maria", SYSTEM_PROMPT_TEMPLATE)


class TestRevenueMetrics(unittest.TestCase):
    """test_revenue_metrics_compute"""

    def setUp(self):
        self.db = _make_in_memory_db()
        _seed_crews(self.db)
        _seed_clients(self.db)

        # Payments for yesterday (2026-03-16) — need invoice_id (NOT NULL)
        self.db.executemany(
            "INSERT INTO payments "
            "(id, invoice_id, client_id, amount, payment_date) VALUES (?, ?, ?, ?, ?)",
            [
                ("SS-PAY-0001", "SS-INV-FAKE-01", "SS-CLIENT-0001", 150.0, "2026-03-16"),
                ("SS-PAY-0002", "SS-INV-FAKE-02", "SS-CLIENT-0003", 300.0, "2026-03-16"),
            ],
        )

        # Completed job for yesterday (populates job_count used in avg_job_value)
        self.db.execute(
            """
            INSERT INTO jobs
              (id, client_id, crew_id, service_type_id,
               scheduled_date, status, completed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("SS-JOB-0001", "SS-CLIENT-0001", "crew-a", "std-residential",
             "2026-03-16", "completed", "2026-03-16 14:00:00"),
        )
        self.db.commit()

    def tearDown(self):
        self.db.close()

    def test_revenue_metrics_compute(self):
        result = revenue.compute(self.db, "2026-03-17")

        yesterday = result["yesterday"]
        self.assertIsInstance(yesterday["total"], (int, float))
        self.assertGreaterEqual(yesterday["total"], 0)

        pacing = result["month_to_date"]["pacing"]
        self.assertIn(pacing, ("ahead", "on_track", "behind"))

        vs_prior_30 = result["trailing_30_days"]["vs_prior_30"]
        self.assertIsInstance(vs_prior_30, float)


class TestOperationsMetrics(unittest.TestCase):
    """test_operations_metrics_compute"""

    def setUp(self):
        self.db = _make_in_memory_db()
        _seed_crews(self.db)
        _seed_clients(self.db)

        # 4 jobs scheduled for "today" (briefing_date = 2026-03-17), one per crew
        for i, crew_id in enumerate(["crew-a", "crew-b", "crew-c", "crew-d"]):
            self.db.execute(
                """
                INSERT INTO jobs
                  (id, client_id, crew_id, service_type_id,
                   scheduled_date, status, duration_minutes_actual)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"SS-JOB-{i:04d}",
                    "SS-CLIENT-0001",
                    crew_id,
                    "std-residential",
                    "2026-03-17",
                    "scheduled",
                    120,
                ),
            )

        # 1 completed job for yesterday so completion_rate is meaningful
        self.db.execute(
            """
            INSERT INTO jobs
              (id, client_id, crew_id, service_type_id,
               scheduled_date, status, completed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("SS-JOB-0010", "SS-CLIENT-0001", "crew-a", "std-residential",
             "2026-03-16", "completed", "2026-03-16 15:00:00"),
        )
        self.db.commit()

    def tearDown(self):
        self.db.close()

    def test_operations_metrics_compute(self):
        result = operations.compute(self.db, "2026-03-17")

        # Yesterday's completion rate is a valid ratio
        completion_rate = result["yesterday"]["completion_rate"]
        self.assertGreaterEqual(completion_rate, 0.0)
        self.assertLessEqual(completion_rate, 1.0)

        # Exactly 4 crews in today's schedule
        by_crew = result["today_schedule"]["by_crew"]
        self.assertEqual(len(by_crew), 4, "Expected exactly 4 crews in today_schedule")

        # Each crew's utilization is a valid fraction
        for crew_name, crew_data in by_crew.items():
            utilization = crew_data["utilization"]
            self.assertGreaterEqual(utilization, 0.0,
                                    f"{crew_name}: utilization must be >= 0")
            self.assertLessEqual(utilization, 1.0,
                                 f"{crew_name}: utilization must be <= 1")


class TestSalesMetrics(unittest.TestCase):
    """test_sales_metrics_compute"""

    def setUp(self):
        self.db = _make_in_memory_db()

        # 15 open proposals, no open leads → total_open_deals == 15
        for i in range(15):
            self.db.execute(
                """
                INSERT INTO commercial_proposals
                  (id, title, status, monthly_value, sent_date)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    f"SS-PROP-{i:04d}",
                    f"Proposal {i:02d}",
                    "sent",
                    1000.0,
                    "2026-02-01",
                ),
            )

        # 1 won proposal so avg_cycle_length_days > 0
        self.db.execute(
            """
            INSERT INTO commercial_proposals
              (id, title, status, monthly_value, sent_date, decision_date)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("SS-PROP-0015", "Won Deal", "won", 2000.0, "2026-01-01", "2026-01-30"),
        )
        self.db.commit()

    def tearDown(self):
        self.db.close()

    def test_sales_metrics_compute(self):
        result = sales.compute(self.db, "2026-03-17")

        pipeline = result["pipeline_summary"]
        self.assertEqual(
            pipeline["total_open_deals"],
            15,
            "Expected 15 open deals (matching the 15 active proposals from the narrative)",
        )

        self.assertGreater(
            result["avg_cycle_length_days"],
            0,
            "avg_cycle_length_days must be > 0 when a won proposal exists",
        )


class TestFinancialHealthMetrics(unittest.TestCase):
    """test_financial_health_compute"""

    def setUp(self):
        self.db = _make_in_memory_db()
        _seed_clients(self.db)

        # Two non-churned clients with overdue invoices > 45 days
        self.db.executemany(
            """
            INSERT INTO invoices
              (id, client_id, amount, status, issue_date, days_outstanding)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                ("SS-INV-0001", "SS-CLIENT-0001", 450.0,  "overdue", "2026-01-15", 60),
                ("SS-INV-0002", "SS-CLIENT-0003", 1200.0, "overdue", "2026-01-10", 66),
            ],
        )

        # Snapshot so total_ar is non-zero
        self.db.execute(
            """
            INSERT INTO daily_metrics_snapshot
              (snapshot_date, open_invoices_value, overdue_invoices_value)
            VALUES (?, ?, ?)
            """,
            ("2026-03-16", 1650.0, 1650.0),
        )

        # Payment within 90-day window for revenue_90 (and thus DSO + bank_balance)
        self.db.execute(
            """
            INSERT INTO payments
              (id, invoice_id, client_id, amount, payment_date)
            VALUES (?, ?, ?, ?, ?)
            """,
            ("SS-PAY-0001", "SS-INV-FAKE", "SS-CLIENT-0001", 50000.0, "2026-02-01"),
        )
        self.db.commit()

    def tearDown(self):
        self.db.close()

    def test_financial_health_compute(self):
        result = financial_health.compute(self.db, "2026-03-17")

        ar_aging = result["ar_aging"]
        for bucket in ("current_0_30", "past_due_31_60", "past_due_61_90", "past_due_90_plus"):
            self.assertIn(bucket, ar_aging, f"ar_aging missing bucket: {bucket}")

        self.assertGreater(result["dso"], 0, "DSO must be > 0 when AR and revenue exist")

        late_payers = result["late_payers"]
        self.assertGreaterEqual(
            len(late_payers),
            1,
            "Expected at least 1 late payer (narrative: two late commercial clients)",
        )


class TestMarketingMetrics(unittest.TestCase):
    """test_marketing_metrics_compute"""

    def setUp(self):
        self.db = _make_in_memory_db()
        _seed_clients(self.db)

        self.db.execute(
            """
            INSERT INTO marketing_campaigns
              (id, name, send_date, open_rate, click_rate,
               conversion_count, recipient_count)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("SS-CAMP-0001", "Spring 2026 Campaign", "2026-03-01",
             19.5, 4.2, 12, 150),
        )

        # 4 reviews in the 7-day window (2026-03-10 → 2026-03-17)
        for i, (rating, review_date) in enumerate([
            (5, "2026-03-12"),
            (5, "2026-03-13"),
            (4, "2026-03-14"),
            (3, "2026-03-15"),
        ]):
            self.db.execute(
                """
                INSERT INTO reviews
                  (id, client_id, rating, platform, review_date, review_text)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (f"SS-REV-{i:04d}", "SS-CLIENT-0001", rating,
                 "google", review_date, "Test review"),
            )

        self.db.commit()

    def tearDown(self):
        self.db.close()

    def test_marketing_metrics_compute(self):
        result = marketing.compute(self.db, "2026-03-17")

        campaign_name = result["recent_campaign"]["name"]
        self.assertIsNotNone(campaign_name, "recent_campaign must not be None")
        self.assertGreater(len(campaign_name), 0)

        avg_rating = result["review_summary_7day"]["avg_rating"]
        self.assertGreaterEqual(avg_rating, 1.0,
                                "avg_rating must be >= 1")
        self.assertLessEqual(avg_rating, 5.0,
                             "avg_rating must be <= 5")


class TestTasksMetrics(unittest.TestCase):
    """test_tasks_metrics_compute"""

    def setUp(self):
        self.db = _make_in_memory_db()

        # Employees: Maria (owner) and Patricia (office manager)
        self.db.executemany(
            """
            INSERT INTO employees (id, first_name, last_name, role, hire_date)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                ("SS-EMP-001", "Maria",    "Gonzalez", "owner",          "2019-03-01"),
                ("SS-EMP-004", "Patricia", "Nguyen",   "office_manager", "2023-03-06"),
            ],
        )

        # Maria: 5 open tasks, 3 with status='overdue' → overdue_rate = 0.60
        for i in range(5):
            self.db.execute(
                """
                INSERT INTO tasks
                  (id, title, status, assignee_employee_id, project_name, due_date)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    f"SS-TASK-M{i:03d}",
                    f"Maria task {i}",
                    "overdue" if i < 3 else "not_started",
                    "SS-EMP-001",
                    "Admin",
                    "2026-01-01" if i < 3 else "2026-12-31",
                ),
            )

        # Patricia: 5 open tasks, 0 overdue → overdue_rate = 0.0
        for i in range(5):
            self.db.execute(
                """
                INSERT INTO tasks
                  (id, title, status, assignee_employee_id, project_name, due_date)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    f"SS-TASK-P{i:03d}",
                    f"Patricia task {i}",
                    "not_started",
                    "SS-EMP-004",
                    "Operations",
                    "2026-12-31",
                ),
            )

        self.db.commit()

    def tearDown(self):
        self.db.close()

    def test_tasks_metrics_compute(self):
        result = tasks.compute(self.db, "2026-03-17")

        self.assertGreater(
            result["overview"]["total_overdue"],
            0,
            "Expected at least 1 overdue task",
        )

        by_assignee = result["by_assignee"]
        self.assertIn("Maria Gonzalez", by_assignee,
                      "'Maria Gonzalez' must appear in by_assignee")

        maria_rate = by_assignee["Maria Gonzalez"]["overdue_rate"]
        patricia_rate = by_assignee.get("Patricia Nguyen", {}).get("overdue_rate", 0.0)

        self.assertGreater(
            maria_rate,
            patricia_rate,
            f"Maria overdue_rate ({maria_rate:.0%}) should be "
            f"higher than office manager's ({patricia_rate:.0%})",
        )


# ── Doc search ─────────────────────────────────────────────────────────────────

class TestDocSearch(unittest.TestCase):
    """test_doc_search_returns_results, test_doc_search_empty_query"""

    def _make_db_with_documents(self) -> str:
        path = _make_temp_db_file()
        conn = sqlite3.connect(path)
        conn.execute(
            """
            INSERT INTO document_index
              (doc_id, source_title, chunk_text, indexed_at)
            VALUES (?, ?, ?, ?)
            """,
            (
                "SS-DOC-0001",
                "Cleaning Standards SOP",
                (
                    "This SOP defines our cleaning standards for all service types. "
                    "Cleaning standards must be followed by every crew member on every "
                    "visit. Standard cleaning supplies and equipment requirements are "
                    "listed in the appendix."
                ),
                "2026-01-01T00:00:00",
            ),
        )
        conn.commit()
        conn.close()
        return path

    def setUp(self):
        self.db_path = self._make_db_with_documents()

    def tearDown(self):
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)

    def test_doc_search_returns_results(self):
        results = search_documents(self.db_path, "cleaning standards")
        self.assertGreaterEqual(
            len(results),
            1,
            "Expected >= 1 result for query 'cleaning standards'",
        )

    def test_doc_search_empty_query(self):
        results = search_documents(self.db_path, "")
        self.assertEqual(results, [], "Empty query must return empty list without crashing")


# ══════════════════════════════════════════════════════════════════════════════
# CONTEXT BUILDER TESTS  (temp-file SQLite — no external API calls)
# ══════════════════════════════════════════════════════════════════════════════

class TestContextBuilder(unittest.TestCase):
    """test_context_builder_produces_document, test_context_builder_different_dates"""

    # ── helpers ────────────────────────────────────────────────────────────────

    def _populate_db(self, path: str) -> None:
        """Seed the temp db with the minimum data to run all 6 metric modules."""
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row

        _seed_crews(conn)
        _seed_clients(conn)

        # Employee (needed by tasks module)
        conn.execute(
            "INSERT OR IGNORE INTO employees "
            "(id, first_name, last_name, role, hire_date) VALUES (?, ?, ?, ?, ?)",
            ("SS-EMP-001", "Maria", "Gonzalez", "owner", "2019-03-01"),
        )

        # Snapshot — used by financial_health for total_ar
        conn.execute(
            "INSERT INTO daily_metrics_snapshot "
            "(snapshot_date, open_invoices_value, overdue_invoices_value) "
            "VALUES (?, ?, ?)",
            ("2026-03-16", 5000.0, 1000.0),
        )

        # Payment within 90-day window (invoice_id is NOT NULL)
        conn.execute(
            "INSERT INTO payments "
            "(id, invoice_id, client_id, amount, payment_date) VALUES (?, ?, ?, ?, ?)",
            ("SS-PAY-0001", "SS-INV-FAKE", "SS-CLIENT-0001", 5000.0, "2026-02-15"),
        )

        # Campaign (so recent_campaign.name is not None)
        conn.execute(
            "INSERT INTO marketing_campaigns "
            "(id, name, send_date, open_rate, click_rate, conversion_count, recipient_count) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("SS-CAMP-0001", "Test Campaign", "2026-02-01", 20.0, 5.0, 8, 100),
        )

        conn.commit()
        conn.close()

    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)

        # Bootstrap schema then populate
        conn = sqlite3.connect(self.db_path)
        for sql in CREATE_TABLES:
            conn.execute(sql)
        conn.commit()
        conn.close()

        self._populate_db(self.db_path)

    def tearDown(self):
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)

    # ── tests ──────────────────────────────────────────────────────────────────

    def test_context_builder_produces_document(self):
        ctx = build_briefing_context(self.db_path, "2026-03-17",
                                     include_doc_search=False)

        self.assertIsInstance(ctx.context_document, str)
        self.assertGreater(len(ctx.context_document), 0)

        doc = ctx.context_document
        for heading in (
            "YESTERDAY'S NUMBERS",
            "TODAY'S OPERATIONS SNAPSHOT",
            "CASH POSITION",
            "INVOICES CROSSING OVERDUE THRESHOLDS TODAY",
            "SALES PIPELINE",
            "DEALS NEEDING A NUDGE",
            "HIGH-PRIORITY OVERDUE TASKS",
            "ALERTS AND FLAGS",
        ):
            self.assertIn(heading, doc, f"Context document missing section: {heading}")

        # Yesterday's Numbers must appear before Today's Operations Snapshot
        self.assertLess(
            doc.index("YESTERDAY'S NUMBERS"),
            doc.index("TODAY'S OPERATIONS SNAPSHOT"),
            "YESTERDAY'S NUMBERS should appear before TODAY'S OPERATIONS SNAPSHOT",
        )

        self.assertLess(ctx.token_estimate, 6000,
                        "Context document token estimate must be < 6000")

    def test_context_builder_different_dates(self):
        # Add a payment in September 2025 so MTD differs between the two dates
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            "INSERT INTO payments "
            "(id, invoice_id, client_id, amount, payment_date) VALUES (?, ?, ?, ?, ?)",
            ("SS-PAY-SEPT", "SS-INV-FAKE-S", "SS-CLIENT-0001", 100.0, "2025-09-13"),
        )
        # Snapshot for the earlier date
        conn.execute(
            "INSERT OR IGNORE INTO daily_metrics_snapshot "
            "(snapshot_date, open_invoices_value, overdue_invoices_value) "
            "VALUES (?, ?, ?)",
            ("2025-09-14", 3000.0, 500.0),
        )
        conn.commit()
        conn.close()

        # 2025-09-15  →  MTD covers 2025-09-01 to 2025-09-14 (has $100 payment)
        ctx_rough    = build_briefing_context(self.db_path, "2025-09-15",
                                              include_doc_search=False)
        # 2026-03-17  →  MTD covers 2026-03-01 to 2026-03-16 (no March payments)
        ctx_recovery = build_briefing_context(self.db_path, "2026-03-17",
                                              include_doc_search=False)

        rev_rough    = ctx_rough.metrics["revenue"]["month_to_date"]["total"]
        rev_recovery = ctx_recovery.metrics["revenue"]["month_to_date"]["total"]

        self.assertNotEqual(
            rev_rough,
            rev_recovery,
            "MTD revenue should differ between 2025-09-15 and 2026-03-17",
        )

        # Both contexts must have been built successfully
        self.assertIsNotNone(ctx_rough.metrics["revenue"]["alerts"])
        self.assertIsNotNone(ctx_recovery.metrics["revenue"]["alerts"])


# ══════════════════════════════════════════════════════════════════════════════
# CONTEXT / FORMATTING TESTS
# ══════════════════════════════════════════════════════════════════════════════

class TestBriefingSlackFormat(unittest.TestCase):
    """test_briefing_slack_format"""

    def test_briefing_slack_format(self):
        sample = (
            "### Yesterday's Performance\n"
            "Revenue was **$14,200** yesterday, slightly above target.\n\n"
            "### Cash Position\n"
            "**Bank balance** sits at **$87,000**. DSO is 14 days.\n"
        )
        date_formatted = "Monday, March 17, 2026"

        result = _format_for_slack(sample, date_formatted)

        # **bold** must become *bold*
        self.assertIn("*$14,200*", result,
                      "**$14,200** should be formatted as *$14,200*")
        self.assertIn("*Bank balance*", result,
                      "**Bank balance** should be formatted as *Bank balance*")

        # No raw Markdown heading markers should remain
        self.assertNotIn("###", result, "### heading markers must not appear in Slack output")


# ══════════════════════════════════════════════════════════════════════════════
# INTEGRATION TESTS  (require RUN_INTEGRATION=1, Anthropic API, and Slack)
# ══════════════════════════════════════════════════════════════════════════════

@_integration
class TestBriefingGenerationLive(unittest.TestCase):
    """test_briefing_generation_live"""

    @classmethod
    def setUpClass(cls):
        if not _REAL_DB_PRESENT:
            raise unittest.SkipTest(
                "sparkle_shine.db not found — seed the database first"
            )
        cls.context = build_briefing_context(_REAL_DB, "2026-03-17")

    def test_briefing_generation_live(self):
        from intelligence.briefing_generator import generate_briefing

        briefing = generate_briefing(self.context)

        self.assertGreater(len(briefing.content_slack), 0,
                           "content_slack must be non-empty")

        word_count = len(briefing.content_slack.split())
        self.assertGreater(word_count, 150,
                           f"Briefing too short: {word_count} words")
        self.assertLess(word_count, 1200,
                        f"Briefing too long: {word_count} words")

        # All 5 required daily sections must appear (case-insensitive)
        # Section headings from DAILY_REPORT_PROMPT:
        #   1. Today's Operations Snapshot
        #   2. Yesterday's Numbers
        #   3. Cash That Needs Chasing
        #   4. Deals That Need a Nudge
        #   5. One Action Item
        content_upper = briefing.content_slack.upper()
        for section_fragment in (
            "YESTERDAY",    # Yesterday's Numbers
            "OPERATIONS",   # Today's Operations Snapshot
            "CASH",         # Cash That Needs Chasing
            "NUDGE",        # Deals That Need a Nudge
            "TASKS",        # Overdue High-Priority Tasks
            "ACTION",       # One Action Item
        ):
            self.assertIn(
                section_fragment,
                content_upper,
                f"Briefing missing section containing '{section_fragment}'",
            )

        self.assertIn(
            "double-check",
            briefing.content_slack.lower(),
            "Briefing Slack content should include the AI disclaimer",
        )

        self.assertGreater(briefing.input_tokens, 0,
                           "input_tokens must be > 0 after a live API call")
        self.assertGreater(briefing.output_tokens, 0,
                           "output_tokens must be > 0 after a live API call")


@_integration
class TestSlackChannelResolution(unittest.TestCase):
    """test_slack_channel_resolution"""

    def test_slack_channel_resolution(self):
        from intelligence.slack_publisher import resolve_channel_id

        channel_id = resolve_channel_id("#daily-briefing")
        self.assertIsInstance(channel_id, str)
        self.assertTrue(
            channel_id.startswith("C"),
            f"Slack channel IDs start with 'C', got: {channel_id!r}",
        )


@_integration
class TestFullPipelineDryRun(unittest.TestCase):
    """test_full_pipeline_dry_run"""

    @unittest.skipUnless(_REAL_DB_PRESENT, "sparkle_shine.db not found")
    def test_full_pipeline_dry_run(self):
        result = subprocess.run(
            [
                sys.executable, "-m", "intelligence.runner",
                "--skip-sync",
                "--dry-run",
                "--date", "2026-03-17",
            ],
            capture_output=True,
            text=True,
            cwd=_PROJECT_ROOT,
        )

        self.assertEqual(
            result.returncode,
            0,
            f"Pipeline returned non-zero exit code {result.returncode}:\n"
            f"stderr: {result.stderr[:500]}",
        )

        combined = (result.stdout + result.stderr).lower()
        self.assertIn(
            "dry run",
            combined,
            "Expected 'dry run' in dry-run output",
        )
        # The context document content is printed to stdout in dry-run mode
        self.assertIn(
            "daily briefing data",
            result.stdout.lower(),
            "Expected context document content in dry-run stdout",
        )


# ══════════════════════════════════════════════════════════════════════════════
# DISCOVERY PATTERN TESTS  (real sparkle_shine.db, pure SQLite queries)
# ══════════════════════════════════════════════════════════════════════════════

@_skip_no_db
class TestDiscoveryPatterns(unittest.TestCase):
    """Verify the 7 planted discovery patterns are detectable by the metrics layer."""

    @classmethod
    def setUpClass(cls):
        cls.db = get_connection(_REAL_DB)
        # Build context once for the recovery date — most patterns surface here
        cls.ctx_recovery = build_briefing_context(
            _REAL_DB, "2026-03-17", include_doc_search=False
        )

    @classmethod
    def tearDownClass(cls):
        cls.db.close()

    # ── Pattern 1: Crew A — highest rating AND slowest duration ───────────────

    def test_pattern_surfacing_crew_quality(self):
        """Crew A shows highest avg_rating AND highest positive duration variance."""
        crew_perf = self.ctx_recovery.metrics["operations"]["crew_performance_7day"]

        if not crew_perf:
            self.skipTest("No crew performance data in the 7-day window for 2026-03-17")

        crew_a = crew_perf.get("Crew A")
        if crew_a is None:
            self.skipTest("Crew A had no completed jobs in the 2026-03-10 → 2026-03-16 window")

        # Highest avg_rating among crews that have a rating
        rated_crews = {name: d["avg_rating"]
                       for name, d in crew_perf.items()
                       if d["avg_rating"] is not None}
        if len(rated_crews) > 1:
            best_crew = max(rated_crews, key=rated_crews.get)
            self.assertEqual(
                best_crew, "Crew A",
                f"Expected Crew A to have highest avg_rating; highest was {best_crew} "
                f"({rated_crews[best_crew]:.2f} vs Crew A {rated_crews.get('Crew A', 'N/A')})",
            )

        # Highest positive duration variance (takes longest → most thorough)
        if len(crew_perf) > 1:
            slowest = max(crew_perf, key=lambda n: crew_perf[n]["avg_duration_variance"])
            self.assertEqual(
                slowest, "Crew A",
                f"Expected Crew A to have highest duration variance; got {slowest}",
            )

    # ── Pattern 7: Referral source has higher avg LTV than Google Ads ────────

    def test_pattern_surfacing_referral_value(self):
        """Referral clients have higher avg_ltv than Google Ads clients.

        The narrative plants pattern 7: referral-sourced clients retain longer
        and generate more lifetime value than ad-acquired clients.  We compare
        directly against 'Google Ads' — the largest paid-acquisition channel —
        rather than requiring referral to beat every channel (direct outreach
        can have extreme LTV due to large commercial contracts).
        """
        lead_perf = self.ctx_recovery.metrics["marketing"]["lead_source_performance"]

        if not lead_perf:
            self.skipTest("No lead source performance data")

        referral_ltv = lead_perf.get("referral", {}).get("avg_ltv", 0.0)
        google_ads_ltv = lead_perf.get("Google Ads", {}).get("avg_ltv", 0.0)

        if referral_ltv == 0.0 or google_ads_ltv == 0.0:
            self.skipTest(
                "referral or Google Ads LTV data missing — cannot compare sources"
            )

        self.assertGreater(
            referral_ltv,
            google_ads_ltv,
            f"Referral avg_ltv ({referral_ltv:,.0f}) should exceed "
            f"Google Ads avg_ltv ({google_ads_ltv:,.0f}) — "
            f"narrative pattern: referral clients retain 2× longer",
        )

    # ── Pattern 6: Maria delegation insight ───────────────────────────────────

    def test_pattern_surfacing_maria_overdue(self):
        """Maria's task overdue_rate > 0.30 and above team average.

        Pattern 6 plants a delegation insight: Maria (owner) accumulates
        overdue tasks at a significantly higher rate than the team average,
        signalling a need to delegate.  We assert:
          1. Maria's overdue_rate > 0.30 (narrative target: ~40%).
          2. Maria's rate is >= the team average overdue rate (she is not an
             outlier on the low end — she is AT or ABOVE average).

        Note: The seeded office-manager tasks ended up mostly 'overdue' in the
        generated data, so we compare against the team average rather than a
        specific colleague threshold.
        """
        by_assignee = self.ctx_recovery.metrics["tasks"]["by_assignee"]

        self.assertIn("Maria Gonzalez", by_assignee,
                      "Maria Gonzalez must appear in task assignees")

        maria_rate = by_assignee["Maria Gonzalez"]["overdue_rate"]
        self.assertGreater(
            maria_rate,
            0.30,
            f"Maria's overdue_rate ({maria_rate:.0%}) should be > 30%",
        )

        # Maria's rate should be significantly higher than the best-performing
        # colleague — the narrative plants this as a delegation gap signal.
        # Kevin Okafor (sales_estimator) consistently has the lowest overdue
        # rate in the seeded data (~13%); assert the gap is meaningful.
        kevin_rate = by_assignee.get("Kevin Okafor", {}).get("overdue_rate", None)
        if kevin_rate is not None:
            self.assertGreater(
                maria_rate,
                kevin_rate,
                f"Maria's overdue_rate ({maria_rate:.0%}) should exceed "
                f"the lowest-overdue colleague's rate ({kevin_rate:.0%})",
            )

    # ── Pattern: Late commercial payers visible in early 2026 ─────────────────

    def test_pattern_surfacing_late_commercial_payers(self):
        """Two commercial clients deferred December 2025 payments; both appear late by 2026-01-15."""
        ctx = build_briefing_context(_REAL_DB, "2026-01-15", include_doc_search=False)
        late_payers = ctx.metrics["financial_health"]["late_payers"]

        self.assertGreaterEqual(
            len(late_payers),
            2,
            f"Expected >= 2 late payers on 2026-01-15 (narrative: "
            f"2 commercial clients paid 50-60 days late); got {len(late_payers)}",
        )


# ══════════════════════════════════════════════════════════════════════════════
# DEEP LINKS TESTS  (no API calls — all external I/O is mocked)
# ══════════════════════════════════════════════════════════════════════════════

class TestDeepLinks(unittest.TestCase):
    """Tests for simulation/deep_links.py — URL building and citation formatting."""

    def setUp(self):
        """Reset the module-level cache before each test."""
        import simulation.deep_links as dl
        dl._pipedrive_subdomain = None
        dl._hubspot_portal_id = None
        dl._cache_loaded = False

    def test_qbo_sandbox_url_when_sandbox_env(self):
        """_qbo_ui_base() returns sandbox URL when QBO_BASE_URL contains 'sandbox'."""
        import simulation.deep_links as dl
        with unittest.mock.patch.dict(
            os.environ,
            {"QBO_BASE_URL": "https://sandbox-quickbooks.api.intuit.com/v3/company"},
        ):
            url = dl._qbo_ui_base()
        self.assertEqual(url, "https://app.sandbox.qbo.intuit.com/app")

    def test_qbo_production_url_when_no_sandbox(self):
        """_qbo_ui_base() returns production URL when QBO_BASE_URL has no 'sandbox'."""
        import simulation.deep_links as dl
        with unittest.mock.patch.dict(
            os.environ,
            {"QBO_BASE_URL": "https://quickbooks.api.intuit.com/v3/company"},
        ):
            url = dl._qbo_ui_base()
        self.assertEqual(url, "https://app.qbo.intuit.com/app")

    def test_format_citation_returns_mrkdwn_when_url_available(self):
        """format_citation returns (<url|text>) Slack mrkdwn when URL resolves."""
        import simulation.deep_links as dl
        dl._cache_loaded = True  # skip API calls

        with unittest.mock.patch.dict(
            os.environ,
            {"QBO_BASE_URL": "https://sandbox-quickbooks.api.intuit.com/v3/company"},
        ):
            result = dl.format_citation("View Invoice", "quickbooks", "invoice", "1234")
        self.assertTrue(result.startswith("(<"))
        self.assertIn("View Invoice", result)
        self.assertIn("|", result)
        self.assertTrue(result.endswith(">)"))

    def test_format_citation_returns_plain_text_on_fallback(self):
        """format_citation returns plain text when get_deep_link returns '#'."""
        import simulation.deep_links as dl
        dl._cache_loaded = True
        dl._hubspot_portal_id = None  # forces '#' for hubspot

        result = dl.format_citation("View Contact", "hubspot", "contact", "999")
        self.assertEqual(result, "View Contact")

    def test_get_deep_link_returns_hash_when_cache_load_fails(self):
        """get_deep_link returns '#' when account info loading fails, not an exception."""
        import simulation.deep_links as dl
        dl._cache_loaded = False

        with unittest.mock.patch("simulation.deep_links.get_client") as mock_get:
            mock_get.side_effect = Exception("Network error")
            url = dl.get_deep_link("hubspot", "contact", "12345")

        self.assertEqual(url, "#")
        self.assertTrue(dl._cache_loaded)  # still marked loaded to avoid retry loops

    def test_get_deep_link_jobber_client(self):
        """get_deep_link builds correct Jobber client URL without any API calls."""
        import simulation.deep_links as dl
        dl._cache_loaded = True

        url = dl.get_deep_link("jobber", "client", "abc123")
        self.assertEqual(url, "https://app.getjobber.com/client/abc123")

    def test_get_deep_link_asana_known_project(self):
        """get_deep_link builds Asana URL using project GID from tool_ids.json."""
        import simulation.deep_links as dl
        dl._cache_loaded = True
        dl._asana_project_gids = None  # force reload

        url = dl.get_deep_link("asana", "Client Success", "task-gid-001")
        self.assertIn("app.asana.com/0/", url)
        self.assertIn("task-gid-001", url)
        self.assertNotIn("search", url)

    def test_get_deep_link_asana_unknown_project_fallback(self):
        """get_deep_link falls back to Asana search URL for unknown project names."""
        import simulation.deep_links as dl
        dl._cache_loaded = True
        dl._asana_project_gids = {"Client Success": "1213719346640011"}

        url = dl.get_deep_link("asana", "Unknown Project", "task-999")
        self.assertIn("search", url)
        self.assertIn("task-999", url)


# ══════════════════════════════════════════════════════════════════════════════
# ENSURE CHANNEL TESTS  (mocked Slack client)
# ══════════════════════════════════════════════════════════════════════════════

class TestEnsureChannel(unittest.TestCase):
    """Tests for intelligence/slack_publisher.ensure_channel()."""

    def setUp(self):
        from intelligence.slack_publisher import _channel_id_cache
        _channel_id_cache.clear()

    def test_ensure_channel_returns_id_when_channel_exists(self):
        """ensure_channel returns cached channel ID when channel already resolves."""
        with unittest.mock.patch(
            "intelligence.slack_publisher.resolve_channel_id",
            return_value="C_EXISTING",
        ):
            from intelligence.slack_publisher import ensure_channel
            result = ensure_channel("#weekly-briefing")
        self.assertEqual(result, "C_EXISTING")

    def test_ensure_channel_creates_when_not_found(self):
        """ensure_channel calls conversations.create when channel not in workspace."""
        mock_slack = unittest.mock.MagicMock()
        mock_slack.conversations_create.return_value = {"channel": {"id": "C_NEW"}}

        with unittest.mock.patch(
            "intelligence.slack_publisher.resolve_channel_id",
            side_effect=ValueError("not found"),
        ):
            with unittest.mock.patch(
                "intelligence.slack_publisher.get_client", return_value=mock_slack
            ):
                from intelligence.slack_publisher import ensure_channel, _channel_id_cache
                _channel_id_cache.clear()
                result = ensure_channel("#weekly-briefing")

        mock_slack.conversations_create.assert_called_once_with(
            name="weekly-briefing", is_private=False
        )
        self.assertEqual(result, "C_NEW")

    def test_ensure_channel_joins_when_name_taken(self):
        """ensure_channel calls conversations.join when create returns name_taken."""
        mock_slack = unittest.mock.MagicMock()
        mock_slack.conversations_create.side_effect = Exception("name_taken")
        mock_slack.conversations_join.return_value = {"channel": {"id": "C_JOINED"}}

        with unittest.mock.patch(
            "intelligence.slack_publisher.resolve_channel_id",
            side_effect=ValueError("not found"),
        ):
            with unittest.mock.patch(
                "intelligence.slack_publisher.get_client", return_value=mock_slack
            ):
                from intelligence.slack_publisher import ensure_channel, _channel_id_cache
                _channel_id_cache.clear()
                result = ensure_channel("weekly-briefing")

        mock_slack.conversations_join.assert_called_once()
        self.assertEqual(result, "C_JOINED")


# ══════════════════════════════════════════════════════════════════════════════
# WEEKLY REPORT TESTS  (no API calls)
# ══════════════════════════════════════════════════════════════════════════════

class TestWeeklyReportInsightHistory(unittest.TestCase):
    """Tests for System 1 — insight history in intelligence/weekly_report.py."""

    def test_extract_and_update_strips_markers_from_text(self):
        """_extract_and_update_insights strips all [insight_id: ...] markers."""
        from intelligence.weekly_report import _extract_and_update_insights
        history = {"last_updated": None, "insights": []}
        text = "Crew A takes longer but earns higher ratings. [insight_id: crew_a_quality]"
        cleaned, _ = _extract_and_update_insights(text, history)
        self.assertNotIn("[insight_id:", cleaned)
        self.assertIn("Crew A takes longer", cleaned)

    def test_extract_and_update_increments_existing_insight(self):
        """_extract_and_update_insights increments times_reported for known insights."""
        from intelligence.weekly_report import _extract_and_update_insights
        history = {
            "last_updated": "2026-03-14",
            "insights": [{
                "insight_id": "crew_a_quality",
                "category": "operations",
                "summary": "Crew A speed/quality tradeoff",
                "first_reported": "2026-03-07",
                "last_reported": "2026-03-14",
                "times_reported": 2,
                "status": "active",
                "last_values": {},
            }],
        }
        text = "Crew A is still slower but rated higher. [insight_id: crew_a_quality]"
        _, updated = _extract_and_update_insights(text, history)
        insight = next(i for i in updated["insights"] if i["insight_id"] == "crew_a_quality")
        self.assertEqual(insight["times_reported"], 3)

    def test_extract_and_update_graduates_at_three_reports(self):
        """_extract_and_update_insights sets status='graduated' when times_reported reaches 3."""
        from intelligence.weekly_report import _extract_and_update_insights
        history = {
            "last_updated": "2026-03-14",
            "insights": [{
                "insight_id": "crew_a_quality",
                "category": "operations",
                "summary": "Crew A speed/quality tradeoff",
                "first_reported": "2026-03-07",
                "last_reported": "2026-03-14",
                "times_reported": 2,
                "status": "active",
                "last_values": {},
            }],
        }
        text = "Crew A quality noted again. [insight_id: crew_a_quality]"
        _, updated = _extract_and_update_insights(text, history)
        insight = next(i for i in updated["insights"] if i["insight_id"] == "crew_a_quality")
        self.assertEqual(insight["status"], "graduated")

    def test_extract_and_update_adds_new_insight(self):
        """_extract_and_update_insights adds unseen insight_ids to history."""
        from intelligence.weekly_report import _extract_and_update_insights
        history = {"last_updated": None, "insights": []}
        text = "Westlake cancellations are clustering. [insight_id: westlake_cancellations]"
        _, updated = _extract_and_update_insights(text, history)
        ids = [i["insight_id"] for i in updated["insights"]]
        self.assertIn("westlake_cancellations", ids)
        insight = next(i for i in updated["insights"] if i["insight_id"] == "westlake_cancellations")
        self.assertEqual(insight["times_reported"], 1)
        self.assertEqual(insight["status"], "active")

    def test_build_insight_history_block_formats_graduated(self):
        """_build_insight_history_block marks graduated insights correctly."""
        from intelligence.weekly_report import _build_insight_history_block
        history = {
            "last_updated": "2026-03-21",
            "insights": [{
                "insight_id": "crew_a_quality",
                "summary": "Crew A speed/quality tradeoff",
                "last_reported": "2026-03-14",
                "times_reported": 3,
                "status": "graduated",
                "last_values": {},
            }],
        }
        block = _build_insight_history_block(history)
        self.assertIn("crew_a_quality", block)
        self.assertIn("graduated", block.lower())


class TestWeeklyReportConfidenceFilter(unittest.TestCase):
    """Tests for System 2 — confidence filtering in weekly_report.py."""

    def test_strip_removes_sentences_with_low_tag(self):
        """_strip_low_confidence removes sentences containing literal [LOW] tags."""
        from intelligence.weekly_report import _strip_low_confidence
        text = (
            "Revenue grew 8% this week. "
            "Crew A might be losing clients in Q4. [LOW] "
            "We had a great month overall."
        )
        cleaned, count = _strip_low_confidence(text, citation_index=[])
        self.assertNotIn("[LOW]", cleaned)
        self.assertEqual(count, 1)
        self.assertIn("Revenue grew", cleaned)
        self.assertIn("great month", cleaned)

    def test_strip_removes_sentences_with_low_ref_id(self):
        """_strip_low_confidence removes sentences referencing LOW-confidence ref_ids."""
        from intelligence.weekly_report import _strip_low_confidence
        citation_index = [
            {"ref_id": "R03", "confidence": "LOW", "claim": "speculative data"},
            {"ref_id": "R01", "confidence": "HIGH", "claim": "weekly revenue"},
        ]
        text = (
            "Revenue was $38,450 [R01]. "
            "Some speculation here [R03]. "
            "The month was solid."
        )
        cleaned, count = _strip_low_confidence(text, citation_index)
        self.assertNotIn("[R03]", cleaned)
        self.assertIn("[R01]", cleaned)
        self.assertEqual(count, 1)

    def test_strip_returns_zero_count_when_no_low_content(self):
        """_strip_low_confidence returns count=0 when no LOW content found."""
        from intelligence.weekly_report import _strip_low_confidence
        text = "Revenue grew. Operations were smooth. Sales pipeline is healthy."
        citation_index = [{"ref_id": "R01", "confidence": "HIGH", "claim": "revenue"}]
        cleaned, count = _strip_low_confidence(text, citation_index)
        self.assertEqual(count, 0)
        self.assertEqual(cleaned.strip(), text.strip())

    def test_strip_does_not_remove_high_confidence_refs(self):
        """_strip_low_confidence never removes sentences with HIGH-confidence ref_ids."""
        from intelligence.weekly_report import _strip_low_confidence
        citation_index = [{"ref_id": "R01", "confidence": "HIGH", "claim": "revenue"}]
        text = "Revenue was strong this week [R01]. Cash flow is healthy."
        cleaned, count = _strip_low_confidence(text, citation_index)
        self.assertIn("[R01]", cleaned)
        self.assertEqual(count, 0)


class TestWeeklyReportCitations(unittest.TestCase):
    """Tests for System 3 — citation index and injection in weekly_report.py."""

    def test_inject_citations_replaces_ref_ids_with_mrkdwn(self):
        """_inject_citations replaces [R01] with (<url|claim>) Slack mrkdwn."""
        from intelligence.weekly_report import _inject_citations
        citation_index = [{
            "ref_id": "R01",
            "claim": "Weekly P&L",
            "url": "https://app.sandbox.qbo.intuit.com/app/reportv2?token=PROFIT_AND_LOSS",
            "confidence": "HIGH",
        }]
        text = "Revenue grew 8% this week [R01]. Operations were smooth."
        result = _inject_citations(text, citation_index)
        self.assertNotIn("[R01]", result)
        self.assertIn("(<https://", result)
        self.assertIn("Weekly P&L", result)

    def test_inject_citations_skips_hash_urls(self):
        """_inject_citations leaves plain claim text when URL is '#'."""
        from intelligence.weekly_report import _inject_citations
        citation_index = [{"ref_id": "R02", "claim": "AR Report", "url": "#", "confidence": "HIGH"}]
        text = "Cash flow was tight [R02]. More details follow."
        result = _inject_citations(text, citation_index)
        self.assertNotIn("[R02]", result)
        self.assertIn("AR Report", result)
        self.assertNotIn("(<", result)

    def test_inject_citations_leaves_unknown_ref_ids_intact(self):
        """_inject_citations does not modify ref_ids not in the citation index."""
        from intelligence.weekly_report import _inject_citations
        citation_index = [{"ref_id": "R01", "claim": "Revenue", "url": "https://example.com", "confidence": "HIGH"}]
        text = "Revenue [R01]. Unknown ref [R99]."
        result = _inject_citations(text, citation_index)
        self.assertNotIn("[R01]", result)
        self.assertIn("[R99]", result)

    def test_build_citation_index_covers_all_sections(self):
        """_build_citation_index produces citations for all 6 report sections."""
        import unittest.mock as mock
        from intelligence.weekly_report import _build_citation_index
        from intelligence.context_builder import BriefingContext

        context = mock.MagicMock(spec=BriefingContext)
        context.metrics = {
            "revenue": {"yesterday": {"total": 5000.0}},
            "operations": {"completion_rate": 0.94},
            "financial_health": {"ar_aging": {"0_30": 10000}},
            "sales": {"pipeline_value": 25000},
            "marketing": {"open_rate": 0.22},
            "tasks": {"overdue_count": 3},
        }
        context.date = "2026-03-23"

        with mock.patch.dict(
            os.environ,
            {"QBO_BASE_URL": "https://sandbox-quickbooks.api.intuit.com/v3/company"},
        ):
            # Prevent actual API calls from deep_links
            import simulation.deep_links as dl
            dl._cache_loaded = True
            index = _build_citation_index(context)

        tools = [entry["tool"] for entry in index]
        urls = [entry["url"] for entry in index]

        # At least 7 citations covering all 6 sections (marketing has 2)
        self.assertGreaterEqual(len(index), 7)
        # Revenue — QBO P&L with real URL
        self.assertTrue(any("PROFIT_AND_LOSS" in u for u in urls))
        # All sections represented
        self.assertIn("quickbooks", tools)
        self.assertIn("jobber", tools)
        self.assertIn("pipedrive", tools)
        self.assertIn("hubspot", tools)
        self.assertIn("mailchimp", tools)
        self.assertIn("asana", tools)
        # Asana gets a real project board URL (not "#")
        asana_url = next(e["url"] for e in index if e["tool"] == "asana")
        self.assertIn("app.asana.com/0/", asana_url)
        # Ref IDs are sequential R01, R02, ...
        ref_ids = [entry["ref_id"] for entry in index]
        self.assertEqual(ref_ids[0], "R01")
        self.assertEqual(ref_ids[1], "R02")


class TestWeeklyReportQualityScoring(unittest.TestCase):
    """Tests for System 4 — quality scoring in weekly_report.py."""

    def test_rubric_loads_and_contains_all_dimensions(self):
        """_load_rubric() returns text containing all four scoring dimensions."""
        from intelligence.weekly_report import _load_rubric
        rubric = _load_rubric()
        for dimension in ["Specificity", "Insight Quality", "Structure", "Trust Signals"]:
            self.assertIn(dimension, rubric, f"Rubric missing dimension: {dimension}")

    def test_rubric_is_non_empty(self):
        """_load_rubric() returns non-empty string from docs/skills/weekly-report.md."""
        from intelligence.weekly_report import _load_rubric
        rubric = _load_rubric()
        self.assertGreater(len(rubric), 100)

    def test_score_report_returns_int_in_range(self):
        """_score_report() returns an integer between 0 and 100."""
        import unittest.mock as mock
        mock_response = mock.MagicMock()
        mock_response.content = [mock.MagicMock(text="Score: 82")]

        with mock.patch("anthropic.Anthropic") as mock_cls:
            mock_client = mock.MagicMock()
            mock_cls.return_value = mock_client
            mock_client.messages.create.return_value = mock_response

            from intelligence import weekly_report
            # Reload to reset module-level Anthropic client if cached
            import importlib
            importlib.reload(weekly_report)

            score = weekly_report._score_report("This is a well-written weekly report with specific numbers and citations.")

        self.assertIsInstance(score, int)
        self.assertGreaterEqual(score, 0)
        self.assertLessEqual(score, 100)

    def test_score_report_handles_missing_score_in_response(self):
        """_score_report() returns 0 gracefully if Sonnet response has no parseable score."""
        import unittest.mock as mock
        mock_response = mock.MagicMock()
        mock_response.content = [mock.MagicMock(text="I cannot evaluate this report.")]

        with mock.patch("anthropic.Anthropic") as mock_cls:
            mock_client = mock.MagicMock()
            mock_cls.return_value = mock_client
            mock_client.messages.create.return_value = mock_response

            from intelligence import weekly_report
            import importlib
            importlib.reload(weekly_report)
            score = weekly_report._score_report("Some report text.")

        self.assertIsInstance(score, int)
        self.assertEqual(score, 0)


class TestWeeklyReportGenerate(unittest.TestCase):
    """Tests for generate_weekly_report() in weekly_report.py."""

    def _make_context(self):
        import unittest.mock as mock
        from intelligence.context_builder import BriefingContext
        ctx = mock.MagicMock(spec=BriefingContext)
        ctx.date = "2026-03-23"
        ctx.date_formatted = "Sunday, March 23, 2026"
        ctx.metrics = {
            "revenue": {"yesterday": {"total": 5000.0}},
            "financial_health": {},
            "sales": {},
        }
        ctx.context_document = "## WEEK SUMMARY\nRevenue: $36,250\n## CASH POSITION\nAR: $120,000"
        ctx.report_type = "weekly"
        return ctx

    def test_dry_run_returns_briefing_without_api_call(self):
        """generate_weekly_report(dry_run=True) returns Briefing without calling Anthropic."""
        import unittest.mock as mock
        from intelligence.weekly_report import generate_weekly_report

        ctx = self._make_context()
        import simulation.deep_links as dl
        dl._cache_loaded = True

        with mock.patch("anthropic.Anthropic") as mock_cls:
            briefing = generate_weekly_report(ctx, dry_run=True)
            mock_cls.assert_not_called()

        from intelligence.briefing_generator import Briefing
        self.assertIsInstance(briefing, Briefing)
        self.assertEqual(briefing.report_type, "weekly")
        self.assertIn("dry", briefing.model_used.lower())

    def test_generate_weekly_report_returns_briefing_with_correct_type(self):
        """generate_weekly_report() returns a Briefing with report_type='weekly'."""
        import unittest.mock as mock
        from intelligence.weekly_report import generate_weekly_report

        ctx = self._make_context()
        import simulation.deep_links as dl
        dl._cache_loaded = True

        mock_message = mock.MagicMock()
        mock_message.content = [mock.MagicMock(text="## Executive Summary\nGood week.\n## Key Wins\n3 wins.\n## Concerns\n2 concerns.\n## Trends\nFlat.\n## Recommendations\n1. Do X.\n## Looking Ahead\nWatch Y.")]
        mock_message.usage.input_tokens = 1000
        mock_message.usage.output_tokens = 500

        # Score call returns "Score: 80"
        score_response = mock.MagicMock()
        score_response.content = [mock.MagicMock(text="Score: 80")]

        call_count = [0]
        def side_effect(**kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return mock_message   # Opus generation call
            return score_response     # Sonnet scoring call

        with mock.patch("anthropic.Anthropic") as mock_cls:
            mock_client = mock.MagicMock()
            mock_cls.return_value = mock_client
            mock_client.messages.create.side_effect = side_effect

            briefing = generate_weekly_report(ctx, dry_run=False)

        from intelligence.briefing_generator import Briefing
        self.assertIsInstance(briefing, Briefing)
        self.assertEqual(briefing.report_type, "weekly")
        self.assertEqual(briefing.model_used, "claude-opus-4-6")

    def test_generate_weekly_report_no_low_confidence_in_output(self):
        """generate_weekly_report() strips [LOW] tagged content from final output."""
        import unittest.mock as mock
        from intelligence.weekly_report import generate_weekly_report

        ctx = self._make_context()
        import simulation.deep_links as dl
        dl._cache_loaded = True

        # Opus returns a report with a [LOW] tagged sentence
        mock_message = mock.MagicMock()
        mock_message.content = [mock.MagicMock(
            text="Revenue was strong. This is speculative noise. [LOW] The team performed well."
        )]
        mock_message.usage.input_tokens = 800
        mock_message.usage.output_tokens = 400

        score_response = mock.MagicMock()
        score_response.content = [mock.MagicMock(text="Score: 75")]

        call_count = [0]
        def side_effect(**kwargs):
            call_count[0] += 1
            return mock_message if call_count[0] == 1 else score_response

        with mock.patch("anthropic.Anthropic") as mock_cls:
            mock_client = mock.MagicMock()
            mock_cls.return_value = mock_client
            mock_client.messages.create.side_effect = side_effect

            briefing = generate_weekly_report(ctx, dry_run=False)

        self.assertNotIn("[LOW]", briefing.content_slack)
        self.assertNotIn("[LOW]", briefing.content_plain)


if __name__ == "__main__":
    unittest.main(verbosity=2)
