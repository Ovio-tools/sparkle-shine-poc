# Weekly Report Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a weekly business intelligence report using Claude Opus 4.6 with clickable Slack citations, delivered via `--report-type weekly` on the existing intelligence runner.

**Architecture:** Two new files handle distinct concerns: `simulation/deep_links.py` builds and formats tool-specific UI URLs; `intelligence/weekly_report.py` orchestrates Opus 4.6 with four post-processing systems (insight history, confidence filtering, citation injection, quality scoring). The existing runner's `--report-type weekly` path is updated to call `generate_weekly_report()`, which returns the same `Briefing` type as the daily path — no downstream changes needed.

**Tech Stack:** Python 3, `anthropic` SDK (claude-opus-4-6 for generation, claude-sonnet-4-6 for quality scoring), existing `auth.get_client()` for tool API calls, `simulation/config.py SEASONAL_WEIGHTS`, `docs/skills/weekly-report.md` for system prompt template + quality rubric (read at runtime, not hard-coded).

---

## File Map

| Action | File | Responsibility |
|--------|------|---------------|
| Create | `simulation/deep_links.py` | URL building, lazy account-info cache, format_citation |
| Create | `intelligence/weekly_report.py` | 4 systems + generate_weekly_report() |
| Create | `weekly_reports/insight_history.json` | auto-created at runtime, no manual setup |
| Modify | `intelligence/config.py` | add weekly_model / max_tokens_weekly / temperature_weekly |
| Modify | `intelligence/runner.py` | route weekly path to generate_weekly_report(); call ensure_channel |
| Modify | `intelligence/slack_publisher.py` | add ensure_channel() |
| Modify | `tests/test_phase4.py` | new test classes for deep_links, weekly_report, ensure_channel |

---

### Task 1: Add weekly model config keys

**Files:**
- Modify: `intelligence/config.py`

- [ ] **Step 1: Add three keys to MODEL_CONFIG**

Open `intelligence/config.py`. In `MODEL_CONFIG`, add after `"briefing_model"`:

```python
MODEL_CONFIG: dict = {
    "briefing_model":       "claude-sonnet-4-6",
    "weekly_model":         "claude-opus-4-6",      # weekly report (Opus for pattern analysis)
    "analysis_model":       "claude-opus-4-6",      # reserved for complex pattern analysis
    "max_tokens_briefing":  2800,
    "max_tokens_weekly":    3000,
    "max_tokens_analysis":  1500,
    "temperature_briefing": 0.3,
    "temperature_weekly":   0.4,
    "temperature_analysis": 0.5,
}
```

- [ ] **Step 2: Verify config imports cleanly**

```bash
cd /Users/ovieoghor/Documents/Claude\ Code\ Exercises/Simulation\ Exercise/sparkle-shine-poc
python -c "from intelligence.config import MODEL_CONFIG; print(MODEL_CONFIG['weekly_model'], MODEL_CONFIG['max_tokens_weekly'])"
```

Expected output:
```
claude-opus-4-6 3000
```

- [ ] **Step 3: Commit**

```bash
git add intelligence/config.py
git commit -m "Add weekly model config keys to MODEL_CONFIG (Task 1)"
```

---

### Task 2: Create simulation/deep_links.py

**Files:**
- Create: `simulation/deep_links.py`
- Modify: `tests/test_phase4.py`

**Design note:** `context.metrics` from the weekly context builder contains aggregate values (totals, averages, counts) but not individual record IDs. The citation index therefore builds aggregate-level links (e.g., QuickBooks P&L report URL) rather than per-record links. Individual record links (specific invoices, specific deals) are not buildable from metrics alone without a separate DB query — this is acceptable for a POC.

- [ ] **Step 1: Write failing tests in tests/test_phase4.py**

Add this class at the bottom of `tests/test_phase4.py`, before the `if __name__ == "__main__":` block:

```python
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
        self.assertIn("sandbox.qbo.intuit.com", url)
        self.assertNotIn("app.qbo.intuit.com/app", url.replace("sandbox.", ""))

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
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
python tests/test_phase4.py -v -k "TestDeepLinks" 2>&1 | head -30
```

Expected: `ModuleNotFoundError: No module named 'simulation.deep_links'` or similar import error for all tests.

- [ ] **Step 3: Create simulation/deep_links.py**

```python
"""
simulation/deep_links.py

Build clickable UI deep links for each SaaS tool and format them as
Slack mrkdwn citations. Used by the weekly report generator.

Lazy-loads Pipedrive subdomain and HubSpot portal ID on first call to
get_deep_link() — one API call each per process lifetime. Falls back
gracefully if the call fails: deep links degrade to plain text, not an
error.

For Asana tasks, record_type is the project name (e.g. "Client Success").
Project GIDs are loaded from config/tool_ids.json.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

from auth import get_client
from intelligence.logging_config import setup_logging

logger = setup_logging(__name__)

# ── Module-level lazy cache ──────────────────────────────────────────────────
_pipedrive_subdomain: str | None = None
_hubspot_portal_id: str | None = None
_cache_loaded: bool = False

# ── Asana project GIDs (from config/tool_ids.json, loaded on first use) ─────
_asana_project_gids: dict[str, str] | None = None


def _get_asana_project_gids() -> dict[str, str]:
    global _asana_project_gids
    if _asana_project_gids is None:
        tool_ids_path = Path(__file__).parent.parent / "config" / "tool_ids.json"
        with open(tool_ids_path) as f:
            tool_ids = json.load(f)
        _asana_project_gids = tool_ids.get("asana", {}).get("projects", {})
    return _asana_project_gids


# ── Account info loader ──────────────────────────────────────────────────────

def _load_account_info() -> None:
    """Populate _pipedrive_subdomain and _hubspot_portal_id.

    Sets _cache_loaded=True regardless of success so failures don't
    trigger repeated API calls within the same process.
    """
    global _pipedrive_subdomain, _hubspot_portal_id, _cache_loaded
    if _cache_loaded:
        return

    try:
        session = get_client("pipedrive")
        resp = session.get("https://api.pipedrive.com/v1/users/me")
        resp.raise_for_status()
        _pipedrive_subdomain = resp.json().get("data", {}).get("company_domain")
    except Exception as exc:
        logger.warning("Could not load Pipedrive subdomain for deep links: %s", exc)

    try:
        session = get_client("hubspot")
        resp = session.get("https://api.hubapi.com/integrations/v1/me")
        resp.raise_for_status()
        portal_id = resp.json().get("portalId")
        _hubspot_portal_id = str(portal_id) if portal_id else None
    except Exception as exc:
        logger.warning("Could not load HubSpot portal ID for deep links: %s", exc)

    _cache_loaded = True


# ── QBO environment detection ────────────────────────────────────────────────

def _qbo_ui_base() -> str:
    """Return the QBO UI base URL.

    Mirrors the sandbox-detection logic in auth/quickbooks_auth.py get_base_url():
    check QBO_BASE_URL env var; if it contains 'sandbox', use the sandbox UI host.
    """
    api_base = os.getenv(
        "QBO_BASE_URL",
        "https://sandbox-quickbooks.api.intuit.com/v3/company",
    )
    if "sandbox" in api_base:
        return "https://app.sandbox.qbo.intuit.com/app"
    return "https://app.qbo.intuit.com/app"


# ── URL builders ─────────────────────────────────────────────────────────────

def get_deep_link(tool: str, record_type: str, record_id: str) -> str:
    """Return a clickable UI URL for the given tool record.

    For Asana tasks, pass the project name as record_type
    (e.g. "Client Success", "Admin & Operations"). The project GID
    is looked up in config/tool_ids.json. If the project isn't found,
    falls back to app.asana.com/0/search?q={record_id}.

    Returns "#" if the URL cannot be built (missing credentials,
    unknown tool/record_type, or API failure on cache load).
    """
    _load_account_info()

    try:
        if tool == "hubspot":
            pid = _hubspot_portal_id or ""
            if not pid:
                return "#"
            if record_type == "contact":
                return f"https://app.hubspot.com/contacts/{pid}/contact/{record_id}"
            if record_type == "deal":
                return f"https://app.hubspot.com/contacts/{pid}/deal/{record_id}"

        elif tool == "pipedrive":
            sub = _pipedrive_subdomain or ""
            if not sub:
                return "#"
            if record_type == "deal":
                return f"https://{sub}.pipedrive.com/deal/{record_id}"
            if record_type == "person":
                return f"https://{sub}.pipedrive.com/person/{record_id}"

        elif tool == "jobber":
            if record_type == "client":
                return f"https://app.getjobber.com/client/{record_id}"
            if record_type == "job":
                return f"https://app.getjobber.com/work_requests/{record_id}"

        elif tool == "quickbooks":
            base = _qbo_ui_base()
            if record_type == "invoice":
                return f"{base}/invoice?txnId={record_id}"
            if record_type == "report_pl":
                return f"{base}/reportv2?token=PROFIT_AND_LOSS"
            if record_type == "report_ar":
                return f"{base}/reportv2?token=AGING_DETAIL"

        elif tool == "asana":
            gids = _get_asana_project_gids()
            project_gid = gids.get(record_type)
            if project_gid:
                return f"https://app.asana.com/0/{project_gid}/{record_id}"
            logger.debug(
                "Asana project '%s' not in tool_ids.json — using search fallback",
                record_type,
            )
            return f"https://app.asana.com/0/search?q={record_id}"

        elif tool == "mailchimp":
            # MAILCHIMP_SERVER_PREFIX stores the full prefix (e.g. "us4"), not just
            # the numeric part. Use it directly — do NOT prepend "us" again.
            dc = os.getenv("MAILCHIMP_SERVER_PREFIX") or os.getenv(
                "MAILCHIMP_DATA_CENTER", "us1"
            )
            if record_type == "campaign":
                return f"https://{dc}.admin.mailchimp.com/campaigns/show?id={record_id}"

    except Exception as exc:
        logger.warning(
            "Deep link build failed for %s/%s/%s: %s", tool, record_type, record_id, exc
        )

    return "#"


def get_report_link(tool: str) -> str:
    """Return a URL to the tool's aggregate report or dashboard.

    Used for revenue and cash-flow citations that refer to a whole-week
    view rather than a specific transaction.
    """
    if tool == "quickbooks":
        return f"{_qbo_ui_base()}/reportv2?token=PROFIT_AND_LOSS"
    return "#"


def format_citation(text: str, tool: str, record_type: str, record_id: str) -> str:
    """Format a Slack mrkdwn citation.

    Returns "(<URL|text>)" if a URL is available, or plain text if not.
    The plain-text fallback means the report generates even when deep
    links are unavailable.
    """
    url = get_deep_link(tool, record_type, record_id)
    if url == "#":
        return text
    return f"(<{url}|{text}>)"
```

- [ ] **Step 4: Run tests — expect all to pass**

```bash
python tests/test_phase4.py -v -k "TestDeepLinks"
```

Expected: 8 tests, all PASS.

- [ ] **Step 5: Commit**

```bash
git add simulation/deep_links.py tests/test_phase4.py
git commit -m "Add simulation/deep_links.py with URL builders and citation formatting (Task 2)"
```

---

### Task 3: Add ensure_channel() to slack_publisher.py

**Files:**
- Modify: `intelligence/slack_publisher.py`
- Modify: `tests/test_phase4.py`

- [ ] **Step 1: Write failing tests**

Add this class to `tests/test_phase4.py`, after `TestDeepLinks`:

```python
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
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
python tests/test_phase4.py -v -k "TestEnsureChannel"
```

Expected: `ImportError: cannot import name 'ensure_channel'` for all 3 tests.

- [ ] **Step 3: Add ensure_channel() to slack_publisher.py**

In `intelligence/slack_publisher.py`, add this function after the `post_alert()` function (before the CLI section):

```python
def ensure_channel(channel_name: str) -> str:
    """Ensure a Slack channel exists and the bot is a member.

    Resolution order:
      1. resolve_channel_id() — fast path if channel already exists.
      2. conversations.create — if not found, create it.
      3. conversations.join — if create fails with name_taken (channel
         exists but bot not yet a member).

    Returns the channel ID. Raises on unexpected errors.
    Called once before posting the weekly report to guarantee the
    channel exists. The daily-briefing channel is not affected.
    """
    name = channel_name.lstrip("#").strip()

    try:
        channel_id = resolve_channel_id(name)
        logger.debug("Channel #%s already exists (%s)", name, channel_id)
        return channel_id
    except ValueError:
        pass  # channel not found — create it

    slack_client = get_client("slack")

    try:
        resp = slack_client.conversations_create(name=name, is_private=False)
        channel_id = resp["channel"]["id"]
        _channel_id_cache[name] = channel_id
        logger.info("Created Slack channel #%s (%s)", name, channel_id)
        return channel_id
    except Exception as exc:
        if "name_taken" not in str(exc):
            raise

    # Channel exists but bot is not a member — join it
    resp = slack_client.conversations_join(channel=name)
    channel_id = resp["channel"]["id"]
    _channel_id_cache[name] = channel_id
    logger.info("Joined existing Slack channel #%s (%s)", name, channel_id)
    return channel_id
```

- [ ] **Step 4: Run tests — expect all to pass**

```bash
python tests/test_phase4.py -v -k "TestEnsureChannel"
```

Expected: 3 tests, all PASS.

- [ ] **Step 5: Commit**

```bash
git add intelligence/slack_publisher.py tests/test_phase4.py
git commit -m "Add ensure_channel() to slack_publisher for weekly-briefing setup (Task 3)"
```

---

### Task 4: weekly_report.py — insight history system

**Files:**
- Create: `intelligence/weekly_report.py`
- Modify: `tests/test_phase4.py`

The insight history system is the first of four post-processing steps. This task creates the file skeleton and implements System 1 in full:
- `_load_insight_history()` / `_save_insight_history()`
- `_build_insight_history_block()`
- `_extract_and_update_insights()` — parses `[insight_id: <id>]` markers, updates history, strips markers

- [ ] **Step 1: Write failing tests**

Add this class to `tests/test_phase4.py`, after `TestEnsureChannel`:

```python
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
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
python tests/test_phase4.py -v -k "TestWeeklyReportInsightHistory"
```

Expected: `ModuleNotFoundError: No module named 'intelligence.weekly_report'` for all 5 tests.

- [ ] **Step 3: Create intelligence/weekly_report.py with insight history system**

```python
"""
intelligence/weekly_report.py

Weekly business intelligence report generator using Claude Opus 4.6.

Four post-processing systems (applied in order after the Opus API call):
  1. _extract_and_update_insights() — parse [insight_id:] markers,
     update weekly_reports/insight_history.json, strip markers from text
  2. _strip_low_confidence()        — remove sentences tagged [LOW] or
     referencing LOW-confidence citation entries
  3. _inject_citations()            — replace [R01] ref_ids with Slack mrkdwn
  4. _score_report()                — Sonnet quality scoring against rubric

Usage:
    from intelligence.weekly_report import generate_weekly_report
    briefing = generate_weekly_report(context, dry_run=False)
"""
from __future__ import annotations

import copy
import json
import re
import time
from datetime import date
from pathlib import Path
from typing import Optional

from intelligence.briefing_generator import Briefing
from intelligence.config import MODEL_CONFIG
from intelligence.context_builder import BriefingContext
from intelligence.logging_config import setup_logging
from simulation.config import SEASONAL_WEIGHTS

logger = setup_logging(__name__)

# ── File paths ───────────────────────────────────────────────────────────────
_INSIGHT_HISTORY_FILE = Path("weekly_reports/insight_history.json")
_SKILL_DOC_PATH = Path("docs/skills/weekly-report.md")

# ── Rubric cache (lazy-loaded on first _score_report call) ───────────────────
_rubric_text: Optional[str] = None

# ── Seasonal notes (plain-English interpretation of SEASONAL_WEIGHTS) ────────
_SEASONAL_NOTES: dict[int, str] = {
    1:  "January is typically slow. A 15-20% dip from December is expected and not alarming.",
    2:  "February is a recovery month. Modest growth from January is typical.",
    3:  "March marks the start of spring pickup. Growth should be resuming.",
    4:  "April is peak spring cleaning season. Strong demand is expected.",
    5:  "May continues strong seasonal momentum.",
    6:  "June is the start of the summer surge. Higher volumes are expected.",
    7:  "July is peak summer. Historically the highest-volume month.",
    8:  "August stays strong but late-summer softening begins.",
    9:  "September is a seasonal dip. A 10-15% decline from August is normal.",
    10: "October rebounds as routines stabilize after summer.",
    11: "November picks up with pre-holiday cleaning demand.",
    12: "December is peak holiday season. Highest revenue month of the year.",
}


# ══════════════════════════════════════════════════════════════════════════════
# System 1 — Insight History
# ══════════════════════════════════════════════════════════════════════════════

def _load_insight_history() -> dict:
    """Load weekly_reports/insight_history.json. Auto-creates if missing."""
    empty = {"last_updated": None, "insights": []}
    if not _INSIGHT_HISTORY_FILE.exists():
        _INSIGHT_HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
        return empty
    try:
        return json.loads(_INSIGHT_HISTORY_FILE.read_text())
    except Exception as exc:
        logger.warning("Could not load insight history (%s) — starting fresh", exc)
        return empty


def _save_insight_history(history: dict) -> None:
    """Write updated history back to weekly_reports/insight_history.json."""
    _INSIGHT_HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    _INSIGHT_HISTORY_FILE.write_text(json.dumps(history, indent=2))


def _build_insight_history_block(history: dict) -> str:
    """Format the last 4 weeks of insight history as the PREVIOUSLY REPORTED block.

    Only active and graduated insights from the last 4 report dates are included.
    This text is injected into the Opus system prompt.
    """
    insights = history.get("insights", [])
    if not insights:
        return "(No previously reported insights — this is the first weekly report.)"

    # Sort by last_reported descending; take insights from the most recent 4 dates
    dated = sorted(
        [i for i in insights if i.get("last_reported")],
        key=lambda x: x["last_reported"],
        reverse=True,
    )
    recent_dates = sorted(
        {i["last_reported"] for i in dated}, reverse=True
    )[:4]
    recent = [i for i in dated if i["last_reported"] in recent_dates]

    lines = []
    for ins in recent:
        status = ins.get("status", "active")
        times = ins.get("times_reported", 1)
        last = ins.get("last_reported", "unknown date")
        if status == "graduated":
            lines.append(
                f'- "{ins["summary"]}" (insight_id: {ins["insight_id"]}) — '
                f"reported {times}x, last on {last}. "
                f"GRADUATED to standing fact. Reference only when supporting a new recommendation."
            )
        else:
            lines.append(
                f'- "{ins["summary"]}" (insight_id: {ins["insight_id"]}) — '
                f"reported {times}x, last on {last}. "
                f"{'Re-report only if underlying data changed.' if times > 1 else 'OK to follow up if new data exists.'}"
            )

    return "\n".join(lines)


def _extract_and_update_insights(text: str, history: dict) -> tuple[str, dict]:
    """Parse [insight_id: <id>] markers from Opus output.

    Steps:
      1. Find all [insight_id: <id>] markers in the text.
      2. For each: increment times_reported, update last_reported.
         Graduate to 'graduated' if times_reported reaches 3.
         Add as new entry (status='active', times_reported=1) if unseen.
      3. Strip all markers from the text.
      4. Return (cleaned_text, updated_history).

    This is post-processing step 1 — runs before confidence filtering
    so markers never reach the final Slack output.
    """
    history = copy.deepcopy(history)
    pattern = re.compile(r'\[insight_id:\s*([^\]]+)\]')
    found_ids = [m.strip() for m in pattern.findall(text)]

    # Strip markers from text
    cleaned = pattern.sub("", text)
    cleaned = re.sub(r'  +', ' ', cleaned)
    cleaned = re.sub(r' \.', '.', cleaned)
    cleaned = cleaned.strip()

    today = date.today().isoformat()
    existing = {ins["insight_id"]: ins for ins in history.get("insights", [])}

    for insight_id in found_ids:
        if insight_id in existing:
            existing[insight_id]["times_reported"] += 1
            existing[insight_id]["last_reported"] = today
            if (
                existing[insight_id]["times_reported"] >= 3
                and existing[insight_id]["status"] == "active"
            ):
                existing[insight_id]["status"] = "graduated"
        else:
            existing[insight_id] = {
                "insight_id": insight_id,
                "category": "general",
                "summary": insight_id.replace("_", " "),
                "first_reported": today,
                "last_reported": today,
                "times_reported": 1,
                "status": "active",
                "last_values": {},
            }

    history["insights"] = list(existing.values())
    history["last_updated"] = today
    return cleaned, history
```

- [ ] **Step 4: Run tests — expect all to pass**

```bash
python tests/test_phase4.py -v -k "TestWeeklyReportInsightHistory"
```

Expected: 5 tests, all PASS.

- [ ] **Step 5: Commit**

```bash
git add intelligence/weekly_report.py tests/test_phase4.py
git commit -m "Add weekly_report.py insight history system — System 1 (Task 4)"
```

---

### Task 5: weekly_report.py — confidence filtering

**Files:**
- Modify: `intelligence/weekly_report.py`
- Modify: `tests/test_phase4.py`

System 2: post-generation string scan that strips LOW-confidence content before citations are injected. This is a pure string operation — no LLM call.

- [ ] **Step 1: Write failing tests**

Add to the `TestWeeklyReportInsightHistory` class (or add a new class — new class is cleaner):

```python
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
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
python tests/test_phase4.py -v -k "TestWeeklyReportConfidenceFilter"
```

Expected: `ImportError: cannot import name '_strip_low_confidence'` for all 4 tests.

- [ ] **Step 3: Add _strip_low_confidence() to weekly_report.py**

Append after `_extract_and_update_insights()`:

```python
# ══════════════════════════════════════════════════════════════════════════════
# System 2 — Confidence Filtering
# ══════════════════════════════════════════════════════════════════════════════

def _strip_low_confidence(text: str, citation_index: list[dict]) -> tuple[str, int]:
    """Remove LOW-confidence content from the Opus output.

    Two passes (string matching, no LLM):
      Pass 1: Remove sentences containing literal "[LOW]" tags.
      Pass 2: Remove sentences containing ref_ids where citation_index
              entry has confidence == "LOW".

    Returns (cleaned_text, removed_sentence_count).

    This is post-processing step 2 — runs after insight marker stripping
    and before citation injection.
    """
    # Build set of LOW-confidence ref_ids
    low_refs = {
        entry["ref_id"]
        for entry in citation_index
        if entry.get("confidence") == "LOW"
    }

    # Split on sentence boundaries (period/exclamation/question + whitespace)
    sentences = re.split(r'(?<=[.!?])\s+', text)
    removed = 0
    kept = []

    for sentence in sentences:
        if "[LOW]" in sentence:
            removed += 1
            continue
        if low_refs and any(f"[{ref}]" in sentence for ref in low_refs):
            removed += 1
            continue
        kept.append(sentence)

    return " ".join(kept), removed
```

- [ ] **Step 4: Run tests — expect all to pass**

```bash
python tests/test_phase4.py -v -k "TestWeeklyReportConfidenceFilter"
```

Expected: 4 tests, all PASS.

- [ ] **Step 5: Commit**

```bash
git add intelligence/weekly_report.py tests/test_phase4.py
git commit -m "Add confidence filtering to weekly_report.py — System 2 (Task 5)"
```

---

### Task 6: weekly_report.py — citation index and injection

**Files:**
- Modify: `intelligence/weekly_report.py`
- Modify: `tests/test_phase4.py`

System 3: build the citation index from `context.metrics` (aggregate-level links only — the metrics dict has totals and averages, not individual record IDs), then post-process Opus output to replace `[R01]` references with Slack mrkdwn links.

- [ ] **Step 1: Write failing tests**

Add to `tests/test_phase4.py`:

```python
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
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
python tests/test_phase4.py -v -k "TestWeeklyReportCitations"
```

Expected: `ImportError: cannot import name '_inject_citations'` for all 4 tests.

- [ ] **Step 3: Add _build_citation_index() and _inject_citations() to weekly_report.py**

Append after `_strip_low_confidence()`:

```python
# ══════════════════════════════════════════════════════════════════════════════
# System 3 — Citation Index and Injection
# ══════════════════════════════════════════════════════════════════════════════

def _build_citation_index(context: BriefingContext) -> list[dict]:
    """Build the citation index from context.metrics.

    Design note: context.metrics contains aggregate values (totals,
    averages, counts) but not individual record IDs. The citation index
    therefore contains aggregate-level links (e.g. QBO P&L report) rather
    than per-transaction links. This is intentional — the spec states
    "Aggregate metrics cite the report or dashboard, not individual records."

    Covers all 6 weekly report sections:
      - Revenue (Section 1) → QBO P&L
      - Operations (Section 2) → Jobber jobs (#, no aggregate URL in spec)
      - Cash Flow (Section 3) → QBO AR Aging
      - Sales (Section 4) → Pipedrive pipeline (#, no aggregate URL in spec)
      - Marketing (Section 5) → HubSpot contacts + Mailchimp campaigns (#)
      - Tasks (Section 6) → Asana Admin & Operations project board

    "#" URLs degrade gracefully: format_citation() returns plain text.

    Returns a list of citation dicts, each with:
        ref_id      — e.g. "R01" (used as [R01] in Opus output)
        claim       — short display label
        tool        — tool name
        record_type — record type string
        record_id   — None for aggregate reports
        url         — pre-built UI URL (or "#" if unavailable)
        confidence  — "HIGH" / "MEDIUM" / "LOW"
    """
    from simulation.deep_links import get_report_link, get_deep_link

    citations = []
    _counter = [0]

    def _next_ref() -> str:
        _counter[0] += 1
        return f"R{_counter[0]:02d}"

    def _add(claim: str, tool: str, record_type: str, url: str, confidence: str = "HIGH") -> None:
        citations.append({
            "ref_id": _next_ref(),
            "claim": claim,
            "tool": tool,
            "record_type": record_type,
            "record_id": None,
            "url": url,
            "confidence": confidence,
        })

    metrics = context.metrics or {}

    # ── Section 1 (Week in Review) — Revenue ─────────────────────────────────
    if metrics.get("revenue"):
        _add("Weekly P&L", "quickbooks", "report_pl", get_report_link("quickbooks"))

    # ── Section 2 (Crew Performance) — Operations ────────────────────────────
    if metrics.get("operations"):
        _add("Jobber Jobs", "jobber", "jobs", "#")

    # ── Section 3 (Cash Flow) — AR Aging ─────────────────────────────────────
    fin = metrics.get("financial_health", {})
    if fin.get("ar_aging") or fin.get("cash_position"):
        _add("AR Aging Report", "quickbooks", "report_ar",
             get_deep_link("quickbooks", "report_ar", ""))

    # ── Section 4 (Sales Pipeline) — Pipedrive ───────────────────────────────
    if metrics.get("sales"):
        # No aggregate pipeline URL in deep_links spec — degrade to plain text
        _add("Pipedrive Pipeline", "pipedrive", "pipeline", "#")

    # ── Section 5 (Marketing & Reputation) — HubSpot + Mailchimp ─────────────
    if metrics.get("marketing"):
        _add("HubSpot Contacts", "hubspot", "contacts", "#")
        _add("Mailchimp Campaigns", "mailchimp", "campaigns", "#")

    # ── Section 6 (Task & Delegation Health) — Asana ─────────────────────────
    if metrics.get("tasks"):
        # Admin & Operations project GID from config/tool_ids.json
        asana_ops_gid = "1213719394454339"
        _add("Asana Admin & Operations", "asana", "project_board",
             f"https://app.asana.com/0/{asana_ops_gid}/list")

    return citations


def _inject_citations(text: str, citation_index: list[dict]) -> str:
    """Replace [R01] ref_id markers with Slack mrkdwn citation links.

    For each citation in the index:
      - If url is a real URL: replace [ref_id] with (<url|claim>)
      - If url is "#": replace [ref_id] with plain claim text

    Ref_ids not found in the index are left as-is (they may be hallucinated
    by Opus — leaving them visible makes them easy to spot and fix).

    This is post-processing step 3 — runs after confidence filtering.
    """
    for entry in citation_index:
        ref_id = entry["ref_id"]
        url = entry.get("url", "#")
        claim = entry.get("claim", ref_id)
        marker = f"[{ref_id}]"
        if marker not in text:
            continue
        if url and url != "#":
            replacement = f"(<{url}|{claim}>)"
        else:
            replacement = claim
        text = text.replace(marker, replacement)
    return text
```

- [ ] **Step 4: Run tests — expect all to pass**

```bash
python tests/test_phase4.py -v -k "TestWeeklyReportCitations"
```

Expected: 4 tests, all PASS.

- [ ] **Step 5: Commit**

```bash
git add intelligence/weekly_report.py tests/test_phase4.py
git commit -m "Add citation index and injection to weekly_report.py — System 3 (Task 6)"
```

---

### Task 7: weekly_report.py — quality scoring

**Files:**
- Modify: `intelligence/weekly_report.py`
- Modify: `tests/test_phase4.py`

System 4: lazy-load the quality scoring rubric verbatim from `docs/skills/weekly-report.md`, then score the final report text with a cheap Sonnet call (200 tokens, temperature 0.0).

- [ ] **Step 1: Write failing tests**

Add to `tests/test_phase4.py`:

```python
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
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
python tests/test_phase4.py -v -k "TestWeeklyReportQualityScoring"
```

Expected: `ImportError: cannot import name '_load_rubric'` for rubric tests; `_score_report` also missing.

- [ ] **Step 3: Add quality scoring functions to weekly_report.py**

Append after `_inject_citations()`:

```python
# ══════════════════════════════════════════════════════════════════════════════
# System 4 — Quality Scoring
# ══════════════════════════════════════════════════════════════════════════════

def _extract_section(doc: str, heading: str) -> str:
    """Extract the content between a markdown heading and the next '---' divider."""
    lines = doc.splitlines()
    start: Optional[int] = None
    for i, line in enumerate(lines):
        if line.strip() == heading:
            start = i + 1
            break
    if start is None:
        return ""
    result = []
    for line in lines[start:]:
        if line.strip() == "---":
            break
        result.append(line)
    return "\n".join(result).strip()


def _load_rubric() -> str:
    """Lazy-load the quality scoring rubric verbatim from docs/skills/weekly-report.md.

    The rubric is cached in _rubric_text after the first call. If the file
    is missing, logs an error and returns an empty string — quality scoring
    is skipped for that run but the report still posts.
    """
    global _rubric_text
    if _rubric_text is not None:
        return _rubric_text
    try:
        doc = _SKILL_DOC_PATH.read_text()
        _rubric_text = _extract_section(doc, "## Quality Scoring Rubric")
        if not _rubric_text:
            logger.error(
                "Could not extract '## Quality Scoring Rubric' section from %s",
                _SKILL_DOC_PATH,
            )
            _rubric_text = ""
    except Exception as exc:
        logger.error("Could not load quality rubric from %s: %s", _SKILL_DOC_PATH, exc)
        _rubric_text = ""
    return _rubric_text


def _score_report(report_text: str) -> int:
    """Score the report against the rubric from docs/skills/weekly-report.md.

    Uses claude-sonnet-4-6 (cheap call, 200 tokens max, temperature 0.0).
    Parses 'Score: <N>' from the response. Returns 0 on failure so the
    caller can distinguish a real 0 from a scoring error.

    This is post-processing step 4 — runs last, on the fully-processed text.
    """
    rubric = _load_rubric()
    if not rubric:
        logger.warning("Quality rubric unavailable — skipping score for this run")
        return 0

    import anthropic
    client = anthropic.Anthropic()

    system = f"""You are evaluating a weekly business intelligence report.
Score it using this rubric (each dimension is 0-25, total 100):

{rubric}

Reply with ONLY: "Score: <total>" on the first line, then one line per
dimension: "Specificity: <N>", "Insight Quality: <N>", etc.
"""
    try:
        response = client.messages.create(
            model=MODEL_CONFIG["briefing_model"],  # Sonnet — cheap scoring call
            max_tokens=200,
            temperature=0.0,
            system=system,
            messages=[{"role": "user", "content": report_text[:4000]}],
        )
        text = response.content[0].text
        match = re.search(r"Score:\s*(\d+)", text)
        if match:
            return int(match.group(1))
        logger.warning("Could not parse score from Sonnet response: %s", text[:200])
    except Exception as exc:
        logger.warning("Quality scoring failed: %s", exc)
    return 0
```

- [ ] **Step 4: Run tests — expect all to pass**

```bash
python tests/test_phase4.py -v -k "TestWeeklyReportQualityScoring"
```

Expected: 4 tests, all PASS.

- [ ] **Step 5: Commit**

```bash
git add intelligence/weekly_report.py tests/test_phase4.py
git commit -m "Add quality scoring to weekly_report.py — System 4 (Task 7)"
```

---

### Task 8: weekly_report.py — system prompt and generate_weekly_report()

**Files:**
- Modify: `intelligence/weekly_report.py`
- Modify: `tests/test_phase4.py`

The main entry point. Loads system prompt from the skill doc, builds the Opus user message with citation index and metrics context, calls Opus 4.6, runs all four post-processing steps, and returns a `Briefing`.

- [ ] **Step 1: Write failing tests**

Add to `tests/test_phase4.py`:

```python
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
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
python tests/test_phase4.py -v -k "TestWeeklyReportGenerate"
```

Expected: `ImportError: cannot import name 'generate_weekly_report'` for all 3 tests.

- [ ] **Step 3: Add system prompt builder and generate_weekly_report() to weekly_report.py**

Append after `_score_report()`:

```python
# ══════════════════════════════════════════════════════════════════════════════
# System prompt
# ══════════════════════════════════════════════════════════════════════════════

def _extract_code_block(text: str) -> str:
    """Extract content from the first ``` code block."""
    match = re.search(r"```\n(.*?)```", text, re.DOTALL)
    if match:
        return match.group(1).strip()
    return text


def _build_system_prompt(insight_history_block: str, briefing_date: str) -> str:
    """Build the Opus system prompt from the template in docs/skills/weekly-report.md.

    Injects:
      {insight_history_block} — last 4 weeks of insight history
      {month_name}            — e.g. "March"
      {seasonal_weight}       — from simulation/config.py SEASONAL_WEIGHTS
      {seasonal_note}         — plain-English seasonal interpretation
    """
    try:
        doc = _SKILL_DOC_PATH.read_text()
        template_section = _extract_section(doc, "## System Prompt Template")
        template = _extract_code_block(template_section)
    except Exception as exc:
        logger.error("Could not load system prompt template from %s: %s", _SKILL_DOC_PATH, exc)
        template = (
            "You are a business analyst for Sparkle & Shine Cleaning Co. "
            "Write a weekly business intelligence report.\n\n"
            "PREVIOUSLY REPORTED INSIGHTS:\n{insight_history_block}\n\n"
            "SEASONAL CONTEXT:\nCurrent month: {month_name} (weight: {seasonal_weight})\n{seasonal_note}"
        )

    d = date.fromisoformat(briefing_date)
    month_num = d.month
    month_nm = d.strftime("%B")
    weight = SEASONAL_WEIGHTS.get(month_num, 1.0)
    note = _SEASONAL_NOTES.get(month_num, "")

    return template.format(
        insight_history_block=insight_history_block,
        month_name=month_nm,
        seasonal_weight=weight,
        seasonal_note=note,
    )


# ══════════════════════════════════════════════════════════════════════════════
# Main entry point
# ══════════════════════════════════════════════════════════════════════════════

def generate_weekly_report(context: BriefingContext, dry_run: bool = False) -> Briefing:
    """Generate the weekly business intelligence report using Claude Opus 4.6.

    Pipeline:
      1. Load insight history → build PREVIOUSLY REPORTED block
      2. Build citation index from context.metrics
      3. Build system prompt (template from docs/skills/weekly-report.md)
      4. Call Opus 4.6 (skipped in dry_run)
      5. Post-process:
           a. _extract_and_update_insights() — strip [insight_id:] markers, update history
           b. _strip_low_confidence()        — remove LOW-confidence sentences
           c. _inject_citations()            — replace [R01] with Slack mrkdwn links
           d. _score_report()               — Sonnet quality score on final text

    Returns a Briefing with report_type="weekly" and model_used="claude-opus-4-6".
    Compatible with the existing runner's post_briefing() call.
    """
    start_time = time.time()

    # ── Step 1: Insight history ───────────────────────────────────────────
    history = _load_insight_history()
    insight_block = _build_insight_history_block(history)

    # ── Step 2: Citation index ────────────────────────────────────────────
    citation_index = _build_citation_index(context)

    # ── Step 3: System prompt ─────────────────────────────────────────────
    system_prompt = _build_system_prompt(insight_block, context.date)

    # ── Build citation index block for user message ───────────────────────
    citation_block = "CITATION INDEX (use ref_ids inline when citing):\n"
    for entry in citation_index:
        citation_block += (
            f"  [{entry['ref_id']}] {entry['claim']} "
            f"(confidence: {entry.get('confidence', 'MEDIUM')})\n"
        )

    user_message = (
        f"{citation_block}\n\n"
        f"DATA AND CONTEXT:\n{context.context_document}"
    )

    # ── Dry run: skip API call ────────────────────────────────────────────
    if dry_run:
        logger.info("[DRY RUN] Would call Opus 4.6 with %d-char context", len(user_message))
        return Briefing(
            date=context.date,
            content_slack="[DRY RUN] Weekly report not generated.",
            content_plain="[DRY RUN] Weekly report not generated.",
            model_used="dry_run",
            input_tokens=0,
            output_tokens=0,
            generation_time_seconds=0.0,
            retry_count=0,
            report_type="weekly",
        )

    # ── Step 4: Opus API call ─────────────────────────────────────────────
    import anthropic
    client = anthropic.Anthropic()

    retry_count = 0
    raw_text = ""
    input_tokens = 0
    output_tokens = 0

    for attempt in range(3):
        try:
            response = client.messages.create(
                model=MODEL_CONFIG["weekly_model"],
                max_tokens=MODEL_CONFIG["max_tokens_weekly"],
                temperature=MODEL_CONFIG["temperature_weekly"],
                system=system_prompt,
                messages=[{"role": "user", "content": user_message}],
            )
            raw_text = response.content[0].text
            input_tokens = response.usage.input_tokens
            output_tokens = response.usage.output_tokens
            break
        except Exception as exc:
            retry_count += 1
            if attempt < 2:
                wait = 2 ** attempt
                logger.warning("Opus call failed (attempt %d): %s — retrying in %ds", attempt + 1, exc, wait)
                time.sleep(wait)
            else:
                logger.error("Opus call failed after 3 attempts: %s", exc)
                raise

    # ── Step 5a: Extract insight markers and update history ───────────────
    cleaned, updated_history = _extract_and_update_insights(raw_text, history)
    _save_insight_history(updated_history)

    # ── Step 5b: Strip LOW-confidence content ─────────────────────────────
    cleaned, removed = _strip_low_confidence(cleaned, citation_index)
    if removed > 0:
        logger.warning("Removed %d LOW-confidence claims from weekly report", removed)

    # ── Step 5c: Inject citation links ────────────────────────────────────
    final_text = _inject_citations(cleaned, citation_index)

    # ── Step 5d: Quality score on final text ──────────────────────────────
    score = _score_report(final_text)
    generation_time = round(time.time() - start_time, 2)

    if score > 0:
        logger.info("Weekly report quality score: %d/100", score)
    if 0 < score < 60:
        logger.warning("Weekly report quality score %d is below threshold (60)", score)

    return Briefing(
        date=context.date,
        content_slack=final_text,
        content_plain=final_text,
        model_used=MODEL_CONFIG["weekly_model"],
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        generation_time_seconds=generation_time,
        retry_count=retry_count,
        report_type="weekly",
    )
```

- [ ] **Step 4: Run all weekly report tests**

```bash
python tests/test_phase4.py -v -k "TestWeeklyReport"
```

Expected: All tests across `TestWeeklyReportInsightHistory`, `TestWeeklyReportConfidenceFilter`, `TestWeeklyReportCitations`, `TestWeeklyReportQualityScoring`, and `TestWeeklyReportGenerate` pass.

- [ ] **Step 5: Run the full non-integration test suite to check for regressions**

```bash
python tests/test_phase4.py -v -k "not live and not slack_channel"
```

Expected: All existing tests still pass; no regressions.

- [ ] **Step 6: Commit**

```bash
git add intelligence/weekly_report.py tests/test_phase4.py
git commit -m "Add generate_weekly_report() with system prompt and full pipeline (Task 8)"
```

---

### Task 9: Wire runner.py to use generate_weekly_report()

**Files:**
- Modify: `intelligence/runner.py`

The runner already has `--report-type weekly`, `build_weekly_context()`, and archives with `"weekly_report"` prefix. The only change needed is in Stage 4 (Generate) and a channel setup call before Stage 5 (Publish).

- [ ] **Step 1: Find the generate_briefing call in runner.py**

```bash
grep -n "generate_briefing\|report_type\|weekly" intelligence/runner.py | head -30
```

Look for the line that calls `generate_briefing(context, ...)`. This is in Stage 4.

- [ ] **Step 2: Update Stage 4 in runner.py**

Find the Stage 4 generate block. It will look like:

```python
briefing = generate_briefing(context, dry_run=args.dry_run)
```

Replace with:

```python
if args.report_type == "weekly":
    from intelligence.weekly_report import generate_weekly_report
    briefing = generate_weekly_report(context, dry_run=args.dry_run)
else:
    briefing = generate_briefing(context, dry_run=args.dry_run)
```

- [ ] **Step 3: Add ensure_channel call before Stage 5 (Publish)**

Find the Stage 5 publish block. It will contain the call to `post_briefing(briefing, ...)`.

Immediately before the `post_briefing` call, add:

```python
if args.report_type == "weekly" and not args.dry_run:
    from intelligence.slack_publisher import ensure_channel
    ensure_channel(SLACK_CONFIG["weekly_channel"])
```

- [ ] **Step 4: Verify dry-run runs without errors**

```bash
cd /Users/ovieoghor/Documents/Claude\ Code\ Exercises/Simulation\ Exercise/sparkle-shine-poc
python -m intelligence.runner --skip-sync --date 2026-03-23 --report-type weekly --dry-run 2>&1 | tail -20
```

Expected output (no crash, dry-run confirmed):
```
[DRY RUN] Would call Opus 4.6 with ...-char context
[DRY RUN] Weekly report not generated.
```

- [ ] **Step 5: Run the full test suite once more**

```bash
python tests/test_phase4.py -v -k "not live and not slack_channel"
```

Expected: All tests pass.

- [ ] **Step 6: Commit**

```bash
git add intelligence/runner.py
git commit -m "Wire runner.py weekly path to generate_weekly_report() with ensure_channel (Task 9)"
```

---

## Self-Review

**Spec coverage check:**

| Spec requirement | Task |
|-----------------|------|
| `simulation/deep_links.py` with get_deep_link, get_report_link, format_citation | Task 2 |
| QBO sandbox vs production URL detection via QBO_BASE_URL | Task 2 |
| Pipedrive subdomain lazy cache via /users/me | Task 2 |
| HubSpot portal ID lazy cache via /integrations/v1/me | Task 2 |
| Asana project_gid from tool_ids.json with search fallback | Task 2 |
| Graceful degradation to "#" / plain text on failure | Task 2 |
| `ensure_channel()` in slack_publisher.py | Task 3 |
| System 1: Insight history load/save/block/extract | Task 4 |
| Insight graduation at times_reported >= 3 | Task 4 |
| System 2: POST-GENERATION string validation for LOW confidence | Task 5 |
| System 3: Citation index + ref_id injection | Task 6 |
| `weekly_model`, `max_tokens_weekly`, `temperature_weekly` in config.py | Task 1 |
| System 4: Quality scoring with verbatim rubric from skill doc | Task 7 |
| `generate_weekly_report()` returning Briefing | Task 8 |
| Post-processing order: insights → confidence → citations → score | Task 8 |
| System prompt loaded from weekly-report.md, not hard-coded | Task 8 |
| Seasonal context injection ({month_name}, {seasonal_weight}, {seasonal_note}) | Task 8 |
| runner.py weekly path routing | Task 9 |
| ensure_channel called before post_briefing | Task 9 |

**No gaps found.**

**Placeholder scan:** All code blocks contain complete implementations. No "TBD" or "TODO" in code. ✓

**Type consistency check:**
- `_extract_and_update_insights(text, history) -> tuple[str, dict]` — used in Task 4 tests and Task 8 implementation. ✓
- `_strip_low_confidence(text, citation_index) -> tuple[str, int]` — consistent across Task 5 tests and Task 8 call. ✓
- `_inject_citations(text, citation_index) -> str` — consistent across Task 6 and Task 8. ✓
- `generate_weekly_report(context, dry_run) -> Briefing` — used in Task 8 tests and Task 9 runner. ✓
- `Briefing` imported from `intelligence.briefing_generator` in weekly_report.py — matches existing type. ✓
