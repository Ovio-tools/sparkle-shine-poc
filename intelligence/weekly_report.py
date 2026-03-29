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
