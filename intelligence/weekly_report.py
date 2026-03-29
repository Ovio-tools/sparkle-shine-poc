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

    # Preprocess: mark sentences that are followed by [LOW]
    # Replace ". [LOW] " with a sentinel that we can detect after splitting
    text = re.sub(r'([.!?])\s*\[LOW\]\s+', r'\1 <<<MARK_PREV_FOR_REMOVAL>>> ', text)

    # Split on sentence boundaries
    sentences = re.split(r'(?<=[.!?])\s+', text)

    removed = 0
    kept = []

    for i, sentence in enumerate(sentences):
        should_remove = False
        has_marker = "<<<MARK_PREV_FOR_REMOVAL>>>" in sentence

        # If this sentence has the marker, remove the previous one from kept list
        if has_marker:
            if kept:
                kept.pop()
                removed += 1

        # Check if this sentence contains [LOW] tag
        if "[LOW]" in sentence:
            should_remove = True

        # Check if sentence starts with [LOW]
        elif sentence.strip().startswith("[LOW]"):
            should_remove = True

        # Check if sentence references a LOW-confidence ref_id
        elif low_refs and any(f"[{ref}]" in sentence for ref in low_refs):
            should_remove = True

        if should_remove:
            removed += 1
        else:
            # Clean up any remaining markers and add to kept
            cleaned = re.sub(r'\s*<<<MARK_PREV_FOR_REMOVAL>>>\s*', ' ', sentence).strip()
            if cleaned:
                kept.append(cleaned)

    return " ".join(kept), removed


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
