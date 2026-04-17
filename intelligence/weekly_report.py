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

    Each entry has a pre-built UI URL (or "#" which degrades to plain text),
    a record_count pulled from metrics so Opus knows how much data backs each
    claim, and a confidence level.

    Covers all 6 weekly report sections:
      Section 1 (Week in Review)       → QBO P&L
      Section 2 (Crew Performance)     → Jobber client list
      Section 3 (Cash Flow)            → QBO AR Aging
      Section 4 (Sales Pipeline)       → Pipedrive deals
      Section 5 (Marketing/Reputation) → HubSpot contacts + Mailchimp campaigns
      Section 6 (Task Health)          → Asana Admin & Operations + Client Success
    """
    from simulation.deep_links import get_report_link, get_deep_link

    citations = []
    _counter = [0]

    def _next_ref() -> str:
        _counter[0] += 1
        return f"R{_counter[0]:02d}"

    def _add(
        claim: str,
        tool: str,
        record_type: str,
        url: str,
        confidence: str = "HIGH",
        record_count=None,
    ) -> None:
        entry: dict = {
            "ref_id": _next_ref(),
            "claim": claim,
            "tool": tool,
            "record_type": record_type,
            "record_id": None,
            "url": url,
            "confidence": confidence,
        }
        if record_count is not None:
            entry["record_count"] = record_count
        citations.append(entry)

    metrics = context.metrics or {}
    ops  = metrics.get("operations", {})
    fin  = metrics.get("financial_health", {})
    sales = metrics.get("sales", {})
    mktg  = metrics.get("marketing", {})
    tasks = metrics.get("tasks", {})

    # Shared: weekly job count from crew_performance_7day (sum across all crews)
    week_jobs: int | None = (
        sum(v.get("jobs", 0) for v in ops.get("crew_performance_7day", {}).values())
        or None
    )

    # ── Section 1 (Week in Review) — Revenue ─────────────────────────────────
    if metrics.get("booked_revenue") or metrics.get("revenue"):
        _add("Weekly P&L", "quickbooks", "report_pl",
             get_report_link("quickbooks"),
             record_count=week_jobs)

    # ── Section 2 (Crew Performance) — Operations ────────────────────────────
    if ops:
        _add("Jobber Jobs", "jobber", "client",
             get_deep_link("jobber", "client", ""),
             record_count=week_jobs)

    # ── Section 3 (Cash Flow) — AR Aging ─────────────────────────────────────
    if fin.get("ar_aging") or fin.get("cash_position"):
        ar_count: int | None = sum(
            fin.get("ar_aging", {}).get(k, {}).get("count", 0)
            for k in ("current_0_30", "past_due_31_60", "past_due_61_90", "past_due_90_plus")
        ) or None
        _add("AR Aging Report", "quickbooks", "report_ar",
             get_deep_link("quickbooks", "report_ar", ""),
             record_count=ar_count)

    # ── Section 4 (Sales Pipeline) — Pipedrive ───────────────────────────────
    if sales:
        deal_count = sales.get("pipeline_summary", {}).get("total_open_deals") or None
        _add("Pipedrive Pipeline", "pipedrive", "deal",
             get_deep_link("pipedrive", "deal", ""),
             record_count=deal_count)

    # ── Section 5 (Marketing & Reputation) — HubSpot + Mailchimp ─────────────
    if mktg:
        hs_count = mktg.get("audience_health", {}).get("total_subscribers") or None
        _add("HubSpot Contacts", "hubspot", "contact",
             get_deep_link("hubspot", "contact", ""),
             record_count=hs_count)
        mc_count = 1 if mktg.get("recent_campaign") else None
        _add("Mailchimp Campaigns", "mailchimp", "campaign",
             get_deep_link("mailchimp", "campaign", ""),
             confidence="MEDIUM", record_count=mc_count)

    # ── Section 6 (Task & Delegation Health) — Asana ─────────────────────────
    if tasks:
        task_count = tasks.get("overview", {}).get("total_open") or None
        for project_name in ("Admin & Operations", "Client Success"):
            _add(f"Asana {project_name}", "asana", project_name,
                 get_deep_link("asana", project_name, "list"),
                 record_count=task_count)

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
    client = anthropic.Anthropic(timeout=60.0)  # scoring call — short timeout

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
        count = entry.get("record_count")
        count_str = f"{count} records this week, " if count is not None else ""
        citation_block += (
            f"  [{entry['ref_id']}] {entry['claim']} "
            f"({count_str}confidence: {entry.get('confidence', 'MEDIUM')})\n"
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
    client = anthropic.Anthropic(timeout=120.0)  # 2-min per-attempt ceiling

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

    score_line = f"quality_score: {score}" if score > 0 else "quality_score: unavailable"
    archive_header = (
        f"---\n"
        f"report_type: weekly\n"
        f"date: {context.date}\n"
        f"model: {MODEL_CONFIG['weekly_model']}\n"
        f"{score_line}\n"
        f"---\n\n"
    )

    return Briefing(
        date=context.date,
        content_slack=final_text,           # Slack post — no header
        content_plain=archive_header + final_text,  # Archive — includes score header
        model_used=MODEL_CONFIG["weekly_model"],
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        generation_time_seconds=generation_time,
        retry_count=retry_count,
        report_type="weekly",
    )
