"""
intelligence/context_builder.py

Assembles the structured context document passed to Claude for briefing
generation. Calls the metrics engine and document search, then formats
everything into a clean text document.

Usage:
    python -m intelligence.context_builder --date 2026-03-17
    python -m intelligence.context_builder --date yesterday
    python -m intelligence.context_builder --date today
"""

from __future__ import annotations

import argparse
import glob
import logging
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import date, timedelta

from intelligence.documents.doc_search import search_for_alert_context
from intelligence.metrics import compute_all_metrics

logger = logging.getLogger(__name__)

MAX_DOC_LOOKUPS = 5
TOKEN_WARNING_THRESHOLD = 5000
RECENT_BRIEFING_MAX_WORDS = 300  # truncate each past briefing to keep token overhead manageable

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DEFAULT_BRIEFINGS_DIR = os.path.join(_PROJECT_ROOT, "briefings")


@dataclass
class BriefingContext:
    date: str
    date_formatted: str             # "Tuesday, March 17, 2026"
    metrics: dict                   # full output from compute_all_metrics
    document_excerpts: list[dict]   # top doc excerpts attached to alerts
    context_document: str           # the formatted text document for Claude
    token_estimate: int             # rough token count of context_document
    recent_briefings_loaded: int = 0  # number of past briefings injected as context
    report_type: str = "daily"        # "daily" or "weekly"


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def _fmt_date(briefing_date: str) -> str:
    """Format ISO date as 'Tuesday, March 17, 2026' (no zero-padded day)."""
    d = date.fromisoformat(briefing_date)
    return d.strftime("%A, %B ") + str(d.day) + d.strftime(", %Y")


def _resolve_date(date_arg: str) -> str:
    """Resolve --date argument to ISO date string."""
    today = date.today()
    if date_arg == "today":
        return str(today)
    if date_arg == "yesterday":
        return str(today - timedelta(days=1))
    return date_arg


# ---------------------------------------------------------------------------
# Recent briefing history loader
# ---------------------------------------------------------------------------

def _load_recent_briefings(
    briefings_dir: str,
    current_date: str,
    n: int = 3,
) -> list[tuple[str, str]]:
    """Load the N most recent briefing files strictly before current_date.

    Returns a list of (iso_date, cleaned_content) tuples, oldest first
    (so Claude reads them in chronological order).
    """
    pattern = os.path.join(briefings_dir, "briefing_*.md")
    paths = glob.glob(pattern)

    dated: list[tuple[str, str]] = []
    for path in paths:
        filename = os.path.basename(path)
        m = re.match(r"briefing_(\d{4}-\d{2}-\d{2})\.md$", filename)
        if m and m.group(1) < current_date:
            dated.append((m.group(1), path))

    # Most recent N, then reverse to chronological order for reading
    dated.sort(key=lambda x: x[0], reverse=True)
    recent = list(reversed(dated[:n]))

    result: list[tuple[str, str]] = []
    for file_date, path in recent:
        try:
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
            # Strip metadata footer ("---\nModel: ...")
            content = re.sub(r"\n---\nModel:.*$", "", content, flags=re.DOTALL)
            # Strip the # Daily Briefing — YYYY-MM-DD header line
            content = re.sub(r"^#[^\n]*\n+", "", content).strip()
            # Truncate to RECENT_BRIEFING_MAX_WORDS
            words = content.split()
            if len(words) > RECENT_BRIEFING_MAX_WORDS:
                content = " ".join(words[:RECENT_BRIEFING_MAX_WORDS]) + " [...]"
            result.append((file_date, content))
        except Exception:
            logger.debug("Could not load recent briefing: %s", path, exc_info=True)

    return result


def _format_recent_briefings_section(
    recent_briefings: list[tuple[str, str]],
) -> list[str]:
    """Format past briefings as a RECENT BRIEFING HISTORY section."""
    if not recent_briefings:
        return []

    lines: list[str] = []
    lines.append("## RECENT BRIEFING HISTORY")
    lines.append(
        "The following briefings were already delivered to Maria. "
        "Use them to avoid repeating issues verbatim and to note whether "
        "situations have improved, worsened, or remained the same."
    )
    lines.append("")

    for iso_date, content in recent_briefings:
        d = date.fromisoformat(iso_date)
        label = d.strftime("%A, %B ") + str(d.day) + d.strftime(", %Y")
        lines.append(f"### {label}")
        lines.append(content)
        lines.append("")

    return lines


# ---------------------------------------------------------------------------
# Context document formatter
# ---------------------------------------------------------------------------

def _format_context_document(
    date_formatted: str,
    metrics: dict,
    all_alerts: list[dict],
    recent_briefings: list[tuple[str, str]] | None = None,
    failed_tools: list[str] | None = None,
) -> str:
    """Build the daily context document string from metrics data.

    Sections included (aligned with DAILY_REPORT_PROMPT 6-section structure):
      YESTERDAY'S NUMBERS, TODAY'S OPERATIONS SNAPSHOT, CASH POSITION,
      INVOICES CROSSING OVERDUE THRESHOLDS TODAY, CREW PERFORMANCE (7-day rolling),
      SALES PIPELINE, DEALS NEEDING A NUDGE, HIGH-PRIORITY OVERDUE TASKS,
      ALERTS AND FLAGS, RECENT BRIEFING HISTORY.

    Deliberately excluded (per daily report spec):
      REVENUE TREND, MARKETING / campaign performance, CONVERSION BY SOURCE.
    """
    lines: list[str] = []

    rev = metrics.get("revenue", {})
    ops = metrics.get("operations", {})
    fin = metrics.get("financial_health", {})
    sales = metrics.get("sales", {})
    mkt = metrics.get("marketing", {})
    tsk = metrics.get("tasks", {})

    # ---- Header ----
    lines.append(f"# Daily Briefing Data -- {date_formatted}")
    lines.append("")

    # ---- DATA GAPS (sync failures) ----
    if failed_tools:
        lines.append("## DATA GAPS — SYNC FAILURES")
        lines.append(
            "The following tools failed to sync. Their data may be stale or "
            "incomplete. DO NOT guess or fabricate numbers for affected metrics. "
            "Instead, explicitly state that the data is unavailable due to a "
            "sync failure."
        )
        _TOOL_IMPACT = {
            "jobber": "Job completions, crew schedules, and recurring agreements",
            "quickbooks": "Revenue, invoices, payments, and cash position",
            "hubspot": "Contacts, leads, and marketing metrics",
            "pipedrive": "Sales pipeline and deal progression",
            "asana": "Task completion and overdue tasks",
            "mailchimp": "Email campaign metrics",
            "google": "Google Workspace documents, calendar, and email metadata",
            "slack": "Slack message history",
        }
        for tool in failed_tools:
            impact = _TOOL_IMPACT.get(tool, "Unknown")
            lines.append(f"- {tool}: FAILED — affected metrics: {impact}")
        lines.append("")

    # ---- YESTERDAY'S NUMBERS ----
    lines.append("## YESTERDAY'S NUMBERS")
    yesterday_ops = ops.get("yesterday", {})
    yesterday_rev = rev.get("yesterday", {})
    completion_rate = yesterday_ops.get("completion_rate", 0.0)
    lines.append(f"- Jobs completed: {yesterday_ops.get('completed', 0)}")
    lines.append(f"- Revenue collected: ${yesterday_rev.get('total', 0.0):,.0f}")
    lines.append(f"- Completion rate: {completion_rate:.0%}")

    # Flag if notably off
    # Derive daily run rate from projected_month_end / 30 as a rough proxy
    mtd_data = rev.get("month_to_date", {})
    projected_month_end = mtd_data.get("projected_month_end", 0.0)
    daily_run_rate = projected_month_end / 30.0 if projected_month_end > 0 else 0.0
    yesterday_total_rev = yesterday_rev.get("total", 0.0)
    if completion_rate < 0.90:
        lines.append(
            f"- FLAG: Completion rate dropped to {completion_rate:.0%} — below the 90% threshold."
        )
    elif daily_run_rate > 0 and yesterday_total_rev < daily_run_rate * 0.80:
        shortfall_pct = (1 - yesterday_total_rev / daily_run_rate) * 100
        lines.append(
            f"- FLAG: Revenue was {shortfall_pct:.0f}% below the daily run rate "
            f"(${daily_run_rate:,.0f}/day expected)."
        )
    lines.append("")

    # ---- TODAY'S OPERATIONS SNAPSHOT ----
    lines.append("## TODAY'S OPERATIONS SNAPSHOT")
    today_sched = ops.get("today_schedule", {})
    lines.append(f"- Total jobs scheduled today: {today_sched.get('total_jobs', 0)}")

    by_crew = today_sched.get("by_crew", {})
    if by_crew:
        lines.append("- Crew breakdown:")
        for crew_name, crew_data in by_crew.items():
            jobs = crew_data.get("jobs", 0)
            hours = crew_data.get("hours", 0.0)
            utilization = crew_data.get("utilization", 0.0)
            lines.append(
                f"  - {crew_name}: {jobs} jobs, {hours:.1f}h scheduled, "
                f"{utilization:.0%} utilized"
            )

    for gap in today_sched.get("gaps", []):
        avail = gap.get("available_hours", 0.0)
        util = gap.get("utilization", 0.0)
        lines.append(
            f"- CAPACITY AVAILABLE: {gap['crew']} at only {util:.0%} utilization "
            f"({avail:.1f}h open) — could slot a last-minute booking"
        )

    for ol in today_sched.get("overloaded", []):
        util = ol.get("utilization", 0.0)
        lines.append(
            f"- OVERLOADED: {ol['crew']} is at {util:.0%} utilization "
            f"— one cancellation from a difficult day"
        )

    # Overnight cancellations from yesterday
    overnight_cancelled = yesterday_ops.get("cancelled", 0)
    if overnight_cancelled > 0:
        lines.append(f"- Overnight cancellations: {overnight_cancelled} job(s) cancelled")

    lines.append("")

    # ---- CASH POSITION ----
    lines.append("## CASH POSITION")
    cash = fin.get("cash_position", {})
    ar_aging = fin.get("ar_aging", {})
    lines.append(f"- Bank balance: ${cash.get('bank_balance', 0.0):,.0f}")
    lines.append(f"- Total receivable: ${cash.get('total_ar', 0.0):,.0f}")
    lines.append(f"- Avg. days to collect payment: {fin.get('dso', 0.0):.0f} days")
    lines.append("- Invoices by age:")

    ar_buckets = [
        ("current_0_30",    "Current (0-30 days)"),
        ("past_due_31_60",  "31-60 days"),
        ("past_due_61_90",  "61-90 days"),
        ("past_due_90_plus","90+ days"),
    ]
    for key, label in ar_buckets:
        bucket = ar_aging.get(key, {"total": 0.0, "count": 0})
        lines.append(
            f"  - {label}: ${bucket.get('total', 0.0):,.0f} "
            f"from {bucket.get('count', 0)} invoices"
        )

    late_payers = fin.get("late_payers", [])
    if late_payers:
        lines.append("- Late payers requiring attention:")
        for lp in late_payers[:5]:
            lines.append(
                f"  - {lp['client_name']}: ${lp['amount']:,.0f} outstanding, "
                f"{lp['days_overdue']} days overdue"
            )
        if len(late_payers) > 5:
            rest = late_payers[5:]
            rest_total = sum(lp["amount"] for lp in rest)
            lines.append(f"  - ...and {len(rest)} more totalling ${rest_total:,.0f}")
    lines.append("")

    # ---- INVOICES CROSSING OVERDUE THRESHOLDS TODAY ----
    lines.append("## INVOICES CROSSING OVERDUE THRESHOLDS TODAY")
    crossings = fin.get("ar_threshold_crossings", [])
    if crossings:
        lines.append("Invoices that just crossed the 30-day or 60-day overdue mark:")
        for c in crossings:
            lines.append(
                f"- {c['client_name']}: ${c['amount']:,.0f} just hit {c['days_overdue']} days "
                f"(crossed {c['threshold_crossed']}-day threshold)"
            )
    else:
        lines.append("- No invoices crossed a 30-day or 60-day threshold today.")
    lines.append("")

    # ---- CREW PERFORMANCE (7-day rolling) ----
    lines.append("## CREW PERFORMANCE (7-day rolling)")
    crew_perf = ops.get("crew_performance_7day", {})
    if crew_perf:
        for crew_name, perf in crew_perf.items():
            rating = perf.get("avg_rating")
            variance = perf.get("avg_duration_variance", 0.0)
            jobs = perf.get("jobs", 0)
            rating_str = f"{rating:.1f}/5" if rating is not None else "N/A"
            lines.append(
                f"- {crew_name}: avg rating {rating_str}, "
                f"avg duration variance {variance:+.0f}%, {jobs} jobs"
            )
    else:
        lines.append("- No crew performance data for the past 7 days")
    lines.append("")

    # ---- SALES PIPELINE ----
    lines.append("## SALES PIPELINE")
    pipeline = sales.get("pipeline_summary", {})
    total_open_deals = pipeline.get("total_open_deals", 0)
    total_pipeline_value = pipeline.get("total_pipeline_value", 0.0)
    lines.append(f"- Open deals: {total_open_deals} worth ${total_pipeline_value:,.0f}/year")

    by_stage = pipeline.get("by_stage", {})
    if by_stage:
        lines.append("- By stage:")
        for stage_name, stage_data in by_stage.items():
            count = stage_data.get("count", 0)
            value = stage_data.get("value", 0.0)
            lines.append(f"  - {stage_name}: {count} deals, ${value:,.0f}")
    lines.append("")

    # ---- DEALS NEEDING A NUDGE ----
    # Combine stale deals (14+ days) and proposals needing a nudge (7-13 days),
    # de-duplicate by deal title, sort by value, cap at 3.
    lines.append("## DEALS NEEDING A NUDGE")
    stale_deals = sales.get("stale_deals", [])
    nudge_proposals = sales.get("proposals_needing_nudge", [])

    # Merge: stale_deals have "stage", nudge_proposals do not — tag them
    nudge_combined: list[dict] = []
    seen_titles: set[str] = set()
    for d in stale_deals:
        title = d.get("deal_title", "")
        if title not in seen_titles:
            nudge_combined.append({
                "deal_title": title,
                "days_stale": d.get("days_stale", 0),
                "value": d.get("value", 0.0),
                "stage": d.get("stage", ""),
            })
            seen_titles.add(title)
    for d in nudge_proposals:
        title = d.get("deal_title", "")
        if title not in seen_titles:
            nudge_combined.append({
                "deal_title": title,
                "days_stale": d.get("days_stale", 0),
                "value": d.get("value", 0.0),
                "stage": "Proposal Sent",
            })
            seen_titles.add(title)

    # Sort by value descending and cap at 3
    nudge_combined.sort(key=lambda x: x["value"], reverse=True)
    top_nudges = nudge_combined[:3]

    if top_nudges:
        for nd in top_nudges:
            stage_note = f" ({nd['stage']})" if nd.get("stage") else ""
            lines.append(
                f"- {nd['deal_title']}{stage_note}: "
                f"${nd['value']:,.0f}/year, {nd['days_stale']} days idle"
            )
        if len(nudge_combined) > 3:
            lines.append(
                f"- ...and {len(nudge_combined) - 3} more deals needing follow-up "
                f"(showing top 3 by value)"
            )
    else:
        lines.append("- No deals currently stale or needing a nudge.")
    lines.append("")

    # ---- HIGH-PRIORITY OVERDUE TASKS ----
    lines.append("## HIGH-PRIORITY OVERDUE TASKS")
    hp_by_project = tsk.get("high_priority_overdue_by_project", {})
    if hp_by_project:
        parts = [f"{cnt} {proj}" for proj, cnt in hp_by_project.items()]
        lines.append("- " + ", ".join(parts))
    else:
        lines.append("- No high-priority tasks currently overdue.")
    lines.append("")

    # ---- OVERNIGHT NEGATIVE REVIEWS (1-star only) ----
    # Include only if a 1-star review arrived — this is the exception carved
    # out from the daily exclusion list for marketing/review data.
    reviews = mkt.get("review_summary_7day", {})
    negative_details = reviews.get("negative_details", [])
    one_star = [nd for nd in negative_details if nd.get("rating", 5) <= 1]
    if one_star:
        lines.append("## OVERNIGHT 1-STAR REVIEWS")
        for nd in one_star:
            excerpt = nd.get("excerpt", "")
            lines.append(
                f'- {nd["rating"]} stars on {nd["platform"]}: '
                f'"{excerpt}" — {nd["client_name"]}'
            )
        lines.append("")

    # ---- ALERTS AND FLAGS ----
    lines.append("## ALERTS AND FLAGS")
    if all_alerts:
        for alert in all_alerts:
            source = alert.get("source", "unknown")
            text = alert.get("text", "")
            lines.append(f"- [{source}] {text}")
            excerpt_info = alert.get("document_excerpt")
            if excerpt_info:
                doc_title = excerpt_info.get("title", "Unknown")
                excerpt_text = excerpt_info.get("relevant_excerpt", "")
                lines.append(f'  Related doc: {doc_title} -- "{excerpt_text}"')
    else:
        lines.append("- No active alerts")
    lines.append("")

    # ---- RECENT BRIEFING HISTORY (appended last so metrics come first) ----
    if recent_briefings:
        lines.extend(_format_recent_briefings_section(recent_briefings))

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_briefing_context(
    db_path: str,
    briefing_date: str,
    include_doc_search: bool = True,
    briefings_dir: str | None = None,
    recent_briefings_count: int | None = None,
    failed_tools: list[str] | None = None,
) -> BriefingContext:
    """Assemble the full briefing context for the given date.

    Args:
        db_path: Path to sparkle_shine.db
        briefing_date: ISO date string, e.g. "2026-03-17"
        include_doc_search: Whether to search docs for alert context
        briefings_dir: Directory containing past briefing_*.md files.
                       Defaults to <project_root>/briefings.
        recent_briefings_count: How many past briefings to inject as context.
                                Defaults to RECENT_BRIEFINGS_COUNT from config.
        failed_tools: List of tool names whose syncers failed (e.g.
                      ["jobber", "google"]).  Passed through to the context
                      document so the LLM knows which data is unavailable.

    Returns:
        BriefingContext with all fields populated.
    """
    from intelligence.config import RECENT_BRIEFINGS_COUNT

    logger.info("Building briefing context for %s", briefing_date)

    _briefings_dir = briefings_dir or _DEFAULT_BRIEFINGS_DIR
    _n = recent_briefings_count if recent_briefings_count is not None else RECENT_BRIEFINGS_COUNT

    # Step 1: Compute all metrics
    metrics = compute_all_metrics(db_path, briefing_date)
    logger.info("Metrics computed at %s", metrics.get("computed_at"))

    # Step 2: Collect all alerts across all 6 metrics modules
    all_alerts: list[dict] = []
    for module_name, module_result in metrics.items():
        if isinstance(module_result, dict) and "alerts" in module_result:
            for alert_text in module_result["alerts"]:
                all_alerts.append({"source": module_name, "text": alert_text})
    logger.info("Collected %d total alerts", len(all_alerts))

    # Step 3: Attach document excerpts to alerts (cap at MAX_DOC_LOOKUPS)
    document_excerpts: list[dict] = []
    if include_doc_search:
        lookup_count = 0
        for alert in all_alerts:
            if lookup_count >= MAX_DOC_LOOKUPS:
                break
            try:
                results = search_for_alert_context(db_path, alert["text"])
            except Exception:
                logger.debug("Doc search failed for alert: %s", alert["text"][:60], exc_info=True)
                results = []
            if results:
                top = results[0]
                alert["document_excerpt"] = top
                document_excerpts.append(top)
                lookup_count += 1
                logger.debug(
                    "Alert '%s...' matched doc '%s'",
                    alert["text"][:60],
                    top.get("title", ""),
                )

    # Step 4: Load recent briefings for continuity context
    recent_briefings: list[tuple[str, str]] = []
    if _n > 0 and os.path.isdir(_briefings_dir):
        recent_briefings = _load_recent_briefings(_briefings_dir, briefing_date, n=_n)
        logger.info("Loaded %d recent briefing(s) as context", len(recent_briefings))
    elif _n > 0:
        logger.debug("Briefings directory not found: %s — skipping history", _briefings_dir)

    # Step 5: Format the context document
    date_formatted = _fmt_date(briefing_date)
    context_document = _format_context_document(
        date_formatted, metrics, all_alerts, recent_briefings,
        failed_tools=failed_tools,
    )

    # Step 6: Estimate tokens; warn if large
    token_estimate = len(context_document) // 4
    if token_estimate > TOKEN_WARNING_THRESHOLD:
        logger.warning(
            "Context document is large: ~%d tokens (threshold: %d). "
            "Consider reducing sections or truncating excerpts.",
            token_estimate,
            TOKEN_WARNING_THRESHOLD,
        )
    logger.info(
        "Context document built: %d chars, ~%d tokens",
        len(context_document),
        token_estimate,
    )

    return BriefingContext(
        date=briefing_date,
        date_formatted=date_formatted,
        metrics=metrics,
        document_excerpts=document_excerpts,
        context_document=context_document,
        token_estimate=token_estimate,
        recent_briefings_loaded=len(recent_briefings),
        report_type="daily",
    )


# ---------------------------------------------------------------------------
# Weekly context builder
# ---------------------------------------------------------------------------

def _format_weekly_context_document(
    week_start: str,
    week_end: str,
    daily_metrics: list[dict],
    snapshot_metrics: dict,
    all_alerts: list[dict],
) -> str:
    """Build the weekly context document by aggregating 7 days of metrics.

    daily_metrics: list of compute_all_metrics() results, one per day (oldest first).
    snapshot_metrics: metrics from the last day of the week (for balance-sheet data).
    all_alerts: deduplicated alerts collected across all 7 days.
    """
    lines: list[str] = []

    # Derive formatted date range label
    start_d = date.fromisoformat(week_start)
    end_d = date.fromisoformat(week_end)
    week_label = (
        start_d.strftime("%B ") + str(start_d.day)
        + end_d.strftime("–%B ") + str(end_d.day)
        + end_d.strftime(", %Y")
    )

    lines.append(f"# Weekly Report Data -- Week of {week_label}")
    lines.append("")

    # ---- WEEK SUMMARY ----
    lines.append("## WEEK SUMMARY")
    total_completed = sum(
        m.get("operations", {}).get("yesterday", {}).get("completed", 0)
        for m in daily_metrics
    )
    total_cancelled = sum(
        m.get("operations", {}).get("yesterday", {}).get("cancelled", 0)
        for m in daily_metrics
    )
    total_jobs = total_completed + total_cancelled
    completion_rate = total_completed / total_jobs if total_jobs > 0 else 0.0

    total_revenue = sum(
        m.get("revenue", {}).get("yesterday", {}).get("total", 0.0)
        for m in daily_metrics
    )
    total_residential = sum(
        m.get("revenue", {}).get("yesterday", {}).get("residential", 0.0)
        for m in daily_metrics
    )
    total_commercial = sum(
        m.get("revenue", {}).get("yesterday", {}).get("commercial", 0.0)
        for m in daily_metrics
    )
    active_days = len(daily_metrics) or 1

    lines.append(f"- Week: {week_start} to {week_end} ({active_days} days of data)")
    lines.append(f"- Total jobs completed: {total_completed}")
    lines.append(f"- Total jobs cancelled: {total_cancelled}")
    lines.append(f"- Overall completion rate: {completion_rate:.0%}")
    lines.append(f"- Total revenue collected: ${total_revenue:,.0f}")
    lines.append(f"  - Residential: ${total_residential:,.0f}")
    lines.append(f"  - Commercial: ${total_commercial:,.0f}")
    lines.append(f"- Average daily jobs: {total_completed / active_days:.1f}")
    lines.append(f"- Average daily revenue: ${total_revenue / active_days:,.0f}")
    lines.append("")

    # ---- REVENUE TREND ----
    snap_rev = snapshot_metrics.get("revenue", {})
    lines.append("## REVENUE TREND")
    wtd = snap_rev.get("week_to_date", {})
    mtd = snap_rev.get("month_to_date", {})
    direction = wtd.get("vs_last_week_direction", "flat")
    vs_last_week = wtd.get("vs_last_week", 0.0)
    target_low = mtd.get("target_low", 0.0)
    target_high = mtd.get("target_high", 0.0)
    lines.append(
        f"- Week total (WTD metric): ${wtd.get('total', 0.0):,.0f} "
        f"({direction} {abs(vs_last_week):.1f}% vs prior week)"
    )
    lines.append(f"- Month to date: ${mtd.get('total', 0.0):,.0f}")
    lines.append(f"- Monthly target range: ${target_low:,.0f} - ${target_high:,.0f}")
    lines.append(f"- Pacing: {mtd.get('pacing', 'unknown')}")
    lines.append(f"- Projected month-end: ${mtd.get('projected_month_end', 0.0):,.0f}")
    lines.append("")

    # ---- CASH POSITION (end-of-week snapshot) ----
    snap_fin = snapshot_metrics.get("financial_health", {})
    lines.append("## CASH POSITION (end of week)")
    cash = snap_fin.get("cash_position", {})
    ar_aging = snap_fin.get("ar_aging", {})
    lines.append(f"- Bank balance: ${cash.get('bank_balance', 0.0):,.0f}")
    lines.append(f"- Total receivable: ${cash.get('total_ar', 0.0):,.0f}")
    lines.append(f"- Avg. days to collect payment: {snap_fin.get('dso', 0.0):.0f} days")
    lines.append("- Invoices by age:")
    ar_buckets = [
        ("current_0_30",    "Current (0-30 days)"),
        ("past_due_31_60",  "31-60 days"),
        ("past_due_61_90",  "61-90 days"),
        ("past_due_90_plus","90+ days"),
    ]
    for key, label in ar_buckets:
        bucket = ar_aging.get(key, {"total": 0.0, "count": 0})
        lines.append(
            f"  - {label}: ${bucket.get('total', 0.0):,.0f} "
            f"from {bucket.get('count', 0)} invoices"
        )
    late_payers = snap_fin.get("late_payers", [])
    if late_payers:
        lines.append("- Late payers requiring attention:")
        for lp in late_payers[:5]:
            lines.append(
                f"  - {lp['client_name']}: ${lp['amount']:,.0f} outstanding, "
                f"{lp['days_overdue']} days overdue"
            )
        if len(late_payers) > 5:
            rest = late_payers[5:]
            rest_total = sum(lp["amount"] for lp in rest)
            lines.append(f"  - ...and {len(rest)} more totalling ${rest_total:,.0f}")

    # DSO trend (4 weeks)
    dso_trend = snap_fin.get("dso_trend", [])
    if dso_trend:
        lines.append("- Avg. days to collect payment (past 4 weeks):")
        for entry in dso_trend:
            lines.append(f"  - Week ending {entry['week_ending']}: {entry['dso']:.0f} days")
    lines.append("")

    # ---- CREW PERFORMANCE SCORECARD (week) ----
    snap_ops = snapshot_metrics.get("operations", {})
    lines.append("## CREW PERFORMANCE SCORECARD (week)")
    crew_perf = snap_ops.get("crew_performance_7day", {})
    if crew_perf:
        # Sort crews for ranking: lower duration variance is better (efficiency),
        # but also reward higher ratings. Use (variance - rating*5) as a rough rank score.
        def _crew_rank_key(item: tuple) -> float:
            _, perf = item
            variance = perf.get("avg_duration_variance", 0.0)
            rating = perf.get("avg_rating") or 0.0
            return variance - (rating * 2)  # lower = better ranked

        ranked_crews = sorted(crew_perf.items(), key=_crew_rank_key)
        lines.append("Ranked best to worst (lower duration variance + higher rating = better):")
        for rank, (crew_name, perf) in enumerate(ranked_crews, start=1):
            rating = perf.get("avg_rating")
            variance = perf.get("avg_duration_variance", 0.0)
            jobs = perf.get("jobs", 0)
            rating_str = f"{rating:.1f}/5" if rating is not None else "N/A"
            lines.append(
                f"- #{rank} {crew_name}: duration variance {variance:+.0f}%, "
                f"avg rating {rating_str}, {jobs} jobs"
            )
    else:
        lines.append("- No crew performance data for this week")
    lines.append("")

    # ---- SALES PIPELINE ----
    snap_sales = snapshot_metrics.get("sales", {})
    lines.append("## SALES PIPELINE")
    pipeline = snap_sales.get("pipeline_summary", {})
    total_open_deals = pipeline.get("total_open_deals", 0)
    total_pipeline_value = pipeline.get("total_pipeline_value", 0.0)
    lines.append(f"- Open deals: {total_open_deals} worth ${total_pipeline_value:,.0f}/year")
    by_stage = pipeline.get("by_stage", {})
    if by_stage:
        lines.append("- By stage:")
        for stage_name, stage_data in by_stage.items():
            count = stage_data.get("count", 0)
            value = stage_data.get("value", 0.0)
            lines.append(f"  - {stage_name}: {count} deals, ${value:,.0f}")

    # Aggregate week's pipeline movement
    week_new_leads = sum(
        m.get("sales", {}).get("movement_yesterday", {}).get("new_leads", 0)
        for m in daily_metrics
    )
    week_advances = sum(
        m.get("sales", {}).get("movement_yesterday", {}).get("stage_advances", 0)
        for m in daily_metrics
    )
    week_won = sum(
        m.get("sales", {}).get("movement_yesterday", {}).get("deals_won", 0)
        for m in daily_metrics
    )
    week_won_value = sum(
        m.get("sales", {}).get("movement_yesterday", {}).get("won_value", 0.0)
        for m in daily_metrics
    )
    week_lost = sum(
        m.get("sales", {}).get("movement_yesterday", {}).get("deals_lost", 0)
        for m in daily_metrics
    )
    lines.append(
        f"- This week: {week_new_leads} new leads, {week_advances} advances, "
        f"{week_won} won (${week_won_value:,.0f}), {week_lost} lost"
    )
    lines.append(
        f"- Average cycle length: {snap_sales.get('avg_cycle_length_days', 0.0):.0f} days"
    )
    stale_deals = snap_sales.get("stale_deals", [])
    if stale_deals:
        lines.append("- STALE DEALS (no activity 14+ days):")
        for deal in stale_deals:
            lines.append(
                f"  - {deal['deal_title']} at {deal['stage']}: "
                f"{deal['days_stale']} days idle, ${deal['value']:,.0f}"
            )
    lines.append("")

    # ---- CONVERSION BY SOURCE ----
    lines.append("## CONVERSION BY SOURCE")
    snap_mkt = snapshot_metrics.get("marketing", {})
    conversion = snap_sales.get("conversion_by_source", {})
    mkt_lead_perf = snap_mkt.get("lead_source_performance", {})
    if conversion:
        for src, src_data in conversion.items():
            leads = src_data.get("leads", 0)
            won_count = src_data.get("won", 0)
            rate = src_data.get("rate", 0.0)
            avg_ltv = mkt_lead_perf.get(src, {}).get("avg_ltv", 0.0) or 0.0
            lines.append(
                f"- {src}: {leads} leads, {won_count} won, "
                f"{rate:.0%} conversion, avg LTV ${avg_ltv:,.0f}"
            )
    else:
        lines.append("- No conversion data available")
    lines.append("")

    # ---- MARKETING ----
    lines.append("## MARKETING")
    campaign = snap_mkt.get("recent_campaign", {})
    audience = snap_mkt.get("audience_health", {})
    reviews = snap_mkt.get("review_summary_7day", {})
    if campaign.get("name"):
        open_rate = campaign.get("open_rate", 0.0)
        click_rate = campaign.get("click_rate", 0.0)
        conversions = campaign.get("conversions", 0)
        sent_date = campaign.get("sent_date", "unknown")
        lines.append(f"- Latest campaign: {campaign['name']} ({sent_date})")
        lines.append(
            f"  - Open rate: {open_rate:.1%}, Click rate: {click_rate:.1%}, "
            f"Conversions: {conversions}"
        )
    else:
        lines.append("- No campaigns on record")
    total_subs = audience.get("total_subscribers", 0)
    new_subs = audience.get("new_subscribers_30day", 0)
    lines.append(f"- Audience: {total_subs} subscribers, {new_subs} new in 30 days")
    total_reviews = reviews.get("total_reviews", 0)
    avg_review_rating = reviews.get("avg_rating", 0.0)
    lines.append(f"- Reviews (7 days): {total_reviews} reviews, avg {avg_review_rating:.1f}/5")
    negative_details = reviews.get("negative_details", [])
    if negative_details:
        lines.append("- NEGATIVE REVIEWS:")
        for nd in negative_details:
            excerpt = nd.get("excerpt", "")
            lines.append(
                f'  - {nd["rating"]} stars on {nd["platform"]}: '
                f'"{excerpt}" -- {nd["client_name"]}'
            )
    lines.append("")

    # ---- TASK STATUS ----
    snap_tasks = snapshot_metrics.get("tasks", {})
    lines.append("## TASK STATUS (end of week)")
    task_overview = snap_tasks.get("overview", {})
    total_open_tasks = task_overview.get("total_open", 0)
    total_overdue_tasks = task_overview.get("total_overdue", 0)
    task_overdue_rate = task_overview.get("overdue_rate", 0.0)
    lines.append(
        f"- Open: {total_open_tasks}, Overdue: {total_overdue_tasks} "
        f"({task_overdue_rate:.0%})"
    )
    by_assignee = snap_tasks.get("by_assignee", {})
    if by_assignee:
        lines.append("- By assignee:")
        for assignee_name, astats in by_assignee.items():
            open_t = astats.get("open", 0)
            overdue_t = astats.get("overdue", 0)
            rate_t = astats.get("overdue_rate", 0.0)
            lines.append(
                f"  - {assignee_name}: {open_t} open, {overdue_t} overdue "
                f"({rate_t:.0%})"
            )
    critical_overdue = snap_tasks.get("critical_overdue", [])
    if critical_overdue:
        lines.append("- CRITICAL (14+ days overdue):")
        for ct in critical_overdue[:5]:
            lines.append(
                f"  - {ct['title']} assigned to {ct['assignee']}, "
                f"{ct['days_overdue']} days overdue"
            )
        if len(critical_overdue) > 5:
            lines.append(f"  - ...and {len(critical_overdue) - 5} more critical tasks")
    lines.append("")

    # ---- NEIGHBORHOOD CANCELLATIONS (past 28 days) ----
    lines.append("## NEIGHBORHOOD CANCELLATIONS (past 28 days)")
    neighborhood_cancels = snap_ops.get("cancellation_by_neighborhood", [])
    if neighborhood_cancels:
        for entry in neighborhood_cancels:
            lines.append(
                f"- {entry['neighborhood']}: {entry['cancel_count']} cancellation(s)"
            )
    else:
        lines.append("- No neighborhood cancellation data available for this period.")
    lines.append("")

    # ---- ALERTS AND FLAGS ----
    lines.append("## ALERTS AND FLAGS (week)")
    if all_alerts:
        for alert in all_alerts:
            source = alert.get("source", "unknown")
            text = alert.get("text", "")
            lines.append(f"- [{source}] {text}")
            excerpt_info = alert.get("document_excerpt")
            if excerpt_info:
                doc_title = excerpt_info.get("title", "Unknown")
                excerpt_text = excerpt_info.get("relevant_excerpt", "")
                lines.append(f'  Related doc: {doc_title} -- "{excerpt_text}"')
    else:
        lines.append("- No active alerts this week")
    lines.append("")

    return "\n".join(lines)


def build_weekly_context(
    db_path: str,
    week_end_date: str,
    include_doc_search: bool = True,
) -> BriefingContext:
    """Assemble the weekly report context for the 7 days ending on week_end_date.

    Args:
        db_path: Path to sparkle_shine.db
        week_end_date: ISO date string of any day in the target week. The function
            snaps it to the most recent Sunday, so the report always covers
            Monday–Sunday.
        include_doc_search: Whether to search docs for alert context.

    Returns:
        BriefingContext populated with weekly aggregated data and report_type="weekly".
    """
    logger.info("Building weekly context for week ending %s", week_end_date)

    end = date.fromisoformat(week_end_date)
    # Snap to the most recent Sunday (weeks run Monday–Sunday).
    # weekday(): Mon=0 … Sun=6.  Days since last Sunday = (weekday + 1) % 7.
    days_since_sunday = (end.weekday() + 1) % 7
    end = end - timedelta(days=days_since_sunday)
    start = end - timedelta(days=6)  # the preceding Monday
    week_start_date = str(start)
    if days_since_sunday:
        logger.info(
            "week_end_date snapped from %s to most recent Sunday %s",
            week_end_date,
            end,
        )

    # Compute metrics for each of the 7 days (oldest first)
    daily_metrics: list[dict] = []
    for i in range(7):
        day = str(start + timedelta(days=i))
        try:
            m = compute_all_metrics(db_path, day)
            daily_metrics.append(m)
        except Exception:
            logger.warning("Could not compute metrics for %s — skipping", day, exc_info=True)

    if not daily_metrics:
        raise RuntimeError(
            f"No metrics could be computed for the week ending {week_end_date}"
        )

    snapshot_metrics = daily_metrics[-1]  # end-of-week snapshot for balance-sheet data

    # Collect alerts from all 7 days, deduplicated by alert text
    seen_alert_texts: set[str] = set()
    all_alerts: list[dict] = []
    for m in daily_metrics:
        for module_name, module_result in m.items():
            if isinstance(module_result, dict) and "alerts" in module_result:
                for alert_text in module_result["alerts"]:
                    if alert_text not in seen_alert_texts:
                        seen_alert_texts.add(alert_text)
                        all_alerts.append({"source": module_name, "text": alert_text})

    logger.info("Collected %d unique alerts across the week", len(all_alerts))

    # Attach document excerpts to alerts
    document_excerpts: list[dict] = []
    if include_doc_search:
        lookup_count = 0
        for alert in all_alerts:
            if lookup_count >= MAX_DOC_LOOKUPS:
                break
            try:
                results = search_for_alert_context(db_path, alert["text"])
            except Exception:
                logger.debug("Doc search failed for alert: %s", alert["text"][:60], exc_info=True)
                results = []
            if results:
                top = results[0]
                alert["document_excerpt"] = top
                document_excerpts.append(top)
                lookup_count += 1

    context_document = _format_weekly_context_document(
        week_start=week_start_date,
        week_end=week_end_date,
        daily_metrics=daily_metrics,
        snapshot_metrics=snapshot_metrics,
        all_alerts=all_alerts,
    )

    token_estimate = len(context_document) // 4
    if token_estimate > TOKEN_WARNING_THRESHOLD:
        logger.warning(
            "Weekly context document is large: ~%d tokens (threshold: %d).",
            token_estimate,
            TOKEN_WARNING_THRESHOLD,
        )
    logger.info(
        "Weekly context document built: %d chars, ~%d tokens",
        len(context_document),
        token_estimate,
    )

    date_formatted = (
        _fmt_date(week_start_date)
        + " – "
        + _fmt_date(week_end_date)
    )

    return BriefingContext(
        date=week_end_date,
        date_formatted=date_formatted,
        metrics=snapshot_metrics,
        document_excerpts=document_excerpts,
        context_document=context_document,
        token_estimate=token_estimate,
        recent_briefings_loaded=0,
        report_type="weekly",
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _main() -> None:
    parser = argparse.ArgumentParser(
        description="Build and print the daily briefing context document.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python -m intelligence.context_builder --date 2026-03-17\n"
            "  python -m intelligence.context_builder --date yesterday\n"
            "  python -m intelligence.context_builder --date today --no-doc-search\n"
        ),
    )
    parser.add_argument(
        "--date",
        default="today",
        metavar="DATE",
        help="Briefing date as YYYY-MM-DD, 'today', or 'yesterday' (default: today)",
    )
    parser.add_argument(
        "--no-doc-search",
        action="store_true",
        help="Skip document search for alert context",
    )
    parser.add_argument(
        "--db",
        default=None,
        metavar="PATH",
        help="Path to sparkle_shine.db (default: auto-detected from project root)",
    )
    args = parser.parse_args()

    briefing_date = _resolve_date(args.date)

    # Auto-detect db path relative to project root (two levels up from this file)
    if args.db:
        db_path = args.db
    else:
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        db_path = os.path.join(project_root, "sparkle_shine.db")

    if not os.path.exists(db_path):
        print(f"Error: database not found at {db_path}", file=sys.stderr)
        sys.exit(1)

    ctx = build_briefing_context(
        db_path=db_path,
        briefing_date=briefing_date,
        include_doc_search=not args.no_doc_search,
    )

    print(ctx.context_document)
    print("─" * 60)

    total_alerts = sum(
        len(m["alerts"])
        for m in ctx.metrics.values()
        if isinstance(m, dict) and "alerts" in m
    )
    print(f"Token estimate : ~{ctx.token_estimate:,} tokens")
    print(f"Total alerts   : {total_alerts}")
    print(f"Doc lookups    : {len(ctx.document_excerpts)}")


if __name__ == "__main__":
    _main()
