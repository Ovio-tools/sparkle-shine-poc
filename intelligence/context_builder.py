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
) -> str:
    """Build the full context document string from metrics data."""
    lines: list[str] = []

    rev = metrics.get("revenue", {})
    ops = metrics.get("operations", {})
    fin = metrics.get("financial_health", {})
    sales = metrics.get("sales", {})
    mkt = metrics.get("marketing", {})
    tasks = metrics.get("tasks", {})

    # ---- Header ----
    lines.append(f"# Daily Briefing Data -- {date_formatted}")
    lines.append("")

    # ---- YESTERDAY'S NUMBERS ----
    lines.append("## YESTERDAY'S NUMBERS")
    yesterday_ops = ops.get("yesterday", {})
    yesterday_rev = rev.get("yesterday", {})
    lines.append(f"- Jobs completed: {yesterday_ops.get('completed', 0)}")
    lines.append(f"- Jobs cancelled: {yesterday_ops.get('cancelled', 0)}")
    completion_rate = yesterday_ops.get("completion_rate", 0.0)
    lines.append(f"- Completion rate: {completion_rate:.0%}")
    lines.append(f"- Revenue collected: ${yesterday_rev.get('total', 0.0):,.0f}")
    lines.append(f"  - Residential: ${yesterday_rev.get('residential', 0.0):,.0f}")
    lines.append(f"  - Commercial: ${yesterday_rev.get('commercial', 0.0):,.0f}")
    lines.append(f"- Average job value: ${yesterday_rev.get('avg_job_value', 0.0):,.0f}")
    lines.append("")

    # ---- REVENUE TREND ----
    lines.append("## REVENUE TREND")
    wtd = rev.get("week_to_date", {})
    mtd = rev.get("month_to_date", {})
    direction = wtd.get("vs_last_week_direction", "flat")
    vs_last_week = wtd.get("vs_last_week", 0.0)
    target_low = mtd.get("target_low", 0.0)
    target_high = mtd.get("target_high", 0.0)
    lines.append(
        f"- Week to date: ${wtd.get('total', 0.0):,.0f} "
        f"({direction} {abs(vs_last_week):.1f}% vs last week)"
    )
    lines.append(f"- Month to date: ${mtd.get('total', 0.0):,.0f}")
    lines.append(f"- Monthly target range: ${target_low:,.0f} - ${target_high:,.0f}")
    lines.append(f"- Pacing: {mtd.get('pacing', 'unknown')}")
    lines.append(f"- Projected month-end: ${mtd.get('projected_month_end', 0.0):,.0f}")
    lines.append("")

    # ---- CASH POSITION ----
    lines.append("## CASH POSITION")
    cash = fin.get("cash_position", {})
    ar_aging = fin.get("ar_aging", {})
    lines.append(f"- Bank balance: ${cash.get('bank_balance', 0.0):,.0f}")
    lines.append(f"- Total receivable: ${cash.get('total_ar', 0.0):,.0f}")
    lines.append(f"- Days sales outstanding: {fin.get('dso', 0.0):.0f} days")
    lines.append("- AR aging:")

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

    # ---- TODAY'S SCHEDULE ----
    lines.append("## TODAY'S SCHEDULE")
    today_sched = ops.get("today_schedule", {})
    lines.append(f"- Total jobs: {today_sched.get('total_jobs', 0)}")

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
            f"- GAPS: {gap['crew']} has capacity -- only {util:.0%} utilized "
            f"({avail:.1f}h open)"
        )

    for ol in today_sched.get("overloaded", []):
        util = ol.get("utilization", 0.0)
        lines.append(f"- WARNING: {ol['crew']} is at {util:.0%} utilization")

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
            # variance is stored as raw percent (e.g. 20.0 means 20%), use :+.0f
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

    movement = sales.get("movement_yesterday", {})
    new_leads = movement.get("new_leads", 0)
    stage_advances = movement.get("stage_advances", 0)
    won = movement.get("deals_won", 0)
    won_value = movement.get("won_value", 0.0)
    lost = movement.get("deals_lost", 0)
    lines.append(
        f"- Yesterday: {new_leads} new leads, {stage_advances} advances, "
        f"{won} won (${won_value:,.0f}), {lost} lost"
    )
    lines.append(f"- Average cycle length: {sales.get('avg_cycle_length_days', 0.0):.0f} days")

    stale_deals = sales.get("stale_deals", [])
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
    conversion = sales.get("conversion_by_source", {})
    # Merge avg_ltv from marketing lead source performance where available
    mkt_lead_perf = mkt.get("lead_source_performance", {})
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
    campaign = mkt.get("recent_campaign", {})
    audience = mkt.get("audience_health", {})
    reviews = mkt.get("review_summary_7day", {})

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
    lines.append("## TASK STATUS")
    task_overview = tasks.get("overview", {})
    total_open_tasks = task_overview.get("total_open", 0)
    total_overdue_tasks = task_overview.get("total_overdue", 0)
    task_overdue_rate = task_overview.get("overdue_rate", 0.0)
    lines.append(
        f"- Open: {total_open_tasks}, Overdue: {total_overdue_tasks} "
        f"({task_overdue_rate:.0%})"
    )

    by_assignee = tasks.get("by_assignee", {})
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

    critical_overdue = tasks.get("critical_overdue", [])
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
        date_formatted, metrics, all_alerts, recent_briefings
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
