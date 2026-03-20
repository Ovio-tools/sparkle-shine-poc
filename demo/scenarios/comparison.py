"""
demo/scenarios/comparison.py

Side-by-side comparison of two ScenarioResult objects.
Compares key metrics and provides a heuristic briefing tone analysis.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from demo.scenarios.scenario_runner import ScenarioResult

# ---------------------------------------------------------------------------
# Tone heuristics
# ---------------------------------------------------------------------------

_CAUTIOUS_WORDS = {
    "warning", "concern", "risk", "decline", "declining", "below", "shortfall",
    "gap", "miss", "missed", "overdue", "late", "churn", "churned", "negative",
    "lost", "loss", "alert", "critical", "unstable", "tight", "tightening",
    "slow", "falling", "dropped", "shortage", "understaffed",
}

_OPTIMISTIC_WORDS = {
    "growth", "opportunity", "strong", "ahead", "surge", "win", "won", "above",
    "exceeding", "record", "peak", "healthy", "recovering", "recovery", "improved",
    "improving", "gained", "gaining", "launch", "launched", "referral", "new contract",
    "closed", "booked", "pipeline", "momentum", "positive", "great", "excellent",
}


def _classify_tone(text: str) -> tuple[str, int, int]:
    """Classify briefing tone.

    Returns (classification, cautious_count, optimistic_count).
    classification is one of: "cautious", "neutral", "optimistic".
    """
    words = re.findall(r"\b\w+\b", text.lower())
    cautious = sum(1 for w in words if w in _CAUTIOUS_WORDS)
    optimistic = sum(1 for w in words if w in _OPTIMISTIC_WORDS)

    if cautious > optimistic * 1.5:
        tone = "cautious"
    elif optimistic > cautious * 1.5:
        tone = "optimistic"
    else:
        tone = "neutral"

    return tone, cautious, optimistic


# ---------------------------------------------------------------------------
# Metric extraction helpers
# ---------------------------------------------------------------------------

def _get_revenue(metrics: dict) -> dict:
    return metrics.get("revenue", {})


def _get_operations(metrics: dict) -> dict:
    return metrics.get("operations", {})


def _get_financial_health(metrics: dict) -> dict:
    return metrics.get("financial_health", {})


def _get_sales(metrics: dict) -> dict:
    return metrics.get("sales", {})


def _get_tasks(metrics: dict) -> dict:
    return metrics.get("tasks", {})


def _fmt_pacing(rev: dict) -> str:
    """Return a human-readable pacing label from the revenue dict."""
    mtd = rev.get("month_to_date", {})
    pacing = mtd.get("pacing", "")
    if isinstance(pacing, str) and pacing:
        return pacing
    projected = mtd.get("projected_month_end", 0)
    target_high = mtd.get("target_high", 0)
    if projected and target_high:
        diff_pct = (projected - target_high) / target_high
        if diff_pct > 0.05:
            return "ahead of target"
        elif diff_pct < -0.05:
            return "below target"
        return "on track"
    return "unknown"


def _unique_alerts(alerts_a: list[str], alerts_b: list[str]) -> tuple[list[str], list[str], list[str]]:
    """Return (only_in_a, only_in_b, in_both) based on alert text overlap."""
    set_a = {a.strip() for a in alerts_a}
    set_b = {a.strip() for a in alerts_b}

    def _similar(text_a: str, text_b: str) -> bool:
        # Consider two alerts the same if they share 3+ common words (>3 chars)
        wa = {w.lower() for w in re.findall(r"\b\w{4,}\b", text_a)}
        wb = {w.lower() for w in re.findall(r"\b\w{4,}\b", text_b)}
        return len(wa & wb) >= 3

    matched_b: set[str] = set()
    in_both: list[str] = []
    only_a: list[str] = []

    for a in set_a:
        found_match = False
        for b in set_b:
            if b not in matched_b and _similar(a, b):
                in_both.append(a)
                matched_b.add(b)
                found_match = True
                break
        if not found_match:
            only_a.append(a)

    only_b = [b for b in set_b if b not in matched_b]
    return sorted(only_a), sorted(only_b), sorted(in_both)


def _collect_all_alerts(metrics: dict) -> list[str]:
    alerts: list[str] = []
    for module_data in metrics.values():
        if isinstance(module_data, dict):
            alerts.extend(module_data.get("alerts", []))
    return alerts


# ---------------------------------------------------------------------------
# Main comparison function
# ---------------------------------------------------------------------------

def compare_scenarios(result_a: "ScenarioResult", result_b: "ScenarioResult") -> str:
    """Generate a side-by-side comparison of two ScenarioResults.

    Returns a formatted multi-line string suitable for printing or saving.
    """
    name_a = result_a.scenario_name
    name_b = result_b.scenario_name
    date_a = result_a.date
    date_b = result_b.date

    m_a = result_a.context.metrics
    m_b = result_b.context.metrics

    rev_a = _get_revenue(m_a)
    rev_b = _get_revenue(m_b)
    ops_a = _get_operations(m_a)
    ops_b = _get_operations(m_b)
    fin_a = _get_financial_health(m_a)
    fin_b = _get_financial_health(m_b)
    sales_a = _get_sales(m_a)
    sales_b = _get_sales(m_b)
    tasks_a = _get_tasks(m_a)
    tasks_b = _get_tasks(m_b)

    lines: list[str] = []

    # -----------------------------------------------------------------------
    # Header
    # -----------------------------------------------------------------------
    sep = "=" * 57
    lines.append(sep)
    lines.append("SCENARIO COMPARISON")
    lines.append(f"{name_a} ({date_a}) vs. {name_b} ({date_b})")
    lines.append(sep)

    # -----------------------------------------------------------------------
    # Revenue
    # -----------------------------------------------------------------------
    yest_a = rev_a.get("yesterday", {})
    yest_b = rev_b.get("yesterday", {})
    mtd_a = rev_a.get("month_to_date", {})
    mtd_b = rev_b.get("month_to_date", {})

    rev_yest_a = yest_a.get("total", 0)
    rev_yest_b = yest_b.get("total", 0)
    rev_mtd_a = mtd_a.get("total", 0)
    rev_mtd_b = mtd_b.get("total", 0)
    pacing_a = _fmt_pacing(rev_a)
    pacing_b = _fmt_pacing(rev_b)

    # Monthly-equivalent: compare projected_month_end or trailing_30_days
    proj_a = mtd_a.get("projected_month_end", rev_a.get("trailing_30_days", {}).get("total", 0))
    proj_b = mtd_b.get("projected_month_end", rev_b.get("trailing_30_days", {}).get("total", 0))
    rev_diff = proj_b - proj_a
    direction = "+" if rev_diff >= 0 else "-"

    lines.append("")
    lines.append("REVENUE")
    lines.append(
        f"  {name_a}: ${rev_yest_a:,.0f} yesterday, MTD ${rev_mtd_a:,.0f} ({pacing_a})"
    )
    lines.append(
        f"  {name_b}: ${rev_yest_b:,.0f} yesterday, MTD ${rev_mtd_b:,.0f} ({pacing_b})"
    )
    lines.append(f"  Delta: {direction}${abs(rev_diff):,.0f}/month (projected)")

    # -----------------------------------------------------------------------
    # Operations
    # -----------------------------------------------------------------------
    yest_ops_a = ops_a.get("yesterday", {})
    yest_ops_b = ops_b.get("yesterday", {})
    jobs_a = yest_ops_a.get("completed", 0)
    jobs_b = yest_ops_b.get("completed", 0)
    rate_a = yest_ops_a.get("completion_rate", 0)
    rate_b = yest_ops_b.get("completion_rate", 0)

    lines.append("")
    lines.append("OPERATIONS")
    lines.append(f"  {name_a}: {rate_a:.0%} completion, {jobs_a} jobs completed yesterday")
    lines.append(f"  {name_b}: {rate_b:.0%} completion, {jobs_b} jobs completed yesterday")

    # -----------------------------------------------------------------------
    # Cash Health
    # -----------------------------------------------------------------------
    dso_a = fin_a.get("dso", 0)
    dso_b = fin_b.get("dso", 0)
    ar_a = fin_a.get("ar_aging", {}).get("past_due_61_90", {}).get("total", 0) + \
           fin_a.get("ar_aging", {}).get("past_due_90_plus", {}).get("total", 0)
    ar_b = fin_b.get("ar_aging", {}).get("past_due_61_90", {}).get("total", 0) + \
           fin_b.get("ar_aging", {}).get("past_due_90_plus", {}).get("total", 0)

    lines.append("")
    lines.append("CASH HEALTH")
    lines.append(f"  {name_a}: DSO {dso_a:.0f} days, AR 60+ ${ar_a:,.0f}")
    lines.append(f"  {name_b}: DSO {dso_b:.0f} days, AR 60+ ${ar_b:,.0f}")

    # -----------------------------------------------------------------------
    # Sales Pipeline
    # -----------------------------------------------------------------------
    pipe_a = sales_a.get("pipeline_summary", {})
    pipe_b = sales_b.get("pipeline_summary", {})
    deals_a = pipe_a.get("total_open_deals", 0)
    deals_b = pipe_b.get("total_open_deals", 0)
    val_a = pipe_a.get("total_pipeline_value", 0)
    val_b = pipe_b.get("total_pipeline_value", 0)

    lines.append("")
    lines.append("SALES PIPELINE")
    lines.append(f"  {name_a}: {deals_a} open deals, ${val_a:,.0f} pipeline value")
    lines.append(f"  {name_b}: {deals_b} open deals, ${val_b:,.0f} pipeline value")

    # -----------------------------------------------------------------------
    # Task Health
    # -----------------------------------------------------------------------
    overview_a = tasks_a.get("overview", {})
    overview_b = tasks_b.get("overview", {})
    overdue_a = overview_a.get("total_overdue", 0)
    overdue_b = overview_b.get("total_overdue", 0)
    overdue_rate_a = overview_a.get("overdue_rate", 0)
    overdue_rate_b = overview_b.get("overdue_rate", 0)

    lines.append("")
    lines.append("TASK HEALTH")
    lines.append(f"  {name_a}: {overdue_a} overdue ({overdue_rate_a:.0%})")
    lines.append(f"  {name_b}: {overdue_b} overdue ({overdue_rate_b:.0%})")

    # -----------------------------------------------------------------------
    # Alert Comparison
    # -----------------------------------------------------------------------
    alerts_a = _collect_all_alerts(m_a)
    alerts_b = _collect_all_alerts(m_b)
    only_a, only_b, in_both = _unique_alerts(alerts_a, alerts_b)

    lines.append("")
    lines.append("ALERT COMPARISON")

    if only_a:
        lines.append(f"  Only in {name_a}:")
        for a in only_a:
            lines.append(f"    • {a}")
    else:
        lines.append(f"  Only in {name_a}: (none)")

    if only_b:
        lines.append(f"  Only in {name_b}:")
        for a in only_b:
            lines.append(f"    • {a}")
    else:
        lines.append(f"  Only in {name_b}: (none)")

    if in_both:
        lines.append("  In both:")
        for a in in_both:
            lines.append(f"    • {a}")
    else:
        lines.append("  In both: (none)")

    # -----------------------------------------------------------------------
    # Briefing Tone Shift
    # -----------------------------------------------------------------------
    lines.append("")
    lines.append("BRIEFING TONE SHIFT")

    text_a = result_a.briefing.content_plain if result_a.briefing else ""
    text_b = result_b.briefing.content_plain if result_b.briefing else ""

    if not text_a and not text_b:
        lines.append("  (No briefings generated — dry run)")
    else:
        tone_a, caut_a, opt_a = _classify_tone(text_a) if text_a else ("—", 0, 0)
        tone_b, caut_b, opt_b = _classify_tone(text_b) if text_b else ("—", 0, 0)

        lines.append(
            f"  {name_a}: {tone_a.upper()} "
            f"(cautious words: {caut_a}, optimistic words: {opt_a})"
        )
        lines.append(
            f"  {name_b}: {tone_b.upper()} "
            f"(cautious words: {caut_b}, optimistic words: {opt_b})"
        )

        # Narrative tone shift
        if tone_a != tone_b:
            shift = f"Tone shifts from {tone_a} → {tone_b} between these periods."
        else:
            shift = f"Both periods share a {tone_a} tone."
        lines.append(f"  {shift}")

    lines.append("")
    lines.append(sep)

    return "\n".join(lines)
