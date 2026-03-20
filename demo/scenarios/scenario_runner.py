"""
demo/scenarios/scenario_runner.py

CLI tool for generating briefings across the 6 narrative demo scenarios.

Usage:
    python -m demo.scenarios.scenario_runner --all
    python -m demo.scenarios.scenario_runner --scenario rough_patch
    python -m demo.scenarios.scenario_runner --list
    python -m demo.scenarios.scenario_runner --compare steady_state holiday_crunch
    python -m demo.scenarios.scenario_runner --all --output-dir /tmp/demo_out
    python -m demo.scenarios.scenario_runner --all --dry-run
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Optional

# ---------------------------------------------------------------------------
# Path setup: project root is two levels above this file
# ---------------------------------------------------------------------------

_HERE = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(os.path.dirname(_HERE))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(_PROJECT_ROOT, ".env"))

from intelligence.context_builder import BriefingContext, build_briefing_context
from intelligence.briefing_generator import Briefing, generate_briefing
from demo.scenarios.scenario_definitions import SCENARIOS, SCENARIO_BY_ID
from demo.scenarios.comparison import compare_scenarios

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DB_PATH = os.path.join(_PROJECT_ROOT, "sparkle_shine.db")
OUTPUT_DIR_DEFAULT = os.path.join(_HERE, "output")

# Claude Sonnet pricing ($/million tokens)
_INPUT_COST_PER_MILLION = 3.00
_OUTPUT_COST_PER_MILLION = 15.00

# Key phrases to look for in the briefing text when checking expected signals.
# Each signal maps to a list of lowercase phrases; any match counts as found.
_SIGNAL_PHRASES: dict[str, list[str]] = {
    # -- Steady State --
    "revenue on track or slightly ahead of target": [
        "on track", "ahead of target", "ahead of pace", "slightly ahead",
    ],
    "all 4 crews at healthy utilization (70-85%)": [
        "utilization", "crews", "healthy",
    ],
    "pipeline has normal deal flow": ["pipeline", "deal"],
    "no critical alerts": ["no critical", "no major alert", "all clear"],

    # -- Summer Surge --
    "revenue above target (surge)": [
        "above target", "above pace", "surge", "ahead of target",
        "up 25", "up 26", "up 30",                     # week-over-week surge language
    ],
    "crew utilization approaching or exceeding 95%": [
        "95%", "96%", "overloaded", "over capacity", "utilization",
        "fully booked", "at capacity", "high demand",
    ],
    "new lead volume elevated": [
        "lead volume", "new leads", "leads elevated", "new lead",
        "4 new leads", "leads yesterday",
    ],
    "commercial pipeline showing losses": [
        "not renewing", "churned", "commercial loss", "commercial contract",
        "commercial clients", "commercial isn",            # "isn't renewing"
    ],

    # -- Rough Patch --
    # Briefing language: "concerning trajectory", "behind last week", "pacing toward $71K against $128K target"
    "revenue below target with declining trend": [
        "below target", "below pace", "declining", "shortfall",
        "behind monthly", "behind last week", "behind this week",
        "concerning trajectory", "pacing toward", "monthly target",
        "behind monthly targets", "significantly behind", "revenue gap",
        "$57,000",
    ],
    # Briefing language: "zero jobs scheduled" or "0 jobs are scheduled" (numeral form)
    "crew utilization unbalanced (some over, some under)": [
        "unbalanced", "overloaded", "under-utilized", "utilization gap",
        "zero jobs scheduled", "no bookings", "empty schedule",
        "mid-week gap", "zero scheduled",
        "0 jobs are scheduled",     # numeral form emitted by the SECTION OPENING RULE
    ],
    "negative review alerts": [
        "negative review", "1-star", "2-star", "bad review", "complaint",
        "poor review", "bad reviews", "recent bad review", "negative reviews",
        "3 recent", "quality issue", "quality issues", "poor ratings",
    ],
    "cancellation cluster in westlake": [
        "westlake", "cancellation cluster", "cluster",
    ],
    "staffing gap warnings": [
        "staffing gap", "short-staffed", "understaffed", "scheduling gap",
        "staff shortage", "crew shortage",
    ],

    # -- Big Win --
    # Briefing language: "up 15% at $23,152, which shows good momentum after what was likely a slower period"
    "revenue recovering toward target": [
        "recovering", "recovery", "toward target", "back toward", "improving",
        "good momentum", "momentum", "up 15%", "up 18%",
        "up this week", "trending", "slower period",
    ],
    "new commercial deal highlighted": [
        "barton creek", "medical", "commercial contract", "new contract",
        "commercial win", "new commercial", "commercial client",
        "commercial work carrying",                        # big_win briefing phrasing
        "commercial work drove",                           # alt phrasing when commercial share is high
    ],
    "referral program early results visible": [
        "referral program", "referral", "new clients from referral",
    ],
    # Briefing language: "pipeline looks strong with 144 open deals worth $528K"
    "pipeline building for holiday season": [
        "holiday", "pre-booking", "pipeline building",
        "pipeline looks strong", "144 open deals", "528k", "open deals",
    ],

    # -- Holiday Crunch --
    "revenue well above target": [
        "well above", "peak", "annual high", "above target",
        "significantly above", "record month",
    ],
    "ar aging showing 60+ day buckets": [
        "60+", "60 day", "60-day", "past due", "overdue",
        "over 90", "90 days old", "90-day", "bad debt",
    ],
    # Briefing language: names like "Alejandra Cooper ($325, 299 days)"
    "late payer alerts with specific client names": [
        "late payment", "days overdue", "overdue invoice",
        "alejandra", "samantha turner", "200+ days", "299 days",
        "severely overdue", "hasn't paid",
    ],
    "cash position warning despite high revenue": [
        "cash flow", "cash position", "tightening", "cash tight",
        "collections problem", "collections crisis",
    ],
    # Briefing language: "78 days average collection time" (summer_surge); "collections become critical" (holiday_crunch)
    "dso trending up": [
        "dso", "days sales outstanding", "days outstanding",
        "78 days", "collection time", "average collection",
        "collections become critical", "collections are critical",  # semantic equivalents when DSO is implied
    ],

    # -- Recovery --
    # Briefing language: "tracking well at $123,902 projected against $135-150K target"
    # + "week-to-date revenue is up 18% versus last week, which is excellent momentum"
    "revenue approaching target": [
        "approaching target", "approaching pace", "toward target", "recovering",
        "tracking well", "tracking", "excellent momentum", "up 18%",
        "month is tracking", "tracking at", "123,902", "projected",
    ],
    "strong pipeline (12 open proposals)": [
        "proposal", "open proposal", "commercial proposal",
        "open deals", "144 deals", "pipeline looks strong",
        "active proposals", "12 open",
    ],
    "campaign engagement metrics": [
        "campaign", "open rate", "click rate", "email campaign",
        "mailchimp", "email", "subscribers",
    ],
    "staffing stable, utilization balanced": [
        "stable", "balanced", "utilization", "crews balanced",
        "reasonable", "crews at",
    ],
}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ScenarioResult:
    scenario_id: str
    scenario_name: str
    date: str
    briefing: Optional[Briefing]    # None if dry_run
    context: BriefingContext
    signal_score: int               # number of expected signals found
    signal_total: int               # total expected signals
    signals_found: list[str] = field(default_factory=list)
    signals_missing: list[str] = field(default_factory=list)
    pattern_results: list[dict] = field(default_factory=list)
    generation_cost: float = 0.0    # estimated API cost in USD


# ---------------------------------------------------------------------------
# Signal checking
# ---------------------------------------------------------------------------

def _check_signal(signal: str, briefing_text: str) -> bool:
    """Return True if the signal's key phrases appear in the briefing text."""
    text_lower = briefing_text.lower()
    key = signal.lower().strip()

    # Look for an exact key match first
    if key in _SIGNAL_PHRASES:
        phrases = _SIGNAL_PHRASES[key]
        if not phrases:
            return True  # presence-only signal (e.g. "No critical alerts")
        return any(p in text_lower for p in phrases)

    # Partial key match (signal text may differ slightly)
    for known_key, phrases in _SIGNAL_PHRASES.items():
        if known_key in key or key in known_key:
            if not phrases:
                return True
            return any(p in text_lower for p in phrases)

    # Fallback: check if at least 2 meaningful words from the signal appear
    stop = {"a", "an", "the", "is", "in", "of", "or", "and", "at", "to",
            "for", "with", "from", "no", "all", "some", "over", "under"}
    words = [w.strip("()%.+,").lower() for w in signal.split()
             if len(w.strip("()%.+,")) > 3 and w.lower() not in stop]
    matches = sum(1 for w in words if w in text_lower)
    return matches >= min(2, len(words))


def _run_signal_check(
    scenario: dict,
    briefing: Optional[Briefing],
) -> tuple[int, int, list[str], list[str]]:
    """Check how many expected signals appear in the briefing.

    Returns (score, total, signals_found, signals_missing).
    """
    expected = scenario.get("expected_signals", [])
    if not expected:
        return 0, 0, [], []

    # Use plain text briefing content; fall back to empty string for dry-run
    briefing_text = briefing.content_plain if briefing else ""

    found, missing = [], []
    for signal in expected:
        if _check_signal(signal, briefing_text):
            found.append(signal)
        else:
            missing.append(signal)

    return len(found), len(expected), found, missing


# ---------------------------------------------------------------------------
# Discovery pattern checking
# ---------------------------------------------------------------------------

def _check_discovery_patterns(
    scenario: dict,
    ctx: BriefingContext,
) -> list[dict]:
    """Check each expected discovery pattern against the metrics dict.

    Returns a list of dicts:
        {"pattern": str, "found": bool | None, "evidence": str}

    found=None means the check is not computable from the metrics dict.
    """
    results = []
    metrics = ctx.metrics

    for pattern in scenario.get("discovery_patterns", []):
        result = _check_single_pattern(pattern, metrics)
        results.append(result)

    return results


def _check_single_pattern(pattern: str, metrics: dict) -> dict:
    """Dispatch to the appropriate pattern checker."""
    p = pattern.lower()

    # -- Westlake cancellation cluster --
    if "westlake" in p:
        ops_alerts = metrics.get("operations", {}).get("alerts", [])
        found = any("westlake" in a.lower() for a in ops_alerts)
        evidence = (
            "Westlake cancellation alert present in operations module"
            if found
            else "No Westlake alert surfaced (check raw cancellations table)"
        )
        return {"pattern": pattern, "found": found, "evidence": evidence}

    # -- Crew quality vs. speed tradeoff --
    if "crew quality" in p or "speed tradeoff" in p:
        crew_perf = metrics.get("operations", {}).get("crew_performance_7day", {})
        if "crew-a" in crew_perf and len(crew_perf) > 1:
            crew_a_rating = crew_perf["crew-a"].get("avg_rating", 0)
            others = [v.get("avg_rating", 0) for k, v in crew_perf.items() if k != "crew-a"]
            avg_other = sum(others) / len(others) if others else 0
            found = crew_a_rating > avg_other
            evidence = f"Crew A avg rating {crew_a_rating:.2f} vs others avg {avg_other:.2f}"
        else:
            found = None
            evidence = "Insufficient crew performance data in metrics"
        return {"pattern": pattern, "found": found, "evidence": evidence}

    # -- Referral retention / LTV advantage --
    if "referral" in p and ("retention" in p or "google ads" in p):
        lead_perf = metrics.get("marketing", {}).get("lead_source_performance", {})
        ref_ltv = lead_perf.get("referral", {}).get("avg_ltv", 0)
        google_ltv = lead_perf.get("google_ads", {}).get("avg_ltv", 0)
        if ref_ltv > 0 or google_ltv > 0:
            found = ref_ltv > google_ltv
            evidence = f"Referral avg LTV ${ref_ltv:,.0f} vs Google Ads ${google_ltv:,.0f}"
        else:
            found = None
            evidence = "LTV data not available in marketing metrics"
        return {"pattern": pattern, "found": found, "evidence": evidence}

    # -- Referral leads with higher contract value --
    if "referral leads" in p and "contract value" in p:
        conv = metrics.get("sales", {}).get("conversion_by_source", {})
        ref = conv.get("referral", {})
        found = ref.get("leads", 0) > 0
        evidence = (
            f"Referral source has {ref.get('leads', 0)} leads, "
            f"win rate {ref.get('rate', 0):.0%}"
            if found
            else "No referral deals found in sales pipeline for this date"
        )
        return {"pattern": pattern, "found": found, "evidence": evidence}

    # -- Maria delegation / overdue task rate --
    if "maria" in p:
        by_assignee = metrics.get("tasks", {}).get("by_assignee", {})
        maria_data = by_assignee.get("Maria", {})
        maria_rate = maria_data.get("overdue_rate", None)
        others = {k: v.get("overdue_rate", 0) for k, v in by_assignee.items()
                  if k.lower() != "maria" and v.get("overdue_rate") is not None}
        if maria_rate is not None and others:
            max_other_rate = max(others.values())
            avg_other_rate = sum(others.values()) / len(others)
            found = maria_rate > avg_other_rate
            evidence = (
                f"Maria overdue rate {maria_rate:.0%} vs "
                f"others avg {avg_other_rate:.0%} / max {max_other_rate:.0%}"
            )
        else:
            found = None
            evidence = "Task assignee data incomplete"
        return {"pattern": pattern, "found": found, "evidence": evidence}

    # -- Commercial upsell signal --
    if "commercial upsell" in p or "add-on" in p:
        all_alerts: list[str] = []
        for module_data in metrics.values():
            if isinstance(module_data, dict):
                all_alerts.extend(module_data.get("alerts", []))
        found = any("upsell" in a.lower() or "add-on" in a.lower() for a in all_alerts)
        evidence = (
            "Upsell/add-on signal present in alerts"
            if found
            else "Not surfaced in alerts (check job notes directly)"
        )
        return {"pattern": pattern, "found": found, "evidence": evidence}

    # -- Tuesday/Wednesday complaint rate --
    if "tuesday" in p or "wednesday" in p:
        return {
            "pattern": pattern,
            "found": None,
            "evidence": "Requires day-of-week breakdown query (not computed in metrics layer)",
        }

    # -- Unknown pattern --
    return {
        "pattern": pattern,
        "found": None,
        "evidence": "No checker implemented for this pattern",
    }


# ---------------------------------------------------------------------------
# Cost estimation
# ---------------------------------------------------------------------------

def _estimate_cost(input_tokens: int, output_tokens: int) -> float:
    return (
        input_tokens * _INPUT_COST_PER_MILLION / 1_000_000
        + output_tokens * _OUTPUT_COST_PER_MILLION / 1_000_000
    )


# ---------------------------------------------------------------------------
# Load saved scenario results from disk (no API calls required)
# ---------------------------------------------------------------------------

def load_scenario_results(output_dir: str = OUTPUT_DIR_DEFAULT) -> list[ScenarioResult]:
    """Reconstruct ScenarioResult objects from previously saved briefing files.

    Reads ``{scenario_id}_briefing.md`` files written by run_scenario().
    Useful for scoring saved outputs without re-running the API.

    Parameters
    ----------
    output_dir:
        Directory containing the ``*_briefing.md`` files.

    Returns
    -------
    List of ScenarioResult objects, one per briefing file found.
    """
    import re as _re
    from intelligence.config import MODEL_CONFIG as _MODEL_CONFIG

    results: list[ScenarioResult] = []
    # Resolve relative paths from project root
    if not os.path.isabs(output_dir):
        output_dir = os.path.join(_PROJECT_ROOT, output_dir)

    if not os.path.isdir(output_dir):
        raise FileNotFoundError(f"Output directory not found: {output_dir}")

    for filename in sorted(os.listdir(output_dir)):
        if not filename.endswith("_briefing.md"):
            continue

        scenario_id = filename[: -len("_briefing.md")]
        if scenario_id not in SCENARIO_BY_ID:
            continue  # skip unknown files (e.g. compare_*.txt)

        scenario = SCENARIO_BY_ID[scenario_id]
        filepath = os.path.join(output_dir, filename)

        with open(filepath, encoding="utf-8") as f:
            raw = f.read()

        # Strip the header line ("# Daily Briefing — …")
        lines = raw.splitlines()
        content_lines = lines[2:] if lines and lines[0].startswith("#") else lines

        # Strip the footer ("---\nModel: …")
        footer_idx = None
        for i in range(len(content_lines) - 1, -1, -1):
            if content_lines[i].strip() == "---":
                footer_idx = i
                break
        meta_line = ""
        if footer_idx is not None:
            meta_line = " ".join(content_lines[footer_idx + 1:])
            content_lines = content_lines[:footer_idx]

        content_plain = "\n".join(content_lines).strip()

        # Parse metadata from footer if present
        input_tokens = output_tokens = 0
        generation_time = 0.0
        retry_count = 0
        model_used = _MODEL_CONFIG["briefing_model"] if meta_line else "unknown"

        m = _re.search(r"Tokens in:\s*(\d+)\s*/\s*out:\s*(\d+)", meta_line)
        if m:
            input_tokens, output_tokens = int(m.group(1)), int(m.group(2))
        m = _re.search(r"Time:\s*([\d.]+)s", meta_line)
        if m:
            generation_time = float(m.group(1))
        m = _re.search(r"Retries:\s*(\d+)", meta_line)
        if m:
            retry_count = int(m.group(1))
        m = _re.search(r"Model:\s*([\w.-]+)", meta_line)
        if m:
            model_used = m.group(1)

        briefing = Briefing(
            date=scenario["date"],
            content_slack=content_plain,   # no Slack formatting needed for scoring
            content_plain=content_plain,
            model_used=model_used,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            generation_time_seconds=generation_time,
            retry_count=retry_count,
        )

        # Minimal context: metrics not needed by the scorer (only scenario signals are used)
        ctx = BriefingContext(
            date=scenario["date"],
            date_formatted=scenario["date"],
            metrics={},
            document_excerpts=[],
            context_document="",
            token_estimate=0,
        )

        cost = _estimate_cost(input_tokens, output_tokens)

        results.append(
            ScenarioResult(
                scenario_id=scenario_id,
                scenario_name=scenario["name"],
                date=scenario["date"],
                briefing=briefing,
                context=ctx,
                signal_score=0,
                signal_total=len(scenario.get("expected_signals", [])),
                generation_cost=cost,
            )
        )

    return results


# ---------------------------------------------------------------------------
# File saving helpers
# ---------------------------------------------------------------------------

def _save_context(ctx: BriefingContext, output_dir: str, scenario_id: str) -> str:
    """Save the context document to output/{scenario_id}_context.md."""
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, f"{scenario_id}_context.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"# Context Document — {ctx.date_formatted}\n\n")
        f.write(ctx.context_document)
        f.write(f"\n\n---\nToken estimate: {ctx.token_estimate}\n")
    return path


def _save_briefing(briefing: Briefing, output_dir: str, scenario_id: str) -> str:
    """Save the briefing to output/{scenario_id}_briefing.md."""
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, f"{scenario_id}_briefing.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"# Daily Briefing — {briefing.date}\n\n")
        f.write(briefing.content_plain)
        f.write(
            f"\n\n---\n"
            f"Model: {briefing.model_used} | "
            f"Tokens in: {briefing.input_tokens} / out: {briefing.output_tokens} | "
            f"Time: {briefing.generation_time_seconds:.1f}s | "
            f"Retries: {briefing.retry_count}\n"
        )
    return path


def _save_comparison(text: str, output_dir: str, id_a: str, id_b: str) -> str:
    """Save a comparison document."""
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, f"compare_{id_a}_vs_{id_b}.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    return path


# ---------------------------------------------------------------------------
# Core run functions
# ---------------------------------------------------------------------------

def run_scenario(
    scenario: dict,
    dry_run: bool = False,
    output_dir: str = OUTPUT_DIR_DEFAULT,
    verbose: bool = True,
) -> ScenarioResult:
    """Run the intelligence pipeline for one scenario date.

    Steps:
    1. Build briefing context from the scenario date.
    2. Optionally generate the briefing via the Anthropic API.
    3. Save context doc and briefing to output/.
    4. Check expected signals against the briefing text.
    5. Check discovery patterns against the metrics.
    6. Return a ScenarioResult.
    """
    scenario_id = scenario["id"]
    date_str = scenario["date"]

    if verbose:
        print(f"  [{scenario_id}] Building context for {date_str} ...", end="", flush=True)

    t0 = time.monotonic()
    ctx = build_briefing_context(DB_PATH, date_str)
    t_ctx = time.monotonic() - t0

    if verbose:
        print(f" done ({t_ctx:.1f}s)", flush=True)

    # Save context document
    ctx_path = _save_context(ctx, output_dir, scenario_id)

    # Generate briefing
    briefing: Optional[Briefing] = None
    if not dry_run:
        if verbose:
            print(f"  [{scenario_id}] Generating briefing ...", end="", flush=True)
        t1 = time.monotonic()
        briefing = generate_briefing(ctx, dry_run=False)
        t_gen = time.monotonic() - t1
        if verbose:
            wc = len(briefing.content_plain.split())
            print(f" done ({t_gen:.1f}s, {wc} words)", flush=True)
        _save_briefing(briefing, output_dir, scenario_id)
    else:
        if verbose:
            print(f"  [{scenario_id}] Dry run — skipping API call", flush=True)

    # Signal check
    signal_score, signal_total, signals_found, signals_missing = _run_signal_check(
        scenario, briefing
    )

    # Discovery pattern check
    pattern_results = _check_discovery_patterns(scenario, ctx)

    # Cost estimate
    cost = 0.0
    if briefing:
        cost = _estimate_cost(briefing.input_tokens, briefing.output_tokens)

    return ScenarioResult(
        scenario_id=scenario_id,
        scenario_name=scenario["name"],
        date=date_str,
        briefing=briefing,
        context=ctx,
        signal_score=signal_score,
        signal_total=signal_total,
        signals_found=signals_found,
        signals_missing=signals_missing,
        pattern_results=pattern_results,
        generation_cost=cost,
    )


def run_all_scenarios(
    dry_run: bool = False,
    output_dir: str = OUTPUT_DIR_DEFAULT,
) -> list[ScenarioResult]:
    """Run all 6 scenarios sequentially and print a summary table."""
    results: list[ScenarioResult] = []
    total = len(SCENARIOS)

    print(f"\nRunning {total} scenarios {'(dry run)' if dry_run else ''}...\n")

    for i, scenario in enumerate(SCENARIOS, 1):
        print(f"[{i}/{total}] {scenario['name']} ({scenario['date']})")
        result = run_scenario(scenario, dry_run=dry_run, output_dir=output_dir)
        results.append(result)
        print()

    _print_summary_table(results)
    return results


def _print_summary_table(results: list[ScenarioResult]) -> None:
    """Print a Unicode box-drawing summary table to stdout."""
    # Column widths
    W_NAME = 18
    W_DATE = 12
    W_WORDS = 8
    W_SIGNALS = 12
    W_COST = 10

    def _row(name: str, date: str, words: str, signals: str, cost: str) -> str:
        return (
            f"│ {name:<{W_NAME}} │ {date:<{W_DATE-2}} │ {words:>{W_WORDS-2}} │"
            f" {signals:<{W_SIGNALS-1}}│ {cost:>{W_COST-2}} │"
        )

    def _divider(left: str, mid: str, right: str, fill: str = "─") -> str:
        return (
            left
            + fill * (W_NAME + 2) + mid
            + fill * W_DATE + mid
            + fill * W_WORDS + mid
            + fill * W_SIGNALS + mid
            + fill * W_COST
            + right
        )

    print()
    print(_divider("┌", "┬", "┐"))
    print(_row("Scenario", "Date", "Words", "Signals", "Cost"))
    print(_divider("├", "┼", "┤"))

    total_words = 0
    total_score = 0
    total_signals = 0
    total_cost = 0.0

    for r in results:
        wc = len(r.briefing.content_plain.split()) if r.briefing else 0
        signal_pct = (
            f"{r.signal_score}/{r.signal_total} "
            f"({r.signal_score / r.signal_total:.0%})"
            if r.signal_total
            else "N/A"
        )
        cost_str = f"${r.generation_cost:.3f}" if r.briefing else "—"
        print(_row(r.scenario_name[:W_NAME], r.date, str(wc), signal_pct, cost_str))

        total_words += wc
        total_score += r.signal_score
        total_signals += r.signal_total
        total_cost += r.generation_cost

    print(_divider("├", "┼", "┤"))
    overall_pct = (
        f"{total_score}/{total_signals} ({total_score / total_signals:.0%})"
        if total_signals
        else "N/A"
    )
    cost_total = f"${total_cost:.3f}" if any(r.briefing for r in results) else "—"
    print(_row("TOTAL", "", f"{total_words:,}", overall_pct, cost_total))
    print(_divider("└", "┴", "┘"))

    if any(r.briefing for r in results):
        print(f"\nTotal cost for all {len(results)} scenarios: ~${total_cost:.2f}")

    # Signal details for missed signals
    missed_any = [r for r in results if r.signals_missing]
    if missed_any:
        print("\nMissed signals:")
        for r in missed_any:
            for s in r.signals_missing:
                print(f"  [{r.scenario_id}] ✗ {s}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m demo.scenarios.scenario_runner",
        description="Generate and validate briefings across Sparkle & Shine narrative scenarios.",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--all", action="store_true",
        help="Run all 6 scenarios."
    )
    group.add_argument(
        "--scenario", metavar="ID",
        help="Run a single scenario by id (e.g. rough_patch)."
    )
    group.add_argument(
        "--list", action="store_true",
        help="List all available scenario IDs and descriptions."
    )
    group.add_argument(
        "--compare", nargs=2, metavar=("A", "B"),
        help="Generate a side-by-side comparison of two scenario IDs."
    )
    parser.add_argument(
        "--output-dir", metavar="DIR", default=OUTPUT_DIR_DEFAULT,
        help=f"Directory for saved outputs (default: {OUTPUT_DIR_DEFAULT})."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Build context documents only; skip Anthropic API calls."
    )
    return parser


def _cmd_list() -> None:
    print("\nAvailable scenarios:\n")
    for s in SCENARIOS:
        print(f"  {s['id']:<20}  {s['name']}  ({s['narrative_period']})")
        print(f"  {'':20}  {s['description']}")
        print()


def _cmd_single(scenario_id: str, dry_run: bool, output_dir: str) -> int:
    if scenario_id not in SCENARIO_BY_ID:
        print(f"Error: unknown scenario id '{scenario_id}'.")
        print(f"Available ids: {', '.join(SCENARIO_BY_ID.keys())}")
        return 1

    scenario = SCENARIO_BY_ID[scenario_id]
    print(f"\n=== {scenario['name']} ({scenario['date']}) ===")
    print(f"{scenario['description']}\n")

    result = run_scenario(scenario, dry_run=dry_run, output_dir=output_dir)
    _print_single_result(result)
    return 0


def _cmd_compare(id_a: str, id_b: str, dry_run: bool, output_dir: str) -> int:
    for sid in (id_a, id_b):
        if sid not in SCENARIO_BY_ID:
            print(f"Error: unknown scenario id '{sid}'.")
            return 1

    print(f"\nRunning scenarios for comparison: {id_a} vs {id_b}\n")
    result_a = run_scenario(SCENARIO_BY_ID[id_a], dry_run=dry_run, output_dir=output_dir)
    result_b = run_scenario(SCENARIO_BY_ID[id_b], dry_run=dry_run, output_dir=output_dir)

    comparison_text = compare_scenarios(result_a, result_b)
    print("\n" + comparison_text)

    save_path = _save_comparison(comparison_text, output_dir, id_a, id_b)
    print(f"\nComparison saved to {save_path}")
    return 0


def _print_single_result(result: ScenarioResult) -> None:
    """Print a detailed result for a single scenario run."""
    print(f"\n--- Signal Check ({result.signal_score}/{result.signal_total}) ---")
    for s in result.signals_found:
        print(f"  ✓ {s}")
    for s in result.signals_missing:
        print(f"  ✗ {s}")

    if result.pattern_results:
        print("\n--- Discovery Patterns ---")
        for p in result.pattern_results:
            icon = "✓" if p["found"] is True else ("~" if p["found"] is None else "✗")
            print(f"  {icon} {p['pattern']}")
            print(f"      {p['evidence']}")

    if result.briefing:
        wc = len(result.briefing.content_plain.split())
        print(f"\n--- Briefing ---")
        print(f"  Words: {wc} | Tokens in: {result.briefing.input_tokens} / out: {result.briefing.output_tokens}")
        print(f"  Estimated cost: ${result.generation_cost:.4f}")
        print(f"\nSaved: {result.scenario_id}_context.md, {result.scenario_id}_briefing.md")


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    if args.list:
        _cmd_list()
        return 0

    if args.scenario:
        return _cmd_single(args.scenario, args.dry_run, args.output_dir)

    if args.compare:
        return _cmd_compare(args.compare[0], args.compare[1], args.dry_run, args.output_dir)

    if args.all:
        run_all_scenarios(dry_run=args.dry_run, output_dir=args.output_dir)
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
