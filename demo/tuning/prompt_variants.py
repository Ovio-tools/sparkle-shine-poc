"""
demo/tuning/prompt_variants.py

Store system prompt variants for A/B testing briefing quality.

Usage:
    python -m demo.tuning.prompt_variants --test v2_synthesis_emphasis --scenario rough_patch
    python -m demo.tuning.prompt_variants --compare v1_baseline v2_synthesis_emphasis --scenario rough_patch
    python -m demo.tuning.prompt_variants --list
"""

from __future__ import annotations

import argparse
import os
import sys
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

from intelligence.config import SYSTEM_PROMPT_TEMPLATE

# ---------------------------------------------------------------------------
# Variant registry
# ---------------------------------------------------------------------------

PROMPT_VARIANTS: dict[str, dict] = {
    "v1_baseline": {
        "description": "Original system prompt from config.py",
        "prompt": None,    # uses SYSTEM_PROMPT_TEMPLATE from config
    },
    "v2_synthesis_emphasis": {
        "description": "Adds explicit instruction to connect dots across domains",
        "prompt": """
{base_prompt}

IMPORTANT ADDITIONAL INSTRUCTION: Every section should connect to at
least one other domain. For example:
- In Yesterday's Performance, mention how operational issues (cancellations,
  delays) affected revenue.
- In Cash Position, reference whether the sales pipeline will address
  any shortfalls.
- In Today's Schedule, flag if crew capacity issues could impact pending
  commercial proposals.
- In Action Items, group items that are connected rather than listing
  them by domain.

Do not artificially force connections. But when the data shows a real
link, always call it out.
""",
    },
    "v3_narrative_arc": {
        "description": "Encourages a story arc: situation, tension, resolution path",
        "prompt": """
{base_prompt}

STYLE INSTRUCTION: Structure each briefing as a mini-narrative.
Open with the single most important thing Maria needs to know today.
Build through the data sections, connecting each to the opening theme
where relevant. Close the Opportunity section by tying back to the
opening -- what is the one action that addresses today's biggest
challenge or capitalizes on today's biggest opportunity?

Keep the 6-section structure, but let a through-line connect them.
""",
    },
}


# ---------------------------------------------------------------------------
# Prompt resolution
# ---------------------------------------------------------------------------

def get_prompt_variant(variant_id: str) -> str:
    """Return the full system prompt for a variant.

    If prompt is None, return SYSTEM_PROMPT_TEMPLATE from config.
    If prompt contains {base_prompt}, substitute SYSTEM_PROMPT_TEMPLATE.
    """
    if variant_id not in PROMPT_VARIANTS:
        raise ValueError(
            f"Unknown variant '{variant_id}'. "
            f"Available: {', '.join(PROMPT_VARIANTS.keys())}"
        )

    variant = PROMPT_VARIANTS[variant_id]
    template = variant["prompt"]

    if template is None:
        return SYSTEM_PROMPT_TEMPLATE

    if "{base_prompt}" in template:
        return template.replace("{base_prompt}", SYSTEM_PROMPT_TEMPLATE).strip()

    return template.strip()


# ---------------------------------------------------------------------------
# Test / score a single variant + scenario
# ---------------------------------------------------------------------------

def test_variant(
    variant_id: str,
    scenario_id: str,
) -> tuple:
    """Generate a briefing using a specific prompt variant and scenario,
    then score it. Returns (Briefing, BriefingScore).
    """
    from intelligence.context_builder import build_briefing_context
    from intelligence.briefing_generator import generate_briefing, Briefing
    import anthropic
    import time
    from intelligence.config import MODEL_CONFIG
    from intelligence.briefing_generator import _format_for_slack, _format_plain
    from demo.scenarios.scenario_definitions import SCENARIO_BY_ID
    from demo.tuning.briefing_scorer import score_briefing

    if scenario_id not in SCENARIO_BY_ID:
        raise ValueError(
            f"Unknown scenario '{scenario_id}'. "
            f"Available: {', '.join(SCENARIO_BY_ID.keys())}"
        )

    scenario = SCENARIO_BY_ID[scenario_id]
    db_path = os.path.join(_PROJECT_ROOT, "sparkle_shine.db")

    print(f"  Building context for {scenario['date']} ...", end="", flush=True)
    ctx = build_briefing_context(db_path, scenario["date"])
    print(" done")

    system_prompt = get_prompt_variant(variant_id)

    print(f"  Calling API (variant={variant_id}) ...", end="", flush=True)
    client = anthropic.Anthropic()
    t0 = time.monotonic()
    response = client.messages.create(
        model=MODEL_CONFIG["briefing_model"],
        max_tokens=MODEL_CONFIG["max_tokens_briefing"],
        temperature=MODEL_CONFIG["temperature_briefing"],
        system=system_prompt,
        messages=[{"role": "user", "content": ctx.context_document}],
    )
    elapsed = time.monotonic() - t0
    raw_text = response.content[0].text
    print(f" done ({elapsed:.1f}s, {len(raw_text.split())} words)")

    briefing = Briefing(
        date=ctx.date,
        content_slack=_format_for_slack(raw_text, ctx.date_formatted),
        content_plain=_format_plain(raw_text),
        model_used=MODEL_CONFIG["briefing_model"],
        input_tokens=response.usage.input_tokens,
        output_tokens=response.usage.output_tokens,
        generation_time_seconds=elapsed,
        retry_count=0,
    )

    bs = score_briefing(briefing.content_plain, scenario, ctx.metrics)
    return briefing, bs


# ---------------------------------------------------------------------------
# Printing helpers
# ---------------------------------------------------------------------------

def _print_score(variant_id: str, briefing, bs) -> None:
    """Print a single variant's score in a readable format."""
    dim = bs.dimensions
    print(f"\nVariant: {variant_id} — {PROMPT_VARIANTS[variant_id]['description']}")
    print(f"  Overall: {bs.overall:.0f}/100")
    print(
        f"  Structure: {dim['structure'].score:.0f}/{dim['structure'].max_score}  "
        f"Specificity: {dim['specificity'].score:.0f}/{dim['specificity'].max_score}  "
        f"Signals: {dim['signal_coverage'].score:.0f}/{dim['signal_coverage'].max_score}"
    )
    print(
        f"  Synthesis: {dim['synthesis'].score:.0f}/{dim['synthesis'].max_score}   "
        f"Tone: {dim['tone'].score:.0f}/{dim['tone'].max_score}"
    )
    if bs.issues:
        print("  Issues:")
        for issue in bs.issues:
            print(f"    - {issue}")
    if bs.suggestions:
        print("  Suggestions:")
        for s in bs.suggestions:
            print(f"    - {s}")
    print(
        f"  Tokens: in={briefing.input_tokens} / out={briefing.output_tokens} | "
        f"Time: {briefing.generation_time_seconds:.1f}s"
    )


def _print_comparison(variants: list[tuple[str, object, object]]) -> None:
    """Print a side-by-side comparison of N variant scores.

    Parameters
    ----------
    variants:
        List of (variant_id, briefing, BriefingScore) tuples.
    """
    dim_keys = ["structure", "specificity", "signal_coverage", "synthesis", "tone"]
    dim_labels = {
        "structure":      "Structure",
        "specificity":    "Specificity",
        "signal_coverage":"Signals",
        "synthesis":      "Synthesis",
        "tone":           "Tone",
    }

    COL_LABEL = 16
    COL_VAL = 10
    ids = [vid for vid, _, _ in variants]
    scores = [bs for _, _, bs in variants]

    total_width = COL_LABEL + COL_VAL * len(variants) + 14
    sep = "-" * total_width

    print()
    # Header
    header = f"{'Dimension':<{COL_LABEL}}"
    for vid in ids:
        header += f"  {vid[:COL_VAL-2]:>{COL_VAL-2}}"
    print(header)
    print(sep)

    # Per-dimension rows
    for key in dim_keys:
        label = dim_labels[key]
        dim_scores = [bs.dimensions[key] for bs in scores]
        best_score = max(d.score for d in dim_scores)

        row = f"{label:<{COL_LABEL}}"
        for d in dim_scores:
            cell = f"{d.score:.0f}/{d.max_score}"
            row += f"  {cell:>{COL_VAL-2}}"

        # Annotate who wins this dimension (if there's a clear winner)
        winners = [ids[i] for i, d in enumerate(dim_scores) if d.score == best_score]
        if len(winners) < len(ids):
            row += f"  ← {winners[0][:10]}"
        print(row)

    print(sep)

    # Overall row
    overall_row = f"{'OVERALL':<{COL_LABEL}}"
    overall_scores = [bs.overall for bs in scores]
    best_overall = max(overall_scores)
    for s in overall_scores:
        overall_row += f"  {s:.0f}/100  "[: COL_VAL]
    print(overall_row)

    winner_ids = [ids[i] for i, s in enumerate(overall_scores) if s == best_overall]
    print(f"\nWinner: {', '.join(winner_ids)}  ({best_overall:.0f}/100)")

    # Notable deltas vs. the baseline (first variant)
    if len(variants) > 1:
        baseline_id, _, baseline_score = variants[0]
        print(f"\nDeltas vs {baseline_id}:")
        for vid, _, bs in variants[1:]:
            delta_overall = bs.overall - baseline_score.overall
            sign = "+" if delta_overall >= 0 else ""
            print(f"  {vid}: {sign}{delta_overall:.0f} overall", end="")
            notable = []
            for key in dim_keys:
                d = bs.overall - baseline_score.overall  # just overall already printed
                dd = bs.dimensions[key].score - baseline_score.dimensions[key].score
                if abs(dd) >= 3:
                    sign_d = "+" if dd >= 0 else ""
                    notable.append(f"{dim_labels[key]} {sign_d}{dd:.0f}")
            if notable:
                print(f"  ({', '.join(notable)})", end="")
            print()

    # Token cost summary
    print(f"\nAPI usage:")
    for vid, briefing, _ in variants:
        print(
            f"  {vid}: in={briefing.input_tokens} / out={briefing.output_tokens} | "
            f"{briefing.generation_time_seconds:.1f}s"
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m demo.tuning.prompt_variants",
        description="A/B test system prompt variants for Sparkle & Shine briefings.",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--list", action="store_true",
        help="List all available variant IDs and descriptions.",
    )
    group.add_argument(
        "--test", metavar="VARIANT",
        help="Generate and score a briefing with a single variant.",
    )
    group.add_argument(
        "--compare", nargs="+", metavar="VARIANT",
        help="Generate 2+ variants and print a side-by-side score comparison.",
    )
    parser.add_argument(
        "--scenario", metavar="SCENARIO_ID",
        help="Scenario ID to use (required for --test and --compare).",
    )
    return parser


def _cmd_list() -> None:
    print("\nAvailable prompt variants:\n")
    for vid, vdata in PROMPT_VARIANTS.items():
        print(f"  {vid:<30}  {vdata['description']}")
    print()


def _cmd_test(variant_id: str, scenario_id: str) -> int:
    print(f"\nTesting variant '{variant_id}' on scenario '{scenario_id}'\n")
    try:
        briefing, bs = test_variant(variant_id, scenario_id)
    except ValueError as e:
        print(f"Error: {e}")
        return 1
    _print_score(variant_id, briefing, bs)
    return 0


def _cmd_compare(variant_ids: list[str], scenario_id: str) -> int:
    if len(variant_ids) < 2:
        print("Error: --compare requires at least 2 variant IDs")
        return 1
    label = " vs ".join(f"'{v}'" for v in variant_ids)
    print(f"\nComparing {label} on scenario '{scenario_id}'\n")

    results: list[tuple[str, object, object]] = []
    try:
        for i, vid in enumerate(variant_ids, 1):
            print(f"[{i}/{len(variant_ids)}] Generating {vid} ...")
            briefing, bs = test_variant(vid, scenario_id)
            results.append((vid, briefing, bs))
    except ValueError as e:
        print(f"Error: {e}")
        return 1

    _print_comparison(results)
    return 0


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    if args.list:
        _cmd_list()
        return 0

    if args.test:
        if not args.scenario:
            print("Error: --scenario is required with --test")
            return 1
        return _cmd_test(args.test, args.scenario)

    if args.compare:
        if not args.scenario:
            print("Error: --scenario is required with --compare")
            return 1
        return _cmd_compare(args.compare, args.scenario)

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
