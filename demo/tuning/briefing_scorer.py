"""
demo/tuning/briefing_scorer.py

Automated quality checks that score a briefing against multiple dimensions.

Usage:
    from demo.tuning.briefing_scorer import score_briefing, score_all_scenarios
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional

# ---------------------------------------------------------------------------
# Section definitions (must appear in this order)
# ---------------------------------------------------------------------------

SECTION_HEADERS: list[str] = [
    "Yesterday's Performance",
    "Cash Position",
    "Today's Schedule",
    "Sales Pipeline",
    "Action Items",
    "One Opportunity",
]

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class DimensionScore:
    name: str
    score: float        # 0–100 (raw points, scaled to max for that dimension)
    max_score: float
    notes: str


@dataclass
class BriefingScore:
    overall: float                              # 0–100
    dimensions: dict[str, DimensionScore]
    issues: list[str] = field(default_factory=list)       # must-fix items
    suggestions: list[str] = field(default_factory=list)  # nice-to-haves


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _word_count(text: str) -> int:
    return len(text.split())


def _section_texts(briefing_text: str) -> dict[str, str]:
    """Split the briefing into a dict of {header: body_text}.

    Matches headers case-insensitively, allowing for bold markers and
    numbering prefixes (e.g. "### 1. Yesterday's Performance").
    """
    result: dict[str, str] = {}
    lower = briefing_text.lower()

    boundaries: list[tuple[int, str]] = []
    for header in SECTION_HEADERS:
        # Search for the header text (case-insensitive) anywhere on a line
        pattern = re.compile(
            r"(?:^|\n)[^\n]*" + re.escape(header.lower()) + r"[^\n]*",
            re.IGNORECASE,
        )
        m = pattern.search(lower)
        if m:
            boundaries.append((m.end(), header))

    boundaries.sort(key=lambda x: x[0])

    for i, (start, header) in enumerate(boundaries):
        end = boundaries[i + 1][0] if i + 1 < len(boundaries) else len(briefing_text)
        result[header] = briefing_text[start:end].strip()

    return result


def _find_section_order(briefing_text: str) -> list[str]:
    """Return the headers found in the order they appear in the text."""
    lower = briefing_text.lower()
    positions: list[tuple[int, str]] = []
    for header in SECTION_HEADERS:
        pattern = re.compile(re.escape(header.lower()), re.IGNORECASE)
        m = pattern.search(lower)
        if m:
            positions.append((m.start(), header))
    positions.sort(key=lambda x: x[0])
    return [h for _, h in positions]


# ---------------------------------------------------------------------------
# Dimension 1: Structure (20 points)
# ---------------------------------------------------------------------------

def _score_structure(briefing_text: str) -> DimensionScore:
    """
    - All 6 section headers present?  12 points (2 per section)
    - Sections in correct order?       4 points
    - No section exceeds 200 words?    2 points
    - Opportunity section >= 50 words? 2 points
    """
    score = 0.0
    notes_parts: list[str] = []

    lower = briefing_text.lower()

    # Presence: 2 points per header
    found_headers: list[str] = []
    missing_headers: list[str] = []
    for header in SECTION_HEADERS:
        if header.lower() in lower:
            score += 2
            found_headers.append(header)
        else:
            missing_headers.append(header)

    if missing_headers:
        notes_parts.append(f"Missing sections: {', '.join(missing_headers)}")

    # Order: 4 points if all found and in correct order
    found_order = _find_section_order(briefing_text)
    expected_order = [h for h in SECTION_HEADERS if h in found_order]
    if found_order == expected_order and len(found_order) == len(SECTION_HEADERS):
        score += 4
    elif found_order == expected_order:
        score += 2  # partial credit if present ones are in order
        notes_parts.append("Sections present are in correct order but some are missing")
    else:
        notes_parts.append(f"Sections out of order: {found_order}")

    # Per-section length checks
    sections = _section_texts(briefing_text)

    over_limit: list[str] = []
    for header, body in sections.items():
        wc = _word_count(body)
        if wc > 200:
            over_limit.append(f"{header} ({wc} words)")

    if not over_limit:
        score += 2
    else:
        notes_parts.append(f"Sections over 200 words: {', '.join(over_limit)}")

    # Opportunity section length
    opp_text = sections.get("One Opportunity", "")
    opp_wc = _word_count(opp_text)
    if opp_wc >= 50:
        score += 2
    else:
        notes_parts.append(
            f"Opportunity section is only {opp_wc} words (target: 50+)"
        )

    return DimensionScore(
        name="Structure",
        score=score,
        max_score=20,
        notes="; ".join(notes_parts) if notes_parts else "All structure checks passed",
    )


# ---------------------------------------------------------------------------
# Dimension 2: Specificity (25 points)
# ---------------------------------------------------------------------------

_VAGUE_QUALIFIERS = [
    r"\bsome\b",
    r"\bseveral\b",
    r"\bvarious\b",
    r"\ba number of\b",
    r"\bsignificant\b(?!\s+\$|\s+\d)",  # "significant" not followed by $ or digit
]

def _score_specificity(briefing_text: str) -> DimensionScore:
    """
    - Dollar amounts >= 8:    8 points  (scaled 0–8)
    - Percentage values >= 4: 6 points  (scaled 0–6)
    - Named entities >= 3:    6 points  (scaled 0–6)
    - Date/time refs >= 2:    5 points  (scaled 0–5)
    - Deduct for vague qualifiers (up to -3 points)
    """
    score = 0.0
    notes_parts: list[str] = []

    # Dollar amounts: $123, $1,234, $1.2K, $45K, $1.2M
    dollar_matches = re.findall(
        r"\$[\d,]+(?:\.\d+)?(?:[KkMm])?|\$[\d]+[KkMm]?",
        briefing_text,
    )
    n_dollars = len(dollar_matches)
    dollar_pts = min(8, (n_dollars / 8) * 8)
    score += dollar_pts
    if n_dollars < 8:
        notes_parts.append(
            f"Only {n_dollars} dollar amounts (target: 8+); add more specific figures"
        )

    # Percentage values: 12%, 4.5%
    pct_matches = re.findall(r"\d+(?:\.\d+)?\s*%", briefing_text)
    n_pcts = len(pct_matches)
    pct_pts = min(6, (n_pcts / 4) * 6)
    score += pct_pts
    if n_pcts < 4:
        notes_parts.append(f"Only {n_pcts} percentage values (target: 4+)")

    # Named entities: client names (Title Case), crew names (Crew A/B/C/D),
    # campaign names (quoted strings or proper-name pairs)
    # Simple heuristic: Capitalized two-word sequences + "Crew [A-D]" + quoted text
    named_entities: set[str] = set()

    # Crew references
    for m in re.finditer(r"\bCrew\s+[A-D]\b", briefing_text):
        named_entities.add(m.group())

    # Quoted strings (campaign names, client company names in quotes)
    for m in re.finditer(r'"([^"]{3,40})"', briefing_text):
        named_entities.add(m.group(1))

    # Consecutive Capitalized words (proper names, 2–3 words)
    for m in re.finditer(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})\b", briefing_text):
        candidate = m.group(1)
        # Exclude section headers and common sentence starters
        if candidate not in SECTION_HEADERS and len(candidate) > 6:
            named_entities.add(candidate)

    n_entities = len(named_entities)
    entity_pts = min(6, (n_entities / 3) * 6)
    score += entity_pts
    if n_entities < 3:
        notes_parts.append(f"Only {n_entities} named entities (target: 3+)")

    # Date/time references: "Monday", "yesterday", "this week", "Dec 15", etc.
    date_pattern = re.compile(
        r"\b(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday|"
        r"yesterday|today|tomorrow|this week|last week|next week|"
        r"Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
        r"Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
        r"(?:\s+\d{1,2}(?:st|nd|rd|th)?)?"
        r"|\b\d{1,2}/\d{1,2}(?:/\d{2,4})?\b",
        re.IGNORECASE,
    )
    date_matches = date_pattern.findall(briefing_text)
    n_dates = len(set(m.lower() for m in date_matches))
    date_pts = min(5, (n_dates / 2) * 5)
    score += date_pts
    if n_dates < 2:
        notes_parts.append(f"Only {n_dates} date/time references (target: 2+)")

    # Vague qualifier deductions (up to -3)
    vague_hits: list[str] = []
    for pattern in _VAGUE_QUALIFIERS:
        for m in re.finditer(pattern, briefing_text, re.IGNORECASE):
            vague_hits.append(m.group())

    if vague_hits:
        deduction = min(3.0, len(vague_hits) * 0.75)
        score = max(0, score - deduction)
        notes_parts.append(
            f"Vague qualifiers found ({len(vague_hits)}x): "
            + ", ".join(f'"{v}"' for v in sorted(set(vague_hits)))
            + " — replace with specific numbers"
        )

    return DimensionScore(
        name="Specificity",
        score=round(score, 1),
        max_score=25,
        notes="; ".join(notes_parts) if notes_parts else "Strong specificity",
    )


# ---------------------------------------------------------------------------
# Dimension 3: Signal Coverage (25 points)
# ---------------------------------------------------------------------------

def _score_signal_coverage(
    briefing_text: str,
    scenario: dict,
    metrics: dict,
) -> DimensionScore:
    """
    For each expected_signal in scenario, scan the briefing for phrase matches.

    Uses the curated _SIGNAL_PHRASES registry from scenario_runner when a key
    is registered there; falls back to keyword extraction for unregistered signals.

    score = (signals_found / signals_expected) * 25
    """
    # Lazy import to avoid circular deps at module load time
    try:
        from demo.scenarios.scenario_runner import _SIGNAL_PHRASES as _runner_phrases
    except Exception:
        _runner_phrases = {}

    expected_signals: list[str] = scenario.get("expected_signals", [])

    if not expected_signals:
        return DimensionScore(
            name="Signal Coverage",
            score=25,
            max_score=25,
            notes="No expected signals defined for this scenario",
        )

    lower = briefing_text.lower()
    found_count = 0
    notes_parts: list[str] = []

    for signal in expected_signals:
        signal_key = signal.lower()

        # Prefer curated phrase list over naive keyword extraction
        if signal_key in _runner_phrases:
            phrases = _runner_phrases[signal_key]
            if any(phrase in lower for phrase in phrases):
                found_count += 1
            else:
                notes_parts.append(f"Signal not found: '{signal}'")
            continue

        # Fallback: extract keywords and require >= 50% to match
        stop = {
            "a", "an", "the", "is", "in", "of", "or", "and", "at", "to",
            "for", "with", "from", "no", "all", "some", "over", "under",
        }
        keywords = [
            w.strip("()%.+,").lower()
            for w in signal.split()
            if len(w.strip("()%.+,")) > 3 and w.lower() not in stop
        ]
        matched = sum(1 for kw in keywords if kw in lower)
        threshold = max(1, len(keywords) // 2)
        if matched >= threshold:
            found_count += 1
        else:
            notes_parts.append(f"Signal not found: '{signal}'")

    score = (found_count / len(expected_signals)) * 25

    notes = (
        f"{found_count}/{len(expected_signals)} signals covered"
        + ("; " + "; ".join(notes_parts) if notes_parts else "")
    )

    return DimensionScore(
        name="Signal Coverage",
        score=round(score, 1),
        max_score=25,
        notes=notes,
    )


# ---------------------------------------------------------------------------
# Dimension 4: Synthesis (15 points)
# ---------------------------------------------------------------------------

_OPS_KEYWORDS = {"crew", "schedule", "completion", "cancellation", "cancelled",
                 "staffing", "utilization"}
_REVENUE_KEYWORDS = {"revenue", "income", "earned", "$"}
_SALES_KEYWORDS = {"deal", "pipeline", "lead", "proposal", "prospect"}
_FINANCIAL_KEYWORDS = {"invoice", "payment", "ar", "cash", "overdue", "receivable"}


def _sentences(text: str) -> list[str]:
    """Split text into sentences (simple punctuation-based split)."""
    return re.split(r"(?<=[.!?])\s+", text)


def _sentence_has_both(sentence: str, set_a: set[str], set_b: set[str]) -> bool:
    lower = sentence.lower()
    has_a = any(kw in lower for kw in set_a)
    has_b = any(kw in lower for kw in set_b)
    return has_a and has_b


def _score_synthesis(briefing_text: str) -> DimensionScore:
    """
    - Ops-revenue link present?     5 points
    - Sales-ops link present?       5 points
    - Financial-ops link present?   5 points
    """
    score = 0.0
    notes_parts: list[str] = []

    sents = _sentences(briefing_text)

    ops_revenue = any(
        _sentence_has_both(s, _OPS_KEYWORDS, _REVENUE_KEYWORDS) for s in sents
    )
    if ops_revenue:
        score += 5
    else:
        notes_parts.append(
            "No cross-link between operations and revenue "
            "(mention how crew/scheduling issues affect $)"
        )

    sales_ops = any(
        _sentence_has_both(s, _SALES_KEYWORDS, _OPS_KEYWORDS) for s in sents
    )
    if sales_ops:
        score += 5
    else:
        notes_parts.append(
            "No cross-link between sales and operations "
            "(e.g., can crew capacity handle pipeline growth?)"
        )

    financial_ops = any(
        _sentence_has_both(s, _FINANCIAL_KEYWORDS, _OPS_KEYWORDS) for s in sents
    )
    if financial_ops:
        score += 5
    else:
        notes_parts.append(
            "No cross-link between financial and operational data "
            "(e.g., cash tightness tied to job volume)"
        )

    return DimensionScore(
        name="Synthesis",
        score=score,
        max_score=15,
        notes="; ".join(notes_parts) if notes_parts else "Good cross-domain synthesis",
    )


# ---------------------------------------------------------------------------
# Dimension 5: Tone (15 points)
# ---------------------------------------------------------------------------

def _avg_sentence_length(text: str) -> float:
    sents = [s.strip() for s in _sentences(text) if s.strip()]
    if not sents:
        return 0.0
    words_per_sent = [len(s.split()) for s in sents]
    return sum(words_per_sent) / len(words_per_sent)


def _score_tone(briefing_text: str) -> DimensionScore:
    """
    - Word count 400–800?                5 points (0 if outside 300–900)
    - Avg sentence length < 25 words?    5 points
    - First sentence of each section
      contains a number?                 5 points
    """
    score = 0.0
    notes_parts: list[str] = []

    wc = _word_count(briefing_text)
    if 400 <= wc <= 800:
        score += 5
    elif 300 <= wc <= 900:
        score += 2
        notes_parts.append(
            f"Word count {wc} is outside 400–800 target "
            f"(acceptable 300–900 range gives partial credit)"
        )
    else:
        notes_parts.append(
            f"Word count {wc} is outside acceptable range 300–900 "
            f"(target: 400–800)"
        )

    avg_sent = _avg_sentence_length(briefing_text)
    if avg_sent < 25:
        score += 5
    else:
        notes_parts.append(
            f"Average sentence length {avg_sent:.1f} words "
            f"(target: under 25 words) — shorten sentences"
        )

    # First sentence of each section starts with / contains a number
    sections = _section_texts(briefing_text)
    sections_with_number = 0
    sections_without: list[str] = []
    for header, body in sections.items():
        first_sent = _sentences(body)[0] if body else ""
        if re.search(r"\d", first_sent):
            sections_with_number += 1
        else:
            sections_without.append(header)

    if sections:
        ratio = sections_with_number / len(sections)
        tone_pts = ratio * 5
        score += tone_pts
        if sections_without:
            notes_parts.append(
                "Lead sentences without a number: "
                + ", ".join(f'"{h}"' for h in sections_without)
                + " — open each section with a specific figure"
            )

    return DimensionScore(
        name="Tone",
        score=round(score, 1),
        max_score=15,
        notes="; ".join(notes_parts) if notes_parts else "Tone checks passed",
    )


# ---------------------------------------------------------------------------
# Main scorer
# ---------------------------------------------------------------------------

def score_briefing(
    briefing_text: str,
    scenario: dict,
    metrics: dict,
) -> BriefingScore:
    """Score a briefing on multiple dimensions.

    Parameters
    ----------
    briefing_text:
        Plain-text briefing content (no Slack mrkdwn).
    scenario:
        Scenario definition dict (from SCENARIOS in scenario_definitions.py).
    metrics:
        Metrics dict from BriefingContext.metrics (used for signal coverage).

    Returns
    -------
    BriefingScore with individual dimension scores and an overall quality score.
    """
    dim_structure = _score_structure(briefing_text)
    dim_specificity = _score_specificity(briefing_text)
    dim_signals = _score_signal_coverage(briefing_text, scenario, metrics)
    dim_synthesis = _score_synthesis(briefing_text)
    dim_tone = _score_tone(briefing_text)

    dimensions = {
        "structure": dim_structure,
        "specificity": dim_specificity,
        "signal_coverage": dim_signals,
        "synthesis": dim_synthesis,
        "tone": dim_tone,
    }

    # Overall = sum of raw points (max is 100)
    total = sum(d.score for d in dimensions.values())
    overall = round(total, 1)

    # Collect issues (notes from dimensions scoring below 60% of max)
    issues: list[str] = []
    suggestions: list[str] = []

    for dim in dimensions.values():
        ratio = dim.score / dim.max_score if dim.max_score else 1.0
        if dim.notes and "passed" not in dim.notes.lower() and "no expected" not in dim.notes.lower():
            if ratio < 0.6:
                for note in dim.notes.split("; "):
                    if note:
                        issues.append(note)
            else:
                for note in dim.notes.split("; "):
                    if note and "passed" not in note.lower():
                        suggestions.append(note)

    return BriefingScore(
        overall=overall,
        dimensions=dimensions,
        issues=issues,
        suggestions=suggestions,
    )


# ---------------------------------------------------------------------------
# Multi-scenario report
# ---------------------------------------------------------------------------

def score_all_scenarios(scenario_results: list) -> str:
    """Score all scenario briefings and return a formatted quality report.

    Parameters
    ----------
    scenario_results:
        List of ScenarioResult objects from demo.scenarios.scenario_runner.
    """
    from demo.scenarios.scenario_definitions import SCENARIO_BY_ID

    lines: list[str] = []
    lines.append("BRIEFING QUALITY REPORT")
    lines.append("=" * 44)
    lines.append("")

    all_scores: list[tuple[str, float]] = []
    issue_frequency: dict[str, int] = {}

    for result in scenario_results:
        if not result.briefing:
            lines.append(f"Scenario: {result.scenario_name} ({result.date})")
            lines.append("  [skipped — no briefing generated (dry run?)]")
            lines.append("")
            continue

        scenario = SCENARIO_BY_ID.get(result.scenario_id, {})
        metrics = result.context.metrics if result.context else {}

        bs = score_briefing(
            result.briefing.content_plain,
            scenario,
            metrics,
        )

        all_scores.append((result.scenario_name, bs.overall))

        lines.append(f"Scenario: {result.scenario_name} ({result.date})")
        lines.append(f"  Overall: {bs.overall:.0f}/100")

        dim = bs.dimensions
        lines.append(
            f"  Structure: {dim['structure'].score:.0f}/{dim['structure'].max_score}"
            f"  Specificity: {dim['specificity'].score:.0f}/{dim['specificity'].max_score}"
            f"  Signals: {dim['signal_coverage'].score:.0f}/{dim['signal_coverage'].max_score}"
        )
        lines.append(
            f"  Synthesis: {dim['synthesis'].score:.0f}/{dim['synthesis'].max_score}"
            f"   Tone: {dim['tone'].score:.0f}/{dim['tone'].max_score}"
        )

        if bs.issues:
            lines.append("  Issues:")
            for issue in bs.issues:
                lines.append(f"    - {issue}")

        if bs.suggestions:
            lines.append("  Suggestions:")
            for suggestion in bs.suggestions:
                lines.append(f"    - {suggestion}")

        # Accumulate issue frequency for cross-scenario analysis
        for issue in bs.issues:
            # Normalise to a short key
            key = issue[:60]
            issue_frequency[key] = issue_frequency.get(key, 0) + 1

        lines.append("")

    if all_scores:
        avg = sum(s for _, s in all_scores) / len(all_scores)
        best_name, best_score = max(all_scores, key=lambda x: x[1])
        worst_name, worst_score = min(all_scores, key=lambda x: x[1])

        lines.append(f"OVERALL AVERAGE: {avg:.0f}/100")
        lines.append(f"BEST: {best_name} ({best_score:.0f}/100)")
        lines.append(f"WEAKEST: {worst_name} ({worst_score:.0f}/100)")
        lines.append("")

        # Top cross-scenario issues (appeared in 2+ scenarios)
        recurring = sorted(
            [(issue, cnt) for issue, cnt in issue_frequency.items() if cnt >= 2],
            key=lambda x: -x[1],
        )
        if recurring:
            n = len(scenario_results)
            lines.append("TOP ISSUES ACROSS ALL SCENARIOS:")
            for i, (issue, cnt) in enumerate(recurring[:5], 1):
                lines.append(f"{i}. {issue} (appeared in {cnt}/{n} scenarios)")

    return "\n".join(lines)
