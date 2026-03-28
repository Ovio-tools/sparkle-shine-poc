---
name: weekly-report
description: "Guide for generating the Sparkle & Shine weekly business intelligence report. Use this skill whenever building, modifying, or debugging the weekly report generator (intelligence/weekly_report.py), tuning the system prompt for the Opus model, evaluating report quality, or adjusting how insights are selected, scored, and presented. Also use when the user mentions 'weekly report', 'insight quality', 'confidence levels', 'report formatting', 'citation formatting', or 'avoiding repetitive insights'."
---

# Weekly Business Intelligence Report

This skill governs everything about the weekly report: its structure, the analytical standards behind every insight, how confidence levels work, how to avoid repeating the same observations week after week, how citations are formatted, and what tone to strike. The system prompt passed to Claude Opus 4.6 should encode these principles. The scoring and filtering logic in `intelligence/weekly_report.py` should enforce them programmatically.

The weekly report is the single most client-visible artifact this system produces. A prospect who reads one report and finds a vague, unsourced, or repetitive insight will not trust the next one. Everything here is in service of earning and keeping that trust.

---

## Report Structure

The report has 6 sections in a fixed order. Each section has a specific job. Do not add sections, merge sections, or reorder them.

```
1. Executive Summary        (3-4 sentences, the week in a nutshell)
2. Key Wins                 (what went well, with evidence)
3. Concerns                 (what needs attention, with evidence)
4. Trends                   (patterns across 2+ weeks)
5. Recommendations          (3-5 ranked actions)
6. Looking Ahead            (what to watch next week)
```

### Section-by-Section Guide

**1. Executive Summary**
Open with the single most important thing that happened this week. Not a recap of all six sections, but the one headline. Follow with 2-3 supporting sentences that give context. The reader should be able to stop after this section and still know whether the week was good, bad, or mixed.

Bad: "This week, revenue was $38,450. Crew utilization averaged 82%. The pipeline has 14 open deals."
Good: "Revenue hit $38,450 this week, 8% above the same week last month, driven by three new commercial contracts that started service. Cash collection lagged behind, with two invoices crossing the 60-day mark."

The difference: the bad version lists numbers. The good version tells a story with cause and consequence.

**2. Key Wins**
Highlight 2-4 things that went well. Each win must include a specific number and a citation linking to the source record. Wins should be genuinely notable, not "we completed jobs today." A win is something that moved a needle: a new client signed, a metric improved week-over-week, a problem from last week was resolved.

**3. Concerns**
Highlight 2-4 things that need attention. Same rules: specific numbers, citations, genuine significance. Concerns should be actionable, not just alarming. Pair each concern with enough context that the reader understands what to do about it.

Bad: "AR aging is getting worse."
Good: "Two invoices from Barton Creek Medical Group have crossed 60 days outstanding, totaling $9,000. This is the second consecutive week they've appeared in the aging report. Consider a direct call to their accounts payable department."

**4. Trends**
This is the section that justifies using Opus. Trends require connecting data across at least 2 weeks (preferably 4+). A single week's data point is not a trend. If there isn't enough history to identify a real trend, say so honestly rather than inventing one.

Every trend must include:
- The direction (improving, declining, flat)
- The timeframe (over the last 3 weeks, since January, etc.)
- The magnitude (revenue up 12%, churn rate down 1.5 percentage points)
- Why it matters in business terms

**5. Recommendations**
Exactly 3-5 actions, ranked by expected impact. Each recommendation follows this format:

```
[Action]: [What to do, specifically]
[Why]: [What data supports this, with citation]
[Expected impact]: [Quantified where possible]
```

Recommendations must be things Maria can actually do. "Improve customer satisfaction" is not a recommendation. "Call the 3 clients who gave 2-star reviews this week to understand what went wrong" is.

**6. Looking Ahead**
What should the reader watch for next week? Upcoming deadlines, deals expected to close, seasonal shifts, expiring contracts. Keep this to 2-3 items. The purpose is to make next week's report feel like a continuation, not a fresh start.

---

## Word Count

Target: 700-1,200 words total. Hard floor: 500. Hard ceiling: 1,400.

If the week was uneventful, a 700-word report is fine. Do not pad with filler to reach a target. If the week was eventful (a major client churned, a big deal closed, a pattern reversed), go up to 1,200.

Section-level guidelines:
- Executive Summary: 50-80 words
- Key Wins: 100-200 words
- Concerns: 100-250 words
- Trends: 150-300 words
- Recommendations: 150-250 words
- Looking Ahead: 50-100 words

---

## Writing Tone

The audience is Maria Gonzalez, the owner of a $2M cleaning company. She is practical, busy, and not technical. She wants to know what the data means, not just what it says.

**Do:**
- Use specific numbers ("revenue grew 8% to $38,450") not vague qualifiers ("revenue increased significantly")
- Use plain language ("two clients are paying late") not jargon ("AR aging deterioration in the 60+ bucket")
- Be direct about bad news ("this is a problem") not evasive ("this may warrant further monitoring")
- Connect dots across domains ("the crew shortage is starting to affect revenue")
- Use the second person when recommending action ("consider calling them directly")

**Don't:**
- Start sentences with "It is worth noting that" or "It should be noted that" or "Interestingly"
- Use hedge words when the data is clear ("may potentially indicate a possible trend")
- List metrics without interpreting them
- Use superlatives without evidence ("best week ever", "unprecedented growth")
- Repeat the same phrasing across sections (if the executive summary says "revenue grew 8%", the Key Wins section should not repeat those exact words)

**Voice test:** Read each sentence aloud. If it sounds like a consulting report, rewrite it. If it sounds like a smart colleague giving you a heads-up over coffee, keep it.

---

## Confidence Levels

Every insight in the report (except pure factual statements like "revenue was $38,450") gets a confidence level. The confidence level determines whether and how the insight appears in the report.

### Three Levels

**High confidence (include in report, state directly):**
The insight is backed by concrete data from multiple consistent sources, covers a sufficient time window (2+ weeks for trends), and has a clear causal mechanism.

Example: "Referral clients have a 14% churn rate versus 28% for Google Ads clients over the trailing 90 days." This is a factual comparison across a meaningful window, sourced from verifiable records in HubSpot and the SQLite client table.

**Medium confidence (include with qualifier):**
The insight is backed by data but the sample is small, the timeframe is short, or the causal mechanism is unclear. Present it, but flag the uncertainty.

Example: "Three cancellations in Westlake this month may signal a new competitor in the area, though the sample is small enough that this could be coincidence."

The words "may", "could indicate", and "worth watching" are appropriate here, but only here. These words are forbidden at high confidence.

**Low confidence (exclude from report, log internally):**
The insight is speculative, based on a single data point, or depends on an assumption that can't be verified from available data. Do not include it in the report. Log it internally for potential inclusion if more supporting data emerges in future weeks.

Example: "The Tuesday/Wednesday complaint rate advantage might be because clients are in better moods mid-week." This is a causal theory with no supporting data. The observation itself (lower complaints mid-week) might be high-confidence. The explanation is low-confidence. Report the observation. Don't report the explanation.

### How to Assign Confidence

The context document passed to Opus includes metrics from the metrics engine. Use this rubric:

| Signal | Confidence |
|--------|-----------|
| Metric computed from 50+ records over 4+ weeks | High |
| Metric computed from 20-50 records over 2-3 weeks | High (if consistent) or Medium (if volatile) |
| Metric computed from 5-20 records or 1 week | Medium |
| Metric computed from fewer than 5 records | Low (exclude) |
| Cross-tool pattern (visible in 2+ tools) | Boost one level |
| Single-tool observation | No boost |
| Observation contradicted by another metric | Drop one level |

### Encoding in the System Prompt

The system prompt should instruct Opus:

```
Before including any insight, assign it a confidence level:
- HIGH: backed by 20+ records over 2+ weeks, or a clear factual comparison.
  State the insight directly.
- MEDIUM: backed by data but small sample or short timeframe.
  Include the insight with an explicit qualifier ("early signal",
  "worth watching", "small sample").
- LOW: speculative or single data point. Do not include in the report.

Never present a medium-confidence insight as if it were established fact.
Never include a low-confidence insight regardless of how interesting it seems.
```

---

## Avoiding Repetitive Insights

The fastest way to lose a reader's trust is to tell them the same thing every week. If the weekly report says "Crew A takes longer but has higher ratings" for six consecutive weeks with no new information, Maria will stop reading.

### The Repetition Detection System

The weekly report generator should maintain a `weekly_reports/insight_history.json` file tracking previously reported insights. Each entry records:

```json
{
    "insight_id": "crew_a_speed_quality_tradeoff",
    "first_reported": "2026-03-09",
    "last_reported": "2026-03-16",
    "times_reported": 3,
    "last_values": {"avg_rating": 4.7, "duration_variance": "+18%"},
    "category": "operations"
}
```

### Repetition Rules

**Rule 1: Never report the same insight two weeks in a row with the same numbers.**
If Crew A's avg rating was 4.7 last week and is 4.7 this week, do not report it again. It's the same data point.

**Rule 2: Report a recurring insight only if the underlying data changed meaningfully.**
If Crew A's avg rating moved from 4.7 to 4.3, that's a change worth reporting: "Crew A's rating dipped from 4.7 to 4.3 this week -- worth investigating if this is a blip or the start of a trend."

**Rule 3: After 3 consecutive reports of the same pattern, graduate it to a standing assumption.**
After reporting the referral retention advantage 3 times, it becomes a known fact about the business. Stop reporting it as a discovery. Reference it only when it's relevant to a new recommendation. ("Because referral clients retain at 2x the rate of ad-sourced clients, consider shifting $500/month from Google Ads to referral bonuses.")

**Rule 4: Rotate the "opportunity" spotlight.**
If the report has a forward-looking opportunity or strategic insight, cycle through different business areas each week. Don't spotlight the sales pipeline three weeks in a row while ignoring operations.

### Implementation in the Generator

Pass the last 4 weeks of `insight_history.json` entries to the Opus system prompt as a "previously reported" section:

```
PREVIOUSLY REPORTED INSIGHTS (last 4 weeks):
- "Crew A speed vs. quality tradeoff" -- reported 3x, last on 2026-03-16.
  DO NOT report again unless values changed meaningfully.
- "Westlake cancellation cluster" -- reported 1x on 2026-03-16.
  OK to follow up if new data exists.
- "Referral retention advantage" -- reported 4x, graduated to standing fact.
  Reference only when supporting a recommendation.
```

After Opus generates the report, scan the output for insights and update `insight_history.json`.

---

## Citations and Deep Links

Every factual claim in the report must include a citation that links to the source record in the relevant tool's UI. This is the single most important trust-building mechanism. When Maria reads "Barton Creek Medical Group is 52 days overdue on $4,500," she can click the link and see the invoice in QuickBooks.

### Citation Format (Slack mrkdwn)

```
Revenue grew 8% compared to last week. (<https://app.qbo.intuit.com/app/reportv2?token=PROFIT_AND_LOSS|QuickBooks P&L>)

Barton Creek Medical Group has $4,500 outstanding at 52 days. (<https://app.qbo.intuit.com/app/invoice?txnId=2456|View Invoice>)

The Eastside Dental deal moved to Negotiation stage. (<https://yourcompany.pipedrive.com/deal/789|View Deal>)
```

### Citation Rules

1. **One citation per factual claim.** Not one citation per paragraph. Each specific number or record reference gets its own link.

2. **Aggregate metrics cite the report or dashboard, not individual records.** "Revenue was $38,450 this week" cites the QuickBooks P&L report URL, not 150 individual invoices.

3. **Specific client mentions cite the client record.** "Barton Creek Medical Group is overdue" cites the specific invoice or customer record.

4. **Never fabricate a URL.** All URLs come from the citation index built by the context builder. If a URL isn't available for a claim, omit the citation rather than guessing.

5. **Citation display text should name the tool.** Use "View in QuickBooks", "View Deal", "View Contact" -- not the raw URL.

### Building the Citation Index

The context builder (Step 5 in the Phase 4 plan) assembles a citation index alongside the metrics. Each entry contains:

```json
{
    "ref_id": "rev-weekly-total",
    "claim": "Weekly revenue: $38,450",
    "tool": "quickbooks",
    "record_type": "reports_pl",
    "record_id": null,
    "url": "https://app.qbo.intuit.com/app/reportv2?token=PROFIT_AND_LOSS"
}
```

Pass the full index to Opus. Instruct it to use `ref_id` values when citing. Post-process the output to replace `ref_id` references with actual Slack mrkdwn links.

---

## Data Analysis Instructions

These are the analytical standards the metrics engine and the Opus prompt should follow. They apply to the weekly report specifically (the daily briefing has simpler requirements).

### Compare Against the Right Baselines

Every metric should be compared against at least one baseline. The priority order:

1. **Same week last year** (if data exists -- only possible after 12+ months of simulation)
2. **Same week last month** (most useful for detecting trends)
3. **Prior week** (useful for momentum but noisy)
4. **Monthly target from intelligence/config.py** (useful for pacing)

Present at most two comparisons per metric. More than that clutters the report.

### Distinguish Between Correlation and Causation

The weekly report should observe correlations but not assert causation unless the mechanism is obvious and direct.

OK: "Crew A's customer ratings dropped the same week they were reassigned to cover Westlake routes. The unfamiliar territory may be a factor."
Not OK: "Crew A's ratings dropped because they were reassigned to Westlake."

The word "because" should appear rarely. Prefer "coincided with", "followed", "may be related to".

### Use Rate-Based Metrics, Not Just Absolutes

Absolute numbers can be misleading when the base changes. If the business added 15 new clients this month, the number of complaints will naturally rise even if quality is constant.

Always pair absolute numbers with rates:
- Not just "8 cancellations" but "8 cancellations (4.4% of active clients, up from 3.1% last month)"
- Not just "$9,000 overdue" but "$9,000 overdue (6.2% of total AR, up from 4.8%)"

### Flag Seasonality Before Interpreting

Before flagging a metric as concerning or exciting, check if the change is seasonal. A 15% revenue dip in January is not a crisis -- it happens every year. A 15% revenue dip in June is unusual and worth flagging.

The system prompt should include the seasonal context:
```
Current month seasonal weight: 0.95 (March -- spring pickup)
This means a 5% dip from February is expected and not alarming.
A 15% dip from February would be genuinely concerning.
```

### Cross-Tool Insights Are the Highest Value

Any insight that connects data from two or more tools is inherently more valuable than a single-tool observation. These are what no individual tool dashboard can show. Prioritize them.

Examples of cross-tool insights:
- Revenue (QuickBooks) dropping while job count (Jobber) stays flat = average job value is declining
- New leads (HubSpot) up but pipeline value (Pipedrive) flat = leads are lower quality
- Task completion rate (Asana) dropping while churn (HubSpot/Jobber) rising = operational capacity may be causing service quality issues

### Handle Missing or Incomplete Data

If a syncer failed and data is missing for a tool, do not silently omit that section. State it:

"Note: Mailchimp data was unavailable this week due to a sync error. Campaign metrics are excluded from this report."

This maintains trust. A report that silently drops a section looks incomplete. A report that explains why a section is missing looks honest.

---

## Quality Scoring Rubric

Use this to evaluate weekly reports during development and tuning. Each dimension is scored 0-25 for a total of 100.

### Specificity (0-25)
- 25: Every claim has a specific number and a citation. Named clients, crews, campaigns referenced where relevant. No vague qualifiers.
- 15: Most claims have numbers. Some citations missing. Occasional "several" or "significant" without a number.
- 5: Heavy on qualifiers, light on specifics. Few or no citations.

### Insight Quality (0-25)
- 25: At least 2 cross-tool insights. All insights are high or medium confidence. No previously-graduated insights re-reported. Recommendations are specific and actionable.
- 15: Some cross-tool insights. Mostly high confidence. One or two repeated insights. Recommendations are reasonable but generic.
- 5: Single-tool observations only. Low-confidence speculation included. Repetitive. Recommendations are vague.

### Structure and Tone (0-25)
- 25: All 6 sections present, correct order. Executive summary tells a story, not a list. Tone is direct and warm. Word count within range. No consulting-speak.
- 15: Sections present but some feel padded or thin. Tone is mostly right but lapses into jargon occasionally.
- 5: Sections missing or merged. Reads like a data dump. Tone is robotic or overly formal.

### Trust Signals (0-25)
- 25: Confidence levels correctly applied. Missing data acknowledged. No fabricated URLs. Previously reported insights handled per rules. Seasonal context considered.
- 15: Confidence levels mostly right. One or two unqualified medium-confidence claims. Seasonal context partially addressed.
- 5: No confidence signaling. Speculative insights stated as fact. Repetitive across weeks. Missing data silently omitted.

Target score: 75+ across all reports. Below 60 means the system prompt or context builder needs revision.

---

## System Prompt Template

This is the template used for the Opus API call. The actual values (insight history, seasonal context, citation index) are injected by the weekly report generator at runtime.

```
You are the senior AI business analyst for Sparkle & Shine Cleaning Co.,
a $2M/year cleaning company in Austin, TX owned by Maria Gonzalez.

You are writing the WEEKLY report. This is different from the daily
briefing. Your job is to surface PATTERNS and TRENDS, not individual
events. Connect dots across the week and across tools.

AUDIENCE: Maria. She is busy, practical, and not technical. She wants
to know what the data means, not just what it says.

TONE: Direct, warm, specific. Use plain language. If something is
concerning, say so plainly. If something is good news, say why it
matters. Never hedge when the data is clear. Never speculate when it's not.

STRUCTURE (exactly 6 sections, this order):
1. Executive Summary (3-4 sentences: the single most important headline,
   then context)
2. Key Wins (2-4 wins, each with a number and citation)
3. Concerns (2-4 concerns, each actionable with enough context to act)
4. Trends (patterns across 2+ weeks; if insufficient history, say so)
5. Recommendations (3-5 ranked actions: what to do, why, expected impact)
6. Looking Ahead (2-3 things to watch next week)

WORD COUNT: 700-1,200 words. Do not pad. Do not exceed 1,400.

CONFIDENCE LEVELS (apply to every non-factual insight):
- HIGH: 20+ records, 2+ weeks, clear pattern. State directly.
- MEDIUM: smaller sample or shorter window. Include with qualifier
  ("early signal", "worth watching", "small sample").
- LOW: speculative or single data point. DO NOT include.

CITATIONS: Every factual claim must cite its source using this format:
  (<URL|Tool Name: Record Type>)
You will receive a CITATION INDEX with pre-built URLs. Use the ref_id
to match claims to citations. Never fabricate a URL.

PREVIOUSLY REPORTED INSIGHTS:
{insight_history_block}
Rules:
- Do not re-report an insight with the same numbers as last week.
- Only re-report if the underlying data changed meaningfully.
- After 3 consecutive reports, an insight is graduated to a known fact.
  Reference it only when supporting a new recommendation.
- Rotate the opportunity spotlight across business areas.

SEASONAL CONTEXT:
Current month: {month_name} (seasonal weight: {seasonal_weight})
{seasonal_note}

CROSS-TOOL PRIORITY: Insights connecting data from 2+ tools are
the most valuable. Prioritize them over single-tool observations.
Examples: revenue trend + job count mismatch, lead volume + pipeline
quality divergence, task completion + churn correlation.

DATA GAPS: If data from any tool is missing or incomplete, state this
explicitly in the relevant section. Never silently omit a section.

BASELINES: Compare metrics against (in priority order):
1. Same week last month
2. Prior week
3. Monthly target

Use at most two comparisons per metric.

RATES OVER ABSOLUTES: Always pair absolute numbers with rates when the
base is changing (e.g., "8 cancellations, 4.4% of active clients").

DO NOT:
- Start sentences with "It is worth noting" or "Interestingly"
- Use "significant" without a number
- Assert causation without a clear, direct mechanism
- Include insights below medium confidence
- Repeat the exact phrasing from the Executive Summary in later sections
```

---

## Insight History File Format

`weekly_reports/insight_history.json`:

```json
{
    "last_updated": "2026-03-23",
    "insights": [
        {
            "insight_id": "crew_a_speed_quality",
            "category": "operations",
            "summary": "Crew A takes 20% longer but has highest satisfaction",
            "first_reported": "2026-03-02",
            "last_reported": "2026-03-16",
            "times_reported": 3,
            "status": "graduated",
            "last_values": {
                "avg_rating": 4.7,
                "duration_variance": "+18%"
            }
        },
        {
            "insight_id": "westlake_cancellations",
            "category": "client_health",
            "summary": "Cancellation cluster in Westlake neighborhood",
            "first_reported": "2026-03-16",
            "last_reported": "2026-03-16",
            "times_reported": 1,
            "status": "active",
            "last_values": {
                "count": 3,
                "timeframe_days": 14
            }
        }
    ]
}
```

The `status` field:
- `"active"`: reported fewer than 3 times, eligible for re-reporting if values change
- `"graduated"`: reported 3+ times, only reference when supporting a new recommendation
- `"resolved"`: the underlying condition no longer exists (e.g., the overdue invoice was paid)

After each weekly report is generated, update this file: add new insights, increment `times_reported` for repeated ones, graduate those at 3+, and mark resolved ones.
