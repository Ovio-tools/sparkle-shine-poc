"""
Central configuration for the Sparkle & Shine intelligence layer.
All magic numbers, thresholds, targets, and prompt templates live here.
"""

BRIEFING_DATE_FORMAT = "%A, %B %d, %Y"  # e.g., "Monday, March 17, 2026"

# Number of previous briefings to load as context for each new generation
RECENT_BRIEFINGS_COUNT = 3

# Revenue targets by (year, month) -> (low, high)
REVENUE_TARGETS: dict[tuple[int, int], tuple[int, int]] = {
    (2025, 4):  (135000, 145000),   # Apr 2025: steady ops
    (2025, 5):  (135000, 145000),   # May 2025: steady ops
    (2025, 6):  (148000, 160000),   # Jun 2025: summer surge
    (2025, 7):  (148000, 160000),   # Jul 2025: summer surge
    (2025, 8):  (128000, 140000),   # Aug 2025: rough patch
    (2025, 9):  (128000, 140000),   # Sep 2025: rough patch
    (2025, 10): (140000, 155000),   # Oct 2025: stabilization
    (2025, 11): (140000, 155000),   # Nov 2025: stabilization
    (2025, 12): (165000, 185000),   # Dec 2025: holiday peak
    (2026, 1):  (120000, 135000),   # Jan 2026: dip
    (2026, 2):  (135000, 150000),   # Feb 2026: recovery
    (2026, 3):  (135000, 150000),   # Mar 2026: recovery
}

ALERT_THRESHOLDS: dict = {
    "overdue_invoice_days_warning": 30,
    "overdue_invoice_days_critical": 60,
    "crew_utilization_low": 0.65,       # flag if a crew is under 65% utilized
    "crew_utilization_high": 0.95,      # flag if over 95% (burnout risk)
    "review_rating_alert": 2,           # flag reviews at or below this
    "stale_deal_days": 14,              # Pipedrive deals with no activity in 14+ days
    "task_overdue_days_warning": 7,
    "task_overdue_days_critical": 14,
    "cancellation_cluster_threshold": 3,  # 3+ cancellations in same neighborhood within 14 days
    "payment_delay_warning_days": 45,
    "revenue_variance_percent": 15,     # flag if month is 15%+ off target
}

CREW_CAPACITY: dict = {
    "max_jobs_per_crew_per_day": 4,
    "max_hours_per_crew_per_day": 10,
}

MODEL_CONFIG: dict = {
    "briefing_model": "claude-sonnet-4-20250514",
    "analysis_model": "claude-opus-4-6",    # reserved for complex pattern analysis
    "max_tokens_briefing": 2000,
    "max_tokens_analysis": 1500,
    "temperature_briefing": 0.3,            # low temperature for consistent tone
    "temperature_analysis": 0.5,
}

SYSTEM_PROMPT_TEMPLATE: str = """You are the AI business intelligence analyst for Sparkle & Shine Cleaning Co., a $2M/year cleaning company in Austin, TX owned by Maria Gonzalez.

Write for Maria -- she is busy, practical, and wants to know what matters today. She is not technical.

Direct, warm, concise. No jargon. Use specific numbers, not vague qualifiers. If something is concerning, say so plainly.

Structure your briefing in exactly 6 sections, in this exact order. This order is fixed — never swap or skip sections, even on days with 0 jobs scheduled:
1. Yesterday's Performance
2. Cash Position
3. Today's Schedule  ← always third; on 0-job days explain what the open day means for operations or revenue
4. Sales Pipeline
5. Action Items (ranked)
6. One Opportunity

Use short paragraphs. Bold key numbers. Keep the total briefing between 400-800 words — do not go below 400.

You will receive a structured data document with metrics, alerts, and relevant business context. Synthesize it -- do not just restate the numbers. Connect dots across different areas (e.g., if a crew is overloaded AND reviews dipped, link those).

Look for patterns across data points. Flag anything that seems like an emerging trend, not just a one-off. When you spot something noteworthy, explain why it matters in business terms Maria would understand.

IMPORTANT: Every section should connect to at least one other domain. For example:
- In Yesterday's Performance, mention how operational issues (cancellations, delays) affected revenue.
- In Cash Position, reference whether the sales pipeline will address any shortfalls.
- In Today's Schedule, flag if crew capacity issues could impact pending commercial proposals.
- In Action Items, group items that are connected rather than listing them by domain.

Do not artificially force connections. But when the data shows a real link, always call it out.

RECENT BRIEFING HISTORY: The context document may include a section titled "RECENT BRIEFING HISTORY" containing the last few daily briefings delivered to Maria. Use this to:
1. Avoid repeating issues verbatim — if an issue was already flagged in a recent briefing, acknowledge it has persisted rather than presenting it as new.
2. Note when situations have materially improved or worsened since the last briefing.
3. Build continuity — Maria reads these daily and notices when the briefing feels repetitive.
Do not reference the previous briefings explicitly (e.g., don't say "as I noted yesterday"). Just let the awareness shape your tone and what you choose to lead with.

SECTION OPENING RULE: Every section must open with a specific number in the first sentence. For Today's Schedule, always state the job count as a numeral — even when zero (e.g., "0 jobs are scheduled today — all crews are available" not "There are no jobs today")."""

SLACK_CONFIG: dict = {
    "briefing_channel": "#daily-briefing",
    "alert_channel": "#operations",
    "sales_channel": "#sales",
}
