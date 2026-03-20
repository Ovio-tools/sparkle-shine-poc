"""
demo/scenarios/scenario_definitions.py

Defines the 6 narrative scenarios used by the demo scenario runner.
Each scenario maps to a specific date in the 12-month Sparkle & Shine story
and declares the business signals and discovery patterns we expect to see.
"""

from __future__ import annotations

SCENARIOS: list[dict] = [
    {
        "id": "steady_state",
        "name": "Steady State Operations",
        "date": "2025-05-15",
        "narrative_period": "Months 1-2 (Apr-May 2025)",
        "description": (
            "Business running smoothly. 140 residential clients, "
            "7 commercial contracts, ~$140K/month revenue. "
            "No major issues. Baseline for comparison."
        ),
        "expected_signals": [
            "Revenue on track or slightly ahead of target",
            "All 4 crews at healthy utilization (70-85%)",
            "Pipeline has normal deal flow",
            "No critical alerts",
        ],
        "discovery_patterns": [],  # none expected at baseline
    },
    {
        "id": "summer_surge",
        "name": "Summer Surge",
        "date": "2025-06-20",
        "narrative_period": "Months 3-4 (Jun-Jul 2025)",
        "description": (
            "Deep clean demand spike. 2 new hires ramping up. "
            "'Spring into summer' campaign converting 18 of 25 leads. "
            "2 commercial contracts not renewing."
        ),
        "expected_signals": [
            "Revenue above target (surge)",
            "Crew utilization approaching or exceeding 95%",
            "New lead volume elevated",
            "Commercial pipeline showing losses",
        ],
        "discovery_patterns": [
            "Tuesday/Wednesday complaint rate advantage",
        ],
    },
    {
        "id": "rough_patch",
        "name": "Rough Patch",
        "date": "2025-09-10",
        "narrative_period": "Months 5-6 (Aug-Sep 2025)",
        "description": (
            "2 cleaners quit. Scheduling stretched thin. "
            "3 negative reviews. Maria raised rates, causing "
            "5 more cancellations. Revenue below target."
        ),
        "expected_signals": [
            "Revenue below target with declining trend",
            "Crew utilization unbalanced (some over, some under)",
            "Negative review alerts",
            "Cancellation cluster in Westlake",
            "Staffing gap warnings",
        ],
        "discovery_patterns": [
            "Westlake cancellation cluster (competitor signal)",
            "Crew quality vs. speed tradeoff visible",
        ],
    },
    {
        "id": "big_win",
        "name": "Stabilization + Big Win",
        "date": "2025-10-20",
        "narrative_period": "Months 7-8 (Oct-Nov 2025)",
        "description": (
            "Replacement hires on board. New medical office contract "
            "($4,500/mo). Referral program launched with 15 new "
            "residential clients. Holiday pre-bookings starting."
        ),
        "expected_signals": [
            "Revenue recovering toward target",
            "New commercial deal highlighted",
            "Referral program early results visible",
            "Pipeline building for holiday season",
        ],
        "discovery_patterns": [
            "Referral clients showing higher retention",
            "Referral leads with higher contract value",
        ],
    },
    {
        "id": "holiday_crunch",
        "name": "Holiday Peak + Cash Crunch",
        "date": "2025-12-18",
        "narrative_period": "Months 9-10 (Dec-Jan 2026)",
        "description": (
            "Revenue at annual peak. But 2 commercial clients "
            "paying late (50-60 days on net-30 terms). Cash flow "
            "tightening despite strong top line."
        ),
        "expected_signals": [
            "Revenue well above target",
            "AR aging showing 60+ day buckets",
            "Late payer alerts with specific client names",
            "Cash position warning despite high revenue",
            "DSO trending up",
        ],
        "discovery_patterns": [
            "Commercial upsell signal (monthly add-ons)",
            "Maria delegation insight (overdue task rate)",
        ],
    },
    {
        "id": "recovery",
        "name": "Recovery + Pipeline Building",
        "date": "2026-03-17",
        "narrative_period": "Months 11-12 (Feb-Mar 2026)",
        "description": (
            "Spring recovery. 12 active commercial proposals. "
            "'New year fresh start' campaign running. Revenue "
            "trending back toward target."
        ),
        "expected_signals": [
            "Revenue approaching target",
            "Strong pipeline (12 open proposals)",
            "Campaign engagement metrics",
            "Staffing stable, utilization balanced",
        ],
        "discovery_patterns": [
            "Referral vs. Google Ads retention comparison",
            "Maria overdue task rate vs. office manager",
        ],
    },
]

# Convenience lookup by id
SCENARIO_BY_ID: dict[str, dict] = {s["id"]: s for s in SCENARIOS}
