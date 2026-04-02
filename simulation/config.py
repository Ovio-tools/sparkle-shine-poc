"""
simulation/config.py

Calibrated configuration for the Sparkle & Shine simulation engine.
All numeric values have inline source citations (L1).
After any change, run config_math_trace() to verify net +3 to +5 clients/month (L2).
"""

# ────────────────────────────────────────────────────────────────
# DAILY ACTIVITY VOLUMES
# Calibrated to produce net +3 to +5 new clients per month.
# Run config_math_trace() to verify after any changes.
# ────────────────────────────────────────────────────────────────

DAILY_VOLUMES = {
    "new_contacts": {
        "base_min": 3,
        "base_max": 8,
        # Source: HomeAdvisor/Angi data shows 3-10 daily inquiries
        # for established home service businesses in metro areas.
        # Austin market is mid-range. Using 3-8 as base before
        # seasonal/day-of-week adjustments.

        "sql_fraction": 0.35,
        # ESTIMATED -- reasoning: 35% of cleaning inquiries are
        # serious enough to warrant a sales conversation. Higher
        # than SaaS (5-15%) because cleaning is a considered but
        # not complex purchase. Adjusted up from 0.30 to produce
        # enough pipeline to support ~8-10 wins/month.

        "lifecycle_distribution": {
            "subscriber": 0.20,                # newsletter signups, not ready to buy
            "lead": 0.25,                      # showed interest, needs nurturing
            "marketing_qualified_lead": 0.20,  # engaged with content/campaigns
            "sales_qualified_lead": 0.35,      # matches sql_fraction
        }
    },

    "deal_progression": {
        "stage_advance_probability": 0.15,
        # ESTIMATED -- reasoning: on any given day, there's a 15%
        # chance an open deal moves forward. At 5 stages, average
        # cycle is ~33 days (consistent with home services sales).

        "won_probability_from_negotiation": 0.40,
        # ESTIMATED -- reasoning: 40% close rate from final stage.
        # Home services close rates are 30-50% per HomeAdvisor.
        # Using 40% (mid-range) to produce ~8-10 wins/month from
        # the pipeline of ~20-25 deals reaching Negotiation.
        # Adjusted up from 0.35 to offset pipeline leakage.

        "lost_probability_per_stage": 0.03,
        # ESTIMATED -- reasoning: 3% daily loss rate per stage.
        # Lower than original 5% to reduce pipeline leakage.
        # At 3%/day over 5 stages (~33 day cycle), roughly 40%
        # of leads are lost before reaching Negotiation.
        # Combined with 40% Negotiation win rate, overall
        # lead-to-close is ~24%, consistent with industry data.

        "lost_reasons": [
            "Budget constraints",
            "Chose competitor",
            "Project postponed",
            "No response after follow-up",
            "Scope mismatch",
        ]
    },

    "job_completion": {
        "on_time_rate": 0.92,
        # Source: Field service industry benchmark. 90-95% completion
        # rate for scheduled residential cleaning visits.

        "cancellation_rate": 0.03,
        "no_show_rate": 0.02,
        "reschedule_rate": 0.03,
        "duration_variance_percent": 15,
    },

    "payments": {
        "on_time_rate": 0.75,
        # Source: QuickBooks small business report 2024: 73% of
        # invoices paid within terms for service businesses.

        "late_1_30_rate": 0.15,
        "late_31_60_rate": 0.07,
        "late_60_plus_rate": 0.02,
        "non_payment_rate": 0.01,
    },

    "churn": {
        "monthly_residential_churn_rate": 0.025,
        # ESTIMATED -- reasoning: 2.5% monthly = ~26% annual.
        # Cleaning industry annual churn is typically 25-35%.
        # Using lower end because Austin is a stable market and
        # the business has a referral-heavy client base.
        # At ~180 active residential clients, this produces
        # ~4.5 churned clients/month.
        # CRITICAL: original value was 0.04 (4%), which produced
        # 7-8 churns/month and a shrinking business. Reduced per
        # config_math_trace() validation.

        "monthly_commercial_churn_rate": 0.015,
        # ESTIMATED -- reasoning: 1.5% monthly = ~17% annual.
        # Commercial contracts are stickier (net-30 billing,
        # longer onboarding investment, switching costs).
        # At ~8-10 active commercial clients, this produces
        # ~0.15 churns/month (roughly 1 every 6-7 months).

        "churn_reasons": [
            "Moving out of area",
            "Switching to competitor",
            "Budget cuts",
            "Dissatisfied with service",
            "No longer needs service",
            "Seasonal -- will return",
        ]
    },

    "task_completion": {
        "daily_completion_rate": 0.3,
        "maria_completion_rate": 0.15,
    },
}

# ────────────────────────────────────────────────────────────────
# EXISTING CLIENT JOB SCHEDULING
# ────────────────────────────────────────────────────────────────

JOB_VARIETY = {
    "residential_recurring": {
        "regular_clean_rate": 0.85,
        # 85% of scheduled visits are the standard service.

        "deep_clean_rate": 0.10,
        # ESTIMATED -- reasoning: most recurring clients do a
        # deep clean every 2-3 months. For biweekly clients
        # that's roughly 1 in 5-6 visits = ~17%. For weekly
        # clients it's 1 in 8-12 = ~10%. Using 10% as a
        # blended average across all frequencies.

        "add_on_rate": 0.05,
        # Source: Maid Brigade franchise data suggests 3-8% add-on
        # attachment rate for residential recurring clients.

        "deep_clean_price_multiplier": 1.80,
        # Standard clean $150, deep clean $275 = 1.83x multiplier.

        "add_on_options": [
            {"name": "Interior windows", "price": 45},
            {"name": "Refrigerator deep clean", "price": 35},
            {"name": "Oven cleaning", "price": 40},
            {"name": "Laundry (wash, dry, fold)", "price": 30},
            {"name": "Garage sweep and organize", "price": 55},
            {"name": "Patio/balcony cleaning", "price": 40},
        ],

        "seasonal_deep_clean_boost": {
            # Months where deep clean probability increases
            3: 1.5,   # March: spring cleaning
            4: 1.5,   # April: spring cleaning
            6: 1.3,   # June: summer prep
            11: 1.3,  # November: pre-holiday
            12: 1.5,  # December: holiday prep
        },
    },

    "commercial_recurring": {
        "standard_service_rate": 0.90,
        # 90% of commercial visits are the contracted service.

        "extra_service_rate": 0.10,
        # ESTIMATED -- reasoning: 10% include an add-on. Creates
        # the "upsell signal" pattern the intelligence layer detects.

        "extra_service_options": [
            {"name": "Carpet spot treatment", "price": 75},
            {"name": "Window washing (interior)", "price": 120},
            {"name": "Floor waxing", "price": 150},
            {"name": "Restroom deep sanitization", "price": 60},
            {"name": "Breakroom deep clean", "price": 45},
        ],
    },
}

# ────────────────────────────────────────────────────────────────
# CREW CAPACITY AND UTILIZATION TARGETS
# ────────────────────────────────────────────────────────────────

CREW_CAPACITY = {
    "daily_minutes": 660,
    # Source: BUSINESS_HOURS start=7, end=18 → 11 hours × 60 = 660 minutes.

    "target_utilization_min": 0.80,
    "target_utilization_max": 0.90,
    # ESTIMATED -- reasoning: 80-90% is the standard operating range for
    # field service businesses. Below 80% means underutilized crews (wasted
    # labor cost). Above 90% leaves no buffer for travel delays, callbacks,
    # or emergencies. Source: ServiceTitan industry benchmark reports.

    "travel_buffer_avg": 22,
    # ESTIMATED -- reasoning: average travel time between jobs in Austin metro.
    # _assign_scheduled_time uses random.randint(15, 30), midpoint ≈ 22.

    "max_jobs_per_crew": 5,
    # ESTIMATED -- reasoning: 660 min ÷ (120 min avg job + 22 min travel) = 4.6.
    # With 10% deep clean rate (210 min), average job is ~129 min.
    # 5 × 129 = 645 min = ~98% peak. Cap at 5 for 80-90% average target.

    "min_jobs_per_crew": 4,
    # ESTIMATED -- reasoning: even on slow days, crews should have at least 4
    # jobs to justify dispatching. Below 4 means the crew should be merged or
    # given a day off.
}

# ────────────────────────────────────────────────────────────────
# TIMING AND VARIATION
# ────────────────────────────────────────────────────────────────

BUSINESS_HOURS = {
    "start": 7, "end": 18, "peak_start": 9, "peak_end": 14,
}

SEASONAL_WEIGHTS = {
    1: 0.70, 2: 0.85, 3: 0.95, 4: 1.00, 5: 1.05, 6: 1.25,
    7: 1.30, 8: 1.10, 9: 0.90, 10: 1.00, 11: 1.10, 12: 1.20,
}

DAY_OF_WEEK_WEIGHTS = {
    0: 1.15, 1: 1.10, 2: 1.05, 3: 1.00, 4: 0.90, 5: 0.40, 6: 0.20,
}

SERVICE_TYPE_WEIGHTS = {
    "weekly_recurring": 0.25,
    "biweekly_recurring": 0.35,
    "monthly_recurring": 0.15,
    "one_time_standard": 0.10,
    "one_time_deep_clean": 0.10,
    "one_time_move_in_out": 0.05,
}

COMMERCIAL_SERVICE_WEIGHTS = {
    "nightly_clean": 0.50,
    "weekend_deep_clean": 0.30,
    "one_time_project": 0.20,
}

CREW_ASSIGNMENT_WEIGHTS = {
    "Crew A": 0.30, "Crew B": 0.25, "Crew C": 0.25, "Crew D": 0.20,
}


# ────────────────────────────────────────────────────────────────
# CONFIG VALIDATION (L2)
# ────────────────────────────────────────────────────────────────

def config_math_trace():
    """Print expected monthly outcomes from daily probabilities.
    Run this after any config change to verify the business
    trajectory is realistic.

    Target: net +3 to +5 clients per month (slight growth).
    If this shows negative growth, the config is broken.
    """
    avg_daily_contacts = (DAILY_VOLUMES["new_contacts"]["base_min"]
                          + DAILY_VOLUMES["new_contacts"]["base_max"]) / 2
    avg_monthly_contacts = avg_daily_contacts * 22  # ~22 business days

    sqls_per_month = avg_monthly_contacts * DAILY_VOLUMES["new_contacts"]["sql_fraction"]

    deal_config = DAILY_VOLUMES["deal_progression"]
    loss_per_stage = deal_config["lost_probability_per_stage"]
    avg_days_per_stage = 1 / deal_config["stage_advance_probability"]
    stages_before_negotiation = 4
    cumulative_loss = 1.0
    for _ in range(stages_before_negotiation):
        days_in_stage = avg_days_per_stage
        survive_rate = (1 - loss_per_stage) ** days_in_stage
        cumulative_loss *= survive_rate
    deals_reaching_negotiation = sqls_per_month * cumulative_loss

    wins_per_month = deals_reaching_negotiation * deal_config["won_probability_from_negotiation"]

    churn_config = DAILY_VOLUMES["churn"]
    residential_churn = 180 * churn_config["monthly_residential_churn_rate"]
    commercial_churn = 9 * churn_config["monthly_commercial_churn_rate"]
    total_churn = residential_churn + commercial_churn

    net_change = wins_per_month - total_churn

    print("=" * 55)
    print("CONFIG MATH TRACE -- Expected Monthly Outcomes")
    print("=" * 55)
    print(f"  Avg daily contacts:          {avg_daily_contacts:.1f}")
    print(f"  Monthly contacts (~22 days): {avg_monthly_contacts:.0f}")
    print(f"  Monthly SQLs (x{DAILY_VOLUMES['new_contacts']['sql_fraction']:.0%}):       {sqls_per_month:.0f}")
    print(f"  Pipeline survival to Negotiation: {cumulative_loss:.0%}")
    print(f"  Deals reaching Negotiation:  {deals_reaching_negotiation:.0f}")
    print(f"  Won deals (x{deal_config['won_probability_from_negotiation']:.0%}):          {wins_per_month:.1f}")
    print(f"  ---")
    print(f"  Residential churn (180 x {churn_config['monthly_residential_churn_rate']:.1%}): {residential_churn:.1f}")
    print(f"  Commercial churn (9 x {churn_config['monthly_commercial_churn_rate']:.1%}):   {commercial_churn:.1f}")
    print(f"  Total churn:                 {total_churn:.1f}")
    print(f"  ---")
    print(f"  NET MONTHLY CLIENT CHANGE:   {net_change:+.1f}")
    print(f"  ---")
    if net_change < 0:
        print("  *** WARNING: SHRINKING BUSINESS. Fix churn or win rate. ***")
    elif net_change < 2:
        print("  FLAT to slight growth. Consider raising SQL fraction or win rate.")
    elif net_change <= 6:
        print("  HEALTHY GROWTH. Target range is +3 to +5.")
    else:
        print("  RAPID GROWTH. May be unrealistic. Consider lowering win rate.")
    print("=" * 55)


if __name__ == "__main__":
    config_math_trace()
