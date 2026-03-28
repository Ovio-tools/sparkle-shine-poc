"""
simulation/variation.py

Applies daily multipliers (seasonal × day-of-week × noise) to volume
and probability values. Imported by the simulation engine and generators.
"""

import random
from datetime import date, datetime

from simulation.config import BUSINESS_HOURS, DAY_OF_WEEK_WEIGHTS, SEASONAL_WEIGHTS


def get_daily_multiplier(target_date: date) -> float:
    """Combine seasonal weight, day-of-week weight, and ±15% random noise.

    When the engine seeds the RNG with the target date (L7), calling this
    with the same date twice in the same process returns identical results.
    """
    seasonal = SEASONAL_WEIGHTS.get(target_date.month, 1.0)
    day_of_week = DAY_OF_WEEK_WEIGHTS.get(target_date.weekday(), 1.0)
    noise = random.uniform(0.85, 1.15)
    return seasonal * day_of_week * noise


def get_adjusted_volume(base_min: int, base_max: int, target_date: date) -> int:
    """Return a daily event count scaled by the daily multiplier. Minimum 0."""
    base = random.randint(base_min, base_max)
    multiplied = base * get_daily_multiplier(target_date)
    return max(0, round(multiplied))


def should_event_happen(probability: float, target_date: date) -> bool:
    """Return True with probability adjusted by the daily multiplier.

    Used for low-frequency per-entity events (churn checks, add-ons).
    A probability of 0.0 always returns False. Values above 1.0 after
    adjustment always return True since random.random() is in [0, 1).
    """
    adjusted = probability * get_daily_multiplier(target_date)
    return random.random() < adjusted


def get_next_event_delay(target_date: date) -> float:
    """Return seconds until the next simulated event.

    Busier days (higher multiplier) produce shorter delays.
    Peak hours (9-14): 3-15 min. Business hours (7-18): 10-30 min.
    Off-hours: 45-120 min. Minimum 30s regardless of multiplier.
    """
    now = datetime.now()
    hour = now.hour
    multiplier = get_daily_multiplier(target_date)

    peak_start = BUSINESS_HOURS["peak_start"]
    peak_end = BUSINESS_HOURS["peak_end"]
    biz_start = BUSINESS_HOURS["start"]
    biz_end = BUSINESS_HOURS["end"]

    if peak_start <= hour < peak_end:
        base_delay = random.uniform(180, 900)    # 3-15 minutes
    elif biz_start <= hour < biz_end:
        base_delay = random.uniform(600, 1800)   # 10-30 minutes
    else:
        base_delay = random.uniform(2700, 7200)  # 45-120 minutes

    return max(30.0, base_delay / max(multiplier, 0.1))
