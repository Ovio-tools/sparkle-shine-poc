"""
intelligence/metrics/revenue.py

Revenue metrics relative to the briefing date.

Primary pacing metrics use booked revenue from job-linked invoices so month
targets are compared against earned work, not collection timing. Cash
collection is included alongside booked revenue for finance visibility.
"""

import calendar
from datetime import date, timedelta
from typing import Optional

from intelligence import config as intel_config
from intelligence.config import ALERT_THRESHOLDS, REVENUE_TARGETS


def _sum_cash(db, start_date: str, end_date: Optional[str] = None) -> float:
    """Return cash collected from payments over a date or date range."""
    if end_date is None:
        return db.execute(
            "SELECT COALESCE(SUM(amount), 0.0) AS total FROM payments WHERE payment_date = %s",
            (start_date,),
        ).fetchone()["total"]

    return db.execute(
        "SELECT COALESCE(SUM(amount), 0.0) AS total FROM payments WHERE payment_date BETWEEN %s AND %s",
        (start_date, end_date),
    ).fetchone()["total"]


def _segment_cash(db, target_date: str) -> dict[str, float]:
    rows = db.execute(
        """
        SELECT c.client_type, COALESCE(SUM(p.amount), 0.0) AS total
        FROM payments p
        JOIN clients c ON p.client_id = c.id
        WHERE p.payment_date = %s
        GROUP BY c.client_type
        """,
        (target_date,),
    ).fetchall()
    return {row["client_type"]: row["total"] for row in rows}


def _sum_booked(db, start_date: str, end_date: Optional[str] = None) -> float:
    """Return booked revenue from job-linked invoices over a completion-date window.

    Defense in depth: the INNER JOIN already excludes orphan invoices
    (job_id IS NULL), but we add an explicit NOT NULL filter so a future
    refactor to LEFT JOIN cannot silently reintroduce them.
    """
    if end_date is None:
        return db.execute(
            """
            SELECT COALESCE(SUM(i.amount), 0.0) AS total
            FROM invoices i
            JOIN jobs j ON j.id = i.job_id
            WHERE i.job_id IS NOT NULL
              AND j.status = 'completed'
              AND j.completed_at::date = %s
            """,
            (start_date,),
        ).fetchone()["total"]

    return db.execute(
        """
        SELECT COALESCE(SUM(i.amount), 0.0) AS total
        FROM invoices i
        JOIN jobs j ON j.id = i.job_id
        WHERE i.job_id IS NOT NULL
          AND j.status = 'completed'
          AND j.completed_at::date BETWEEN %s AND %s
        """,
        (start_date, end_date),
    ).fetchone()["total"]


def _segment_booked(db, target_date: str) -> dict[str, float]:
    rows = db.execute(
        """
        SELECT c.client_type, COALESCE(SUM(i.amount), 0.0) AS total
        FROM invoices i
        JOIN jobs j ON j.id = i.job_id
        JOIN clients c ON j.client_id = c.id
        WHERE i.job_id IS NOT NULL
          AND j.status = 'completed'
          AND j.completed_at::date = %s
        GROUP BY c.client_type
        """,
        (target_date,),
    ).fetchall()
    return {row["client_type"]: row["total"] for row in rows}


def compute(db, briefing_date: str) -> dict:
    """Return revenue metrics dict for the given briefing date.

    briefing_date is the *current* day; "yesterday" is briefing_date - 1.
    """
    today = date.fromisoformat(briefing_date)
    yesterday = today - timedelta(days=1)

    # --- date anchors ---
    monday_this_week = yesterday - timedelta(days=yesterday.weekday())
    first_of_month = today.replace(day=1)
    days_elapsed = max((yesterday - first_of_month).days + 1, 1)
    days_in_month = calendar.monthrange(today.year, today.month)[1]

    # ------------------------------------------------------------------ #
    # Yesterday
    # ------------------------------------------------------------------ #
    yesterday_total = _sum_booked(db, str(yesterday))
    yesterday_cash = _sum_cash(db, str(yesterday))

    job_count = db.execute(
        "SELECT COUNT(*) AS cnt FROM jobs WHERE completed_at::date = %s AND status = 'completed'",
        (str(yesterday),),
    ).fetchone()["cnt"]

    type_rev = _segment_booked(db, str(yesterday))
    type_cash = _segment_cash(db, str(yesterday))
    residential = type_rev.get("residential", 0.0)
    commercial = type_rev.get("commercial", 0.0)
    residential_cash = type_cash.get("residential", 0.0)
    commercial_cash = type_cash.get("commercial", 0.0)
    avg_job_value = (yesterday_total / job_count) if job_count > 0 else 0.0

    # ------------------------------------------------------------------ #
    # Week-to-date vs. same days last week
    # ------------------------------------------------------------------ #
    wtd_total = _sum_booked(db, str(monday_this_week), str(yesterday))
    wtd_cash = _sum_cash(db, str(monday_this_week), str(yesterday))

    last_monday = monday_this_week - timedelta(days=7)
    last_week_end = yesterday - timedelta(days=7)
    wtd_last = _sum_booked(db, str(last_monday), str(last_week_end))

    if wtd_last > 0:
        vs_last_week = ((wtd_total - wtd_last) / wtd_last) * 100.0
    else:
        vs_last_week = 0.0

    if vs_last_week > 1.0:
        vs_last_week_direction = "up"
    elif vs_last_week < -1.0:
        vs_last_week_direction = "down"
    else:
        vs_last_week_direction = "flat"

    # ------------------------------------------------------------------ #
    # Month-to-date
    # ------------------------------------------------------------------ #
    mtd_total = _sum_booked(db, str(first_of_month), str(yesterday))
    mtd_cash = _sum_cash(db, str(first_of_month), str(yesterday))

    target_low, target_high = REVENUE_TARGETS.get(
        (today.year, today.month), (135_000, 150_000)
    )
    mid_target = (target_low + target_high) / 2.0
    expected_by_now = mid_target * (days_elapsed / days_in_month)

    if days_elapsed > 0:
        projected_month_end = (mtd_total / days_elapsed) * days_in_month
    else:
        projected_month_end = 0.0

    if mtd_total >= expected_by_now * 1.05:
        pacing = "ahead"
    elif mtd_total >= expected_by_now * 0.85:
        pacing = "on_track"
    else:
        pacing = "behind"

    # ------------------------------------------------------------------ #
    # Cash collection pacing (separate from booked-revenue pacing)
    #
    # Booked revenue says "did we earn enough work?" Cash pacing says
    # "is collection lag tracking what we expect?" The two must be
    # evaluated separately so finance can tell the difference between
    # "work shortfall" and "customers paying slower than usual".
    #
    # Expected collection ratio range (MTD cash / MTD booked):
    #   low  = 0.70 — blended floor given Sparkle's ~70/30 residential/
    #                 commercial mix (residential due-on-receipt lands
    #                 same/next day; commercial on net-30 lags into the
    #                 following month). Source: intelligence/config.py
    #                 REVENUE_TARGETS trajectory + simulation/generators/
    #                 payments.py timing windows.
    #   high = 1.05 — if cash exceeds booked, we're collecting prior-
    #                 month AR faster than usual (healthy) or repriced
    #                 historical invoices landed this month.
    # Pacing buckets mirror the booked thresholds so both read the same.
    # ------------------------------------------------------------------ #
    EXPECTED_CASH_RATIO_LOW = 0.70
    EXPECTED_CASH_RATIO_HIGH = 1.05

    if mtd_total > 0:
        collection_ratio = mtd_cash / mtd_total
    else:
        collection_ratio = 0.0

    if mtd_total <= 0:
        cash_pacing = "unknown"
    elif collection_ratio >= EXPECTED_CASH_RATIO_HIGH:
        cash_pacing = "ahead"
    elif collection_ratio >= EXPECTED_CASH_RATIO_LOW:
        cash_pacing = "on_track"
    else:
        cash_pacing = "behind"

    if days_elapsed > 0:
        projected_month_end_cash = (mtd_cash / days_elapsed) * days_in_month
    else:
        projected_month_end_cash = 0.0

    # ------------------------------------------------------------------ #
    # Trailing 30 days vs. prior 30 days
    # ------------------------------------------------------------------ #
    t30_start = yesterday - timedelta(days=29)
    prior_end = t30_start - timedelta(days=1)
    prior_start = prior_end - timedelta(days=29)

    t30_total = _sum_booked(db, str(t30_start), str(yesterday))
    t30_cash = _sum_cash(db, str(t30_start), str(yesterday))

    prior_30_total = _sum_booked(db, str(prior_start), str(prior_end))

    if prior_30_total > 0:
        vs_prior_30 = ((t30_total - prior_30_total) / prior_30_total) * 100.0
    else:
        vs_prior_30 = 0.0

    # ------------------------------------------------------------------ #
    # Alerts
    # ------------------------------------------------------------------ #
    alerts = []
    variance_threshold = ALERT_THRESHOLDS["revenue_variance_percent"]

    if days_elapsed >= 3 and expected_by_now > 0:
        pct_behind = ((expected_by_now - mtd_total) / expected_by_now) * 100.0
        pct_ahead = ((mtd_total - expected_by_now) / expected_by_now) * 100.0

        if pct_behind >= variance_threshold:
            alerts.append(
                f"Booked revenue is {pct_behind:.0f}% below monthly target pace "
                f"(${mtd_total:,.0f} vs. expected ${expected_by_now:,.0f})"
            )
        elif pct_ahead >= variance_threshold:
            alerts.append(
                f"Booked revenue is {pct_ahead:.0f}% above monthly target pace — "
                f"strong month so far (${mtd_total:,.0f} vs. expected ${expected_by_now:,.0f})"
            )

    if vs_prior_30 <= -15.0:
        alerts.append(
            f"Trailing 30-day booked revenue is down {abs(vs_prior_30):.0f}% vs. prior 30 days "
            f"(${t30_total:,.0f} vs. ${prior_30_total:,.0f})"
        )

    # Cash-collection alert: fires only when booked revenue is healthy but
    # cash is materially lagging. If booked is also behind, the booked alert
    # already tells the story and a second alert would just add noise.
    # Gated on CASH_COLLECTION_ALERT_ENABLED — off until Track D brings
    # payment timing in line with the 0.70 floor (see intelligence/config.py).
    if (
        getattr(intel_config, "CASH_COLLECTION_ALERT_ENABLED", False)
        and days_elapsed >= 5
        and mtd_total > 0
        and cash_pacing == "behind"
        and pacing in ("on_track", "ahead")
    ):
        alerts.append(
            f"Cash collection is lagging booked revenue — ${mtd_cash:,.0f} collected vs. "
            f"${mtd_total:,.0f} booked MTD ({collection_ratio:.0%}, below {EXPECTED_CASH_RATIO_LOW:.0%} floor)"
        )

    return {
        "metric_basis": "booked_revenue",
        "yesterday": {
            "total": round(yesterday_total, 2),
            "job_count": job_count,
            "residential": round(residential, 2),
            "commercial": round(commercial, 2),
            "avg_job_value": round(avg_job_value, 2),
            "cash_collected": round(yesterday_cash, 2),
            "cash_collected_residential": round(residential_cash, 2),
            "cash_collected_commercial": round(commercial_cash, 2),
        },
        "week_to_date": {
            "total": round(wtd_total, 2),
            "vs_last_week": round(vs_last_week, 1),
            "vs_last_week_direction": vs_last_week_direction,
            "cash_collected": round(wtd_cash, 2),
        },
        "month_to_date": {
            "total": round(mtd_total, 2),
            "target_low": float(target_low),
            "target_high": float(target_high),
            "pacing": pacing,
            "projected_month_end": round(projected_month_end, 2),
            "cash_collected": round(mtd_cash, 2),
        },
        "cash_pacing": {
            "mtd_cash": round(mtd_cash, 2),
            "mtd_booked": round(mtd_total, 2),
            "collection_ratio": round(collection_ratio, 3),
            "expected_ratio_low": EXPECTED_CASH_RATIO_LOW,
            "expected_ratio_high": EXPECTED_CASH_RATIO_HIGH,
            "pacing": cash_pacing,
            "projected_month_end_cash": round(projected_month_end_cash, 2),
        },
        "trailing_30_days": {
            "total": round(t30_total, 2),
            "vs_prior_30": round(vs_prior_30, 1),
            "cash_collected": round(t30_cash, 2),
        },
        "alerts": alerts,
    }
