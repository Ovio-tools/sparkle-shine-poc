"""
intelligence/metrics/revenue.py

Revenue metrics relative to the briefing date.
All monetary values come from the payments table; client segmentation is
resolved via payments -> invoices -> jobs -> clients.
"""

import calendar
from datetime import date, timedelta

from intelligence.config import ALERT_THRESHOLDS, REVENUE_TARGETS


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
    yesterday_total = db.execute(
        "SELECT COALESCE(SUM(amount), 0.0) FROM payments WHERE payment_date = ?",
        (str(yesterday),),
    ).fetchone()[0]

    job_count = db.execute(
        "SELECT COUNT(*) FROM jobs WHERE date(completed_at) = ? AND status = 'completed'",
        (str(yesterday),),
    ).fetchone()[0]

    # Revenue by client_type: payments -> clients (direct FK on payments.client_id)
    rows = db.execute(
        """
        SELECT c.client_type, COALESCE(SUM(p.amount), 0.0)
        FROM payments p
        JOIN clients c ON p.client_id = c.id
        WHERE p.payment_date = ?
        GROUP BY c.client_type
        """,
        (str(yesterday),),
    ).fetchall()
    type_rev = {r[0]: r[1] for r in rows}
    residential = type_rev.get("residential", 0.0)
    commercial = type_rev.get("commercial", 0.0)
    avg_job_value = (yesterday_total / job_count) if job_count > 0 else 0.0

    # ------------------------------------------------------------------ #
    # Week-to-date vs. same days last week
    # ------------------------------------------------------------------ #
    wtd_total = db.execute(
        "SELECT COALESCE(SUM(amount), 0.0) FROM payments WHERE payment_date BETWEEN ? AND ?",
        (str(monday_this_week), str(yesterday)),
    ).fetchone()[0]

    last_monday = monday_this_week - timedelta(days=7)
    last_week_end = yesterday - timedelta(days=7)
    wtd_last = db.execute(
        "SELECT COALESCE(SUM(amount), 0.0) FROM payments WHERE payment_date BETWEEN ? AND ?",
        (str(last_monday), str(last_week_end)),
    ).fetchone()[0]

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
    mtd_total = db.execute(
        "SELECT COALESCE(SUM(amount), 0.0) FROM payments WHERE payment_date BETWEEN ? AND ?",
        (str(first_of_month), str(yesterday)),
    ).fetchone()[0]

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
    # Trailing 30 days vs. prior 30 days
    # ------------------------------------------------------------------ #
    t30_start = yesterday - timedelta(days=29)
    prior_end = t30_start - timedelta(days=1)
    prior_start = prior_end - timedelta(days=29)

    t30_total = db.execute(
        "SELECT COALESCE(SUM(amount), 0.0) FROM payments WHERE payment_date BETWEEN ? AND ?",
        (str(t30_start), str(yesterday)),
    ).fetchone()[0]

    prior_30_total = db.execute(
        "SELECT COALESCE(SUM(amount), 0.0) FROM payments WHERE payment_date BETWEEN ? AND ?",
        (str(prior_start), str(prior_end)),
    ).fetchone()[0]

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
                f"Revenue is {pct_behind:.0f}% below monthly target pace "
                f"(${mtd_total:,.0f} vs. expected ${expected_by_now:,.0f})"
            )
        elif pct_ahead >= variance_threshold:
            alerts.append(
                f"Revenue is {pct_ahead:.0f}% above monthly target pace — "
                f"strong month so far (${mtd_total:,.0f} vs. expected ${expected_by_now:,.0f})"
            )

    if vs_prior_30 <= -15.0:
        alerts.append(
            f"Trailing 30-day revenue is down {abs(vs_prior_30):.0f}% vs. prior 30 days "
            f"(${t30_total:,.0f} vs. ${prior_30_total:,.0f})"
        )

    return {
        "yesterday": {
            "total": round(yesterday_total, 2),
            "job_count": job_count,
            "residential": round(residential, 2),
            "commercial": round(commercial, 2),
            "avg_job_value": round(avg_job_value, 2),
        },
        "week_to_date": {
            "total": round(wtd_total, 2),
            "vs_last_week": round(vs_last_week, 1),
            "vs_last_week_direction": vs_last_week_direction,
        },
        "month_to_date": {
            "total": round(mtd_total, 2),
            "target_low": float(target_low),
            "target_high": float(target_high),
            "pacing": pacing,
            "projected_month_end": round(projected_month_end, 2),
        },
        "trailing_30_days": {
            "total": round(t30_total, 2),
            "vs_prior_30": round(vs_prior_30, 1),
        },
        "alerts": alerts,
    }
