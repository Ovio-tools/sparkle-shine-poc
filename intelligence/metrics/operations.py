"""
intelligence/metrics/operations.py

Operational metrics: yesterday's job outcomes, today's crew schedule,
7-day crew performance, duration variance, and cancellation cluster detection.
"""

from datetime import date, timedelta

from config.business import CREWS as CREWS_CONFIG, SERVICE_TYPES
from intelligence.config import ALERT_THRESHOLDS, CREW_CAPACITY

# Expected duration (minutes) by service_type_id
_SERVICE_DURATION: dict[str, int] = {
    s["id"]: s["duration_minutes"] for s in SERVICE_TYPES
}

# Crew display names by DB id
_CREW_NAMES: dict[str, str] = {c["id"]: c["name"] for c in CREWS_CONFIG}

_MAX_HOURS = CREW_CAPACITY["max_hours_per_crew_per_day"]
_MAX_MINUTES = _MAX_HOURS * 60


def compute(db, briefing_date: str) -> dict:
    today = date.fromisoformat(briefing_date)
    yesterday = today - timedelta(days=1)
    seven_days_ago = today - timedelta(days=7)
    fourteen_days_ago = today - timedelta(days=14)

    # ------------------------------------------------------------------ #
    # Yesterday's job outcomes
    # ------------------------------------------------------------------ #
    rows = db.execute(
        """
        SELECT status, COUNT(*) AS cnt
        FROM jobs
        WHERE scheduled_date = %s
        GROUP BY status
        """,
        (str(yesterday),),
    ).fetchall()
    status_counts = {r["status"]: r["cnt"] for r in rows}

    completed = status_counts.get("completed", 0)
    cancelled = status_counts.get("cancelled", 0)
    no_show = status_counts.get("no-show", 0)
    total_yesterday = completed + cancelled + no_show
    completion_rate = (completed / total_yesterday) if total_yesterday > 0 else 0.0

    # ------------------------------------------------------------------ #
    # Today's schedule — grouped by crew
    # ------------------------------------------------------------------ #
    today_jobs = db.execute(
        """
        SELECT j.crew_id, cr.name AS crew_name, j.service_type_id,
               j.duration_minutes_actual
        FROM jobs j
        LEFT JOIN crews cr ON j.crew_id = cr.id
        WHERE j.scheduled_date = %s
          AND j.status IN ('scheduled', 'in_progress')
        """,
        (str(today),),
    ).fetchall()

    # Accumulate per-crew totals
    crew_jobs: dict[str, dict] = {}
    for row in today_jobs:
        crew_id = row["crew_id"] or "unassigned"
        crew_name = row["crew_name"] or _CREW_NAMES.get(crew_id, crew_id)
        if crew_id not in crew_jobs:
            crew_jobs[crew_id] = {"name": crew_name, "jobs": 0, "minutes": 0}
        crew_jobs[crew_id]["jobs"] += 1
        # Use actual duration if set; fall back to service type expected duration
        expected_min = _SERVICE_DURATION.get(row["service_type_id"], 120)
        duration = row["duration_minutes_actual"] or expected_min
        crew_jobs[crew_id]["minutes"] += duration

    by_crew = {}
    gaps = []
    overloaded = []

    for crew_id, data in crew_jobs.items():
        hours = round(data["minutes"] / 60.0, 2)
        utilization = round(data["minutes"] / _MAX_MINUTES, 3) if _MAX_MINUTES > 0 else 0.0
        available_hours = round(_MAX_HOURS - hours, 2)

        by_crew[data["name"]] = {
            "jobs": data["jobs"],
            "hours": hours,
            "utilization": utilization,
        }

        low_threshold = ALERT_THRESHOLDS["crew_utilization_low"]
        high_threshold = ALERT_THRESHOLDS["crew_utilization_high"]

        if utilization < low_threshold:
            gaps.append({
                "crew": data["name"],
                "utilization": utilization,
                "available_hours": available_hours,
            })
        if utilization > high_threshold:
            overloaded.append({
                "crew": data["name"],
                "utilization": utilization,
            })

    total_jobs_today = sum(d["jobs"] for d in by_crew.values())

    # ------------------------------------------------------------------ #
    # 7-day crew performance (completed jobs)
    # ------------------------------------------------------------------ #
    perf_rows = db.execute(
        """
        SELECT j.crew_id, cr.name AS crew_name,
               COUNT(j.id) AS job_count,
               AVG(r.rating) AS avg_rating
        FROM jobs j
        LEFT JOIN crews cr ON j.crew_id = cr.id
        LEFT JOIN reviews r ON r.job_id = j.id
        WHERE j.status = 'completed'
          AND j.completed_at::date BETWEEN %s AND %s
        GROUP BY j.crew_id, cr.name
        """,
        (str(seven_days_ago), str(yesterday)),
    ).fetchall()

    # Duration variance for the same window
    var_rows = db.execute(
        """
        SELECT j.crew_id, cr.name AS crew_name,
               j.service_type_id, j.duration_minutes_actual
        FROM jobs j
        LEFT JOIN crews cr ON j.crew_id = cr.id
        WHERE j.status = 'completed'
          AND j.completed_at::date BETWEEN %s AND %s
          AND j.duration_minutes_actual IS NOT NULL
        """,
        (str(seven_days_ago), str(yesterday)),
    ).fetchall()

    # Compute per-crew variance percentages
    crew_variances: dict[str, list[float]] = {}
    for row in var_rows:
        crew_id = row["crew_id"] or "unassigned"
        expected = _SERVICE_DURATION.get(row["service_type_id"], 120)
        if expected > 0:
            variance_pct = ((row["duration_minutes_actual"] - expected) / expected) * 100.0
            crew_variances.setdefault(crew_id, []).append(variance_pct)

    # Build crew_performance_7day (merge perf + variance)
    crew_performance_7day = {}
    for row in perf_rows:
        crew_id = row["crew_id"] or "unassigned"
        crew_name = row["crew_name"] or _CREW_NAMES.get(crew_id, crew_id)
        variances = crew_variances.get(crew_id, [])
        avg_var = round(sum(variances) / len(variances), 1) if variances else 0.0
        crew_performance_7day[crew_name] = {
            "avg_duration_variance": avg_var,
            "avg_rating": round(float(row["avg_rating"]), 2) if row["avg_rating"] is not None else None,
            "jobs": row["job_count"],
        }

    # Overall duration variance summary
    all_variances = [v for vlist in crew_variances.values() for v in vlist]
    avg_variance_percent = round(sum(all_variances) / len(all_variances), 1) if all_variances else 0.0

    worst_crew = None
    worst_variance = 0.0
    for crew_id, vlist in crew_variances.items():
        avg_v = sum(vlist) / len(vlist)
        if abs(avg_v) > abs(worst_variance):
            worst_variance = round(avg_v, 1)
            worst_crew = _CREW_NAMES.get(crew_id, crew_id)

    # ------------------------------------------------------------------ #
    # Alerts
    # ------------------------------------------------------------------ #
    alerts = []

    for entry in overloaded:
        alerts.append(
            f"{entry['crew']} is at {entry['utilization']*100:.0f}% utilization today "
            f"-- consider redistributing jobs"
        )

    for entry in gaps:
        alerts.append(
            f"Staffing gap — {entry['crew']} at {entry['utilization']*100:.0f}% utilization today "
            f"({entry['available_hours']:.1f} hrs available); may indicate understaffing or reduced bookings"
        )

    # Cancellation cluster detection (last 14 days by neighborhood)
    cluster_threshold = ALERT_THRESHOLDS["cancellation_cluster_threshold"]
    cluster_rows = db.execute(
        """
        SELECT c.neighborhood, COUNT(*) AS cancel_count
        FROM jobs j
        JOIN clients c ON j.client_id = c.id
        WHERE j.status = 'cancelled'
          AND j.scheduled_date BETWEEN %s AND %s
          AND c.neighborhood IS NOT NULL
        GROUP BY c.neighborhood
        HAVING COUNT(*) >= %s
        """,
        (str(fourteen_days_ago), str(yesterday), cluster_threshold),
    ).fetchall()

    for row in cluster_rows:
        alerts.append(
            f"{row['cancel_count']} cancellations in {row['neighborhood']} in the last 14 days "
            f"-- possible competitor activity"
        )

    # Full cancellation breakdown by neighborhood (last 28 days, all counts ≥ 1)
    # Used by the weekly report to surface neighbourhood trends.
    neighborhood_28d = today - timedelta(days=28)
    neighborhood_rows = db.execute(
        """
        SELECT c.neighborhood, COUNT(*) AS cancel_count
        FROM jobs j
        JOIN clients c ON j.client_id = c.id
        WHERE j.status = 'cancelled'
          AND j.scheduled_date BETWEEN %s AND %s
          AND c.neighborhood IS NOT NULL
        GROUP BY c.neighborhood
        ORDER BY cancel_count DESC
        """,
        (str(neighborhood_28d), str(yesterday)),
    ).fetchall()

    cancellation_by_neighborhood = [
        {"neighborhood": row["neighborhood"], "cancel_count": row["cancel_count"]}
        for row in neighborhood_rows
    ]

    if yesterday_total := completed + cancelled + no_show:
        if completion_rate < 0.80:
            alerts.append(
                f"Low completion rate yesterday: {completion_rate*100:.0f}% "
                f"({completed} completed, {cancelled} cancelled, {no_show} no-show)"
            )

    return {
        "yesterday": {
            "completed": completed,
            "cancelled": cancelled,
            "no_show": no_show,
            "completion_rate": round(completion_rate, 3),
        },
        "today_schedule": {
            "total_jobs": total_jobs_today,
            "by_crew": by_crew,
            "gaps": gaps,
            "overloaded": overloaded,
        },
        "crew_performance_7day": crew_performance_7day,
        "duration_variance_7day": {
            "avg_variance_percent": avg_variance_percent,
            "worst_crew": worst_crew,
            "worst_variance": worst_variance,
        },
        "cancellation_by_neighborhood": cancellation_by_neighborhood,
        "alerts": alerts,
    }
