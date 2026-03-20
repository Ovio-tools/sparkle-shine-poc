"""
intelligence/metrics/tasks.py

Task health metrics: open/overdue counts by project and assignee,
critical overdue tasks (14+ days), and per-assignee overdue rates.

Overdue definition: status = 'overdue' OR
    (status IN ('not_started','in_progress') AND due_date < briefing_date)
"""

from datetime import date, timedelta

from intelligence.config import ALERT_THRESHOLDS


def compute(db, briefing_date: str) -> dict:
    today = date.fromisoformat(briefing_date)
    yesterday = today - timedelta(days=1)

    critical_overdue_days = ALERT_THRESHOLDS["task_overdue_days_critical"]

    # ------------------------------------------------------------------ #
    # Helper: "is overdue" expression for SQL (always qualified with t.)
    # ------------------------------------------------------------------ #
    overdue_expr = f"""
        (t.status = 'overdue'
         OR (t.status IN ('not_started','in_progress')
             AND t.due_date IS NOT NULL
             AND t.due_date < '{briefing_date}'))
    """

    # ------------------------------------------------------------------ #
    # Overview totals
    # ------------------------------------------------------------------ #
    overview_row = db.execute(
        f"""
        SELECT
            SUM(CASE WHEN t.status != 'completed' THEN 1 ELSE 0 END) AS total_open,
            SUM(CASE WHEN {overdue_expr} THEN 1 ELSE 0 END)           AS total_overdue,
            SUM(CASE WHEN date(t.completed_date) = ?  THEN 1 ELSE 0 END) AS completed_yesterday,
            SUM(CASE WHEN date(t.created_at) = ?      THEN 1 ELSE 0 END) AS created_yesterday
        FROM tasks t
        """,
        (str(yesterday), str(yesterday)),
    ).fetchone()

    total_open = overview_row["total_open"] or 0
    total_overdue = overview_row["total_overdue"] or 0
    overdue_rate = round(total_overdue / total_open, 3) if total_open > 0 else 0.0

    # ------------------------------------------------------------------ #
    # By project
    # ------------------------------------------------------------------ #
    project_rows = db.execute(
        f"""
        SELECT t.project_name,
               SUM(CASE WHEN t.status != 'completed' THEN 1 ELSE 0 END) AS open_cnt,
               SUM(CASE WHEN {overdue_expr} THEN 1 ELSE 0 END)           AS overdue_cnt
        FROM tasks t
        WHERE t.project_name IS NOT NULL
        GROUP BY t.project_name
        """,
    ).fetchall()

    by_project = {}
    for row in project_rows:
        by_project[row["project_name"]] = {
            "open": row["open_cnt"] or 0,
            "overdue": row["overdue_cnt"] or 0,
        }

    # ------------------------------------------------------------------ #
    # By assignee (join employees for display name)
    # ------------------------------------------------------------------ #
    assignee_rows = db.execute(
        f"""
        SELECT
            t.assignee_employee_id,
            e.first_name || ' ' || COALESCE(e.last_name, '') AS full_name,
            e.role,
            SUM(CASE WHEN t.status != 'completed' THEN 1 ELSE 0 END) AS open_cnt,
            SUM(CASE WHEN {overdue_expr} THEN 1 ELSE 0 END)           AS overdue_cnt
        FROM tasks t
        LEFT JOIN employees e ON t.assignee_employee_id = e.id
        WHERE t.assignee_employee_id IS NOT NULL
        GROUP BY t.assignee_employee_id
        """,
    ).fetchall()

    by_assignee = {}
    for row in assignee_rows:
        open_cnt = row["open_cnt"] or 0
        overdue_cnt = row["overdue_cnt"] or 0
        rate = round(overdue_cnt / open_cnt, 3) if open_cnt > 0 else 0.0
        display = (row["full_name"] or "").strip() or row["role"] or row["assignee_employee_id"]
        by_assignee[display] = {
            "open": open_cnt,
            "overdue": overdue_cnt,
            "overdue_rate": rate,
        }

    # ------------------------------------------------------------------ #
    # Critical overdue tasks (14+ days past due date)
    # ------------------------------------------------------------------ #
    critical_rows = db.execute(
        f"""
        SELECT t.title,
               e.first_name || ' ' || COALESCE(e.last_name, '') AS full_name,
               e.role,
               t.project_name,
               CAST(julianday('{briefing_date}') - julianday(t.due_date) AS INTEGER) AS days_overdue
        FROM tasks t
        LEFT JOIN employees e ON t.assignee_employee_id = e.id
        WHERE {overdue_expr}
          AND t.due_date IS NOT NULL
          AND julianday('{briefing_date}') - julianday(t.due_date) >= ?
        ORDER BY days_overdue DESC
        """,
        (critical_overdue_days,),
    ).fetchall()

    critical_overdue = []
    for row in critical_rows:
        assignee = (row["full_name"] or "").strip() or row["role"] or "Unassigned"
        critical_overdue.append({
            "title": row["title"],
            "assignee": assignee,
            "days_overdue": row["days_overdue"] or critical_overdue_days,
            "project": row["project_name"],
        })

    # ------------------------------------------------------------------ #
    # Alerts
    # ------------------------------------------------------------------ #
    alerts = []

    warning_threshold = ALERT_THRESHOLDS["task_overdue_days_warning"]
    team_overdue_rate = overdue_rate

    # Flag individuals with overdue rates significantly above team average
    for assignee, stats in by_assignee.items():
        if stats["open"] >= 5 and stats["overdue_rate"] > team_overdue_rate * 1.5:
            alerts.append(
                f"{assignee} has {stats['overdue']} overdue tasks "
                f"({stats['overdue_rate']*100:.0f}% overdue rate) "
                f"vs. team avg of {team_overdue_rate*100:.0f}%"
            )

    if critical_overdue:
        alerts.append(
            f"{len(critical_overdue)} task(s) overdue by {critical_overdue_days}+ days "
            f"— requires immediate attention"
        )

    if total_overdue > 20:
        alerts.append(
            f"High overall task debt: {total_overdue} overdue tasks "
            f"({overdue_rate*100:.0f}% overdue rate)"
        )

    return {
        "overview": {
            "total_open": total_open,
            "total_overdue": total_overdue,
            "overdue_rate": overdue_rate,
            "completed_yesterday": overview_row["completed_yesterday"] or 0,
            "created_yesterday": overview_row["created_yesterday"] or 0,
        },
        "by_project": by_project,
        "by_assignee": by_assignee,
        "critical_overdue": critical_overdue,
        "alerts": alerts,
    }
