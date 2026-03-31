"""
intelligence/metrics/financial_health.py

Cash position, AR aging, DSO, late payer detection, and bad debt risk.

Note: bank_balance is not stored in sparkle_shine.db (it would come from a
QBO sync). open_invoices_value from daily_metrics_snapshot is used as a
proxy for total_ar.
"""

from datetime import date, timedelta

from intelligence.config import ALERT_THRESHOLDS


def compute(db, briefing_date: str) -> dict:
    today = date.fromisoformat(briefing_date)
    ninety_days_ago = today - timedelta(days=90)

    payment_delay_warning = ALERT_THRESHOLDS["payment_delay_warning_days"]
    critical_days = ALERT_THRESHOLDS["overdue_invoice_days_critical"]

    # ------------------------------------------------------------------ #
    # 90-day revenue — computed first so it can be used for bank balance
    # proxy and DSO.
    # ------------------------------------------------------------------ #
    revenue_90 = db.execute(
        """
        SELECT COALESCE(SUM(amount), 0.0) AS total
        FROM payments
        WHERE payment_date BETWEEN %s AND %s
        """,
        (str(ninety_days_ago), str(today)),
    ).fetchone()["total"]

    # ------------------------------------------------------------------ #
    # Cash position — pull latest snapshot for AR; derive bank balance
    # from recent cash flow (no QBO sync required for a proxy).
    # ------------------------------------------------------------------ #
    snapshot = db.execute(
        """
        SELECT open_invoices_value, overdue_invoices_value
        FROM daily_metrics_snapshot
        ORDER BY snapshot_date DESC
        LIMIT 1
        """,
    ).fetchone()

    total_ar = snapshot["open_invoices_value"] if snapshot else 0.0
    total_ap = 0.0  # not tracked in this schema

    # Expected AR collectible in 30 days = current (0-30 day) invoices
    ar_30_row = db.execute(
        """
        SELECT COALESCE(SUM(amount), 0.0) AS total
        FROM invoices
        WHERE status IN ('sent', 'overdue')
          AND (days_outstanding IS NULL OR days_outstanding <= 30)
        """,
    ).fetchone()["total"]

    # Bank balance proxy: 90-day revenue minus estimated operating expenses.
    # Cleaning industry typical expense ratio ≈ 65% (labor, supplies, overhead).
    # This is a derived estimate; a live QBO sync would give the real figure.
    _EXPENSE_RATIO = 0.65
    bank_balance = round(max(0.0, revenue_90 * (1.0 - _EXPENSE_RATIO)), 2)

    net_position = bank_balance - total_ap + ar_30_row

    # ------------------------------------------------------------------ #
    # AR aging buckets
    # ------------------------------------------------------------------ #
    aging_rows = db.execute(
        """
        SELECT
            SUM(CASE WHEN COALESCE(days_outstanding, 0) <= 30
                     THEN 1 ELSE 0 END) AS cnt_0_30,
            SUM(CASE WHEN COALESCE(days_outstanding, 0) <= 30
                     THEN amount ELSE 0 END) AS val_0_30,

            SUM(CASE WHEN COALESCE(days_outstanding, 0) BETWEEN 31 AND 60
                     THEN 1 ELSE 0 END) AS cnt_31_60,
            SUM(CASE WHEN COALESCE(days_outstanding, 0) BETWEEN 31 AND 60
                     THEN amount ELSE 0 END) AS val_31_60,

            SUM(CASE WHEN COALESCE(days_outstanding, 0) BETWEEN 61 AND 90
                     THEN 1 ELSE 0 END) AS cnt_61_90,
            SUM(CASE WHEN COALESCE(days_outstanding, 0) BETWEEN 61 AND 90
                     THEN amount ELSE 0 END) AS val_61_90,

            SUM(CASE WHEN COALESCE(days_outstanding, 0) > 90
                     THEN 1 ELSE 0 END) AS cnt_90_plus,
            SUM(CASE WHEN COALESCE(days_outstanding, 0) > 90
                     THEN amount ELSE 0 END) AS val_90_plus
        FROM invoices
        WHERE status IN ('sent', 'overdue')
        """,
    ).fetchone()

    ar_aging = {
        "current_0_30": {
            "count": aging_rows["cnt_0_30"] or 0,
            "total": round(aging_rows["val_0_30"] or 0.0, 2),
        },
        "past_due_31_60": {
            "count": aging_rows["cnt_31_60"] or 0,
            "total": round(aging_rows["val_31_60"] or 0.0, 2),
        },
        "past_due_61_90": {
            "count": aging_rows["cnt_61_90"] or 0,
            "total": round(aging_rows["val_61_90"] or 0.0, 2),
        },
        "past_due_90_plus": {
            "count": aging_rows["cnt_90_plus"] or 0,
            "total": round(aging_rows["val_90_plus"] or 0.0, 2),
        },
    }

    bad_debt_risk = ar_aging["past_due_90_plus"]["total"]

    # ------------------------------------------------------------------ #
    # DSO — (total current AR / revenue last 90 days) * 90
    # ------------------------------------------------------------------ #
    dso = round((total_ar / revenue_90 * 90.0), 1) if revenue_90 > 0 else 0.0

    # ------------------------------------------------------------------ #
    # DSO trend — compute DSO for each of the 4 most recent weeks
    # (ending on briefing_date, going back 3 more weeks).
    # Uses the same bank-balance proxy approach (revenue * expense ratio).
    # ------------------------------------------------------------------ #
    dso_trend: list[dict] = []
    for weeks_back in range(3, -1, -1):  # 3, 2, 1, 0 → oldest first
        week_end = today - timedelta(weeks=weeks_back)
        week_start_90 = week_end - timedelta(days=90)
        rev_w = db.execute(
            "SELECT COALESCE(SUM(amount), 0.0) AS total FROM payments WHERE payment_date BETWEEN %s AND %s",
            (str(week_start_90), str(week_end)),
        ).fetchone()["total"]
        # Pull closest AR snapshot on or before week_end
        snap_w = db.execute(
            """
            SELECT open_invoices_value FROM daily_metrics_snapshot
            WHERE snapshot_date <= %s
            ORDER BY snapshot_date DESC LIMIT 1
            """,
            (str(week_end),),
        ).fetchone()
        ar_w = snap_w["open_invoices_value"] if snap_w else total_ar
        dso_w = round((ar_w / rev_w * 90.0), 1) if rev_w > 0 else 0.0
        dso_trend.append({"week_ending": str(week_end), "dso": dso_w})

    # ------------------------------------------------------------------ #
    # AR threshold crossings — invoices that just hit the 30-day or
    # 60-day overdue mark today (within a ±2 day window to catch any
    # daily variance in how days_outstanding is updated).
    # ------------------------------------------------------------------ #
    crossing_rows = db.execute(
        """
        SELECT c.first_name || ' ' || COALESCE(c.last_name, '') AS full_name,
               COALESCE(c.company_name, '') AS company_name,
               SUM(i.amount) AS total_outstanding,
               MAX(COALESCE(i.days_outstanding, 0)) AS max_days
        FROM invoices i
        JOIN clients c ON i.client_id = c.id
        WHERE i.status IN ('sent', 'overdue')
          AND (
              COALESCE(i.days_outstanding, 0) BETWEEN 28 AND 32
              OR COALESCE(i.days_outstanding, 0) BETWEEN 58 AND 62
          )
          AND c.status != 'churned'
        GROUP BY i.client_id, c.first_name, c.last_name, c.company_name
        ORDER BY total_outstanding DESC
        """,
    ).fetchall()

    ar_threshold_crossings: list[dict] = []
    for row in crossing_rows:
        display_name = row["company_name"].strip() if row["company_name"].strip() else row["full_name"].strip()
        max_d = row["max_days"]
        threshold = 60 if max_d >= 58 else 30
        ar_threshold_crossings.append({
            "client_name": display_name,
            "amount": round(row["total_outstanding"], 2),
            "days_overdue": max_d,
            "threshold_crossed": threshold,
        })

    # ------------------------------------------------------------------ #
    # Late payers (invoices outstanding > warning threshold)
    # ------------------------------------------------------------------ #
    # Exclude churned clients: their unpaid invoices are bad debt (already
    # captured in bad_debt_risk), not actionable "late payers".
    late_rows = db.execute(
        """
        SELECT c.first_name || ' ' || COALESCE(c.last_name, '') AS client_name,
               COALESCE(c.company_name, '') AS company_name,
               SUM(i.amount) AS total_outstanding,
               MAX(COALESCE(i.days_outstanding, 0)) AS max_days,
               COUNT(i.id) AS invoice_count
        FROM invoices i
        JOIN clients c ON i.client_id = c.id
        WHERE i.status IN ('sent', 'overdue')
          AND COALESCE(i.days_outstanding, 0) > %s
          AND c.status != 'churned'
        GROUP BY i.client_id, c.first_name, c.last_name, c.company_name
        ORDER BY total_outstanding DESC
        """,
        (payment_delay_warning,),
    ).fetchall()

    late_payers = []
    for row in late_rows:
        display_name = row["company_name"].strip() if row["company_name"].strip() else row["client_name"].strip()
        late_payers.append({
            "client_name": display_name,
            "amount": round(row["total_outstanding"], 2),
            "days_overdue": row["max_days"],
            "invoice_count": row["invoice_count"],
        })

    # ------------------------------------------------------------------ #
    # Alerts
    # ------------------------------------------------------------------ #
    alerts = []

    # Emit individual alerts only for the top 5 late payers (already sorted by
    # amount desc); summarise the remainder to avoid flooding the alert list.
    _LATE_PAYER_ALERT_CAP = 5
    warning_days = ALERT_THRESHOLDS["overdue_invoice_days_warning"]
    for lp in late_payers[:_LATE_PAYER_ALERT_CAP]:
        if lp["days_overdue"] >= critical_days:
            alerts.append(
                f"{lp['client_name']} has ${lp['amount']:,.0f} outstanding "
                f"at {lp['days_overdue']} days overdue"
            )
        elif lp["days_overdue"] >= warning_days:
            alerts.append(
                f"{lp['client_name']} has ${lp['amount']:,.0f} outstanding "
                f"at {lp['days_overdue']} days (approaching overdue)"
            )

    remaining = late_payers[_LATE_PAYER_ALERT_CAP:]
    if remaining:
        remaining_total = sum(lp["amount"] for lp in remaining)
        alerts.append(
            f"{len(remaining)} additional late payers totalling "
            f"${remaining_total:,.0f} — see cash position section for full list"
        )

    if bad_debt_risk > 5_000:
        alerts.append(
            f"${bad_debt_risk:,.0f} in invoices have been unpaid for 90+ days — worth reviewing"
        )

    # Flag significant 60+ day AR buckets
    aged_61_90_total = ar_aging["past_due_61_90"]["total"]
    aged_90_plus_total = ar_aging["past_due_90_plus"]["total"]
    aged_60_plus = aged_61_90_total + aged_90_plus_total
    aged_60_plus_count = ar_aging["past_due_61_90"]["count"] + ar_aging["past_due_90_plus"]["count"]
    if aged_60_plus > 0:
        alerts.append(
            f"${aged_60_plus:,.0f} in invoices have been unpaid for 60+ days "
            f"across {aged_60_plus_count} invoices — it may be worth following up"
        )

    if dso > 45:
        alerts.append(
            f"On average, invoices are taking {dso:.0f} days to be paid "
            f"(aim for under 30) — collections may need attention"
        )

    return {
        "cash_position": {
            "bank_balance": round(bank_balance, 2),
            "total_ar": round(total_ar, 2),
            "total_ap": round(total_ap, 2),
            "net_position": round(net_position, 2),
        },
        "ar_aging": ar_aging,
        "dso": dso,
        "dso_trend": dso_trend,
        "ar_threshold_crossings": ar_threshold_crossings,
        "late_payers": late_payers,
        "bad_debt_risk": round(bad_debt_risk, 2),
        "alerts": alerts,
    }
