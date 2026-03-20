"""seeding/generators/gen_anomalies.py

Verifies the 7 planted discovery patterns from gen_tasks_events.py data.
Also handles planting the Westlake cancellation cluster if it is missing.

Run:
    python seeding/generators/gen_anomalies.py
"""
from __future__ import annotations

import random
import sqlite3
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

DB_PATH = ROOT / "sparkle_shine.db"

_RNG = random.Random(42)


# ---------------------------------------------------------------------------
# Plant step — Westlake cancellation cluster (if missing)
# ---------------------------------------------------------------------------

def _ensure_westlake_cancellations(conn: sqlite3.Connection) -> None:
    """
    If the Westlake cancellation cluster is missing but gen_tasks_events has run
    (tasks table has data), plant the cluster using a fresh RNG to avoid interference.
    """
    cur = conn.cursor()

    churned_count = cur.execute(
        """SELECT COUNT(*) FROM clients
           WHERE neighborhood='Westlake' AND status='churned'
             AND last_service_date BETWEEN '2026-02-01' AND '2026-03-15'"""
    ).fetchone()[0]

    tasks_count = cur.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]

    if churned_count >= 3:
        return

    if tasks_count == 0:
        # gen_tasks_events hasn't run yet — can't plant here
        return

    # Use a fresh RNG (different seed) to avoid interfering with any other seeding
    plant_rng = random.Random(99)

    rows = cur.execute(
        """SELECT c.id AS client_id, ra.id AS agreement_id
           FROM clients c
           JOIN recurring_agreements ra ON ra.client_id = c.id
           WHERE c.neighborhood = 'Westlake'
             AND ra.status = 'active'
           ORDER BY c.id"""
    ).fetchall()

    if len(rows) < 3:
        return

    selected = plant_rng.sample(rows, 3)

    cancel_dates = [
        date(2026, 2, 10),
        date(2026, 2, 14),
        date(2026, 2, 21),
    ]

    for i, row in enumerate(selected):
        cancel_str = cancel_dates[i].isoformat()
        cur.execute(
            "UPDATE recurring_agreements SET status='cancelled', end_date=? WHERE id=?",
            (cancel_str, row["agreement_id"]),
        )
        cur.execute(
            """UPDATE clients
               SET status='churned',
                   last_service_date=?,
                   notes = COALESCE(notes || ' | ', '') || 'Client cancelled recurring service — reason unclear'
               WHERE id=?""",
            (cancel_str, row["client_id"]),
        )

    conn.commit()
    print("  Planted Westlake cancellation cluster (was missing)")


# ---------------------------------------------------------------------------
# Pattern verification helpers
# ---------------------------------------------------------------------------

def _pass_label(passed: bool) -> str:
    return "PASS" if passed else "FAIL"


# ---------------------------------------------------------------------------
# run_all
# ---------------------------------------------------------------------------

def run_all(db_path: str | Path = DB_PATH) -> bool:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Ensure Westlake cluster exists
    _ensure_westlake_cancellations(conn)

    all_pass = True
    results: list[tuple[str, bool, str]] = []

    # ------------------------------------------------------------------ #
    # PATTERN 1 — CREW SPEED VS QUALITY
    # ------------------------------------------------------------------ #
    review_count_check = cur.execute("SELECT COUNT(*) FROM reviews").fetchone()[0]

    if review_count_check == 0:
        results.append((
            "Pattern 1 (crew-a quality + duration)",
            False,
            "SKIPPED — reviews table is empty (run gen_tasks_events.py first)",
        ))
    else:
        cur.execute(
            """SELECT j.crew_id,
                      ROUND(AVG(j.duration_minutes_actual), 1) AS avg_duration,
                      ROUND(AVG(r.rating), 2) AS avg_rating,
                      COUNT(r.id) AS review_count
               FROM jobs j
               JOIN reviews r ON r.job_id = j.id
               WHERE j.crew_id IS NOT NULL
               GROUP BY j.crew_id
               ORDER BY avg_rating DESC"""
        )
        crew_rows = {r["crew_id"]: dict(r) for r in cur.fetchall()}

        crew_a = crew_rows.get("crew-a", {})
        crew_a_rating = crew_a.get("avg_rating") or 0.0
        crew_a_duration = crew_a.get("avg_duration") or 0.0

        other_crews = {k: v for k, v in crew_rows.items() if k != "crew-a"}
        other_ratings = [v["avg_rating"] for v in other_crews.values() if v.get("avg_rating")]
        fleet_durations = [v["avg_duration"] for v in crew_rows.values() if v.get("avg_duration")]
        fleet_avg_duration = sum(fleet_durations) / len(fleet_durations) if fleet_durations else 0.0

        highest_rating_among_all = (
            crew_a_rating > max(other_ratings)
            if other_ratings else False
        )
        longer_than_fleet = crew_a_duration > fleet_avg_duration

        p1_pass = highest_rating_among_all and longer_than_fleet

        detail_parts = []
        for crew_id, row in sorted(crew_rows.items()):
            detail_parts.append(
                f"{crew_id}: avg_rating={row.get('avg_rating', 0):.2f} "
                f"avg_duration={row.get('avg_duration', 0):.1f}min "
                f"reviews={row.get('review_count', 0)}"
            )
        detail_parts.append(f"fleet_avg_duration={fleet_avg_duration:.1f}min")
        detail_parts.append(
            f"(need crew-a highest rating + >fleet duration)"
        )
        p1_detail = "  |  ".join(detail_parts)

        results.append(("Pattern 1 (crew-a quality + duration)", p1_pass, p1_detail))

    # ------------------------------------------------------------------ #
    # PATTERN 2 — REFERRAL RETENTION (churn-rate differential)
    # ------------------------------------------------------------------ #
    cur.execute(
        """SELECT acquisition_source,
                  COUNT(*) AS total,
                  SUM(CASE WHEN status='churned' THEN 1 ELSE 0 END) AS churned,
                  ROUND(100.0 * SUM(CASE WHEN status='churned' THEN 1 ELSE 0 END) / COUNT(*), 1)
                      AS churn_rate_pct
           FROM clients
           WHERE client_type='residential'
             AND acquisition_source IN ('referral','Google Ads','organic search','Yelp')
           GROUP BY acquisition_source
           ORDER BY churn_rate_pct ASC"""
    )
    churn_rows = cur.fetchall()

    churn_by_source: dict[str, float] = {}
    for r in churn_rows:
        churn_by_source[r["acquisition_source"]] = float(r["churn_rate_pct"])

    ref_rate = churn_by_source.get("referral", 100.0)
    gads_rate = churn_by_source.get("Google Ads", 0.0)

    c1 = ref_rate < 10.0
    c2 = gads_rate > 20.0
    c3 = ref_rate < gads_rate * 0.40 if gads_rate > 0 else False

    p2_pass = c1 and c2 and c3

    churn_detail_parts = [
        f"{src}: churn={rate:.1f}%"
        for src, rate in sorted(churn_by_source.items(), key=lambda x: x[1])
    ]
    ratio_str = f"{ref_rate / gads_rate:.2f}" if gads_rate > 0 else "N/A"
    p2_detail = (
        f"referral={ref_rate:.1f}% (need <10%)  "
        f"google_ads={gads_rate:.1f}% (need >20%)  "
        f"ratio={ratio_str}× (need <0.40×)"
    )
    if not c1:
        p2_detail += f"  FAIL: referral churn {ref_rate:.1f}% >= 10%"
    if not c2:
        p2_detail += f"  FAIL: Google Ads churn {gads_rate:.1f}% <= 20%"
    if not c3:
        p2_detail += f"  FAIL: referral churn not <40% of Google Ads churn"

    results.append(("Pattern 2 (referral retention)", p2_pass, p2_detail))

    # ------------------------------------------------------------------ #
    # PATTERN 3 — DAY OF WEEK COMPLAINT RATE
    # ------------------------------------------------------------------ #
    if review_count_check == 0:
        results.append((
            "Pattern 3 (day-of-week complaint rate)",
            False,
            "SKIPPED — reviews table is empty (run gen_tasks_events.py first)",
        ))
    else:
        cur.execute(
            """SELECT CASE CAST(strftime('%w', j.scheduled_date) AS INTEGER)
                          WHEN 1 THEN 'Monday'
                          WHEN 2 THEN 'Tuesday'
                          WHEN 3 THEN 'Wednesday'
                          WHEN 4 THEN 'Thursday'
                          WHEN 5 THEN 'Friday'
                          ELSE 'Weekend' END AS dow,
                     CAST(strftime('%w', j.scheduled_date) AS INTEGER) AS dow_num,
                     COUNT(*) AS total_reviews,
                     SUM(CASE WHEN r.rating <= 2 THEN 1 ELSE 0 END) AS complaint_reviews,
                     ROUND(100.0 * SUM(CASE WHEN r.rating <= 2 THEN 1 ELSE 0 END) / COUNT(*), 1)
                         AS complaint_rate_pct
               FROM reviews r
               JOIN jobs j ON j.id = r.job_id
               WHERE strftime('%w', j.scheduled_date) BETWEEN '1' AND '5'
               GROUP BY dow
               ORDER BY CAST(strftime('%w', j.scheduled_date) AS INTEGER)"""
        )
        dow_rows = {r["dow"]: dict(r) for r in cur.fetchall()}

        mon_rate = dow_rows.get("Monday", {}).get("complaint_rate_pct") or 0.0
        tue_rate = dow_rows.get("Tuesday", {}).get("complaint_rate_pct") or 0.0
        wed_rate = dow_rows.get("Wednesday", {}).get("complaint_rate_pct") or 0.0
        thu_rate = dow_rows.get("Thursday", {}).get("complaint_rate_pct") or 0.0
        fri_rate = dow_rows.get("Friday", {}).get("complaint_rate_pct") or 0.0

        tue_wed_avg = (tue_rate + wed_rate) / 2
        mon_thu_fri_avg = (mon_rate + thu_rate + fri_rate) / 3 if (mon_rate + thu_rate + fri_rate) > 0 else 0.0

        p3_pass = tue_wed_avg < mon_thu_fri_avg * 0.80 if mon_thu_fri_avg > 0 else False

        p3_detail = (
            f"Mon={mon_rate:.1f}%  Tue={tue_rate:.1f}%  Wed={wed_rate:.1f}%  "
            f"Thu={thu_rate:.1f}%  Fri={fri_rate:.1f}%  "
            f"Tue+Wed avg={tue_wed_avg:.1f}%  Mon+Thu+Fri avg={mon_thu_fri_avg:.1f}%  "
            f"(need Tue+Wed < Mon+Thu+Fri × 0.80)"
        )
        results.append(("Pattern 3 (day-of-week complaint rate)", p3_pass, p3_detail))

    # ------------------------------------------------------------------ #
    # PATTERN 4 — COMMERCIAL UPSELL SIGNAL
    # ------------------------------------------------------------------ #
    cur.execute(
        """SELECT id, notes FROM jobs
           WHERE client_id = (SELECT id FROM clients WHERE company_name = 'Barton Creek Medical Group')
             AND notes LIKE '%add-on%'"""
    )
    upsell_rows = cur.fetchall()
    upsell_count = len(upsell_rows)
    p4_pass = upsell_count >= 3
    p4_detail = (
        f"jobs with 'add-on' upsell signal={upsell_count}  (need >=3)"
    )
    results.append(("Pattern 4 (commercial upsell signal)", p4_pass, p4_detail))

    # ------------------------------------------------------------------ #
    # PATTERN 5 — WESTLAKE CANCELLATION CLUSTER
    # ------------------------------------------------------------------ #
    cur.execute(
        """SELECT c.id, c.last_service_date, ra.end_date
           FROM clients c
           JOIN recurring_agreements ra ON ra.client_id = c.id
           WHERE c.neighborhood = 'Westlake'
             AND ra.status = 'cancelled'
             AND ra.end_date BETWEEN '2026-02-01' AND '2026-03-15'
           ORDER BY ra.end_date"""
    )
    cancel_rows = cur.fetchall()
    n_cancels = len(cancel_rows)

    p5_pass = False
    window_days = 0
    if n_cancels >= 3:
        end_dates = [date.fromisoformat(r["end_date"]) for r in cancel_rows]
        window_days = (max(end_dates) - min(end_dates)).days
        p5_pass = window_days <= 14

    p5_detail = (
        f"Westlake cancellations in Feb-Mar 2026={n_cancels}  "
        f"window={window_days} days  (need >=3 within 14-day window)"
    )
    results.append(("Pattern 5 (Westlake cancellation cluster)", p5_pass, p5_detail))

    # ------------------------------------------------------------------ #
    # PATTERN 6 — MARIA DELEGATION INSIGHT
    # ------------------------------------------------------------------ #
    tasks_count_check = cur.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]

    if tasks_count_check == 0:
        results.append((
            "Pattern 6 (Maria delegation insight)",
            False,
            "SKIPPED — tasks table is empty (run gen_tasks_events.py first)",
        ))
    else:
        cur.execute(
            """SELECT e.first_name || ' ' || e.last_name AS assignee,
                      e.role,
                      COUNT(*) AS total_tasks,
                      SUM(CASE WHEN t.status = 'overdue' THEN 1 ELSE 0 END) AS overdue_tasks,
                      ROUND(100.0 * SUM(CASE WHEN t.status = 'overdue' THEN 1 ELSE 0 END) / COUNT(*), 1)
                          AS overdue_rate_pct
               FROM tasks t
               JOIN employees e ON e.id = t.assignee_employee_id
               WHERE t.project_name = 'Admin & Operations'
               GROUP BY t.assignee_employee_id
               ORDER BY overdue_rate_pct DESC"""
        )
        assignee_rows = {r["assignee"]: dict(r) for r in cur.fetchall()}

        maria_data = assignee_rows.get("Maria Gonzalez", {})
        patricia_data = assignee_rows.get("Patricia Nguyen", {})

        maria_overdue_rate = float(maria_data.get("overdue_rate_pct") or 0.0)
        patricia_overdue_rate = float(patricia_data.get("overdue_rate_pct") or 0.0)

        p6_pass = maria_overdue_rate >= 30.0 and patricia_overdue_rate <= 20.0

        p6_detail = (
            f"Maria overdue_rate={maria_overdue_rate:.1f}% (need >=30%)  "
            f"total={maria_data.get('total_tasks', 0)} tasks  "
            f"Patricia overdue_rate={patricia_overdue_rate:.1f}% (need <=20%)  "
            f"total={patricia_data.get('total_tasks', 0)} tasks"
        )
        results.append(("Pattern 6 (Maria delegation insight)", p6_pass, p6_detail))

    # ------------------------------------------------------------------ #
    # PATTERN 7 — REFERRAL PROGRAM CONTRACT VALUE
    # ------------------------------------------------------------------ #
    cur.execute(
        """SELECT l.source,
                  COUNT(*) AS proposals,
                  ROUND(AVG(p.monthly_value), 2) AS avg_monthly_value
           FROM commercial_proposals p
           JOIN leads l ON l.id = p.lead_id
           WHERE p.status IN ('draft', 'sent', 'negotiating', 'lost')
             AND l.lead_type = 'commercial'
           GROUP BY l.source
           ORDER BY avg_monthly_value DESC"""
    )
    proposal_rows = {r["source"]: dict(r) for r in cur.fetchall()}

    referral_data = proposal_rows.get("referral", {})
    referral_avg = float(referral_data.get("avg_monthly_value") or 0.0)

    non_referral_avgs = [
        float(v["avg_monthly_value"])
        for src, v in proposal_rows.items()
        if src != "referral" and v.get("avg_monthly_value")
    ]
    non_referral_avg = (
        sum(non_referral_avgs) / len(non_referral_avgs)
        if non_referral_avgs else 0.0
    )

    # Pass if referral_avg > non_referral_avg × 1.20 (10% tolerance on 30% target)
    p7_pass = referral_avg > non_referral_avg * 1.20 if non_referral_avg > 0 else False

    source_details = "  ".join(
        f"{src}: n={v['proposals']} avg=${float(v['avg_monthly_value']):.0f}"
        for src, v in sorted(proposal_rows.items(), key=lambda x: -float(x[1].get("avg_monthly_value") or 0))
    )
    ratio_str = f"{referral_avg / non_referral_avg:.2f}" if non_referral_avg > 0 else "N/A"
    p7_detail = (
        f"referral avg=${referral_avg:.0f}  non-referral avg=${non_referral_avg:.0f}  "
        f"ratio={ratio_str}× (need >1.20×)  |  {source_details}"
    )
    results.append(("Pattern 7 (referral program contract value)", p7_pass, p7_detail))

    # ------------------------------------------------------------------ #
    # Print results
    # ------------------------------------------------------------------ #
    conn.close()

    print()
    print("=" * 72)
    print("  SPARKLE & SHINE — PLANTED PATTERN DETECTION RESULTS (Phase 3)")
    print("=" * 72)

    for name, passed, detail in results:
        flag = _pass_label(passed)
        print(f"\n  [{flag}] {name}")
        print(f"         {detail}")
        if not passed:
            all_pass = False

    print()
    print("-" * 72)
    passing = sum(1 for _, p, _ in results if p)
    print(f"  {passing}/{len(results)} patterns confirmed detectable.")
    if all_pass:
        print("  Calibration complete. All 7 patterns confirmed detectable.")
    else:
        print("  Some patterns failed — review detail lines above.")
    print("-" * 72)
    print()

    return all_pass


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    import os
    db = os.environ.get("SS_DB_PATH", str(DB_PATH))
    success = run_all(db)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
