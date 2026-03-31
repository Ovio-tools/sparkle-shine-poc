"""
intelligence/metrics/marketing.py

Marketing metrics: most recent campaign performance, audience health,
lead source conversion, and 7-day review summary.

Note: campaign_cost is not stored in this schema, so roi_estimate is
computed using a $500 flat cost estimate per campaign as a placeholder.
Actual cost tracking would require a QuickBooks expense sync.
"""

from datetime import date, timedelta

from intelligence.config import ALERT_THRESHOLDS

_CAMPAIGN_COST_ESTIMATE = 500.0  # placeholder until QBO expense sync is added


def compute(db, briefing_date: str) -> dict:
    today = date.fromisoformat(briefing_date)
    yesterday = today - timedelta(days=1)
    seven_days_ago = today - timedelta(days=7)
    thirty_days_ago = today - timedelta(days=30)

    review_alert_threshold = ALERT_THRESHOLDS["review_rating_alert"]

    # ------------------------------------------------------------------ #
    # Most recent campaign
    # ------------------------------------------------------------------ #
    campaign = db.execute(
        """
        SELECT id, name, send_date, open_rate, click_rate,
               conversion_count, recipient_count
        FROM marketing_campaigns
        WHERE send_date IS NOT NULL
        ORDER BY send_date DESC
        LIMIT 1
        """,
    ).fetchone()

    if campaign:
        # Estimate revenue from conversions using avg job value (~$200 proxy)
        avg_job_value_estimate = 200.0
        conversion_revenue = campaign["conversion_count"] * avg_job_value_estimate
        cost = _CAMPAIGN_COST_ESTIMATE
        roi_estimate = round((conversion_revenue - cost) / cost, 3) if cost > 0 else None

        # open_rate and click_rate are stored as percentage values (e.g. 19.8),
        # so divide by 100 to normalise to fractions (e.g. 0.198) for formatting.
        recent_campaign = {
            "name": campaign["name"],
            "sent_date": campaign["send_date"],
            "open_rate": round(campaign["open_rate"] / 100.0, 4),
            "click_rate": round(campaign["click_rate"] / 100.0, 4),
            "conversions": campaign["conversion_count"],
            "roi_estimate": roi_estimate,
        }
    else:
        recent_campaign = {
            "name": None,
            "sent_date": None,
            "open_rate": 0.0,
            "click_rate": 0.0,
            "conversions": 0,
            "roi_estimate": None,
        }

    # ------------------------------------------------------------------ #
    # Audience health
    # ------------------------------------------------------------------ #
    # Total unique contacts who have received at least one campaign
    total_subscribers = db.execute(
        """
        SELECT COUNT(DISTINCT COALESCE(client_id, lead_id)) AS cnt
        FROM marketing_interactions
        WHERE interaction_type = 'open'
        """,
    ).fetchone()["cnt"]

    # New subscribers in last 30 days: contacts whose first interaction is in window
    new_subscribers_30day = db.execute(
        """
        SELECT COUNT(DISTINCT COALESCE(mi.client_id, mi.lead_id)) AS cnt
        FROM marketing_interactions mi
        WHERE mi.interaction_date >= %s
          AND interaction_type = 'open'
          AND NOT EXISTS (
              SELECT 1 FROM marketing_interactions mi2
              WHERE COALESCE(mi2.client_id, mi2.lead_id) = COALESCE(mi.client_id, mi.lead_id)
                AND mi2.interaction_date < %s
          )
        """,
        (str(thirty_days_ago), str(thirty_days_ago)),
    ).fetchone()["cnt"]

    # Unsubscribe rate is not tracked in the current schema; default to 0.0
    unsubscribe_rate_30day = 0.0

    audience_health = {
        "total_subscribers": total_subscribers,
        "unsubscribe_rate_30day": unsubscribe_rate_30day,
        "new_subscribers_30day": new_subscribers_30day,
    }

    # ------------------------------------------------------------------ #
    # Lead source performance (last 30 days)
    #
    # avg_ltv is computed from clients.acquisition_source (all-time) because
    # converted leads are removed from the leads table — joining leads to
    # clients by email would never match a converted record.
    # ------------------------------------------------------------------ #
    source_rows = db.execute(
        """
        SELECT source,
               COUNT(id) AS leads_30day,
               SUM(CASE WHEN status = 'qualified' THEN 1 ELSE 0 END) AS converted
        FROM leads
        WHERE created_at >= %s
        GROUP BY source
        """,
        (str(thirty_days_ago),),
    ).fetchall()

    ltv_rows = db.execute(
        """
        SELECT acquisition_source AS source,
               AVG(lifetime_value) AS avg_ltv
        FROM clients
        WHERE acquisition_source IS NOT NULL
        GROUP BY acquisition_source
        """,
    ).fetchall()
    ltv_by_source = {
        r["source"]: round(r["avg_ltv"], 2) if r["avg_ltv"] is not None else 0.0
        for r in ltv_rows
    }

    # Build from recent-lead sources first, then fill in LTV for any source
    # that has clients but no recent leads (e.g. past campaigns).
    lead_source_performance: dict = {}
    for row in source_rows:
        src = row["source"] or "unknown"
        lead_source_performance[src] = {
            "leads_30day": row["leads_30day"],
            "converted": row["converted"],
            "avg_ltv": ltv_by_source.get(src, 0.0),
        }
    for src, ltv in ltv_by_source.items():
        if src not in lead_source_performance:
            lead_source_performance[src] = {
                "leads_30day": 0,
                "converted": 0,
                "avg_ltv": ltv,
            }

    # ------------------------------------------------------------------ #
    # 7-day review summary
    # ------------------------------------------------------------------ #
    review_rows = db.execute(
        """
        SELECT r.rating, r.platform, r.review_text,
               c.first_name || ' ' || COALESCE(c.last_name, '') AS client_name
        FROM reviews r
        JOIN clients c ON r.client_id = c.id
        WHERE r.review_date BETWEEN %s AND %s
        """,
        (str(seven_days_ago), str(today)),
    ).fetchall()

    total_reviews = len(review_rows)
    ratings = [r["rating"] for r in review_rows]
    avg_rating = round(sum(ratings) / len(ratings), 2) if ratings else 0.0
    five_star_count = sum(1 for r in ratings if r == 5)
    negative_count = sum(1 for r in ratings if r <= review_alert_threshold)

    negative_details = []
    for row in review_rows:
        if row["rating"] <= review_alert_threshold:
            excerpt = (row["review_text"] or "")[:120]
            negative_details.append({
                "rating": row["rating"],
                "platform": row["platform"],
                "excerpt": excerpt,
                "client_name": row["client_name"].strip(),
            })

    review_summary_7day = {
        "total_reviews": total_reviews,
        "avg_rating": avg_rating,
        "five_star_count": five_star_count,
        "negative_count": negative_count,
        "negative_details": negative_details,
    }

    # ------------------------------------------------------------------ #
    # Alerts
    # ------------------------------------------------------------------ #
    alerts = []

    if negative_count > 0:
        alerts.append(
            f"{negative_count} negative review(s) (≤{review_alert_threshold} stars) "
            f"in the last 7 days — review and respond promptly"
        )

    if campaign and campaign["open_rate"] < 0.15:
        alerts.append(
            f"Last campaign open rate was {campaign['open_rate']*100:.1f}% "
            f"(industry benchmark ~20%) — consider subject line testing"
        )

    return {
        "recent_campaign": recent_campaign,
        "audience_health": audience_health,
        "lead_source_performance": lead_source_performance,
        "review_summary_7day": review_summary_7day,
        "alerts": alerts,
    }
