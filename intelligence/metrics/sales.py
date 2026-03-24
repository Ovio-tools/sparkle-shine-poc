"""
intelligence/metrics/sales.py

Sales pipeline metrics drawn from leads + commercial_proposals tables.

Pipeline stage mapping (DB values → display names):
  leads.status='new'          → "New Lead"
  leads.status='contacted'    → "Site Visit Scheduled"
  leads.status='qualified'    → "Qualified"
  proposals.status='sent'     → "Proposal Sent"
  proposals.status='negotiating' → "Negotiation"
"""

from datetime import date, timedelta

from intelligence.config import ALERT_THRESHOLDS


def compute(db, briefing_date: str) -> dict:
    today = date.fromisoformat(briefing_date)
    yesterday = today - timedelta(days=1)

    stale_days = ALERT_THRESHOLDS["stale_deal_days"]
    stale_cutoff = today - timedelta(days=stale_days)

    # ------------------------------------------------------------------ #
    # Pipeline summary
    # ------------------------------------------------------------------ #

    # Leads-based stages
    lead_stage_rows = db.execute(
        """
        SELECT status, COUNT(*) AS cnt, COALESCE(SUM(estimated_value), 0.0) AS val
        FROM leads
        WHERE status IN ('new', 'contacted', 'qualified')
        GROUP BY status
        """,
    ).fetchall()

    lead_stage_map = {r["status"]: {"count": r["cnt"], "value": r["val"]} for r in lead_stage_rows}

    # Proposal-based stages (open only = sent or negotiating)
    prop_stage_rows = db.execute(
        """
        SELECT status, COUNT(*) AS cnt,
               COALESCE(SUM(COALESCE(monthly_value, 0.0) * 12), 0.0) AS annual_val
        FROM commercial_proposals
        WHERE status IN ('sent', 'negotiating')
        GROUP BY status
        """,
    ).fetchall()

    prop_stage_map = {r["status"]: {"count": r["cnt"], "value": r["annual_val"]} for r in prop_stage_rows}

    by_stage = {
        "New Lead": lead_stage_map.get("new", {"count": 0, "value": 0.0}),
        "Site Visit Scheduled": lead_stage_map.get("contacted", {"count": 0, "value": 0.0}),
        "Qualified": lead_stage_map.get("qualified", {"count": 0, "value": 0.0}),
        "Proposal Sent": prop_stage_map.get("sent", {"count": 0, "value": 0.0}),
        "Negotiation": prop_stage_map.get("negotiating", {"count": 0, "value": 0.0}),
    }

    total_open_deals = sum(s["count"] for s in by_stage.values())
    total_pipeline_value = sum(s["value"] for s in by_stage.values())

    # ------------------------------------------------------------------ #
    # Movement yesterday
    # ------------------------------------------------------------------ #
    new_leads = db.execute(
        "SELECT COUNT(*) FROM leads WHERE date(created_at) = ?",
        (str(yesterday),),
    ).fetchone()[0]

    stage_advances = db.execute(
        """
        SELECT COUNT(*) FROM leads
        WHERE last_activity_at IS NOT NULL
          AND date(last_activity_at) = ?
          AND status NOT IN ('new', 'lost')
        """,
        (str(yesterday),),
    ).fetchone()[0]

    won_row = db.execute(
        """
        SELECT COUNT(*) AS cnt,
               COALESCE(SUM(COALESCE(monthly_value, 0.0) * 12), 0.0) AS val
        FROM commercial_proposals
        WHERE status = 'won' AND date(decision_date) = ?
        """,
        (str(yesterday),),
    ).fetchone()

    lost_row = db.execute(
        """
        SELECT COUNT(*) AS cnt,
               COALESCE(SUM(COALESCE(monthly_value, 0.0) * 12), 0.0) AS val
        FROM commercial_proposals
        WHERE status = 'lost' AND date(decision_date) = ?
        """,
        (str(yesterday),),
    ).fetchone()

    # ------------------------------------------------------------------ #
    # Stale deals (proposals open with no movement in 14+ days)
    # ------------------------------------------------------------------ #
    stale_rows = db.execute(
        """
        SELECT cp.title, cp.status,
               COALESCE(cp.monthly_value * 12, 0.0) AS annual_value,
               CAST(julianday('now') - julianday(COALESCE(cp.sent_date, cp.id)) AS INTEGER) AS days_stale
        FROM commercial_proposals cp
        WHERE cp.status IN ('sent', 'negotiating')
          AND (cp.sent_date IS NULL OR cp.sent_date < ?)
          AND cp.decision_date IS NULL
        ORDER BY days_stale DESC
        """,
        (str(stale_cutoff),),
    ).fetchall()

    stale_deals = [
        {
            "deal_title": r["title"],
            "stage": "Proposal Sent" if r["status"] == "sent" else "Negotiation",
            "days_stale": r["days_stale"] or stale_days,
            "value": round(r["annual_value"], 2),
        }
        for r in stale_rows
    ]

    # ------------------------------------------------------------------ #
    # Proposals needing a nudge (in "Proposal Sent" for 7–13 days)
    # These are proposals that are not yet "stale" (14+ days) but have
    # been sitting without a response long enough to warrant a follow-up.
    # ------------------------------------------------------------------ #
    nudge_days = ALERT_THRESHOLDS["stale_proposal_days"]
    nudge_cutoff_near = today - timedelta(days=stale_days)    # 14 days ago (upper bound)
    nudge_cutoff_far  = today - timedelta(days=nudge_days)    # 7 days ago (lower bound)

    nudge_rows = db.execute(
        """
        SELECT cp.title,
               COALESCE(cp.monthly_value * 12, 0.0) AS annual_value,
               CAST(julianday('now') - julianday(cp.sent_date) AS INTEGER) AS days_stale
        FROM commercial_proposals cp
        WHERE cp.status = 'sent'
          AND cp.decision_date IS NULL
          AND cp.sent_date IS NOT NULL
          AND cp.sent_date BETWEEN ? AND ?
        ORDER BY annual_value DESC
        """,
        (str(nudge_cutoff_near), str(nudge_cutoff_far)),
    ).fetchall()

    proposals_needing_nudge = [
        {
            "deal_title": r["title"],
            "days_stale": r["days_stale"] or nudge_days,
            "value": round(r["annual_value"], 2),
        }
        for r in nudge_rows
    ]

    # ------------------------------------------------------------------ #
    # Conversion rate by lead source
    #
    # Total leads per source = unconverted leads (leads table) + converted
    # leads that became clients (clients table by acquisition_source).
    # Won count = clients from that source (all statuses — churned clients
    # still converted at point of sale).
    # Commercial proposals with lead_id = NULL are counted via their linked
    # client's acquisition_source.
    # ------------------------------------------------------------------ #
    conversion_rows = db.execute(
        """
        SELECT source, SUM(total) AS total_leads, SUM(won) AS won_count
        FROM (
            -- Unconverted leads still in the funnel
            SELECT source, COUNT(*) AS total, 0 AS won
            FROM leads
            GROUP BY source

            UNION ALL

            -- Converted leads that became clients (residential + commercial)
            SELECT acquisition_source AS source,
                   COUNT(*) AS total,
                   COUNT(*) AS won
            FROM clients
            WHERE acquisition_source IS NOT NULL
            GROUP BY acquisition_source
        )
        GROUP BY source
        """,
    ).fetchall()

    conversion_by_source = {}
    for row in conversion_rows:
        src = row["source"] or "unknown"
        leads_count = row["total_leads"] or 0
        won_count   = row["won_count"]   or 0
        rate = round(won_count / leads_count, 3) if leads_count > 0 else 0.0
        conversion_by_source[src] = {
            "leads": leads_count,
            "won":   won_count,
            "rate":  rate,
        }

    # ------------------------------------------------------------------ #
    # Average sales cycle (days from sent_date to decision_date for won deals)
    # ------------------------------------------------------------------ #
    cycle_row = db.execute(
        """
        SELECT AVG(julianday(decision_date) - julianday(sent_date))
        FROM commercial_proposals
        WHERE status = 'won'
          AND sent_date IS NOT NULL
          AND decision_date IS NOT NULL
        """,
    ).fetchone()[0]
    avg_cycle_length_days = round(cycle_row, 1) if cycle_row is not None else 0.0

    # ------------------------------------------------------------------ #
    # Alerts
    # ------------------------------------------------------------------ #
    alerts = []

    if stale_deals:
        negotiation_stale = [d for d in stale_deals if d["stage"] == "Negotiation"]
        if negotiation_stale:
            day_list = ", ".join(str(d["days_stale"]) for d in negotiation_stale)
            alerts.append(
                f"{len(negotiation_stale)} deal(s) in Negotiation stage gone stale "
                f"({day_list} days)"
            )
        if len(stale_deals) > len(negotiation_stale):
            other_count = len(stale_deals) - len(negotiation_stale)
            alerts.append(
                f"{other_count} open proposal(s) with no movement in {stale_days}+ days"
            )

    if won_row["cnt"] > 0:
        alerts.append(
            f"Deal won yesterday: ${won_row['val']:,.0f} annual value"
        )

    if lost_row["cnt"] > 0:
        alerts.append(
            f"{lost_row['cnt']} deal(s) lost yesterday (${lost_row['val']:,.0f} annual value)"
        )

    return {
        "pipeline_summary": {
            "total_open_deals": total_open_deals,
            "total_pipeline_value": round(total_pipeline_value, 2),
            "by_stage": by_stage,
        },
        "movement_yesterday": {
            "new_leads": new_leads,
            "stage_advances": stage_advances,
            "deals_won": won_row["cnt"],
            "deals_lost": lost_row["cnt"],
            "won_value": round(won_row["val"], 2),
            "lost_value": round(lost_row["val"], 2),
        },
        "stale_deals": stale_deals,
        "proposals_needing_nudge": proposals_needing_nudge,
        "conversion_by_source": conversion_by_source,
        "avg_cycle_length_days": avg_cycle_length_days,
        "alerts": alerts,
    }
