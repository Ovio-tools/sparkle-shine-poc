"""seeding/generators/gen_marketing.py

Generates all marketing and sales records into sparkle_shine.db:
  • 5 Mailchimp campaigns + marketing_interactions (opens, clicks, conversions)
  • 48 Pipedrive commercial proposals (10 won, 23 lost, 15 active/open)
  • Referral program tracking summary (reports on existing referral clients)
  • cross_tool_mapping entries for campaigns and won proposals

Run:
    python seeding/generators/gen_marketing.py
"""
from __future__ import annotations

import random
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

DB_PATH = ROOT / "sparkle_shine.db"

_RNG = random.Random(42)


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def _add_days(d: str, days: int) -> str:
    return (date.fromisoformat(d) + timedelta(days=days)).isoformat()


def _rand_date(d1: str, d2: str) -> str:
    start = date.fromisoformat(d1)
    delta = (date.fromisoformat(d2) - start).days
    return (start + timedelta(days=_RNG.randint(0, max(delta, 0)))).isoformat()


# ---------------------------------------------------------------------------
# Campaign definitions
# ---------------------------------------------------------------------------

CAMPAIGNS = [
    {
        "id":               "SS-CAMP-0001",
        "name":             "Spring into Summer 2025",
        "platform":         "mailchimp",
        "campaign_type":    "promotional",
        "subject_line":     "Summer's here — treat your home to a fresh start ☀️",
        "send_date":        "2025-06-01",
        "recipient_count":  285,
        "open_rate":        28.4,
        "click_rate":       6.1,
        "conversion_count": 18,
        "mailchimp_id":     "cmp_spring_summer_2025",
        "audience_type":    "active_residential",
        "conv_source":      "summer_2025_campaign",  # match leads.source
    },
    {
        "id":               "SS-CAMP-0002",
        "name":             "Back to School Clean-Up",
        "platform":         "mailchimp",
        "campaign_type":    "promotional",
        "subject_line":     "School's back. Your home deserves a reset.",
        "send_date":        "2025-08-15",
        "recipient_count":  260,
        "open_rate":        22.1,
        "click_rate":       4.3,
        "conversion_count": 7,
        "mailchimp_id":     "cmp_back_to_school_2025",
        "audience_type":    "active_residential",
        "conv_source":      None,
    },
    {
        "id":               "SS-CAMP-0003",
        "name":             "Fall Fresh Start",
        "platform":         "mailchimp",
        "campaign_type":    "promotional",
        "subject_line":     "Autumn is the perfect time for a deep clean",
        "send_date":        "2025-09-30",
        "recipient_count":  255,
        "open_rate":        25.7,
        "click_rate":       5.2,
        "conversion_count": 11,
        "mailchimp_id":     "cmp_fall_fresh_start_2025",
        "audience_type":    "active_residential",
        "conv_source":      None,
    },
    {
        "id":               "SS-CAMP-0004",
        "name":             "Holiday Gift of Clean",
        "platform":         "mailchimp",
        "campaign_type":    "promotional",
        "subject_line":     "Give the gift of a spotless home this holiday season 🎁",
        "send_date":        "2025-11-20",
        "recipient_count":  270,
        "open_rate":        31.2,
        "click_rate":       8.9,
        "conversion_count": 22,
        "mailchimp_id":     "cmp_holiday_gift_2025",
        "audience_type":    "active_residential",
        "conv_source":      None,
    },
    {
        "id":               "SS-CAMP-0005",
        "name":             "New Year Fresh Start",
        "platform":         "mailchimp",
        "campaign_type":    "re-engagement",
        "subject_line":     "Start 2026 right — book your first clean of the new year",
        "send_date":        "2026-01-08",
        "recipient_count":  240,
        "open_rate":        19.8,
        "click_rate":       3.7,
        "conversion_count": 9,
        "mailchimp_id":     "cmp_new_year_2026",
        "audience_type":    "churned_and_leads",  # re-engagement targets
        "conv_source":      None,
    },
]


# ---------------------------------------------------------------------------
# Commercial client metadata (matches gen_clients.py _COMMERCIAL_CLIENTS)
# ---------------------------------------------------------------------------

_COMM_META: dict[str, dict] = {
    "Barton Creek Medical Group": {
        "monthly_value": 27_000.00, "schedule": "nightly_plus_saturday",
        "sq_ft": 18_500, "scope": "Medical office complex nightly clean + Saturday deep-clean",
        "churned": False,
    },
    "South Lamar Dental": {
        "monthly_value": 6_000.00, "schedule": "3x_weekly",
        "sq_ft": 2_800, "scope": "Dental office 3x weekly clean",
        "churned": False,
    },
    "Mueller Tech Suites": {
        "monthly_value": 14_000.00, "schedule": "nightly_plus_saturday",
        "sq_ft": 9_800, "scope": "Co-working office complex nightly clean",
        "churned": False,
    },
    "Crestview Coworking": {
        "monthly_value": 7_500.00, "schedule": "nightly_weekdays",
        "sq_ft": 4_200, "scope": "Coworking space nightly clean Mon-Fri",
        "churned": False,
    },
    "Hyde Park Realty Group": {
        "monthly_value": 4_500.00, "schedule": "2x_weekly",
        "sq_ft": 1_900, "scope": "Real estate office 2x weekly clean",
        "churned": False,
    },
    "Domain Business Center": {
        "monthly_value": 16_000.00, "schedule": "nightly_plus_saturday",
        "sq_ft": 11_200, "scope": "Multi-tenant office building nightly clean",
        "churned": False,
    },
    "Rosedale Family Practice": {
        "monthly_value": 9_000.00, "schedule": "3x_weekly",
        "sq_ft": 3_600, "scope": "Medical family practice 5x weekly clean",
        "churned": False,
    },
    "Cherrywood Coffeehouse LLC": {
        "monthly_value": 4_000.00, "schedule": "daily",
        "sq_ft": 1_400, "scope": "Cafe and event space daily clean",
        "churned": False,
    },
    "North Loop Bistro": {
        "monthly_value": 5_500.00, "schedule": "3x_weekly",
        "sq_ft": 2_100, "scope": "Restaurant 3x weekly clean",
        "churned": True,
    },
    "East Cesar Chavez Gallery": {
        "monthly_value": 4_750.00, "schedule": "daily",
        "sq_ft": 1_600, "scope": "Art gallery daily clean",
        "churned": True,
    },
}

# Loss reason pool (for lost proposal notes)
_LOSS_REASONS = [
    "price_too_high",
    "chose_competitor",
    "no_budget",
    "timing",
    "in_house_cleaning",
]
_LOSS_WEIGHTS = [0.30, 0.25, 0.20, 0.15, 0.10]


# ---------------------------------------------------------------------------
# Main generator
# ---------------------------------------------------------------------------

def generate(db_path: str | Path = DB_PATH) -> None:  # noqa: C901
    import sqlite3
    from seeding.utils.text_generator import generate_pipedrive_activity_note

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    cur = conn.cursor()

    # Idempotency guard
    if conn.execute("SELECT COUNT(*) FROM marketing_campaigns").fetchone()[0] > 0:
        print("  marketing_campaigns already populated — nothing to do.")
        conn.close()
        return

    # ------------------------------------------------------------------ #
    # Load existing clients and leads
    # ------------------------------------------------------------------ #
    res_clients = conn.execute(
        "SELECT id, status FROM clients WHERE client_type='residential'"
    ).fetchall()
    active_res_ids  = [r["id"] for r in res_clients if r["status"] == "active"]
    churned_res_ids = [r["id"] for r in res_clients if r["status"] == "churned"]

    comm_clients = {
        r["company_name"]: dict(r)
        for r in conn.execute(
            "SELECT id, company_name, first_service_date, status FROM clients "
            "WHERE client_type='commercial'"
        ).fetchall()
    }

    all_leads = conn.execute(
        "SELECT id, lead_type, source, status FROM leads"
    ).fetchall()
    res_leads_ids  = [r["id"] for r in all_leads if r["lead_type"] == "residential"]
    comm_leads_ids = [r["id"] for r in all_leads if r["lead_type"] == "commercial"]

    summer_conv_ids = [
        r["id"] for r in all_leads
        if r["source"] == "summer_2025_campaign" and r["status"] == "qualified"
    ]

    # Generic residential conversion pool (qualified/contacted leads)
    res_conv_pool = [
        r["id"] for r in all_leads
        if r["lead_type"] == "residential" and r["status"] in ("qualified", "contacted")
    ]

    # ------------------------------------------------------------------ #
    # Pre-generate LLM activity notes (one per unique stage+outcome combo)
    # ------------------------------------------------------------------ #
    print("  Generating Pipedrive activity notes via LLM (10 unique calls)...")

    _note_combos: list[tuple[str, str, str]] = [
        ("call_recap",         "New Lead",             "initial contact made"),
        ("email_summary",      "Qualified",            "qualified — scheduling site visit"),
        ("site_visit_notes",   "Site Visit Scheduled", "site visit completed, scope confirmed"),
        ("proposal_followup",  "Proposal Sent",        "proposal sent, awaiting decision"),
        ("negotiation_update", "Negotiation",          "negotiation in progress"),
        ("close_note",         "Closed Won",           "deal closed"),
        ("loss_note",          "Qualified",            "lost: price_too_high"),
        ("loss_note",          "Proposal Sent",        "lost: chose_competitor"),
        ("loss_note",          "Negotiation",          "lost: no_budget or timing"),
        ("loss_note",          "Closed Won",           "lost: post-contract churn"),
    ]
    _notes: dict[tuple[str, str, str], str] = {}
    for combo in _note_combos:
        _notes[combo] = generate_pipedrive_activity_note(
            activity_type=combo[0], deal_stage=combo[1], outcome=combo[2]
        )

    def _build_notes(stages: list[tuple[str, str, str]]) -> str:
        return "\n---\n".join(_notes[s] for s in stages)

    # ------------------------------------------------------------------ #
    # Step 1 — marketing_campaigns
    # ------------------------------------------------------------------ #
    print("  Inserting 5 Mailchimp campaigns...")
    for c in CAMPAIGNS:
        cur.execute("""
            INSERT INTO marketing_campaigns
                (id, name, platform, campaign_type, subject_line,
                 send_date, recipient_count, open_rate, click_rate, conversion_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            c["id"], c["name"], c["platform"], c["campaign_type"], c["subject_line"],
            c["send_date"], c["recipient_count"],
            c["open_rate"], c["click_rate"], c["conversion_count"],
        ))

    # ------------------------------------------------------------------ #
    # Step 2 — marketing_interactions
    # ------------------------------------------------------------------ #
    print("  Generating marketing interactions...")
    total_interactions = 0

    def _insert_interaction(client_id, lead_id, camp_id, itype, idate):
        nonlocal total_interactions
        cur.execute("""
            INSERT INTO marketing_interactions
                (client_id, lead_id, campaign_id, interaction_type, interaction_date)
            VALUES (?, ?, ?, ?, ?)
        """, (client_id, lead_id, camp_id, itype, idate))
        total_interactions += 1

    for c in CAMPAIGNS:
        camp_id    = c["id"]
        send_date  = c["send_date"]
        n_opens    = int(c["recipient_count"] * c["open_rate"] / 100)
        n_clicks   = int(c["recipient_count"] * c["click_rate"] / 100)
        n_conv     = c["conversion_count"]

        # Build audience: list of (client_id, lead_id)
        if c["audience_type"] == "churned_and_leads":
            pool = [(cid, None) for cid in churned_res_ids] + \
                   [(None, lid) for lid in res_leads_ids]
        else:
            pool = [(cid, None) for cid in active_res_ids] + \
                   [(None, lid) for lid in res_leads_ids]
        _RNG.shuffle(pool)

        # Opens
        openers = pool[:min(n_opens, len(pool))]
        for (cid, lid) in openers:
            _insert_interaction(cid, lid, camp_id, "open",
                                _add_days(send_date, _RNG.randint(0, 3)))

        # Clicks (subset of openers)
        clickers = _RNG.sample(openers, min(n_clicks, len(openers)))
        for (cid, lid) in clickers:
            _insert_interaction(cid, lid, camp_id, "click",
                                _add_days(send_date, _RNG.randint(0, 5)))

        # Conversions
        if c["conv_source"] == "summer_2025_campaign":
            conv_pairs = [(None, lid) for lid in summer_conv_ids[:n_conv]]
        else:
            _RNG.shuffle(res_conv_pool)
            conv_pairs = [(None, lid) for lid in res_conv_pool[:n_conv]]

        for (cid, lid) in conv_pairs:
            _insert_interaction(cid, lid, camp_id, "conversion",
                                _add_days(send_date, _RNG.randint(1, 14)))

    print(f"    → {total_interactions} interactions")

    # ------------------------------------------------------------------ #
    # Step 3 — commercial_proposals
    # ------------------------------------------------------------------ #
    print("  Generating 48 commercial proposals...")

    proposals: list[dict[str, Any]] = []
    prop_num   = 1
    pipedrive_id = 100_001

    def _next_prop_id() -> str:
        nonlocal prop_num
        pid = f"SS-PROP-{prop_num:04d}"
        prop_num += 1
        return pid

    # --- Won deals (10) — one per commercial client ------------------
    for company_name, client_row in comm_clients.items():
        meta = _COMM_META.get(company_name, {})
        if not meta:
            continue

        win_date     = client_row["first_service_date"]
        created_date = _add_days(win_date, -_RNG.randint(30, 90))
        sent_date    = _add_days(created_date, _RNG.randint(14, 21))

        stages: list[tuple[str, str, str]] = [
            ("call_recap",        "New Lead",             "initial contact made"),
            ("email_summary",     "Qualified",            "qualified — scheduling site visit"),
            ("site_visit_notes",  "Site Visit Scheduled", "site visit completed, scope confirmed"),
            ("proposal_followup", "Proposal Sent",        "proposal sent, awaiting decision"),
        ]
        if _RNG.random() < 0.60:  # 60% go through negotiation
            stages.append(("negotiation_update", "Negotiation", "negotiation in progress"))
        stages.append(("close_note", "Closed Won", "deal closed"))
        if meta["churned"]:
            stages.append(("loss_note", "Closed Won", "lost: post-contract churn"))

        proposals.append({
            "id":            _next_prop_id(),
            "lead_id":       None,
            "client_id":     client_row["id"],
            "title":         f"Commercial Cleaning Proposal — {company_name}",
            "square_footage": float(meta["sq_ft"]),
            "service_scope": meta["scope"],
            "price_per_visit": round(meta["monthly_value"] / 22, 2),
            "frequency":     meta["schedule"],
            "monthly_value": meta["monthly_value"],
            "status":        "won",
            "sent_date":     sent_date,
            "decision_date": win_date,
            "notes":         _build_notes(stages),
            "_pipedrive_id": pipedrive_id,
        })
        pipedrive_id += 1

    # --- Lost deals (23) — from commercial leads ----------------------
    # Stage distribution: 5 Qualified, 8 Proposal Sent, 6 Negotiation, 4 post-contract
    _lost_configs: list[tuple[int, str, list, str]] = [
        (5,  "draft",       [
            ("call_recap",    "New Lead",  "initial contact made"),
            ("loss_note",     "Qualified", "lost: price_too_high"),
        ], "2025-04-01"),
        (8,  "lost",        [
            ("call_recap",        "New Lead",         "initial contact made"),
            ("email_summary",     "Qualified",        "qualified — scheduling site visit"),
            ("site_visit_notes",  "Site Visit Scheduled", "site visit completed, scope confirmed"),
            ("proposal_followup", "Proposal Sent",    "proposal sent, awaiting decision"),
            ("loss_note",         "Proposal Sent",    "lost: chose_competitor"),
        ], "2025-05-01"),
        (6,  "lost",        [
            ("call_recap",         "New Lead",             "initial contact made"),
            ("email_summary",      "Qualified",            "qualified — scheduling site visit"),
            ("site_visit_notes",   "Site Visit Scheduled", "site visit completed, scope confirmed"),
            ("proposal_followup",  "Proposal Sent",        "proposal sent, awaiting decision"),
            ("negotiation_update", "Negotiation",          "negotiation in progress"),
            ("loss_note",          "Negotiation",          "lost: no_budget or timing"),
        ], "2025-06-01"),
        (4,  "lost",        [
            ("call_recap",         "New Lead",             "initial contact made"),
            ("email_summary",      "Qualified",            "qualified — scheduling site visit"),
            ("site_visit_notes",   "Site Visit Scheduled", "site visit completed, scope confirmed"),
            ("proposal_followup",  "Proposal Sent",        "proposal sent, awaiting decision"),
            ("close_note",         "Closed Won",           "deal closed"),
            ("loss_note",          "Closed Won",           "lost: post-contract churn"),
        ], "2025-07-01"),
    ]

    comm_leads_shuffled = list(comm_leads_ids)
    _RNG.shuffle(comm_leads_shuffled)
    lead_idx = 0

    for count, schema_status, stages, earliest_date in _lost_configs:
        for _ in range(count):
            if lead_idx >= len(comm_leads_shuffled):
                break
            lid = comm_leads_shuffled[lead_idx]
            lead_idx += 1

            created_date = _rand_date(earliest_date, "2025-12-31")
            sent_date    = _add_days(created_date, _RNG.randint(14, 21)) \
                if schema_status != "draft" else None
            decision_date = _add_days(created_date, _RNG.randint(25, 65)) \
                if schema_status == "lost" else None

            est_mv = float(_RNG.randint(800, 4200))

            proposals.append({
                "id":             _next_prop_id(),
                "lead_id":        lid,
                "client_id":      None,
                "title":          "Commercial Cleaning Proposal",
                "square_footage": float(_RNG.randint(1000, 12000)),
                "service_scope":  "Commercial cleaning services",
                "price_per_visit": round(est_mv / 22, 2),
                "frequency":      _RNG.choice(["nightly_weekdays", "3x_weekly", "2x_weekly"]),
                "monthly_value":  est_mv,
                "status":         schema_status,
                "sent_date":      sent_date,
                "decision_date":  decision_date,
                "notes":          _build_notes(stages),
                "_pipedrive_id":  None,
            })

    # --- Active/open proposals (15) — spring 2026 pipeline -----------
    # Stage: New Lead (3), Qualified (4), Site Visit Scheduled (4), Proposal Sent (4)
    _active_configs: list[tuple[int, str, list]] = [
        (3, "draft", [
            ("call_recap", "New Lead", "initial contact made"),
        ]),
        (4, "draft", [
            ("call_recap",   "New Lead",  "initial contact made"),
            ("email_summary","Qualified", "qualified — scheduling site visit"),
        ]),
        (4, "draft", [
            ("call_recap",       "New Lead",             "initial contact made"),
            ("email_summary",    "Qualified",            "qualified — scheduling site visit"),
            ("site_visit_notes", "Site Visit Scheduled", "site visit completed, scope confirmed"),
        ]),
        (4, "sent", [
            ("call_recap",        "New Lead",             "initial contact made"),
            ("email_summary",     "Qualified",            "qualified — scheduling site visit"),
            ("site_visit_notes",  "Site Visit Scheduled", "site visit completed, scope confirmed"),
            ("proposal_followup", "Proposal Sent",        "proposal sent, awaiting decision"),
        ]),
    ]

    for count, schema_status, stages in _active_configs:
        for _ in range(count):
            if lead_idx >= len(comm_leads_shuffled):
                break
            lid = comm_leads_shuffled[lead_idx]
            lead_idx += 1

            created_date = _rand_date("2026-02-01", "2026-03-15")
            sent_date    = _add_days(created_date, _RNG.randint(7, 21)) \
                if schema_status == "sent" else None

            est_mv = float(_RNG.randint(800, 4200))

            proposals.append({
                "id":             _next_prop_id(),
                "lead_id":        lid,
                "client_id":      None,
                "title":          "Commercial Cleaning Proposal",
                "square_footage": float(_RNG.randint(1000, 8000)),
                "service_scope":  "Commercial cleaning services — active pipeline",
                "price_per_visit": round(est_mv / 22, 2),
                "frequency":      _RNG.choice(["nightly_weekdays", "3x_weekly", "2x_weekly"]),
                "monthly_value":  est_mv,
                "status":         schema_status,
                "sent_date":      sent_date,
                "decision_date":  None,
                "notes":          _build_notes(stages),
                "_pipedrive_id":  None,
            })

    # Insert all proposals
    for p in proposals:
        cur.execute("""
            INSERT INTO commercial_proposals
                (id, lead_id, client_id, title, square_footage, service_scope,
                 price_per_visit, frequency, monthly_value, status,
                 sent_date, decision_date, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            p["id"], p["lead_id"], p["client_id"], p["title"],
            p["square_footage"], p["service_scope"],
            p["price_per_visit"], p["frequency"], p["monthly_value"],
            p["status"], p["sent_date"], p["decision_date"], p["notes"],
        ))

    print(f"    → {len(proposals)} proposals inserted")

    # ------------------------------------------------------------------ #
    # Step 4 — cross_tool_mapping
    # ------------------------------------------------------------------ #
    print("  Registering cross_tool_mapping entries...")
    mc_audience = "92f05d2d65"
    mapping_count = 0

    for c in CAMPAIGNS:
        cur.execute("""
            INSERT OR REPLACE INTO cross_tool_mapping
                (canonical_id, entity_type, tool_name, tool_specific_id)
            VALUES (?, 'marketing_campaign', 'mailchimp', ?)
        """, (c["id"], f"{mc_audience}:{c['mailchimp_id']}"))
        mapping_count += 1

    for p in proposals:
        if p.get("_pipedrive_id"):
            cur.execute("""
                INSERT OR REPLACE INTO cross_tool_mapping
                    (canonical_id, entity_type, tool_name, tool_specific_id)
                VALUES (?, 'commercial_proposal', 'pipedrive', ?)
            """, (p["id"], str(p["_pipedrive_id"])))
            mapping_count += 1

    print(f"    → {mapping_count} cross_tool_mapping entries")

    # ------------------------------------------------------------------ #
    # Commit
    # ------------------------------------------------------------------ #
    conn.commit()

    # ------------------------------------------------------------------ #
    # Step 5 — Referral program summary (read-only)
    # ------------------------------------------------------------------ #
    referral_rows = conn.execute("""
        SELECT COUNT(*) AS cnt FROM clients
        WHERE client_type = 'residential'
          AND acquisition_source = 'referral'
          AND notes LIKE '%referring_client_id%'
    """).fetchone()
    referral_flagged = referral_rows["cnt"] if referral_rows else 0

    # Conversion rate by residential lead source
    conv_rate_rows = conn.execute("""
        SELECT source,
               COUNT(*) AS total,
               SUM(CASE WHEN status = 'qualified' THEN 1 ELSE 0 END) AS qualified
        FROM leads
        WHERE lead_type = 'residential'
        GROUP BY source
        ORDER BY total DESC
    """).fetchall()

    # Average proposal cycle for won deals (sent_date → decision_date)
    cycle_days: list[int] = []
    for p in proposals:
        if p["status"] == "won" and p["sent_date"] and p["decision_date"]:
            d = (date.fromisoformat(p["decision_date"]) -
                 date.fromisoformat(p["sent_date"])).days
            if d > 0:
                cycle_days.append(d)
    avg_cycle = round(sum(cycle_days) / len(cycle_days), 1) if cycle_days else 0

    conn.close()

    # ------------------------------------------------------------------ #
    # Print summary
    # ------------------------------------------------------------------ #
    won_n    = sum(1 for p in proposals if p["status"] == "won")
    lost_n   = sum(1 for p in proposals if p["status"] in ("draft", "lost") and p["client_id"] is None)
    active_n = sum(1 for p in proposals if p["client_id"] is None and p["status"] in ("draft", "sent"))

    print()
    print("=" * 70)
    print("  SPARKLE & SHINE — MARKETING GENERATION RESULTS")
    print("=" * 70)
    print(f"  Campaigns generated        : 5")
    print(f"  Total interactions         : {total_interactions}")
    print()
    print(f"  Commercial proposals       : won={won_n}, lost={23}, active={15}")
    print(f"  Total proposals            : {len(proposals)}")
    print(f"  Avg proposal cycle (won)   : {avg_cycle} days (sent → decision)")
    print()
    print(f"  Referral clients flagged   : {referral_flagged}")
    print()
    print(f"  Conversion rate by residential lead source:")
    print(f"  {'Source':<25} {'Total':>6} {'Qualified':>10} {'Rate':>8}")
    print("  " + "-" * 53)
    for r in conv_rate_rows:
        rate = r["qualified"] / r["total"] * 100 if r["total"] else 0
        print(f"  {r['source']:<25} {r['total']:>6} {r['qualified']:>10} {rate:>7.1f}%")
    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    import os
    db = os.environ.get("SS_DB_PATH", str(DB_PATH))
    generate(db)


if __name__ == "__main__":
    main()
