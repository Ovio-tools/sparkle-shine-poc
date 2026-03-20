"""seeding/generators/gen_financials.py

Generate all invoice and payment records into sparkle_shine.db.
References the jobs table. Uses random.seed(42).

Run:
    python seeding/generators/gen_financials.py
"""
from __future__ import annotations

import json
import random
import sqlite3
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from seeding.generators.gen_clients import get_commercial_per_visit_rate  # noqa: E402

DB_PATH = ROOT / "sparkle_shine.db"

# ─── Deterministic RNG ────────────────────────────────────────────────────────
_rng = random.Random(42)

# ─── Service base prices (residential) ───────────────────────────────────────
SERVICE_PRICE: dict[str, float] = {
    "recurring-weekly":   135.0,
    "recurring-biweekly": 150.0,
    "recurring-monthly":  165.0,
    "deep-clean":         275.0,
    "move-in-out":        325.0,
    "std-residential":    150.0,
    # commercial-nightly is priced via get_commercial_per_visit_rate()
}

# ─── Due-date offset by client type ──────────────────────────────────────────
DUE_OFFSET: dict[str, int] = {"residential": 7, "commercial": 30}

# ─── Monthly revenue validation targets {YYYY-MM: (low, high)} ───────────────
REVENUE_TARGETS: dict[str, tuple[float, float]] = {
    "2025-04": (135_000, 145_000),
    "2025-05": (135_000, 145_000),
    "2025-06": (148_000, 160_000),
    "2025-07": (148_000, 160_000),
    "2025-08": (128_000, 140_000),
    "2025-09": (128_000, 140_000),
    "2025-10": (140_000, 155_000),
    "2025-11": (140_000, 155_000),
    "2025-12": (165_000, 185_000),
    "2026-01": (120_000, 135_000),
    "2026-02": (135_000, 150_000),
    "2026-03": (135_000, 150_000),
}

# "Today" in the simulation
TODAY = date(2026, 3, 17)


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _date_from_ts(ts: str) -> date:
    """Parse 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DD' into a date."""
    return date.fromisoformat(ts[:10])


def _fmt(d: date) -> str:
    return d.isoformat()


# ─── Main generation ─────────────────────────────────────────────────────────

def generate(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    # ------------------------------------------------------------------
    # 0. Clear existing data (idempotent re-run)
    # ------------------------------------------------------------------
    cur.execute("DELETE FROM payments")
    cur.execute("DELETE FROM invoices")
    print("Cleared existing invoices and payments.")

    # ------------------------------------------------------------------
    # 1. Resolve narrative late-payer IDs by company name (resilient to
    #    regeneration — IDs shift when clients are regenerated).
    # ------------------------------------------------------------------
    cur.execute("SELECT id FROM clients WHERE company_name LIKE '%Mueller Tech%'")
    row = cur.fetchone()
    late_payer_a = row[0] if row else None   # Mueller Tech Suites → day 52

    cur.execute("SELECT id FROM clients WHERE company_name LIKE '%Rosedale Family%'")
    row = cur.fetchone()
    late_payer_b = row[0] if row else None   # Rosedale Family Practice → day 61

    print(f"Late-payer A (Mueller Tech Suites):       {late_payer_a}")
    print(f"Late-payer B (Rosedale Family Practice):  {late_payer_b}")

    # ------------------------------------------------------------------
    # 2. Load all invoiceable jobs ordered by id
    # ------------------------------------------------------------------
    cur.execute("""
        SELECT j.id, j.client_id, j.service_type_id, j.status,
               COALESCE(j.completed_at, j.scheduled_date) AS effective_date,
               c.client_type, j.scheduled_date
        FROM   jobs j
        JOIN   clients c ON j.client_id = c.id
        WHERE  j.status IN ('completed', 'no-show')
        ORDER  BY j.id
    """)
    jobs = cur.fetchall()
    print(f"Found {len(jobs):,} invoiceable jobs.")

    # ------------------------------------------------------------------
    # 3. Load client-type map
    # ------------------------------------------------------------------
    cur.execute("SELECT id, client_type FROM clients")
    client_type_map: dict[str, str] = dict(cur.fetchall())

    # ------------------------------------------------------------------
    # 4. Generate invoices
    # ------------------------------------------------------------------
    invoices: list[dict] = []
    inv_seq = 1

    for job_id, client_id, svc_type, _status, completed_at, client_type, scheduled_date in jobs:
        inv_id = f"SS-INV-{inv_seq:04d}"
        inv_seq += 1

        issue_date = _date_from_ts(completed_at)
        due_date   = issue_date + timedelta(days=DUE_OFFSET[client_type])

        if client_type == "commercial":
            try:
                amount = get_commercial_per_visit_rate(
                    client_id=client_id,
                    job_date=scheduled_date,
                    service_type_id=svc_type,
                )
            except ValueError as exc:
                print(f"  WARNING: {exc}  — defaulting to $0.00")
                amount = 0.0
        else:
            amount = SERVICE_PRICE.get(svc_type, 150.0)

        invoices.append({
            "id":               inv_id,
            "client_id":        client_id,
            "job_id":           job_id,
            "amount":           round(amount, 2),
            "status":           "sent",   # updated after payment pass
            "issue_date":       _fmt(issue_date),
            "due_date":         _fmt(due_date),
            "paid_date":        None,
            "days_outstanding": None,
        })

    print(f"Generated {len(invoices):,} invoice stubs (status='sent').")

    # ------------------------------------------------------------------
    # 5. Generate payments and update invoice statuses
    # ------------------------------------------------------------------
    payments: list[dict] = []
    pay_seq = 1

    for inv in invoices:
        inv_id     = inv["id"]
        client_id  = inv["client_id"]
        amount     = inv["amount"]
        issue_dt   = date.fromisoformat(inv["issue_date"])
        client_type = client_type_map[client_id]

        pay_date: date | None = None

        if client_type == "residential":
            # ── Credit-card timing distribution ──────────────────────
            r = _rng.random()
            if r < 0.85:
                pay_date = issue_dt                                    # same day
            elif r < 0.95:
                pay_date = issue_dt + timedelta(days=_rng.randint(1, 3))
            elif r < 0.98:
                pay_date = issue_dt + timedelta(days=_rng.randint(4, 7))
            else:
                pay_date = None                                        # bad debt
            payment_method = "credit_card"

        else:
            # ── Commercial net-30 terms ───────────────────────────────
            is_dec_2025 = inv["issue_date"].startswith("2025-12")

            if client_id == late_payer_a and is_dec_2025:
                # Mueller Tech Suites — Dec 2025 invoices paid on day 52
                pay_date = issue_dt + timedelta(days=52)
            elif client_id == late_payer_b and is_dec_2025:
                # Rosedale Family Practice — Dec 2025 invoices paid on day 61
                pay_date = issue_dt + timedelta(days=61)
            else:
                pay_date = issue_dt + timedelta(days=_rng.randint(28, 32))

            payment_method = "ach"

        # Record payment only if the pay date has occurred by TODAY
        if pay_date is not None and pay_date <= TODAY:
            pay_id = f"SS-PAY-{pay_seq:04d}"
            pay_seq += 1
            payments.append({
                "id":             pay_id,
                "invoice_id":     inv_id,
                "client_id":      client_id,
                "amount":         amount,
                "payment_method": payment_method,
                "payment_date":   _fmt(pay_date),
            })
            inv["status"]           = "paid"
            inv["paid_date"]        = _fmt(pay_date)
            inv["days_outstanding"] = (pay_date - issue_dt).days

    # ------------------------------------------------------------------
    # 6. Mark overdue invoices (unpaid, due_date < TODAY)
    # ------------------------------------------------------------------
    overdue_count = 0
    for inv in invoices:
        if inv["status"] == "sent":
            due_dt = date.fromisoformat(inv["due_date"])
            if due_dt < TODAY:
                inv["status"]           = "overdue"
                inv["days_outstanding"] = (TODAY - due_dt).days
                overdue_count += 1

    paid_count     = sum(1 for inv in invoices if inv["status"] == "paid")
    bad_debt_count = sum(
        1 for inv in invoices
        if inv["status"] == "overdue"
        and client_type_map[inv["client_id"]] == "residential"
        and inv["days_outstanding"] is not None
        and inv["days_outstanding"] > 90
    )

    print(f"Generated {len(payments):,} payment records.")
    print(f"  Paid invoices:    {paid_count:,}")
    print(f"  Overdue invoices: {overdue_count:,}  (incl. ~{bad_debt_count} residential 90d+)")

    # ------------------------------------------------------------------
    # 7. Write to database
    # ------------------------------------------------------------------
    cur.executemany(
        """
        INSERT INTO invoices
            (id, client_id, job_id, amount, status,
             issue_date, due_date, paid_date, days_outstanding)
        VALUES
            (:id, :client_id, :job_id, :amount, :status,
             :issue_date, :due_date, :paid_date, :days_outstanding)
        """,
        invoices,
    )

    cur.executemany(
        """
        INSERT INTO payments
            (id, invoice_id, client_id, amount, payment_method, payment_date)
        VALUES
            (:id, :invoice_id, :client_id, :amount, :payment_method, :payment_date)
        """,
        payments,
    )

    conn.commit()
    print(f"\nInserted {len(invoices):,} invoices and {len(payments):,} payments into DB.")

    # ------------------------------------------------------------------
    # 8. Validate monthly revenue against narrative targets
    # ------------------------------------------------------------------
    print("\n" + "─" * 72)
    print("Monthly Revenue Validation (revenue recognized by service/issue date)")
    print("─" * 72)

    # Accrual accounting: count ALL invoices (paid + outstanding) at service date.
    # Commercial net-30 invoices are earned at service date even if not yet paid.
    monthly_revenue: dict[str, float] = {}
    for inv in invoices:
        month = inv["issue_date"][:7]
        monthly_revenue[month] = monthly_revenue.get(month, 0.0) + inv["amount"]

    all_months = sorted(
        set(list(REVENUE_TARGETS.keys()) + list(monthly_revenue.keys()))
    )

    for month in all_months:
        actual = monthly_revenue.get(month, 0.0)
        if month not in REVENUE_TARGETS:
            print(f"  {month}:  ${actual:>12,.2f}  (no target)")
            continue

        lo, hi = REVENUE_TARGETS[month]
        mid    = (lo + hi) / 2.0
        pct    = abs(actual - mid) / mid * 100.0

        if lo <= actual <= hi:
            flag = "OK"
        elif pct > 25:
            direction = "above" if actual > hi else "below"
            flag = f"⚠  WARNING  {pct:.0f}% {direction} target"
        else:
            direction = "above" if actual > hi else "below"
            flag = f"note: {pct:.0f}% {direction} target (minor)"

        print(f"  {month}:  ${actual:>12,.2f}  target [{lo:,.0f}–{hi:,.0f}]  {flag}")

    # ------------------------------------------------------------------
    # 9. AR Aging Report as of TODAY
    # ------------------------------------------------------------------
    print("\n" + "─" * 72)
    print(f"Accounts Receivable Aging — as of {TODAY}")
    print("─" * 72)

    # Buckets keyed by (days_past_due) breakpoints
    buckets: dict[str, dict] = {
        "b0_30":  {"label": "Current (0–30 days)",  "amount": 0.0, "count": 0, "comm_clients": set()},
        "b31_60": {"label": "31–60 days",            "amount": 0.0, "count": 0, "comm_clients": set()},
        "b61_90": {"label": "61–90 days",            "amount": 0.0, "count": 0, "comm_clients": set()},
        "b90p":   {"label": "90+ days",              "amount": 0.0, "count": 0, "comm_clients": set()},
    }

    for inv in invoices:
        if inv["status"] not in ("sent", "overdue"):
            continue

        due_dt       = date.fromisoformat(inv["due_date"])
        days_past_due = (TODAY - due_dt).days   # negative = not yet due

        if days_past_due <= 30:
            key = "b0_30"
        elif days_past_due <= 60:
            key = "b31_60"
        elif days_past_due <= 90:
            key = "b61_90"
        else:
            key = "b90p"

        buckets[key]["amount"] += inv["amount"]
        buckets[key]["count"]  += 1
        if client_type_map.get(inv["client_id"]) == "commercial":
            buckets[key]["comm_clients"].add(inv["client_id"])

    total_ar    = sum(b["amount"] for b in buckets.values())
    total_count = sum(b["count"]  for b in buckets.values())

    for key, b in buckets.items():
        comm_note = ""
        if b["comm_clients"]:
            names_q = ",".join(f"'{c}'" for c in b["comm_clients"])
            cur.execute(
                f"SELECT company_name FROM clients WHERE id IN ({names_q})"
            )
            names = [r[0] for r in cur.fetchall()]
            comm_note = f"  ← commercial: {', '.join(sorted(names))}"
        print(f"  {b['label']:25s}: ${b['amount']:>12,.2f}  from {b['count']:>5} invoices{comm_note}")

    print(f"  {'─'*25}    {'─'*13}  {'─'*16}")
    print(f"  {'Total outstanding':25s}: ${total_ar:>12,.2f}  from {total_count:>5} invoices")

    # ------------------------------------------------------------------
    # 10. Write AR snapshot to daily_metrics_snapshot
    # ------------------------------------------------------------------
    open_inv_value = sum(
        inv["amount"] for inv in invoices
        if inv["status"] == "sent"
        and date.fromisoformat(inv["due_date"]) >= TODAY
    )
    overdue_inv_value = sum(
        inv["amount"] for inv in invoices if inv["status"] == "overdue"
    )

    aging_json = {
        "ar_aging_as_of": str(TODAY),
        "buckets": {
            key: {
                "label":  b["label"],
                "amount": round(b["amount"], 2),
                "count":  b["count"],
            }
            for key, b in buckets.items()
        },
        "total_outstanding": round(total_ar, 2),
        "total_count":       total_count,
    }

    # Gather a few other snapshot metrics from existing tables
    cur.execute(
        "SELECT COUNT(*) FROM jobs WHERE status='completed' "
        "AND completed_at >= '2026-03-01' AND completed_at < '2026-03-18'"
    )
    mtd_completed = cur.fetchone()[0]

    cur.execute(
        "SELECT COUNT(*) FROM jobs WHERE status='scheduled'"
    )
    scheduled_count = cur.fetchone()[0]

    cur.execute(
        "SELECT COUNT(*) FROM jobs WHERE status='cancelled' "
        "AND scheduled_date >= '2026-03-01' AND scheduled_date < '2026-03-18'"
    )
    mtd_cancelled = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM clients WHERE status='active'")
    active_clients = cur.fetchone()[0]

    cur.execute(
        "SELECT COALESCE(SUM(amount), 0) FROM invoices "
        "WHERE status='paid' AND paid_date >= '2026-03-01' AND paid_date < '2026-03-18'"
    )
    revenue_mtd = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM leads WHERE created_at >= '2026-03-01'")
    new_leads = cur.fetchone()[0]

    cur.execute(
        "SELECT COALESCE(SUM(estimated_value), 0) FROM leads "
        "WHERE status IN ('new','contacted','qualified')"
    )
    pipeline_value = cur.fetchone()[0]

    cur.execute(
        """
        INSERT OR REPLACE INTO daily_metrics_snapshot
            (snapshot_date, total_revenue_mtd, jobs_completed, jobs_scheduled,
             jobs_cancelled, active_clients, new_leads,
             open_invoices_value, overdue_invoices_value, pipeline_value, raw_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(TODAY),
            round(revenue_mtd, 2),
            mtd_completed,
            scheduled_count,
            mtd_cancelled,
            active_clients,
            new_leads,
            round(open_inv_value, 2),
            round(overdue_inv_value, 2),
            round(pipeline_value, 2),
            json.dumps(aging_json),
        ),
    )
    conn.commit()

    print(f"\nWrote daily_metrics_snapshot for {TODAY}:")
    print(f"  total_revenue_mtd:      ${revenue_mtd:>12,.2f}")
    print(f"  open_invoices_value:    ${open_inv_value:>12,.2f}")
    print(f"  overdue_invoices_value: ${overdue_inv_value:>12,.2f}")


# ─── Entry point ─────────────────────────────────────────────────────────────

def main() -> None:
    print(f"Connecting to {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        generate(conn)
    finally:
        conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
