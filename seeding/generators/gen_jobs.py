"""seeding/generators/gen_jobs.py

Generate all job and recurring_agreement records into sparkle_shine.db.
Use random.seed(42). Reference config/narrative.py for monthly context.

Run:
    python seeding/generators/gen_jobs.py
"""
from __future__ import annotations

import random
import sqlite3
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from database.mappings import bulk_register
from seeding.utils.text_generator import generate_job_note

DB_PATH = ROOT / "sparkle_shine.db"

# ─── Deterministic RNG ────────────────────────────────────────────────────────
random.seed(42)
_rng       = random.Random(42)   # main RNG (all variance / rolls)
_pause_rng = random.Random(99)   # isolated RNG for Jan-pause selection

# ─── Constants ────────────────────────────────────────────────────────────────
ACTIVE_END    = date(2026, 3, 31)
MAX_CREW_DAILY = 4

SERVICE_CFG = {
    "recurring-weekly":   {"duration": 120, "price": 135.0},
    "recurring-biweekly": {"duration": 120, "price": 150.0},
    "recurring-monthly":  {"duration": 120, "price": 165.0},
    "deep-clean":         {"duration": 210, "price": 275.0},
    "move-in-out":        {"duration": 240, "price": 325.0},
    "std-residential":    {"duration": 120, "price": 150.0},
    "commercial-nightly": {"duration": 180, "price":   0.0},  # variable
}

FREQ_TO_SERVICE  = {"weekly": "recurring-weekly", "biweekly": "recurring-biweekly", "monthly": "recurring-monthly"}
FREQ_INTERVAL    = {"weekly": 7, "biweekly": 14, "monthly": 30}
ZONE_TO_CREW     = {"West Austin": "crew-a", "East Austin": "crew-b", "South Austin": "crew-c"}
DOW_NAMES        = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
TIME_SLOTS       = ["08:00", "10:00", "13:00", "15:00"]

# "Today" for the POC — jobs on/after this date are still in the future
TODAY = date(2026, 3, 17)

# Narrative date ranges
_AUG25_S  = date(2025, 8,  1);  _AUG25_E  = date(2025, 8, 31)
_SEP25_S  = date(2025, 9,  1);  _SEP25_E  = date(2025, 9, 30)
_JUN25_S  = date(2025, 6,  1);  _JUL25_E  = date(2025, 7, 31)
_DEC25_S  = date(2025, 12, 1);  _DEC25_E  = date(2025, 12, 20)
_JAN26_S  = date(2026, 1,  1);  _JAN26_E  = date(2026, 1, 31)
_BARTON_CREEK_START = date(2025, 10, 15)

# Commercial scope → weekday indices (0=Mon … 6=Sun)
_SCOPE_DAYS = {
    "nightly":   [0, 1, 2, 3, 4],
    "5x weekly": [0, 1, 2, 3, 4],
    "3x weekly": [0, 2, 4],
    "2x weekly": [1, 3],
    "daily":     [0, 1, 2, 3, 4, 5, 6],
}

# ─── Global crew-capacity tracker ─────────────────────────────────────────────
_crew_day: dict[tuple, int] = {}   # (crew_id, date_iso) → job count

_recur_counter = 0

def _next_recur_id() -> str:
    global _recur_counter
    _recur_counter += 1
    return f"SS-RECUR-{_recur_counter:04d}"


# ─── Small helpers ─────────────────────────────────────────────────────────────

def _months_between(s: str, e: str) -> int:
    """Same formula as gen_clients: max(1, calendar-month span inclusive)."""
    sd, ed = date.fromisoformat(s), date.fromisoformat(e)
    return max(1, (ed.year - sd.year) * 12 + (ed.month - sd.month) + 1)


def _infer_frequency(ltv: float, first_svc: str, last_svc: str) -> str:
    """Reverse-engineer service frequency from stored LTV.

    gen_clients computed:  ltv = months * visits_per_month * price
      weekly   → months * 4 * 135  = months * 540
      biweekly → months * 2 * 150  = months * 300
      monthly  → months * 1 * 165  = months * 165
    """
    m = _months_between(first_svc, last_svc)
    targets = {"weekly": m * 540.0, "biweekly": m * 300.0, "monthly": m * 165.0}
    return min(targets, key=lambda k: abs(ltv - targets[k]))


def _crew_for(c: dict) -> str:
    if c["client_type"] == "commercial":
        return "crew-d"
    return ZONE_TO_CREW.get(c["zone"], "crew-b")


def _duration(base: int, crew_id: str) -> int:
    if crew_id == "crew-a":
        return int(base * 1.20)
    if crew_id == "crew-d":
        return base
    return int(base * _rng.uniform(0.95, 1.10))


def _advance_if_full(crew_id: str, d: date, cap: Optional[date] = None) -> Optional[date]:
    """Push d forward until the crew has capacity.
    Returns None if the resulting date would exceed cap (defaults to ACTIVE_END)."""
    limit = cap or ACTIVE_END
    while _crew_day.get((crew_id, d.isoformat()), 0) >= MAX_CREW_DAILY:
        d += timedelta(days=1)
        if d > limit:
            return None
    return d


def _timeslot(crew_id: str, d: date) -> str:
    idx = _crew_day.get((crew_id, d.isoformat()), 0)
    return TIME_SLOTS[idx % len(TIME_SLOTS)]


def _book(crew_id: str, d: date):
    key = (crew_id, d.isoformat())
    _crew_day[key] = _crew_day.get(key, 0) + 1


def _roll_status(job_date: date) -> str:
    """Return job status; future-dated jobs are always 'scheduled'."""
    if job_date >= TODAY:
        return "scheduled"
    r = _rng.random()
    if r < 0.03:
        return "cancelled"
    if r < 0.05:
        return "no-show"
    return "completed"


def _maybe_note(svc_id: str, neighborhood: str, issues: bool = False) -> str | None:
    if _rng.random() > 0.15:
        return None
    try:
        return generate_job_note(svc_id, neighborhood, issues)
    except Exception:
        return None


def _completed_at(d: date, ts: str, dur: int) -> str:
    h, m = map(int, ts.split(":"))
    end = datetime(d.year, d.month, d.day, h, m) + timedelta(minutes=dur)
    return end.strftime("%Y-%m-%d %H:%M:%S")


def _monthly_value(ltv: float, first_svc: str, last_svc: str) -> float:
    return ltv / _months_between(first_svc, last_svc)


# ─── Client loading ────────────────────────────────────────────────────────────

def _load_clients(conn) -> list[dict]:
    cur = conn.cursor()
    return [dict(r) for r in cur.execute("SELECT * FROM clients").fetchall()]


# ─── Recurring agreements ──────────────────────────────────────────────────────

def _build_agreements(recurring_clients: list[dict]) -> list[dict]:
    agreements = []
    for c in recurring_clients:
        freq    = _infer_frequency(c["lifetime_value"], c["first_service_date"], c["last_service_date"])
        svc_id  = FREQ_TO_SERVICE[freq]
        crew_id = _crew_for(c)
        price   = SERVICE_CFG[svc_id]["price"]
        end     = c["last_service_date"] if c["status"] == "churned" else None
        dow     = DOW_NAMES[int(c["id"].split("-")[-1]) % 5]
        status  = "cancelled" if c["status"] == "churned" else "active"
        agreements.append({
            "id":              _next_recur_id(),
            "client_id":       c["id"],
            "service_type_id": svc_id,
            "crew_id":         crew_id,
            "frequency":       freq,
            "price_per_visit": price,
            "start_date":      c["first_service_date"],
            "end_date":        end,
            "status":          status,
            "day_of_week":     dow,
            # internal helpers (not written to DB)
            "_freq":           freq,
            "_crew_id":        crew_id,
        })
    return agreements


# ─── Recurring job generation ──────────────────────────────────────────────────

def _gen_recurring_jobs(
    agr: dict,
    client: dict,
    paused_jan: set[str],
) -> list[dict]:
    freq     = agr["_freq"]
    crew_id  = agr["_crew_id"]
    svc_id   = agr["service_type_id"]
    price    = agr["price_per_visit"]
    interval = FREQ_INTERVAL[freq]
    nbhd     = client.get("neighborhood") or ""
    cid      = client["id"]
    address  = client["address"]

    start = date.fromisoformat(agr["start_date"])
    end   = date.fromisoformat(agr["end_date"]) if agr["end_date"] else ACTIVE_END

    jobs: list[dict] = []
    cur = start

    while cur <= end:
        # Jan 2026 pause
        if cid in paused_jan and _JAN26_S <= cur <= _JAN26_E:
            cur += timedelta(days=interval)
            continue

        actual_date = _advance_if_full(crew_id, cur, cap=end)
        if actual_date is None:
            cur += timedelta(days=interval)
            continue

        status = _roll_status(actual_date)
        ts     = _timeslot(crew_id, actual_date)
        _book(crew_id, actual_date)

        base_dur = SERVICE_CFG[svc_id]["duration"]
        dur      = _duration(base_dur, crew_id)

        # Aug 2025 rough patch: overloaded crews run long
        if crew_id in ("crew-c", "crew-d") and _AUG25_S <= actual_date <= _AUG25_E:
            dur = int(dur * 1.15)

        note = None
        if status == "completed":
            note = _maybe_note(svc_id, nbhd)
        elif status == "cancelled":
            note = _rng.choice([
                "Client requested cancellation.",
                "Crew unavailable — rescheduled.",
                "Client not home — cancelled.",
            ])

        jobs.append({
            "client_id":               cid,
            "crew_id":                 crew_id,
            "service_type_id":         svc_id,
            "scheduled_date":          actual_date.isoformat(),
            "scheduled_time":          ts,
            "duration_minutes_actual": dur,
            "status":                  status,
            "address":                 address,
            "notes":                   note,
            "review_requested":        1 if (status == "completed" and _rng.random() < 0.30) else 0,
            "completed_at":            _completed_at(actual_date, ts, dur) if status == "completed" else None,
            "_price":                  price,
            "_recurring":              True,
        })

        cur += timedelta(days=interval)

    return jobs


# ─── One-time job helper ───────────────────────────────────────────────────────

def _onetime(
    client: dict,
    svc_id: str,
    job_date: date,
    forced_note: Optional[str] = None,
    price_override: Optional[float] = None,
) -> Optional[dict]:
    crew_id = _crew_for(client)
    d       = _advance_if_full(crew_id, job_date)
    if d is None:
        return None
    ts      = _timeslot(crew_id, d)
    _book(crew_id, d)

    base_dur = SERVICE_CFG[svc_id]["duration"]
    dur      = _duration(base_dur, crew_id)
    price    = price_override if price_override is not None else SERVICE_CFG[svc_id]["price"]
    status   = "completed" if d < TODAY else "scheduled"
    note     = (forced_note or _maybe_note(svc_id, client.get("neighborhood") or "")) if status == "completed" else None

    return {
        "client_id":               client["id"],
        "crew_id":                 crew_id,
        "service_type_id":         svc_id,
        "scheduled_date":          d.isoformat(),
        "scheduled_time":          ts,
        "duration_minutes_actual": dur,
        "status":                  status,
        "address":                 client["address"],
        "notes":                   note,
        "review_requested":        1 if (status == "completed" and _rng.random() < 0.30) else 0,
        "completed_at":            _completed_at(d, ts, dur) if status == "completed" else None,
        "_price":                  price,
    }


# ─── Quick-churn: 1 job each ───────────────────────────────────────────────────

_LTV_TO_SVC = {
    135.0: "recurring-weekly",
    150.0: "recurring-biweekly",
    165.0: "recurring-monthly",
    275.0: "deep-clean",
    325.0: "move-in-out",
}

def _gen_quickchurn_jobs(clients: list[dict]) -> list[dict]:
    jobs = []
    for c in clients:
        ltv    = c["lifetime_value"]
        svc_id = min(_LTV_TO_SVC, key=lambda k: abs(k - ltv))
        svc_id = _LTV_TO_SVC[svc_id]
        d      = date.fromisoformat(c["first_service_date"])
        j = _onetime(c, svc_id, d)
        if j:
            jobs.append(j)
    return jobs


# ─── Occasional: 1–3 jobs each ────────────────────────────────────────────────

def _gen_occasional_jobs(clients: list[dict]) -> list[dict]:
    jobs = []
    for c in clients:
        raw      = c.get("notes") or ""
        num_jobs = 1
        svc_id   = "deep-clean"

        for part in raw.split("|"):
            p = part.strip()
            if p.startswith("num_jobs:"):
                try:
                    num_jobs = int(p.split(":", 1)[1].strip())
                except ValueError:
                    pass
            elif p.startswith("service_type:"):
                svc_id = p.split(":", 1)[1].strip()

        start = date.fromisoformat(c["first_service_date"])
        for i in range(num_jobs):
            d = start + timedelta(days=i * 35)   # ~5 weeks apart
            if d > ACTIVE_END:
                break
            j = _onetime(c, svc_id, d)
            if j:
                jobs.append(j)

    return jobs


# ─── Commercial jobs ───────────────────────────────────────────────────────────

def _parse_scope(notes: str) -> str:
    n = (notes or "").lower()
    if "nightly" in n:        return "nightly"
    if "5x weekly" in n:      return "5x weekly"
    if "3x weekly" in n:      return "3x weekly"
    if "2x weekly" in n:      return "2x weekly"
    if "daily" in n:           return "daily"
    return "3x weekly"


def _gen_commercial_jobs(clients: list[dict]) -> list[dict]:
    jobs: list[dict] = []

    for c in clients:
        crew_id = "crew-d"
        nbhd    = c.get("neighborhood") or ""
        notes   = c.get("notes") or ""
        address = c["address"]
        ltv     = c["lifetime_value"]
        first   = c["first_service_date"]
        last    = c["last_service_date"]

        start_date = date.fromisoformat(first)
        end_date   = date.fromisoformat(last) if c["status"] == "churned" else ACTIVE_END

        # ── Barton Creek Medical Group (special schedule) ─────────────────
        if "Barton Creek Medical Group" in (c.get("company_name") or ""):
            start_date   = max(start_date, _BARTON_CREEK_START)
            monthly      = _monthly_value(ltv, first, last)
            nightly_price = round(monthly / (5 * 4.33), 2)   # ~$207/visit
            sat_price     = 462.0                             # per narrative spec

            cur = start_date
            while cur <= end_date:
                wd = cur.weekday()
                if wd < 5:    # Mon–Fri nightly
                    d  = _advance_if_full(crew_id, cur, cap=end_date)
                    if d is None:
                        cur += timedelta(days=1)
                        continue
                    ts  = _timeslot(crew_id, d)
                    _book(crew_id, d)
                    st  = "completed" if d < TODAY else "scheduled"
                    jobs.append({
                        "client_id": c["id"], "crew_id": crew_id,
                        "service_type_id": "commercial-nightly",
                        "scheduled_date": d.isoformat(), "scheduled_time": ts,
                        "duration_minutes_actual": 180, "status": st,
                        "address": address, "notes": None, "review_requested": 0,
                        "completed_at": _completed_at(d, ts, 180) if st == "completed" else None,
                        "_price": nightly_price,
                    })
                elif wd == 5:  # Saturday deep clean
                    d  = _advance_if_full(crew_id, cur, cap=end_date)
                    if d is None:
                        cur += timedelta(days=1)
                        continue
                    ts  = _timeslot(crew_id, d)
                    _book(crew_id, d)
                    st  = "completed" if d < TODAY else "scheduled"
                    jobs.append({
                        "client_id": c["id"], "crew_id": crew_id,
                        "service_type_id": "deep-clean",
                        "scheduled_date": d.isoformat(), "scheduled_time": ts,
                        "duration_minutes_actual": 300, "status": st,
                        "address": address, "notes": None, "review_requested": 0,
                        "completed_at": _completed_at(d, ts, 300) if st == "completed" else None,
                        "_price": sat_price,
                    })
                cur += timedelta(days=1)
            continue

        # ── All other commercial clients ───────────────────────────────────
        scope   = _parse_scope(notes)
        weekdays = _SCOPE_DAYS[scope]
        vpm      = len(weekdays) * (365 / 12 / 7)        # avg visits/month
        monthly  = _monthly_value(ltv, first, last)
        price    = round(monthly / vpm, 2) if vpm else 0.0

        cur = start_date
        while cur <= end_date:
            if cur.weekday() in weekdays:
                d  = _advance_if_full(crew_id, cur, cap=end_date)
                if d is None:
                    cur += timedelta(days=1)
                    continue
                ts = _timeslot(crew_id, d)
                _book(crew_id, d)
                st = "completed" if d < TODAY else "scheduled"
                jobs.append({
                    "client_id": c["id"], "crew_id": crew_id,
                    "service_type_id": "commercial-nightly",
                    "scheduled_date": d.isoformat(), "scheduled_time": ts,
                    "duration_minutes_actual": 120, "status": st,
                    "address": address, "notes": None, "review_requested": 0,
                    "completed_at": _completed_at(d, ts, 120) if st == "completed" else None,
                    "_price": price,
                })
            cur += timedelta(days=1)

    return jobs


# ─── Narrative: summer deep-clean surge ───────────────────────────────────────

def _gen_summer_surge(occasional: list[dict], count: int = 45) -> list[dict]:
    """45 deep-clean jobs in Jun–Jul 2025, preferring B/C-zone occasional clients."""
    pool = [c for c in occasional if c["zone"] in ("East Austin", "South Austin")] or occasional
    span = (_JUL25_E - _JUN25_S).days
    jobs = []
    for i in range(count):
        c  = pool[i % len(pool)]
        d  = _JUN25_S + timedelta(days=int(_rng.random() * span))
        j  = _onetime(c, "deep-clean", d)
        if j:
            jobs.append(j)
    return jobs


# ─── Narrative: holiday peak ───────────────────────────────────────────────────

_HOLIDAY_SVC = ["deep-clean", "deep-clean", "move-in-out", "std-residential"]

def _gen_holiday_peak(occasional: list[dict], count: int = 60) -> list[dict]:
    """60 holiday cleaning jobs Dec 1–20 2025."""
    span = (_DEC25_E - _DEC25_S).days
    jobs = []
    for i in range(count):
        c      = occasional[i % len(occasional)]
        d      = _DEC25_S + timedelta(days=int(_rng.random() * span))
        svc_id = _HOLIDAY_SVC[i % len(_HOLIDAY_SVC)]
        j      = _onetime(c, svc_id, d)
        if j:
            jobs.append(j)
    return jobs


# ─── Narrative: Sep complaint notes ───────────────────────────────────────────

def _apply_complaint_notes(jobs: list[dict], count: int = 3):
    sep_ok = [
        j for j in jobs
        if j["status"] == "completed" and j["scheduled_date"].startswith("2025-09")
    ]
    if len(sep_ok) < count:
        return
    for j in _rng.sample(sep_ok, count):
        j["notes"] = "Client reported dissatisfaction — escalated to Maria."


# ─── DB helpers ────────────────────────────────────────────────────────────────

_SQL_INSERT_RECUR = """
INSERT OR IGNORE INTO recurring_agreements
  (id, client_id, service_type_id, crew_id, frequency, price_per_visit,
   start_date, end_date, status, day_of_week)
VALUES (?,?,?,?,?,?,?,?,?,?)
"""

_SQL_INSERT_JOB = """
INSERT OR IGNORE INTO jobs
  (id, client_id, crew_id, service_type_id, scheduled_date, scheduled_time,
   duration_minutes_actual, status, address, notes, review_requested, completed_at)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
"""


def _insert_agreements(conn, agreements: list[dict]):
    conn.cursor().executemany(_SQL_INSERT_RECUR, [
        (
            a["id"], a["client_id"], a["service_type_id"], a["crew_id"],
            a["frequency"], a["price_per_visit"],
            a["start_date"], a["end_date"], a["status"], a["day_of_week"],
        )
        for a in agreements
    ])
    conn.commit()


def _insert_jobs(conn, jobs: list[dict]) -> list[dict]:
    """Sort by date, assign sequential SS-JOB-NNNN IDs, insert."""
    jobs_sorted = sorted(jobs, key=lambda j: j["scheduled_date"])
    rows = []
    for i, j in enumerate(jobs_sorted, 1):
        jid    = f"SS-JOB-{i:04d}"
        j["id"] = jid
        rows.append((
            jid,
            j["client_id"], j["crew_id"], j["service_type_id"],
            j["scheduled_date"], j["scheduled_time"],
            j["duration_minutes_actual"], j["status"],
            j["address"], j["notes"],
            j["review_requested"], j.get("completed_at"),
        ))
    conn.cursor().executemany(_SQL_INSERT_JOB, rows)
    conn.commit()
    return jobs_sorted


def _mark_paused_agreements(conn, paused_cids: set[str]):
    cur = conn.cursor()
    for cid in paused_cids:
        cur.execute(
            "UPDATE recurring_agreements SET status='paused' WHERE client_id=?", (cid,)
        )
    conn.commit()


# ─── Summary ───────────────────────────────────────────────────────────────────

def _print_summary(jobs: list[dict], agreements: list[dict]):
    by_status: dict[str, int]       = {}
    by_month:  dict[str, dict]      = {}
    by_crew:   dict[str, dict]      = {}

    for j in jobs:
        s = j["status"]
        by_status[s] = by_status.get(s, 0) + 1

        ym = j["scheduled_date"][:7]
        bm = by_month.setdefault(ym, {"count": 0, "revenue": 0.0})
        bm["count"] += 1
        if s in ("completed", "no-show", "scheduled"):
            bm["revenue"] += j.get("_price") or 0.0

        crew = j["crew_id"]
        bc   = by_crew.setdefault(crew, {"count": 0, "dur_total": 0})
        bc["count"]     += 1
        bc["dur_total"] += j["duration_minutes_actual"]

    recurring_ct  = sum(1 for j in jobs if j.get("_recurring"))
    onetime_ct    = len(jobs) - recurring_ct
    commercial_ct = sum(1 for j in jobs if j["crew_id"] == "crew-d")

    W = 62
    print("\n" + "═" * W)
    print("  SPARKLE & SHINE — JOB GENERATION SUMMARY")
    print("═" * W)
    print(f"  Total jobs generated : {len(jobs):,}")
    print(f"  Recurring agreements : {len(agreements)}")
    print(f"  Recurring jobs       : {recurring_ct:,}")
    print(f"  One-time jobs        : {onetime_ct:,}")
    print(f"  Commercial (Crew D)  : {commercial_ct:,}")
    print()
    print("  By status:")
    for s in sorted(by_status):
        print(f"    {s:<12}  {by_status[s]:,}")
    print()
    print(f"  {'Month':<9}  {'Jobs':>5}  {'Est. Revenue':>14}")
    print("  " + "-" * 33)
    for ym in sorted(by_month):
        d = by_month[ym]
        print(f"  {ym:<9}  {d['count']:>5}  ${d['revenue']:>13,.0f}")
    print()
    print(f"  {'Crew':<8}  {'Jobs':>5}  {'Avg Duration':>14}")
    print("  " + "-" * 32)
    for crew in sorted(by_crew):
        d   = by_crew[crew]
        avg = d["dur_total"] / d["count"] if d["count"] else 0
        print(f"  {crew:<8}  {d['count']:>5}  {avg:>13.0f} min")
    print("═" * W)


# ─── Main ──────────────────────────────────────────────────────────────────────

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    print("Loading clients…")
    all_clients = _load_clients(conn)
    client_map  = {c["id"]: c for c in all_clients}

    # Categorise
    recurring_clients = [
        c for c in all_clients
        if c["client_type"] == "residential"
        and c["status"] in ("active", "churned")
        and "churn_fast" not in (c.get("notes") or "")
    ]
    quick_churn_clients = [
        c for c in all_clients
        if c["status"] == "churned"
        and "churn_fast: True" in (c.get("notes") or "")
    ]
    occasional_clients = [c for c in all_clients if c["status"] == "occasional"]
    commercial_clients = [c for c in all_clients if c["client_type"] == "commercial"]

    print(f"  Recurring residential : {len(recurring_clients)}")
    print(f"  Quick-churn           : {len(quick_churn_clients)}")
    print(f"  Occasional            : {len(occasional_clients)}")
    print(f"  Commercial            : {len(commercial_clients)}")

    # ── Recurring agreements ─────────────────────────────────────────────────
    print("\nBuilding recurring agreements…")
    agreements    = _build_agreements(recurring_clients)
    agr_by_client = {a["client_id"]: a for a in agreements}

    # ── Jan 2026 pause: 8 active recurring clients ───────────────────────────
    active_cids = [
        a["client_id"] for a in agreements
        if client_map[a["client_id"]]["status"] == "active"
    ]
    paused_jan: set[str] = set(_pause_rng.sample(active_cids, min(8, len(active_cids))))
    for a in agreements:
        if a["client_id"] in paused_jan:
            a["status"] = "paused"

    # ── Recurring jobs ───────────────────────────────────────────────────────
    print("Generating recurring jobs…")
    all_jobs: list[dict] = []

    for c in recurring_clients:
        agr  = agr_by_client[c["id"]]
        jobs = _gen_recurring_jobs(agr, c, paused_jan)
        all_jobs.extend(jobs)

    print(f"  Recurring jobs so far : {len(all_jobs):,}")

    # ── Quick-churn (1 job each) ─────────────────────────────────────────────
    print("Generating quick-churn jobs…")
    all_jobs.extend(_gen_quickchurn_jobs(quick_churn_clients))

    # ── Occasional (1–3 jobs each) ───────────────────────────────────────────
    print("Generating occasional jobs…")
    all_jobs.extend(_gen_occasional_jobs(occasional_clients))

    # ── Narrative: summer surge (Jun–Jul 2025) ───────────────────────────────
    print("Adding summer deep-clean surge (Jun–Jul 2025, 45 jobs)…")
    all_jobs.extend(_gen_summer_surge(occasional_clients))

    # ── Narrative: holiday peak (Dec 1–20 2025) ──────────────────────────────
    print("Adding holiday peak (Dec 2025, 60 jobs)…")
    all_jobs.extend(_gen_holiday_peak(occasional_clients))

    # ── Narrative: Sep 2025 complaint notes ──────────────────────────────────
    _apply_complaint_notes(all_jobs)

    # ── Commercial jobs (including Barton Creek) ─────────────────────────────
    print("Generating commercial jobs…")
    all_jobs.extend(_gen_commercial_jobs(commercial_clients))

    # ── Insert ───────────────────────────────────────────────────────────────
    total_before = len(all_jobs)
    print(f"\nTotal jobs pre-insert : {total_before:,}")

    print("Inserting recurring agreements…")
    _insert_agreements(conn, agreements)

    print("Inserting jobs (sorted by date)…")
    all_jobs = _insert_jobs(conn, all_jobs)

    # Update paused agreement statuses
    _mark_paused_agreements(conn, paused_jan)

    # ── Cross-tool mappings ──────────────────────────────────────────────────
    print("Registering cross-tool mappings…")
    mappings = (
        [(j["id"], "local", j["id"]) for j in all_jobs]
        + [(a["id"], "local", a["id"]) for a in agreements]
    )
    bulk_register(mappings, str(DB_PATH))

    # ── Summary ──────────────────────────────────────────────────────────────
    _print_summary(all_jobs, agreements)
    conn.close()


if __name__ == "__main__":
    main()
