"""
seeding/generators/gen_clients.py

Generates all client and lead records into sparkle_shine.db.

Run from the project root:
    python seeding/generators/gen_clients.py

Produces:
  - 320 client rows  (310 residential + 10 commercial)
  - 160 lead rows    (110 residential + 50 commercial)
  - cross_tool_mapping rows (tool_name='local') for every canonical ID
"""

import random
import sqlite3
import sys
from collections import Counter
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Path bootstrap — allow running from project root or directly
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_PROJECT_ROOT))

from config.business import CREWS, ZONES
from database.schema import get_connection, init_db
from database.mappings import bulk_register
from seeding.utils.faker_austin import (
    fake_address,
    fake_business,
    fake_email,
    fake_person,
    fake_date_in_range,
)

random.seed(42)

DB_PATH = str(_PROJECT_ROOT / "sparkle_shine.db")
TODAY = "2026-03-17"

# ---------------------------------------------------------------------------
# Lookup tables derived from business config
# ---------------------------------------------------------------------------

CREW_ZONE_STRING: Dict[str, str] = {c["id"]: c["zone"] for c in CREWS}

NEIGHBORHOOD_TO_CREW: Dict[str, str] = {}
for _crew in CREWS:
    for _n in ZONES[_crew["id"]]:
        NEIGHBORHOOD_TO_CREW[_n] = _crew["id"]

# Residential crews only (crew-d is commercial/overflow)
RES_NEIGHBORHOODS: List[str] = [
    n
    for cid, hoods in ZONES.items()
    if cid != "crew-d"
    for n in hoods
]

# ---------------------------------------------------------------------------
# Distribution constants
# ---------------------------------------------------------------------------

ACQU_SOURCES = ["Google Ads", "referral", "organic search", "Yelp", "other"]
ACQU_WEIGHTS = [35, 25, 20, 10, 10]

FREQ_CFG = {
    "weekly":   {"visits_per_month": 4, "price": 135.00},
    "biweekly": {"visits_per_month": 2, "price": 150.00},
    "monthly":  {"visits_per_month": 1, "price": 165.00},
}
FREQ_CHOICES = ["biweekly", "weekly", "monthly"]
FREQ_WEIGHTS = [40, 35, 25]

# ---------------------------------------------------------------------------
# Hardcoded commercial clients (narrative-exact data)
# ---------------------------------------------------------------------------

_COMMERCIAL_CLIENTS = [
    # ── 8 active ──────────────────────────────────────────────────────────
    {
        "company_name":  "Barton Creek Medical Group",
        "neighborhood":  "Westlake",
        "win_date":      "2025-10-01",
        "monthly_value": 27_000.00,          # corrected from 4_500 (per-sqft calibration)
        "schedule":      "nightly_plus_saturday",   # 22 nightly Mon-Fri + 4 Saturday deep-cleans
        "status":        "active",
        "extra_notes":   "high_value: True | service_scope: medical office complex nightly clean",
    },
    {
        "company_name":  "South Lamar Dental",
        "neighborhood":  "South Austin",
        "win_date":      "2025-05-15",
        "monthly_value": 6_000.00,           # corrected from 1_200
        "schedule":      "3x_weekly",        # 13 visits/month
        "status":        "active",
        "extra_notes":   "service_scope: dental office 3x weekly",
    },
    {
        "company_name":  "Mueller Tech Suites",
        "neighborhood":  "Mueller",
        "win_date":      "2025-06-01",
        "monthly_value": 14_000.00,          # corrected from 2_800
        "schedule":      "nightly_plus_saturday",   # 22 nightly Mon-Fri + 4 Saturday deep-cleans
        "status":        "active",
        "extra_notes":   "service_scope: co-working office complex nightly",
    },
    {
        "company_name":  "Crestview Coworking",
        "neighborhood":  "Hyde Park",
        "win_date":      "2025-04-15",
        "monthly_value": 7_500.00,           # corrected from 1_500
        "schedule":      "nightly_weekdays", # 22 visits/month Mon-Fri only
        "status":        "active",
        "extra_notes":   "service_scope: coworking space nightly clean",
    },
    {
        "company_name":  "Hyde Park Realty Group",
        "neighborhood":  "Hyde Park",
        "win_date":      "2025-07-10",
        "monthly_value": 4_500.00,           # corrected from 900
        "schedule":      "2x_weekly",        # 9 visits/month
        "status":        "active",
        "extra_notes":   "service_scope: real estate office 2x weekly",
    },
    {
        "company_name":  "Domain Business Center",
        "neighborhood":  "East Austin",
        "win_date":      "2025-08-20",
        "monthly_value": 16_000.00,          # corrected from 3_200
        "schedule":      "nightly_plus_saturday",   # 22 nightly Mon-Fri + 4 Saturday deep-cleans
        "status":        "active",
        "extra_notes":   "service_scope: multi-tenant office building nightly",
    },
    {
        "company_name":  "Rosedale Family Practice",
        "neighborhood":  "Tarrytown",
        "win_date":      "2025-09-05",
        "monthly_value": 9_000.00,           # corrected from 1_800
        "schedule":      "3x_weekly",        # 13 visits/month
        "status":        "active",
        "extra_notes":   "service_scope: medical family practice 5x weekly",
    },
    {
        "company_name":  "Cherrywood Coffeehouse LLC",
        "neighborhood":  "Cherrywood",
        "win_date":      "2026-01-10",
        "monthly_value": 4_000.00,           # corrected from 800
        "schedule":      "daily",            # 30 visits/month
        "status":        "active",
        "extra_notes":   "service_scope: cafe and event space daily clean",
    },
    # ── 2 churned ─────────────────────────────────────────────────────────
    {
        "company_name":  "North Loop Bistro",
        "neighborhood":  "East Austin",
        "win_date":      "2025-04-01",
        "monthly_value": 5_500.00,           # corrected from 1_100
        "schedule":      "3x_weekly",        # 13 visits/month
        "status":        "churned",
        "churn_date":    "2025-06-30",
        "extra_notes":   "cancellation_reason: Client relocated",
    },
    {
        "company_name":  "East Cesar Chavez Gallery",
        "neighborhood":  "East Austin",
        "win_date":      "2025-04-01",
        "monthly_value": 4_750.00,           # corrected from 950
        "schedule":      "daily",            # 30 visits/month
        "status":        "churned",
        "churn_date":    "2025-07-31",
        "extra_notes":   "cancellation_reason: Client relocated",
    },
]

# ---------------------------------------------------------------------------
# Commercial per-visit rate registry
# ---------------------------------------------------------------------------

def _compute_commercial_rates(data: dict) -> dict:
    """
    Derive per-visit invoice rates from a client's monthly_value and schedule.

    Schedule types and their visit counts:
      nightly_plus_saturday  22 nightly (Mon-Fri) + 4 Saturday deep-cleans/month
      nightly_weekdays       22 nightly Mon-Fri visits/month
      3x_weekly              13 visits/month
      2x_weekly               9 visits/month
      daily                  30 visits/month

    For nightly_plus_saturday the two rates are:
      nightly_rate  = monthly_value / 26            (26 total visits/month)
      saturday_rate = nightly_rate  × (4 / 9)       (ratio from calibration exercise)
    """
    mv       = data["monthly_value"]
    schedule = data.get("schedule", "nightly_weekdays")

    if schedule == "nightly_plus_saturday":
        nightly  = round(mv / 26, 2)
        saturday = round(nightly * 4 / 9, 2)
        return {"nightly_rate": nightly, "saturday_rate": saturday, "per_visit_rate": nightly}
    elif schedule == "nightly_weekdays":
        return {"per_visit_rate": round(mv / 22, 2)}
    elif schedule == "3x_weekly":
        return {"per_visit_rate": round(mv / 13, 2)}
    elif schedule == "2x_weekly":
        return {"per_visit_rate": round(mv / 9, 2)}
    elif schedule == "daily":
        return {"per_visit_rate": round(mv / 30, 2)}
    else:
        return {"per_visit_rate": round(mv / 22, 2)}


# Keyed by company_name — built once at module load.
_COMMERCIAL_RATES_BY_COMPANY: Dict[str, dict] = {
    data["company_name"]: _compute_commercial_rates(data)
    for data in _COMMERCIAL_CLIENTS
}

# Populated by _gen_commercial() at runtime (client_id → company_name).
# Also lazy-loaded from DB by get_commercial_per_visit_rate() when gen_financials
# runs in a separate process.
_client_id_to_company: Dict[str, str] = {}
_commercial_cache_loaded: List[bool] = [False]


def _load_commercial_cache() -> None:
    """Load client_id → company_name mapping from DB into module-level cache."""
    try:
        import sqlite3 as _sq
        _conn = _sq.connect(str(_PROJECT_ROOT / "sparkle_shine.db"))
        _cur  = _conn.cursor()
        _cur.execute(
            "SELECT id, company_name FROM clients WHERE client_type = 'commercial'"
        )
        _client_id_to_company.update(dict(_cur.fetchall()))
        _conn.close()
        _commercial_cache_loaded[0] = True
    except Exception:
        pass  # DB may not exist yet; caller will raise ValueError


def get_commercial_per_visit_rate(
    client_id: str,
    job_date: Optional[str] = None,
    service_type_id: Optional[str] = None,
) -> float:
    """
    Return the correct per-visit invoice amount for a commercial client.

    For clients with a nightly/Saturday split (schedule='nightly_plus_saturday'):
      - If service_type_id == 'deep-clean' OR the job falls on a Saturday: saturday_rate
      - Otherwise: nightly_rate
    For all other commercial clients: per_visit_rate directly.

    Raises ValueError for unknown client_id or missing rate config.
    """
    # Ensure cache is populated (lazy load from DB on first call from gen_financials)
    if not _commercial_cache_loaded[0]:
        _load_commercial_cache()

    company_name = _client_id_to_company.get(client_id)
    if company_name is None:
        raise ValueError(
            f"get_commercial_per_visit_rate: unknown commercial client_id={client_id!r}. "
            "Ensure gen_clients.py ran successfully and the DB is populated."
        )

    rates = _COMMERCIAL_RATES_BY_COMPANY.get(company_name)
    if rates is None:
        raise ValueError(
            f"get_commercial_per_visit_rate: no rate entry for company={company_name!r}. "
            "Check _COMMERCIAL_CLIENTS in gen_clients.py."
        )

    # Split-rate clients: choose nightly vs. Saturday deep-clean rate
    if "nightly_rate" in rates and "saturday_rate" in rates:
        is_saturday = False
        if service_type_id == "deep-clean":
            is_saturday = True
        elif job_date is not None:
            try:
                from datetime import date as _d
                is_saturday = (_d.fromisoformat(str(job_date)[:10]).weekday() == 5)
            except (ValueError, TypeError):
                pass
        return rates["saturday_rate"] if is_saturday else rates["nightly_rate"]

    return rates["per_visit_rate"]


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

_used_emails: set = set()
_client_seq: List[int] = [1]
_lead_seq: List[int] = [1]


def _init_counters(conn: sqlite3.Connection) -> None:
    cur = conn.execute("SELECT id FROM clients ORDER BY id DESC LIMIT 1")
    row = cur.fetchone()
    if row:
        _client_seq[0] = int(row["id"].split("-")[-1]) + 1

    cur = conn.execute("SELECT id FROM leads ORDER BY id DESC LIMIT 1")
    row = cur.fetchone()
    if row:
        _lead_seq[0] = int(row["id"].split("-")[-1]) + 1


def _next_client_id() -> str:
    n = _client_seq[0]
    _client_seq[0] += 1
    return f"SS-CLIENT-{n:04d}"


def _next_lead_id() -> str:
    n = _lead_seq[0]
    _lead_seq[0] += 1
    return f"SS-LEAD-{n:04d}"


def _unique_email(first: str, last: str, company: Optional[str] = None) -> str:
    base = fake_email(first, last, company)
    if base not in _used_emails:
        _used_emails.add(base)
        return base
    parts = base.split("@")
    for i in range(1, 300):
        candidate = f"{parts[0]}{i}@{parts[1]}"
        if candidate not in _used_emails:
            _used_emails.add(candidate)
            return candidate
    raise RuntimeError(f"Cannot generate unique email for {first} {last}")


# ---------------------------------------------------------------------------
# Date utilities
# ---------------------------------------------------------------------------

def _first_of(ym: str) -> str:
    return f"{ym}-01"


def _last_of(ym: str) -> str:
    y, m = map(int, ym.split("-"))
    if m == 12:
        return f"{y}-12-31"
    return (date(y, m + 1, 1) - timedelta(days=1)).isoformat()


def _months_between(start: str, end: str) -> int:
    s = date.fromisoformat(start)
    e = date.fromisoformat(end)
    return max(1, (e.year - s.year) * 12 + (e.month - s.month) + 1)


def _calc_ltv(first_svc: str, last_svc: str, frequency: str, price: float) -> float:
    months = _months_between(first_svc, last_svc)
    visits = FREQ_CFG[frequency]["visits_per_month"]
    return round(months * visits * price, 2)


# ---------------------------------------------------------------------------
# Cancellation reason pool
# ---------------------------------------------------------------------------

_CANCEL_FALLBACKS = [
    "Decided to handle cleaning in-house.",
    "Moving to a different part of town.",
    "Budget constraints required cutting discretionary expenses.",
    "Schedule conflicts made regular appointments difficult.",
    "Switched to a cleaning service closer to new address.",
    "No longer needed after lifestyle change.",
    "Unhappy with the most recent visit.",
    "Found a family friend who cleans for lower cost.",
]

_QUICK_CHURN_FALLBACKS = [
    "Complained about the quality of the first visit.",
    "Stopped responding after initial booking.",
    "One-time clean — no ongoing need.",
]


def _build_cancel_pool() -> List[str]:
    """
    Pre-generate ~8 distinct cancellation reasons using the LLM, falling back
    to hardcoded strings if the API is unavailable.
    """
    types = [
        "residential homeowner",
        "residential renter moving away",
        "residential client on fixed budget",
        "residential client with schedule conflicts",
        "residential client switching to DIY cleaning",
        "residential client dissatisfied with quality",
        "residential senior on fixed income",
        "residential client who hired a private cleaner",
    ]
    pool = []
    try:
        from seeding.utils.text_generator import generate_cancellation_reason
        for t in types:
            try:
                pool.append(generate_cancellation_reason(t))
            except Exception:
                pool.append(_CANCEL_FALLBACKS[len(pool) % len(_CANCEL_FALLBACKS)])
    except ImportError:
        pool = list(_CANCEL_FALLBACKS)
    return pool


def _build_quick_cancel_pool() -> List[str]:
    types = [
        "residential client who complained after first visit",
        "residential client who stopped responding after booking",
        "residential one-time client with no further needs",
    ]
    pool = []
    try:
        from seeding.utils.text_generator import generate_cancellation_reason
        for t in types:
            try:
                pool.append(generate_cancellation_reason(t))
            except Exception:
                pool.append(_QUICK_CHURN_FALLBACKS[len(pool) % len(_QUICK_CHURN_FALLBACKS)])
    except ImportError:
        pool = list(_QUICK_CHURN_FALLBACKS)
    return pool


# ---------------------------------------------------------------------------
# Referral acquisition distribution constants (Issue 2 fix)
# ---------------------------------------------------------------------------

# Referral clients are spread across the full 12-month window:
#   40% acquired Apr–Sep 2025 (pre-program, organic word-of-mouth — 7 "early" clients)
#   60% acquired Oct 2025–Feb 2026 (formal referral program launch Nov 2025)
_REFERRAL_ACQ_MONTHS = [
    "2025-04", "2025-05", "2025-06", "2025-07", "2025-08", "2025-09",
    "2025-10", "2025-11", "2025-12", "2026-01", "2026-02",
]
_REFERRAL_MONTH_WEIGHTS = [
    6, 7, 7, 7, 7, 6,    # Apr–Sep: ~40%
    12, 12, 12, 12, 12,  # Oct–Feb: ~60%
]

# Referral clients skew toward higher-value frequency tiers vs. the overall population.
# Overall:  biweekly 40%, weekly 35%, monthly 25%
# Referral: biweekly 45%, weekly 25%, monthly 30%
# (Matches FREQ_CHOICES = ["biweekly", "weekly", "monthly"])
_REFERRAL_FREQ_WEIGHTS = [45, 25, 30]

# ---------------------------------------------------------------------------
# Client generators
# ---------------------------------------------------------------------------

def _gen_active_recurring(person_offset: int) -> Tuple[List[dict], List[str]]:
    """
    180 active recurring residential clients.

    Strategy:
      - First 18 use acq_month=Jun-2025 and acquisition_source='summer_2025_campaign'.
        Their IDs are returned as campaign_client_ids for lead linking.
      - Remaining 162 follow weighted monthly acquisition distribution.
    """
    acq_months = [
        "2025-04", "2025-05", "2025-06", "2025-07", "2025-08",
        "2025-09", "2025-10", "2025-11", "2025-12", "2026-01", "2026-02",
    ]
    month_weights = [25, 20, 18, 15, 5, 10, 20, 25, 15, 10, 17]

    records: List[dict] = []
    campaign_client_ids: List[str] = []

    # ── 18 summer-campaign clients (Jun 2025) ──────────────────────────
    for i in range(18):
        p = fake_person(person_offset + i)
        acq_date = fake_date_in_range(_first_of("2025-06"), _last_of("2025-06"))
        neighborhood = random.choice(RES_NEIGHBORHOODS)
        crew_id = NEIGHBORHOOD_TO_CREW[neighborhood]
        addr = fake_address(neighborhood)
        frequency = random.choices(FREQ_CHOICES, weights=FREQ_WEIGHTS, k=1)[0]
        price = FREQ_CFG[frequency]["price"]
        client_id = _next_client_id()
        campaign_client_ids.append(client_id)

        records.append({
            "id":                client_id,
            "client_type":       "residential",
            "first_name":        p["first_name"],
            "last_name":         p["last_name"],
            "company_name":      None,
            "email":             _unique_email(p["first_name"], p["last_name"]),
            "phone":             p["phone"],
            "address":           f"{addr['street']}, {addr['city']}, TX {addr['zip']}",
            "neighborhood":      neighborhood,
            "zone":              CREW_ZONE_STRING[crew_id],
            "status":            "active",
            "acquisition_source": "summer_2025_campaign",
            "first_service_date": acq_date,
            "last_service_date":  TODAY,
            "lifetime_value":    _calc_ltv(acq_date, TODAY, frequency, price),
            "notes":             None,
            "_frequency":        frequency,
        })

    # ── 162 regular active recurring ──────────────────────────────────
    # Track how many formal-program referral clients we've tagged (Oct 2025+)
    referral_program_count = 0

    for j in range(162):
        i = 18 + j
        p = fake_person(person_offset + i)

        # Determine acquisition source FIRST so referrals can use different
        # month and frequency distributions (Issue 2 fix).
        acq_source = random.choices(ACQU_SOURCES, weights=ACQU_WEIGHTS, k=1)[0]

        if acq_source == "referral":
            acq_month = random.choices(
                _REFERRAL_ACQ_MONTHS, weights=_REFERRAL_MONTH_WEIGHTS, k=1
            )[0]
            frequency = random.choices(
                FREQ_CHOICES, weights=_REFERRAL_FREQ_WEIGHTS, k=1
            )[0]
        else:
            acq_month = random.choices(acq_months, weights=month_weights, k=1)[0]
            frequency = random.choices(FREQ_CHOICES, weights=FREQ_WEIGHTS, k=1)[0]

        acq_date = fake_date_in_range(_first_of(acq_month), _last_of(acq_month))
        neighborhood = random.choice(RES_NEIGHBORHOODS)
        crew_id = NEIGHBORHOOD_TO_CREW[neighborhood]
        addr = fake_address(neighborhood)
        price = FREQ_CFG[frequency]["price"]
        client_id = _next_client_id()

        # Build notes for referral clients:
        #   - Oct 2025+ referrals: first 15 tagged as formal referral-program participants
        #   - All referrals carry "referral_pending" for post-gen back-link wiring
        if acq_source == "referral":
            is_formal = acq_month >= "2025-10" and referral_program_count < 15
            if is_formal:
                referral_program_count += 1
                notes = "referral_program: True | referral_pending"
            else:
                notes = "referral_pending"
        else:
            notes = None

        records.append({
            "id":                client_id,
            "client_type":       "residential",
            "first_name":        p["first_name"],
            "last_name":         p["last_name"],
            "company_name":      None,
            "email":             _unique_email(p["first_name"], p["last_name"]),
            "phone":             p["phone"],
            "address":           f"{addr['street']}, {addr['city']}, TX {addr['zip']}",
            "neighborhood":      neighborhood,
            "zone":              CREW_ZONE_STRING[crew_id],
            "status":            "active",
            "acquisition_source": acq_source,
            "first_service_date": acq_date,
            "last_service_date":  TODAY,
            "lifetime_value":    _calc_ltv(acq_date, TODAY, frequency, price),
            # referral clients get their referring_client_id wired in post-gen
            "notes":             notes,
            "_frequency":        frequency,
        })

    return records, campaign_client_ids


def _gen_churned_residential(
    person_offset: int,
    cancel_pool: List[str],
) -> List[dict]:
    """
    60 churned residential clients.

    Buckets:
      - 35 churned Aug-Sep 2025 (rough patch)
        * First 5: rate-increase cancellations (Aug 2025)
        * Remaining 30: varied reasons
      - 25 churned Dec 2025 – Jan 2026
    """
    records: List[dict] = []

    # (acq_start, acq_end, churn_start, churn_end, count)
    buckets = [
        ("2025-04", "2025-08", "2025-08", "2025-09", 35),
        ("2025-04", "2025-11", "2025-12", "2026-01", 25),
    ]

    for acq_lo, acq_hi, churn_lo, churn_hi, count in buckets:
        for j in range(count):
            idx = len(records)
            p = fake_person(person_offset + idx)
            acq_date = fake_date_in_range(_first_of(acq_lo), _last_of(acq_hi))
            churn_date = fake_date_in_range(_first_of(churn_lo), _last_of(churn_hi))

            # Guarantee churn is after acquisition
            if churn_date <= acq_date:
                churn_date = (
                    date.fromisoformat(acq_date) + timedelta(days=45)
                ).isoformat()

            neighborhood = random.choice(RES_NEIGHBORHOODS)
            crew_id = NEIGHBORHOOD_TO_CREW[neighborhood]
            addr = fake_address(neighborhood)
            frequency = random.choices(FREQ_CHOICES, weights=FREQ_WEIGHTS, k=1)[0]
            price = FREQ_CFG[frequency]["price"]
            acq_source = random.choices(ACQU_SOURCES, weights=ACQU_WEIGHTS, k=1)[0]

            # First 5 of the Aug-Sep bucket are rate-increase cancellations
            if churn_lo == "2025-08" and j < 5:
                cancel_reason = "Price increase - 5% rate adjustment in August 2025"
                churn_date = fake_date_in_range("2025-08-01", "2025-08-31")
                if churn_date <= acq_date:
                    churn_date = "2025-08-20"
            else:
                cancel_reason = cancel_pool[idx % len(cancel_pool)]

            records.append({
                "id":                _next_client_id(),
                "client_type":       "residential",
                "first_name":        p["first_name"],
                "last_name":         p["last_name"],
                "company_name":      None,
                "email":             _unique_email(p["first_name"], p["last_name"]),
                "phone":             p["phone"],
                "address":           f"{addr['street']}, {addr['city']}, TX {addr['zip']}",
                "neighborhood":      neighborhood,
                "zone":              CREW_ZONE_STRING[crew_id],
                "status":            "churned",
                "acquisition_source": acq_source,
                "first_service_date": acq_date,
                "last_service_date":  churn_date,
                "lifetime_value":    _calc_ltv(acq_date, churn_date, frequency, price),
                "notes":             f"cancellation_reason: {cancel_reason}",
                "_frequency":        frequency,
            })

    # Cap referral clients in the churned cohort at ≤3 to preserve the
    # referral-retention pattern (referral clients should churn less).
    referral_churned = [r for r in records if r["acquisition_source"] == "referral"]
    for r in referral_churned[3:]:
        r["acquisition_source"] = "organic search"

    return records


def _gen_occasional(person_offset: int) -> List[dict]:
    """40 occasional / one-time residential clients."""
    one_time_months = [
        "2025-04", "2025-05", "2025-06", "2025-07", "2025-08",
        "2025-09", "2025-10", "2025-11", "2025-12", "2026-01",
        "2026-02", "2026-03",
    ]
    records: List[dict] = []

    for i in range(40):
        p = fake_person(person_offset + i)
        svc_month = random.choice(one_time_months)
        svc_date = fake_date_in_range(_first_of(svc_month), _last_of(svc_month))
        neighborhood = random.choice(RES_NEIGHBORHOODS)
        crew_id = NEIGHBORHOOD_TO_CREW[neighborhood]
        addr = fake_address(neighborhood)
        acq_source = random.choices(ACQU_SOURCES, weights=ACQU_WEIGHTS, k=1)[0]

        num_jobs = random.randint(1, 3)
        svc_type = random.choice(["deep-clean", "move-in-out"])
        price = 275.00 if svc_type == "deep-clean" else 325.00
        ltv = round(num_jobs * price, 2)

        records.append({
            "id":                _next_client_id(),
            "client_type":       "residential",
            "first_name":        p["first_name"],
            "last_name":         p["last_name"],
            "company_name":      None,
            "email":             _unique_email(p["first_name"], p["last_name"]),
            "phone":             p["phone"],
            "address":           f"{addr['street']}, {addr['city']}, TX {addr['zip']}",
            "neighborhood":      neighborhood,
            "zone":              CREW_ZONE_STRING[crew_id],
            "status":            "occasional",
            "acquisition_source": acq_source,
            "first_service_date": svc_date,
            "last_service_date":  svc_date,
            "lifetime_value":    ltv,
            "notes":             f"service_type: {svc_type} | num_jobs: {num_jobs}",
            "_frequency":        None,
        })

    return records


def _gen_quick_churn(
    person_offset: int,
    quick_cancel_pool: List[str],
) -> List[dict]:
    """30 quick-churn clients: 1 job then gone within 30 days."""
    early_months = [
        "2025-04", "2025-05", "2025-06", "2025-07",
        "2025-08", "2025-09", "2025-10", "2025-11",
    ]
    records: List[dict] = []

    for i in range(30):
        p = fake_person(person_offset + i)
        svc_month = random.choice(early_months)
        svc_date = fake_date_in_range(_first_of(svc_month), _last_of(svc_month))
        days_to_churn = random.randint(3, 30)
        churn_date = (date.fromisoformat(svc_date) + timedelta(days=days_to_churn)).isoformat()

        neighborhood = random.choice(RES_NEIGHBORHOODS)
        crew_id = NEIGHBORHOOD_TO_CREW[neighborhood]
        addr = fake_address(neighborhood)
        acq_source = random.choices(ACQU_SOURCES, weights=ACQU_WEIGHTS, k=1)[0]

        # Every 3rd client has "no reason given"
        reason = (
            "No reason given."
            if i % 3 == 2
            else quick_cancel_pool[i % len(quick_cancel_pool)]
        )
        ltv = random.choice([135.00, 150.00, 165.00, 275.00, 325.00])

        records.append({
            "id":                _next_client_id(),
            "client_type":       "residential",
            "first_name":        p["first_name"],
            "last_name":         p["last_name"],
            "company_name":      None,
            "email":             _unique_email(p["first_name"], p["last_name"]),
            "phone":             p["phone"],
            "address":           f"{addr['street']}, {addr['city']}, TX {addr['zip']}",
            "neighborhood":      neighborhood,
            "zone":              CREW_ZONE_STRING[crew_id],
            "status":            "churned",
            "acquisition_source": acq_source,
            "first_service_date": svc_date,
            "last_service_date":  churn_date,
            "lifetime_value":    ltv,
            "notes":             f"churn_fast: True | cancellation_reason: {reason}",
            "_frequency":        None,
        })

    # Cap referral clients in the quick-churn cohort: together with the regular
    # churned cohort (capped at 3), total referral-churned should stay ≤3.
    # Override extras to "organic search" to avoid inflating referral churn rate.
    referral_quick = [r for r in records if r["acquisition_source"] == "referral"]
    for r in referral_quick:
        r["acquisition_source"] = "organic search"

    return records


def _gen_commercial(person_offset: int) -> List[dict]:
    """10 commercial clients from the hardcoded narrative data."""
    records: List[dict] = []
    comm_acq_sources = ["referral", "direct outreach", "Google Ads"]

    for i, data in enumerate(_COMMERCIAL_CLIENTS):
        p = fake_person(person_offset + i)
        neighborhood = data["neighborhood"]
        crew_id = NEIGHBORHOOD_TO_CREW.get(neighborhood, "crew-d")
        addr = fake_address(neighborhood)
        last_svc = data.get("churn_date", TODAY)
        months = _months_between(data["win_date"], last_svc)
        ltv = round(months * data["monthly_value"], 2)

        notes_parts = [data["extra_notes"]]
        if data["status"] == "churned":
            notes_parts.append(f"churn_date: {data.get('churn_date', '')}")

        client_id = _next_client_id()
        # Register id → company_name so get_commercial_per_visit_rate() works
        # within the same process (gen_financials lazy-loads from DB otherwise).
        _client_id_to_company[client_id] = data["company_name"]

        records.append({
            "id":                client_id,
            "client_type":       "commercial",
            "first_name":        p["first_name"],
            "last_name":         p["last_name"],
            "company_name":      data["company_name"],
            "email":             _unique_email(
                                     p["first_name"], p["last_name"],
                                     data["company_name"],
                                 ),
            "phone":             p["phone"],
            "address":           f"{addr['street']}, {addr['city']}, TX {addr['zip']}",
            "neighborhood":      neighborhood,
            "zone":              CREW_ZONE_STRING.get(crew_id, "North Austin / Round Rock"),
            "status":            data["status"],
            "acquisition_source": random.choice(comm_acq_sources),
            "first_service_date": data["win_date"],
            "last_service_date":  last_svc,
            "lifetime_value":    ltv,
            "notes":             " | ".join(notes_parts),
            "_frequency":        None,
        })

    return records


# ---------------------------------------------------------------------------
# Post-process: wire up referral back-links
# ---------------------------------------------------------------------------

def _resolve_referrals(all_clients: List[dict]) -> None:
    """
    Replace 'referral_pending' token in notes with 'referring_client_id: SS-CLIENT-XXXX'.
    Handles both plain "referral_pending" and embedded occurrences (e.g. in
    "referral_program: True | referral_pending"). Picks a random active client
    (not itself) as the referrer.
    """
    active_ids = [c["id"] for c in all_clients if c["status"] == "active"]
    if not active_ids:
        return

    for client in all_clients:
        notes = client.get("notes") or ""
        if "referral_pending" not in notes:
            continue
        ref_id = random.choice(active_ids)
        # Retry once if self-referral
        if ref_id == client["id"] and len(active_ids) > 1:
            ref_id = random.choice([x for x in active_ids if x != client["id"]])
        client["notes"] = notes.replace("referral_pending", f"referring_client_id: {ref_id}")


# ---------------------------------------------------------------------------
# Lead generators
# ---------------------------------------------------------------------------

def _gen_commercial_leads(person_offset: int) -> List[dict]:
    """
    50 commercial leads.
    Status: 20 new, 13 contacted, 12 qualified (Feb-Mar 2026 proposals), 5 lost.
    Source: 40% referral, 30% direct outreach, 20% website, 10% event.
    """
    statuses = ["new"] * 20 + ["contacted"] * 13 + ["qualified"] * 12 + ["lost"] * 5
    random.shuffle(statuses)

    lead_sources = ["referral", "direct outreach", "website", "event"]
    lead_weights = [40, 30, 20, 10]
    value_choices = [800, 1_000, 1_200, 1_500, 1_800, 2_200, 2_800, 3_200, 4_000, 5_000]

    records: List[dict] = []

    for i, status in enumerate(statuses):
        p = fake_person(person_offset + i)
        biz = fake_business(person_offset + i + 500)

        # All 12 qualified leads are the active Feb-Mar 2026 proposals
        if status == "qualified":
            created_month = random.choice(["2026-02", "2026-03"])
        else:
            created_month = random.choice([
                "2025-04", "2025-05", "2025-06", "2025-07", "2025-08",
                "2025-09", "2025-10", "2025-11", "2025-12", "2026-01",
            ])

        created_date = fake_date_in_range(_first_of(created_month), _last_of(created_month))
        source = random.choices(lead_sources, weights=lead_weights, k=1)[0]

        records.append({
            "id":             _next_lead_id(),
            "first_name":     p["first_name"],
            "last_name":      p["last_name"],
            "company_name":   biz["company_name"],
            "email":          _unique_email(p["first_name"], p["last_name"], biz["company_name"]),
            "phone":          p["phone"],
            "lead_type":      "commercial",
            "source":         source,
            "status":         status,
            "estimated_value": float(random.choice(value_choices)),
            "created_at":     created_date,
            "last_activity_at": created_date,
            "notes":          None,
        })

    return records


def _gen_residential_leads(
    person_offset: int,
    campaign_client_ids: List[str],
) -> List[dict]:
    """
    110 residential leads.

    Summer campaign block (25 leads, Jun 2025):
      - 18 converted: status=qualified, notes include converted_client_id
      -  7 not converted: 4 contacted, 3 lost

    Regular block (85 leads):
      - new: 40, contacted: 31, qualified: 2, lost: 12  → 85
    """
    lead_sources = ["Google Ads", "referral", "organic search", "Yelp", "other"]
    lead_weights = [35, 25, 20, 10, 10]
    records: List[dict] = []

    # ── 25 summer campaign leads ──────────────────────────────────────
    # Build the 25 slots explicitly so every converted lead gets a client_id
    # regardless of shuffle order.
    campaign_slots = (
        [{"status": "qualified", "converted_id": cid} for cid in campaign_client_ids[:18]]
        + [{"status": "contacted", "converted_id": None}] * 4
        + [{"status": "lost",      "converted_id": None}] * 3
    )
    random.shuffle(campaign_slots)

    for i, slot in enumerate(campaign_slots):
        p = fake_person(person_offset + i)
        created_date = fake_date_in_range(_first_of("2025-06"), _last_of("2025-06"))

        notes_parts = ["campaign_source: summer_2025_campaign"]
        if slot["converted_id"]:
            notes_parts.append(f"converted_client_id: {slot['converted_id']}")

        records.append({
            "id":             _next_lead_id(),
            "first_name":     p["first_name"],
            "last_name":      p["last_name"],
            "company_name":   None,
            "email":          _unique_email(p["first_name"], p["last_name"]),
            "phone":          p["phone"],
            "lead_type":      "residential",
            "source":         "summer_2025_campaign",
            "status":         slot["status"],
            "estimated_value": float(random.choice([135, 150, 165]) * random.randint(6, 24)),
            "created_at":     created_date,
            "last_activity_at": created_date,
            "notes":          " | ".join(notes_parts),
        })

    # ── 85 regular residential leads ─────────────────────────────────
    regular_statuses = (
        ["new"] * 40 +
        ["contacted"] * 31 +
        ["qualified"] * 2 +
        ["lost"] * 12
    )
    random.shuffle(regular_statuses)

    all_months = [
        "2025-04", "2025-05", "2025-06", "2025-07", "2025-08",
        "2025-09", "2025-10", "2025-11", "2025-12", "2026-01",
        "2026-02", "2026-03",
    ]

    for j, status in enumerate(regular_statuses):
        i = 25 + j
        p = fake_person(person_offset + i)
        created_month = random.choice(all_months)
        created_date = fake_date_in_range(_first_of(created_month), _last_of(created_month))
        source = random.choices(lead_sources, weights=lead_weights, k=1)[0]

        records.append({
            "id":             _next_lead_id(),
            "first_name":     p["first_name"],
            "last_name":      p["last_name"],
            "company_name":   None,
            "email":          _unique_email(p["first_name"], p["last_name"]),
            "phone":          p["phone"],
            "lead_type":      "residential",
            "source":         source,
            "status":         status,
            "estimated_value": float(random.choice([135, 150, 165]) * random.randint(6, 24)),
            "created_at":     created_date,
            "last_activity_at": created_date,
            "notes":          None,
        })

    return records


# ---------------------------------------------------------------------------
# Database writes
# ---------------------------------------------------------------------------

_CLIENT_COLS = (
    "id", "client_type", "first_name", "last_name", "company_name",
    "email", "phone", "address", "neighborhood", "zone", "status",
    "acquisition_source", "first_service_date", "last_service_date",
    "lifetime_value", "notes", "created_at",
)

_LEAD_COLS = (
    "id", "first_name", "last_name", "company_name", "email", "phone",
    "lead_type", "source", "status", "estimated_value",
    "created_at", "last_activity_at", "notes",
)


def _ensure_leads_notes_col(conn: sqlite3.Connection) -> None:
    """Add notes column to leads if the schema predates this generator."""
    try:
        conn.execute("ALTER TABLE leads ADD COLUMN notes TEXT")
        conn.commit()
    except sqlite3.OperationalError:
        pass  # already exists


def _insert_clients(conn: sqlite3.Connection, clients: List[dict]) -> None:
    cols = ", ".join(_CLIENT_COLS)
    placeholders = ", ".join("?" * len(_CLIENT_COLS))
    rows = []
    for c in clients:
        rows.append(tuple(
            c.get(col) if col != "created_at"
            else f"{c['first_service_date']} 08:00:00"
            for col in _CLIENT_COLS
        ))
    with conn:
        conn.executemany(
            f"INSERT OR IGNORE INTO clients ({cols}) VALUES ({placeholders})",
            rows,
        )


def _insert_leads(conn: sqlite3.Connection, leads: List[dict]) -> None:
    cols = ", ".join(_LEAD_COLS)
    placeholders = ", ".join("?" * len(_LEAD_COLS))
    rows = [
        tuple(lead.get(col) for col in _LEAD_COLS)
        for lead in leads
    ]
    with conn:
        conn.executemany(
            f"INSERT OR IGNORE INTO leads ({cols}) VALUES ({placeholders})",
            rows,
        )


# ---------------------------------------------------------------------------
# Summary printer
# ---------------------------------------------------------------------------

def _print_summary(
    all_clients: List[dict],
    all_leads: List[dict],
) -> None:
    status_counts = Counter(c["status"] for c in all_clients)
    type_counts   = Counter(c["client_type"] for c in all_clients)
    lead_type_counts = Counter(l["lead_type"] for l in all_leads)
    src_counts    = Counter(c["acquisition_source"] for c in all_clients)
    neighborhoods = sorted({c["neighborhood"] for c in all_clients if c.get("neighborhood")})

    active_res = [
        c for c in all_clients
        if c["status"] == "active" and c["client_type"] == "residential"
    ]
    avg_ltv = (
        sum(c["lifetime_value"] for c in active_res) / len(active_res)
        if active_res else 0.0
    )

    print()
    print("=" * 60)
    print("  SPARKLE & SHINE — CLIENT GENERATION SUMMARY")
    print("=" * 60)
    print(f"  Total clients     : {len(all_clients)}")
    print(f"    active          : {status_counts.get('active', 0)}")
    print(f"    churned         : {status_counts.get('churned', 0)}")
    print(f"    occasional      : {status_counts.get('occasional', 0)}")
    print(f"  Residential       : {type_counts.get('residential', 0)}")
    print(f"  Commercial        : {type_counts.get('commercial', 0)}")
    print()
    print(f"  Total leads       : {len(all_leads)}")
    print(f"    commercial      : {lead_type_counts.get('commercial', 0)}")
    print(f"    residential     : {lead_type_counts.get('residential', 0)}")
    print()
    print(f"  Neighborhoods covered ({len(neighborhoods)}):")
    for n in neighborhoods:
        print(f"    • {n}")
    print()
    print("  Acquisition source breakdown:")
    for src, cnt in sorted(src_counts.items(), key=lambda x: -x[1]):
        print(f"    {src:<25} {cnt:>4}")
    print()
    print(f"  Avg lifetime value (active residential): ${avg_ltv:,.2f}")
    print("=" * 60)
    print()


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main() -> None:
    # Ensure DB and schema exist
    init_db(DB_PATH)

    conn = get_connection(DB_PATH)
    _ensure_leads_notes_col(conn)
    _init_counters(conn)
    conn.close()

    print("Building cancellation reason pools (may call LLM)...")
    cancel_pool = _build_cancel_pool()
    quick_cancel_pool = _build_quick_cancel_pool()

    print("Generating residential clients...")
    active_recurring, campaign_client_ids = _gen_active_recurring(person_offset=0)
    churned = _gen_churned_residential(person_offset=200, cancel_pool=cancel_pool)
    occasional = _gen_occasional(person_offset=300)
    quick_churn = _gen_quick_churn(person_offset=400, quick_cancel_pool=quick_cancel_pool)

    print("Generating commercial clients...")
    commercial = _gen_commercial(person_offset=500)

    all_clients = active_recurring + churned + occasional + quick_churn + commercial

    print(f"  Resolving {sum(1 for c in all_clients if c.get('notes') == 'referral_pending')} referral back-links...")
    _resolve_referrals(all_clients)

    print("Generating leads...")
    commercial_leads = _gen_commercial_leads(person_offset=600)
    residential_leads = _gen_residential_leads(
        person_offset=700,
        campaign_client_ids=campaign_client_ids,
    )
    all_leads = commercial_leads + residential_leads

    print(f"Writing {len(all_clients)} clients and {len(all_leads)} leads to DB...")
    conn = get_connection(DB_PATH)
    _insert_clients(conn, all_clients)
    _insert_leads(conn, all_leads)
    conn.close()

    print("Registering cross_tool_mapping (tool_name='local')...")
    mapping_rows = (
        [(c["id"], "local", c["id"]) for c in all_clients]
        + [(l["id"], "local", l["id"]) for l in all_leads]
    )
    bulk_register(mapping_rows, DB_PATH)

    _print_summary(all_clients, all_leads)


if __name__ == "__main__":
    main()
