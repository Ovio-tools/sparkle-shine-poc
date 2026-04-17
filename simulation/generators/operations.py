"""
simulation/generators/operations.py

Three simulation generators for operations:
  NewClientSetupGenerator  — Type 1: creates Jobber client + schedule for won deals
  JobSchedulingGenerator   — Type 2: creates today's jobs for active recurring clients
  JobCompletionGenerator   — Type 2: fires at realistic times, records outcomes + reviews
"""
from __future__ import annotations

import calendar
import heapq
import random
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Callable, Optional

from auth import get_client
from config.business import SERVICE_TYPES
from database.connection import get_connection, get_column_names, date_subtract_sql
from database.mappings import generate_id, get_tool_id, get_tool_url, register_mapping
from intelligence.logging_config import setup_logging
from seeding.utils.throttler import JOBBER as throttler
from simulation.config import CREW_CAPACITY, DAILY_VOLUMES, JOB_VARIETY

logger = setup_logging("simulation.operations")


# ── Result type ───────────────────────────────────────────────────────────────

@dataclass
class GeneratorResult:
    success: bool
    message: str = ""


# ── Module-level constants ────────────────────────────────────────────────────

_JOBBER_GQL_URL = "https://api.getjobber.com/api/graphql"
_JOBBER_VERSION_HEADER = {"X-JOBBER-GRAPHQL-VERSION": "2026-03-10"}

_CREW_WINDOW = {
    "crew-a": time(7, 0),
    "crew-b": time(7, 30),
    "crew-c": time(8, 0),
    "crew-d": time(17, 0),
}

_FREQ_TO_RECUR_FREQ = {
    "weekly_recurring":   "weekly",
    "biweekly_recurring": "biweekly",
    "monthly_recurring":  "monthly",
    "nightly_clean":      "weekly",
    "weekend_deep_clean": "weekly",
}

_FREQ_TO_SERVICE_ID = {
    "weekly_recurring":     "recurring-weekly",
    "biweekly_recurring":   "recurring-biweekly",
    "monthly_recurring":    "recurring-monthly",
    "nightly_clean":        "commercial-nightly",
    "weekend_deep_clean":   "deep-clean",
    "one_time_standard":    "std-residential",
    "one_time_deep_clean":  "deep-clean",
    "one_time_move_in_out": "move-in-out",
    "one_time_project":     "commercial-nightly",
}

_DURATION_MAP = {st["id"]: st["duration_minutes"] for st in SERVICE_TYPES}

_JOBBER_FREQ_MAP = {
    "weekly":   {"type": "WEEKLY",  "interval": 1},
    "biweekly": {"type": "WEEKLY",  "interval": 2},
    "monthly":  {"type": "MONTHLY", "interval": 1},
}

_DOW_NAMES = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]

_ONE_TIME_FREQS = {
    "one_time_standard", "one_time_deep_clean",
    "one_time_move_in_out", "one_time_project",
}

_COMMERCIAL_SCOPE_DAYS = {
    "nightly": {0, 1, 2, 3, 4},
    "5x weekly": {0, 1, 2, 3, 4},
    "daily": {0, 1, 2, 3, 4, 5},
    "3x weekly": {0, 2, 4},
    "2x weekly": {1, 3},
}


def _next_review_id(conn) -> str:
    """Return the next numeric review ID, ignoring malformed legacy values."""
    max_review_num = 0
    for row in conn.execute("SELECT id FROM reviews").fetchall():
        review_id = str(row["id"] or "")
        if not review_id.startswith("SS-REV-"):
            continue
        suffix = review_id.split("-")[-1]
        if suffix.isdigit():
            max_review_num = max(max_review_num, int(suffix))
    return f"SS-REV-{max_review_num + 1:04d}"

# ── GraphQL mutation strings ──────────────────────────────────────────────────

_CLIENT_CREATE = """
mutation ClientCreate($input: ClientCreateInput!) {
  clientCreate(input: $input) {
    client { id firstName lastName companyName }
    userErrors { message path }
  }
}
"""

_PROPERTY_CREATE = """
mutation PropertyCreate($clientId: EncodedId!, $input: PropertyCreateInput!) {
  propertyCreate(clientId: $clientId, input: $input) {
    properties { id }
    userErrors { message path }
  }
}
"""

_JOB_CREATE = """
mutation JobCreate($input: JobCreateAttributes!) {
  jobCreate(input: $input) {
    job { id title jobStatus }
    userErrors { message path }
  }
}
"""

_JOB_CLOSE = """
mutation CloseJob($jobId: EncodedId!, $input: JobCloseInput!) {
  jobClose(jobId: $jobId, input: $input) {
    job { id jobStatus }
    userErrors { message path }
  }
}
"""

_CLIENT_PROPERTIES_QUERY = """
query ClientProperties($id: EncodedId!) {
  client(id: $id) {
    id
    clientProperties { nodes { id } }
  }
}
"""

_JOB_CREATE_ATTRS_QUERY = """
query JobCreateAttrs {
  __type(name: "JobCreateAttributes") {
    inputFields { name }
  }
}
"""


# ── Module-level helpers ──────────────────────────────────────────────────────

def _gql(session, query: str, variables: dict) -> dict:
    """Execute a Jobber GraphQL call and return the parsed response dict."""
    throttler.wait()
    resp = session.post(
        _JOBBER_GQL_URL,
        json={"query": query, "variables": variables},
        headers=_JOBBER_VERSION_HEADER,
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    top_errors = data.get("errors", [])
    if top_errors:
        raise RuntimeError(f"Jobber GQL top-level errors: {top_errors}")
    return data


def _gql_user_errors(data: dict, mutation_key: str) -> list:
    """Extract userErrors from a Jobber mutation response."""
    return (
        data.get("data", {}).get(mutation_key, {}).get("userErrors", [])
        or data.get("errors", [])
    )


def _add_business_days(d: date, n: int) -> date:
    """Return d + n business days (skipping Saturday=5, Sunday=6)."""
    result = d
    while n > 0:
        result += timedelta(days=1)
        if result.weekday() < 5:
            n -= 1
    return result


def _expected_duration(service_type_id: str, job_type: str) -> int:
    """Return expected job duration in minutes.

    Source: config/business.py SERVICE_TYPES[n]["duration_minutes"].
    deep_clean uses the deep-clean service type duration (210 min) regardless
    of the base service_type_id. Default 120 if service_type_id not found.
    """
    if job_type == "deep_clean":
        return _DURATION_MAP.get("deep-clean", 210)
    return _DURATION_MAP.get(service_type_id, 120)


def _adjusted_rating_distribution(crew_name: str, day_of_week: int) -> list[tuple[int, float]]:
    """Return rating distribution adjusted for crew and day-of-week.

    Distributions (5★, 4★, 3★, 2★, 1★):
      Base:             60%, 25%, 10%, 4%, 1%
      Crew A only:      70%, 20%, 7%, 2.5%, 0.5%
      Tue/Wed only:     65%, 23%, 8%, 3%, 1%
      Crew A + Tue/Wed: 75%, 17%, 5%, 2.5%, 0.5%

    Cap: 5★ never exceeds 80%; excess redistributed into 4★.

    Args:
        crew_name: e.g. "Crew A" (matches clients.crew_assignment values)
        day_of_week: 0=Monday … 6=Sunday

    Returns:
        List of (rating, weight) tuples, weights summing to 1.0.
    """
    is_crew_a = (crew_name == "Crew A")
    is_tue_wed = (day_of_week in (1, 2))  # Tuesday=1, Wednesday=2

    if is_crew_a and is_tue_wed:
        dist = {5: 0.75, 4: 0.17, 3: 0.05, 2: 0.025, 1: 0.005}
    elif is_crew_a:
        dist = {5: 0.70, 4: 0.20, 3: 0.07, 2: 0.025, 1: 0.005}
    elif is_tue_wed:
        dist = {5: 0.65, 4: 0.23, 3: 0.08, 2: 0.03, 1: 0.01}
    else:
        dist = {5: 0.60, 4: 0.25, 3: 0.10, 2: 0.04, 1: 0.01}

    # Cap 5★ at 80%, redistribute excess into 4★
    if dist[5] > 0.80:
        excess = dist[5] - 0.80
        dist[5] = 0.80
        dist[4] = round(dist[4] + excess, 6)

    return sorted(dist.items(), reverse=True)


def _is_due_today(agreement: dict, today: date) -> bool:
    """Return True if a recurring agreement has a job due on `today`.

    Args:
        agreement: dict with keys: start_date (ISO str), day_of_week (str or None),
                   frequency ('weekly'|'biweekly'|'monthly')
        today: date to check

    `day_of_week` may be a single weekday name ('monday') or a comma-separated
    list ('monday,wednesday,friday') for multi-day schedules used by commercial
    recurring agreements. Whitespace around names is tolerated.
    """
    start = date.fromisoformat(agreement["start_date"])
    freq = agreement["frequency"]

    _DOW_MAP = {
        "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
        "friday": 4, "saturday": 5, "sunday": 6,
    }

    raw_dow = (agreement.get("day_of_week") or "").lower()
    if raw_dow:
        day_set = {
            _DOW_MAP[d.strip()]
            for d in raw_dow.split(",")
            if d.strip() in _DOW_MAP
        }
        if not day_set:
            day_set = {start.weekday()}
    else:
        day_set = {start.weekday()}

    if freq == "weekly":
        return today.weekday() in day_set

    elif freq == "biweekly":
        return (
            today.weekday() in day_set
            and (today - start).days % 14 < 7
        )

    elif freq == "monthly":
        days_in_month = calendar.monthrange(today.year, today.month)[1]
        due_day = min(start.day, days_in_month)
        return today.day == due_day

    return False


def _pick_job_type(agreement: dict, today: date) -> str:
    """Return job type string for a given recurring agreement and date.

    Residential recurring: 'regular', 'deep_clean', or 'add_on'
    Commercial recurring: 'standard' or 'extra_service'

    Source: simulation/config.py JOB_VARIETY
    """
    is_commercial = (agreement.get("client_type") == "commercial")

    if is_commercial:
        cfg = JOB_VARIETY["commercial_recurring"]
        if random.random() < cfg["extra_service_rate"]:
            return "extra_service"
        return "standard"
    else:
        cfg = JOB_VARIETY["residential_recurring"]
        month = today.month
        boost = cfg["seasonal_deep_clean_boost"].get(month, 1.0)
        if random.random() < cfg["deep_clean_rate"] * boost:
            return "deep_clean"
        if random.random() < cfg["add_on_rate"]:
            return "add_on"
        return "regular"


def _assign_scheduled_time(crew_id: str, prior_jobs: list[dict], today: date) -> datetime:
    """Return the start datetime for the next job in a crew's route.

    Jobs are assigned sequentially: window_start + sum of (prior durations +
    travel buffers). First job in the day gets the window_start exactly.
    Travel buffer per job: random.randint(15, 30) minutes.

    Args:
        crew_id: e.g. "crew-a" (matches recurring_agreements.crew_id)
        prior_jobs: list of dicts, each with key "expected_duration" (int, minutes)
        today: the date being scheduled

    Returns:
        datetime for the job's scheduled start.
    """
    window_time = _CREW_WINDOW.get(crew_id, time(7, 0))
    base = datetime.combine(today, window_time)
    if not prior_jobs:
        return base
    offset = sum(
        j["expected_duration"] + random.randint(15, 30)
        for j in prior_jobs
    )
    return base + timedelta(minutes=offset)


def _commercial_scope(notes: str | None) -> str:
    """Map free-form commercial notes to a scheduling cadence.

    Blank notes fall back to 3x weekly to match the seeding model and avoid
    silently dropping active commercial accounts from the daily schedule.
    """
    normalized = (notes or "").lower()
    if "nightly" in normalized:
        return "nightly"
    if "5x weekly" in normalized:
        return "5x weekly"
    if "daily" in normalized:
        return "daily"
    if "3x weekly" in normalized:
        return "3x weekly"
    if "2x weekly" in normalized:
        return "2x weekly"
    return "3x weekly"


def _fillin_candidates(
    conn,
    *,
    today_iso: str,
    limit: int,
    preferred_zone: str | None = None,
    client_group: str = "residential",
) -> list[dict]:
    """Return fill-in candidates ordered by route fit and service staleness."""
    if client_group == "commercial":
        type_filter = "c.client_type = 'commercial'"
    else:
        type_filter = "c.client_type IN ('residential', 'one-time')"

    if preferred_zone:
        rows = conn.execute(
            f"""
            SELECT c.id AS client_id,
                   c.zone,
                   c.last_service_date,
                   CASE
                       WHEN COALESCE(NULLIF(c.zone, ''), '') = %s THEN 0
                       WHEN COALESCE(NULLIF(c.zone, ''), '') = '' THEN 1
                       ELSE 2
                   END AS zone_rank
            FROM clients c
            WHERE c.status IN ('active', 'occasional')
              AND {type_filter}
              AND c.id NOT IN (
                  SELECT j.client_id FROM jobs j
                  WHERE j.scheduled_date = %s
              )
            ORDER BY zone_rank, c.last_service_date ASC NULLS FIRST, c.id ASC
            LIMIT %s
            """,
            (preferred_zone, today_iso, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            f"""
            SELECT c.id AS client_id, c.zone, c.last_service_date
            FROM clients c
            WHERE c.status IN ('active', 'occasional')
              AND {type_filter}
              AND c.id NOT IN (
                  SELECT j.client_id FROM jobs j
                  WHERE j.scheduled_date = %s
              )
            ORDER BY c.last_service_date ASC NULLS FIRST, c.id ASC
            LIMIT %s
            """,
            (today_iso, limit),
        ).fetchall()
    return [dict(row) for row in rows]


# ── Recurrence field discovery (cached) ──────────────────────────────────────

_recurrence_field_cache: Optional[str] = None
_recurrence_field_checked: bool = False


def _get_recurrence_field(session) -> Optional[str]:
    """Introspect Jobber schema to find the recurrence input field name on JobCreateAttributes.

    Returns the field name (e.g. 'recurrences') or None if not found.
    Result is cached for the lifetime of the process.
    """
    global _recurrence_field_cache, _recurrence_field_checked
    if _recurrence_field_checked:
        return _recurrence_field_cache
    try:
        data = _gql(session, _JOB_CREATE_ATTRS_QUERY, {})
        fields = (
            data.get("data", {})
            .get("__type", {})
            .get("inputFields", [])
        )
        field_names = {f["name"] for f in fields}
        for candidate in ("recurrences", "repeat", "schedule", "recurrence"):
            if candidate in field_names:
                _recurrence_field_cache = candidate
                break
        _recurrence_field_checked = True
    except Exception:
        logger.warning("Failed to introspect Jobber recurrence field; will retry next call")
    return _recurrence_field_cache


# ── Property ID helper (lazy fetch + register) ────────────────────────────────

def _get_or_fetch_property_id(canonical_id: str, session, db_path: str) -> Optional[str]:
    """Return the Jobber property ID for a client canonical_id.

    First checks cross_tool_mapping for a 'jobber_property' entry.
    If absent, queries Jobber via ClientProperties and registers the result.
    Returns None if the client has no Jobber mapping or no properties.
    """
    # Fast path 1: already registered as a dedicated jobber_property entry
    prop_id = get_tool_id(canonical_id, "jobber_property", db_path=db_path)
    if prop_id:
        return prop_id

    # Fast path 2: seeder stored the property ID in tool_specific_url on the
    # jobber row — check there before making an API call
    seeder_prop_id = get_tool_url(canonical_id, "jobber", db_path=db_path)
    if seeder_prop_id:
        register_mapping(canonical_id, "jobber_property", seeder_prop_id, db_path=db_path)
        return seeder_prop_id

    # Slow path: query Jobber API
    jobber_client_id = get_tool_id(canonical_id, "jobber", db_path=db_path)
    if not jobber_client_id:
        return None

    try:
        data = _gql(session, _CLIENT_PROPERTIES_QUERY, {"id": jobber_client_id})
        nodes = (
            data.get("data", {})
            .get("client", {})
            .get("clientProperties", {})
            .get("nodes", [])
        )
        if not nodes:
            return None
        prop_id = nodes[0]["id"]
        register_mapping(canonical_id, "jobber_property", prop_id, db_path=db_path)
        return prop_id
    except Exception:
        return None


# ── NewClientSetupGenerator ───────────────────────────────────────────────────

class NewClientSetupGenerator:
    """Type 1 generator: creates Jobber client + schedule for won deals.

    Reads won_deals table for deals whose start_date <= today with no Jobber
    mapping yet. Processes all ready deals in one execute() call. Each client
    is wrapped in its own try/except so a single failure does not abort the rest.
    """

    def __init__(self, db_path: str = "sparkle_shine.db"):
        self.db_path = db_path

    def _ensure_schema(self, conn) -> None:
        """Add client_type column to recurring_agreements if not present."""
        cols = get_column_names(conn, "recurring_agreements")
        if "client_type" not in cols:
            conn.execute(
                "ALTER TABLE recurring_agreements ADD COLUMN client_type TEXT DEFAULT 'residential'"
            )
            conn.commit()

    def execute(self, dry_run: bool = False) -> GeneratorResult:
        conn = get_connection()
        try:
            self._ensure_schema(conn)
            ready_deals = conn.execute("""
                SELECT * FROM won_deals
                WHERE start_date::date <= CURRENT_DATE
                  AND canonical_id NOT IN (
                      SELECT canonical_id FROM cross_tool_mapping
                      WHERE tool_name = 'jobber'
                  )
            """).fetchall()
            ready_deals = [dict(r) for r in ready_deals]

            if not ready_deals:
                return GeneratorResult(success=True, message="no new clients ready")

            session = get_client("jobber") if not dry_run else None
            results = []

            for deal in ready_deals:
                try:
                    if dry_run:
                        results.append(("ok", deal["canonical_id"]))
                        continue
                    conn.execute(f"SAVEPOINT deal_{deal['canonical_id'].replace('-', '_')}")
                    self._setup_client(deal, session, conn)
                    conn.execute(f"RELEASE SAVEPOINT deal_{deal['canonical_id'].replace('-', '_')}")
                    results.append(("ok", deal["canonical_id"]))
                except Exception as e:
                    conn.execute(f"ROLLBACK TO SAVEPOINT deal_{deal['canonical_id'].replace('-', '_')}")
                    conn.execute(f"RELEASE SAVEPOINT deal_{deal['canonical_id'].replace('-', '_')}")
                    logger.exception("Setup failed for %s", deal["canonical_id"])
                    results.append(("failed", deal["canonical_id"], str(e)))

            conn.commit()
            succeeded = sum(1 for r in results if r[0] == "ok")
            failed = len(results) - succeeded
            if failed:
                return GeneratorResult(
                    success=False,
                    message=f"setup {succeeded} clients, {failed} failed",
                )
            return GeneratorResult(success=True, message=f"setup {succeeded} new clients")

        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _setup_client(self, deal: dict, session, conn) -> None:
        """Create a Jobber client + schedule for one won deal."""
        canonical_id = deal["canonical_id"]
        client_type = deal["client_type"]
        service_frequency = deal["service_frequency"]
        contract_value = deal["contract_value"] or 0.0
        start_date = date.fromisoformat(deal["start_date"])
        crew_assignment = deal["crew_assignment"] or "Crew A"
        pipedrive_deal_id = deal.get("pipedrive_deal_id")

        # 1. Look up client info from the appropriate table
        if client_type == "commercial":
            row = conn.execute(
                "SELECT * FROM clients WHERE id = %s", (canonical_id,)
            ).fetchone()
            if row is None:
                raise RuntimeError(f"No clients row for {canonical_id}")
            row = dict(row)
            name = row.get("company_name") or f"{row.get('first_name','')} {row.get('last_name','')}".strip()
            email = row.get("email", "")
            phone = row.get("phone", "")
            address = row.get("address", "Austin, TX")
        else:
            row = conn.execute(
                "SELECT * FROM leads WHERE id = %s", (canonical_id,)
            ).fetchone()
            if row is None:
                raise RuntimeError(f"No leads row for {canonical_id}")
            row = dict(row)
            name = f"{row.get('first_name','')} {row.get('last_name','')}".strip()
            email = row.get("email", "")
            phone = row.get("phone", "")
            address = "Austin, TX"  # leads table has no address column

        # 2. clientCreate → jobber_client_id
        client_input: dict = {}
        if client_type == "residential":
            parts = name.split(" ", 1)
            client_input["firstName"] = parts[0]
            client_input["lastName"] = parts[1] if len(parts) > 1 else ""
        else:
            client_input["companyName"] = name
        if email:
            client_input["emails"] = [{"description": "MAIN", "address": email}]
        if phone:
            client_input["phones"] = [{"description": "MAIN", "number": phone}]
        if address:
            addr_parts = [p.strip() for p in address.split(",")]
            # Residential fallback is "Austin, TX" — no real street number.
            # Use the full address string as street1 only when it looks like
            # a real street (more than 2 parts or starts with a digit).
            if len(addr_parts) >= 3 or (addr_parts and addr_parts[0][:1].isdigit()):
                client_input["billingAddress"] = {
                    "street1": addr_parts[0],
                    "city": addr_parts[1] if len(addr_parts) > 1 else "Austin",
                    "province": "TX",
                    "country": "US",
                }
            else:
                # City-only fallback: don't set a street, just city
                city = addr_parts[0] if addr_parts else "Austin"
                client_input["billingAddress"] = {
                    "city": city,
                    "province": "TX",
                    "country": "US",
                }

        resp = _gql(session, _CLIENT_CREATE, {"input": client_input})
        errs = _gql_user_errors(resp, "clientCreate")
        if errs:
            raise RuntimeError(f"clientCreate errors: {errs}")
        jobber_client_id = resp["data"]["clientCreate"]["client"]["id"]

        # 3. Register client mapping
        register_mapping(canonical_id, "jobber", jobber_client_id, db_path=self.db_path)

        # 4. propertyCreate → jobber_property_id
        prop_parts = [p.strip() for p in address.split(",")]
        if len(prop_parts) >= 3 or (prop_parts and prop_parts[0][:1].isdigit()):
            prop_addr = {
                "street1": prop_parts[0],
                "city": prop_parts[1] if len(prop_parts) > 1 else "Austin",
                "province": "TX",
                "country": "US",
            }
        else:
            prop_addr = {
                "city": prop_parts[0] if prop_parts else "Austin",
                "province": "TX",
                "country": "US",
            }
        prop_input = {
            "properties": [{"address": prop_addr}]
        }
        resp2 = _gql(session, _PROPERTY_CREATE,
                     {"clientId": jobber_client_id, "input": prop_input})
        errs2 = _gql_user_errors(resp2, "propertyCreate")
        if errs2:
            raise RuntimeError(f"propertyCreate errors: {errs2}")
        props = resp2["data"]["propertyCreate"]["properties"]
        jobber_property_id = props[0]["id"] if props else None
        if jobber_property_id:
            register_mapping(canonical_id, "jobber_property", jobber_property_id,
                             db_path=self.db_path)

        # 5. Branch: recurring vs one-time
        is_one_time = service_frequency in _ONE_TIME_FREQS
        service_type_id = _FREQ_TO_SERVICE_ID.get(service_frequency, "std-residential")
        crew_id = "crew-" + crew_assignment.lower().replace("crew ", "")

        if is_one_time:
            self._setup_one_time(
                deal, canonical_id, service_type_id, service_frequency,
                start_date, crew_id, contract_value, jobber_property_id, session, conn,
            )
        else:
            self._setup_recurring(
                deal, canonical_id, service_type_id, service_frequency,
                start_date, crew_id, contract_value, jobber_property_id, session, conn,
            )

        # 6. Pipedrive activity note
        if pipedrive_deal_id:
            try:
                pd_session = get_client("pipedrive")
                pd_session.post(
                    "https://api.pipedrive.com/v1/activities",
                    json={
                        "subject": f"SS-ID: {canonical_id} — Jobber client created",
                        "deal_id": pipedrive_deal_id,
                        "type": "task",
                        "done": 1,
                        "note": (
                            f"SS-ID: {canonical_id} — Jobber client created, "
                            f"{service_frequency} schedule initialized"
                        ),
                    },
                    timeout=15,
                )
            except Exception:
                logger.warning("Pipedrive activity failed for %s", canonical_id)

    def _setup_recurring(
        self, deal, canonical_id, service_type_id, service_frequency,
        start_date, crew_id, contract_value, jobber_property_id, session, conn,
    ) -> None:
        recur_frequency = _FREQ_TO_RECUR_FREQ[service_frequency]
        recur_field = _get_recurrence_field(session)

        # Determine day_of_week: first available weekday on or after start_date
        day_of_week_int = start_date.weekday()  # 0=Mon … 4=Fri; clamp to weekday
        if day_of_week_int >= 5:  # Sat or Sun → move to Monday
            day_of_week_int = 0
            start_date = start_date + timedelta(days=(7 - start_date.weekday()))
        day_of_week_name = _DOW_NAMES[day_of_week_int]

        # jobCreate with recurrence
        job_input: dict = {
            "propertyId": jobber_property_id or "",
            "title": f"Recurring: {service_type_id.replace('-', ' ').title()}",
            "invoicing": {
                "invoicingType": "FIXED_PRICE",
                "invoicingSchedule": "ON_COMPLETION",
            },
            "timeframe": {"startAt": start_date.isoformat()},
        }
        freq_config = _JOBBER_FREQ_MAP.get(recur_frequency)
        if recur_field and freq_config:
            job_input[recur_field] = freq_config

        resp = _gql(session, _JOB_CREATE, {"input": job_input})
        errs = _gql_user_errors(resp, "jobCreate")
        if errs:
            raise RuntimeError(f"jobCreate (recurring) errors: {errs}")
        jobber_recurrence_id = resp["data"]["jobCreate"]["job"]["id"]

        # Insert into recurring_agreements
        recur_id = generate_id("RECUR", db_path=self.db_path)
        conn.execute("""
            INSERT INTO recurring_agreements
            (id, client_id, service_type_id, crew_id, frequency,
             price_per_visit, start_date, status, day_of_week, client_type)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
        """, (
            recur_id, canonical_id, service_type_id, crew_id,
            recur_frequency, contract_value,
            start_date.isoformat(), "active", day_of_week_name,
            deal.get("client_type", "residential"),
        ))
        register_mapping(recur_id, "jobber", jobber_recurrence_id, db_path=self.db_path)

    def _setup_one_time(
        self, deal, canonical_id, service_type_id, service_frequency,
        start_date, crew_id, contract_value, jobber_property_id, session, conn,
    ) -> None:
        job_input: dict = {
            "propertyId": jobber_property_id or "",
            "title": service_type_id.replace("-", " ").title(),
            "invoicing": {
                "invoicingType": "FIXED_PRICE",
                "invoicingSchedule": "ON_COMPLETION",
            },
            "timeframe": {"startAt": start_date.isoformat()},
        }
        resp = _gql(session, _JOB_CREATE, {"input": job_input})
        errs = _gql_user_errors(resp, "jobCreate")
        if errs:
            raise RuntimeError(f"jobCreate (one-time) errors: {errs}")
        jobber_job_id = resp["data"]["jobCreate"]["job"]["id"]

        job_id = generate_id("JOB", db_path=self.db_path)
        conn.execute("""
            INSERT INTO jobs
            (id, client_id, crew_id, service_type_id, scheduled_date, status)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
        """, (
            job_id, canonical_id, crew_id, service_type_id,
            start_date.isoformat(), "scheduled",
        ))
        register_mapping(job_id, "jobber", jobber_job_id, db_path=self.db_path)


# ── JobSchedulingGenerator ────────────────────────────────────────────────────

class JobSchedulingGenerator:
    """Type 2 generator: creates today's jobs for active recurring clients.

    Run once per day (placed second in plan_day before shuffle).
    Also picks up already-scheduled jobs (rescheduled or one-time) that need
    completion events queued.

    Args:
        db_path: Path to sparkle_shine.db.
        queue_fn: Callable(fire_at, generator_name, kwargs) — injected by engine.
                  None in tests that don't need the queue.
    """

    def __init__(self, db_path: str = "sparkle_shine.db", queue_fn: Optional[Callable] = None):
        self.db_path = db_path
        self._queue_fn = queue_fn

    def execute(self, dry_run: bool = False) -> GeneratorResult:
        today = date.today()
        conn = get_connection()
        try:
            # Track E: rollout flag for canonical commercial agreement scheduling.
            # Read at call time so a config flip takes effect on the next tick
            # without a worker restart.
            from intelligence import config as _intel_config
            track_e_enabled = getattr(
                _intel_config,
                "TRACK_E_COMMERCIAL_AGREEMENT_SCHEDULING_ENABLED",
                False,
            )

            session = get_client("jobber") if not dry_run else None
            results = []
            jobs_created_this_run: list[str] = []  # canonical job IDs created here

            # ── Pass 1: recurring_agreements ──────────────────────────────────
            agreements = conn.execute("""
                SELECT * FROM recurring_agreements WHERE status = 'active'
            """).fetchall()
            agreements = [dict(a) for a in agreements]

            # Track prior jobs per crew for sequential time assignment.
            # Preload existing jobs for today so re-runs don't over-schedule.
            prior_jobs_by_crew: dict[str, list[dict]] = {}
            existing_today = conn.execute("""
                SELECT crew_id, COALESCE(duration_minutes_actual, 120) AS dur
                FROM jobs WHERE scheduled_date = %s
            """, (today.isoformat(),)).fetchall()
            for ej in existing_today:
                prior_jobs_by_crew.setdefault(
                    ej["crew_id"] or "crew-a", []
                ).append({"expected_duration": ej["dur"]})

            max_jobs = CREW_CAPACITY["max_jobs_per_crew"]
            max_minutes = int(
                CREW_CAPACITY["daily_minutes"] * CREW_CAPACITY["target_utilization_max"]
            )

            for agreement in agreements:
                if not _is_due_today(agreement, today):
                    continue

                # Track E gate: skip commercial agreements when flag is off.
                # Pass 1b (notes-based) still handles these clients in legacy mode.
                if (
                    agreement.get("client_type") == "commercial"
                    and not track_e_enabled
                ):
                    continue

                crew_id_check = agreement.get("crew_id") or "crew-a"
                prior = prior_jobs_by_crew.get(crew_id_check, [])
                crew_minutes = sum(j["expected_duration"] for j in prior)
                next_duration = _expected_duration(
                    agreement["service_type_id"], "regular"
                )
                if len(prior) >= max_jobs or (crew_minutes + next_duration) > max_minutes:
                    logger.debug(
                        "Capacity cap: skipping %s for %s (%d jobs, %d min)",
                        agreement["id"], crew_id_check, len(prior), crew_minutes,
                    )
                    continue

                # Idempotent guard: skip if job already exists for this client+date
                existing = conn.execute(
                    "SELECT id FROM jobs WHERE client_id = %s AND scheduled_date = %s",
                    (agreement["client_id"], today.isoformat()),
                ).fetchone()
                if existing:
                    prior_jobs_by_crew.setdefault(crew_id_check, []).append(
                        {"expected_duration": _expected_duration(
                            agreement["service_type_id"], "regular"
                        )}
                    )
                    continue

                try:
                    job_type = _pick_job_type(agreement, today)
                    crew_id = agreement.get("crew_id") or "crew-a"
                    prior = prior_jobs_by_crew.get(crew_id, [])
                    scheduled_dt = _assign_scheduled_time(crew_id, prior, today)
                    duration = _expected_duration(agreement["service_type_id"], job_type)
                    completion_dt = scheduled_dt + timedelta(minutes=duration)
                    completion_dt += timedelta(
                        minutes=random.uniform(-duration * 0.15, duration * 0.15)
                    )

                    if not dry_run:
                        job_id = generate_id("JOB", db_path=self.db_path)
                        property_id = _get_or_fetch_property_id(
                            agreement["client_id"], session, self.db_path
                        )
                        if property_id is None:
                            logger.warning(
                                "Skipping job for agreement %s: no Jobber property ID "
                                "found for client %s",
                                agreement["id"], agreement["client_id"],
                            )
                            continue
                        job_input = {
                            "propertyId": property_id,
                            "title": agreement["service_type_id"].replace("-", " ").title(),
                            "invoicing": {
                                "invoicingType": "FIXED_PRICE",
                                "invoicingSchedule": "ON_COMPLETION",
                            },
                            "timeframe": {"startAt": today.isoformat()},
                        }
                        resp = _gql(session, _JOB_CREATE, {"input": job_input})
                        errs = _gql_user_errors(resp, "jobCreate")
                        if errs:
                            raise RuntimeError(f"jobCreate errors: {errs}")
                        jobber_job_id = resp["data"]["jobCreate"]["job"]["id"]

                        conn.execute("""
                            INSERT INTO jobs
                            (id, client_id, crew_id, service_type_id,
                             scheduled_date, scheduled_time, status)
                            VALUES (%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT DO NOTHING
                        """, (
                            job_id, agreement["client_id"], crew_id,
                            agreement["service_type_id"], today.isoformat(),
                            scheduled_dt.strftime("%H:%M"), "scheduled",
                        ))
                        register_mapping(job_id, "jobber", jobber_job_id, db_path=self.db_path)
                    else:
                        job_id = f"dry-run-{agreement['id']}"

                    if self._queue_fn:
                        self._queue_fn(
                            fire_at=completion_dt,
                            generator_name="job_completion",
                            kwargs={"job_id": job_id},
                        )

                    prior_jobs_by_crew.setdefault(crew_id, []).append(
                        {"expected_duration": duration}
                    )
                    jobs_created_this_run.append(job_id)
                    results.append(("ok", job_id))

                except Exception as e:
                    logger.exception("Job scheduling failed for agreement %s", agreement.get("id"))
                    results.append(("failed", agreement.get("id"), str(e)))

            # ── Pass 1b: commercial nightly scheduling ────────────────────
            # Commercial clients (Crew D) don't have recurring_agreements.
            # Their schedules are encoded in client notes (nightly, 3x, 2x, daily).
            # This pass creates jobs for active commercial clients on scheduled days.
            #
            # Track E gate: when the flag is on, skip clients that have an
            # active commercial agreement — Pass 1 handled them. The existing
            # idempotency guard (SELECT id FROM jobs WHERE client_id AND date)
            # is a second line of defense, but the explicit skip keeps this
            # pass's intent clear and avoids redundant work on the hot path.
            commercial_clients = conn.execute("""
                SELECT id, company_name, notes
                FROM clients
                WHERE client_type = 'commercial' AND status = 'active'
            """).fetchall()
            commercial_clients = [dict(c) for c in commercial_clients]

            clients_with_active_agreement: set[str] = set()
            if track_e_enabled:
                rows = conn.execute("""
                    SELECT DISTINCT client_id FROM recurring_agreements
                    WHERE status = 'active' AND client_type = 'commercial'
                """).fetchall()
                clients_with_active_agreement = {r["client_id"] for r in rows}

            for client in commercial_clients:
                if client["id"] in clients_with_active_agreement:
                    continue
                notes = client.get("notes")
                client_id = client["id"]
                weekday = today.weekday()  # 0=Mon ... 6=Sun

                # Determine if this client has a job today. Blank notes default
                # to 3x weekly instead of silently removing the client from the
                # commercial schedule.
                is_scheduled = weekday in _COMMERCIAL_SCOPE_DAYS[_commercial_scope(notes)]

                if not is_scheduled:
                    continue

                # Capacity cap for Crew D
                prior_d = prior_jobs_by_crew.get("crew-d", [])
                d_minutes = sum(j["expected_duration"] for j in prior_d)
                if len(prior_d) >= max_jobs or (d_minutes + 180) > max_minutes:
                    continue

                # Idempotent guard
                existing = conn.execute(
                    "SELECT id FROM jobs WHERE client_id = %s AND scheduled_date = %s",
                    (client_id, today.isoformat()),
                ).fetchone()
                if existing:
                    prior_jobs_by_crew.setdefault("crew-d", []).append(
                        {"expected_duration": 180}
                    )
                    continue

                try:
                    crew_id = "crew-d"
                    duration = 180  # commercial-nightly duration
                    prior = prior_jobs_by_crew.get(crew_id, [])
                    scheduled_dt = _assign_scheduled_time(crew_id, prior, today)
                    completion_dt = scheduled_dt + timedelta(minutes=duration)
                    completion_dt += timedelta(
                        minutes=random.uniform(-duration * 0.15, duration * 0.15)
                    )

                    if not dry_run:
                        job_id = generate_id("JOB", db_path=self.db_path)
                        property_id = _get_or_fetch_property_id(
                            client_id, session, self.db_path
                        )
                        if property_id is None:
                            logger.warning(
                                "Skipping commercial job for %s: no Jobber property ID",
                                client["company_name"],
                            )
                            continue
                        job_input = {
                            "propertyId": property_id,
                            "title": f"Commercial Nightly Clean - {client['company_name']}",
                            "invoicing": {
                                "invoicingType": "FIXED_PRICE",
                                "invoicingSchedule": "ON_COMPLETION",
                            },
                            "timeframe": {"startAt": today.isoformat()},
                        }
                        resp = _gql(session, _JOB_CREATE, {"input": job_input})
                        errs = _gql_user_errors(resp, "jobCreate")
                        if errs:
                            raise RuntimeError(f"jobCreate errors: {errs}")
                        jobber_job_id = resp["data"]["jobCreate"]["job"]["id"]

                        conn.execute("""
                            INSERT INTO jobs
                            (id, client_id, crew_id, service_type_id,
                             scheduled_date, scheduled_time, status)
                            VALUES (%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT DO NOTHING
                        """, (
                            job_id, client_id, crew_id,
                            "commercial-nightly", today.isoformat(),
                            scheduled_dt.strftime("%H:%M"), "scheduled",
                        ))
                        register_mapping(job_id, "jobber", jobber_job_id, db_path=self.db_path)
                    else:
                        job_id = f"dry-run-comm-{client_id}"

                    if self._queue_fn:
                        self._queue_fn(
                            fire_at=completion_dt,
                            generator_name="job_completion",
                            kwargs={"job_id": job_id},
                        )

                    prior_jobs_by_crew.setdefault(crew_id, []).append(
                        {"expected_duration": duration}
                    )
                    jobs_created_this_run.append(job_id)
                    results.append(("ok", job_id))

                except Exception as e:
                    logger.exception("Commercial scheduling failed for %s", client.get("company_name"))
                    results.append(("failed", client.get("id"), str(e)))

            # ── Pass 1c: catch-up — complete past-due scheduled jobs ────────
            # If the engine was down or restarted, jobs from prior days may
            # still be in 'scheduled' status.  Their timed completion events
            # were lost when the process stopped.  Complete them now using the
            # same outcome probabilities as JobCompletionGenerator so the
            # statistical distribution stays realistic.
            overdue_jobs = conn.execute("""
                SELECT * FROM jobs
                WHERE status = 'scheduled'
                  AND scheduled_date::date < %s
                ORDER BY scheduled_date, crew_id
            """, (today.isoformat(),)).fetchall()
            overdue_jobs = [dict(j) for j in overdue_jobs]

            if overdue_jobs:
                if dry_run:
                    logger.debug(
                        "Catch-up: %d past-due scheduled jobs found, completing now",
                        len(overdue_jobs),
                    )
                else:
                    logger.info(
                        "Catch-up: %d past-due scheduled jobs found, completing now",
                        len(overdue_jobs),
                    )
                catchup_session = session  # reuse the Jobber session
                cfg_jc = DAILY_VOLUMES["job_completion"]
                for oj in overdue_jobs:
                    sp_name = f"catchup_{oj['id'].replace('-', '_')}"
                    try:
                        conn.execute(f"SAVEPOINT {sp_name}")

                        roll = random.random()
                        if roll < cfg_jc["on_time_rate"]:
                            outcome = "completed"
                        elif roll < cfg_jc["on_time_rate"] + cfg_jc["cancellation_rate"]:
                            outcome = "cancelled"
                        elif roll < (cfg_jc["on_time_rate"] + cfg_jc["cancellation_rate"]
                                     + cfg_jc["no_show_rate"]):
                            outcome = "no-show"
                        else:
                            # Rescheduled: for catch-up, just mark cancelled
                            # (the reschedule window has passed)
                            outcome = "cancelled"

                        if outcome == "completed":
                            service_type_id = oj.get("service_type_id", "std-residential")
                            expected = _expected_duration(service_type_id, "regular")
                            actual = int(expected * random.uniform(0.85, 1.15))
                            # Use a plausible completion time on the original date
                            completed_at = f"{oj['scheduled_date']}T{random.randint(10,17):02d}:{random.randint(0,59):02d}:00"

                            # Close in Jobber if we have a mapping and a session
                            if not dry_run:
                                jobber_job_id = get_tool_id(oj["id"], "jobber", db_path=self.db_path)
                                if jobber_job_id and catchup_session:
                                    try:
                                        _gql(catchup_session, _JOB_CLOSE, {
                                            "jobId": jobber_job_id,
                                            "input": {"modifyIncompleteVisitsBy": "COMPLETE_PAST_DESTROY_FUTURE"},
                                        })
                                    except Exception as e:
                                        logger.warning("Catch-up Jobber close failed for %s: %s", oj["id"], e)

                            conn.execute("""
                                UPDATE jobs
                                SET status = 'completed', duration_minutes_actual = %s,
                                    completed_at = %s
                                WHERE id = %s
                            """, (actual, completed_at, oj["id"]))

                            # Insert a review using the same numeric ID format as
                            # the normal completion path.
                            crew_id_r = oj.get("crew_id") or ""
                            crew_name_r = crew_id_r.replace("crew-", "Crew ").title()
                            sched_date = date.fromisoformat(oj["scheduled_date"])
                            dist = _adjusted_rating_distribution(crew_name_r, sched_date.weekday())
                            ratings = [r for r, _ in dist]
                            weights = [w for _, w in dist]
                            rating = random.choices(ratings, weights=weights, k=1)[0]
                            review_id = _next_review_id(conn)
                            conn.execute("""
                                INSERT INTO reviews (id, client_id, job_id, rating, platform, review_date)
                                VALUES (%s, %s, %s, %s, 'internal', %s)
                                ON CONFLICT (id) DO NOTHING
                            """, (review_id, oj["client_id"], oj["id"], rating, oj["scheduled_date"]))
                        else:
                            # cancelled or no-show
                            if not dry_run:
                                jobber_job_id = get_tool_id(oj["id"], "jobber", db_path=self.db_path)
                                if jobber_job_id and catchup_session:
                                    try:
                                        _gql(catchup_session, _JOB_CLOSE, {
                                            "jobId": jobber_job_id,
                                            "input": {"modifyIncompleteVisitsBy": "COMPLETE_PAST_DESTROY_FUTURE"},
                                        })
                                    except Exception as e:
                                        logger.warning("Catch-up Jobber close failed for %s: %s", oj["id"], e)
                            conn.execute(
                                "UPDATE jobs SET status = %s WHERE id = %s",
                                (outcome, oj["id"]),
                            )

                        conn.execute(f"RELEASE SAVEPOINT {sp_name}")
                        results.append(("ok_catchup", oj["id"]))
                    except Exception as e:
                        conn.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                        conn.execute(f"RELEASE SAVEPOINT {sp_name}")
                        logger.exception("Catch-up failed for job %s", oj["id"])
                        results.append(("failed", oj["id"], str(e)))

                if dry_run:
                    logger.debug("Catch-up complete: %d jobs processed", len(overdue_jobs))
                else:
                    logger.info("Catch-up complete: %d jobs processed", len(overdue_jobs))

            # ── Pass 2: already-scheduled jobs (rescheduled + one-time) ──────
            already_scheduled = conn.execute("""
                SELECT * FROM jobs
                WHERE status = 'scheduled'
                  AND scheduled_date = %s
            """, (today.isoformat(),)).fetchall()
            already_scheduled = [dict(j) for j in already_scheduled]

            for job in already_scheduled:
                if job["id"] in jobs_created_this_run:
                    continue  # just created this run — already has a completion event
                try:
                    crew_id = job.get("crew_id") or "crew-a"
                    prior = prior_jobs_by_crew.get(crew_id, [])
                    if job.get("scheduled_time"):
                        try:
                            h, m = map(int, job["scheduled_time"].split(":"))
                            scheduled_dt = datetime.combine(today, time(h, m))
                        except Exception:
                            scheduled_dt = _assign_scheduled_time(crew_id, prior, today)
                    else:
                        scheduled_dt = _assign_scheduled_time(crew_id, prior, today)

                    duration = _expected_duration(job.get("service_type_id", "std-residential"), "regular")
                    completion_dt = scheduled_dt + timedelta(minutes=duration)
                    completion_dt += timedelta(
                        minutes=random.uniform(-duration * 0.15, duration * 0.15)
                    )

                    if self._queue_fn:
                        self._queue_fn(
                            fire_at=completion_dt,
                            generator_name="job_completion",
                            kwargs={"job_id": job["id"]},
                        )

                    prior_jobs_by_crew.setdefault(crew_id, []).append(
                        {"expected_duration": duration}
                    )
                    results.append(("ok_existing", job["id"]))
                except Exception as e:
                    logger.exception("Completion queuing failed for existing job %s", job.get("id"))
                    results.append(("failed", job.get("id"), str(e)))

            # ── Pass 3: utilization balancing (fill-in jobs) ────────────
            # For crews below 80% target, add extra work from the active client
            # base. Residential crews prefer zone-matched clients but fall back
            # to unzoned customers on slow days; crew-d pulls commercial fill-ins.
            _CREW_FILL_CONFIG = {
                "crew-a": {"zone": "West Austin", "client_group": "residential"},
                "crew-b": {"zone": "East Austin", "client_group": "residential"},
                "crew-c": {"zone": "South Austin", "client_group": "residential"},
                "crew-d": {"zone": None, "client_group": "commercial"},
            }
            target_min_minutes = int(
                CREW_CAPACITY["daily_minutes"] * CREW_CAPACITY["target_utilization_min"]
            )
            target_max_minutes = int(
                CREW_CAPACITY["daily_minutes"] * CREW_CAPACITY["target_utilization_max"]
            )
            max_jobs = CREW_CAPACITY["max_jobs_per_crew"]

            for crew_id in ("crew-a", "crew-b", "crew-c", "crew-d"):
                prior = prior_jobs_by_crew.get(crew_id, [])
                total_minutes = sum(j["expected_duration"] for j in prior)
                job_count = len(prior)

                if total_minutes >= target_min_minutes or job_count >= max_jobs:
                    continue

                # Don't overshoot the max target
                gap_minutes = min(
                    target_min_minutes - total_minutes,
                    target_max_minutes - total_minutes,
                )
                fill_cfg = _CREW_FILL_CONFIG[crew_id]
                zone = fill_cfg["zone"]
                fillin_candidates = _fillin_candidates(
                    conn,
                    today_iso=today.isoformat(),
                    limit=max((max_jobs - job_count) * 8, 12),
                    preferred_zone=zone,
                    client_group=fill_cfg["client_group"],
                )

                for candidate in fillin_candidates:
                    if gap_minutes <= 0 or job_count >= max_jobs:
                        break

                    if fill_cfg["client_group"] == "commercial":
                        service_type_id = "commercial-nightly"
                        duration = _expected_duration(service_type_id, "standard")
                        fill_title = "Commercial Clean (Fill-In)"
                    else:
                        # Choose service type based on available gap:
                        # deep clean (210 min) if gap is large, standard (120 min) if small
                        if gap_minutes >= 200:
                            service_type_id = "deep-clean"
                            duration = _expected_duration(service_type_id, "deep_clean")
                            fill_title = "Deep Clean (Fill-In)"
                        else:
                            service_type_id = "std-residential"
                            duration = _expected_duration(service_type_id, "regular")
                            fill_title = "Standard Clean (Fill-In)"
                    scheduled_dt = _assign_scheduled_time(crew_id, prior, today)
                    completion_dt = scheduled_dt + timedelta(minutes=duration)
                    completion_dt += timedelta(
                        minutes=random.uniform(-duration * 0.15, duration * 0.15)
                    )

                    try:
                        if not dry_run:
                            fill_job_id = generate_id("JOB", db_path=self.db_path)
                            property_id = _get_or_fetch_property_id(
                                candidate["client_id"], session, self.db_path
                            )
                            if property_id is None:
                                continue

                            job_input = {
                                "propertyId": property_id,
                                "title": fill_title,
                                "invoicing": {
                                    "invoicingType": "FIXED_PRICE",
                                    "invoicingSchedule": "ON_COMPLETION",
                                },
                                "timeframe": {"startAt": today.isoformat()},
                            }
                            resp = _gql(session, _JOB_CREATE, {"input": job_input})
                            errs = _gql_user_errors(resp, "jobCreate")
                            if errs:
                                raise RuntimeError(f"jobCreate errors: {errs}")
                            jobber_job_id = resp["data"]["jobCreate"]["job"]["id"]

                            conn.execute("""
                                INSERT INTO jobs
                                (id, client_id, crew_id, service_type_id,
                                 scheduled_date, scheduled_time, status)
                                VALUES (%s,%s,%s,%s,%s,%s,%s)
                                ON CONFLICT DO NOTHING
                            """, (
                                fill_job_id, candidate["client_id"], crew_id,
                                service_type_id, today.isoformat(),
                                scheduled_dt.strftime("%H:%M"), "scheduled",
                            ))
                            register_mapping(fill_job_id, "jobber", jobber_job_id,
                                             db_path=self.db_path)
                        else:
                            fill_job_id = f"dry-run-fill-{candidate['client_id']}"

                        if self._queue_fn:
                            self._queue_fn(
                                fire_at=completion_dt,
                                generator_name="job_completion",
                                kwargs={"job_id": fill_job_id},
                            )

                        prior_jobs_by_crew.setdefault(crew_id, prior).append(
                            {"expected_duration": duration}
                        )
                        gap_minutes -= duration
                        job_count += 1
                        results.append(("ok_fill", fill_job_id))
                        service_label = "commercial clean" if service_type_id == "commercial-nightly" else (
                            "deep clean" if service_type_id == "deep-clean" else "standard clean"
                        )
                        if dry_run:
                            logger.debug(
                                "Fill-in: %s %s for %s (%s), %d min gap remaining",
                                crew_id, service_label, candidate["client_id"], zone or "any-zone", gap_minutes,
                            )
                        else:
                            logger.info(
                                "Fill-in: %s %s for %s (%s), %d min gap remaining",
                                crew_id, service_label, candidate["client_id"], zone or "any-zone", gap_minutes,
                            )

                    except Exception as e:
                        logger.exception(
                            "Fill-in scheduling failed for %s", candidate["client_id"]
                        )
                        results.append(("failed", candidate.get("client_id"), str(e)))

            # ── Utilization summary ──────────────────────────────────────
            for crew_id in ("crew-a", "crew-b", "crew-c", "crew-d"):
                prior = prior_jobs_by_crew.get(crew_id, [])
                total_min = sum(j["expected_duration"] for j in prior)
                util_pct = total_min / CREW_CAPACITY["daily_minutes"] * 100
                if dry_run:
                    logger.debug(
                        "%s: %d jobs, %d min scheduled, %.0f%% utilization",
                        crew_id, len(prior), total_min, util_pct,
                    )
                else:
                    logger.info(
                        "%s: %d jobs, %d min scheduled, %.0f%% utilization",
                        crew_id, len(prior), total_min, util_pct,
                    )

            if not dry_run:
                conn.commit()
            succeeded = sum(1 for r in results if r[0] in ("ok", "ok_existing", "ok_fill"))
            catchup_count = sum(1 for r in results if r[0] == "ok_catchup")
            fill_count = sum(1 for r in results if r[0] == "ok_fill")
            failed = sum(1 for r in results if r[0] == "failed")
            msg = f"scheduled {succeeded} jobs"
            if catchup_count:
                msg += f" ({catchup_count} catch-up)"
            if fill_count:
                msg += f" ({fill_count} fill-in)"
            if failed:
                return GeneratorResult(success=False, message=f"{msg}, {failed} failed")
            return GeneratorResult(success=True, message=msg)

        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


# ── JobCompletionGenerator ────────────────────────────────────────────────────

class JobCompletionGenerator:
    """Type 2 generator: fires at realistic times, records job outcomes + reviews.

    Dispatched by timed events queued by JobSchedulingGenerator.
    Receives job_id in kwargs.

    Outcome probabilities from DAILY_VOLUMES["job_completion"]:
      92% completed, 3% cancelled, 2% no-show, 3% rescheduled
    """

    def __init__(self, db_path: str = "sparkle_shine.db"):
        self.db_path = db_path

    def _ensure_schema(self, conn) -> None:
        """Add churn_risk column to clients if not present."""
        cols = get_column_names(conn, "clients")
        if "churn_risk" not in cols:
            conn.execute("ALTER TABLE clients ADD COLUMN churn_risk TEXT DEFAULT 'normal'")
            conn.commit()

    def execute(self, dry_run: bool = False, job_id: Optional[str] = None) -> GeneratorResult:
        if not job_id:
            return GeneratorResult(success=False, message="job_id required")

        conn = get_connection()
        try:
            self._ensure_schema(conn)

            job = conn.execute(
                "SELECT * FROM jobs WHERE id = %s", (job_id,)
            ).fetchone()
            if not job:
                return GeneratorResult(success=False, message=f"job {job_id}: not found")
            job = dict(job)

            # Duplicate timed events can happen after a restart when the engine
            # restores a checkpointed queue and also re-queues today's scheduled
            # jobs. Treat already-finished jobs as idempotent no-ops instead of
            # letting a second outcome overwrite the terminal status.
            if job.get("completed_at"):
                if job.get("status") == "scheduled":
                    conn.execute(
                        "UPDATE jobs SET status = 'completed' WHERE id = %s",
                        (job_id,),
                    )
                    conn.commit()
                    return GeneratorResult(
                        success=True,
                        message=f"job {job_id}: normalized completed status",
                    )
                return GeneratorResult(
                    success=True,
                    message=f"job {job_id}: already completed",
                )

            if job.get("status") != "scheduled":
                return GeneratorResult(
                    success=True,
                    message=f"job {job_id}: already {job['status']}",
                )

            if dry_run:
                return GeneratorResult(success=True, message=f"job {job_id}: dry_run skip")

            session = get_client("jobber")
            jobber_job_id = get_tool_id(job_id, "jobber", db_path=self.db_path)
            if not jobber_job_id:
                return GeneratorResult(success=False, message=f"job {job_id}: no jobber mapping")

            # Determine outcome
            cfg = DAILY_VOLUMES["job_completion"]
            roll = random.random()
            if roll < cfg["on_time_rate"]:
                outcome = "completed"
            elif roll < cfg["on_time_rate"] + cfg["cancellation_rate"]:
                outcome = "cancelled"
            elif roll < cfg["on_time_rate"] + cfg["cancellation_rate"] + cfg["no_show_rate"]:
                outcome = "no-show"
            else:
                outcome = "rescheduled"

            if outcome == "completed":
                self._handle_completed(job, job_id, jobber_job_id, session, conn)
            elif outcome in ("cancelled", "no-show"):
                self._handle_cancelled_or_noshow(job, job_id, jobber_job_id, outcome, session, conn)
            else:
                self._handle_rescheduled(job, job_id, jobber_job_id, session, conn)

            conn.commit()
            return GeneratorResult(success=True, message=f"job {job_id}: {outcome}")

        except Exception as e:
            conn.rollback()
            logger.exception("JobCompletionGenerator failed for %s", job_id)
            return GeneratorResult(success=False, message=f"job {job_id}: {e}")
        finally:
            conn.close()

    def _handle_completed(
        self, job: dict, job_id: str, jobber_job_id: str, session, conn
    ) -> None:
        service_type_id = job.get("service_type_id", "std-residential")
        expected = _expected_duration(service_type_id, "regular")
        actual = int(expected * random.uniform(0.85, 1.15))
        completed_at = datetime.utcnow().isoformat()

        # Jobber: close the job
        resp = _gql(session, _JOB_CLOSE, {
            "jobId": jobber_job_id,
            "input": {"modifyIncompleteVisitsBy": "COMPLETE_PAST_DESTROY_FUTURE"},
        })
        errs = _gql_user_errors(resp, "jobClose")
        if errs:
            raise RuntimeError(f"jobClose errors: {errs}")

        conn.execute("""
            UPDATE jobs
            SET status = 'completed', duration_minutes_actual = %s, completed_at = %s
            WHERE id = %s
        """, (actual, completed_at, job_id))

        # Insert review
        today = date.today()
        crew_id = job.get("crew_id") or ""
        crew_name = crew_id.replace("crew-", "Crew ").title()  # "crew-a" → "Crew A"
        dist = _adjusted_rating_distribution(crew_name, today.weekday())
        ratings = [r for r, _ in dist]
        weights = [w for _, w in dist]
        rating = random.choices(ratings, weights=weights, k=1)[0]

        review_id = _next_review_id(conn)
        conn.execute("""
            INSERT INTO reviews (id, client_id, job_id, rating, platform, review_date)
            VALUES (%s, %s, %s, %s, 'internal', %s)
        """, (review_id, job["client_id"], job_id, rating, today.isoformat()))

    def _handle_cancelled_or_noshow(
        self, job: dict, job_id: str, jobber_job_id: str,
        outcome: str, session, conn
    ) -> None:
        conn.execute("UPDATE jobs SET status = %s WHERE id = %s", (outcome, job_id))

        close_resp = _gql(session, _JOB_CLOSE, {
            "jobId": jobber_job_id,
            "input": {"modifyIncompleteVisitsBy": "COMPLETE_PAST_DESTROY_FUTURE"},
        })
        close_errs = _gql_user_errors(close_resp, "jobClose")
        if close_errs:
            raise RuntimeError(f"jobClose errors: {close_errs}")

        # Churn risk: 3+ cancelled/no-show in 60 days → high
        count_row = conn.execute("""
            SELECT COUNT(*) AS cnt FROM jobs
            WHERE client_id = %s
              AND status IN ('cancelled', 'no-show')
              AND scheduled_date::date >= CURRENT_DATE - INTERVAL '60 days'
        """, (job["client_id"],)).fetchone()
        if count_row and count_row["cnt"] >= 3:
            conn.execute(
                "UPDATE clients SET churn_risk = 'high' WHERE id = %s",
                (job["client_id"],),
            )

    def _handle_rescheduled(
        self, job: dict, job_id: str, jobber_job_id: str, session, conn
    ) -> None:
        # Cancel original slot
        conn.execute("UPDATE jobs SET status = 'cancelled' WHERE id = %s", (job_id,))
        close_resp = _gql(session, _JOB_CLOSE, {
            "jobId": jobber_job_id,
            "input": {"modifyIncompleteVisitsBy": "COMPLETE_PAST_DESTROY_FUTURE"},
        })
        close_errs = _gql_user_errors(close_resp, "jobClose")
        if close_errs:
            raise RuntimeError(f"jobClose errors: {close_errs}")

        # New job for next business day
        tomorrow = _add_business_days(date.today(), 1)
        new_job_id = generate_id("JOB", db_path=self.db_path)

        service_type_id = job.get("service_type_id", "std-residential")
        crew_id = job.get("crew_id") or "crew-a"

        # jobCreate for the rescheduled slot
        property_id = _get_or_fetch_property_id(job["client_id"], session, self.db_path)
        if property_id is None:
            raise RuntimeError(
                f"Cannot reschedule job {job_id}: no Jobber property ID for client "
                f"{job['client_id']}"
            )
        job_input = {
            "propertyId": property_id,
            "title": service_type_id.replace("-", " ").title(),
            "invoicing": {
                "invoicingType": "FIXED_PRICE",
                "invoicingSchedule": "ON_COMPLETION",
            },
            "timeframe": {"startAt": tomorrow.isoformat()},
        }
        resp = _gql(session, _JOB_CREATE, {"input": job_input})
        errs = _gql_user_errors(resp, "jobCreate")
        if errs:
            raise RuntimeError(f"jobCreate (rescheduled) errors: {errs}")
        new_jobber_job_id = resp["data"]["jobCreate"]["job"]["id"]

        conn.execute("""
            INSERT INTO jobs (id, client_id, crew_id, service_type_id, scheduled_date, status)
            VALUES (%s, %s, %s, %s, %s, 'scheduled')
        """, (new_job_id, job["client_id"], crew_id, service_type_id, tomorrow.isoformat()))

        register_mapping(new_job_id, "jobber", new_jobber_job_id, db_path=self.db_path)
