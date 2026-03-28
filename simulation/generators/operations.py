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
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Callable, Optional

from auth import get_client
from config.business import SERVICE_TYPES
from database.mappings import generate_id, get_tool_id, register_mapping
from intelligence.logging_config import setup_logging
from seeding.utils.throttler import JOBBER as throttler
from simulation.config import DAILY_VOLUMES, JOB_VARIETY

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

_ONE_TIME_FREQS = {
    "one_time_standard", "one_time_deep_clean",
    "one_time_move_in_out", "one_time_project",
}

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
    """
    start = date.fromisoformat(agreement["start_date"])
    freq = agreement["frequency"]

    _DOW_MAP = {
        "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
        "friday": 4, "saturday": 5, "sunday": 6,
    }
    dow_str = (agreement.get("day_of_week") or "").lower()
    day_of_week_int = _DOW_MAP.get(dow_str, start.weekday())

    if freq == "weekly":
        return today.weekday() == day_of_week_int

    elif freq == "biweekly":
        return (
            today.weekday() == day_of_week_int
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
    except Exception:
        pass
    _recurrence_field_checked = True
    return _recurrence_field_cache


# ── Property ID helper (lazy fetch + register) ────────────────────────────────

def _get_or_fetch_property_id(canonical_id: str, session, db_path: str) -> Optional[str]:
    """Return the Jobber property ID for a client canonical_id.

    First checks cross_tool_mapping for a 'jobber_property' entry.
    If absent, queries Jobber via ClientProperties and registers the result.
    Returns None if the client has no Jobber mapping or no properties.
    """
    # Fast path: already registered
    prop_id = get_tool_id(canonical_id, "jobber_property", db_path=db_path)
    if prop_id:
        return prop_id

    # Slow path: query Jobber
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
