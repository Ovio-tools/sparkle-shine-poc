"""
simulation/jobber_utils.py

Shared Jobber-payload helpers used by the simulation engine, the seeder,
and the new-client-onboarding automation.

Single chokepoint for building `JobCreateAttributes` so every jobCreate
call site emits a consistent payload (with `assignedUsers` + `endAt`).

Schema-name discovery uses GraphQL introspection so a Jobber field rename
does not silently drop data — the soft-fail path logs once and the field
is omitted, matching the precedent set by the old `_get_recurrence_field`.
"""
from __future__ import annotations

import json
import os
import random
from datetime import date, datetime, timedelta
from typing import Any, Optional

from config.business import SERVICE_TYPES
from intelligence.logging_config import setup_logging
from seeding.utils.throttler import JOBBER as throttler

logger = setup_logging("simulation.jobber_utils")


# ── Constants ─────────────────────────────────────────────────────────────────

_JOBBER_GQL_URL = "https://api.getjobber.com/api/graphql"
_JOBBER_VERSION_HEADER = {"X-JOBBER-GRAPHQL-VERSION": "2026-03-10"}

_DURATION_MAP = {st["id"]: st["duration_minutes"] for st in SERVICE_TYPES}

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_TOOL_IDS_PATH = os.path.join(_PROJECT_ROOT, "config", "tool_ids.json")

# Candidate field names tried during schema introspection. Order = priority.
_ASSIGNED_USERS_CANDIDATES = (
    "assignedUsers", "userAssignmentIds", "assignedUserIds", "assignees",
)
_TIMEFRAME_END_CANDIDATES = ("endAt", "end", "endTime", "finishAt")
_RECURRENCE_CANDIDATES = ("recurrences", "repeat", "schedule", "recurrence")


# ── Module-level caches ──────────────────────────────────────────────────────

# Discovered Jobber schema field names. None means "introspection ran and
# found no matching candidate" — caller should omit the field.
JOBBER_FIELD_CACHE: dict[str, Optional[str]] = {
    "assigned_users": None,
    "timeframe_end": None,
    "recurrence": None,
}

_field_discovery_done: bool = False
_field_warned: set[str] = set()  # field names we've already WARN'd about


# ── GraphQL helper ────────────────────────────────────────────────────────────

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
    return resp.json()


# ── tool_ids.json access ─────────────────────────────────────────────────────

def _load_jobber_config() -> dict:
    """Return the `jobber` block from tool_ids.json, or {} if absent."""
    try:
        with open(_TOOL_IDS_PATH) as f:
            return json.load(f).get("jobber", {}) or {}
    except FileNotFoundError:
        return {}


# ── Schema introspection ─────────────────────────────────────────────────────

_JOB_CREATE_ATTRS_QUERY = """
query JobCreateAttrs {
  __type(name: "JobCreateAttributes") {
    inputFields {
      name
      type {
        name
        kind
        ofType { name kind }
      }
    }
  }
}
"""

_TYPE_FIELDS_QUERY = """
query TypeFields($name: String!) {
  __type(name: $name) {
    inputFields { name }
  }
}
"""


def _resolve_input_type_name(field_type: dict) -> Optional[str]:
    """Walk a GraphQL type ref to the underlying INPUT_OBJECT name, if any."""
    while field_type:
        if field_type.get("kind") == "INPUT_OBJECT" and field_type.get("name"):
            return field_type["name"]
        field_type = field_type.get("ofType")
    return None


def discover_job_create_fields(session) -> None:
    """Populate JOBBER_FIELD_CACHE via introspection. Idempotent.

    Discovers, in one schema query:
      - the top-level `assignedUsers` field name
      - the top-level `recurrence`/`recurrences` field name
      - the name of the `timeframe` input type (for the second query)

    Then issues a second query to discover the end-time field on the
    timeframe input type.
    """
    global _field_discovery_done
    if _field_discovery_done:
        return

    try:
        data = _gql(session, _JOB_CREATE_ATTRS_QUERY, {})
        fields = (
            data.get("data", {}).get("__type", {}).get("inputFields", []) or []
        )
        names_to_field = {f["name"]: f for f in fields}

        for cand in _ASSIGNED_USERS_CANDIDATES:
            if cand in names_to_field:
                JOBBER_FIELD_CACHE["assigned_users"] = cand
                break

        for cand in _RECURRENCE_CANDIDATES:
            if cand in names_to_field:
                JOBBER_FIELD_CACHE["recurrence"] = cand
                break

        timeframe_field = names_to_field.get("timeframe")
        timeframe_type_name = (
            _resolve_input_type_name(timeframe_field["type"])
            if timeframe_field else None
        )
        if timeframe_type_name:
            tf_data = _gql(session, _TYPE_FIELDS_QUERY, {"name": timeframe_type_name})
            tf_fields = (
                tf_data.get("data", {}).get("__type", {}).get("inputFields", []) or []
            )
            tf_names = {f["name"] for f in tf_fields}
            for cand in _TIMEFRAME_END_CANDIDATES:
                if cand in tf_names:
                    JOBBER_FIELD_CACHE["timeframe_end"] = cand
                    break

        _field_discovery_done = True
        logger.info(
            "Jobber field discovery: assignedUsers=%s, timeframe_end=%s, recurrence=%s",
            JOBBER_FIELD_CACHE["assigned_users"],
            JOBBER_FIELD_CACHE["timeframe_end"],
            JOBBER_FIELD_CACHE["recurrence"],
        )

        for key in ("assigned_users", "timeframe_end", "recurrence"):
            if JOBBER_FIELD_CACHE[key] is None and key not in _field_warned:
                _field_warned.add(key)
                logger.warning(
                    "Jobber introspection did not find a field for '%s'; "
                    "payload will omit it. Candidates tried: %s",
                    key, _candidates_for(key),
                )
    except Exception:
        logger.warning(
            "Failed to introspect Jobber JobCreateAttributes; will retry next call"
        )


def _candidates_for(cache_key: str) -> tuple:
    return {
        "assigned_users": _ASSIGNED_USERS_CANDIDATES,
        "timeframe_end": _TIMEFRAME_END_CANDIDATES,
        "recurrence": _RECURRENCE_CANDIDATES,
    }.get(cache_key, ())


def get_recurrence_field(session) -> Optional[str]:
    """Back-compat shim for callers that imported `_get_recurrence_field`."""
    discover_job_create_fields(session)
    return JOBBER_FIELD_CACHE["recurrence"]


# ── Visit / assignee helpers ────────────────────────────────────────────────

def _parse_iso_datetime(value: object) -> Optional[datetime]:
    """Parse a Jobber ISO-ish timestamp into a datetime, or None."""
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        pass

    try:
        parsed = date.fromisoformat(text[:10])
    except ValueError:
        return None
    return datetime(parsed.year, parsed.month, parsed.day)


def _duration_to_minutes(raw_duration: object) -> Optional[int]:
    """Normalize Jobber duration values into minutes.

    Jobber visit durations are documented in minutes, but legacy code and test
    fixtures in this repo have historically treated large values as seconds.
    Accept both shapes so we do not regress existing data handling while still
    supporting minute-based responses.
    """
    if raw_duration in (None, "", 0):
        return None
    try:
        value = float(raw_duration)
    except (TypeError, ValueError):
        return None
    if value <= 0:
        return None
    if value > 1440:
        return round(value / 60.0)
    return round(value)


def normalize_jobber_assigned_users(value: object) -> list[dict[str, str]]:
    """Return a stable [{id, name, email}] representation of Jobber assignees."""
    if value is None:
        return []

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            value = json.loads(text)
        except json.JSONDecodeError:
            return []

    if isinstance(value, dict):
        nodes = value.get("nodes") or []
    elif isinstance(value, list):
        nodes = value
    else:
        return []

    normalized: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for node in nodes:
        if not isinstance(node, dict):
            continue
        user_id = str(node.get("id") or "").strip()
        name_obj = node.get("name") or {}
        full_name = ""
        if isinstance(name_obj, dict):
            full_name = str(name_obj.get("full") or "").strip()
        full_name = full_name or str(node.get("name") or "").strip()
        email_obj = node.get("email") or {}
        email = ""
        if isinstance(email_obj, dict):
            email = str(email_obj.get("raw") or "").strip()
        email = email or str(node.get("email") or "").strip()
        record = {"id": user_id, "name": full_name, "email": email}
        key = (record["id"], record["name"].lower(), record["email"].lower())
        if key in seen:
            continue
        seen.add(key)
        if any(record.values()):
            normalized.append(record)
    return normalized


def assigned_user_display_names(users: object) -> list[str]:
    """Return display names for normalized or raw Jobber assignee values."""
    names: list[str] = []
    for user in normalize_jobber_assigned_users(users):
        name = (user.get("name") or "").strip()
        email = (user.get("email") or "").strip()
        display = name or email
        if display:
            names.append(display)
    return names


def choose_relevant_visit(visit_nodes: list[dict]) -> Optional[dict]:
    """Pick the Jobber visit we should treat as the source of truth."""
    if not visit_nodes:
        return None

    completed = [
        (visit, _parse_iso_datetime(visit.get("completedAt")))
        for visit in visit_nodes
    ]
    completed = [(visit, dt) for visit, dt in completed if dt is not None]
    if completed:
        return max(completed, key=lambda item: item[1])[0]

    started = [
        (visit, _parse_iso_datetime(visit.get("startAt")))
        for visit in visit_nodes
    ]
    started = [(visit, dt) for visit, dt in started if dt is not None]
    if started:
        return max(started, key=lambda item: item[1])[0]

    return visit_nodes[0]


def duration_minutes_from_visit(visit: Optional[dict]) -> Optional[int]:
    """Return the chosen visit's duration in minutes."""
    if not visit:
        return None

    duration_minutes = _duration_to_minutes(visit.get("duration"))
    if duration_minutes is not None:
        return duration_minutes

    start_at = _parse_iso_datetime(visit.get("startAt"))
    end_at = _parse_iso_datetime(visit.get("endAt"))
    if start_at is None or end_at is None:
        return None

    delta_minutes = (end_at - start_at).total_seconds() / 60.0
    if delta_minutes <= 0:
        return None
    return round(delta_minutes)


def extract_job_visit_details(node: dict) -> dict[str, Any]:
    """Select the relevant visit and return normalized visit-derived fields."""
    visit_nodes = (node.get("visits") or {}).get("nodes") or []
    visit = choose_relevant_visit(visit_nodes)
    return {
        "visit": visit,
        "duration_minutes": duration_minutes_from_visit(visit),
        "jobber_assigned_users": normalize_jobber_assigned_users(
            (visit or {}).get("assignedUsers")
        ),
        "visit_completed_at": (visit or {}).get("completedAt"),
    }


def derive_canonical_crew_id(
    db,
    jobber_assigned_users: object,
    *,
    existing_crew_id: Optional[str] = None,
) -> Optional[str]:
    """Best-effort crew derivation from Jobber assignees.

    Only returns a derived crew when every assignee maps cleanly to the same
    non-null employees.crew_id. Otherwise preserves the existing crew_id.
    """
    users = normalize_jobber_assigned_users(jobber_assigned_users)
    if not users:
        return existing_crew_id

    rows = db.execute(
        """
        SELECT first_name, last_name, email, crew_id
        FROM employees
        WHERE status = 'active' OR status IS NULL
        """
    ).fetchall()

    email_map: dict[str, Any] = {}
    name_map: dict[str, Any] = {}
    for row in rows:
        email = str(row["email"] or "").strip().lower()
        if email:
            email_map[email] = row
        full_name = f"{row['first_name']} {row['last_name']}".strip().lower()
        if full_name:
            name_map[full_name] = row

    crew_ids: list[str] = []
    for user in users:
        email = str(user.get("email") or "").strip().lower()
        name = str(user.get("name") or "").strip().lower()
        match = email_map.get(email) if email else None
        if match is None and name:
            match = name_map.get(name)
        if match is None:
            return existing_crew_id
        crew_id = match["crew_id"]
        if not crew_id:
            return existing_crew_id
        crew_ids.append(crew_id)

    unique_crew_ids = {crew_id for crew_id in crew_ids if crew_id}
    if len(unique_crew_ids) != 1:
        return existing_crew_id
    return next(iter(unique_crew_ids))


# ── Duration helpers ─────────────────────────────────────────────────────────

def expected_duration(service_type_id: str, job_type: str) -> int:
    """Return expected job duration in minutes from SERVICE_TYPES.

    deep_clean overrides any service_type_id with the deep-clean duration.
    Default 120 min if service_type_id is not recognised.
    """
    if job_type == "deep_clean":
        return _DURATION_MAP.get("deep-clean", 210)
    return _DURATION_MAP.get(service_type_id, 120)


# Back-compat alias for the underscore-prefixed name used by the old call sites.
_expected_duration = expected_duration


def crew_size_for(duration_minutes: int, pool_size: int) -> int:
    """Return number of users to assign to a job based on duration tier.

    Tiers (configurable via tool_ids.json["jobber"]["crew_size_tiers"]):
      duration < small_max   → 1 user
      duration <= medium_max → 2 users
      duration > medium_max  → 3 users

    Always clamped to min(pool_size, 3) so a 2-user pool never gets 3.
    """
    if pool_size <= 0:
        return 0
    cfg = _load_jobber_config().get("crew_size_tiers") or {}
    small_max = int(cfg.get("small_max", 90))
    medium_max = int(cfg.get("medium_max", 150))

    if duration_minutes < small_max:
        tier = 1
    elif duration_minutes <= medium_max:
        tier = 2
    else:
        tier = 3
    return max(1, min(tier, pool_size))


# ── Timeframe / endAt computation ────────────────────────────────────────────

def _parse_start(start_iso: str) -> tuple[datetime, bool]:
    """Parse a start ISO string. Returns (datetime, was_date_only)."""
    if "T" in start_iso:
        return datetime.fromisoformat(start_iso), False
    # Date-only — treat as start of day in local naive time.
    d = date.fromisoformat(start_iso)
    return datetime(d.year, d.month, d.day), True


def compute_end_at(
    start_iso: str,
    duration_minutes: int,
    *,
    jitter_minutes: float = 5.0,
) -> str:
    """Return an ISO end-time string `jitter_minutes` around start + duration.

    Preserves date-only input shape when the resulting end falls on the same
    calendar day; emits a full datetime when duration would push past
    midnight so Jobber sees a proper end time.
    """
    start_dt, was_date_only = _parse_start(start_iso)
    jitter = random.uniform(-jitter_minutes, jitter_minutes) if jitter_minutes > 0 else 0
    end_dt = start_dt + timedelta(minutes=duration_minutes + jitter)

    if was_date_only and end_dt.date() == start_dt.date():
        # Both ends on the same day — keep date-only shape.
        return end_dt.date().isoformat()
    return end_dt.isoformat()


# ── The chokepoint: payload builder ──────────────────────────────────────────

def build_job_create_input(
    *,
    property_id: str,
    title: str,
    invoicing: dict,
    start_iso: Optional[str],
    duration_minutes: Optional[int],
    user_pool=None,
    session=None,
    extra: Optional[dict] = None,
) -> dict:
    """Build a JobCreateAttributes dict including `endAt` and `assignedUsers`
    when their field names have been discovered via introspection.

    Soft-fails when fields can't be discovered or the user pool can't satisfy
    the requested crew size — never raises; just omits.

    Args:
        property_id: Jobber property ID (required).
        title: Job title.
        invoicing: invoicing block ({invoicingType, invoicingSchedule}).
        start_iso: ISO start; pass None to omit the timeframe block.
        duration_minutes: expected duration; pass None or 0 to omit endAt.
        user_pool: optional UserPool instance. None → no assignedUsers.
        session: requests.Session for Jobber; required when user_pool is set
                 or when fields haven't been discovered yet.
        extra: dict merged into the result last (e.g. recurrence config).
    """
    if session is not None:
        discover_job_create_fields(session)

    payload: dict = {
        "propertyId": property_id,
        "title": title,
        "invoicing": invoicing,
    }

    end_field = JOBBER_FIELD_CACHE["timeframe_end"]
    cfg = _load_jobber_config()
    jitter = float(cfg.get("endAt_jitter_minutes", 5))

    if start_iso is not None:
        timeframe: dict = {"startAt": start_iso}
        if end_field and duration_minutes and duration_minutes > 0:
            timeframe[end_field] = compute_end_at(
                start_iso, duration_minutes, jitter_minutes=jitter,
            )
        payload["timeframe"] = timeframe

    assigned_field = JOBBER_FIELD_CACHE["assigned_users"]
    if assigned_field and user_pool is not None and start_iso is not None \
            and duration_minutes and duration_minutes > 0:
        start_dt, _ = _parse_start(start_iso)
        end_dt = start_dt + timedelta(minutes=duration_minutes)
        size = crew_size_for(duration_minutes, user_pool.size)
        if size > 0:
            user_ids = user_pool.assign(start_dt, end_dt, size)
            if user_ids:
                payload[assigned_field] = user_ids

    if extra:
        payload.update(extra)

    return payload
