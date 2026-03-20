"""
Push Sparkle & Shine clients, recurring agreements, and jobs to Jobber
via GraphQL.

Full run:     python seeding/pushers/push_jobber.py
Dry run:      python seeding/pushers/push_jobber.py --dry-run

Push order (Jobber requires client → property → agreement/job):
  Phase 1 — Clients          (320 total: 310 residential + 10 commercial)
  Phase 2 — Recurring agreements (~220, mapped to Jobber recurring jobs)
  Phase 3 — Jobs             (~8,200, all statuses, chronological order)

Auth is handled by auth/jobber_auth.py (token file → auto-refresh → env fallback).
"""

import json
import os
import sys
from typing import Optional

import requests

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from auth.jobber_auth import get_jobber_session                                       # noqa: E402
from database.schema import get_connection                                             # noqa: E402
from database.mappings import find_unmapped                                            # noqa: E402
from seeding.utils.checkpoint import CheckpointIterator, load_checkpoint, clear_checkpoint  # noqa: E402
from seeding.utils.throttler import JOBBER                                             # noqa: E402

_DB_PATH = os.path.join(_PROJECT_ROOT, "sparkle_shine.db")
JOBBER_API_URL = "https://api.getjobber.com/api/graphql"

# Jobber job status for jobs that start as active/open (created via jobCreate)
_JOB_STATUS_MAP = {
    "completed": "completed",   # create then close
    "cancelled": "cancelled",   # create then archive/cancel
    "no-show":   "cancelled",
    "scheduled": "scheduled",   # leave open
}


# ---------------------------------------------------------------------------
# Low-level GraphQL helper
# ---------------------------------------------------------------------------

def _gql(session: requests.Session, query: str, variables: dict) -> dict:
    """Execute a GraphQL request and return the parsed response JSON.

    On a 401 the access token has expired; refresh it and retry once.
    """
    JOBBER.wait()
    resp = session.post(
        JOBBER_API_URL,
        json={"query": query, "variables": variables},
        timeout=30,
    )
    if resp.status_code == 401:
        # Token expired mid-run — refresh and retry
        new_session = get_jobber_session()
        session.headers.update(new_session.headers)
        JOBBER.wait()
        resp = session.post(
            JOBBER_API_URL,
            json={"query": query, "variables": variables},
            timeout=30,
        )
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Mutation / query strings
# ---------------------------------------------------------------------------

_CLIENT_CREATE_MUTATION = """
mutation ClientCreate($input: ClientCreateInput!) {
  clientCreate(input: $input) {
    client {
      id
      firstName
      lastName
      companyName
    }
    userErrors {
      message
      path
    }
  }
}
"""

_PROPERTY_CREATE_MUTATION = """
mutation PropertyCreate($clientId: EncodedId!, $input: PropertyCreateInput!) {
  propertyCreate(clientId: $clientId, input: $input) {
    properties {
      id
    }
    userErrors {
      message
      path
    }
  }
}
"""

_JOB_CREATE_MUTATION = """
mutation JobCreate($input: JobCreateAttributes!) {
  jobCreate(input: $input) {
    job {
      id
      title
      jobStatus
    }
    userErrors {
      message
      path
    }
  }
}
"""

_JOB_CLOSE_MUTATION = """
mutation CloseJob($jobId: EncodedId!, $input: JobCloseInput!) {
  jobClose(jobId: $jobId, input: $input) {
    job {
      id
      jobStatus
    }
    userErrors {
      message
      path
    }
  }
}
"""

_CLIENT_ARCHIVE_MUTATION = """
mutation ArchiveClient($id: EncodedId!) {
  clientArchive(id: $id) {
    client {
      id
    }
    userErrors {
      message
      path
    }
  }
}
"""

_DRY_RUN_VERIFY_QUERY = """
query DryRunCheck($id: EncodedId!) {
  client(id: $id) {
    id
    firstName
    lastName
    email
  }
}
"""

# Fetch a client's first property ID from Jobber (used for backfill)
_CLIENT_PROPERTIES_QUERY = """
query ClientProperties($id: EncodedId!) {
  client(id: $id) {
    id
    clientProperties {
      nodes { id }
    }
  }
}
"""

# Count query to verify total clients synced in Jobber
_CLIENTS_COUNT_QUERY = """
query ClientsCount {
  clients {
    totalCount
  }
}
"""

# Introspection query to discover whether a named mutation exists
_MUTATION_EXISTS_QUERY = """
query MutationExists($name: String!) {
  __type(name: "Mutation") {
    fields(includeDeprecated: true) {
      name
    }
  }
}
"""

# Recurring job mutation — Jobber represents recurring agreements as jobs
# with a `recurrences` field on JobCreateAttributes.
# We introspect the schema at runtime to find the right field name.
_RECURRING_JOB_CREATE_MUTATION = """
mutation RecurringJobCreate($input: JobCreateAttributes!) {
  jobCreate(input: $input) {
    job {
      id
      title
      jobStatus
    }
    userErrors {
      message
      path
    }
  }
}
"""

# Introspect JobCreateAttributes to discover recurrence field names
_JOB_CREATE_ATTRS_QUERY = """
query JobCreateAttrs {
  __type(name: "JobCreateAttributes") {
    fields: inputFields {
      name
      type {
        name
        kind
        ofType {
          name
          kind
        }
      }
    }
  }
}
"""


# ---------------------------------------------------------------------------
# Input builders
# ---------------------------------------------------------------------------

def _client_input(c: dict) -> dict:
    """Build a ClientCreateInput dict from a SQLite clients row."""
    inp: dict = {}

    if c["client_type"] == "residential":
        inp["firstName"] = c["first_name"] or ""
        inp["lastName"] = c["last_name"] or ""
    else:
        inp["companyName"] = c["company_name"] or ""
        if c["first_name"]:
            inp["firstName"] = c["first_name"]
        if c["last_name"]:
            inp["lastName"] = c["last_name"]

    if c["email"]:
        inp["emails"] = [{"description": "MAIN", "address": c["email"]}]

    if c["phone"]:
        inp["phones"] = [{"description": "MAIN", "number": c["phone"]}]

    if c["address"]:
        inp["billingAddress"] = {
            "street1": c["address"],
            "city": "Austin",
            "province": "TX",
            "country": "US",
        }

    return inp


def _property_input(c: dict) -> dict:
    """Build a PropertyCreateInput dict from a SQLite clients row."""
    address = c.get("address") or ""
    parts = [p.strip() for p in address.split(",")]
    street1 = parts[0] if parts else address
    city = parts[1] if len(parts) > 1 else "Austin"
    state_zip = parts[2].strip() if len(parts) > 2 else "TX"
    state_parts = state_zip.split()
    province = state_parts[0] if state_parts else "TX"
    postal = state_parts[1] if len(state_parts) > 1 else ""

    prop: dict = {
        "street1": street1,
        "city": city,
        "province": province,
        "country": "US",
    }
    if postal:
        prop["postalCode"] = postal

    return {"properties": [{"address": prop}]}


def _job_input(j: dict, jobber_property_id: str) -> dict:
    """Build a JobCreateAttributes dict from a SQLite jobs row and Jobber property ID."""
    inp: dict = {
        "propertyId": jobber_property_id,
        "title": j["service_type_id"].replace("-", " ").title(),
        "invoicing": {
            "invoicingType": "FIXED_PRICE",
            "invoicingSchedule": "ON_COMPLETION",
        },
    }

    if j["scheduled_date"]:
        inp["timeframe"] = {"startAt": j["scheduled_date"]}

    if j.get("notes"):
        inp["instructions"] = j["notes"][:500]

    return inp


# Frequency mapping: SQLite → Jobber RecurrenceInput frequency enum values
_FREQ_MAP = {
    "weekly":    {"type": "WEEKLY",    "interval": 1},
    "biweekly":  {"type": "WEEKLY",    "interval": 2},
    "monthly":   {"type": "MONTHLY",   "interval": 1},
}


def _recurring_job_input(
    agreement: dict,
    jobber_property_id: str,
    recurrence_field: Optional[str],
) -> dict:
    """
    Build a JobCreateAttributes dict for a recurring agreement.

    recurrence_field is the actual field name on JobCreateAttributes that
    accepts recurrence config (discovered at runtime via introspection).
    Pass None to skip recurrence — the job will be created as a one-time job.
    """
    freq_config = _FREQ_MAP.get(agreement["frequency"])
    title = agreement["service_type_id"].replace("-", " ").title()

    inp: dict = {
        "propertyId": jobber_property_id,
        "title": f"Recurring: {title}",
        "invoicing": {
            "invoicingType": "FIXED_PRICE",
            "invoicingSchedule": "ON_COMPLETION",
        },
    }

    if agreement["start_date"]:
        inp["timeframe"] = {"startAt": agreement["start_date"]}

    if recurrence_field and freq_config:
        inp[recurrence_field] = freq_config

    return inp


# ---------------------------------------------------------------------------
# Schema introspection (one-time, cached in process)
# ---------------------------------------------------------------------------

_recurrence_field_cache: Optional[str] = None
_recurrence_field_checked = False


def _discover_recurrence_field(session: requests.Session) -> Optional[str]:
    """
    Introspect JobCreateAttributes to find the recurrence input field, if any.
    Returns the field name (e.g. 'recurrences', 'repeat', 'schedule') or None.
    Caches the result for the lifetime of the process.
    """
    global _recurrence_field_cache, _recurrence_field_checked
    if _recurrence_field_checked:
        return _recurrence_field_cache

    _recurrence_field_checked = True
    try:
        resp = _gql(session, _JOB_CREATE_ATTRS_QUERY, {})
        fields = resp.get("data", {}).get("__type", {}).get("fields", []) or []
        candidates = {"recurrences", "recurrence", "repeat", "repeats", "schedule", "schedules"}
        for f in fields:
            if f["name"] in candidates:
                _recurrence_field_cache = f["name"]
                print(f"[push_jobber] Recurrence field discovered: '{f['name']}'")
                break
        else:
            print("[push_jobber] No recurrence field found on JobCreateAttributes — "
                  "recurring agreements will be pushed as one-time jobs.")
    except Exception as exc:
        print(f"[push_jobber] Schema introspection for recurrence field failed: {exc}")

    return _recurrence_field_cache


# ---------------------------------------------------------------------------
# Database fetch helpers
# ---------------------------------------------------------------------------

def _fetch_all_clients(conn) -> list:
    rows = conn.execute("""
        SELECT * FROM clients
        ORDER BY client_type ASC, id ASC
    """).fetchall()
    return [dict(r) for r in rows]


def _fetch_all_agreements(conn) -> list:
    rows = conn.execute("""
        SELECT * FROM recurring_agreements
        ORDER BY id ASC
    """).fetchall()
    return [dict(r) for r in rows]


def _fetch_all_jobs_chronological(conn) -> list:
    rows = conn.execute("""
        SELECT * FROM jobs
        ORDER BY scheduled_date ASC, id ASC
    """).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Mapping helpers
# ---------------------------------------------------------------------------

def _register_mapping(
    conn,
    canonical_id: str,
    entity_type: str,
    jobber_id: str,
    url: Optional[str] = None,
) -> None:
    conn.execute("""
        INSERT OR REPLACE INTO cross_tool_mapping
            (canonical_id, entity_type, tool_name, tool_specific_id, tool_specific_url, synced_at)
        VALUES (?, ?, 'jobber', ?, ?, datetime('now'))
    """, (canonical_id, entity_type, jobber_id, url))
    conn.commit()


def _lookup_jobber_id(conn, canonical_id: str) -> Optional[str]:
    row = conn.execute("""
        SELECT tool_specific_id FROM cross_tool_mapping
        WHERE canonical_id = ? AND tool_name = 'jobber'
    """, (canonical_id,)).fetchone()
    return row[0] if row else None


def _lookup_jobber_property_id(conn, canonical_client_id: str) -> Optional[str]:
    """Return the Jobber property ID stored in tool_specific_url for this client."""
    row = conn.execute("""
        SELECT tool_specific_url FROM cross_tool_mapping
        WHERE canonical_id = ? AND tool_name = 'jobber'
    """, (canonical_client_id,)).fetchone()
    return row[0] if row else None


# ---------------------------------------------------------------------------
# Backfill helper
# ---------------------------------------------------------------------------

def _backfill_property_ids(session: requests.Session, conn) -> int:
    """
    For any client already in cross_tool_mapping without a property ID stored in
    tool_specific_url, query Jobber to fetch the client's first property and save it.

    This is idempotent and only runs against rows where tool_specific_url IS NULL.
    Returns the number of rows updated.
    """
    rows = conn.execute("""
        SELECT canonical_id, tool_specific_id
        FROM cross_tool_mapping
        WHERE tool_name = 'jobber'
          AND entity_type = 'CLIENT'
          AND tool_specific_url IS NULL
    """).fetchall()

    if not rows:
        return 0

    print(f"  [backfill] Fetching property IDs for {len(rows)} already-mapped clients...")
    updated = 0
    for row in rows:
        canonical_id = row[0]
        jobber_client_id = row[1]
        try:
            resp = _gql(session, _CLIENT_PROPERTIES_QUERY, {"id": jobber_client_id})
            client_node = resp.get("data", {}).get("client") or {}
            nodes = client_node.get("clientProperties", {}).get("nodes", [])
            if nodes:
                prop_id = nodes[0]["id"]
                conn.execute("""
                    UPDATE cross_tool_mapping
                    SET tool_specific_url = ?
                    WHERE canonical_id = ? AND tool_name = 'jobber'
                """, (prop_id, canonical_id))
                conn.commit()
                updated += 1
        except Exception as exc:
            print(f"  [WARN] backfill property for {canonical_id}: {exc}")

    print(f"  [backfill] Updated {updated}/{len(rows)} client property IDs")
    return updated


# ---------------------------------------------------------------------------
# Phase helpers
# ---------------------------------------------------------------------------

def _push_clients(session: requests.Session, conn, clients: list) -> dict:
    """
    Phase 1: push all clients to Jobber.

    Returns a dict mapping canonical client ID → {"client_id": ..., "property_id": ...}
    for all successfully created clients.
    """
    # Recover property IDs for any clients mapped in a previous interrupted run
    _backfill_property_ids(session, conn)

    created: dict = {}
    pushed = 0
    skipped_dup = 0
    skipped_err = 0

    for client in CheckpointIterator("push_jobber_clients", clients):
        # Idempotent: skip if already mapped; recover property_id from tool_specific_url
        existing = _lookup_jobber_id(conn, client["id"])
        if existing:
            prop_id = _lookup_jobber_property_id(conn, client["id"])
            created[client["id"]] = {"client_id": existing, "property_id": prop_id}
            continue

        resp = _gql(session, _CLIENT_CREATE_MUTATION, {"input": _client_input(client)})
        top_errors = resp.get("errors", [])
        if top_errors:
            msg = top_errors[0].get("message", "")
            if "already exists" in msg.lower() or "duplicate" in msg.lower():
                print(f"  [SKIP] {client['id']} — duplicate email in Jobber")
                skipped_dup += 1
            else:
                print(f"  [ERROR] {client['id']} — {top_errors}")
                skipped_err += 1
            continue

        payload = resp.get("data", {}).get("clientCreate", {})
        user_errors = payload.get("userErrors", [])
        if user_errors:
            msg = " ".join(e.get("message", "") for e in user_errors)
            if "already" in msg.lower() or "duplicate" in msg.lower() or "taken" in msg.lower():
                print(f"  [SKIP] {client['id']} — duplicate email: {msg}")
                skipped_dup += 1
            else:
                print(f"  [WARN] clientCreate {client['id']}: {user_errors}")
                skipped_err += 1
            continue

        client_node = payload.get("client")
        if not client_node:
            print(f"  [ERROR] {client['id']} — clientCreate returned no client node")
            skipped_err += 1
            continue

        jobber_client_id = client_node["id"]
        pushed += 1

        # Create service property so jobs can be linked via propertyId
        prop_resp = _gql(session, _PROPERTY_CREATE_MUTATION, {
            "clientId": jobber_client_id,
            "input": _property_input(client),
        })
        prop_payload = prop_resp.get("data", {}).get("propertyCreate", {})
        prop_nodes = prop_payload.get("properties", [])
        jobber_property_id = prop_nodes[0]["id"] if prop_nodes else None

        if not jobber_property_id:
            print(f"  [WARN] No property returned for {client['id']}: "
                  f"{prop_payload.get('userErrors')}")

        # Store property_id in tool_specific_url so it survives across restarts
        _register_mapping(conn, client["id"], "CLIENT", jobber_client_id, url=jobber_property_id)

        created[client["id"]] = {
            "client_id": jobber_client_id,
            "property_id": jobber_property_id,
        }

    clear_checkpoint("push_jobber_clients")
    total = len(clients)
    print(f"  Clients: {pushed} pushed, {skipped_dup} duplicate-skipped, "
          f"{skipped_err} error-skipped / {total} total")
    return created


def _push_agreements(
    session: requests.Session,
    conn,
    agreements: list,
    client_map: dict,
) -> int:
    """
    Phase 2: push recurring agreements to Jobber as recurring jobs.

    client_map: canonical_client_id → {"client_id": ..., "property_id": ...}
    Returns count of successfully created recurring jobs.
    """
    recurrence_field = _discover_recurrence_field(session)
    pushed = 0
    skipped = 0

    for agreement in CheckpointIterator("push_jobber_agreements", agreements):
        # Idempotent
        if _lookup_jobber_id(conn, agreement["id"]):
            continue

        client_entry = client_map.get(agreement["client_id"])
        if not client_entry:
            # Client wasn't pushed (duplicate/error) — look up from DB
            existing_cid = _lookup_jobber_id(conn, agreement["client_id"])
            if existing_cid:
                # We have the client ID but no property; skip agreement
                print(f"  [SKIP] Agreement {agreement['id']} — "
                      f"no property ID for client {agreement['client_id']}")
                skipped += 1
                continue
            else:
                print(f"  [SKIP] Agreement {agreement['id']} — "
                      f"client {agreement['client_id']} not in Jobber")
                skipped += 1
                continue

        jobber_property_id = client_entry.get("property_id")
        if not jobber_property_id:
            print(f"  [SKIP] Agreement {agreement['id']} — no property ID available")
            skipped += 1
            continue

        inp = _recurring_job_input(agreement, jobber_property_id, recurrence_field)
        resp = _gql(session, _RECURRING_JOB_CREATE_MUTATION, {"input": inp})
        top_errors = resp.get("errors", [])
        if top_errors:
            print(f"  [ERROR] Agreement {agreement['id']}: {top_errors}")
            skipped += 1
            continue

        payload = resp.get("data", {}).get("jobCreate", {})
        user_errors = payload.get("userErrors", [])
        if user_errors:
            print(f"  [WARN] Agreement {agreement['id']}: {user_errors}")
            skipped += 1
            continue

        job_node = payload.get("job")
        if not job_node:
            print(f"  [ERROR] Agreement {agreement['id']} — jobCreate returned no job node")
            skipped += 1
            continue

        _register_mapping(conn, agreement["id"], "RECUR", job_node["id"])
        pushed += 1

    clear_checkpoint("push_jobber_agreements")
    total = len(agreements)
    print(f"  Agreements: {pushed} pushed, {skipped} skipped / {total} total")
    return pushed


def _push_jobs(
    session: requests.Session,
    conn,
    jobs: list,
    client_map: dict,
) -> int:
    """
    Phase 3: push all jobs to Jobber in chronological order.

    For completed jobs: create via jobCreate, then close via jobClose.
    For scheduled jobs: create via jobCreate (leave open).
    For cancelled/no-show: create via jobCreate, then close.

    client_map: canonical_client_id → {"client_id": ..., "property_id": ...}
    Returns count of successfully created jobs.
    """
    pushed = 0
    skipped = 0

    for job in CheckpointIterator("push_jobber_jobs", jobs, id_field="id"):
        # Idempotent
        if _lookup_jobber_id(conn, job["id"]):
            continue

        client_entry = client_map.get(job["client_id"])
        jobber_property_id = None

        if client_entry:
            jobber_property_id = client_entry.get("property_id")
        else:
            # Client was a duplicate skip — try to look up property via DB mapping
            # (We stored client_id in cross_tool_mapping but not property_id)
            # Best effort: skip job if we can't find a property
            jobber_client_id = _lookup_jobber_id(conn, job["client_id"])
            if not jobber_client_id:
                skipped += 1
                continue
            # No property ID available for duplicate-skipped clients; skip
            skipped += 1
            continue

        if not jobber_property_id:
            skipped += 1
            continue

        resp = _gql(session, _JOB_CREATE_MUTATION, {
            "input": _job_input(job, jobber_property_id)
        })
        top_errors = resp.get("errors", [])
        if top_errors:
            print(f"  [ERROR] Job {job['id']}: {top_errors}")
            skipped += 1
            continue

        payload = resp.get("data", {}).get("jobCreate", {})
        user_errors = payload.get("userErrors", [])
        if user_errors:
            print(f"  [WARN] Job {job['id']}: {user_errors}")
            skipped += 1
            continue

        job_node = payload.get("job")
        if not job_node:
            print(f"  [ERROR] Job {job['id']} — jobCreate returned no job node")
            skipped += 1
            continue

        jobber_job_id = job_node["id"]
        _register_mapping(conn, job["id"], "JOB", jobber_job_id)
        pushed += 1

        # Close completed, cancelled, and no-show jobs.
        # JobCloseInput has one required field: modifyIncompleteVisitsBy
        # (IncompleteVisitDecisionEnum: DESTROY_ALL | COMPLETE_PAST_DESTROY_FUTURE)
        if job["status"] in ("completed", "cancelled", "no-show"):
            close_resp = _gql(session, _JOB_CLOSE_MUTATION, {
                "jobId": jobber_job_id,
                "input": {"modifyIncompleteVisitsBy": "COMPLETE_PAST_DESTROY_FUTURE"},
            })
            close_errors = (
                close_resp.get("data", {}).get("jobClose", {}).get("userErrors", [])
                or close_resp.get("errors", [])
            )
            if close_errors:
                print(f"  [WARN] jobClose {job['id']}: {close_errors}")

    clear_checkpoint("push_jobber_jobs")
    total = len(jobs)
    print(f"  Jobs: {pushed} pushed, {skipped} skipped / {total} total")
    return pushed


def _verify_jobber_count(session: requests.Session) -> Optional[int]:
    """Query Jobber for total client count; return it or None on error."""
    try:
        resp = _gql(session, _CLIENTS_COUNT_QUERY, {})
        return resp.get("data", {}).get("clients", {}).get("totalCount")
    except Exception as exc:
        print(f"  [WARN] Could not fetch Jobber client count: {exc}")
        return None


# ---------------------------------------------------------------------------
# Full run
# ---------------------------------------------------------------------------

def main() -> None:
    conn = get_connection(_DB_PATH)
    session = get_jobber_session()

    print("[push_jobber] Loading records from sparkle_shine.db...")
    clients = _fetch_all_clients(conn)
    agreements = _fetch_all_agreements(conn)
    jobs = _fetch_all_jobs_chronological(conn)
    print(f"  {len(clients)} clients, {len(agreements)} agreements, {len(jobs)} jobs")

    # ------------------------------------------------------------------ #
    # Phase 1 — Clients
    # ------------------------------------------------------------------ #
    print("\n[push_jobber] Phase 1: Clients")
    client_map = _push_clients(session, conn, clients)

    # ------------------------------------------------------------------ #
    # Phase 2 — Recurring Agreements
    # ------------------------------------------------------------------ #
    print("\n[push_jobber] Phase 2: Recurring Agreements")
    agreements_pushed = _push_agreements(session, conn, agreements, client_map)

    # ------------------------------------------------------------------ #
    # Phase 3 — Jobs
    # ------------------------------------------------------------------ #
    print("\n[push_jobber] Phase 3: Jobs (chronological, all statuses)")
    jobs_pushed = _push_jobs(session, conn, jobs, client_map)

    # ------------------------------------------------------------------ #
    # Completion summary
    # ------------------------------------------------------------------ #
    print("\n[push_jobber] === PUSH COMPLETE ===")

    clients_pushed_count = sum(
        1 for v in client_map.values() if v.get("client_id")
    )
    print(f"  Clients pushed:    {clients_pushed_count}/{len(clients)}")
    print(f"  Agreements pushed: {agreements_pushed}/{len(agreements)}")
    print(f"  Jobs pushed:       {jobs_pushed}/{len(jobs)}")

    # Unmapped report
    print()
    for entity_type, label in [("CLIENT", "clients"), ("RECUR", "agreements"), ("JOB", "jobs")]:
        unmapped = find_unmapped(entity_type, "jobber", _DB_PATH)
        if unmapped:
            print(f"  [GAP] {len(unmapped)} {label} missing Jobber mapping "
                  f"(first 5: {unmapped[:5]})")
        else:
            print(f"  [OK]  All {label} mapped")

    # Verify against Jobber API
    jobber_total = _verify_jobber_count(session)
    if jobber_total is not None:
        print(f"\n  Jobber total client count: {jobber_total} "
              f"(expected ~{clients_pushed_count})")

    conn.close()


# ---------------------------------------------------------------------------
# Dry run
# ---------------------------------------------------------------------------

def run_dry_run() -> None:
    """
    Push 5 clients and their first completed jobs to Jobber, print raw GraphQL
    responses, delete all created records from Jobber, and confirm
    cross_tool_mapping was not touched.
    """
    # Part 0 — Warn if an existing checkpoint is present
    existing = load_checkpoint("push_jobber_clients")
    if existing:
        print("WARNING: A push_jobber checkpoint exists from a previous run.")
        print(f"   Last completed ID: {existing.get('last_completed_id')}")
        print("   The dry run will NOT affect this checkpoint.")
        print("   The full run will resume from this checkpoint as normal.\n")

    conn = get_connection(_DB_PATH)
    session = get_jobber_session()

    # Part A — Select test records
    clients = [dict(r) for r in conn.execute("""
        SELECT * FROM clients
        WHERE client_type = 'residential'
        ORDER BY id ASC
        LIMIT 5
    """).fetchall()]

    client_ids = [c["id"] for c in clients]
    placeholders = ",".join("?" * len(client_ids))
    all_jobs_rows = conn.execute(f"""
        SELECT * FROM jobs
        WHERE client_id IN ({placeholders})
          AND status = 'completed'
        ORDER BY client_id ASC, scheduled_date ASC
    """, client_ids).fetchall()

    # One job per client (earliest by scheduled_date)
    seen_clients: set = set()
    jobs = []   # type: list
    for row in all_jobs_rows:
        r = dict(row)
        if r["client_id"] not in seen_clients:
            jobs.append(r)
            seen_clients.add(r["client_id"])
        if len(jobs) == 5:
            break

    job_ids = [j["id"] for j in jobs]

    print("=== DRY RUN: Selected test records ===")
    print(f"Clients: {client_ids}")
    print(f"Jobs:    {job_ids}")
    print()

    # Part B — Push clients
    dry_run_created: dict = {"clients": {}, "jobs": {}}
    clients_pushed = 0

    for client in clients:
        print(f"--- Client mutation: {client['id']} ---")
        resp = _gql(session, _CLIENT_CREATE_MUTATION, {"input": _client_input(client)})
        print(json.dumps(resp, indent=2))

        top_errors = resp.get("errors", [])
        if top_errors:
            print(f"  [ERROR] GraphQL errors: {top_errors}")
            continue

        payload = resp.get("data", {}).get("clientCreate", {})
        errors = payload.get("userErrors", [])
        if errors:
            print(f"  [ERROR] userErrors: {errors}")
            continue

        client_node = payload.get("client")
        if not client_node:
            print("  [ERROR] No client node returned")
            continue
        jobber_id = client_node["id"]

        # Create service property for job linkage
        prop_resp = _gql(session, _PROPERTY_CREATE_MUTATION, {
            "clientId": jobber_id,
            "input": _property_input(client),
        })
        prop_payload = prop_resp.get("data", {}).get("propertyCreate", {})
        prop_nodes = prop_payload.get("properties", [])
        jobber_property_id = prop_nodes[0]["id"] if prop_nodes else None

        dry_run_created["clients"][client["id"]] = {
            "client_id": jobber_id,
            "property_id": jobber_property_id,
        }
        clients_pushed += 1

    # Part C — Push jobs
    jobs_pushed = 0

    for job in jobs:
        client_entry = dry_run_created["clients"].get(job["client_id"])
        if not client_entry:
            print(f"\n  [WARN] Skipping job {job['id']} — client push failed for {job['client_id']}")
            continue
        jobber_property_id = client_entry["property_id"]
        if not jobber_property_id:
            print(f"\n  [WARN] Skipping job {job['id']} — no property ID for {job['client_id']}")
            continue

        print(f"\n--- Job mutation: {job['id']} ---")
        resp = _gql(session, _JOB_CREATE_MUTATION, {
            "input": _job_input(job, jobber_property_id)
        })
        print(json.dumps(resp, indent=2))

        top_errors = resp.get("errors", [])
        if top_errors:
            print(f"  [ERROR] GraphQL errors: {top_errors}")
            continue

        payload = resp.get("data", {}).get("jobCreate", {})
        errors = payload.get("userErrors", [])
        if errors:
            print(f"  [ERROR] userErrors: {errors}")
            continue

        job_node = payload.get("job")
        if not job_node:
            print("  [ERROR] No job node returned")
            continue

        dry_run_created["jobs"][job["id"]] = job_node["id"]
        jobs_pushed += 1

    # Part D — Verify one client is visible in Jobber
    print("\n=== DRY RUN: Verification query ===")
    first_entry = next(iter(dry_run_created["clients"].values()), None)
    first_jobber_client_id = first_entry["client_id"] if first_entry else None
    if first_jobber_client_id:
        verify_resp = _gql(session, _DRY_RUN_VERIFY_QUERY, {"id": first_jobber_client_id})
        print(json.dumps(verify_resp, indent=2))
    else:
        print("  [SKIP] No clients were created — skipping verification query.")

    # Part E — Close jobs then archive clients
    print("\n=== DRY RUN: Deleting test records from Jobber ===")
    jobs_deleted = 0
    for canonical_id, jobber_job_id in dry_run_created["jobs"].items():
        resp = _gql(session, _JOB_CLOSE_MUTATION, {
            "jobId": jobber_job_id,
            "input": {"modifyIncompleteVisitsBy": "COMPLETE_PAST_DESTROY_FUTURE"},
        })
        print(f"Closed job {canonical_id} (Jobber {jobber_job_id}): "
              f"{resp.get('data', {}).get('jobClose', {})}")
        errors = resp.get("data", {}).get("jobClose", {}).get("userErrors", [])
        if not errors:
            jobs_deleted += 1

    clients_deleted = 0
    for canonical_id, entry in dry_run_created["clients"].items():
        jobber_client_id = entry["client_id"]
        resp = _gql(session, _CLIENT_ARCHIVE_MUTATION, {"id": jobber_client_id})
        payload = resp.get("data", {}).get("clientArchive", {})
        errors = payload.get("userErrors", [])
        if errors:
            print(f"Archive client {canonical_id} (Jobber {jobber_client_id}): "
                  f"errors={errors}")
        else:
            print(f"Archived client {canonical_id} (Jobber {jobber_client_id}): OK")
            clients_deleted += 1

    # Part F — Confirm cross_tool_mapping is clean
    all_canonical_ids = client_ids + job_ids
    placeholders2 = ",".join("?" * len(all_canonical_ids))
    unmapped_check = conn.execute(f"""
        SELECT COUNT(*) AS cnt FROM cross_tool_mapping
        WHERE tool_name = 'jobber'
          AND canonical_id IN ({placeholders2})
    """, all_canonical_ids).fetchone()
    mapping_clean = unmapped_check["cnt"] == 0

    if mapping_clean:
        print("\nOK cross_tool_mapping is clean — no dry-run records written")
    else:
        print(f"\nWARNING {unmapped_check['cnt']} unexpected rows in cross_tool_mapping")
        print("   Run the following to clean up before the full run:")
        print(f"   DELETE FROM cross_tool_mapping WHERE tool_name='jobber'")
        print(f"   AND canonical_id IN ({', '.join(repr(i) for i in all_canonical_ids)});")

    conn.close()

    # Part G — Summary
    print("\n=== DRY RUN COMPLETE ===")
    print(f"Clients pushed:   {clients_pushed}/5")
    print(f"Jobs pushed:      {jobs_pushed}/5")
    print(f"Clients deleted:  {clients_deleted}/5")
    print(f"Jobs deleted:     {jobs_deleted}/5")
    print(f"Mapping clean:    {'YES' if mapping_clean else 'NO'}")
    print()
    print(f"GraphQL schema: {'OK Valid' if clients_pushed > 0 else 'FAIL Check errors above'}")
    print(f"Auth/headers:   {'OK Working' if clients_pushed > 0 else 'FAIL Check errors above'}")
    print(f"Job linkage:    {'OK Working' if jobs_pushed > 0 else 'FAIL Check errors above'}")
    print()
    if clients_pushed > 0 and jobs_pushed > 0:
        print("If all items above show OK, run the full push with:")
        print("  python seeding/pushers/push_jobber.py")
    else:
        print("Review the errors above before attempting a full run.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Push Sparkle & Shine data to Jobber.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Push 5 clients and 5 jobs, print raw responses, then delete and exit.",
    )
    args = parser.parse_args()

    if args.dry_run:
        run_dry_run()
    else:
        main()
