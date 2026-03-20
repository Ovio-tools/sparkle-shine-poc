"""
automations/triggers.py

Polling functions for the four event sources used by automations.
Each function:
  1. Reads last poll state from poll_state table.
  2. Fetches new records from the source tool.
  3. Updates poll_state before returning.
  4. Returns a list of event dicts for the caller to act on.
"""
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from automations.state import get_last_poll, update_last_poll

# Jobber GraphQL endpoint
_JOBBER_GRAPHQL_URL = os.getenv(
    "JOBBER_BASE_URL", "https://api.getjobber.com/api/graphql"
)

# QuickBooks base URL helper (avoids importing quickbooks_auth at module level)
def _qbo_base_url() -> str:
    from auth.quickbooks_auth import get_base_url
    return get_base_url()


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def _default_since() -> str:
    """Default lookback: 24 hours ago, formatted as ISO-8601 UTC."""
    since = datetime.now(timezone.utc) - timedelta(hours=24)
    return since.strftime("%Y-%m-%dT%H:%M:%S")


# ─────────────────────────────────────────────────────────────────────────────
# 1. Pipedrive: won deals
# ─────────────────────────────────────────────────────────────────────────────

def poll_pipedrive_won_deals(clients: Any, db: sqlite3.Connection) -> list:
    """
    Poll Pipedrive for deals that moved to 'won' since the last poll.

    Returns a list of dicts with keys:
        deal_id, contact_name, contact_email, contact_phone,
        deal_value, client_type, service_type, service_frequency,
        notes
    """
    state = get_last_poll(db, "pipedrive", "deal_won")
    since = state["last_processed_timestamp"] if state else _default_since()

    session = clients("pipedrive")
    base = session.base_url.rstrip("/")
    if not any(seg in base for seg in ("/v1", "/v2")):
        base = f"{base}/v1"

    params = {
        "status": "won",
        "update_time": since,
        "start": 0,
        "limit": 100,
    }

    events = []
    latest_timestamp = since

    while True:
        resp = session.get(f"{base}/deals", params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        deals = data.get("data") or []
        for deal in deals:
            # Skip deals won before our since window
            won_time = deal.get("won_time") or deal.get("update_time", "")
            if won_time > latest_timestamp:
                latest_timestamp = won_time

            # Resolve person details
            person = deal.get("person_id") or {}
            contact_name = (
                person.get("name") if isinstance(person, dict) else None
            ) or deal.get("contact_name", "")

            # Phone / email come back as lists of value objects
            contact_email = ""
            contact_phone = ""
            if isinstance(person, dict):
                emails = person.get("email") or []
                if emails:
                    contact_email = (
                        emails[0].get("value", "") if isinstance(emails[0], dict)
                        else str(emails[0])
                    )
                phones = person.get("phone") or []
                if phones:
                    contact_phone = (
                        phones[0].get("value", "") if isinstance(phones[0], dict)
                        else str(phones[0])
                    )

            # Custom field values (keyed by field hash from tool_ids.json)
            custom = {k: v for k, v in deal.items() if len(k) == 40}

            events.append({
                "deal_id": str(deal.get("id", "")),
                "contact_name": contact_name,
                "contact_email": contact_email,
                "contact_phone": contact_phone,
                "deal_value": deal.get("value") or 0.0,
                "client_type": deal.get("0c33b3b00286f14e71a0e0845a2180d6b524dd39"),
                "service_type": deal.get("29d12ce12832b01642ca5b6b764fed836201ae88"),
                "service_frequency": None,  # not stored as its own field
                "notes": deal.get("title", ""),
            })

        pagination = data.get("additional_data", {}).get("pagination", {})
        if pagination.get("more_items_in_collection"):
            params["start"] = pagination["next_start"]
        else:
            break

    update_last_poll(db, "pipedrive", "deal_won", None, latest_timestamp)
    return events


# ─────────────────────────────────────────────────────────────────────────────
# 2. Jobber: completed jobs
# ─────────────────────────────────────────────────────────────────────────────

_JOBBER_COMPLETED_JOBS_QUERY = """
query CompletedJobs($after: String, $filter: JobFilterAttributes) {
  jobs(after: $after, filter: $filter, first: 50) {
    nodes {
      id
      title
      completedAt
      duration
      instructions
      recurring
      client {
        id
        name
      }
      assignedTeam {
        teamMembers {
          nodes {
            name
          }
        }
      }
      jobType
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""


def poll_jobber_completed_jobs(clients: Any, db: sqlite3.Connection) -> list:
    """
    Poll Jobber (GraphQL) for jobs completed since the last poll.

    Returns a list of dicts with keys:
        job_id, client_id, service_type, duration_minutes,
        crew, completion_notes, is_recurring
    """
    state = get_last_poll(db, "jobber", "completed_job")
    since = state["last_processed_timestamp"] if state else _default_since()

    session = clients("jobber")
    events = []
    latest_timestamp = since
    cursor = None

    while True:
        variables: dict = {
            "filter": {
                "completedAtOrAfter": since,
            }
        }
        if cursor:
            variables["after"] = cursor

        payload = {
            "query": _JOBBER_COMPLETED_JOBS_QUERY,
            "variables": variables,
        }
        resp = session.post(_JOBBER_GRAPHQL_URL, json=payload, timeout=20)
        resp.raise_for_status()
        body = resp.json()

        jobs_data = (
            body.get("data", {}).get("jobs", {}) or {}
        )
        nodes = jobs_data.get("nodes") or []

        for job in nodes:
            completed_at = job.get("completedAt") or ""
            if completed_at > latest_timestamp:
                latest_timestamp = completed_at

            # Crew: join first names of team members
            team_members = (
                job.get("assignedTeam", {})
                .get("teamMembers", {})
                .get("nodes", [])
            ) or []
            crew_names = [m.get("name", "") for m in team_members]
            crew = ", ".join(crew_names) if crew_names else None

            # Duration: Jobber returns seconds; convert to minutes
            raw_duration = job.get("duration") or 0
            duration_minutes = round(raw_duration / 60) if raw_duration else None

            events.append({
                "job_id": str(job.get("id", "")),
                "client_id": str((job.get("client") or {}).get("id", "")),
                "service_type": job.get("jobType") or job.get("title", ""),
                "duration_minutes": duration_minutes,
                "crew": crew,
                "completion_notes": job.get("instructions", ""),
                "is_recurring": bool(job.get("recurring", False)),
            })

        page_info = jobs_data.get("pageInfo", {}) or {}
        if page_info.get("hasNextPage"):
            cursor = page_info.get("endCursor")
        else:
            break

    update_last_poll(db, "jobber", "completed_job", None, latest_timestamp)
    return events


# ─────────────────────────────────────────────────────────────────────────────
# 3. QuickBooks: payments
# ─────────────────────────────────────────────────────────────────────────────

def poll_quickbooks_payments(clients: Any, db: sqlite3.Connection) -> list:
    """
    Poll QuickBooks for new payments since the last poll.

    Returns a list of dicts with keys:
        payment_id, amount, date, method, invoice_id, customer_id
    """
    state = get_last_poll(db, "quickbooks", "payment")
    since = state["last_processed_timestamp"] if state else _default_since()

    headers = clients("quickbooks")
    base_url = _qbo_base_url()

    query = (
        f"SELECT * FROM Payment "
        f"WHERE MetaData.LastUpdatedTime > '{since}' "
        f"MAXRESULTS 100"
    )
    params = {"query": query, "minorversion": "65"}

    resp = __import__("requests").get(
        f"{base_url}/query",
        headers=headers,
        params=params,
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()

    payments_raw = (
        data.get("QueryResponse", {}).get("Payment") or []
    )

    events = []
    latest_timestamp = since

    for pmt in payments_raw:
        updated = (
            pmt.get("MetaData", {}).get("LastUpdatedTime") or ""
        )
        if updated > latest_timestamp:
            latest_timestamp = updated

        # Linked invoice ID (first line, if present)
        invoice_id = ""
        lines = pmt.get("Line") or []
        if lines:
            linked_txn = (lines[0].get("LinkedTxn") or [{}])[0]
            invoice_id = str(linked_txn.get("TxnId", ""))

        events.append({
            "payment_id": str(pmt.get("Id", "")),
            "amount": float(pmt.get("TotalAmt", 0)),
            "date": pmt.get("TxnDate", ""),
            "method": pmt.get("PaymentMethodRef", {}).get("name", ""),
            "invoice_id": invoice_id,
            "customer_id": str(
                (pmt.get("CustomerRef") or {}).get("value", "")
            ),
        })

    update_last_poll(db, "quickbooks", "payment", None, latest_timestamp)
    return events


# ─────────────────────────────────────────────────────────────────────────────
# 4. Google Sheets: negative reviews
# ─────────────────────────────────────────────────────────────────────────────

# Expected column order in the reviews sheet (0-indexed):
# date | client_name | client_email | rating | review_text | crew | service_type
_REVIEWS_COL = {
    "date": 0,
    "client_name": 1,
    "client_email": 2,
    "rating": 3,
    "review_text": 4,
    "crew": 5,
    "service_type": 6,
}

# Sheet ID: pulled from GOOGLE_REVIEWS_SHEET_ID env var or tool_ids.json
def _reviews_sheet_id() -> str:
    sheet_id = os.getenv("GOOGLE_REVIEWS_SHEET_ID", "")
    if not sheet_id:
        import json
        _ids_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "config", "tool_ids.json"
        )
        with open(_ids_path) as f:
            tool_ids = json.load(f)
        sheet_id = (
            tool_ids.get("google", {})
            .get("sheets", {})
            .get("reviews_sheet", "")
        )
    if not sheet_id:
        raise ValueError(
            "Reviews sheet ID not configured. "
            "Set GOOGLE_REVIEWS_SHEET_ID in .env or add "
            "'reviews_sheet' under google.sheets in config/tool_ids.json."
        )
    return sheet_id


def poll_sheets_negative_reviews(clients: Any, db: sqlite3.Connection) -> list:
    """
    Read the reviews Google Sheet and return rows with rating <= 2
    that haven't been processed yet.

    poll_state tracks last_processed_id as the last processed row index
    (1-based, header = row 1, data starts at row 2).

    Returns a list of dicts with keys:
        row_index, date, client_name, client_email,
        rating, review_text, crew, service_type
    """
    state = get_last_poll(db, "google_sheets", "negative_review")
    last_row_index = int(state["last_processed_id"] or 1) if state else 1

    service = clients("google_sheets")
    sheet_id = _reviews_sheet_id()

    result = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=sheet_id, range="A:G")
        .execute()
    )
    rows = result.get("values", [])

    if len(rows) <= 1:
        # Only a header row or empty
        update_last_poll(db, "google_sheets", "negative_review", str(last_row_index), None)
        return []

    events = []
    latest_row_index = last_row_index

    # rows[0] = header; data starts at rows[1] (row index 2 in sheet)
    for sheet_row_index, row in enumerate(rows[1:], start=2):
        if sheet_row_index <= last_row_index:
            continue

        # Pad row to expected width
        padded = row + [""] * (7 - len(row))

        try:
            rating = int(padded[_REVIEWS_COL["rating"]])
        except (ValueError, TypeError):
            latest_row_index = sheet_row_index
            continue

        if rating <= 2:
            events.append({
                "row_index": sheet_row_index,
                "date": padded[_REVIEWS_COL["date"]],
                "client_name": padded[_REVIEWS_COL["client_name"]],
                "client_email": padded[_REVIEWS_COL["client_email"]],
                "rating": rating,
                "review_text": padded[_REVIEWS_COL["review_text"]],
                "crew": padded[_REVIEWS_COL["crew"]],
                "service_type": padded[_REVIEWS_COL["service_type"]],
            })

        latest_row_index = sheet_row_index

    update_last_poll(
        db, "google_sheets", "negative_review", str(latest_row_index), None
    )
    return events
