#!/usr/bin/env python3
"""Repair completed jobs that are missing invoice links in reconciliation.

This script addresses four production-safe cases:

1. Clients mapped under ``quickbooks_customer`` but not ``quickbooks``
2. Jobs with a local invoice row that was created without ``job_id``
3. Jobs that already have a matching QBO invoice but no local invoice/mapping
4. Jobs with no invoice at all, which need a QBO invoice plus a local record

Usage:
    python scripts/remediate_reconciliation_invoices.py --dry-run
    python scripts/remediate_reconciliation_invoices.py --execute
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import requests

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from auth.quickbooks_auth import get_base_url, get_quickbooks_headers
from automations.utils.id_resolver import register_mapping
from database.schema import get_connection
from seeding.generators.gen_clients import get_commercial_per_visit_rate
from seeding.utils.throttler import QUICKBOOKS

logger = logging.getLogger("remediate_reconciliation_invoices")

_INVOICE_ID_LOCK_KEY = 9_214_001
_QBO_NET30_TERM_ID = "3"
_DEFAULT_QBO_ITEM_ID = "19"
_DEFAULT_PRICE = 150.0

_SERVICE_TO_PRICE = {
    "std-residential": 150.0,
    "deep-clean": 275.0,
    "move-in-out": 325.0,
    "recurring-weekly": 135.0,
    "recurring-biweekly": 150.0,
    "recurring-monthly": 165.0,
}

_SERVICE_TO_QBO_ITEM = {
    "std-residential": "19",
    "deep-clean": "20",
    "move-in-out": "21",
    "recurring-weekly": "22",
    "recurring-biweekly": "23",
    "recurring-monthly": "24",
    "commercial-nightly": "25",
}


def _qbo_escape(value: str) -> str:
    return (value or "").replace("\\", "\\\\").replace("'", "\\'")


def _client_canonical_id(record: dict) -> str:
    return record.get("client_id") or record["id"]


def _customer_display_name(client: dict) -> str:
    if client["client_type"] == "commercial" and client.get("company_name"):
        return client["company_name"]

    first = (client.get("first_name") or "").strip()
    last = (client.get("last_name") or "").strip()
    combined = f"{first} {last}".strip()
    return combined or (client.get("email") or client["id"])


def _customer_body(client: dict) -> dict:
    canonical_id = _client_canonical_id(client)
    body: dict = {
        "DisplayName": _customer_display_name(client),
        "Notes": f"SS-ID: {canonical_id}",
    }
    if client.get("email"):
        body["PrimaryEmailAddr"] = {"Address": client["email"]}
    if client.get("phone"):
        body["PrimaryPhone"] = {"FreeFormNumber": client["phone"]}
    if client["client_type"] == "commercial":
        body["CompanyName"] = _customer_display_name(client)
        body["SalesTermRef"] = {"value": _QBO_NET30_TERM_ID}
    return body


def _invoice_details_for_job(job: dict) -> dict:
    issue_date = date.fromisoformat(str(job["service_date"])[:10])
    if job["client_type"] == "commercial":
        amount = round(
            get_commercial_per_visit_rate(
                client_id=job["client_id"],
                job_date=job["scheduled_date"],
                service_type_id=job["service_type_id"],
            ),
            2,
        )
        due_date = issue_date + timedelta(days=30)
    else:
        amount = round(_SERVICE_TO_PRICE.get(job["service_type_id"], _DEFAULT_PRICE), 2)
        due_date = issue_date

    return {
        "issue_date": issue_date.isoformat(),
        "due_date": due_date.isoformat(),
        "amount": amount,
        "qbo_item_id": _SERVICE_TO_QBO_ITEM.get(
            job["service_type_id"], _DEFAULT_QBO_ITEM_ID
        ),
    }


def _pick_unique_candidate(candidates: list[dict], expected_amount: float) -> Optional[dict]:
    exact_matches = [
        row for row in candidates
        if abs(float(row["amount"]) - float(expected_amount)) < 0.01
    ]
    return exact_matches[0] if len(exact_matches) == 1 else None


def _qbo_request(method: str, path: str, **kwargs) -> dict:
    url = f"{get_base_url()}{path}"
    headers = get_quickbooks_headers()

    for attempt in (1, 2):
        QUICKBOOKS.wait()
        response = requests.request(
            method,
            url,
            headers=headers,
            timeout=30,
            **kwargs,
        )
        if response.status_code == 401 and attempt == 1:
            headers = get_quickbooks_headers()
            continue
        response.raise_for_status()
        data = response.json()
        if "Fault" in data:
            error = (data["Fault"].get("Error") or [{}])[0]
            raise RuntimeError(
                "QBO fault "
                f"[{error.get('code', '')}] {error.get('Message', 'Unknown error')}: "
                f"{error.get('Detail', '')}"
            )
        return data

    raise RuntimeError(f"QuickBooks {method.upper()} {path} failed after token refresh retry")


def _qbo_query(sql: str) -> dict:
    return _qbo_request(
        "get",
        "/query",
        params={"query": sql, "minorversion": "65"},
    ).get("QueryResponse", {})


def _fetch_missing_jobs(db, limit: Optional[int]) -> list[dict]:
    sql = """
        SELECT
            j.id,
            j.client_id,
            j.service_type_id,
            j.scheduled_date,
            COALESCE(NULLIF(j.completed_at, ''), j.scheduled_date) AS service_date,
            jm.tool_specific_id AS jobber_job_id,
            c.client_type,
            c.company_name,
            c.first_name,
            c.last_name,
            c.email,
            c.phone
        FROM jobs j
        JOIN clients c ON c.id = j.client_id
        LEFT JOIN cross_tool_mapping jm
          ON jm.canonical_id = j.id
         AND jm.tool_name = 'jobber'
        WHERE j.status = 'completed'
          AND COALESCE(
                NULLIF(j.completed_at, '')::timestamp,
                NULLIF(j.scheduled_date, '')::timestamp
              ) <= CURRENT_TIMESTAMP - INTERVAL '24 hours'
          AND NOT EXISTS (
                SELECT 1
                FROM invoices i
                WHERE i.job_id = j.id
          )
        ORDER BY service_date, j.id
    """
    params = None
    if limit is not None:
        sql += " LIMIT %s"
        params = (limit,)
    return [dict(row) for row in db.execute(sql, params).fetchall()]


def _get_client_qbo_mapping(db, client_id: str) -> Optional[str]:
    row = db.execute(
        """
        SELECT tool_specific_id
        FROM cross_tool_mapping
        WHERE canonical_id = %s AND tool_name = 'quickbooks'
        """,
        (client_id,),
    ).fetchone()
    return row["tool_specific_id"] if row else None


def _get_alias_qbo_mapping(db, client_id: str) -> Optional[str]:
    row = db.execute(
        """
        SELECT tool_specific_id
        FROM cross_tool_mapping
        WHERE canonical_id = %s AND tool_name = 'quickbooks_customer'
        """,
        (client_id,),
    ).fetchone()
    return row["tool_specific_id"] if row else None


def _find_qbo_customer_by_canonical(client_id: str) -> Optional[str]:
    note = _qbo_escape(f"SS-ID: {client_id}")
    try:
        customers = _qbo_query(
            f"SELECT Id FROM Customer WHERE Notes = '{note}' MAXRESULTS 10"
        ).get("Customer", [])
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 400:
            return None
        raise
    if customers:
        return str(customers[0]["Id"])
    return None


def _find_qbo_customer_by_display_name(client: dict) -> Optional[str]:
    display_name = _qbo_escape(_customer_display_name(client))
    try:
        customers = _qbo_query(
            f"SELECT Id FROM Customer WHERE DisplayName = '{display_name}' MAXRESULTS 10"
        ).get("Customer", [])
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 400:
            return None
        raise
    if customers:
        return str(customers[0]["Id"])
    return None


def _create_qbo_customer(client: dict) -> str:
    body = _customer_body(client)
    response = requests.post(
        f"{get_base_url()}/customer",
        headers=get_quickbooks_headers(),
        json=body,
        params={"minorversion": "65"},
        timeout=30,
    )

    if response.status_code == 400:
        error_detail = response.json()
        errors = error_detail.get("Fault", {}).get("Error", [])
        is_duplicate = any(
            error.get("code") == "6240"
            or "Duplicate Name" in error.get("Detail", "")
            for error in errors
        )
        if is_duplicate:
            existing_id = _find_qbo_customer_by_display_name(client)
            if existing_id:
                return existing_id

    response.raise_for_status()
    customer = response.json().get("Customer")
    if not customer:
        raise RuntimeError(f"Unexpected QBO customer response for {client['id']}: {response.text}")
    return str(customer["Id"])


def _ensure_customer_mapping(db, client: dict, dry_run: bool) -> tuple[Optional[str], str]:
    client_id = _client_canonical_id(client)

    quickbooks_id = _get_client_qbo_mapping(db, client_id)
    if quickbooks_id:
        return quickbooks_id, "already_mapped"

    alias_id = _get_alias_qbo_mapping(db, client_id)
    if alias_id:
        if dry_run:
            return alias_id, "would_promote_alias"
        register_mapping(db, client_id, "quickbooks", alias_id)
        return alias_id, "promoted_alias"

    discovered_id = _find_qbo_customer_by_canonical(client_id)
    if not discovered_id:
        discovered_id = _find_qbo_customer_by_display_name(client)

    if discovered_id:
        if dry_run:
            return discovered_id, "would_register_existing_customer"
        register_mapping(db, client_id, "quickbooks", discovered_id)
        register_mapping(db, client_id, "quickbooks_customer", discovered_id)
        return discovered_id, "registered_existing_customer"

    if dry_run:
        return None, "would_create_customer"

    qbo_customer_id = _create_qbo_customer(client)
    register_mapping(db, client_id, "quickbooks", qbo_customer_id)
    register_mapping(db, client_id, "quickbooks_customer", qbo_customer_id)
    return qbo_customer_id, "created_customer"


def _unlinked_local_candidates(db, job: dict, issue_date: str) -> list[dict]:
    rows = db.execute(
        """
        SELECT
            i.id,
            i.amount,
            i.issue_date,
            i.due_date,
            (
                SELECT m.tool_specific_id
                FROM cross_tool_mapping m
                WHERE m.canonical_id = i.id AND m.tool_name = 'quickbooks'
            ) AS quickbooks_invoice_id
        FROM invoices i
        WHERE i.client_id = %s
          AND i.job_id IS NULL
          AND i.issue_date = %s
        ORDER BY i.id
        """,
        (job["client_id"], issue_date),
    ).fetchall()
    return [dict(row) for row in rows]


def _find_local_invoice_by_qbo_id(db, qbo_invoice_id: str) -> Optional[dict]:
    row = db.execute(
        """
        SELECT i.id, i.client_id, i.job_id
        FROM invoices i
        JOIN cross_tool_mapping m
          ON m.canonical_id = i.id
         AND m.tool_name = 'quickbooks'
        WHERE m.tool_specific_id = %s
        """,
        (qbo_invoice_id,),
    ).fetchone()
    return dict(row) if row else None


def _find_logged_qbo_invoice_id(db, job: dict) -> Optional[str]:
    row = db.execute(
        """
        SELECT action_target
        FROM automation_log
        WHERE action_name = 'create_quickbooks_invoice'
          AND status = 'success'
          AND (
                trigger_source = %s
             OR trigger_detail::text LIKE %s
             OR trigger_detail::text LIKE %s
          )
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (
            f"jobber:job:{job.get('jobber_job_id') or ''}",
            f"%{job['id']}%",
            f"%{job.get('jobber_job_id') or ''}%",
        ),
    ).fetchone()
    if not row or not row.get("action_target"):
        return None

    action_target = str(row["action_target"])
    return action_target.rsplit(":", 1)[-1] if ":" in action_target else None


def _qbo_invoice_exists(qbo_invoice_id: str) -> bool:
    invoices = _qbo_query(
        f"SELECT Id FROM Invoice WHERE Id = '{_qbo_escape(qbo_invoice_id)}' MAXRESULTS 1"
    ).get("Invoice", [])
    return bool(invoices)


def _find_qbo_invoice_for_job(
    qbo_customer_id: str,
    job_ids: list[str],
    issue_date: str,
) -> Optional[str]:
    invoices = _qbo_query(
        "SELECT Id, PrivateNote FROM Invoice "
        f"WHERE CustomerRef = '{_qbo_escape(qbo_customer_id)}' "
        f"AND TxnDate = '{issue_date}' MAXRESULTS 1000"
    ).get("Invoice", [])

    matches = [
        invoice for invoice in invoices
        if any(job_id in str(invoice.get("PrivateNote") or "") for job_id in job_ids if job_id)
    ]
    if len(matches) == 1:
        return str(matches[0]["Id"])
    if len(matches) > 1:
        logger.warning("Job identifiers %s matched %d QBO invoices; skipping ambiguous relink", job_ids, len(matches))
    return None


def _link_invoice_to_job(db, invoice_id: str, job_id: str) -> str:
    row = db.execute(
        "SELECT job_id FROM invoices WHERE id = %s",
        (invoice_id,),
    ).fetchone()
    if row is None:
        raise RuntimeError(f"Invoice {invoice_id} disappeared before it could be linked")
    if row["job_id"] and row["job_id"] != job_id:
        raise RuntimeError(
            f"Invoice {invoice_id} is already linked to {row['job_id']}, not {job_id}"
        )
    if row["job_id"] == job_id:
        return "already_linked"

    with db:
        db.execute(
            "UPDATE invoices SET job_id = %s WHERE id = %s AND job_id IS NULL",
            (job_id, invoice_id),
        )
    return "linked"


def _allocate_invoice_id(db) -> str:
    db.execute("SELECT pg_advisory_xact_lock(%s)", (_INVOICE_ID_LOCK_KEY,))
    invoice_row = db.execute(
        """
        SELECT COALESCE(MAX(CAST(split_part(id, '-', 3) AS INTEGER)), 0) AS max_id
        FROM invoices
        WHERE id LIKE 'SS-INV-%'
        """
    ).fetchone()
    mapping_row = db.execute(
        """
        SELECT COALESCE(MAX(CAST(split_part(canonical_id, '-', 3) AS INTEGER)), 0) AS max_id
        FROM cross_tool_mapping
        WHERE entity_type = 'INV' AND canonical_id LIKE 'SS-INV-%'
        """
    ).fetchone()
    next_n = max(invoice_row["max_id"], mapping_row["max_id"]) + 1
    return f"SS-INV-{next_n:04d}"


def _create_local_invoice(db, job: dict, details: dict, qbo_invoice_id: str) -> str:
    with db:
        invoice_id = _allocate_invoice_id(db)
        db.execute(
            """
            INSERT INTO invoices
                (id, client_id, job_id, amount, status, issue_date, due_date)
            VALUES (%s, %s, %s, %s, 'sent', %s, %s)
            """,
            (
                invoice_id,
                job["client_id"],
                job["id"],
                details["amount"],
                details["issue_date"],
                details["due_date"],
            ),
        )
        register_mapping(db, invoice_id, "quickbooks", qbo_invoice_id)
    return invoice_id


def _create_qbo_invoice(job: dict, qbo_customer_id: str, details: dict) -> str:
    body: dict = {
        "CustomerRef": {"value": qbo_customer_id},
        "TxnDate": details["issue_date"],
        "DueDate": details["due_date"],
        "Line": [
            {
                "DetailType": "SalesItemLineDetail",
                "Amount": details["amount"],
                "SalesItemLineDetail": {
                    "ItemRef": {"value": details["qbo_item_id"]},
                },
            }
        ],
        "PrivateNote": f"SS-JOB: {job['id']}",
    }
    if job["client_type"] == "commercial":
        body["SalesTermRef"] = {"value": _QBO_NET30_TERM_ID}

    data = _qbo_request(
        "post",
        "/invoice",
        json=body,
        params={"minorversion": "65"},
    )
    invoice = data.get("Invoice") or {}
    if not invoice.get("Id"):
        raise RuntimeError(f"Unexpected QBO invoice response for {job['id']}: {data}")
    return str(invoice["Id"])


def _materialize_invoice(
    db,
    job: dict,
    details: dict,
    qbo_invoice_id: str,
    candidate: Optional[dict],
) -> str:
    local_by_qbo = _find_local_invoice_by_qbo_id(db, qbo_invoice_id)
    if local_by_qbo:
        return _link_invoice_to_job(db, local_by_qbo["id"], job["id"])

    if candidate:
        if candidate["quickbooks_invoice_id"] and candidate["quickbooks_invoice_id"] != qbo_invoice_id:
            raise RuntimeError(
                f"Invoice {candidate['id']} is mapped to QBO {candidate['quickbooks_invoice_id']}, "
                f"not {qbo_invoice_id}"
            )
        with db:
            db.execute(
                "UPDATE invoices SET job_id = %s WHERE id = %s AND job_id IS NULL",
                (job["id"], candidate["id"]),
            )
            register_mapping(db, candidate["id"], "quickbooks", qbo_invoice_id)
        return "candidate_linked"

    _create_local_invoice(db, job, details, qbo_invoice_id)
    return "created_local_invoice"


def _fetch_orphan_invoices(
    db,
    since: str,
    until: str,
    limit: Optional[int],
) -> list[dict]:
    """Return orphan invoices (``job_id IS NULL``) within ``[since, until]``.

    An orphan is surfaced by the Track B integrity metric and by
    ``audit_orphan_invoices.py``. This query is deliberately kept in sync
    with ``scripts/audit_orphan_invoices._fetch_orphans`` so an operator
    running the audit and then ``--mode=orphans`` over the same window
    sees the same rows.
    """
    sql = """
        SELECT
            i.id,
            i.client_id,
            i.amount,
            i.status,
            i.issue_date,
            c.client_type,
            c.company_name,
            c.first_name,
            c.last_name,
            (
                SELECT m.tool_specific_id
                FROM cross_tool_mapping m
                WHERE m.canonical_id = i.id AND m.tool_name = 'quickbooks'
            ) AS quickbooks_invoice_id
        FROM invoices i
        JOIN clients c ON c.id = i.client_id
        WHERE i.job_id IS NULL
          AND i.issue_date BETWEEN %s AND %s
        ORDER BY i.issue_date, i.id
    """
    params: tuple = (since, until)
    if limit is not None:
        sql += " LIMIT %s"
        params = (since, until, limit)
    return [dict(row) for row in db.execute(sql, params).fetchall()]


def _fetch_candidate_jobs_for_orphan(db, orphan: dict) -> list[dict]:
    """Completed jobs for the same client on the orphan's issue_date.

    Only unlinked jobs are eligible — a job that already has an invoice row
    (i.e. appears in ``invoices.job_id``) is excluded so we never reassign
    an invoice away from a legitimate link.

    Jobs where ``completed_at::date`` equals ``issue_date`` are matched
    first; jobs where ``scheduled_date`` equals ``issue_date`` are matched
    as a secondary fallback (some simulation paths only populate
    ``scheduled_date``). The caller then narrows by expected amount.
    """
    rows = db.execute(
        """
        SELECT
            j.id,
            j.client_id,
            j.service_type_id,
            j.scheduled_date,
            COALESCE(NULLIF(j.completed_at, '')::text,
                     j.scheduled_date::text) AS service_date,
            jm.tool_specific_id AS jobber_job_id
        FROM jobs j
        LEFT JOIN cross_tool_mapping jm
          ON jm.canonical_id = j.id
         AND jm.tool_name = 'jobber'
        WHERE j.client_id = %s
          AND j.status = 'completed'
          AND (
                j.completed_at::date = %s::date
             OR j.scheduled_date::date = %s::date
          )
          AND NOT EXISTS (
                SELECT 1 FROM invoices i2 WHERE i2.job_id = j.id
          )
        ORDER BY j.completed_at NULLS LAST, j.scheduled_date, j.id
        """,
        (orphan["client_id"], orphan["issue_date"], orphan["issue_date"]),
    ).fetchall()
    return [dict(row) for row in rows]


def _match_orphan_to_job(
    orphan: dict,
    candidates: list[dict],
) -> tuple[Optional[dict], str]:
    """Return (matched_job, reason). Only link when exactly one candidate
    has the expected per-job amount. Ambiguity is a skip, not a guess —
    the operator needs to resolve it manually because a wrong link corrupts
    revenue attribution permanently.
    """
    if not candidates:
        return None, "no_candidate_job_on_issue_date"

    orphan_amount = round(float(orphan["amount"]), 2)
    amount_matches: list[dict] = []
    for job in candidates:
        job_with_ctx = {
            **job,
            "client_type": orphan["client_type"],
            "client_id": orphan["client_id"],
        }
        try:
            expected = _invoice_details_for_job(job_with_ctx)["amount"]
        except Exception as exc:  # pragma: no cover — defensive
            logger.warning(
                "orphan %s: could not compute expected amount for job %s: %s",
                orphan["id"], job["id"], exc,
            )
            continue
        if abs(float(expected) - orphan_amount) < 0.01:
            amount_matches.append(job)

    if len(amount_matches) == 1:
        return amount_matches[0], "linked_unique_amount_match"
    if len(amount_matches) > 1:
        return None, "ambiguous_multiple_amount_matches"
    return None, "no_candidate_with_matching_amount"


def remediate_orphans(
    db,
    dry_run: bool,
    since: str,
    until: str,
    limit: Optional[int],
) -> dict[str, int]:
    """Link orphan invoices (``job_id IS NULL``) back to the completed job
    they belong to, when the match is unambiguous.

    Writes are strictly: ``UPDATE invoices SET job_id = ? WHERE id = ?
    AND job_id IS NULL``. No QBO calls are made — the orphan's existing
    QBO mapping (if any) stays on the invoice row. Orphans that don't
    match cleanly are counted and logged so an operator can follow up.
    """
    stats = {
        "orphans_seen": 0,
        "orphans_linked": 0,
        "orphans_no_candidate": 0,
        "orphans_no_amount_match": 0,
        "orphans_ambiguous": 0,
        "orphan_failures": 0,
    }

    orphans = _fetch_orphan_invoices(db, since, until, limit)
    stats["orphans_seen"] = len(orphans)
    logger.info(
        "Loaded %d orphan invoice(s) with issue_date in [%s, %s]",
        len(orphans), since, until,
    )

    for index, orphan in enumerate(orphans, start=1):
        try:
            candidates = _fetch_candidate_jobs_for_orphan(db, orphan)
            matched, reason = _match_orphan_to_job(orphan, candidates)

            if matched is None:
                if reason == "no_candidate_job_on_issue_date":
                    stats["orphans_no_candidate"] += 1
                elif reason == "ambiguous_multiple_amount_matches":
                    stats["orphans_ambiguous"] += 1
                else:
                    stats["orphans_no_amount_match"] += 1
                logger.info(
                    "Orphan %s ($%.2f, %s): %s (candidates=%d)",
                    orphan["id"], float(orphan["amount"]),
                    orphan["issue_date"], reason, len(candidates),
                )
                continue

            if dry_run:
                logger.info(
                    "Orphan %s → job %s (would link, amount=$%.2f, %s)",
                    orphan["id"], matched["id"],
                    float(orphan["amount"]), orphan["issue_date"],
                )
            else:
                with db:
                    result = db.execute(
                        "UPDATE invoices SET job_id = %s "
                        "WHERE id = %s AND job_id IS NULL",
                        (matched["id"], orphan["id"]),
                    )
                    updated = getattr(result, "rowcount", 1)
                if not updated:
                    logger.warning(
                        "Orphan %s: link raced — job_id was no longer NULL",
                        orphan["id"],
                    )
                    stats["orphan_failures"] += 1
                    continue
                logger.info(
                    "Orphan %s → job %s linked (amount=$%.2f, %s)",
                    orphan["id"], matched["id"],
                    float(orphan["amount"]), orphan["issue_date"],
                )

            stats["orphans_linked"] += 1

            if index % 50 == 0 or index == len(orphans):
                logger.info(
                    "Processed %d/%d orphans (linked=%d, no_candidate=%d, "
                    "no_amount=%d, ambiguous=%d)",
                    index, len(orphans),
                    stats["orphans_linked"],
                    stats["orphans_no_candidate"],
                    stats["orphans_no_amount_match"],
                    stats["orphans_ambiguous"],
                )
        except Exception as exc:
            stats["orphan_failures"] += 1
            logger.error("Orphan %s: remediation failed: %s",
                         orphan["id"], exc)

    return stats


def remediate(db, dry_run: bool, limit: Optional[int]) -> dict[str, int]:
    stats = {
        "jobs_seen": 0,
        "customers_promoted": 0,
        "customers_created": 0,
        "customers_registered": 0,
        "customer_failures": 0,
        "local_invoices_linked": 0,
        "local_invoices_created": 0,
        "qbo_invoices_created": 0,
        "jobs_already_in_qbo": 0,
        "job_failures": 0,
    }

    jobs = _fetch_missing_jobs(db, limit)
    stats["jobs_seen"] = len(jobs)
    logger.info("Loaded %d completed job(s) missing linked invoices", len(jobs))

    customer_cache: dict[str, Optional[str]] = {}
    for job in jobs:
        client_id = job["client_id"]
        if client_id in customer_cache:
            continue

        try:
            qbo_customer_id, action = _ensure_customer_mapping(db, job, dry_run=dry_run)
            customer_cache[client_id] = qbo_customer_id
            if action in {"promoted_alias", "would_promote_alias"}:
                stats["customers_promoted"] += 1
            elif action in {"created_customer", "would_create_customer"}:
                stats["customers_created"] += 1
            elif action in {"registered_existing_customer", "would_register_existing_customer"}:
                stats["customers_registered"] += 1
        except Exception as exc:
            customer_cache[client_id] = None
            stats["customer_failures"] += 1
            logger.error("Client %s: failed to ensure QBO customer mapping: %s", client_id, exc)

    for index, job in enumerate(jobs, start=1):
        details = _invoice_details_for_job(job)
        qbo_customer_id = customer_cache.get(job["client_id"])
        if not qbo_customer_id and not dry_run:
            stats["job_failures"] += 1
            logger.warning("Job %s: no QBO customer mapping available after repair", job["id"])
            continue

        try:
            candidates = _unlinked_local_candidates(db, job, details["issue_date"])
            candidate = _pick_unique_candidate(candidates, details["amount"])

            qbo_invoice_id = _find_logged_qbo_invoice_id(db, job)
            if not qbo_invoice_id and qbo_customer_id:
                qbo_invoice_id = _find_qbo_invoice_for_job(
                    qbo_customer_id=qbo_customer_id,
                    job_ids=[job["id"], job.get("jobber_job_id") or ""],
                    issue_date=details["issue_date"],
                )

            if qbo_invoice_id:
                stats["jobs_already_in_qbo"] += 1
                result = "would_link_existing_qbo" if dry_run else _materialize_invoice(
                    db,
                    job,
                    details,
                    qbo_invoice_id,
                    candidate,
                )
            else:
                if dry_run:
                    result = "would_create_qbo_invoice"
                else:
                    if candidate and candidate["quickbooks_invoice_id"]:
                        if _qbo_invoice_exists(candidate["quickbooks_invoice_id"]):
                            qbo_invoice_id = candidate["quickbooks_invoice_id"]
                            stats["jobs_already_in_qbo"] += 1
                        else:
                            logger.warning(
                                "Job %s: invoice %s had stale QBO mapping %s; replacing it",
                                job["id"],
                                candidate["id"],
                                candidate["quickbooks_invoice_id"],
                            )
                    if not qbo_invoice_id:
                        qbo_invoice_id = _create_qbo_invoice(job, qbo_customer_id, details)
                        stats["qbo_invoices_created"] += 1
                    result = _materialize_invoice(db, job, details, qbo_invoice_id, candidate)

            if result in {
                "linked",
                "already_linked",
                "candidate_linked",
                "would_link_existing_qbo",
            }:
                stats["local_invoices_linked"] += 1
            elif result in {"created_local_invoice", "would_create_qbo_invoice"}:
                stats["local_invoices_created"] += 1

            if index % 50 == 0 or index == len(jobs):
                logger.info(
                    "Processed %d/%d jobs (linked=%d, created_local=%d, created_qbo=%d)",
                    index,
                    len(jobs),
                    stats["local_invoices_linked"],
                    stats["local_invoices_created"],
                    stats["qbo_invoices_created"],
                )
        except Exception as exc:
            stats["job_failures"] += 1
            logger.error("Job %s: remediation failed: %s", job["id"], exc)

    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Apply writes locally and in QuickBooks. Omit for dry-run mode.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional max number of records to process.",
    )
    parser.add_argument(
        "--mode",
        choices=("jobs", "orphans"),
        default="jobs",
        help=(
            "jobs: repair completed jobs with no linked invoice (default). "
            "orphans: relink invoices.job_id IS NULL to an unambiguous "
            "completed job in --since/--until (Track B remediation)."
        ),
    )
    parser.add_argument(
        "--since",
        default=None,
        help=(
            "Orphan-mode only: inclusive start of issue_date window. "
            "Defaults to 30 days ago."
        ),
    )
    parser.add_argument(
        "--until",
        default=None,
        help=(
            "Orphan-mode only: inclusive end of issue_date window. "
            "Defaults to today."
        ),
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    )

    if args.mode == "jobs" and (args.since or args.until):
        parser.error("--since/--until are only valid with --mode=orphans")

    dry_run = not args.execute
    logger.info(
        "Starting reconciliation invoice remediation (mode=%s, %s)",
        args.mode, "dry-run" if dry_run else "execute",
    )

    db = get_connection()
    try:
        if args.mode == "orphans":
            since = args.since or (date.today() - timedelta(days=30)).isoformat()
            until = args.until or date.today().isoformat()
            stats = remediate_orphans(
                db, dry_run=dry_run, since=since, until=until,
                limit=args.limit,
            )
        else:
            stats = remediate(db, dry_run=dry_run, limit=args.limit)
    finally:
        db.close()

    logger.info("Remediation summary:")
    for key, value in stats.items():
        logger.info("  %s=%s", key, value)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
