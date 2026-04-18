#!/usr/bin/env python3
"""Create invoices for completed commercial jobs that were skipped only
because their contract rate was unresolved at the time of completion.

This script is intentionally narrower than replaying JobCompletionFlow:
it creates only the QuickBooks/local invoice and increments HubSpot
`outstanding_balance`. It does NOT add a duplicate HubSpot note, bump
`total_services_completed`, or schedule another review request.

Usage:
    python -m scripts.remediate_skipped_commercial_invoices --dry-run --job-id SS-JOB-5553
    python -m scripts.remediate_skipped_commercial_invoices --execute --job-id SS-JOB-5553 --job-id SS-JOB-5555
"""
from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from typing import Optional

from auth import get_client
from automations.job_completion_flow import JobCompletionFlow, _allocate_invoice_id
from automations.utils.hubspot_write_lock import contact_write_lock
from automations.utils.id_resolver import register_mapping
from database.connection import get_connection

logger = logging.getLogger("remediate_skipped_commercial_invoices")


@dataclass
class TargetJob:
    canonical_job_id: str
    canonical_client_id: str
    company_name: Optional[str]
    job_title_raw: Optional[str]
    service_type_id: str
    duration_minutes_actual: Optional[int]
    crew_id: Optional[str]
    completed_at: str
    jobber_job_id: Optional[str]
    jobber_client_id: Optional[str]
    qbo_customer_id: Optional[str]
    hs_contact_id: Optional[str]
    invoice_count: int


class CommercialInvoiceRemediation(JobCompletionFlow):
    """Invoice-only replay path for already-completed commercial jobs."""


def _fetch_targets(db, job_ids: list[str]) -> list[TargetJob]:
    placeholders = ", ".join(["%s"] * len(job_ids))
    rows = db.execute(
        f"""
        SELECT
            j.id AS canonical_job_id,
            j.client_id AS canonical_client_id,
            c.company_name,
            j.job_title_raw,
            j.service_type_id,
            j.duration_minutes_actual,
            j.crew_id,
            j.completed_at,
            jm.tool_specific_id AS jobber_job_id,
            cm.tool_specific_id AS jobber_client_id,
            qbo.tool_specific_id AS qbo_customer_id,
            hs.tool_specific_id AS hs_contact_id,
            COUNT(i.id) AS invoice_count
        FROM jobs j
        JOIN clients c
          ON c.id = j.client_id
        LEFT JOIN cross_tool_mapping jm
          ON jm.canonical_id = j.id AND jm.tool_name = 'jobber'
        LEFT JOIN cross_tool_mapping cm
          ON cm.canonical_id = j.client_id AND cm.tool_name = 'jobber'
        LEFT JOIN cross_tool_mapping qbo
          ON qbo.canonical_id = j.client_id
         AND qbo.tool_name IN ('quickbooks', 'quickbooks_customer')
        LEFT JOIN cross_tool_mapping hs
          ON hs.canonical_id = j.client_id AND hs.tool_name = 'hubspot'
        LEFT JOIN invoices i
          ON i.job_id = j.id
        WHERE j.id IN ({placeholders})
        GROUP BY
            j.id, j.client_id, c.company_name, j.job_title_raw,
            j.service_type_id, j.duration_minutes_actual, j.crew_id,
            j.completed_at, jm.tool_specific_id, cm.tool_specific_id,
            qbo.tool_specific_id, hs.tool_specific_id
        ORDER BY j.id
        """,
        tuple(job_ids),
    ).fetchall()
    return [TargetJob(**dict(row)) for row in rows]


def _validate_target(target: TargetJob) -> None:
    if target.service_type_id != "commercial-nightly":
        raise RuntimeError(
            f"{target.canonical_job_id} is {target.service_type_id!r}, not commercial-nightly"
        )
    if target.invoice_count:
        raise RuntimeError(
            f"{target.canonical_job_id} already has {target.invoice_count} linked invoice(s)"
        )
    if not target.completed_at:
        raise RuntimeError(f"{target.canonical_job_id} is not completed")
    if not target.jobber_job_id:
        raise RuntimeError(f"{target.canonical_job_id} has no Jobber job mapping")
    if not target.jobber_client_id:
        raise RuntimeError(f"{target.canonical_job_id} has no Jobber client mapping")
    if not target.qbo_customer_id:
        raise RuntimeError(
            f"{target.canonical_job_id} / {target.canonical_client_id} has no QuickBooks customer mapping"
        )
    if not target.hs_contact_id:
        raise RuntimeError(
            f"{target.canonical_job_id} / {target.canonical_client_id} has no HubSpot contact mapping"
        )


def _build_trigger_event(target: TargetJob) -> dict:
    return {
        "job_id": target.jobber_job_id,
        "client_id": target.jobber_client_id,
        "service_type": target.job_title_raw or "Commercial Nightly Clean",
        "duration_minutes": target.duration_minutes_actual,
        "crew": target.crew_id or "crew-d",
        "completion_notes": "",
        "is_recurring": True,
        "completed_at": str(target.completed_at)[:10],
    }


def _apply_local_invoice(db, ctx: dict, invoice_id: str, amount: float) -> str:
    inv_due_date = (ctx["completion_date"]).isoformat()
    if ctx["is_commercial"]:
        from datetime import timedelta

        inv_due_date = (ctx["completion_date"] + timedelta(days=30)).isoformat()

    with db:
        inv_canonical_id = _allocate_invoice_id(db)
        db.execute(
            """
            INSERT INTO invoices
                (id, client_id, job_id, amount, status, issue_date, due_date)
            VALUES (%s, %s, %s, %s, 'sent', %s, %s)
            """,
            (
                inv_canonical_id,
                ctx["canonical_id"],
                ctx["canonical_job_id"],
                amount,
                ctx["completion_date"].isoformat(),
                inv_due_date,
            ),
        )
    register_mapping(db, inv_canonical_id, "quickbooks", invoice_id)
    return inv_canonical_id


def _increment_hubspot_outstanding_balance(hs_client, contact_id: str, amount: float, dry_run: bool) -> None:
    if dry_run:
        print(
            f"[DRY RUN] Would increment HubSpot outstanding_balance for {contact_id} by ${amount:.2f}"
        )
        return

    from hubspot.crm.contacts import SimplePublicObjectInput

    with contact_write_lock(contact_id):
        contact = hs_client.crm.contacts.basic_api.get_by_id(
            contact_id,
            properties=["outstanding_balance"],
            _request_timeout=30,
        )
        props = contact.properties or {}
        current_outstanding = float(props.get("outstanding_balance") or 0.0)
        hs_client.crm.contacts.basic_api.update(
            contact_id,
            SimplePublicObjectInput(
                properties={
                    "outstanding_balance": str(round(current_outstanding + amount, 2)),
                }
            ),
            _request_timeout=30,
        )


def remediate(job_ids: list[str], dry_run: bool) -> dict:
    db = get_connection()
    try:
        targets = _fetch_targets(db, job_ids)
        found_ids = {t.canonical_job_id for t in targets}
        missing = [job_id for job_id in job_ids if job_id not in found_ids]
        if missing:
            raise RuntimeError(f"Job(s) not found: {', '.join(missing)}")

        automation = CommercialInvoiceRemediation(clients=get_client, db=db, dry_run=dry_run)
        hs_client = get_client("hubspot")

        stats = {"candidates": len(targets), "invoiced": 0, "failed": 0}

        for target in targets:
            run_id = automation.generate_run_id()
            trigger_source = f"manual:commercial_invoice_remediation:{target.canonical_job_id}"
            try:
                _validate_target(target)
                ctx = automation._build_context(_build_trigger_event(target))
                invoice_id, amount, payment_terms = automation._action_quickbooks_invoice(ctx)
                local_invoice_id = None
                if not dry_run:
                    local_invoice_id = _apply_local_invoice(db, ctx, invoice_id, amount)
                _increment_hubspot_outstanding_balance(
                    hs_client, ctx["hs_contact_id"], amount, dry_run=dry_run
                )
                automation.log_action(
                    run_id,
                    "remediate_commercial_invoice",
                    f"quickbooks:invoice:{invoice_id}",
                    "success",
                    trigger_source=trigger_source,
                    trigger_detail={
                        "job_id": target.canonical_job_id,
                        "local_invoice_id": local_invoice_id,
                        "amount": amount,
                        "payment_terms": payment_terms,
                    },
                )
                stats["invoiced"] += 1
                if dry_run:
                    print(
                        f"DRY-RUN {target.canonical_job_id}: would create QBO invoice for ${amount:.2f}"
                    )
                else:
                    print(
                        f"EXECUTED {target.canonical_job_id}: created {local_invoice_id} / QBO {invoice_id} for ${amount:.2f}"
                    )
            except Exception as exc:
                stats["failed"] += 1
                automation.log_action(
                    run_id,
                    "remediate_commercial_invoice",
                    None,
                    "failed",
                    error_message=str(exc),
                    trigger_source=trigger_source,
                )
                print(f"FAILED {target.canonical_job_id}: {exc}")
        return stats
    finally:
        db.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true")
    group.add_argument("--execute", action="store_true")
    parser.add_argument(
        "--job-id",
        action="append",
        dest="job_ids",
        required=True,
        help="Canonical job ID to remediate. Repeatable.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    stats = remediate(args.job_ids, dry_run=args.dry_run)
    print(
        f"candidates={stats['candidates']} invoiced={stats['invoiced']} failed={stats['failed']} dry_run={args.dry_run}"
    )
    return 0 if stats["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
