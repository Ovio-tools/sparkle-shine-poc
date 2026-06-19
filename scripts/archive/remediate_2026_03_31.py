#!/usr/bin/env python3
"""Remediation script for failures logged in intelligence_2026-03-31.log.

Three impact areas addressed:

1. Missing Jobber property IDs (20+ recurring agreements)
   Root cause: _get_or_fetch_property_id returned None because jobber_property
   mappings were absent from cross_tool_mapping. These were never persisted
   because register_mapping itself was also subject to the DB lock bug (now fixed
   in schema.py with timeout=30).
   Remediation: prefetch and register all missing jobber_property mappings, then
   re-run JobSchedulingGenerator for 2026-03-31 so jobs are created in Jobber.

2. SS-RECUR-0001 unmapped Jobber job
   Root cause: job was created in Jobber but register_mapping failed (DB locked).
   Remediation: find the SQLite job for that agreement's client on 2026-03-31,
   search Jobber for a matching job on that date, register the mapping.

3. Unmapped QuickBooks payment
   Root cause: payment was recorded in SQLite + QBO but register_mapping failed.
   Remediation: find payments in SQLite with no QBO mapping, query QBO for each
   invoice's payments, and register the mapping.

Usage:
    python scripts/remediate_2026_03_31.py [--dry-run]
"""

import argparse
import logging
import sqlite3
import sys
from datetime import date
from pathlib import Path

# Ensure project root is on the path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from auth import get_client
from database.mappings import (
    find_unmapped,
    get_tool_id,
    register_mapping,
)
from database.schema import get_connection
from simulation.generators.operations import (
    JobSchedulingGenerator,
    _CLIENT_PROPERTIES_QUERY,
    _gql,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("remediate_2026_03_31")

DB_PATH = str(PROJECT_ROOT / "sparkle_shine.db")
TARGET_DATE = date(2026, 3, 31)

# ── GraphQL helpers ───────────────────────────────────────────────────────────

_JOBS_FOR_CLIENT_ON_DATE = """
query JobsForClient($clientId: EncodedId!) {
  jobs(filter: {clientId: $clientId}) {
    nodes {
      id
      title
      startAt
      jobStatus
    }
  }
}
"""

_QBO_PAYMENT_QUERY = "SELECT * FROM Payment WHERE DocNumber = '{doc}'"


# ═══════════════════════════════════════════════════════════════════════════════
# Part 1: Prefetch missing Jobber property IDs
# ═══════════════════════════════════════════════════════════════════════════════

def prefetch_missing_property_ids(dry_run: bool) -> int:
    """Register jobber_property mappings for all clients that lack one.

    Returns the number of property IDs successfully registered.
    """
    logger.info("=== Part 1: Prefetch missing Jobber property IDs ===")
    conn = get_connection(DB_PATH)
    try:
        # Clients that have a jobber mapping but no jobber_property mapping
        rows = conn.execute("""
            SELECT c.canonical_id
            FROM cross_tool_mapping c
            WHERE c.tool_name = 'jobber'
              AND c.entity_type = 'CLIENT'
              AND NOT EXISTS (
                  SELECT 1 FROM cross_tool_mapping p
                  WHERE p.canonical_id = c.canonical_id
                    AND p.tool_name = 'jobber_property'
              )
        """).fetchall()
    finally:
        conn.close()

    if not rows:
        logger.info("No clients missing jobber_property mapping — nothing to do.")
        return 0

    logger.info("Found %d client(s) with no jobber_property mapping.", len(rows))
    session = get_client("jobber") if not dry_run else None

    registered = 0
    for row in rows:
        canonical_id = row["canonical_id"]
        jobber_client_id = get_tool_id(canonical_id, "jobber", DB_PATH)
        if not jobber_client_id:
            logger.warning("  %s: no jobber client ID found — skipping", canonical_id)
            continue

        if dry_run:
            logger.info("  [DRY RUN] Would query Jobber for property of %s", canonical_id)
            registered += 1
            continue

        try:
            data = _gql(session, _CLIENT_PROPERTIES_QUERY, {"id": jobber_client_id})
            nodes = (
                data.get("data", {})
                .get("client", {})
                .get("clientProperties", {})
                .get("nodes", [])
            )
            if not nodes:
                logger.warning(
                    "  %s: client %s has no properties in Jobber — cannot create job",
                    canonical_id, jobber_client_id,
                )
                continue

            prop_id = nodes[0]["id"]
            register_mapping(canonical_id, "jobber_property", prop_id, db_path=DB_PATH)
            logger.info("  %s → jobber_property %s ✓", canonical_id, prop_id)
            registered += 1

        except Exception as exc:
            logger.error("  %s: failed to fetch property ID: %s", canonical_id, exc)

    logger.info("Part 1 complete: %d/%d property IDs registered.", registered, len(rows))
    return registered


# ═══════════════════════════════════════════════════════════════════════════════
# Part 2: Re-run JobSchedulingGenerator for 2026-03-31
# ═══════════════════════════════════════════════════════════════════════════════

def recreate_missing_recurring_jobs(dry_run: bool) -> None:
    """Re-run the job scheduler so agreements with now-registered property IDs
    get their Jobber jobs created. The generator's idempotency guard prevents
    duplicate creation for agreements that already have a job for today.
    """
    logger.info("=== Part 2: Recreate missing recurring jobs for %s ===", TARGET_DATE)

    conn = get_connection(DB_PATH)
    try:
        missing = conn.execute("""
            SELECT ra.id, ra.client_id
            FROM recurring_agreements ra
            WHERE ra.status = 'active'
              AND NOT EXISTS (
                  SELECT 1 FROM jobs j
                  WHERE j.client_id = ra.client_id
                    AND j.scheduled_date = ?
              )
        """, (TARGET_DATE.isoformat(),)).fetchall()
    finally:
        conn.close()

    if not missing:
        logger.info("No recurring agreements missing jobs for %s — nothing to do.", TARGET_DATE)
        return

    logger.info(
        "%d agreement(s) have no job for %s. Running JobSchedulingGenerator...",
        len(missing), TARGET_DATE,
    )

    if dry_run:
        for row in missing:
            logger.info(
                "  [DRY RUN] Would create job for agreement %s (client %s)",
                row["id"], row["client_id"],
            )
        return

    gen = JobSchedulingGenerator(db_path=DB_PATH)
    result = gen.execute(dry_run=False)
    logger.info("JobSchedulingGenerator result: %s", result)


# ═══════════════════════════════════════════════════════════════════════════════
# Part 3: Reconcile SS-RECUR-0001 unmapped Jobber job
# ═══════════════════════════════════════════════════════════════════════════════

def reconcile_unmapped_jobber_job(dry_run: bool) -> None:
    """Find the SQLite job created for SS-RECUR-0001's client on 2026-03-31
    that has no Jobber mapping, search Jobber for a matching job, and register it.
    """
    logger.info("=== Part 3: Reconcile SS-RECUR-0001 unmapped Jobber job ===")

    conn = get_connection(DB_PATH)
    try:
        # Find the client for SS-RECUR-0001
        recur_row = conn.execute(
            "SELECT client_id FROM recurring_agreements WHERE id = 'SS-RECUR-0001'"
        ).fetchone()

        if not recur_row:
            logger.warning("SS-RECUR-0001 not found in recurring_agreements — skipping.")
            return

        client_id = recur_row["client_id"]

        # Find any job for that client on 2026-03-31 with no Jobber mapping
        job_row = conn.execute("""
            SELECT j.id FROM jobs j
            WHERE j.client_id = ?
              AND j.scheduled_date = ?
              AND NOT EXISTS (
                  SELECT 1 FROM cross_tool_mapping m
                  WHERE m.canonical_id = j.id
                    AND m.tool_name = 'jobber'
              )
        """, (client_id, TARGET_DATE.isoformat())).fetchone()
    finally:
        conn.close()

    if not job_row:
        logger.info(
            "No unmapped job for SS-RECUR-0001's client (%s) on %s — "
            "already mapped or not yet created.",
            client_id, TARGET_DATE,
        )
        return

    job_canonical_id = job_row["id"]
    logger.info(
        "Found unmapped job %s for client %s on %s.",
        job_canonical_id, client_id, TARGET_DATE,
    )

    jobber_client_id = get_tool_id(client_id, "jobber", DB_PATH)
    if not jobber_client_id:
        logger.warning("  Client %s has no Jobber mapping — cannot reconcile.", client_id)
        return

    if dry_run:
        logger.info(
            "  [DRY RUN] Would search Jobber for jobs of client %s on %s and register mapping.",
            jobber_client_id, TARGET_DATE,
        )
        return

    try:
        session = get_client("jobber")
        data = _gql(session, _JOBS_FOR_CLIENT_ON_DATE, {"clientId": jobber_client_id})
        jobs = (
            data.get("data", {})
            .get("jobs", {})
            .get("nodes", [])
        )

        # Find a job starting on TARGET_DATE that has no local mapping yet
        target_str = TARGET_DATE.isoformat()
        candidates = [
            j for j in jobs
            if j.get("startAt", "").startswith(target_str)
        ]

        if not candidates:
            logger.warning(
                "  No Jobber jobs found for client %s on %s — "
                "job may have been deleted or not yet synced.",
                jobber_client_id, TARGET_DATE,
            )
            return

        if len(candidates) > 1:
            logger.warning(
                "  Multiple Jobber jobs found for client %s on %s — "
                "using the first: %s",
                jobber_client_id, TARGET_DATE, candidates[0]["id"],
            )

        jobber_job_id = candidates[0]["id"]
        register_mapping(job_canonical_id, "jobber", jobber_job_id, db_path=DB_PATH)
        logger.info(
            "  Registered mapping: %s → jobber %s ✓",
            job_canonical_id, jobber_job_id,
        )

    except Exception as exc:
        logger.error("  Failed to reconcile Jobber job for %s: %s", job_canonical_id, exc)


# ═══════════════════════════════════════════════════════════════════════════════
# Part 4: Reconcile unmapped QuickBooks payments
# ═══════════════════════════════════════════════════════════════════════════════

def reconcile_unmapped_qbo_payments(dry_run: bool) -> None:
    """Find payments in SQLite with no QBO mapping and attempt to register them
    by querying QBO for payments against each payment's invoice.
    """
    logger.info("=== Part 4: Reconcile unmapped QBO payments ===")

    unmapped = find_unmapped("PAY", "quickbooks", DB_PATH)
    if not unmapped:
        logger.info(
            "No unmapped QBO payments found in SQLite. "
            "NOTE: An orphaned QBO payment from the 2026-03-31 DB-lock incident "
            "may still exist in QuickBooks — the SQLite transaction was rolled back "
            "when the lock hit, so no local record was written. Check QBO for "
            "payments posted on 2026-03-31 against invoices still 'unpaid' in SQLite "
            "and void any duplicates before the next simulator run."
        )
        return

    logger.info("Found %d unmapped payment(s): %s", len(unmapped), unmapped)

    if dry_run:
        for pay_id in unmapped:
            logger.info("  [DRY RUN] Would reconcile QBO mapping for %s", pay_id)
        return

    headers = get_client("quickbooks")
    from auth.quickbooks_auth import get_base_url
    base_url = get_base_url()

    conn = get_connection(DB_PATH)
    try:
        for pay_canonical in unmapped:
            row = conn.execute(
                "SELECT invoice_id FROM payments WHERE id = ?", (pay_canonical,)
            ).fetchone()
            if not row:
                logger.warning("  %s: not found in payments table — skipping", pay_canonical)
                continue

            invoice_id = row["invoice_id"]
            qbo_invoice_id = get_tool_id(invoice_id, "quickbooks", DB_PATH)
            if not qbo_invoice_id:
                logger.warning(
                    "  %s: invoice %s has no QBO mapping — cannot query QBO",
                    pay_canonical, invoice_id,
                )
                continue

            try:
                # Query QBO for all payments linked to this invoice
                query = (
                    f"SELECT * FROM Payment WHERE Line.LinkedTxn.TxnId = '{qbo_invoice_id}'"
                )
                resp = headers.get(
                    f"{base_url}/query",
                    params={"query": query, "minorversion": "65"},
                    headers={"Accept": "application/json"},
                )
                resp.raise_for_status()
                qbo_payments = (
                    resp.json()
                    .get("QueryResponse", {})
                    .get("Payment", [])
                )

                if not qbo_payments:
                    logger.warning(
                        "  %s: no QBO payment found for invoice %s (QBO id %s)",
                        pay_canonical, invoice_id, qbo_invoice_id,
                    )
                    continue

                qbo_payment_id = str(qbo_payments[0]["Id"])
                register_mapping(pay_canonical, "quickbooks", qbo_payment_id, db_path=DB_PATH)
                logger.info(
                    "  Registered mapping: %s → quickbooks %s ✓",
                    pay_canonical, qbo_payment_id,
                )

            except Exception as exc:
                logger.error("  %s: QBO reconciliation failed: %s", pay_canonical, exc)
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Log actions without making API calls")
    args = parser.parse_args()

    if args.dry_run:
        logger.info("=== DRY RUN MODE — no API calls or DB writes ===")

    prefetch_missing_property_ids(args.dry_run)
    recreate_missing_recurring_jobs(args.dry_run)
    reconcile_unmapped_jobber_job(args.dry_run)
    reconcile_unmapped_qbo_payments(args.dry_run)

    logger.info("=== Remediation complete ===")


if __name__ == "__main__":
    main()
