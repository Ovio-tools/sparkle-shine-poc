"""
scripts/backfill_missing_invoices_20260611.py

One-off remediation for the 2026-06-11 missing-invoice incident
(#automation-failure reconciliation alerts on SS-JOB-5981 / SS-JOB-6058).

Root causes (see docs/superpowers/plans/2026-06-11-alert-suppression-and-
invoice-healing.md for the full investigation):
  * SS-JOB-5981 — Jobber client 142428287 was mapped by BOTH the stale lead
    SS-LEAD-0335 and the promoted client SS-CLIENT-0519; invoice creation
    resolved the lead, which has no QuickBooks mapping.
  * SS-JOB-6058 — onboarding never created a QuickBooks customer for
    SS-CLIENT-0521 (verify step logged "Missing mappings: quickbooks,
    quickbooks_customer" on 2026-06-09 and nothing retried).
  * In both cases the failed invoice was never retried because the Jobber
    poll watermark advances at fetch time.

What this script does (each step idempotent, checks cross_tool_mapping
before creating anything):
  1. Retire SS-LEAD-0335's stale jobber/jobber_property mappings (delete
     when SS-CLIENT-0519 already holds the same tool ID, re-point otherwise).
  2. Ensure a QuickBooks customer exists for SS-CLIENT-0521 (creates via the
     QBO API and registers quickbooks/quickbooks_customer mappings).
  3. Create the two missing invoices through
     JobCompletionFlow.run_invoice_retry (QBO invoice + canonical invoices
     row + INV mapping + Jobber draft-invoice writeback), reusing the
     runner's create_invoice pending handler.

Usage (against Railway production):
    DATABASE_URL=<railway-postgres-url> JOBBER_TOKEN_KEEPER_ENABLED=1 \
        python scripts/backfill_missing_invoices_20260611.py [--dry-run]

JOBBER_TOKEN_KEEPER_ENABLED=1 is REQUIRED for a live run: Jobber refresh
tokens are single-use and owned by the Railway token-keeper service; a
local self-refresh would rotate the token and break production auth.
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))
except ImportError:
    pass

from auth import get_client
from database.connection import get_connection

# Incident constants — verified against Railway Postgres on 2026-06-11.
STALE_LEAD_ID       = "SS-LEAD-0335"
PROMOTED_CLIENT_ID  = "SS-CLIENT-0519"
QBO_CUSTOMER_NEEDED = "SS-CLIENT-0521"
JOBS_TO_INVOICE     = ["SS-JOB-5981", "SS-JOB-6058"]


def retire_stale_lead_mappings(db, dry_run: bool) -> None:
    """Step 1: remove the dual-identity mappings that broke reverse lookups."""
    rows = db.execute(
        "SELECT tool_name, tool_specific_id FROM cross_tool_mapping "
        "WHERE canonical_id = %s AND tool_name IN ('jobber', 'jobber_property')",
        (STALE_LEAD_ID,),
    ).fetchall()
    if not rows:
        print(f"  [OK] {STALE_LEAD_ID} has no stale jobber mappings — nothing to do")
        return

    for row in rows:
        tool = row["tool_name"]
        tool_id = row["tool_specific_id"]
        client_has_it = db.execute(
            "SELECT 1 FROM cross_tool_mapping "
            "WHERE canonical_id = %s AND tool_name = %s",
            (PROMOTED_CLIENT_ID, tool),
        ).fetchone()

        if client_has_it:
            if dry_run:
                print(f"  [DRY RUN] Would DELETE {STALE_LEAD_ID} {tool} mapping ({tool_id})")
                continue
            with db:
                db.execute(
                    "DELETE FROM cross_tool_mapping "
                    "WHERE canonical_id = %s AND tool_name = %s",
                    (STALE_LEAD_ID, tool),
                )
            print(f"  [DONE] Deleted {STALE_LEAD_ID} {tool} mapping "
                  f"({PROMOTED_CLIENT_ID} already holds {tool_id})")
        else:
            if dry_run:
                print(f"  [DRY RUN] Would re-point {STALE_LEAD_ID} {tool} mapping "
                      f"({tool_id}) to {PROMOTED_CLIENT_ID}")
                continue
            with db:
                db.execute(
                    "UPDATE cross_tool_mapping "
                    "SET canonical_id = %s, entity_type = 'CLIENT', "
                    "    synced_at = CURRENT_TIMESTAMP "
                    "WHERE canonical_id = %s AND tool_name = %s",
                    (PROMOTED_CLIENT_ID, STALE_LEAD_ID, tool),
                )
            print(f"  [DONE] Re-pointed {STALE_LEAD_ID} {tool} mapping "
                  f"({tool_id}) to {PROMOTED_CLIENT_ID}")


def ensure_qbo_customer(db, dry_run: bool) -> None:
    """Step 2: create the missing QuickBooks customer for SS-CLIENT-0521."""
    from automations.new_client_onboarding import NewClientOnboarding

    onboarding = NewClientOnboarding(clients=get_client, db=db, dry_run=dry_run)
    customer_id = onboarding.retry_quickbooks_customer(QBO_CUSTOMER_NEEDED)
    print(f"  [DONE] {QBO_CUSTOMER_NEEDED} → QBO customer {customer_id}")


def create_missing_invoices(db, dry_run: bool) -> list[str]:
    """Step 3: invoice the two stuck jobs via the shared retry handler."""
    from automations.runner import _handle_create_invoice

    failures = []
    for job_id in JOBS_TO_INVOICE:
        existing = db.execute(
            "SELECT id FROM invoices WHERE job_id = %s", (job_id,)
        ).fetchone()
        if existing:
            print(f"  [OK] {job_id} already invoiced ({existing['id']}) — skipping")
            continue
        try:
            _handle_create_invoice(
                get_client, db, {"canonical_job_id": job_id}, dry_run
            )
            print(f"  [DONE] {job_id} invoiced")
        except Exception as exc:
            failures.append(job_id)
            print(f"  [FAIL] {job_id}: {exc}")
    return failures


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Log actions without making API calls or DB writes",
    )
    args = parser.parse_args()

    if not args.dry_run and os.getenv("JOBBER_TOKEN_KEEPER_ENABLED") != "1":
        sys.exit(
            "Refusing to run live: set JOBBER_TOKEN_KEEPER_ENABLED=1 so the "
            "Jobber session reads the token-keeper's access token instead of "
            "self-refreshing (which would rotate the single-use refresh token "
            "out from under the Railway token-keeper)."
        )

    db = get_connection()
    try:
        print(f"Step 1/3 — retire stale {STALE_LEAD_ID} mappings:")
        retire_stale_lead_mappings(db, args.dry_run)

        print(f"Step 2/3 — ensure QBO customer for {QBO_CUSTOMER_NEEDED}:")
        ensure_qbo_customer(db, args.dry_run)

        print(f"Step 3/3 — create invoices for {', '.join(JOBS_TO_INVOICE)}:")
        failures = create_missing_invoices(db, args.dry_run)
    finally:
        db.close()

    if failures:
        sys.exit(f"Backfill incomplete — failed jobs: {', '.join(failures)}")
    print("Backfill complete.")


if __name__ == "__main__":
    main()
