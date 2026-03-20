"""
Pre-push validation for the Sparkle & Shine seeding database.

Usage:
    python seeding/utils/validator.py                  # validates sparkle_shine.db
    python seeding/utils/validator.py path/to/other.db # validates a specific file

Programmatic usage:
    from seeding.utils.validator import validate_clients, validate_jobs, validate_financials

    report = validate_clients("sparkle_shine.db")
    if not report.passed:
        for err in report.errors:
            print(f"ERROR: {err}")
"""

import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional, Tuple

# Pull in the narrative targets so revenue checks stay in sync.
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[2]))
from config.narrative import TIMELINE

TODAY = date.today().isoformat()

# Maximum number of jobs a single client can physically have in one calendar month.
# Daily service (e.g. commercial nightly) caps at ~23 per month.
_MAX_JOBS_PER_CLIENT_PER_MONTH = 25

# Tables that hold named entities reachable via cross_tool_mapping.
_ENTITY_TABLE_MAP = {
    "client": "clients",
    "lead": "leads",
    "employee": "employees",
    "job": "jobs",
    "invoice": "invoices",
    "payment": "payments",
    "review": "reviews",
    "task": "tasks",
    "campaign": "marketing_campaigns",
    "proposal": "commercial_proposals",
    "recurring": "recurring_agreements",
    "document": "documents",
    "calendar": "calendar_events",
}


# ---------------------------------------------------------------------------
# ValidationReport
# ---------------------------------------------------------------------------

@dataclass
class ValidationReport:
    validator_name: str
    passed: bool = True
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    stats: Dict = field(default_factory=dict)

    def error(self, msg: str) -> None:
        self.errors.append(msg)
        self.passed = False

    def warn(self, msg: str) -> None:
        self.warnings.append(msg)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = OFF")  # we check FKs manually
    return conn


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    )
    return cur.fetchone() is not None


# ---------------------------------------------------------------------------
# validate_clients
# ---------------------------------------------------------------------------

def validate_clients(db_path: str) -> ValidationReport:
    """
    Check client data integrity.

    Checks:
    - All cross_tool_mapping entries whose canonical_id looks like a client
      (SS-CLIENT-*) have a matching row in clients.
    - Churned clients have no future-scheduled jobs.
    - Churned clients have a last_service_date that is not in the future.
    """
    report = ValidationReport("clients")
    conn = _connect(db_path)

    if not _table_exists(conn, "clients"):
        report.error("Table 'clients' does not exist.")
        conn.close()
        return report

    # --- basic stats ---
    cur = conn.execute("SELECT COUNT(*) FROM clients")
    total = cur.fetchone()[0]
    cur = conn.execute("SELECT status, COUNT(*) as n FROM clients GROUP BY status")
    status_counts = {row["status"]: row["n"] for row in cur.fetchall()}
    report.stats["total_clients"] = total
    report.stats["by_status"] = status_counts

    # --- cross_tool_mapping: all SS-CLIENT-* ids must exist in clients ---
    if _table_exists(conn, "cross_tool_mapping"):
        cur = conn.execute(
            """
            SELECT canonical_id FROM cross_tool_mapping
            WHERE canonical_id LIKE 'SS-CLIENT-%'
            GROUP BY canonical_id
            """
        )
        mapping_ids = {row["canonical_id"] for row in cur.fetchall()}
        cur = conn.execute("SELECT id FROM clients")
        client_ids = {row["id"] for row in cur.fetchall()}
        orphaned_mappings = mapping_ids - client_ids
        report.stats["cross_tool_mapping_client_ids"] = len(mapping_ids)
        if orphaned_mappings:
            for oid in sorted(orphaned_mappings):
                report.error(
                    f"cross_tool_mapping references client '{oid}' "
                    f"which does not exist in clients table."
                )

    # --- churned clients: no future-scheduled jobs ---
    if _table_exists(conn, "jobs"):
        cur = conn.execute(
            """
            SELECT c.id, c.last_service_date, j.id AS job_id, j.scheduled_date
            FROM clients c
            JOIN jobs j ON j.client_id = c.id
            WHERE c.status = 'churned'
              AND j.status = 'scheduled'
              AND j.scheduled_date > ?
            """,
            (TODAY,),
        )
        future_jobs = cur.fetchall()
        report.stats["churned_clients_with_future_jobs"] = len(future_jobs)
        for row in future_jobs:
            report.error(
                f"Churned client '{row['id']}' has a future-scheduled job "
                f"'{row['job_id']}' on {row['scheduled_date']}."
            )

    # --- churned clients: last_service_date must not be in the future ---
    cur = conn.execute(
        "SELECT id, last_service_date FROM clients WHERE status='churned'"
    )
    for row in cur.fetchall():
        lsd = row["last_service_date"]
        if lsd and lsd > TODAY:
            report.error(
                f"Churned client '{row['id']}' has a future last_service_date ({lsd}); "
                f"should reflect the final completed job."
            )

    conn.close()
    return report


# ---------------------------------------------------------------------------
# validate_jobs
# ---------------------------------------------------------------------------

def validate_jobs(db_path: str) -> ValidationReport:
    """
    Check job and invoice data integrity.

    Checks:
    - No orphaned jobs (job.client_id must exist in clients).
    - No orphaned invoices (invoice.job_id must exist in jobs, when set).
    - No future-dated completed jobs.
    - No client has more jobs than physically possible in a single month.
    - Payment amounts never exceed the corresponding invoice amounts.
    """
    report = ValidationReport("jobs")
    conn = _connect(db_path)

    if not _table_exists(conn, "jobs"):
        report.error("Table 'jobs' does not exist.")
        conn.close()
        return report

    cur = conn.execute("SELECT COUNT(*) FROM jobs")
    report.stats["total_jobs"] = cur.fetchone()[0]

    # --- orphaned jobs ---
    if _table_exists(conn, "clients"):
        cur = conn.execute(
            """
            SELECT j.id, j.client_id
            FROM jobs j
            LEFT JOIN clients c ON c.id = j.client_id
            WHERE c.id IS NULL
            """
        )
        orphans = cur.fetchall()
        report.stats["orphaned_jobs"] = len(orphans)
        for row in orphans:
            report.error(
                f"Job '{row['id']}' references client '{row['client_id']}' "
                f"which does not exist."
            )

    # --- orphaned invoices ---
    if _table_exists(conn, "invoices"):
        cur = conn.execute(
            """
            SELECT i.id, i.job_id
            FROM invoices i
            LEFT JOIN jobs j ON j.id = i.job_id
            WHERE i.job_id IS NOT NULL AND j.id IS NULL
            """
        )
        orphan_invs = cur.fetchall()
        report.stats["orphaned_invoices"] = len(orphan_invs)
        for row in orphan_invs:
            report.error(
                f"Invoice '{row['id']}' references job '{row['job_id']}' "
                f"which does not exist."
            )

    # --- no future-dated completed jobs ---
    cur = conn.execute(
        """
        SELECT id, scheduled_date, completed_at
        FROM jobs
        WHERE status = 'completed'
          AND (
              scheduled_date > ?
              OR (completed_at IS NOT NULL AND completed_at > ?)
          )
        """,
        (TODAY, TODAY),
    )
    future_completed = cur.fetchall()
    report.stats["future_dated_completed_jobs"] = len(future_completed)
    for row in future_completed:
        report.error(
            f"Job '{row['id']}' is marked 'completed' but has a future date "
            f"(scheduled={row['scheduled_date']}, completed_at={row['completed_at']})."
        )

    # --- per-client monthly job volume sanity check ---
    cur = conn.execute(
        """
        SELECT client_id,
               strftime('%Y-%m', scheduled_date) AS month,
               COUNT(*) AS job_count
        FROM jobs
        GROUP BY client_id, month
        HAVING job_count > ?
        """,
        (_MAX_JOBS_PER_CLIENT_PER_MONTH,),
    )
    overloaded = cur.fetchall()
    report.stats["clients_exceeding_monthly_job_cap"] = len(overloaded)
    for row in overloaded:
        report.warn(
            f"Client '{row['client_id']}' has {row['job_count']} jobs in "
            f"{row['month']}, exceeding the physical cap of "
            f"{_MAX_JOBS_PER_CLIENT_PER_MONTH} per month."
        )

    # --- payment amounts must not exceed invoice amounts ---
    if _table_exists(conn, "invoices") and _table_exists(conn, "payments"):
        cur = conn.execute(
            """
            SELECT p.invoice_id,
                   i.amount AS invoice_amount,
                   SUM(p.amount) AS total_paid
            FROM payments p
            JOIN invoices i ON i.id = p.invoice_id
            GROUP BY p.invoice_id
            HAVING total_paid > invoice_amount + 0.01
            """
        )
        overpaid = cur.fetchall()
        report.stats["overpaid_invoices"] = len(overpaid)
        for row in overpaid:
            report.error(
                f"Invoice '{row['invoice_id']}' has been overpaid: "
                f"payments total ${row['total_paid']:.2f} against invoice "
                f"amount ${row['invoice_amount']:.2f}."
            )

    conn.close()
    return report


# ---------------------------------------------------------------------------
# validate_financials
# ---------------------------------------------------------------------------

def validate_financials(db_path: str) -> ValidationReport:
    """
    Check financial data against narrative revenue targets.

    Checks:
    - Revenue per month (sum of paid invoice amounts) is within ±20% of the
      narrative TIMELINE range for that month.
    - All cross_tool_mapping canonical_ids exist in their entity table.
    """
    report = ValidationReport("financials")
    conn = _connect(db_path)

    # --- monthly revenue vs narrative targets ---
    if _table_exists(conn, "invoices"):
        cur = conn.execute(
            """
            SELECT strftime('%Y-%m', paid_date) AS month,
                   SUM(amount) AS revenue
            FROM invoices
            WHERE status = 'paid' AND paid_date IS NOT NULL
            GROUP BY month
            ORDER BY month
            """
        )
        actual_by_month: Dict[str, float] = {}
        for row in cur.fetchall():
            if row["month"]:
                actual_by_month[row["month"]] = row["revenue"]

        report.stats["revenue_by_month"] = actual_by_month
        revenue_checks_passed = 0
        revenue_checks_total = 0

        for entry in TIMELINE:
            ym = entry["year_month"]
            rev_lo, rev_hi = entry["expected_revenue_range"]
            # Allow ±20% around the midpoint of the range.
            midpoint = (rev_lo + rev_hi) / 2
            tolerance = 0.20 * midpoint
            lower_bound = midpoint - tolerance
            upper_bound = midpoint + tolerance

            actual = actual_by_month.get(ym)
            if actual is None:
                report.warn(
                    f"{ym}: No paid invoices found. "
                    f"Narrative target: ${rev_lo:,.0f}–${rev_hi:,.0f}."
                )
                continue

            revenue_checks_total += 1
            if lower_bound <= actual <= upper_bound:
                revenue_checks_passed += 1
            else:
                shortfall = actual - midpoint
                direction = "over" if shortfall > 0 else "under"
                report.error(
                    f"{ym}: Revenue ${actual:,.2f} is {direction} the narrative target "
                    f"(${rev_lo:,.0f}–${rev_hi:,.0f}; ±20% window: "
                    f"${lower_bound:,.0f}–${upper_bound:,.0f})."
                )

        report.stats["revenue_months_checked"] = revenue_checks_total
        report.stats["revenue_months_in_range"] = revenue_checks_passed
    else:
        report.warn("Table 'invoices' does not exist; skipping revenue checks.")

    # --- cross_tool_mapping: all canonical IDs must exist in their entity table ---
    if _table_exists(conn, "cross_tool_mapping"):
        cur = conn.execute(
            "SELECT DISTINCT canonical_id, entity_type FROM cross_tool_mapping"
        )
        all_mappings = cur.fetchall()
        report.stats["total_cross_tool_mappings"] = len(all_mappings)

        # Build a set of valid IDs per table to avoid N+1 queries.
        id_sets: Dict[str, set] = {}
        for row in all_mappings:
            entity_type = (row["entity_type"] or "").lower()
            table = _ENTITY_TABLE_MAP.get(entity_type)
            if table is None:
                # Try to infer from the canonical_id pattern (SS-TYPE-NNNN)
                parts = row["canonical_id"].split("-")
                if len(parts) >= 2:
                    inferred_type = parts[1].lower()
                    table = _ENTITY_TABLE_MAP.get(inferred_type)

            if table and _table_exists(conn, table):
                if table not in id_sets:
                    cur2 = conn.execute(f"SELECT id FROM {table}")
                    id_sets[table] = {r["id"] for r in cur2.fetchall()}

        orphaned_mappings: List[Tuple[str, str]] = []
        for row in all_mappings:
            entity_type = (row["entity_type"] or "").lower()
            table = _ENTITY_TABLE_MAP.get(entity_type)
            if table is None:
                parts = row["canonical_id"].split("-")
                if len(parts) >= 2:
                    table = _ENTITY_TABLE_MAP.get(parts[1].lower())

            if table and table in id_sets:
                if row["canonical_id"] not in id_sets[table]:
                    orphaned_mappings.append((row["canonical_id"], table))

        report.stats["orphaned_cross_tool_mappings"] = len(orphaned_mappings)
        for cid, table in sorted(orphaned_mappings):
            report.error(
                f"cross_tool_mapping has canonical_id '{cid}' "
                f"but no matching row in table '{table}'."
            )

    conn.close()
    return report


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _format_report(report: ValidationReport) -> str:
    lines = []
    status_icon = "PASS" if report.passed else "FAIL"
    lines.append(f"\n{'─' * 60}")
    lines.append(f"  [{status_icon}]  {report.validator_name.upper()} VALIDATOR")
    lines.append(f"{'─' * 60}")

    if report.stats:
        lines.append("  Stats:")
        for k, v in report.stats.items():
            if isinstance(v, dict):
                lines.append(f"    {k}:")
                for sub_k, sub_v in v.items():
                    if isinstance(sub_v, float):
                        lines.append(f"      {sub_k}: ${sub_v:,.2f}")
                    else:
                        lines.append(f"      {sub_k}: {sub_v}")
            elif isinstance(v, float):
                lines.append(f"    {k}: ${v:,.2f}")
            else:
                lines.append(f"    {k}: {v}")

    if report.errors:
        lines.append(f"\n  Errors ({len(report.errors)}):")
        for err in report.errors:
            lines.append(f"    ✗ {err}")

    if report.warnings:
        lines.append(f"\n  Warnings ({len(report.warnings)}):")
        for warn in report.warnings:
            lines.append(f"    ⚠ {warn}")

    if report.passed and not report.warnings:
        lines.append("\n  All checks passed.")

    return "\n".join(lines)


def run_all(db_path: str) -> bool:
    """Run all three validators and print a formatted report. Returns True if all pass."""
    print(f"\n{'═' * 60}")
    print(f"  SPARKLE & SHINE — PRE-PUSH VALIDATION")
    print(f"  Database: {db_path}")
    print(f"{'═' * 60}")

    validators = [
        ("clients", validate_clients),
        ("jobs", validate_jobs),
        ("financials", validate_financials),
    ]

    all_passed = True
    for name, fn in validators:
        report = fn(db_path)
        print(_format_report(report))
        if not report.passed:
            all_passed = False

    print(f"\n{'═' * 60}")
    overall = "ALL CHECKS PASSED" if all_passed else "VALIDATION FAILED — see errors above"
    print(f"  {overall}")
    print(f"{'═' * 60}\n")

    return all_passed


if __name__ == "__main__":
    db = sys.argv[1] if len(sys.argv) > 1 else "sparkle_shine.db"
    ok = run_all(db)
    sys.exit(0 if ok else 1)
