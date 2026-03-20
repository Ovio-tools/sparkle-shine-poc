"""
Cross-Tool Data Integrity Audit -- master runner.

Usage:
    python -m demo.audit.cross_tool_audit               # full audit, all 6 tools
    python -m demo.audit.cross_tool_audit --tool jobber # single-tool audit
    python -m demo.audit.cross_tool_audit --fix         # audit then auto-fix mismatches
    python -m demo.audit.cross_tool_audit --db path/to/sparkle_shine.db
"""

from __future__ import annotations

import argparse
import datetime
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Literal, Optional

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from database.schema import get_connection
from database.mappings import get_all_mappings

_DEFAULT_DB = os.path.join(_PROJECT_ROOT, "sparkle_shine.db")


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class AuditFinding:
    severity: Literal["match", "mismatch", "missing", "orphan"]
    entity_type: str            # "client", "invoice", "deal", etc.
    canonical_id: str           # SS-CLIENT-0047
    tool_id: Optional[str] = None        # the ID in the remote tool
    field: Optional[str] = None          # which field mismatched
    expected: Optional[str] = None       # value in SQLite
    actual: Optional[str] = None         # value in the remote tool
    message: str = ""


@dataclass
class ToolAuditResult:
    tool_name: str
    records_checked: int = 0
    findings: list[AuditFinding] = field(default_factory=list)
    duration_seconds: float = 0.0


@dataclass
class AuditSummary:
    total_checks: int = 0
    matches: int = 0
    mismatches: int = 0
    missing: int = 0
    orphans: int = 0
    pass_rate: float = 0.0
    critical_issues: list[str] = field(default_factory=list)


@dataclass
class AuditReport:
    timestamp: str
    tool_results: dict[str, ToolAuditResult] = field(default_factory=dict)
    summary: AuditSummary = field(default_factory=AuditSummary)


@dataclass
class AuditSample:
    """Structured sample of records to audit, grouped by entity type."""
    clients: list[dict] = field(default_factory=list)
    jobs: list[dict] = field(default_factory=list)
    invoices: list[dict] = field(default_factory=list)
    proposals: list[dict] = field(default_factory=list)
    tasks: list[dict] = field(default_factory=list)
    mailchimp_contacts: list[dict] = field(default_factory=list)
    recurring_agreements: list[dict] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Sample builder
# ---------------------------------------------------------------------------

def build_audit_sample(db_path: str, sample_size: int = 20) -> AuditSample:
    """Build a stratified sample of records to check.

    Include:
    - 5 residential clients (1 active recurring, 1 active one-time,
      1 churned, 1 from referral source, 1 recently onboarded)
    - All commercial clients (8 active + 2 churned = ~10 total)
    - 10 recent jobs (last 30 days -- most likely to show in briefings)
    - 10 invoices (5 paid, 3 open, 2 overdue -- covers AR aging)
    - All open commercial proposals
    - 5 Asana tasks (2 overdue, 2 completed, 1 assigned to Maria)
    - 5 Mailchimp contacts (verify tags and merge fields)

    Pull canonical_id values from SQLite.
    Look up cross_tool_mapping to get each tool's native ID.
    Return structured sample grouped by tool.
    """
    conn = get_connection(db_path)
    try:
        # ---- Residential clients (5 stratified picks) ----
        res_clients: list[dict] = []

        # 1. Active recurring
        row = conn.execute("""
            SELECT DISTINCT c.* FROM clients c
            JOIN recurring_agreements ra ON ra.client_id = c.id
            WHERE c.client_type = 'residential' AND c.status = 'active'
              AND ra.status = 'active'
            ORDER BY c.id LIMIT 1
        """).fetchone()
        if row:
            res_clients.append(dict(row))

        # 2. Active one-time (no active recurring agreement)
        seen = {r["id"] for r in res_clients}
        row = conn.execute("""
            SELECT c.* FROM clients c
            WHERE c.client_type = 'residential' AND c.status = 'active'
              AND c.id NOT IN (
                  SELECT client_id FROM recurring_agreements WHERE status = 'active'
              )
            ORDER BY c.last_service_date DESC LIMIT 1
        """).fetchone()
        if row and row["id"] not in seen:
            res_clients.append(dict(row))
            seen.add(row["id"])

        # 3. Churned
        row = conn.execute("""
            SELECT * FROM clients
            WHERE client_type = 'residential' AND status = 'churned'
            ORDER BY last_service_date DESC LIMIT 1
        """).fetchone()
        if row and row["id"] not in seen:
            res_clients.append(dict(row))
            seen.add(row["id"])

        # 4. Referral source
        row = conn.execute("""
            SELECT * FROM clients
            WHERE client_type = 'residential'
              AND (acquisition_source LIKE '%referral%' OR acquisition_source = 'referral')
            ORDER BY first_service_date DESC LIMIT 1
        """).fetchone()
        if row and row["id"] not in seen:
            res_clients.append(dict(row))
            seen.add(row["id"])

        # 5. Recently onboarded (last 90 days)
        row = conn.execute("""
            SELECT * FROM clients
            WHERE client_type = 'residential' AND status = 'active'
              AND first_service_date >= date('now', '-90 days')
            ORDER BY first_service_date DESC LIMIT 1
        """).fetchone()
        if row and row["id"] not in seen:
            res_clients.append(dict(row))

        # ---- All commercial clients ----
        com_rows = conn.execute("""
            SELECT * FROM clients WHERE client_type = 'commercial'
            ORDER BY status, id
        """).fetchall()
        com_clients = [dict(r) for r in com_rows]

        all_clients = res_clients + com_clients

        # Enrich clients with tool IDs
        for c in all_clients:
            m = get_all_mappings(c["id"], db_path)
            c["jobber_id"] = m.get("jobber")
            c["hubspot_id"] = m.get("hubspot")
            c["qbo_id"] = m.get("quickbooks")
            c["pipedrive_person_id"] = m.get("pipedrive")

        # ---- Jobs (recent 30 days, up to sample_size // 2 with minimum 10) ----
        job_limit = max(10, sample_size // 2)
        job_rows = conn.execute("""
            SELECT * FROM jobs
            WHERE scheduled_date >= date('now', '-30 days')
            ORDER BY scheduled_date DESC
            LIMIT ?
        """, (job_limit,)).fetchall()
        jobs = []
        for r in job_rows:
            j = dict(r)
            m = get_all_mappings(j["id"], db_path)
            j["jobber_id"] = m.get("jobber")
            jobs.append(j)

        # ---- Invoices (5 paid, 3 open/sent, 2 overdue) ----
        inv_rows = []
        for status, limit in [("paid", 5), ("sent", 3), ("overdue", 2)]:
            rows = conn.execute("""
                SELECT * FROM invoices WHERE status = ?
                ORDER BY issue_date DESC LIMIT ?
            """, (status, limit)).fetchall()
            inv_rows.extend(rows)
        invoices = []
        for r in inv_rows:
            inv = dict(r)
            m = get_all_mappings(inv["id"], db_path)
            inv["qbo_id"] = m.get("quickbooks")
            invoices.append(inv)

        # ---- All open commercial proposals ----
        prop_rows = conn.execute("""
            SELECT * FROM commercial_proposals
            WHERE status IN ('sent', 'negotiating', 'draft')
            ORDER BY sent_date DESC NULLS LAST
        """).fetchall()
        proposals = []
        for r in prop_rows:
            p = dict(r)
            m = get_all_mappings(p["id"], db_path)
            p["hubspot_deal_id"] = m.get("hubspot")
            p["pipedrive_deal_id"] = m.get("pipedrive")
            # Enrich with client/lead company name (used by HubSpot and Pipedrive checks)
            if p.get("client_id"):
                client_row = conn.execute(
                    "SELECT company_name FROM clients WHERE id = ?",
                    (p["client_id"],),
                ).fetchone()
                if client_row:
                    p["client_company_name"] = client_row["company_name"] or ""
            elif p.get("lead_id"):
                lead_row = conn.execute(
                    "SELECT company_name FROM leads WHERE id = ?",
                    (p["lead_id"],),
                ).fetchone()
                if lead_row:
                    p["client_company_name"] = lead_row["company_name"] or ""
            proposals.append(p)

        # ---- Asana tasks (2 overdue, 2 completed, 1 assigned to Maria) ----
        task_rows = []
        seen_task_ids: set[str] = set()
        for status, limit in [("overdue", 2), ("completed", 2)]:
            rows = conn.execute("""
                SELECT * FROM tasks WHERE status = ?
                ORDER BY due_date ASC LIMIT ?
            """, (status, limit)).fetchall()
            for r in rows:
                if r["id"] not in seen_task_ids:
                    task_rows.append(r)
                    seen_task_ids.add(r["id"])

        maria_row = conn.execute("""
            SELECT id FROM employees WHERE first_name = 'Maria' LIMIT 1
        """).fetchone()
        if maria_row:
            row = conn.execute("""
                SELECT * FROM tasks
                WHERE assignee_employee_id = ? AND status != 'completed'
                ORDER BY due_date ASC LIMIT 1
            """, (maria_row["id"],)).fetchone()
            if row and row["id"] not in seen_task_ids:
                task_rows.append(row)

        tasks = []
        for r in task_rows:
            t = dict(r)
            m = get_all_mappings(t["id"], db_path)
            t["asana_gid"] = m.get("asana")
            tasks.append(t)

        # ---- Mailchimp contacts (5 clients that have a mailchimp mapping) ----
        mc_rows = conn.execute("""
            SELECT c.* FROM clients c
            JOIN cross_tool_mapping ctm
              ON ctm.canonical_id = c.id AND ctm.tool_name = 'mailchimp'
            ORDER BY c.id
            LIMIT 5
        """).fetchall()
        mailchimp_contacts = [dict(r) for r in mc_rows]

        # ---- Recurring agreements (5 active, for Jobber schedule check) ----
        recur_rows = conn.execute("""
            SELECT * FROM recurring_agreements WHERE status = 'active'
            ORDER BY id LIMIT 5
        """).fetchall()
        recurring_agreements = []
        for r in recur_rows:
            ra = dict(r)
            m = get_all_mappings(ra["id"], db_path)
            ra["jobber_id"] = m.get("jobber")
            recurring_agreements.append(ra)

    finally:
        conn.close()

    return AuditSample(
        clients=all_clients,
        jobs=jobs,
        invoices=invoices,
        proposals=proposals,
        tasks=tasks,
        mailchimp_contacts=mailchimp_contacts,
        recurring_agreements=recurring_agreements,
    )


# ---------------------------------------------------------------------------
# Summary helpers
# ---------------------------------------------------------------------------

_CRITICAL_FIELDS = {"amount", "status", "total"}
_CRITICAL_ENTITY_TYPES = {"invoice"}


def _is_critical(finding: AuditFinding) -> bool:
    """Return True if this finding would be visible / break a demo briefing."""
    if finding.severity == "missing":
        return True
    if finding.entity_type in _CRITICAL_ENTITY_TYPES:
        return True
    if finding.field in _CRITICAL_FIELDS:
        return True
    return False


def _compute_summary(tool_results: dict[str, ToolAuditResult]) -> AuditSummary:
    total = sum(len(r.findings) for r in tool_results.values())
    matches = sum(
        sum(1 for f in r.findings if f.severity == "match")
        for r in tool_results.values()
    )
    mismatches = sum(
        sum(1 for f in r.findings if f.severity == "mismatch")
        for r in tool_results.values()
    )
    missing = sum(
        sum(1 for f in r.findings if f.severity == "missing")
        for r in tool_results.values()
    )
    orphans = sum(
        sum(1 for f in r.findings if f.severity == "orphan")
        for r in tool_results.values()
    )

    pass_rate = matches / total if total > 0 else 0.0

    critical_issues = [
        f.message
        for r in tool_results.values()
        for f in r.findings
        if f.severity in ("mismatch", "missing") and _is_critical(f)
    ]

    return AuditSummary(
        total_checks=total,
        matches=matches,
        mismatches=mismatches,
        missing=missing,
        orphans=orphans,
        pass_rate=pass_rate,
        critical_issues=critical_issues,
    )


# ---------------------------------------------------------------------------
# Main audit runner
# ---------------------------------------------------------------------------

_ALL_TOOLS = ["jobber", "quickbooks", "hubspot", "pipedrive", "mailchimp", "asana"]


def run_full_audit(db_path: str = _DEFAULT_DB, sample_size: int = 20,
                   tools: Optional[list[str]] = None) -> AuditReport:
    """Run integrity checks against all 6 data-holding tools.

    Slack and Google Calendar are excluded -- they are output channels,
    not canonical data stores.

    Returns an AuditReport with per-tool findings and a summary.
    """
    from demo.audit.auditors.audit_jobber import JobberAuditor
    from demo.audit.auditors.audit_quickbooks import QuickBooksAuditor
    from demo.audit.auditors.audit_hubspot import HubSpotAuditor
    from demo.audit.auditors.audit_pipedrive import PipedriveAuditor
    from demo.audit.auditors.audit_mailchimp import MailchimpAuditor
    from demo.audit.auditors.audit_asana import AsanaAuditor

    active_tools = tools if tools else _ALL_TOOLS

    timestamp = datetime.datetime.now().isoformat(timespec="seconds")
    print(f"[audit] Building sample from {db_path} ...")
    sample = build_audit_sample(db_path, sample_size)
    print(
        f"[audit] Sample: {len(sample.clients)} clients, "
        f"{len(sample.jobs)} jobs, {len(sample.invoices)} invoices, "
        f"{len(sample.proposals)} proposals, {len(sample.tasks)} tasks, "
        f"{len(sample.mailchimp_contacts)} mailchimp contacts"
    )

    auditor_map = {
        "jobber":      JobberAuditor(db_path),
        "quickbooks":  QuickBooksAuditor(db_path),
        "hubspot":     HubSpotAuditor(db_path),
        "pipedrive":   PipedriveAuditor(db_path),
        "mailchimp":   MailchimpAuditor(db_path),
        "asana":       AsanaAuditor(db_path),
    }

    tool_results: dict[str, ToolAuditResult] = {}
    for tool_name in active_tools:
        auditor = auditor_map[tool_name]
        print(f"[audit] Auditing {tool_name} ...")
        try:
            result = auditor.audit(sample)
        except Exception as exc:
            print(f"[audit] ERROR: {tool_name} auditor raised: {exc}")
            result = ToolAuditResult(
                tool_name=tool_name,
                records_checked=0,
                findings=[
                    AuditFinding(
                        severity="missing",
                        entity_type="tool",
                        canonical_id="N/A",
                        message=f"Auditor failed to run: {exc}",
                    )
                ],
                duration_seconds=0.0,
            )
        tool_results[tool_name] = result
        counts = {sev: 0 for sev in ("match", "mismatch", "missing", "orphan")}
        for f in result.findings:
            counts[f.severity] += 1
        print(
            f"[audit]   {tool_name}: {result.records_checked} checked, "
            f"{counts['match']} match, {counts['mismatch']} mismatch, "
            f"{counts['missing']} missing, {counts['orphan']} orphan "
            f"({result.duration_seconds:.1f}s)"
        )

    summary = _compute_summary(tool_results)
    return AuditReport(timestamp=timestamp, tool_results=tool_results, summary=summary)


# ---------------------------------------------------------------------------
# Fix engine
# ---------------------------------------------------------------------------

def run_fix(report: AuditReport, db_path: str = _DEFAULT_DB) -> None:
    """Attempt to fix simple field-value mismatches by re-pushing SQLite data.

    Only handles mismatch severity. Missing / orphan records need manual work.
    Prints what was fixed and what needs manual intervention.
    """
    from demo.audit.auditors.audit_jobber import JobberAuditor
    from demo.audit.auditors.audit_quickbooks import QuickBooksAuditor
    from demo.audit.auditors.audit_hubspot import HubSpotAuditor
    from demo.audit.auditors.audit_pipedrive import PipedriveAuditor
    from demo.audit.auditors.audit_mailchimp import MailchimpAuditor
    from demo.audit.auditors.audit_asana import AsanaAuditor

    fixer_map = {
        "jobber":      JobberAuditor(db_path),
        "quickbooks":  QuickBooksAuditor(db_path),
        "hubspot":     HubSpotAuditor(db_path),
        "pipedrive":   PipedriveAuditor(db_path),
        "mailchimp":   MailchimpAuditor(db_path),
        "asana":       AsanaAuditor(db_path),
    }

    fixed: list[str] = []
    manual: list[str] = []

    for tool_name, result in report.tool_results.items():
        mismatches = [f for f in result.findings if f.severity == "mismatch"]
        if not mismatches:
            continue

        fixer = fixer_map.get(tool_name)
        if fixer is None:
            manual.extend(f.message for f in mismatches)
            continue

        for finding in mismatches:
            try:
                ok = fixer.fix_mismatch(finding)
                if ok:
                    fixed.append(f"[{tool_name}] {finding.canonical_id}.{finding.field}: "
                                 f"'{finding.actual}' -> '{finding.expected}'")
                else:
                    manual.append(f"[{tool_name}] {finding.message}")
            except Exception as exc:
                manual.append(f"[{tool_name}] {finding.message} (fix error: {exc})")

    print("\n=== FIX RESULTS ===")
    if fixed:
        print(f"\nFixed ({len(fixed)}):")
        for msg in fixed:
            print(f"  + {msg}")
    else:
        print("\nNothing auto-fixed.")

    if manual:
        print(f"\nNeeds manual intervention ({len(manual)}):")
        for msg in manual:
            print(f"  ! {msg}")
    else:
        print("\nNo manual intervention needed.")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m demo.audit.cross_tool_audit",
        description="Cross-tool data integrity audit for Sparkle & Shine POC",
    )
    parser.add_argument(
        "--db",
        default=_DEFAULT_DB,
        help="Path to sparkle_shine.db (default: project root)",
    )
    parser.add_argument(
        "--tool",
        choices=_ALL_TOOLS,
        default=None,
        help="Audit a single tool only",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=20,
        dest="sample_size",
        help="Approximate number of records to sample (default: 20)",
    )
    parser.add_argument(
        "--fix",
        action="store_true",
        help="After auditing, attempt to auto-fix simple field mismatches",
    )
    parser.add_argument(
        "--save",
        action="store_true",
        help="Save the text report to demo/audit/audit_report.txt",
    )
    return parser.parse_args()


def main() -> None:
    from demo.audit.audit_report import generate_report, save_report

    args = _parse_args()
    tools = [args.tool] if args.tool else None

    report = run_full_audit(db_path=args.db, sample_size=args.sample_size, tools=tools)
    report_text = generate_report(report)
    print("\n" + report_text)

    if args.save:
        save_report(report_text)

    if args.fix:
        run_fix(report, db_path=args.db)


if __name__ == "__main__":
    main()
