"""
simulation/reconciliation/reconciler.py

Verifies that every client in the canonical database has matching, consistent
records across all relevant SaaS tools. Runs in two modes:

TARGETED — check one specific client across all tools:
    python -m simulation.reconciliation.reconciler --client SS-CLIENT-0047

DAILY SWEEP — random sample of 15 clients + automation health check:
    python -m simulation.reconciliation.reconciler --sweep
    python -m simulation.reconciliation.reconciler --sweep --repair

Auto-fixable: missing Mailchimp subscriber, missing QBO customer,
              mismatched field values on those two tools.

NOT auto-fixable: missing Jobber client/job, missing Asana tasks,
                  completed jobs without invoices (automation gap).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests

from auth import get_client
from auth.quickbooks_auth import get_base_url as _qbo_base_url
from database.mappings import get_tool_id, register_mapping
from database.schema import get_connection
from intelligence.logging_config import setup_logging
from seeding.utils.throttler import (
    HUBSPOT as hubspot_throttler,
    JOBBER as jobber_throttler,
    MAILCHIMP as mailchimp_throttler,
    QUICKBOOKS as qbo_throttler,
)
from simulation.error_reporter import report_reconciliation_issue

logger = setup_logging("simulation.reconciliation")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_DB_PATH = str(_PROJECT_ROOT / "sparkle_shine.db")
_TOOL_IDS_PATH = _PROJECT_ROOT / "config" / "tool_ids.json"
_JOBBER_GQL_URL = "https://api.getjobber.com/api/graphql"
_JOBBER_VERSION_HEADER = "2026-03-10"


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def _load_tool_ids() -> dict:
    with open(_TOOL_IDS_PATH) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass
class Finding:
    """One discrepancy discovered during reconciliation."""
    category: str            # reconciliation_missing | reconciliation_mismatch | reconciliation_automation_gap
    tool: str
    entity: str              # canonical_id or descriptive label
    description: str
    auto_fixable: bool
    count: int = 0
    details: str = ""


@dataclass
class ReconciliationReport:
    client_id: str
    findings: list[Finding] = field(default_factory=list)
    repaired: list[str] = field(default_factory=list)
    checked_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    @property
    def has_issues(self) -> bool:
        return bool(self.findings)

    def summary(self) -> str:
        if not self.findings:
            return f"{self.client_id}: OK"
        lines = [f"{self.client_id}: {len(self.findings)} finding(s)"]
        for f in self.findings:
            tag = "[AUTO-FIXED]" if f.description in self.repaired else (
                "[AUTO-FIXABLE]" if f.auto_fixable else "[MANUAL]"
            )
            lines.append(f"  {tag} [{f.tool}] {f.description}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main reconciler class
# ---------------------------------------------------------------------------

class Reconciler:
    def __init__(
        self,
        db_path: str = _DB_PATH,
        repair: bool = False,
        dry_run: bool = False,
    ):
        self.db_path = db_path
        self.repair = repair
        self.dry_run = dry_run
        self._tool_ids = _load_tool_ids()

    def _connect(self):
        """Open the shared canonical database connection."""
        return get_connection(self.db_path)

    @staticmethod
    def _text_timestamp_sql(column: str) -> str:
        """Normalize legacy text timestamps before PostgreSQL comparisons.

        Some legacy date/time columns are still stored as TEXT. Normalizing the
        separator keeps both `YYYY-MM-DD HH:MM:SS` and ISO `...T...` values
        comparable as real PostgreSQL timestamps.
        """
        return f"NULLIF(TRIM(REPLACE({column}, 'T', ' ')), '')::timestamp"

    def _client_requires_jobber_record(self, canonical_id: str) -> bool:
        """Return True when downstream state implies a Jobber record should exist.

        Some HubSpot sync artifacts create active canonical client rows before
        any proposal, job, QBO customer, or agreement exists. Alerting on every
        such row creates noisy "missing Jobber" findings that ops cannot act on.
        We only require a Jobber mapping once the client has downstream evidence
        that they are actually operating in the fulfillment/billing flow.
        """
        conn = self._connect()
        try:
            row = conn.execute(
                """
                SELECT
                    EXISTS(
                        SELECT 1 FROM jobs WHERE client_id = %s
                    ) AS has_jobs,
                    EXISTS(
                        SELECT 1 FROM recurring_agreements
                        WHERE client_id = %s AND status = 'active'
                    ) AS has_recurring,
                    EXISTS(
                        SELECT 1 FROM commercial_proposals
                        WHERE client_id = %s
                    ) AS has_proposals,
                    EXISTS(
                        SELECT 1 FROM cross_tool_mapping
                        WHERE canonical_id = %s
                          AND tool_name IN ('quickbooks', 'quickbooks_customer')
                    ) AS has_qbo
                """,
                (canonical_id, canonical_id, canonical_id, canonical_id),
            ).fetchone()
        finally:
            conn.close()

        return any(
            bool(row[key]) for key in ("has_jobs", "has_recurring", "has_proposals", "has_qbo")
        )

    # ------------------------------------------------------------------
    # Public entry points
    # ------------------------------------------------------------------

    def check_client(self, canonical_id: str) -> ReconciliationReport:
        """TARGETED mode: check one client across all relevant tools."""
        report = ReconciliationReport(client_id=canonical_id)

        conn = self._connect()
        try:
            client = conn.execute(
                "SELECT * FROM clients WHERE id = %s", (canonical_id,)
            ).fetchone()
        finally:
            conn.close()

        if client is None:
            report.findings.append(Finding(
                category="reconciliation_missing",
                tool="database",
                entity=canonical_id,
                description=f"{canonical_id} not found in the canonical clients table",
                auto_fixable=False,
            ))
            return report

        client = dict(client)

        for check_fn in (
            self._check_hubspot,
            self._check_mailchimp,
            self._check_quickbooks,
            self._check_jobber,
            self._check_asana,
        ):
            try:
                findings = check_fn(canonical_id, client)
                report.findings.extend(findings)
            except Exception as exc:
                logger.warning("Check %s failed for %s: %s", check_fn.__name__, canonical_id, exc)

        if self.repair and not self.dry_run:
            self._apply_repairs(canonical_id, client, report)

        self._post_non_fixable(report)
        return report

    def run_daily_sweep(self) -> tuple[list[ReconciliationReport], list[Finding]]:
        """DAILY SWEEP mode: sample 15 clients + automation health check."""
        sample = self._build_sweep_sample()
        reports: list[ReconciliationReport] = []
        for canonical_id in sample:
            logger.info("Reconciling %s", canonical_id)
            report = self.check_client(canonical_id)
            reports.append(report)

        automation_findings = self.run_automation_health_check()
        return reports, automation_findings

    def daily_sweep(self) -> tuple[list[ReconciliationReport], list[Finding]]:
        """Backward-compatible alias for older callers."""
        return self.run_daily_sweep()

    def run_automation_health_check(self) -> list[Finding]:
        """
        Find completed jobs older than 24 hours with no matching invoice.
        Post to #automation-failure if any found.  Never creates invoices.
        """
        completed_at_sql = self._text_timestamp_sql("j.completed_at")
        cutoff = datetime.utcnow() - timedelta(hours=24)
        conn = self._connect()
        try:
            rows = conn.execute(
                f"""
                SELECT j.id
                FROM jobs j
                WHERE j.status = 'completed'
                  AND {completed_at_sql} IS NOT NULL
                  AND {completed_at_sql} < %s
                  AND NOT EXISTS (
                      SELECT 1
                      FROM invoices i
                      WHERE i.job_id = j.id
                  )
                ORDER BY {completed_at_sql} DESC
                """,
                (cutoff,),
            ).fetchall()
        finally:
            conn.close()

        if not rows:
            logger.info("Automation health check: no uninvoiced completed jobs found.")
            return []

        job_ids = [row["id"] for row in rows]
        count = len(job_ids)
        id_list = ", ".join(job_ids[:20])
        if count > 20:
            id_list += f", ... (+{count - 20} more)"

        description = (
            f"{count} completed job(s) from yesterday have no invoices. "
            f"The Jobber-to-QuickBooks automation may have missed them. "
            f"Job IDs: {id_list}"
        )

        finding = Finding(
            category="reconciliation_automation_gap",
            tool="quickbooks",
            entity="jobs-without-invoices",
            description=description,
            auto_fixable=False,
            count=count,
            details=f"*Job IDs:* `{id_list}`",
        )

        logger.warning("Automation health check: %s", description)

        if not self.dry_run:
            report_reconciliation_issue(
                {
                    "category": "reconciliation_automation_gap",
                    "tool": "quickbooks",
                    "entity": "completed jobs",
                    "count": count,
                    "details": (
                        f"{count} completed jobs from yesterday don't have invoices.\n"
                        f"The Jobber-to-QuickBooks automation may have missed them.\n"
                        f"Job IDs: {id_list}"
                    ),
                },
                dry_run=self.dry_run,
            )

        return [finding]

    # ------------------------------------------------------------------
    # Per-tool checks
    # ------------------------------------------------------------------

    def _check_hubspot(self, canonical_id: str, client: dict) -> list[Finding]:
        """Verify the HubSpot contact exists and key fields match the database."""
        findings: list[Finding] = []

        hubspot_id = get_tool_id(canonical_id, "hubspot", self.db_path)
        if hubspot_id is None:
            findings.append(Finding(
                category="reconciliation_missing",
                tool="hubspot",
                entity=canonical_id,
                description=f"No HubSpot mapping found for {canonical_id}",
                auto_fixable=False,
            ))
            return findings

        try:
            hs_client = get_client("hubspot")
            hubspot_throttler.wait()
            contact = hs_client.crm.contacts.basic_api.get_by_id(
                contact_id=hubspot_id,
                properties=["email", "firstname", "lastname", "lifecyclestage"],
            )
            props = contact.properties
        except Exception as exc:
            msg = str(exc)
            if "404" in msg or "not found" in msg.lower():
                findings.append(Finding(
                    category="reconciliation_missing",
                    tool="hubspot",
                    entity=canonical_id,
                    description=f"HubSpot contact {hubspot_id} not found (mapped but deleted)",
                    auto_fixable=False,
                ))
            else:
                logger.warning("HubSpot fetch error for %s: %s", canonical_id, exc)
            return findings

        # Field comparisons
        mismatches: list[str] = []

        expected_email = (client.get("email") or "").lower()
        actual_email = (props.get("email") or "").lower()
        if expected_email and actual_email and expected_email != actual_email:
            mismatches.append(f"email: Database={expected_email!r}, HubSpot={actual_email!r}")

        expected_fn = (client.get("first_name") or "").strip().lower()
        actual_fn = (props.get("firstname") or "").strip().lower()
        if expected_fn and actual_fn and expected_fn != actual_fn:
            mismatches.append(f"first_name: Database={expected_fn!r}, HubSpot={actual_fn!r}")

        expected_ln = (client.get("last_name") or "").strip().lower()
        actual_ln = (props.get("lastname") or "").strip().lower()
        if expected_ln and actual_ln and expected_ln != actual_ln:
            mismatches.append(f"last_name: Database={expected_ln!r}, HubSpot={actual_ln!r}")

        if mismatches:
            findings.append(Finding(
                category="reconciliation_mismatch",
                tool="hubspot",
                entity=canonical_id,
                description=f"Field mismatch on HubSpot contact {hubspot_id}",
                auto_fixable=False,
                details="; ".join(mismatches),
            ))

        return findings

    def _check_mailchimp(self, canonical_id: str, client: dict) -> list[Finding]:
        """Verify the Mailchimp subscriber exists and fields match the database."""
        findings: list[Finding] = []

        email = client.get("email")
        if not email:
            return findings

        subscriber_hash = hashlib.md5(email.lower().encode()).hexdigest()
        audience_id = self._tool_ids["mailchimp"]["audience_id"]

        try:
            mc = get_client("mailchimp")
            mailchimp_throttler.wait()
            member = mc.lists.get_list_member(audience_id, subscriber_hash)
        except Exception as exc:
            msg = str(exc)
            if "404" in msg or "Resource Not Found" in msg:
                findings.append(Finding(
                    category="reconciliation_missing",
                    tool="mailchimp",
                    entity=canonical_id,
                    description=f"No Mailchimp subscriber for {email}",
                    auto_fixable=True,
                ))
            else:
                logger.warning("Mailchimp fetch error for %s: %s", canonical_id, exc)
            return findings

        mismatches: list[str] = []

        # Status check: churned clients should be unsubscribed
        actual_status = member.get("status", "")
        client_status = client.get("status", "active")
        expected_status = "unsubscribed" if client_status == "churned" else "subscribed"
        if actual_status not in ("subscribed", "unsubscribed", "cleaned", "pending"):
            pass  # non-standard status, skip
        elif actual_status != expected_status:
            mismatches.append(
                f"status: expected={expected_status!r}, mailchimp={actual_status!r}"
            )

        merge = member.get("merge_fields", {})
        expected_fn = (client.get("first_name") or "").strip()
        actual_fn = (merge.get("FNAME") or "").strip()
        if expected_fn and actual_fn and expected_fn.lower() != actual_fn.lower():
            mismatches.append(f"FNAME: Database={expected_fn!r}, Mailchimp={actual_fn!r}")

        expected_ln = (client.get("last_name") or "").strip()
        actual_ln = (merge.get("LNAME") or "").strip()
        if expected_ln and actual_ln and expected_ln.lower() != actual_ln.lower():
            mismatches.append(f"LNAME: Database={expected_ln!r}, Mailchimp={actual_ln!r}")

        if mismatches:
            findings.append(Finding(
                category="reconciliation_mismatch",
                tool="mailchimp",
                entity=canonical_id,
                description=f"Field mismatch on Mailchimp subscriber {email}",
                auto_fixable=True,
                details="; ".join(mismatches),
            ))

        return findings

    def _check_quickbooks(self, canonical_id: str, client: dict) -> list[Finding]:
        """Verify the QBO customer exists and key fields match the database."""
        findings: list[Finding] = []

        qbo_id = get_tool_id(canonical_id, "quickbooks", self.db_path)
        if qbo_id is None:
            # QBO customer may not be created yet for recent onboards — only flag
            # if the client has at least one completed job (needs invoicing).
            conn = self._connect()
            try:
                job_count = conn.execute(
                    "SELECT COUNT(*) AS cnt FROM jobs WHERE client_id = %s AND status = 'completed'",
                    (canonical_id,),
                ).fetchone()["cnt"]
            finally:
                conn.close()

            if job_count > 0:
                findings.append(Finding(
                    category="reconciliation_missing",
                    tool="quickbooks",
                    entity=canonical_id,
                    description=(
                        f"No QBO customer mapping for {canonical_id} "
                        f"but {job_count} completed job(s) exist"
                    ),
                    auto_fixable=True,
                ))
            return findings

        try:
            headers = get_client("quickbooks")
            base_url = _qbo_base_url()
            qbo_throttler.wait()
            resp = requests.get(
                f"{base_url}/customer/{qbo_id}",
                headers=headers,
                params={"minorversion": "65"},
                timeout=30,
            )
            if resp.status_code == 404:
                findings.append(Finding(
                    category="reconciliation_missing",
                    tool="quickbooks",
                    entity=canonical_id,
                    description=f"QBO customer {qbo_id} not found (mapped but deleted)",
                    auto_fixable=True,
                ))
                return findings
            resp.raise_for_status()
            customer = resp.json().get("Customer", {})
        except requests.HTTPError as exc:
            logger.warning("QBO fetch error for %s: %s", canonical_id, exc)
            return findings

        mismatches: list[str] = []

        qbo_email = (
            (customer.get("PrimaryEmailAddr") or {}).get("Address") or ""
        ).lower()
        expected_email = (client.get("email") or "").lower()
        if expected_email and qbo_email and expected_email != qbo_email:
            mismatches.append(f"email: Database={expected_email!r}, QBO={qbo_email!r}")

        # Check canonical ID is embedded in Notes
        notes = customer.get("Notes", "") or ""
        if canonical_id not in notes:
            mismatches.append(f"Notes missing canonical ID {canonical_id!r}")

        if mismatches:
            findings.append(Finding(
                category="reconciliation_mismatch",
                tool="quickbooks",
                entity=canonical_id,
                description=f"Field mismatch on QBO customer {qbo_id}",
                auto_fixable=True,
                details="; ".join(mismatches),
            ))

        return findings

    def _check_jobber(self, canonical_id: str, client: dict) -> list[Finding]:
        """Verify the Jobber client record exists. NOT auto-fixable."""
        findings: list[Finding] = []

        jobber_id = get_tool_id(canonical_id, "jobber", self.db_path)
        if jobber_id is None:
            # Only flag clients that have been active long enough to need Jobber records.
            # HubSpot-only sync artifacts and pre-fulfillment contacts may not
            # have Jobber records yet, so only alert once downstream state
            # shows the client has entered fulfillment/billing flows.
            client_status = client.get("status", "active")
            if (
                client_status in ("active", "churned", "occasional")
                and self._client_requires_jobber_record(canonical_id)
            ):
                findings.append(Finding(
                    category="reconciliation_missing",
                    tool="jobber",
                    entity=canonical_id,
                    description=f"No Jobber client mapping for {canonical_id} (status={client_status})",
                    auto_fixable=False,
                ))
            return findings

        query = """
        query ClientGet($id: EncodedId!) {
          client(id: $id) {
            id
            firstName
            lastName
            email
          }
        }
        """
        try:
            session = get_client("jobber")
            jobber_throttler.wait()
            resp = session.post(
                _JOBBER_GQL_URL,
                json={"query": query, "variables": {"id": jobber_id}},
                headers={"X-JOBBER-GRAPHQL-VERSION": _JOBBER_VERSION_HEADER},
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.warning("Jobber fetch error for %s: %s", canonical_id, exc)
            return findings

        gql_errors = data.get("errors")
        if gql_errors:
            logger.warning("Jobber GQL errors for %s: %s", canonical_id, gql_errors)
            return findings

        jobber_client = (data.get("data") or {}).get("client")
        if jobber_client is None:
            findings.append(Finding(
                category="reconciliation_missing",
                tool="jobber",
                entity=canonical_id,
                description=f"Jobber client {jobber_id} returned null (deleted or archived)",
                auto_fixable=False,
            ))
            return findings

        # Email mismatch check (informational only — not auto-fixable)
        expected_email = (client.get("email") or "").lower()
        actual_email = (jobber_client.get("email") or "").lower()
        if expected_email and actual_email and expected_email != actual_email:
            findings.append(Finding(
                category="reconciliation_mismatch",
                tool="jobber",
                entity=canonical_id,
                description=f"Email mismatch on Jobber client {jobber_id}",
                auto_fixable=False,
                details=f"Database={expected_email!r}, Jobber={actual_email!r}",
            ))

        return findings

    def _check_asana(self, canonical_id: str, client: dict) -> list[Finding]:
        """Check that database tasks for this client have Asana mappings."""
        findings: list[Finding] = []

        conn = self._connect()
        try:
            tasks = conn.execute(
                "SELECT id FROM tasks WHERE client_id = %s", (canonical_id,)
            ).fetchall()
        finally:
            conn.close()

        if not tasks:
            return findings

        unmapped = []
        for task_row in tasks:
            task_id = task_row["id"]
            asana_id = get_tool_id(task_id, "asana", self.db_path)
            if asana_id is None:
                unmapped.append(task_id)

        if unmapped:
            findings.append(Finding(
                category="reconciliation_missing",
                tool="asana",
                entity=canonical_id,
                description=(
                    f"{len(unmapped)} task(s) for {canonical_id} have no Asana mapping"
                ),
                auto_fixable=False,
                count=len(unmapped),
                details="Task IDs: " + ", ".join(unmapped[:10]),
            ))

        return findings

    # ------------------------------------------------------------------
    # Auto-repair
    # ------------------------------------------------------------------

    def _apply_repairs(
        self,
        canonical_id: str,
        client: dict,
        report: ReconciliationReport,
    ) -> None:
        """Attempt to repair all auto-fixable findings in the report."""
        for finding in report.findings:
            if not finding.auto_fixable:
                continue
            try:
                fixed = False
                if finding.tool == "mailchimp":
                    fixed = self._repair_mailchimp(canonical_id, client)
                elif finding.tool == "quickbooks":
                    fixed = self._repair_quickbooks(canonical_id, client)

                if fixed:
                    report.repaired.append(finding.description)
                    logger.info("Auto-repaired: %s", finding.description)
            except Exception as exc:
                logger.warning("Repair failed for %s/%s: %s", finding.tool, canonical_id, exc)

    def _repair_mailchimp(self, canonical_id: str, client: dict) -> bool:
        """Create or update the Mailchimp subscriber from database data."""
        email = client.get("email")
        if not email:
            return False

        subscriber_hash = hashlib.md5(email.lower().encode()).hexdigest()
        audience_id = self._tool_ids["mailchimp"]["audience_id"]
        client_status = client.get("status", "active")
        mc_status = "unsubscribed" if client_status == "churned" else "subscribed"
        client_type = client.get("client_type", "residential")

        body = {
            "email_address": email,
            "status": mc_status,
            "merge_fields": {
                "FNAME": client.get("first_name") or "",
                "LNAME": client.get("last_name") or "",
                "CLIENTTYPE": client_type,
            },
            "tags": [client_type, client_status],
        }

        mc = get_client("mailchimp")
        mailchimp_throttler.wait()

        if self.dry_run:
            logger.info(
                "[DRY RUN] Would upsert Mailchimp subscriber %s for %s", email, canonical_id
            )
            return True

        mc.lists.set_list_member(audience_id, subscriber_hash, body)
        logger.info("Repaired Mailchimp subscriber %s for %s", email, canonical_id)
        return True

    def _repair_quickbooks(self, canonical_id: str, client: dict) -> bool:
        """Create the QBO customer from database data."""
        if self.dry_run:
            logger.info("[DRY RUN] Would create QBO customer for %s", canonical_id)
            return True

        first = client.get("first_name") or ""
        last = client.get("last_name") or ""
        company = client.get("company_name") or ""
        display_name = company if company else f"{first} {last}".strip()
        if not display_name:
            logger.warning("Cannot create QBO customer for %s: no display name", canonical_id)
            return False

        body: dict = {
            "DisplayName": display_name,
            "Notes": f"SS-ID: {canonical_id}",
        }
        if client.get("email"):
            body["PrimaryEmailAddr"] = {"Address": client["email"]}
        if client.get("phone"):
            body["PrimaryPhone"] = {"FreeFormNumber": client["phone"]}
        if client.get("address"):
            body["BillAddr"] = {
                "Line1": client.get("address") or "",
                "City": client.get("neighborhood") or "",
                "CountrySubDivisionCode": "TX",
                "PostalCode": "",
            }

        headers = get_client("quickbooks")
        base_url = _qbo_base_url()
        qbo_throttler.wait()
        resp = requests.post(
            f"{base_url}/customer",
            headers=headers,
            json=body,
            params={"minorversion": "65"},
            timeout=30,
        )

        if resp.status_code == 400:
            fault = resp.json().get("Fault", {})
            errors = fault.get("Error", [])
            # 6240 = duplicate
            if any(str(e.get("code")) == "6240" for e in errors):
                logger.info("QBO customer already exists for %s (6240)", canonical_id)
                return False
        resp.raise_for_status()

        qbo_customer = resp.json().get("Customer", {})
        qbo_id = str(qbo_customer.get("Id", ""))
        if qbo_id:
            register_mapping(canonical_id, "quickbooks", qbo_id, db_path=self.db_path)
            logger.info("Created QBO customer %s for %s", qbo_id, canonical_id)
            return True

        return False

    # ------------------------------------------------------------------
    # Slack reporting for non-auto-fixable issues
    # ------------------------------------------------------------------

    def _post_non_fixable(self, report: ReconciliationReport) -> None:
        """Post each non-auto-fixable finding to #automation-failure."""
        for finding in report.findings:
            if finding.auto_fixable:
                continue
            if finding.description in report.repaired:
                continue
            report_reconciliation_issue(
                {
                    "category": finding.category,
                    "tool": finding.tool,
                    "entity": finding.entity,
                    "count": finding.count,
                    "details": finding.details or finding.description,
                },
                dry_run=self.dry_run,
            )

    # ------------------------------------------------------------------
    # Sweep sample builder
    # ------------------------------------------------------------------

    def _build_sweep_sample(self) -> list[str]:
        """
        Return up to 15 canonical_ids for the daily sweep:
          5 recently onboarded (last 14 days)
          5 active recurring (random)
          3 recently churned (last 30 days)
          2 commercial (random)
        """
        now = datetime.utcnow()
        fourteen_days_ago = now - timedelta(days=14)
        thirty_days_ago = now - timedelta(days=30)
        created_at_sql = self._text_timestamp_sql("created_at")
        last_service_sql = self._text_timestamp_sql("last_service_date")

        conn = self._connect()
        try:
            recently_onboarded = [
                row["id"] for row in conn.execute(
                    f"""
                    SELECT id FROM clients
                    WHERE {created_at_sql} IS NOT NULL
                      AND {created_at_sql} >= %s
                    ORDER BY {created_at_sql} DESC
                    """,
                    (fourteen_days_ago,),
                ).fetchall()
            ]

            active_recurring = [
                row["id"] for row in conn.execute(
                    """
                    SELECT DISTINCT c.id FROM clients c
                    JOIN recurring_agreements ra ON ra.client_id = c.id
                    WHERE c.status = 'active'
                      AND ra.status = 'active'
                    """
                ).fetchall()
            ]

            recently_churned = [
                row["id"] for row in conn.execute(
                    f"""
                    SELECT id FROM clients
                    WHERE status = 'churned'
                      AND {last_service_sql} IS NOT NULL
                      AND {last_service_sql} >= %s
                    ORDER BY {last_service_sql} DESC
                    """,
                    (thirty_days_ago,),
                ).fetchall()
            ]

            commercial = [
                row["id"] for row in conn.execute(
                    "SELECT id FROM clients WHERE client_type = 'commercial'"
                ).fetchall()
            ]
        finally:
            conn.close()

        def _sample(pool: list, n: int) -> list:
            return random.sample(pool, min(n, len(pool)))

        chosen: list[str] = []
        seen: set[str] = set()

        def _add(ids: list) -> None:
            for cid in ids:
                if cid not in seen:
                    seen.add(cid)
                    chosen.append(cid)

        _add(_sample(recently_onboarded, 5))
        _add(_sample(active_recurring, 5))
        _add(_sample(recently_churned, 3))
        _add(_sample(commercial, 2))

        logger.info(
            "Sweep sample: %d recently onboarded, %d active recurring, "
            "%d recently churned, %d commercial  →  %d unique",
            min(5, len(recently_onboarded)),
            min(5, len(active_recurring)),
            min(3, len(recently_churned)),
            min(2, len(commercial)),
            len(chosen),
        )
        return chosen


# ---------------------------------------------------------------------------
# Report printer
# ---------------------------------------------------------------------------

def _print_sweep_summary(
    reports: list[ReconciliationReport],
    automation_findings: list[Finding],
) -> None:
    total = len(reports)
    with_issues = sum(1 for r in reports if r.has_issues)
    total_findings = sum(len(r.findings) for r in reports)
    total_repaired = sum(len(r.repaired) for r in reports)

    print(f"\n{'='*60}")
    print(f"  RECONCILIATION SWEEP REPORT  —  {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}")
    print(f"  Clients checked : {total}")
    print(f"  Clients with issues : {with_issues}")
    print(f"  Total findings : {total_findings}")
    print(f"  Auto-repaired  : {total_repaired}")
    print()

    for report in reports:
        if report.has_issues:
            print(report.summary())
    if not with_issues:
        print("  All clients OK — no discrepancies found.")

    if automation_findings:
        print()
        print("  AUTOMATION HEALTH CHECK")
        print(f"  {'─'*40}")
        for f in automation_findings:
            print(f"  [CRITICAL] {f.description}")
    else:
        print()
        print("  Automation health check: OK — no uninvoiced completed jobs.")
    print(f"{'='*60}\n")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reconcile Sparkle & Shine client records across all tools.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python -m simulation.reconciliation.reconciler --client SS-CLIENT-0047\n"
            "  python -m simulation.reconciliation.reconciler --sweep\n"
            "  python -m simulation.reconciliation.reconciler --sweep --repair\n"
            "  python -m simulation.reconciliation.reconciler --sweep --dry-run\n"
        ),
    )
    parser.add_argument(
        "--client",
        metavar="CANONICAL_ID",
        help="Targeted mode: check a single client (e.g. SS-CLIENT-0047)",
    )
    parser.add_argument(
        "--sweep",
        action="store_true",
        help="Daily sweep mode: sample 15 clients + automation health check",
    )
    parser.add_argument(
        "--repair",
        action="store_true",
        help="Auto-repair fixable issues (Mailchimp missing, QBO missing, field mismatches)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log what would happen without making any API writes or Slack posts",
    )
    parser.add_argument(
        "--db",
        default=_DB_PATH,
        help="Path to sparkle_shine.db (default: project root)",
    )
    args = parser.parse_args()

    if not args.client and not args.sweep:
        parser.error("Specify --client CANONICAL_ID or --sweep")

    reconciler = Reconciler(
        db_path=args.db,
        repair=args.repair,
        dry_run=args.dry_run,
    )

    if args.client:
        report = reconciler.check_client(args.client)
        print(report.summary())
        if not report.has_issues:
            print(f"  Checked at: {report.checked_at}")

    elif args.sweep:
        reports, automation_findings = reconciler.run_daily_sweep()
        _print_sweep_summary(reports, automation_findings)


if __name__ == "__main__":
    main()
