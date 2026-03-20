"""
Jobber auditor -- compares Jobber GraphQL data against SQLite canonical records.
"""

from __future__ import annotations

import os
import sys
import time
from typing import Optional

_PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import requests

from auth.jobber_auth import get_jobber_session
from seeding.utils.throttler import JOBBER
from demo.audit.cross_tool_audit import AuditFinding, AuditSample, ToolAuditResult

_JOBBER_GQL = "https://api.getjobber.com/api/graphql"

_CLIENT_QUERY = """
query AuditClient($id: EncodedId!) {
  client(id: $id) {
    firstName
    lastName
    emails { address primary }
    phones { number primary }
  }
}
"""

_JOB_QUERY = """
query AuditJob($id: EncodedId!) {
  job(id: $id) {
    title
    startAt
    jobStatus
    client { id }
  }
}
"""

_RECURRING_QUERY = """
query AuditRecurringJob($id: EncodedId!) {
  job(id: $id) {
    id
    title
    visitSchedule { recurrenceSchedule { friendly } }
  }
}
"""


def _gql(session: requests.Session, query: str, variables: dict) -> dict:
    """Execute a Jobber GraphQL request, retrying once on 401."""
    JOBBER.wait()
    resp = session.post(_JOBBER_GQL, json={"query": query, "variables": variables}, timeout=30)
    if resp.status_code == 401:
        new_session = get_jobber_session()
        session.headers.update(new_session.headers)
        JOBBER.wait()
        resp = session.post(_JOBBER_GQL, json={"query": query, "variables": variables}, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _primary_email(emails: list[dict]) -> str:
    if not emails:
        return ""
    for e in emails:
        if e.get("primary"):
            return (e.get("address") or "").strip().lower()
    return (emails[0].get("address") or "").strip().lower()


def _primary_phone(phones: list[dict]) -> str:
    if not phones:
        return ""
    for p in phones:
        if p.get("primary"):
            return _normalize_phone(p.get("number", ""))
    return _normalize_phone(phones[0].get("number", ""))


def _normalize_phone(phone: str) -> str:
    """Strip all non-digit characters for comparison."""
    return "".join(c for c in (phone or "") if c.isdigit())


def _normalize_str(s: str) -> str:
    return (s or "").strip().lower()


# Jobber job status → SQLite status
_STATUS_MAP = {
    "active":           "scheduled",
    "action_required":  "scheduled",   # visit needs attention but job is still scheduled
    "completed":        "completed",
    "archived":         "cancelled",
    "cancelled":        "cancelled",
}

# SQLite frequency → Jobber recurring schedule frequency
_FREQ_MAP = {
    "weekly":    "WEEKLY",
    "biweekly":  "EVERY_TWO_WEEKS",
    "monthly":   "MONTHLY",
}


class JobberAuditor:
    def __init__(self, db_path: str):
        self.db_path = db_path

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def audit(self, sample: AuditSample) -> ToolAuditResult:
        """Check Jobber data against SQLite canonical records."""
        start = time.time()
        findings: list[AuditFinding] = []
        records_checked = 0

        session = get_jobber_session()

        # ---- Clients ----
        for c in sample.clients:
            jobber_id = c.get("jobber_id")
            if not jobber_id:
                findings.append(AuditFinding(
                    severity="missing",
                    entity_type="client",
                    canonical_id=c["id"],
                    message=f"Client {c['id']} has no Jobber mapping in cross_tool_mapping",
                ))
                continue

            try:
                data = _gql(session, _CLIENT_QUERY, {"id": jobber_id})
                client_data = (data.get("data") or {}).get("client")
            except Exception as exc:
                findings.append(AuditFinding(
                    severity="missing",
                    entity_type="client",
                    canonical_id=c["id"],
                    tool_id=jobber_id,
                    message=f"Client {c['id']}: Jobber API error: {exc}",
                ))
                continue

            if not client_data:
                findings.append(AuditFinding(
                    severity="missing",
                    entity_type="client",
                    canonical_id=c["id"],
                    tool_id=jobber_id,
                    message=f"Client {c['id']} (Jobber {jobber_id}) not found in Jobber",
                ))
                continue

            records_checked += 1

            # name
            expected_name = _normalize_str(
                f"{c.get('first_name', '')} {c.get('last_name', '')}".strip()
            )
            actual_name = _normalize_str(
                f"{client_data.get('firstName', '')} {client_data.get('lastName', '')}".strip()
            )
            findings.append(_compare(
                "client", c["id"], jobber_id, "name", expected_name, actual_name,
            ))

            # email
            expected_email = _normalize_str(c.get("email", ""))
            actual_email = _primary_email(client_data.get("emails") or [])
            findings.append(_compare(
                "client", c["id"], jobber_id, "email", expected_email, actual_email,
            ))

            # phone (digits only)
            expected_phone = _normalize_phone(c.get("phone", ""))
            actual_phone = _primary_phone(client_data.get("phones") or [])
            if expected_phone or actual_phone:
                findings.append(_compare(
                    "client", c["id"], jobber_id, "phone", expected_phone, actual_phone,
                ))

        # ---- Jobs ----
        for j in sample.jobs:
            jobber_id = j.get("jobber_id")
            if not jobber_id:
                findings.append(AuditFinding(
                    severity="missing",
                    entity_type="job",
                    canonical_id=j["id"],
                    message=f"Job {j['id']} has no Jobber mapping",
                ))
                continue

            try:
                data = _gql(session, _JOB_QUERY, {"id": jobber_id})
                job_data = (data.get("data") or {}).get("job")
            except Exception as exc:
                findings.append(AuditFinding(
                    severity="missing",
                    entity_type="job",
                    canonical_id=j["id"],
                    tool_id=jobber_id,
                    message=f"Job {j['id']}: Jobber API error: {exc}",
                ))
                continue

            if not job_data:
                findings.append(AuditFinding(
                    severity="missing",
                    entity_type="job",
                    canonical_id=j["id"],
                    tool_id=jobber_id,
                    message=f"Job {j['id']} (Jobber {jobber_id}) not found in Jobber",
                ))
                continue

            records_checked += 1

            # status
            jobber_status_raw = (job_data.get("jobStatus") or "").lower()
            expected_status = j.get("status", "")
            actual_status = _STATUS_MAP.get(jobber_status_raw, jobber_status_raw)
            findings.append(_compare(
                "job", j["id"], jobber_id, "status", expected_status, actual_status,
            ))

            # client assignment -- compare canonical client ID via reverse lookup
            from database.mappings import get_canonical_id
            jobber_client_id = (job_data.get("client") or {}).get("id")
            expected_client = j.get("client_id", "")
            actual_client = ""
            if jobber_client_id:
                actual_client = get_canonical_id("jobber", jobber_client_id, self.db_path) or jobber_client_id
            findings.append(_compare(
                "job", j["id"], jobber_id, "client_id", expected_client, actual_client,
            ))

        # ---- Recurring agreements ----
        for ra in sample.recurring_agreements:
            jobber_id = ra.get("jobber_id")
            if not jobber_id:
                findings.append(AuditFinding(
                    severity="missing",
                    entity_type="recurring_agreement",
                    canonical_id=ra["id"],
                    message=f"Recurring agreement {ra['id']} has no Jobber mapping",
                ))
                continue

            try:
                data = _gql(session, _RECURRING_QUERY, {"id": jobber_id})
                rec_data = (data.get("data") or {}).get("job")
            except Exception as exc:
                findings.append(AuditFinding(
                    severity="missing",
                    entity_type="recurring_agreement",
                    canonical_id=ra["id"],
                    tool_id=jobber_id,
                    message=f"Recurring {ra['id']}: Jobber API error: {exc}",
                ))
                continue

            if not rec_data:
                findings.append(AuditFinding(
                    severity="missing",
                    entity_type="recurring_agreement",
                    canonical_id=ra["id"],
                    tool_id=jobber_id,
                    message=f"Recurring {ra['id']} (Jobber {jobber_id}) not found in Jobber",
                ))
                continue

            records_checked += 1

            # existence confirmed; record a match finding
            findings.append(AuditFinding(
                severity="match",
                entity_type="recurring_agreement",
                canonical_id=ra["id"],
                tool_id=jobber_id,
                field="exists",
                message=f"{ra['id']}: recurring job found in Jobber ({rec_data.get('title', '')})",
            ))

        return ToolAuditResult(
            tool_name="jobber",
            records_checked=records_checked,
            findings=findings,
            duration_seconds=time.time() - start,
        )

    def fix_mismatch(self, finding: AuditFinding) -> bool:
        """Attempt to fix a simple field mismatch by updating Jobber.

        Returns True if the fix was applied, False if this finding requires
        manual intervention (structural issue, unsupported field, etc.).
        """
        if finding.entity_type != "client" or finding.field not in ("email", "phone"):
            return False
        if not finding.tool_id or not finding.expected:
            return False

        session = get_jobber_session()

        if finding.field == "email":
            mutation = """
            mutation ClientEdit($id: EncodedId!, $input: ClientEditInput!) {
              clientEdit(id: $id, input: $input) {
                client { id }
                userErrors { message }
              }
            }
            """
            variables = {
                "id": finding.tool_id,
                "input": {"emails": [{"address": finding.expected, "isPrimary": True}]},
            }
        elif finding.field == "phone":
            mutation = """
            mutation ClientEdit($id: EncodedId!, $input: ClientEditInput!) {
              clientEdit(id: $id, input: $input) {
                client { id }
                userErrors { message }
              }
            }
            """
            variables = {
                "id": finding.tool_id,
                "input": {"phones": [{"number": finding.expected, "isPrimary": True}]},
            }
        else:
            return False

        try:
            resp = _gql(session, mutation, variables)
            errors = (((resp.get("data") or {}).get("clientEdit") or {}).get("userErrors") or [])
            return len(errors) == 0
        except Exception:
            return False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _compare(
    entity_type: str,
    canonical_id: str,
    tool_id: str,
    field: str,
    expected: str,
    actual: str,
) -> AuditFinding:
    if expected == actual:
        return AuditFinding(
            severity="match",
            entity_type=entity_type,
            canonical_id=canonical_id,
            tool_id=tool_id,
            field=field,
            expected=expected,
            actual=actual,
            message=f"{canonical_id}: {field} matches ({expected!r})",
        )
    return AuditFinding(
        severity="mismatch",
        entity_type=entity_type,
        canonical_id=canonical_id,
        tool_id=tool_id,
        field=field,
        expected=expected,
        actual=actual,
        message=(
            f"{canonical_id}: {field} mismatch. "
            f"SQLite={expected!r}, Jobber={actual!r}"
        ),
    )
