"""
Mailchimp auditor -- compares Mailchimp audience members against SQLite.

subscriber_hash = MD5(lowercase email)
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import time
from typing import Optional

_PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from auth.simple_clients import get_mailchimp_client
from seeding.utils.throttler import MAILCHIMP
from demo.audit.cross_tool_audit import AuditFinding, AuditSample, ToolAuditResult

# Mailchimp audience ID from config
_AUDIENCE_ID = "92f05d2d65"

# Merge field names used in the audience
_MERGE_FNAME = "FNAME"
_MERGE_LNAME = "LNAME"
_MERGE_CLIENT_TYPE = "CLIENTTYPE"


def _subscriber_hash(email: str) -> str:
    return hashlib.md5(email.strip().lower().encode()).hexdigest()


def _normalize_str(s) -> str:
    return str(s or "").strip().lower()


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
            f"SQLite={expected!r}, Mailchimp={actual!r}"
        ),
    )


class MailchimpAuditor:
    def __init__(self, db_path: str):
        self.db_path = db_path

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def audit(self, sample: AuditSample) -> ToolAuditResult:
        """Check Mailchimp audience members against SQLite."""
        start = time.time()
        findings: list[AuditFinding] = []
        records_checked = 0

        mc = get_mailchimp_client()

        # ---- Individual member checks ----
        for c in sample.mailchimp_contacts:
            email = (c.get("email") or "").strip().lower()
            if not email:
                findings.append(AuditFinding(
                    severity="missing",
                    entity_type="contact",
                    canonical_id=c["id"],
                    message=f"Contact {c['id']} has no email address",
                ))
                continue

            sub_hash = _subscriber_hash(email)

            try:
                MAILCHIMP.wait()
                member = mc.lists.get_list_member(_AUDIENCE_ID, sub_hash)
            except Exception as exc:
                err_str = str(exc)
                if "404" in err_str or "Resource Not Found" in err_str:
                    findings.append(AuditFinding(
                        severity="missing",
                        entity_type="contact",
                        canonical_id=c["id"],
                        tool_id=sub_hash,
                        message=f"Contact {c['id']} ({email}) not found in Mailchimp audience",
                    ))
                else:
                    findings.append(AuditFinding(
                        severity="missing",
                        entity_type="contact",
                        canonical_id=c["id"],
                        tool_id=sub_hash,
                        message=f"Contact {c['id']}: Mailchimp API error: {exc}",
                    ))
                continue

            records_checked += 1

            # email (identity check)
            actual_email = _normalize_str(member.get("email_address", ""))
            findings.append(_compare("contact", c["id"], sub_hash, "email",
                                     _normalize_str(email), actual_email))

            # subscription status (active → subscribed, churned → unsubscribed)
            expected_mc_status = "subscribed" if c.get("status") in ("active", "occasional") else "unsubscribed"
            actual_mc_status = _normalize_str(member.get("status", ""))
            findings.append(_compare("contact", c["id"], sub_hash, "status",
                                     expected_mc_status, actual_mc_status))

            # merge fields
            merge_fields = member.get("merge_fields") or {}

            # FNAME
            expected_fname = _normalize_str(c.get("first_name", ""))
            actual_fname = _normalize_str(merge_fields.get(_MERGE_FNAME, ""))
            if expected_fname or actual_fname:
                findings.append(_compare("contact", c["id"], sub_hash, "merge_FNAME",
                                         expected_fname, actual_fname))

            # LNAME
            expected_lname = _normalize_str(c.get("last_name", ""))
            actual_lname = _normalize_str(merge_fields.get(_MERGE_LNAME, ""))
            if expected_lname or actual_lname:
                findings.append(_compare("contact", c["id"], sub_hash, "merge_LNAME",
                                         expected_lname, actual_lname))

            # CLIENTTYPE
            expected_client_type = _normalize_str(c.get("client_type", ""))
            actual_client_type = _normalize_str(merge_fields.get(_MERGE_CLIENT_TYPE, ""))
            if expected_client_type or actual_client_type:
                findings.append(_compare("contact", c["id"], sub_hash, "merge_CLIENTTYPE",
                                         expected_client_type, actual_client_type))

            # tags -- verify expected tags are present
            mc_tags = {_normalize_str(t.get("name", "")) for t in (member.get("tags") or [])}
            expected_tags = _expected_tags(c)
            for tag in expected_tags:
                if _normalize_str(tag) in mc_tags:
                    findings.append(AuditFinding(
                        severity="match",
                        entity_type="contact",
                        canonical_id=c["id"],
                        tool_id=sub_hash,
                        field=f"tag:{tag}",
                        message=f"{c['id']}: tag '{tag}' present in Mailchimp",
                    ))
                else:
                    findings.append(AuditFinding(
                        severity="mismatch",
                        entity_type="contact",
                        canonical_id=c["id"],
                        tool_id=sub_hash,
                        field=f"tag:{tag}",
                        expected=tag,
                        actual="(not present)",
                        message=f"{c['id']}: expected tag '{tag}' missing in Mailchimp",
                    ))

        # ---- Aggregate member count check ----
        agg_findings = self._check_member_count_aggregate()
        findings.extend(agg_findings)

        return ToolAuditResult(
            tool_name="mailchimp",
            records_checked=records_checked,
            findings=findings,
            duration_seconds=time.time() - start,
        )

    def _check_member_count_aggregate(self) -> list[AuditFinding]:
        """Compare total Mailchimp member count against SQLite mapped contacts."""
        from database.schema import get_connection

        findings: list[AuditFinding] = []

        try:
            mc = get_mailchimp_client()
            MAILCHIMP.wait()
            list_info = mc.lists.get_list(_AUDIENCE_ID)
            stats = list_info.get("stats") or {}
            mc_total = int(stats.get("member_count", 0)) + int(stats.get("unsubscribe_count", 0))
        except Exception as exc:
            findings.append(AuditFinding(
                severity="missing",
                entity_type="mc_aggregate",
                canonical_id="AGGREGATE",
                message=f"Could not fetch Mailchimp list stats: {exc}",
            ))
            return findings

        try:
            conn = get_connection(self.db_path)
            row = conn.execute("""
                SELECT COUNT(*) AS cnt FROM cross_tool_mapping
                WHERE tool_name = 'mailchimp'
            """).fetchone()
            conn.close()
            sqlite_count = row["cnt"] if row else 0
        except Exception as exc:
            findings.append(AuditFinding(
                severity="missing",
                entity_type="mc_aggregate",
                canonical_id="AGGREGATE",
                message=f"Could not count Mailchimp mappings in SQLite: {exc}",
            ))
            return findings

        tolerance = max(5, int(sqlite_count * 0.05))
        diff = abs(mc_total - sqlite_count)
        if diff <= tolerance:
            findings.append(AuditFinding(
                severity="match",
                entity_type="mc_aggregate",
                canonical_id="AGGREGATE",
                field="member_count",
                expected=str(sqlite_count),
                actual=str(mc_total),
                message=(
                    f"Mailchimp member count within 5% tolerance: "
                    f"SQLite={sqlite_count}, Mailchimp={mc_total} (diff={diff})"
                ),
            ))
        else:
            findings.append(AuditFinding(
                severity="mismatch",
                entity_type="mc_aggregate",
                canonical_id="AGGREGATE",
                field="member_count",
                expected=str(sqlite_count),
                actual=str(mc_total),
                message=(
                    f"Mailchimp member count exceeds 5% tolerance: "
                    f"SQLite={sqlite_count}, Mailchimp={mc_total} (diff={diff})"
                ),
            ))

        return findings

    def fix_mismatch(self, finding: AuditFinding) -> bool:
        """Attempt to fix a simple field mismatch in Mailchimp."""
        if not finding.tool_id or not finding.expected:
            return False

        mc = get_mailchimp_client()

        if finding.field == "status":
            # Resubscribe or unsubscribe
            new_status = finding.expected
            if new_status not in ("subscribed", "unsubscribed", "pending"):
                return False
            try:
                MAILCHIMP.wait()
                mc.lists.update_list_member(
                    _AUDIENCE_ID,
                    finding.tool_id,
                    {"status": new_status},
                )
                return True
            except Exception:
                return False

        if finding.field.startswith("merge_"):
            merge_key = finding.field[len("merge_"):]
            try:
                MAILCHIMP.wait()
                mc.lists.update_list_member(
                    _AUDIENCE_ID,
                    finding.tool_id,
                    {"merge_fields": {merge_key: finding.expected}},
                )
                return True
            except Exception:
                return False

        return False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _expected_tags(client: dict) -> list[str]:
    """Derive expected Mailchimp tags from a SQLite client record."""
    tags = []
    client_type = (client.get("client_type") or "").lower()
    status = (client.get("status") or "").lower()
    source = (client.get("acquisition_source") or "").lower()

    if client_type == "residential":
        tags.append("residential-client")
    elif client_type == "commercial":
        tags.append("commercial-client")

    if status in ("active", "occasional"):
        tags.append("active")
    elif status == "churned":
        tags.append("churned")

    if "referral" in source:
        tags.append("referral")

    return tags
