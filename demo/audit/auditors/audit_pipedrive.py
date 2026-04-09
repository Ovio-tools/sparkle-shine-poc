"""
Pipedrive auditor -- compares Pipedrive deals and persons against SQLite.

Cross-reference check: for every won deal in Pipedrive, verify a matching
commercial client exists in Jobber AND QuickBooks via cross_tool_mapping.
This catches the scenario where a deal was marked won but the downstream
onboarding automation didn't fire.
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

from auth.simple_clients import get_pipedrive_session
from seeding.utils.throttler import PIPEDRIVE
from demo.audit.cross_tool_audit import AuditFinding, AuditSample, ToolAuditResult

# SQLite proposal status → Pipedrive deal status
_STATUS_MAP = {
    "won":         "won",
    "lost":        "lost",
    "sent":        "open",
    "negotiating": "open",
    "draft":       "open",
    "expired":     "lost",
}

# Pipedrive stage IDs → human labels (from config/tool_ids.json)
_STAGE_LABELS = {
    7:  "new lead",
    8:  "qualified",
    9:  "site visit scheduled",
    10: "proposal sent",
    11: "negotiation",
    12: "closed won",
    13: "closed lost",
}


_BASE_URL = "https://api.pipedrive.com/v1"


def _pd_get(session: requests.Session, path: str) -> dict:
    PIPEDRIVE.wait()
    resp = session.get(f"{_BASE_URL}/{path.lstrip('/')}", timeout=30)
    resp.raise_for_status()
    return resp.json()


def _normalize_str(s) -> str:
    return str(s or "").strip().lower()


def _amount_str(v) -> str:
    try:
        return f"{float(v):.2f}"
    except (TypeError, ValueError):
        return str(v or "")


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
            f"SQLite={expected!r}, Pipedrive={actual!r}"
        ),
    )


class PipedriveAuditor:
    def __init__(self, db_path: str):
        self.db_path = db_path

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def audit(self, sample: AuditSample) -> ToolAuditResult:
        """Check Pipedrive deals and persons against SQLite."""
        start = time.time()
        findings: list[AuditFinding] = []
        records_checked = 0

        session = get_pipedrive_session()

        # ---- Commercial proposals as Pipedrive deals ----
        for p in sample.proposals:
            pd_deal_id = p.get("pipedrive_deal_id")
            if not pd_deal_id:
                findings.append(AuditFinding(
                    severity="missing",
                    entity_type="proposal",
                    canonical_id=p["id"],
                    message=f"Proposal {p['id']} has no Pipedrive deal mapping",
                ))
                continue

            try:
                data = _pd_get(session, f"deals/{pd_deal_id}")
                deal = data.get("data")
            except requests.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 404:
                    findings.append(AuditFinding(
                        severity="missing",
                        entity_type="proposal",
                        canonical_id=p["id"],
                        tool_id=str(pd_deal_id),
                        message=f"Proposal {p['id']} (Pipedrive deal {pd_deal_id}) not found",
                    ))
                else:
                    findings.append(AuditFinding(
                        severity="missing",
                        entity_type="proposal",
                        canonical_id=p["id"],
                        tool_id=str(pd_deal_id),
                        message=f"Proposal {p['id']}: Pipedrive API error: {exc}",
                    ))
                continue
            except Exception as exc:
                findings.append(AuditFinding(
                    severity="missing",
                    entity_type="proposal",
                    canonical_id=p["id"],
                    tool_id=str(pd_deal_id),
                    message=f"Proposal {p['id']}: Pipedrive API error: {exc}",
                ))
                continue

            if not deal:
                findings.append(AuditFinding(
                    severity="missing",
                    entity_type="proposal",
                    canonical_id=p["id"],
                    tool_id=str(pd_deal_id),
                    message=f"Proposal {p['id']} (Pipedrive {pd_deal_id}) returned empty",
                ))
                continue

            records_checked += 1

            # title -- Pipedrive uses "{company} — {service description}", not the SQLite
            # generic "Commercial Cleaning Proposal" title. Check company name appears in title.
            actual_title = _normalize_str(deal.get("title", ""))
            company_name = _normalize_str(p.get("client_company_name", "") or p.get("title", ""))
            if company_name and company_name in actual_title:
                findings.append(AuditFinding(
                    severity="match",
                    entity_type="proposal",
                    canonical_id=p["id"],
                    tool_id=str(pd_deal_id),
                    field="title",
                    expected=company_name,
                    actual=actual_title,
                    message=f"{p['id']}: title contains company name ({company_name!r})",
                ))
            else:
                findings.append(_compare("proposal", p["id"], str(pd_deal_id), "title",
                                         company_name, actual_title))

            # value (annual contract value = monthly_value * 12)
            monthly = float(p.get("monthly_value") or 0)
            expected_value = _amount_str(monthly * 12)
            actual_value = _amount_str(deal.get("value", 0))
            findings.append(_compare("proposal", p["id"], str(pd_deal_id), "value",
                                     expected_value, actual_value))

            # status -- draft proposals may be marked "lost" in Pipedrive for historical
            # early-stage deals that never progressed; treat both "open" and "lost" as valid
            sqlite_status = _normalize_str(p.get("status", ""))
            expected_status = _STATUS_MAP.get(sqlite_status, "open")
            actual_status = _normalize_str(deal.get("status", ""))
            if sqlite_status == "draft" and actual_status in ("open", "lost"):
                findings.append(AuditFinding(
                    severity="match",
                    entity_type="proposal",
                    canonical_id=p["id"],
                    tool_id=str(pd_deal_id),
                    field="status",
                    expected=expected_status,
                    actual=actual_status,
                    message=f"{p['id']}: status acceptable for draft ({actual_status!r})",
                ))
            else:
                findings.append(_compare("proposal", p["id"], str(pd_deal_id), "status",
                                         expected_status, actual_status))

            # person_id (verify person is linked)
            pd_person_id = deal.get("person_id") and deal["person_id"].get("value")
            if p.get("client_id") or p.get("lead_id"):
                if pd_person_id:
                    findings.append(AuditFinding(
                        severity="match",
                        entity_type="proposal",
                        canonical_id=p["id"],
                        tool_id=str(pd_deal_id),
                        field="person_id",
                        actual=str(pd_person_id),
                        message=f"{p['id']}: person linked in Pipedrive ({pd_person_id})",
                    ))
                else:
                    findings.append(AuditFinding(
                        severity="mismatch",
                        entity_type="proposal",
                        canonical_id=p["id"],
                        tool_id=str(pd_deal_id),
                        field="person_id",
                        message=f"{p['id']}: deal has no person linked in Pipedrive",
                    ))

        # ---- Clients as Pipedrive persons ----
        for c in sample.clients:
            pd_person_id = c.get("pipedrive_person_id")
            if not pd_person_id:
                # Only commercial clients are expected to have Pipedrive persons
                if c.get("client_type") == "commercial":
                    findings.append(AuditFinding(
                        severity="missing",
                        entity_type="client",
                        canonical_id=c["id"],
                        message=f"Commercial client {c['id']} has no Pipedrive person mapping",
                    ))
                continue

            try:
                data = _pd_get(session, f"persons/{pd_person_id}")
                person = data.get("data")
            except requests.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 404:
                    findings.append(AuditFinding(
                        severity="missing",
                        entity_type="client",
                        canonical_id=c["id"],
                        tool_id=str(pd_person_id),
                        message=f"Client {c['id']} (Pipedrive person {pd_person_id}) not found",
                    ))
                else:
                    findings.append(AuditFinding(
                        severity="missing",
                        entity_type="client",
                        canonical_id=c["id"],
                        tool_id=str(pd_person_id),
                        message=f"Client {c['id']}: Pipedrive API error: {exc}",
                    ))
                continue
            except Exception as exc:
                findings.append(AuditFinding(
                    severity="missing",
                    entity_type="client",
                    canonical_id=c["id"],
                    tool_id=str(pd_person_id),
                    message=f"Client {c['id']}: Pipedrive API error: {exc}",
                ))
                continue

            if not person:
                findings.append(AuditFinding(
                    severity="missing",
                    entity_type="client",
                    canonical_id=c["id"],
                    tool_id=str(pd_person_id),
                    message=f"Client {c['id']} Pipedrive person {pd_person_id} returned empty",
                ))
                continue

            records_checked += 1

            # name -- Pipedrive persons are always created with the contact's full name
            # (not the company name, which is stored on the linked Pipedrive organization)
            expected_name = _normalize_str(
                f"{c.get('first_name', '')} {c.get('last_name', '')}".strip()
            )
            actual_name = _normalize_str(person.get("name", ""))
            findings.append(_compare("client", c["id"], str(pd_person_id), "name",
                                     expected_name, actual_name))

            # email
            emails = person.get("email") or []
            actual_email = _normalize_str(emails[0].get("value", "") if emails else "")
            expected_email = _normalize_str(c.get("email", ""))
            findings.append(_compare("client", c["id"], str(pd_person_id), "email",
                                     expected_email, actual_email))

        # ---- Cross-reference: won deals → Jobber + QBO existence check ----
        cross_ref_findings = self._check_won_deal_onboarding(session)
        findings.extend(cross_ref_findings)

        return ToolAuditResult(
            tool_name="pipedrive",
            records_checked=records_checked,
            findings=findings,
            duration_seconds=time.time() - start,
        )

    def _check_won_deal_onboarding(
        self, session: requests.Session
    ) -> list[AuditFinding]:
        """For every won deal in Pipedrive, verify the client exists in Jobber and QBO.

        Catches: deal marked won but onboarding automation didn't fire.
        """
        from database.mappings import get_canonical_id, get_all_mappings

        findings: list[AuditFinding] = []

        try:
            data = _pd_get(session, "deals?status=won&limit=50")
            won_deals = data.get("data") or []
        except Exception as exc:
            findings.append(AuditFinding(
                severity="missing",
                entity_type="deal_cross_ref",
                canonical_id="AGGREGATE",
                message=f"Could not fetch won deals from Pipedrive: {exc}",
            ))
            return findings

        for deal in won_deals:
            deal_id = str(deal.get("id", ""))
            deal_title = deal.get("title", "")

            # Resolve to canonical ID
            canonical_id = get_canonical_id("pipedrive", deal_id, db_path=self.db_path)
            if not canonical_id:
                # Orphan: won deal with no canonical mapping
                findings.append(AuditFinding(
                    severity="orphan",
                    entity_type="deal_cross_ref",
                    canonical_id="UNKNOWN",
                    tool_id=deal_id,
                    message=(
                        f"Won Pipedrive deal {deal_id} ('{deal_title}') has no "
                        f"canonical mapping -- may be an orphan or seeding gap"
                    ),
                ))
                continue

            # Check that the won client has Jobber and QBO mappings
            all_maps = get_all_mappings(canonical_id, self.db_path)
            missing_tools = []
            if "jobber" not in all_maps:
                missing_tools.append("Jobber")
            if "quickbooks" not in all_maps:
                missing_tools.append("QuickBooks")

            if missing_tools:
                findings.append(AuditFinding(
                    severity="mismatch",
                    entity_type="deal_cross_ref",
                    canonical_id=canonical_id,
                    tool_id=deal_id,
                    field="onboarding",
                    message=(
                        f"Won deal {canonical_id} (Pipedrive {deal_id}) is missing "
                        f"downstream records in: {', '.join(missing_tools)}. "
                        f"Possible onboarding automation failure."
                    ),
                ))
            else:
                findings.append(AuditFinding(
                    severity="match",
                    entity_type="deal_cross_ref",
                    canonical_id=canonical_id,
                    tool_id=deal_id,
                    field="onboarding",
                    message=f"Won deal {canonical_id}: exists in Jobber and QuickBooks",
                ))

        return findings

    def fix_mismatch(self, finding: AuditFinding) -> bool:
        """Attempt to fix simple field mismatches in Pipedrive."""
        if not finding.tool_id or not finding.expected:
            return False

        if finding.entity_type == "proposal" and finding.field in ("title", "value", "status"):
            session = get_pipedrive_session()
            try:
                field_map = {"title": "title", "value": "value", "status": "status"}
                pd_field = field_map.get(finding.field)
                if not pd_field:
                    return False

                PIPEDRIVE.wait()
                base = session.base_url  # type: ignore[attr-defined]
                resp = session.put(
                    f"{base}/deals/{finding.tool_id}",
                    json={pd_field: finding.expected},
                    timeout=30,
                )
                return resp.status_code == 200
            except Exception:
                return False

        return False
