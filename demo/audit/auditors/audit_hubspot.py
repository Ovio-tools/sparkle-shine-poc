"""
HubSpot auditor -- compares HubSpot contacts and deals against SQLite.

IMPORTANT: HubSpot is the canonical source for lead_source.
If lead_source in HubSpot differs from SQLite, the finding flags it as
a SQLite issue (not a HubSpot issue). This distinction matters for the
briefing's conversion-by-source metrics.
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

from credentials import get_credential
from seeding.utils.throttler import HUBSPOT
from demo.audit.cross_tool_audit import AuditFinding, AuditSample, ToolAuditResult

_BASE_URL = "https://api.hubapi.com"

_CONTACT_PROPERTIES = (
    "email,firstname,lastname,lifecyclestage,"
    "client_type,lead_source_detail"
)

_DEAL_PROPERTIES = "dealname,amount,dealstage"

# SQLite status → expected HubSpot lifecycle stage
_LIFECYCLE_MAP = {
    "active":     "customer",
    "occasional": "customer",
    "churned":    "customer",
}

# SQLite proposal status → HubSpot deal stage label fragments
_DEAL_STAGE_MAP = {
    "sent":        "proposal sent",
    "negotiating": "negotiation",
    "won":         "closed won",
    "lost":        "closed lost",
    "draft":       "proposal sent",
    "expired":     "closed lost",
}


def _hs_get(path: str, token: str, params: Optional[dict] = None) -> dict:
    HUBSPOT.wait()
    resp = requests.get(
        f"{_BASE_URL}{path}",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        params=params or {},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def _normalize_str(s) -> str:
    return str(s or "").strip().lower()


def _compare(
    entity_type: str,
    canonical_id: str,
    tool_id: str,
    field: str,
    expected: str,
    actual: str,
    tool_label: str = "HubSpot",
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
            f"SQLite={expected!r}, {tool_label}={actual!r}"
        ),
    )


class HubSpotAuditor:
    def __init__(self, db_path: str):
        self.db_path = db_path

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def audit(self, sample: AuditSample) -> ToolAuditResult:
        """Check HubSpot contacts and deals against SQLite."""
        start = time.time()
        findings: list[AuditFinding] = []
        records_checked = 0

        token = get_credential("HUBSPOT_ACCESS_TOKEN")

        # ---- Contacts (clients + leads) ----
        for c in sample.clients:
            hs_id = c.get("hubspot_id")
            if not hs_id:
                findings.append(AuditFinding(
                    severity="missing",
                    entity_type="client",
                    canonical_id=c["id"],
                    message=f"Client {c['id']} has no HubSpot mapping",
                ))
                continue

            try:
                data = _hs_get(
                    f"/crm/v3/objects/contacts/{hs_id}",
                    token,
                    params={"properties": _CONTACT_PROPERTIES},
                )
                props = data.get("properties") or {}
            except requests.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 404:
                    findings.append(AuditFinding(
                        severity="missing",
                        entity_type="client",
                        canonical_id=c["id"],
                        tool_id=hs_id,
                        message=f"Client {c['id']} (HubSpot {hs_id}) not found in HubSpot",
                    ))
                else:
                    findings.append(AuditFinding(
                        severity="missing",
                        entity_type="client",
                        canonical_id=c["id"],
                        tool_id=hs_id,
                        message=f"Client {c['id']}: HubSpot API error: {exc}",
                    ))
                continue
            except Exception as exc:
                findings.append(AuditFinding(
                    severity="missing",
                    entity_type="client",
                    canonical_id=c["id"],
                    tool_id=hs_id,
                    message=f"Client {c['id']}: HubSpot API error: {exc}",
                ))
                continue

            records_checked += 1

            # email
            expected_email = _normalize_str(c.get("email", ""))
            actual_email = _normalize_str(props.get("email", ""))
            findings.append(_compare("client", c["id"], hs_id, "email",
                                     expected_email, actual_email))

            # first name
            expected_first = _normalize_str(c.get("first_name", ""))
            actual_first = _normalize_str(props.get("firstname", ""))
            findings.append(_compare("client", c["id"], hs_id, "firstname",
                                     expected_first, actual_first))

            # last name
            expected_last = _normalize_str(c.get("last_name", ""))
            actual_last = _normalize_str(props.get("lastname", ""))
            findings.append(_compare("client", c["id"], hs_id, "lastname",
                                     expected_last, actual_last))

            # lifecycle stage
            expected_stage = _LIFECYCLE_MAP.get(
                _normalize_str(c.get("status", "")), "customer"
            )
            actual_stage = _normalize_str(props.get("lifecyclestage", ""))
            findings.append(_compare("client", c["id"], hs_id, "lifecyclestage",
                                     expected_stage, actual_stage))

            # client_type
            expected_type = _normalize_str(c.get("client_type", ""))
            actual_type = _normalize_str(props.get("client_type", ""))
            if expected_type or actual_type:
                findings.append(_compare("client", c["id"], hs_id, "client_type",
                                         expected_type, actual_type))

            # lead_source -- HubSpot is canonical; flag mismatch as SQLite issue
            hs_lead_source = _normalize_str(props.get("lead_source_detail", ""))
            sqlite_lead_source = _normalize_str(c.get("acquisition_source", ""))
            if hs_lead_source and sqlite_lead_source and hs_lead_source != sqlite_lead_source:
                findings.append(AuditFinding(
                    severity="mismatch",
                    entity_type="client",
                    canonical_id=c["id"],
                    tool_id=hs_id,
                    field="lead_source_detail",
                    expected=hs_lead_source,
                    actual=sqlite_lead_source,
                    message=(
                        f"{c['id']}: lead_source mismatch (HubSpot is canonical). "
                        f"HubSpot={hs_lead_source!r}, SQLite={sqlite_lead_source!r}. "
                        f"Fix: update SQLite acquisition_source."
                    ),
                ))
            elif hs_lead_source or sqlite_lead_source:
                findings.append(AuditFinding(
                    severity="match",
                    entity_type="client",
                    canonical_id=c["id"],
                    tool_id=hs_id,
                    field="lead_source_detail",
                    expected=hs_lead_source,
                    actual=sqlite_lead_source,
                    message=f"{c['id']}: lead_source matches ({hs_lead_source!r})",
                ))

        # ---- Commercial proposals as HubSpot deals ----
        # HubSpot deals are created per commercial CLIENT (one deal per client).
        # Lead proposals (client_id is None) have no HubSpot deal by design.
        for p in sample.proposals:
            if not p.get("client_id"):
                # Lead proposal — no HubSpot deal expected; skip silently
                continue

            hs_deal_id = p.get("hubspot_deal_id")
            if not hs_deal_id:
                findings.append(AuditFinding(
                    severity="missing",
                    entity_type="proposal",
                    canonical_id=p["id"],
                    message=f"Proposal {p['id']} has no HubSpot deal mapping",
                ))
                continue

            try:
                data = _hs_get(
                    f"/crm/v3/objects/deals/{hs_deal_id}",
                    token,
                    params={"properties": _DEAL_PROPERTIES},
                )
                props = data.get("properties") or {}
            except requests.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 404:
                    findings.append(AuditFinding(
                        severity="missing",
                        entity_type="proposal",
                        canonical_id=p["id"],
                        tool_id=hs_deal_id,
                        message=f"Proposal {p['id']} (HubSpot deal {hs_deal_id}) not found",
                    ))
                else:
                    findings.append(AuditFinding(
                        severity="missing",
                        entity_type="proposal",
                        canonical_id=p["id"],
                        tool_id=hs_deal_id,
                        message=f"Proposal {p['id']}: HubSpot API error: {exc}",
                    ))
                continue
            except Exception as exc:
                findings.append(AuditFinding(
                    severity="missing",
                    entity_type="proposal",
                    canonical_id=p["id"],
                    tool_id=hs_deal_id,
                    message=f"Proposal {p['id']}: HubSpot API error: {exc}",
                ))
                continue

            records_checked += 1

            # deal name -- HubSpot deals are per-client ("{company} — Commercial Contract"),
            # not per-proposal. Verify the client's company name appears in the deal name.
            actual_name = _normalize_str(props.get("dealname", ""))
            company_name = _normalize_str(p.get("client_company_name", "") or p.get("title", ""))
            if company_name and company_name in actual_name:
                findings.append(AuditFinding(
                    severity="match",
                    entity_type="proposal",
                    canonical_id=p["id"],
                    tool_id=hs_deal_id,
                    field="dealname",
                    expected=company_name,
                    actual=actual_name,
                    message=f"{p['id']}: dealname contains company name ({company_name!r})",
                ))
            else:
                findings.append(_compare("proposal", p["id"], hs_deal_id, "dealname",
                                         company_name, actual_name))

            # deal stage (partial match: check expected fragment in actual)
            expected_stage_fragment = _DEAL_STAGE_MAP.get(
                _normalize_str(p.get("status", "")), ""
            )
            actual_stage = _normalize_str(props.get("dealstage", ""))
            if expected_stage_fragment and expected_stage_fragment in actual_stage:
                findings.append(AuditFinding(
                    severity="match",
                    entity_type="proposal",
                    canonical_id=p["id"],
                    tool_id=hs_deal_id,
                    field="dealstage",
                    expected=expected_stage_fragment,
                    actual=actual_stage,
                    message=f"{p['id']}: dealstage matches ({actual_stage!r})",
                ))
            elif expected_stage_fragment:
                findings.append(_compare("proposal", p["id"], hs_deal_id, "dealstage",
                                         expected_stage_fragment, actual_stage))

        return ToolAuditResult(
            tool_name="hubspot",
            records_checked=records_checked,
            findings=findings,
            duration_seconds=time.time() - start,
        )

    def fix_mismatch(self, finding: AuditFinding) -> bool:
        """Attempt to fix a simple field mismatch in HubSpot."""
        if not finding.tool_id or not finding.expected:
            return False

        # lead_source mismatches are SQLite issues -- not fixable in HubSpot
        if finding.field == "lead_source_detail":
            return False

        fixable_fields = {
            "client": {"firstname", "lastname", "lifecyclestage", "client_type"},
            "proposal": {"dealname", "amount"},
        }
        allowed = fixable_fields.get(finding.entity_type, set())
        if finding.field not in allowed:
            return False

        hs_field = finding.field
        if finding.entity_type == "client":
            endpoint = f"/crm/v3/objects/contacts/{finding.tool_id}"
        else:
            endpoint = f"/crm/v3/objects/deals/{finding.tool_id}"

        try:
            token = get_credential("HUBSPOT_ACCESS_TOKEN")
            HUBSPOT.wait()
            resp = requests.patch(
                f"{_BASE_URL}{endpoint}",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json={"properties": {hs_field: finding.expected}},
                timeout=30,
            )
            return resp.status_code == 200
        except Exception:
            return False
