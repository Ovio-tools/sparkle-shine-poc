"""
QuickBooks Online auditor -- compares QBO REST data against SQLite canonical records.

CRITICAL checks:
- Invoice amounts must match to the penny (affects briefing revenue numbers)
- AR balance aggregate must be within $100 of SQLite totals
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

from auth.quickbooks_auth import get_quickbooks_headers, get_company_id, get_base_url
from seeding.utils.throttler import QUICKBOOKS
from demo.audit.cross_tool_audit import AuditFinding, AuditSample, ToolAuditResult


def _get(path: str, headers: dict) -> dict:
    QUICKBOOKS.wait()
    base = get_base_url()
    url = f"{base}/{path.lstrip('/')}"
    resp = requests.get(url, headers=headers, params={"minorversion": "65"}, timeout=30)
    if resp.status_code == 401:
        headers = get_quickbooks_headers()
        QUICKBOOKS.wait()
        resp = requests.get(url, headers=headers, params={"minorversion": "65"}, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _normalize_str(s) -> str:
    return str(s or "").strip().lower()


def _amount_str(v) -> str:
    try:
        return f"{float(v):.2f}"
    except (TypeError, ValueError):
        return str(v or "")


# QBO invoice payment status → SQLite invoice status
_QBO_STATUS_MAP = {
    "emailsent": "sent",
    "viewed": "sent",
    "paid": "paid",
    "overdue": "overdue",
    "partiallypaid": "sent",
    "draft": "draft",
}


def _qbo_invoice_status(inv_obj: dict) -> str:
    """Derive a SQLite-compatible status from a QBO Invoice object."""
    balance = float(inv_obj.get("Balance", 0))
    total_amt = float(inv_obj.get("TotalAmt", 0))
    email_status = _normalize_str(inv_obj.get("EmailStatus", ""))
    due_date = inv_obj.get("DueDate", "")

    if balance == 0 and total_amt > 0:
        return "paid"

    import datetime
    today = datetime.date.today().isoformat()
    if due_date and due_date < today and balance > 0:
        return "overdue"

    if balance > 0:
        return "sent"

    return "draft"


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
            f"SQLite={expected!r}, QuickBooks={actual!r}"
        ),
    )


class QuickBooksAuditor:
    def __init__(self, db_path: str):
        self.db_path = db_path

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def audit(self, sample: AuditSample) -> ToolAuditResult:
        """Check QuickBooks data against SQLite."""
        start = time.time()
        findings: list[AuditFinding] = []
        records_checked = 0

        headers = get_quickbooks_headers()

        # ---- Invoices ----
        for inv in sample.invoices:
            qbo_id = inv.get("qbo_id")
            if not qbo_id:
                findings.append(AuditFinding(
                    severity="missing",
                    entity_type="invoice",
                    canonical_id=inv["id"],
                    message=f"Invoice {inv['id']} has no QuickBooks mapping",
                ))
                continue

            try:
                data = _get(f"invoice/{qbo_id}", headers)
                inv_obj = data.get("Invoice")
            except Exception as exc:
                findings.append(AuditFinding(
                    severity="missing",
                    entity_type="invoice",
                    canonical_id=inv["id"],
                    tool_id=qbo_id,
                    message=f"Invoice {inv['id']}: QBO API error: {exc}",
                ))
                continue

            if not inv_obj:
                findings.append(AuditFinding(
                    severity="missing",
                    entity_type="invoice",
                    canonical_id=inv["id"],
                    tool_id=qbo_id,
                    message=f"Invoice {inv['id']} (QBO {qbo_id}) not found in QuickBooks",
                ))
                continue

            records_checked += 1

            # CRITICAL: amount must match to the penny
            expected_amount = _amount_str(inv.get("amount", 0))
            actual_amount = _amount_str(inv_obj.get("TotalAmt", 0))
            findings.append(_compare(
                "invoice", inv["id"], qbo_id, "amount", expected_amount, actual_amount,
            ))

            # status
            expected_status = _normalize_str(inv.get("status", ""))
            actual_status = _qbo_invoice_status(inv_obj)
            findings.append(_compare(
                "invoice", inv["id"], qbo_id, "status", expected_status, actual_status,
            ))

            # due date
            expected_due = _normalize_str(inv.get("due_date", ""))
            actual_due = _normalize_str(inv_obj.get("DueDate", ""))
            if expected_due or actual_due:
                findings.append(_compare(
                    "invoice", inv["id"], qbo_id, "due_date", expected_due, actual_due,
                ))

        # ---- Clients as QBO Customers ----
        for c in sample.clients:
            qbo_id = c.get("qbo_id")
            if not qbo_id:
                findings.append(AuditFinding(
                    severity="missing",
                    entity_type="client",
                    canonical_id=c["id"],
                    message=f"Client {c['id']} has no QuickBooks mapping",
                ))
                continue

            try:
                data = _get(f"customer/{qbo_id}", headers)
                cust_obj = data.get("Customer")
            except Exception as exc:
                findings.append(AuditFinding(
                    severity="missing",
                    entity_type="client",
                    canonical_id=c["id"],
                    tool_id=qbo_id,
                    message=f"Client {c['id']}: QBO customer API error: {exc}",
                ))
                continue

            if not cust_obj:
                findings.append(AuditFinding(
                    severity="missing",
                    entity_type="client",
                    canonical_id=c["id"],
                    tool_id=qbo_id,
                    message=f"Client {c['id']} (QBO {qbo_id}) not found as Customer in QuickBooks",
                ))
                continue

            records_checked += 1

            # display name
            if c.get("client_type") == "commercial":
                expected_name = _normalize_str(c.get("company_name", ""))
            else:
                expected_name = _normalize_str(
                    f"{c.get('first_name', '')} {c.get('last_name', '')}".strip()
                )
            actual_name = _normalize_str(cust_obj.get("DisplayName", ""))
            findings.append(_compare(
                "client", c["id"], qbo_id, "display_name", expected_name, actual_name,
            ))

            # active status
            expected_active = "true" if c.get("status") in ("active", "occasional") else "false"
            actual_active = str(cust_obj.get("Active", True)).lower()
            findings.append(_compare(
                "client", c["id"], qbo_id, "active", expected_active, actual_active,
            ))

            # email
            expected_email = _normalize_str(c.get("email", ""))
            actual_email = _normalize_str(
                (cust_obj.get("PrimaryEmailAddr") or {}).get("Address", "")
            )
            if expected_email or actual_email:
                findings.append(_compare(
                    "client", c["id"], qbo_id, "email", expected_email, actual_email,
                ))

        # ---- Aggregate AR check ----
        ar_findings = self._check_ar_aggregate(headers)
        findings.extend(ar_findings)

        return ToolAuditResult(
            tool_name="quickbooks",
            records_checked=records_checked,
            findings=findings,
            duration_seconds=time.time() - start,
        )

    def _check_ar_aggregate(self, headers: dict) -> list[AuditFinding]:
        """Query total AR balance from QBO and compare against SQLite."""
        from database.schema import get_connection

        findings: list[AuditFinding] = []

        # SQLite AR total: sum of (amount - amount paid) for non-paid invoices
        try:
            conn = get_connection(self.db_path)
            row = conn.execute("""
                SELECT COALESCE(SUM(i.amount) - COALESCE(SUM(p.amount), 0), 0) AS ar_balance
                FROM invoices i
                LEFT JOIN payments p ON p.invoice_id = i.id
                WHERE i.status != 'paid'
            """).fetchone()
            conn.close()
            sqlite_ar = float(row["ar_balance"] if row else 0)
        except Exception as exc:
            findings.append(AuditFinding(
                severity="missing",
                entity_type="ar_aggregate",
                canonical_id="AGGREGATE",
                message=f"Could not compute SQLite AR total: {exc}",
            ))
            return findings

        # QBO AR total via query
        try:
            QUICKBOOKS.wait()
            base = get_base_url()
            resp = requests.get(
                f"{base}/query",
                headers=headers,
                params={
                    "query": "SELECT * FROM Invoice WHERE Balance > '0'",
                    "minorversion": "65",
                },
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            invoices_qbo = (data.get("QueryResponse") or {}).get("Invoice") or []
            qbo_ar = sum(float(inv.get("Balance", 0)) for inv in invoices_qbo)
        except Exception as exc:
            findings.append(AuditFinding(
                severity="missing",
                entity_type="ar_aggregate",
                canonical_id="AGGREGATE",
                message=f"Could not query QBO AR total: {exc}",
            ))
            return findings

        diff = abs(sqlite_ar - qbo_ar)
        if diff <= 100.0:
            findings.append(AuditFinding(
                severity="match",
                entity_type="ar_aggregate",
                canonical_id="AGGREGATE",
                field="ar_balance",
                expected=f"${sqlite_ar:.2f}",
                actual=f"${qbo_ar:.2f}",
                message=f"AR balance aggregate matches within tolerance: SQLite=${sqlite_ar:.2f}, QBO=${qbo_ar:.2f} (diff=${diff:.2f})",
            ))
        else:
            findings.append(AuditFinding(
                severity="mismatch",
                entity_type="ar_aggregate",
                canonical_id="AGGREGATE",
                field="ar_balance",
                expected=f"${sqlite_ar:.2f}",
                actual=f"${qbo_ar:.2f}",
                message=(
                    f"AR balance mismatch exceeds $100 tolerance: "
                    f"SQLite=${sqlite_ar:.2f}, QBO=${qbo_ar:.2f} (diff=${diff:.2f})"
                ),
            ))

        return findings

    def fix_mismatch(self, finding: AuditFinding) -> bool:
        """Attempt to fix a simple field mismatch in QuickBooks.

        Only handles invoice status and customer email corrections.
        Amount mismatches must be fixed manually (require line item reconciliation).
        """
        if finding.field == "amount":
            # Amount mismatches need manual reconciliation -- never auto-fix
            return False

        if finding.entity_type == "client" and finding.field == "email":
            if not finding.tool_id or not finding.expected:
                return False
            try:
                headers = get_quickbooks_headers()
                # Read-Modify-Write: QBO requires the full object for updates
                data = _get(f"customer/{finding.tool_id}", headers)
                cust = data.get("Customer")
                if not cust:
                    return False

                cust.setdefault("PrimaryEmailAddr", {})["Address"] = finding.expected
                QUICKBOOKS.wait()
                base = get_base_url()
                resp = requests.post(
                    f"{base}/customer",
                    headers=headers,
                    json={"sparse": True, "Id": cust["Id"], "SyncToken": cust["SyncToken"],
                          "PrimaryEmailAddr": {"Address": finding.expected}},
                    params={"minorversion": "65"},
                    timeout=30,
                )
                return resp.status_code == 200
            except Exception:
                return False

        return False
