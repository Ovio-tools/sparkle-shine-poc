"""
Push Sparkle & Shine customers, invoices, and payments to QuickBooks Online Sandbox.

Full run:  python seeding/pushers/push_quickbooks.py
Dry run:   python seeding/pushers/push_quickbooks.py --dry-run

Push order (QBO requires customer before invoice, invoice before payment):
  Phase 1 — Customers  (320 clients)
  Phase 2 — Invoices   (~7,900 completed jobs)
  Phase 3 — Payments   (~7,600)

After completion, pulls the QBO Balance Sheet and prints the total AR figure.

Auth is handled by auth/quickbooks_auth.py (token file → auto-refresh → env fallback).
"""

import json
import os
import sys
from typing import Optional

import requests

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from auth import get_client                                                          # noqa: E402
from auth.quickbooks_auth import get_quickbooks_headers, get_base_url               # noqa: E402
from database.schema import get_connection                                           # noqa: E402
from database.mappings import register_mapping, get_tool_id, find_unmapped          # noqa: E402
from seeding.utils.checkpoint import (                                               # noqa: E402
    CheckpointIterator, save_checkpoint, clear_checkpoint,
)
from seeding.utils.throttler import QUICKBOOKS                                       # noqa: E402

_DB_PATH = os.path.join(_PROJECT_ROOT, "sparkle_shine.db")

# Map business.py service_type_id → QBO item ID (from config/tool_ids.json)
_SERVICE_TO_QBO_ITEM = {
    "std-residential":    "19",  # Standard Residential Clean
    "deep-clean":         "20",  # Deep Clean
    "move-in-out":        "21",  # Move-In/Move-Out Clean
    "recurring-weekly":   "22",  # Recurring Weekly
    "recurring-biweekly": "23",  # Recurring Biweekly
    "recurring-monthly":  "24",  # Recurring Monthly
    "commercial-nightly": "25",  # Commercial Nightly Clean
}
_DEFAULT_QBO_ITEM = "19"  # Standard Residential Clean (fallback)

# Mutable so _refresh_headers() can update it in-place for all callers
_headers: dict = {}


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

def _refresh_headers() -> None:
    """Reload QBO headers (called on 401 to pick up a refreshed token)."""
    global _headers
    _headers.update(get_quickbooks_headers())


# ---------------------------------------------------------------------------
# Low-level REST helpers
# ---------------------------------------------------------------------------

def _post(path: str, payload: dict) -> dict:
    """POST to QBO REST API; refresh token and retry once on 401."""
    url = f"{get_base_url()}{path}"
    QUICKBOOKS.wait()
    resp = requests.post(url, headers=_headers, json=payload, timeout=30)
    if resp.status_code == 401:
        _refresh_headers()
        QUICKBOOKS.wait()
        resp = requests.post(url, headers=_headers, json=payload, timeout=30)
    # Read body first so QBO fault details are available even on 4xx
    try:
        data = resp.json()
    except Exception:
        resp.raise_for_status()
        raise
    if "Fault" in data:
        errors = data["Fault"].get("Error", [{}])
        code = errors[0].get("code", "") if errors else ""
        msg = errors[0].get("Message", "Unknown QBO error") if errors else "Unknown QBO error"
        detail = errors[0].get("Detail", "") if errors else ""
        raise RuntimeError(f"QBO fault [{code}] on POST {path}: {msg} — {detail}")
    if not resp.ok:
        resp.raise_for_status()
    return data


def _get(path: str, params: Optional[dict] = None) -> dict:
    """GET from QBO REST API; refresh token and retry once on 401."""
    url = f"{get_base_url()}{path}"
    QUICKBOOKS.wait()
    resp = requests.get(url, headers=_headers, params=params or {}, timeout=30)
    if resp.status_code == 401:
        _refresh_headers()
        QUICKBOOKS.wait()
        resp = requests.get(url, headers=_headers, params=params or {}, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if "Fault" in data:
        errors = data["Fault"].get("Error", [{}])
        msg = errors[0].get("Message", "Unknown QBO error") if errors else "Unknown QBO error"
        raise RuntimeError(f"QBO fault on GET {path}: {msg}")
    return data


# ---------------------------------------------------------------------------
# Payload builders
# ---------------------------------------------------------------------------

def _customer_body(client: dict) -> dict:
    """Build QBO Customer create payload from a clients row."""
    if client["client_type"] == "commercial" and client["company_name"]:
        display_name = client["company_name"]
    else:
        first = client["first_name"] or ""
        last = client["last_name"] or ""
        display_name = f"{first} {last}".strip() or client["email"]

    body: dict = {"DisplayName": display_name}
    if client["email"]:
        body["PrimaryEmailAddr"] = {"Address": client["email"]}
    if client["phone"]:
        body["PrimaryPhone"] = {"FreeFormNumber": client["phone"]}
    body["Notes"] = f"SS-ID: {client['id']}"
    return body


def _invoice_body(inv: dict, qbo_customer_id: str, qbo_item_id: str) -> dict:
    """Build QBO Invoice create payload."""
    body: dict = {
        "CustomerRef": {"value": qbo_customer_id},
        "TxnDate": inv["issue_date"],
        "Line": [
            {
                "LineNum": 1,
                "DetailType": "SalesItemLineDetail",
                "Amount": float(inv["amount"]),
                "SalesItemLineDetail": {
                    "ItemRef": {"value": qbo_item_id},
                },
            }
        ],
        "PrivateNote": f"SS-INV: {inv['id']} | SS-JOB: {inv['job_id'] or ''}",
    }
    if inv["due_date"]:
        body["DueDate"] = inv["due_date"]
    return body


def _payment_body(pay: dict, qbo_customer_id: str, qbo_invoice_id: str) -> dict:
    """Build QBO Payment create payload."""
    return {
        "CustomerRef": {"value": qbo_customer_id},
        "TxnDate": pay["payment_date"],
        "TotalAmt": float(pay["amount"]),
        "Line": [
            {
                "Amount": float(pay["amount"]),
                "LinkedTxn": [
                    {
                        "TxnId": qbo_invoice_id,
                        "TxnType": "Invoice",
                    }
                ],
            }
        ],
    }


# ---------------------------------------------------------------------------
# Phase 1: Customers
# ---------------------------------------------------------------------------

def push_customers(dry_run: bool = False) -> int:
    """Push all clients to QBO as customers. Returns count pushed."""
    conn = get_connection(_DB_PATH)
    rows = conn.execute(
        "SELECT id, client_type, first_name, last_name, company_name, "
        "email, phone, notes FROM clients ORDER BY id"
    ).fetchall()
    conn.close()

    records = [dict(r) for r in rows]
    print(f"\n[Phase 1] Customers — {len(records)} clients to push")

    pushed = 0
    for client in CheckpointIterator("push_qbo_customers", records):
        # Skip if already mapped (idempotency guard for interrupted runs without checkpoint)
        if get_tool_id(client["id"], "quickbooks", db_path=_DB_PATH):
            continue

        if dry_run:
            print(f"  [dry-run] Would push customer: {client['id']}")
            pushed += 1
            continue

        try:
            data = _post("/customer", _customer_body(client))
            qbo_id = data["Customer"]["Id"]
            register_mapping(client["id"], "quickbooks", qbo_id, db_path=_DB_PATH)
            pushed += 1
            if pushed % 50 == 0:
                print(f"  ... {pushed}/{len(records)} customers pushed")
        except Exception as exc:
            print(f"  [WARN] Customer {client['id']} failed: {exc}")

    print(f"[Phase 1] Done — {pushed} customers pushed")
    return pushed


# ---------------------------------------------------------------------------
# Phase 2: Invoices
# ---------------------------------------------------------------------------

def push_invoices(dry_run: bool = False) -> int:
    """Push all invoices to QBO. Returns count pushed."""
    conn = get_connection(_DB_PATH)
    rows = conn.execute(
        """
        SELECT i.id, i.client_id, i.job_id, i.amount, i.issue_date, i.due_date,
               j.service_type_id
        FROM invoices i
        LEFT JOIN jobs j ON j.id = i.job_id
        ORDER BY i.id
        """
    ).fetchall()
    conn.close()

    records = [dict(r) for r in rows]
    print(f"\n[Phase 2] Invoices — {len(records)} invoices to push")

    pushed = 0
    skipped = 0
    since_last_checkpoint = 0

    for inv in CheckpointIterator("push_qbo_invoices", records):
        # Skip if already mapped
        if get_tool_id(inv["id"], "quickbooks", db_path=_DB_PATH):
            skipped += 1
            continue

        if dry_run:
            print(f"  [dry-run] Would push invoice: {inv['id']}")
            pushed += 1
            continue

        qbo_customer_id = get_tool_id(inv["client_id"], "quickbooks", db_path=_DB_PATH)
        if not qbo_customer_id:
            print(f"  [WARN] Invoice {inv['id']}: no QBO customer for {inv['client_id']}")
            skipped += 1
            continue

        service_type = inv.get("service_type_id") or "std-residential"
        qbo_item_id = _SERVICE_TO_QBO_ITEM.get(service_type, _DEFAULT_QBO_ITEM)

        try:
            data = _post("/invoice", _invoice_body(inv, qbo_customer_id, qbo_item_id))
            qbo_id = data["Invoice"]["Id"]
            register_mapping(inv["id"], "quickbooks", qbo_id, db_path=_DB_PATH)
            pushed += 1
            since_last_checkpoint += 1

            if since_last_checkpoint >= 100:
                save_checkpoint("push_qbo_invoices", inv["id"])
                since_last_checkpoint = 0

            if pushed % 500 == 0:
                print(f"  ... {pushed}/{len(records)} invoices pushed")
        except Exception as exc:
            print(f"  [WARN] Invoice {inv['id']} failed: {exc}")

    print(f"[Phase 2] Done — {pushed} invoices pushed, {skipped} skipped")
    return pushed


# ---------------------------------------------------------------------------
# Phase 3: Payments
# ---------------------------------------------------------------------------

def push_payments(dry_run: bool = False) -> int:
    """Push all payments to QBO. Returns count pushed."""
    conn = get_connection(_DB_PATH)
    rows = conn.execute(
        "SELECT id, invoice_id, client_id, amount, payment_date "
        "FROM payments ORDER BY id"
    ).fetchall()
    conn.close()

    records = [dict(r) for r in rows]
    print(f"\n[Phase 3] Payments — {len(records)} payments to push")

    pushed = 0
    skipped = 0
    since_last_checkpoint = 0

    for pay in CheckpointIterator("push_qbo_payments", records):
        # Skip if already mapped
        if get_tool_id(pay["id"], "quickbooks", db_path=_DB_PATH):
            skipped += 1
            continue

        if dry_run:
            print(f"  [dry-run] Would push payment: {pay['id']}")
            pushed += 1
            continue

        qbo_customer_id = get_tool_id(pay["client_id"], "quickbooks", db_path=_DB_PATH)
        if not qbo_customer_id:
            print(f"  [WARN] Payment {pay['id']}: no QBO customer for {pay['client_id']}")
            skipped += 1
            continue

        qbo_invoice_id = get_tool_id(pay["invoice_id"], "quickbooks", db_path=_DB_PATH)
        if not qbo_invoice_id:
            print(f"  [WARN] Payment {pay['id']}: no QBO invoice for {pay['invoice_id']}")
            skipped += 1
            continue

        try:
            data = _post("/payment", _payment_body(pay, qbo_customer_id, qbo_invoice_id))
            qbo_id = data["Payment"]["Id"]
            register_mapping(pay["id"], "quickbooks", qbo_id, db_path=_DB_PATH)
            pushed += 1
            since_last_checkpoint += 1

            if since_last_checkpoint >= 100:
                save_checkpoint("push_qbo_payments", pay["id"])
                since_last_checkpoint = 0

            if pushed % 500 == 0:
                print(f"  ... {pushed}/{len(records)} payments pushed")
        except Exception as exc:
            print(f"  [WARN] Payment {pay['id']} failed: {exc}")

    print(f"[Phase 3] Done — {pushed} payments pushed, {skipped} skipped")
    return pushed


# ---------------------------------------------------------------------------
# Post-run: Balance Sheet AR check
# ---------------------------------------------------------------------------

def print_balance_sheet_ar() -> None:
    """GET /reports/BalanceSheet and print the Accounts Receivable total."""
    print("\n[Balance Sheet] Fetching QBO Balance Sheet...")
    try:
        data = _get("/reports/BalanceSheet")
        ar_total = _extract_ar_total(data)
        if ar_total is not None:
            print(f"[Balance Sheet] Total Accounts Receivable: ${ar_total:,.2f}")
            print(
                "  (Should roughly match your open invoices total — "
                "any gap = invoices already paid or timing differences)"
            )
        else:
            print("[Balance Sheet] Could not locate AR line in report response.")
    except Exception as exc:
        print(f"[Balance Sheet] Failed to fetch: {exc}")


def _extract_ar_total(report: dict) -> Optional[float]:
    """Walk the BalanceSheet report JSON to find the Accounts Receivable summary value."""
    rows = report.get("Rows", {}).get("Row", [])
    # QBO BalanceSheet nests sections; AR lives under Current Assets
    for section in rows:
        header_cols = section.get("Header", {}).get("ColData", [])
        label = header_cols[0].get("value", "") if header_cols else ""
        if "Asset" in label:
            result = _scan_rows_for_ar(section.get("Rows", {}).get("Row", []))
            if result is not None:
                return result
    # Fallback: scan all rows without section filtering
    return _scan_rows_for_ar(rows)


def _scan_rows_for_ar(rows: list) -> Optional[float]:
    """Recursively scan report rows for an Accounts Receivable line."""
    for row in rows:
        col_data = row.get("ColData", [])
        if col_data and "Receivable" in col_data[0].get("value", ""):
            for col in col_data[1:]:
                val = col.get("value", "")
                if val:
                    try:
                        return float(val)
                    except ValueError:
                        pass
        # Recurse into nested sub-rows
        sub_rows = row.get("Rows", {}).get("Row", [])
        if sub_rows:
            result = _scan_rows_for_ar(sub_rows)
            if result is not None:
                return result
    return None


# ---------------------------------------------------------------------------
# Repair: push only the unmapped records (for fixing partial runs)
# ---------------------------------------------------------------------------

_QBO_DUPLICATE_NAME_CODE = "6240"


def _is_duplicate_name_error(exc: Exception) -> bool:
    """Return True if the exception is a QBO 'Duplicate Name' fault."""
    return _QBO_DUPLICATE_NAME_CODE in str(exc)


def repair_customers() -> int:
    """Push only clients that have no QBO mapping. Retries with unique name on duplicate."""
    unmapped_ids = find_unmapped("CLIENT", "quickbooks", db_path=_DB_PATH)
    if not unmapped_ids:
        print("[Repair] Customers — nothing to fix.")
        return 0

    conn = get_connection(_DB_PATH)
    placeholders = ",".join("?" * len(unmapped_ids))
    rows = conn.execute(
        f"SELECT id, client_type, first_name, last_name, company_name, email, phone, notes "
        f"FROM clients WHERE id IN ({placeholders}) ORDER BY id",
        unmapped_ids,
    ).fetchall()
    conn.close()

    print(f"\n[Repair] Customers — {len(rows)} unmapped clients to push")
    pushed = 0
    for client in [dict(r) for r in rows]:
        body = _customer_body(client)
        try:
            data = _post("/customer", body)
            qbo_id = data["Customer"]["Id"]
            register_mapping(client["id"], "quickbooks", qbo_id, db_path=_DB_PATH)
            pushed += 1
            print(f"  OK  {client['id']} → QBO {qbo_id}")
        except Exception as exc:
            if _is_duplicate_name_error(exc):
                # Append canonical ID to make the DisplayName unique
                body["DisplayName"] = f"{body['DisplayName']} ({client['id']})"
                try:
                    data = _post("/customer", body)
                    qbo_id = data["Customer"]["Id"]
                    register_mapping(client["id"], "quickbooks", qbo_id, db_path=_DB_PATH)
                    pushed += 1
                    print(f"  OK  {client['id']} → QBO {qbo_id}  [unique name suffix applied]")
                except Exception as exc2:
                    print(f"  FAIL {client['id']}: {exc2}")
            else:
                print(f"  FAIL {client['id']}: {exc}")

    print(f"[Repair] Customers done — {pushed}/{len(rows)} pushed")
    return pushed


def repair_invoices() -> int:
    """Push only invoices that have no QBO mapping."""
    unmapped_ids = find_unmapped("INV", "quickbooks", db_path=_DB_PATH)
    if not unmapped_ids:
        print("[Repair] Invoices — nothing to fix.")
        return 0

    conn = get_connection(_DB_PATH)
    placeholders = ",".join("?" * len(unmapped_ids))
    rows = conn.execute(
        f"""
        SELECT i.id, i.client_id, i.job_id, i.amount, i.issue_date, i.due_date,
               j.service_type_id
        FROM invoices i
        LEFT JOIN jobs j ON j.id = i.job_id
        WHERE i.id IN ({placeholders})
        ORDER BY i.id
        """,
        unmapped_ids,
    ).fetchall()
    conn.close()

    print(f"\n[Repair] Invoices — {len(rows)} unmapped invoices to push")
    pushed = 0
    skipped = 0
    for inv in [dict(r) for r in rows]:
        qbo_customer_id = get_tool_id(inv["client_id"], "quickbooks", db_path=_DB_PATH)
        if not qbo_customer_id:
            print(f"  SKIP {inv['id']}: still no QBO customer for {inv['client_id']}")
            skipped += 1
            continue

        service_type = inv.get("service_type_id") or "std-residential"
        qbo_item_id = _SERVICE_TO_QBO_ITEM.get(service_type, _DEFAULT_QBO_ITEM)
        try:
            data = _post("/invoice", _invoice_body(inv, qbo_customer_id, qbo_item_id))
            qbo_id = data["Invoice"]["Id"]
            register_mapping(inv["id"], "quickbooks", qbo_id, db_path=_DB_PATH)
            pushed += 1
        except Exception as exc:
            print(f"  FAIL {inv['id']}: {exc}")

    print(f"[Repair] Invoices done — {pushed}/{len(rows)} pushed, {skipped} skipped")
    return pushed


def repair_payments() -> int:
    """Push only payments that have no QBO mapping."""
    unmapped_ids = find_unmapped("PAY", "quickbooks", db_path=_DB_PATH)
    if not unmapped_ids:
        print("[Repair] Payments — nothing to fix.")
        return 0

    conn = get_connection(_DB_PATH)
    placeholders = ",".join("?" * len(unmapped_ids))
    rows = conn.execute(
        f"SELECT id, invoice_id, client_id, amount, payment_date "
        f"FROM payments WHERE id IN ({placeholders}) ORDER BY id",
        unmapped_ids,
    ).fetchall()
    conn.close()

    print(f"\n[Repair] Payments — {len(rows)} unmapped payments to push")
    pushed = 0
    skipped = 0
    for pay in [dict(r) for r in rows]:
        qbo_customer_id = get_tool_id(pay["client_id"], "quickbooks", db_path=_DB_PATH)
        if not qbo_customer_id:
            print(f"  SKIP {pay['id']}: still no QBO customer for {pay['client_id']}")
            skipped += 1
            continue

        qbo_invoice_id = get_tool_id(pay["invoice_id"], "quickbooks", db_path=_DB_PATH)
        if not qbo_invoice_id:
            print(f"  SKIP {pay['id']}: no QBO invoice for {pay['invoice_id']}")
            skipped += 1
            continue

        try:
            data = _post("/payment", _payment_body(pay, qbo_customer_id, qbo_invoice_id))
            qbo_id = data["Payment"]["Id"]
            register_mapping(pay["id"], "quickbooks", qbo_id, db_path=_DB_PATH)
            pushed += 1
        except Exception as exc:
            print(f"  FAIL {pay['id']}: {exc}")

    print(f"[Repair] Payments done — {pushed}/{len(rows)} pushed, {skipped} skipped")
    return pushed


def repair() -> None:
    """Retry all unmapped customers → invoices → payments in dependency order."""
    print("=" * 60)
    print("  Sparkle & Shine → QuickBooks Online Sandbox  [REPAIR]")
    print("=" * 60)
    repair_customers()
    repair_invoices()
    repair_payments()
    print_balance_sheet_ar()
    print("\n[Repair] Done.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(dry_run: bool = False) -> None:
    global _headers
    _headers = get_client("quickbooks")

    print("=" * 60)
    print("  Sparkle & Shine → QuickBooks Online Sandbox")
    if dry_run:
        print("  MODE: DRY RUN (no data will be written)")
    print("=" * 60)

    push_customers(dry_run=dry_run)
    push_invoices(dry_run=dry_run)
    push_payments(dry_run=dry_run)

    if not dry_run:
        print_balance_sheet_ar()
        for job_name in ("push_qbo_customers", "push_qbo_invoices", "push_qbo_payments"):
            clear_checkpoint(job_name)
        print("\n[Done] Checkpoints cleared.")

    print("\n[Done] QuickBooks push complete.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Push Sparkle & Shine data to QuickBooks Online Sandbox"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be pushed without making any API calls",
    )
    parser.add_argument(
        "--repair",
        action="store_true",
        help="Retry only unmapped customers/invoices/payments (fixes skipped/timed-out records)",
    )
    args = parser.parse_args()
    if args.repair:
        _headers.update(get_client("quickbooks"))
        repair()
    else:
        main(dry_run=args.dry_run)
