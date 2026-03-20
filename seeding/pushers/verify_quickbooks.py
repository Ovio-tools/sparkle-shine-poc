"""
Verify that QuickBooks Online Sandbox contains the correct Sparkle & Shine data
after push_quickbooks.py has run.

Compares the live QBO API against sparkle_shine.db and cross_tool_mapping,
then prints a structured report with PASS/FAIL verdicts per check.

Run:
    python seeding/pushers/verify_quickbooks.py
    python seeding/pushers/verify_quickbooks.py --verbose
    python seeding/pushers/verify_quickbooks.py --fix

Checks:
  1 — Customer count & mapping completeness
  2 — Invoice count & amount accuracy
  3 — Payment count & linkage
  4 — AR aging & late-payer pattern
  5 — Service item linkage
  6 — Cross-tool mapping completeness
"""

import json
import os
import random
import sys
from typing import Optional

import requests

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from auth.quickbooks_auth import get_quickbooks_headers, get_base_url   # noqa: E402
from database.schema import get_connection                               # noqa: E402
from database.mappings import register_mapping, get_tool_id, find_unmapped  # noqa: E402
from seeding.utils.throttler import QUICKBOOKS                           # noqa: E402

_DB_PATH = os.path.join(_PROJECT_ROOT, "sparkle_shine.db")

# Service item IDs from config/tool_ids.json (quickbooks.items)
_SERVICE_ITEM_IDS = {
    "Standard Residential Clean":  "19",
    "Deep Clean":                  "20",
    "Move-In/Move-Out Clean":      "21",
    "Recurring Weekly":            "22",
    "Recurring Biweekly":          "23",
    "Recurring Monthly":           "24",
    "Commercial Nightly Clean":    "25",
    "Late Payment Fee":            "26",
}

_VALID_ITEM_IDS = set(_SERVICE_ITEM_IDS.values())

# Mutable so _refresh_headers() can update in-place
_headers: dict = {}

PASS = "PASS"
FAIL = "FAIL"
WARN = "WARN"


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

def _refresh_headers() -> None:
    global _headers
    _headers.update(get_quickbooks_headers())


# ---------------------------------------------------------------------------
# Low-level REST helpers
# ---------------------------------------------------------------------------

def _get(path: str, params: Optional[dict] = None) -> dict:
    """GET from QBO; refresh token and retry once on 401."""
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


def _qbo_query(sql: str) -> list:
    """Run a QBO SQL query; return the entity list from QueryResponse."""
    data = _get("/query", {"query": sql})
    qr = data.get("QueryResponse", {})
    for key, val in qr.items():
        if isinstance(val, list):
            return val
    return []


def _qbo_count(entity: str, where: str = "") -> int:
    """Run SELECT COUNT(*) FROM {entity} and return the integer result."""
    sql = f"SELECT COUNT(*) FROM {entity}"
    if where:
        sql += f" WHERE {where}"
    data = _get("/query", {"query": sql})
    return data.get("QueryResponse", {}).get("totalCount", 0)


# ---------------------------------------------------------------------------
# CHECK 1 — Customer count and mapping completeness
# ---------------------------------------------------------------------------

def check1_customers(verbose: bool = False) -> str:
    print("\n" + "=" * 62)
    print("  CHECK 1 — Customer count & mapping completeness")
    print("=" * 62)

    # Step A: All clients from SQLite
    conn = get_connection(_DB_PATH)
    clients = [dict(r) for r in conn.execute(
        "SELECT id, client_type, first_name, last_name, company_name, email "
        "FROM clients ORDER BY id"
    ).fetchall()]
    conn.close()
    total = len(clients)

    # Step B: Mapped entries in cross_tool_mapping
    conn = get_connection(_DB_PATH)
    mapped_rows = conn.execute(
        "SELECT canonical_id, tool_specific_id FROM cross_tool_mapping "
        "WHERE tool_name = 'quickbooks' AND entity_type = 'CLIENT'"
    ).fetchall()
    conn.close()
    mapped = {r["canonical_id"]: r["tool_specific_id"] for r in mapped_rows}

    unmapped = [c for c in clients if c["id"] not in mapped]
    mapped_count = total - len(unmapped)

    # Step C: Spot-check 10 QBO customers via API
    commercial = [c for c in clients if c["client_type"] == "commercial" and c["id"] in mapped]
    residential = [c for c in clients if c["client_type"] == "residential" and c["id"] in mapped]
    sample = (
        random.sample(commercial, min(2, len(commercial))) +
        random.sample(residential, min(8, len(residential)))
    )[:10]

    spot_passed = 0
    spot_failures = []

    for client in sample:
        qbo_id = mapped.get(client["id"])
        if not qbo_id:
            spot_failures.append((client["id"], ["no mapping entry"]))
            continue
        try:
            data = _get(f"/customer/{qbo_id}")
            cust = data.get("Customer", {})

            if client["client_type"] == "commercial" and client["company_name"]:
                expected_name = client["company_name"]
            else:
                expected_name = (
                    f"{client['first_name'] or ''} {client['last_name'] or ''}".strip()
                    or client["email"]
                )

            actual_name = cust.get("DisplayName", "")
            # Allow "(SS-CLIENT-XXXX)" suffix added during duplicate-name repair
            name_ok = actual_name == expected_name or actual_name.startswith(expected_name)
            email_ok = (
                cust.get("PrimaryEmailAddr", {}).get("Address", "") == client["email"]
            )
            notes_ok = f"SS-ID: {client['id']}" in (cust.get("Notes", "") or "")

            if name_ok and email_ok and notes_ok:
                spot_passed += 1
            else:
                problems = []
                if not name_ok:
                    problems.append(f"name: expected '{expected_name}', got '{actual_name}'")
                if not email_ok:
                    problems.append("email mismatch")
                if not notes_ok:
                    problems.append("Notes missing SS-ID")
                spot_failures.append((client["id"], problems))
        except Exception as exc:
            spot_failures.append((client["id"], [str(exc)]))

    spot_failed = len(spot_failures)

    print(f"  Clients in SQLite:          {total}")
    print(f"  Mapped to QBO:              {mapped_count} / {total}")
    print(f"  Unmapped (missing):         {len(unmapped)}")
    print(f"  Spot-check passed:          {spot_passed} / {len(sample)}")
    print(f"  Spot-check failed:          {spot_failed} / {len(sample)}")

    if verbose:
        if unmapped:
            print("\n  Unmapped client IDs:")
            for c in unmapped:
                print(f"    {c['id']}  ({c['email']})")
        if spot_failures:
            print("\n  Spot-check failures:")
            for cid, reasons in spot_failures:
                print(f"    {cid}: {', '.join(reasons)}")

    if len(unmapped) == 0 and spot_passed >= 9:
        verdict = PASS
    elif len(unmapped) <= 5 and spot_passed >= 7:
        verdict = WARN
    else:
        verdict = FAIL

    print(f"\n  VERDICT: {verdict}")
    return verdict


# ---------------------------------------------------------------------------
# CHECK 2 — Invoice count and amount accuracy
# ---------------------------------------------------------------------------

def check2_invoices(verbose: bool = False) -> str:
    print("\n" + "=" * 62)
    print("  CHECK 2 — Invoice count & amount accuracy")
    print("=" * 62)

    # Step A: SQLite invoice totals
    conn = get_connection(_DB_PATH)
    row = conn.execute("""
        SELECT
            COUNT(*) as total_invoices,
            SUM(amount) as total_value,
            COUNT(CASE WHEN status = 'paid' THEN 1 END) as paid_count,
            COUNT(CASE WHEN status IN ('sent','overdue') THEN 1 END) as open_count,
            SUM(CASE WHEN status IN ('sent','overdue') THEN amount ELSE 0 END) as open_value
        FROM invoices
    """).fetchone()
    conn.close()
    sqlite_total = row["total_invoices"]
    sqlite_open_count = row["open_count"]
    sqlite_open_value = row["open_value"] or 0.0

    # Step B: QBO invoice count via pagination (1000-per-page)
    pages_needed = max(1, sqlite_total // 1000 + 1)
    print(f"  Paginating QBO invoices (~{pages_needed} API calls)...")
    qbo_total = 0
    start = 1
    while True:
        sql = f"SELECT * FROM Invoice STARTPOSITION {start} MAXRESULTS 1000"
        data = _get("/query", {"query": sql})
        page = data.get("QueryResponse", {}).get("Invoice", [])
        if not page:
            break
        qbo_total += len(page)
        if len(page) < 1000:
            break
        start += 1000

    count_diff = abs(sqlite_total - qbo_total)

    # Step C: Amount accuracy spot-check (25 invoices)
    conn = get_connection(_DB_PATH)

    def _sample(where_clause: str, n: int) -> list:
        return [dict(r) for r in conn.execute(f"""
            SELECT i.id, i.amount, i.issue_date, i.due_date, i.client_id
            FROM invoices i
            LEFT JOIN jobs j ON j.id = i.job_id
            LEFT JOIN clients c ON c.id = i.client_id
            WHERE {where_clause}
            ORDER BY RANDOM() LIMIT {n}
        """).fetchall()]

    spot_invoices = (
        _sample("j.service_type_id = 'recurring-biweekly'", 5) +
        _sample("j.service_type_id = 'deep-clean'", 5) +
        _sample("j.service_type_id = 'move-in-out'", 5) +
        _sample("c.client_type = 'commercial' AND ABS(i.amount - 1038.46) < 1.0", 5) +
        _sample("c.client_type = 'commercial' AND ABS(i.amount - 461.54) < 1.0", 5)
    )
    conn.close()

    spot_passed = 0
    amount_mismatches = []

    for inv in spot_invoices:
        qbo_inv_id = get_tool_id(inv["id"], "quickbooks", db_path=_DB_PATH)
        if not qbo_inv_id:
            amount_mismatches.append((inv["id"], ["no QBO mapping"]))
            continue
        try:
            data = _get(f"/invoice/{qbo_inv_id}")
            qbo_inv = data.get("Invoice", {})
            amount_ok = abs(float(qbo_inv.get("TotalAmt", 0)) - float(inv["amount"])) <= 0.01
            expected_cust = get_tool_id(inv["client_id"], "quickbooks", db_path=_DB_PATH)
            customer_ok = qbo_inv.get("CustomerRef", {}).get("value", "") == expected_cust
            date_ok = qbo_inv.get("TxnDate", "") == inv["issue_date"]
            due_ok = (inv["due_date"] is None) or (qbo_inv.get("DueDate", "") == inv["due_date"])
            note_ok = inv["id"] in (qbo_inv.get("PrivateNote", "") or "")

            if amount_ok and customer_ok and date_ok and note_ok:
                spot_passed += 1
            else:
                problems = []
                if not amount_ok:
                    problems.append(
                        f"amount expected ${float(inv['amount']):.2f}, "
                        f"got ${float(qbo_inv.get('TotalAmt', 0)):.2f}"
                    )
                if not customer_ok:
                    problems.append("customer mismatch")
                if not date_ok:
                    problems.append("TxnDate mismatch")
                if not note_ok:
                    problems.append("PrivateNote missing canonical ID")
                amount_mismatches.append((inv["id"], problems))
        except Exception as exc:
            amount_mismatches.append((inv["id"], [str(exc)]))

    print(f"  Invoices in SQLite:         {sqlite_total}")
    print(f"  Invoices in QBO:            {qbo_total}")
    flag = "  ⚠️" if count_diff > 50 else ""
    print(f"  Count difference:           {count_diff}{flag}")
    print(f"  Open invoices (SQLite):     {sqlite_open_count}  (${sqlite_open_value:,.2f})")
    print(f"  Spot-check passed:          {spot_passed} / {len(spot_invoices)}")
    print(f"  Amount mismatches:          {len(amount_mismatches)}")

    if verbose and amount_mismatches:
        print("\n  Mismatch details:")
        for inv_id, problems in amount_mismatches:
            print(f"    {inv_id}: {', '.join(problems)}")

    if count_diff <= 50 and spot_passed >= 23:
        verdict = PASS
    elif count_diff <= 200 and spot_passed >= 20:
        verdict = WARN
    else:
        verdict = FAIL

    print(f"\n  VERDICT: {verdict}")
    return verdict


# ---------------------------------------------------------------------------
# CHECK 3 — Payment count and linkage
# ---------------------------------------------------------------------------

def check3_payments(verbose: bool = False) -> str:
    print("\n" + "=" * 62)
    print("  CHECK 3 — Payment count & linkage")
    print("=" * 62)

    # Step A: SQLite payment totals
    conn = get_connection(_DB_PATH)
    row = conn.execute("""
        SELECT
            COUNT(*) as total_payments,
            SUM(amount) as total_paid,
            MIN(payment_date) as earliest,
            MAX(payment_date) as latest
        FROM payments
    """).fetchone()
    sqlite_total = row["total_payments"]

    # Step B: QBO payment count
    qbo_total = _qbo_count("Payment")
    count_diff = abs(sqlite_total - qbo_total)

    # Step C: Spot-check 10 payments
    sample_rows = conn.execute("""
        SELECT p.id, p.invoice_id, p.client_id, p.amount, p.payment_date
        FROM payments p
        ORDER BY RANDOM() LIMIT 10
    """).fetchall()
    conn.close()
    sample = [dict(r) for r in sample_rows]

    spot_passed = 0
    spot_failures = []

    for pay in sample:
        qbo_pay_id = get_tool_id(pay["id"], "quickbooks", db_path=_DB_PATH)
        if not qbo_pay_id:
            spot_failures.append((pay["id"], ["no QBO mapping"]))
            continue
        try:
            data = _get(f"/payment/{qbo_pay_id}")
            qbo_pay = data.get("Payment", {})
            amount_ok = abs(float(qbo_pay.get("TotalAmt", 0)) - float(pay["amount"])) <= 0.01
            date_ok = qbo_pay.get("TxnDate", "") == pay["payment_date"]

            lines = qbo_pay.get("Line", [])
            linked_inv_id = None
            linked_type = None
            if lines:
                ltxns = lines[0].get("LinkedTxn", [])
                if ltxns:
                    linked_inv_id = ltxns[0].get("TxnId")
                    linked_type = ltxns[0].get("TxnType")

            expected_qbo_inv = get_tool_id(pay["invoice_id"], "quickbooks", db_path=_DB_PATH)
            link_ok = linked_inv_id == expected_qbo_inv and linked_type == "Invoice"

            if amount_ok and date_ok and link_ok:
                spot_passed += 1
            else:
                problems = []
                if not amount_ok:
                    problems.append("amount mismatch")
                if not date_ok:
                    problems.append("date mismatch")
                if not link_ok:
                    problems.append(
                        f"invoice link: expected {expected_qbo_inv}, "
                        f"got {linked_inv_id} ({linked_type})"
                    )
                spot_failures.append((pay["id"], problems))
        except Exception as exc:
            spot_failures.append((pay["id"], [str(exc)]))

    # Step D: Sample check for unlinked payments (first 100)
    unlinked_ids = []
    try:
        pays_sample = _qbo_query("SELECT * FROM Payment MAXRESULTS 100")
        for p in pays_sample:
            has_invoice_link = any(
                ltxn.get("TxnType") == "Invoice"
                for line in p.get("Line", [])
                for ltxn in line.get("LinkedTxn", [])
            )
            if not has_invoice_link:
                unlinked_ids.append(p.get("Id", "?"))
    except Exception:
        pass  # non-critical

    print(f"  Payments in SQLite:         {sqlite_total}")
    print(f"  Payments in QBO:            {qbo_total}")
    print(f"  Count difference:           {count_diff}")
    print(f"  Spot-check passed:          {spot_passed} / {len(sample)}")
    print(f"  Unlinked payments (sample): {len(unlinked_ids)}")

    if verbose:
        if spot_failures:
            print("\n  Spot-check failures:")
            for pid, reasons in spot_failures:
                print(f"    {pid}: {', '.join(reasons)}")
        if unlinked_ids:
            print(f"\n  Unlinked payment QBO IDs (from first 100): {unlinked_ids}")

    if count_diff <= 100 and spot_passed >= 9 and len(unlinked_ids) == 0:
        verdict = PASS
    elif count_diff <= 200 and spot_passed >= 7:
        verdict = WARN
    else:
        verdict = FAIL

    print(f"\n  VERDICT: {verdict}")
    return verdict, unlinked_ids  # return unlinked_ids for --fix


# ---------------------------------------------------------------------------
# CHECK 4 — AR aging and late-payer pattern
# ---------------------------------------------------------------------------

def check4_ar_aging(verbose: bool = False) -> str:
    print("\n" + "=" * 62)
    print("  CHECK 4 — AR aging & late-payer pattern")
    print("=" * 62)

    # Step A: Pull QBO AR Aging Detail
    try:
        aging_data = _get("/reports/AgedReceivableDetail", {
            "report_date": "2026-03-17",
            "aging_period": "30",
            "num_periods": "4",
        })
    except Exception as exc:
        print(f"  ERROR fetching AR Aging Detail: {exc}")
        print(f"\n  VERDICT: {FAIL}")
        return FAIL

    # Step B: Late-paying commercial clients from SQLite
    conn = get_connection(_DB_PATH)
    late_rows = conn.execute("""
        SELECT
            c.company_name,
            c.id as canonical_id,
            i.id as invoice_id,
            i.amount,
            i.days_outstanding,
            i.due_date,
            i.status
        FROM invoices i
        JOIN jobs j ON j.id = i.job_id
        JOIN clients c ON c.id = j.client_id
        WHERE c.client_type = 'commercial'
          AND i.days_outstanding > 45
          AND i.status = 'overdue'
        ORDER BY i.days_outstanding DESC
        LIMIT 20
    """).fetchall()

    # Open AR total
    open_ar = conn.execute(
        "SELECT SUM(amount) as total FROM invoices WHERE status IN ('sent','overdue')"
    ).fetchone()["total"] or 0.0
    conn.close()

    # Distinct late-paying clients (by canonical_id)
    seen: dict = {}
    for row in late_rows:
        cid = row["canonical_id"]
        if cid not in seen:
            seen[cid] = dict(row)

    print(f"\n  Late-payer clients found in SQLite:")
    for cid, r in seen.items():
        print(
            f"    {cid}  {r['company_name']:<40} "
            f"days_outstanding={r['days_outstanding']}  status={r['status']}"
        )

    # Parse aging report column headers
    cols = aging_data.get("Columns", {}).get("Column", [])
    col_titles = [c.get("ColTitle", "") for c in cols]

    def _flat_aging_rows(report_data: dict) -> list:
        """Flatten all ColData rows from the report into a list of col lists."""
        results = []
        for section in report_data.get("Rows", {}).get("Row", []):
            for row in section.get("Rows", {}).get("Row", []):
                col_data = row.get("ColData", [])
                if col_data:
                    results.append(col_data)
        return results

    aging_rows = _flat_aging_rows(aging_data)

    # Step C: Match SQLite late-payers to QBO AR Aging rows
    visible_count = 0
    correct_bucket_count = 0

    for cid, client in seen.items():
        company = client["company_name"]
        matched = None
        for ar_row in aging_rows:
            row_name = ar_row[0].get("value", "") if ar_row else ""
            if company.lower() in row_name.lower():
                matched = ar_row
                break

        if matched:
            visible_count += 1
            # Check 31-60 and 61-90 day buckets for non-zero value
            in_late_bucket = False
            for i, title in enumerate(col_titles):
                if any(t in title for t in ("31", "61")) and i < len(matched):
                    try:
                        val = float(matched[i].get("value", "0") or "0")
                        if val > 0:
                            in_late_bucket = True
                    except ValueError:
                        pass
            if in_late_bucket:
                correct_bucket_count += 1
                print(f"  ✓ {company}: visible in 31-90 day aging bucket")
            else:
                print(f"  ⚠ {company}: visible but not in 31-90 day bucket")
        else:
            print(f"  ✗ {company}: NOT found in QBO AR Aging report")

    # Step D: Compare total open AR
    qbo_ar_total: Optional[float] = None
    try:
        summary = _get("/reports/AgedReceivableSummary")
        for section in summary.get("Rows", {}).get("Row", []):
            for row in section.get("Rows", {}).get("Row", []) + [section]:
                col_data = row.get("ColData", [])
                if col_data and "total" in (col_data[0].get("value", "").lower()):
                    for col in col_data[1:]:
                        val = col.get("value", "")
                        if val:
                            try:
                                qbo_ar_total = float(val)
                                break
                            except ValueError:
                                pass
                if qbo_ar_total is not None:
                    break
            if qbo_ar_total is not None:
                break
    except Exception as exc:
        print(f"  [WARN] Could not fetch AR Aging Summary: {exc}")

    ar_variance = abs(open_ar - (qbo_ar_total or 0.0))
    ar_variance_pct = (ar_variance / open_ar * 100) if open_ar else 0.0

    print(f"\n  Late-payer clients found in SQLite:    {len(seen)} (expected: 2)")
    print(f"  Late-payers visible in QBO aging:      {visible_count} / {len(seen)}")
    print(f"  Correct aging bucket (31-90 days):     {correct_bucket_count} / {visible_count}")
    print(f"  Total open AR — SQLite:                ${open_ar:,.2f}")
    if qbo_ar_total is not None:
        print(f"  Total open AR — QBO:                   ${qbo_ar_total:,.2f}")
        print(f"  Variance:                              ${ar_variance:,.2f} ({ar_variance_pct:.1f}%)")
    else:
        print(f"  Total open AR — QBO:                   (unavailable)")

    high_variance = qbo_ar_total is not None and ar_variance_pct > 10.0
    low_variance = qbo_ar_total is None or ar_variance_pct < 5.0
    all_visible = visible_count == len(seen)
    all_correct_bucket = correct_bucket_count == visible_count

    if all_visible and all_correct_bucket and low_variance:
        verdict = PASS
    elif high_variance or (visible_count == 0 and len(seen) > 0):
        verdict = FAIL
    else:
        verdict = WARN

    print(f"\n  VERDICT: {verdict}")
    return verdict


# ---------------------------------------------------------------------------
# CHECK 5 — Service item linkage
# ---------------------------------------------------------------------------

def check5_service_items(verbose: bool = False) -> str:
    print("\n" + "=" * 62)
    print("  CHECK 5 — Service item linkage")
    print("=" * 62)

    # Step A/B: Verify each item ID exists in QBO
    items_verified = 0
    items_failed = []

    for name, item_id in _SERVICE_ITEM_IDS.items():
        try:
            data = _get(f"/item/{item_id}")
            qbo_name = data.get("Item", {}).get("Name", "")
            # Accept partial name match (QBO may abbreviate)
            if name.lower() in qbo_name.lower() or qbo_name.lower() in name.lower():
                items_verified += 1
            else:
                items_failed.append((item_id, name, f"name mismatch: got '{qbo_name}'"))
        except Exception as exc:
            items_failed.append((item_id, name, str(exc)))

    # Step C: Spot-check 10 invoice line items
    conn = get_connection(_DB_PATH)
    sample_invs = [dict(r) for r in conn.execute(
        "SELECT id FROM invoices ORDER BY RANDOM() LIMIT 10"
    ).fetchall()]
    conn.close()

    line_passed = 0
    line_failures = []

    for inv in sample_invs:
        qbo_inv_id = get_tool_id(inv["id"], "quickbooks", db_path=_DB_PATH)
        if not qbo_inv_id:
            line_failures.append((inv["id"], "no QBO mapping"))
            continue
        try:
            data = _get(f"/invoice/{qbo_inv_id}")
            lines = data.get("Invoice", {}).get("Line", [])
            if lines:
                item_ref = lines[0].get("SalesItemLineDetail", {}).get("ItemRef", {})
                item_id = item_ref.get("value", "")
                if item_id in _VALID_ITEM_IDS:
                    line_passed += 1
                else:
                    line_failures.append((inv["id"], f"unknown item_id={item_id!r}"))
            else:
                line_failures.append((inv["id"], "no Line items"))
        except Exception as exc:
            line_failures.append((inv["id"], str(exc)))

    print(f"  Service items in tool_ids.json:    {len(_SERVICE_ITEM_IDS)} (expected: 8)")
    print(f"  Items verified in QBO:             {items_verified} / {len(_SERVICE_ITEM_IDS)}")
    print(f"  Invoice line item check passed:    {line_passed} / {len(sample_invs)}")

    if verbose:
        if items_failed:
            print("\n  Item verification failures:")
            for item_id, name, reason in items_failed:
                print(f"    ID {item_id} ({name}): {reason}")
        if line_failures:
            print("\n  Line item failures:")
            for inv_id, reason in line_failures:
                print(f"    {inv_id}: {reason}")

    if items_verified == len(_SERVICE_ITEM_IDS) and line_passed >= 9:
        verdict = PASS
    elif items_verified >= len(_SERVICE_ITEM_IDS) - 1 and line_passed >= 7:
        verdict = WARN
    else:
        verdict = FAIL

    print(f"\n  VERDICT: {verdict}")
    return verdict


# ---------------------------------------------------------------------------
# CHECK 6 — Cross-tool mapping completeness
# ---------------------------------------------------------------------------

def check6_mapping_completeness(verbose: bool = False) -> str:
    print("\n" + "=" * 62)
    print("  CHECK 6 — Cross-tool mapping completeness")
    print("=" * 62)

    conn = get_connection(_DB_PATH)

    # Summary by entity type
    summary_rows = conn.execute("""
        SELECT
            entity_type,
            COUNT(*) as mapped_count,
            COUNT(DISTINCT tool_specific_id) as unique_qbo_ids,
            MIN(synced_at) as first_synced,
            MAX(synced_at) as last_synced
        FROM cross_tool_mapping
        WHERE tool_name = 'quickbooks'
        GROUP BY entity_type
    """).fetchall()
    summary = {r["entity_type"]: dict(r) for r in summary_rows}

    # Expected counts from SQLite
    sqlite_clients = conn.execute("SELECT COUNT(*) as n FROM clients").fetchone()["n"]
    sqlite_invoices = conn.execute("SELECT COUNT(*) as n FROM invoices").fetchone()["n"]
    sqlite_payments = conn.execute("SELECT COUNT(*) as n FROM payments").fetchone()["n"]

    # Duplicate check
    dup_rows = conn.execute("""
        SELECT canonical_id, COUNT(*) as n
        FROM cross_tool_mapping
        WHERE tool_name = 'quickbooks'
        GROUP BY canonical_id
        HAVING n > 1
    """).fetchall()
    duplicates = [dict(r) for r in dup_rows]
    conn.close()

    client_mapped = summary.get("CLIENT", {}).get("mapped_count", 0)
    inv_mapped = summary.get("INV", {}).get("mapped_count", 0)
    pay_mapped = summary.get("PAY", {}).get("mapped_count", 0)

    first_sync = (
        summary.get("CLIENT", {}).get("first_synced")
        or summary.get("INV", {}).get("first_synced")
        or "N/A"
    )
    last_sync = (
        summary.get("PAY", {}).get("last_synced")
        or summary.get("INV", {}).get("last_synced")
        or "N/A"
    )

    print(f"\n  Mapped entities by type:")
    print(f"    CLIENT:  {client_mapped} / {sqlite_clients}")
    print(f"    INV:     {inv_mapped} / {sqlite_invoices}")
    print(f"    PAY:     {pay_mapped} / {sqlite_payments}")
    print(f"  Duplicate mappings:  {len(duplicates)}")
    print(f"  First sync:          {first_sync}")
    print(f"  Last sync:           {last_sync}")

    if verbose and duplicates:
        print("\n  Duplicate canonical IDs:")
        for dup in duplicates:
            print(f"    {dup['canonical_id']} (n={dup['n']})")
        print("  Note: duplicates suggest the push ran twice without clearing checkpoints.")

    def _within_2pct(actual: int, expected: int) -> bool:
        if expected == 0:
            return actual == 0
        return abs(actual - expected) / expected <= 0.02

    all_within = (
        _within_2pct(client_mapped, sqlite_clients)
        and _within_2pct(inv_mapped, sqlite_invoices)
        and _within_2pct(pay_mapped, sqlite_payments)
    )
    no_dups = len(duplicates) == 0

    if all_within and no_dups:
        verdict = PASS
    elif not no_dups:
        verdict = WARN
    else:
        verdict = FAIL

    print(f"\n  VERDICT: {verdict}")
    return verdict


# ---------------------------------------------------------------------------
# --fix: auto-repair fixable gaps
# ---------------------------------------------------------------------------

def fix_unmapped_clients(verbose: bool = False) -> None:
    """Find clients in QBO by email and register the mapping for any that are unmapped."""
    print("\n[--fix] Fixing unmapped clients via QBO email lookup...")
    unmapped_ids = find_unmapped("CLIENT", "quickbooks", db_path=_DB_PATH)
    if not unmapped_ids:
        print("  No unmapped clients. Nothing to fix.")
        return

    conn = get_connection(_DB_PATH)
    placeholders = ",".join("?" * len(unmapped_ids))
    clients = [dict(r) for r in conn.execute(
        f"SELECT id, email FROM clients WHERE id IN ({placeholders})",
        unmapped_ids,
    ).fetchall()]
    conn.close()

    fixed = 0
    not_found = []

    for client in clients:
        try:
            email_escaped = client["email"].replace("'", "\\'")
            results = _qbo_query(
                f"SELECT * FROM Customer WHERE PrimaryEmailAddr = '{email_escaped}'"
            )
            if results:
                qbo_id = results[0].get("Id")
                register_mapping(client["id"], "quickbooks", qbo_id, db_path=_DB_PATH)
                fixed += 1
                if verbose:
                    print(f"  Fixed {client['id']} → QBO {qbo_id}  ({client['email']})")
            else:
                not_found.append(client["id"])
        except Exception as exc:
            print(f"  Error fixing {client['id']}: {exc}")
            not_found.append(client["id"])

    print(f"  Fixed {fixed} unmapped clients via email lookup.")
    if not_found:
        print(f"  {len(not_found)} client(s) not found in QBO: {not_found}")
        print("  These require re-pushing: python seeding/pushers/push_quickbooks.py --repair")


def fix_unlinked_payments(unlinked_qbo_ids: list, verbose: bool = False) -> None:
    """Report unlinked QBO payments; full re-link is complex and requires manual action."""
    if not unlinked_qbo_ids:
        return
    print(f"\n[--fix] {len(unlinked_qbo_ids)} unlinked payment(s) found.")
    print("  Updating QBO Payment entities to add LinkedTxn requires fetching")
    print("  the full payment object, patching it, and re-POST — not auto-fixable.")
    print("  Payment QBO IDs requiring manual review:")
    for pid in unlinked_qbo_ids:
        print(f"    {pid}")
    print("  To fix: check QBO sandbox and re-link manually, or re-run:")
    print("    python seeding/pushers/push_quickbooks.py --repair")


# ---------------------------------------------------------------------------
# Final summary
# ---------------------------------------------------------------------------

def print_summary(verdicts: dict) -> None:
    _W = 58  # inner box width

    labels = {
        "check1": "Customer count & mapping",
        "check2": "Invoice count & amounts",
        "check3": "Payment count & linkage",
        "check4": "AR aging & late payers",
        "check5": "Service item linkage",
        "check6": "Mapping completeness",
    }

    passed = sum(1 for v in verdicts.values() if v == PASS)
    total = len(labels)

    print("\n")
    print("╔" + "═" * _W + "╗")
    title = "QuickBooks Push Verification — Summary"
    print("║" + title.center(_W) + "║")
    print("╠" + "═" * _W + "╣")
    for i, (key, label) in enumerate(labels.items(), start=1):
        verdict = verdicts.get(key, "SKIP")
        line = f"  Check {i} — {label:<36} [ {verdict:<4} ]  "
        print("║" + line.ljust(_W) + "║")
    print("╠" + "═" * _W + "╣")
    overall_line = f"  Overall: {passed} / {total} checks passed"
    print("║" + overall_line.ljust(_W) + "║")
    print("╚" + "═" * _W + "╝")

    if passed == total:
        print("\n✅ QuickBooks push verified. Safe to proceed to Phase 3.")
    elif passed >= 4:
        print("\n⚠️  QuickBooks push mostly complete. Review warnings above.")
        print("    Proceed with caution — minor gaps may affect automations.")
    else:
        print("\n❌ QuickBooks push has significant gaps. Run push_quickbooks.py")
        print("   with --repair to fill missing records before proceeding.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(verbose: bool = False, fix: bool = False) -> None:
    global _headers
    _headers = get_quickbooks_headers()

    verdicts: dict = {}
    unlinked_ids: list = []

    verdicts["check1"] = check1_customers(verbose=verbose)
    verdicts["check2"] = check2_invoices(verbose=verbose)

    # check3 returns (verdict, unlinked_ids)
    check3_result = check3_payments(verbose=verbose)
    if isinstance(check3_result, tuple):
        verdicts["check3"], unlinked_ids = check3_result
    else:
        verdicts["check3"] = check3_result

    verdicts["check4"] = check4_ar_aging(verbose=verbose)
    verdicts["check5"] = check5_service_items(verbose=verbose)
    verdicts["check6"] = check6_mapping_completeness(verbose=verbose)

    print_summary(verdicts)

    if fix:
        print("\n" + "=" * 62)
        print("  --fix: Auto-repair mode")
        print("=" * 62)

        # Fixable gap 1: unmapped clients
        if verdicts.get("check1") in (FAIL, WARN):
            fix_unmapped_clients(verbose=verbose)

        # Fixable gap 2: unlinked payments
        if unlinked_ids:
            fix_unlinked_payments(unlinked_ids, verbose=verbose)

        # Non-fixable gaps — print clear instructions
        non_fixable = []
        if verdicts.get("check2") == FAIL:
            non_fixable.append(
                "Invoice count mismatch > 200 — run push_quickbooks.py to fill gaps"
            )
        if verdicts.get("check4") == FAIL:
            non_fixable.append(
                "AR aging / late-payer issue — verify QBO sandbox data manually"
            )
        if verdicts.get("check5") == FAIL:
            non_fixable.append(
                "Service item mismatch — check item IDs in config/tool_ids.json "
                "against QBO sandbox Items"
            )

        if non_fixable:
            print("\n[--fix] The following gaps cannot be auto-repaired:")
            for msg in non_fixable:
                print(f"  • {msg}")
            print("\n  To re-push all records:   python seeding/pushers/push_quickbooks.py")
            print("  To fix skipped records:   python seeding/pushers/push_quickbooks.py --repair")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Verify Sparkle & Shine data in QuickBooks Online Sandbox"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print per-record mismatch details for each failed check",
    )
    parser.add_argument(
        "--fix",
        action="store_true",
        help="Attempt to auto-repair fixable gaps after running all checks",
    )
    args = parser.parse_args()
    main(verbose=args.verbose, fix=args.fix)
