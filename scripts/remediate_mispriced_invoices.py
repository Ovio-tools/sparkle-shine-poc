#!/usr/bin/env python3
"""Repair invoices that were priced at the $150 fallback instead of the
canonical catalog price for their ``jobs.service_type_id``.

Track C (revenue remediation plan 2026-04) action 6. Keeps pricing changes
auditable by running from a committed script rather than ad-hoc SQL
(project rule L5).

Identification
--------------
For each invoice linked to a job (``invoices.job_id IS NOT NULL``) with a
known canonical ``service_type_id`` in the catalog, compare
``invoices.amount`` against the expected amount:

  * residential types → ``config.service_catalog.SERVICE_CATALOGUE[sid].base_price``
  * ``commercial-nightly`` → ``get_commercial_per_visit_rate(client_id,
    job_date, service_type_id)``

An invoice is "mispriced" when the actual amount differs from the expected
amount by more than 1 cent.

Policy: already-paid invoices
-----------------------------
Paid invoices are SKIPPED by default. Repricing a paid residential invoice
requires issuing a credit memo or supplemental charge in QuickBooks and
creates a customer-support load that typically dwarfs the recovered
revenue. Explicit policy per service type (Track C action 7):

  * std-residential, deep-clean, move-in-out, recurring-weekly,
    recurring-biweekly, recurring-monthly → forward-only. Accept the
    historical underbilling. Do not re-invoice paid records.
  * commercial-nightly → forward-only by default. Finance may opt into
    supplemental invoicing per contract; handle that as a separate
    finance-owned process, not via this script.

The ``--reprice-paid`` flag overrides the default for a specific run, e.g.
when finance has explicitly approved repricing a short list of invoices.
Use ``--service-types`` and ``--since``/``--until`` to narrow the scope
before using ``--reprice-paid``.

Usage
-----
    python scripts/remediate_mispriced_invoices.py --since 2026-04-01
    python scripts/remediate_mispriced_invoices.py --since 2026-04-01 --execute
    python scripts/remediate_mispriced_invoices.py --service-types deep-clean recurring-weekly
    python scripts/remediate_mispriced_invoices.py --since 2026-04-01 --reprice-paid --execute

The script is dry-run by default. ``--execute`` is required for any local
or QuickBooks mutation.
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Optional

import requests

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from auth.quickbooks_auth import get_base_url, get_quickbooks_headers
from config.service_catalog import SERVICE_CATALOGUE, CANONICAL_SERVICE_IDS
from database.schema import get_connection
from seeding.generators.gen_clients import get_commercial_per_visit_rate
from seeding.utils.throttler import QUICKBOOKS

logger = logging.getLogger("remediate_mispriced_invoices")


# Paid-invoice policy, encoded per service type. Forward-only means the
# script will skip the row when status='paid' or paid_date is set.
_PAID_POLICY: dict[str, str] = {
    "std-residential":    "forward_only",
    "deep-clean":         "forward_only",
    "move-in-out":        "forward_only",
    "recurring-weekly":   "forward_only",
    "recurring-biweekly": "forward_only",
    "recurring-monthly":  "forward_only",
    "commercial-nightly": "forward_only",
}


@dataclass
class MispricedInvoice:
    invoice_id:      str
    client_id:       str
    job_id:          str
    service_type_id: str
    issue_date:      str
    status:          str
    paid_date:       Optional[str]
    current_amount:  float
    expected_amount: float
    qbo_invoice_id:  Optional[str]

    @property
    def delta(self) -> float:
        return round(self.expected_amount - self.current_amount, 2)

    @property
    def is_paid(self) -> bool:
        return self.status == "paid" or bool(self.paid_date)


# ---------------------------------------------------------------------------
# Expected-price resolution
# ---------------------------------------------------------------------------

def _expected_amount(service_type_id: str, client_id: str, issue_date: str) -> Optional[float]:
    catalog = SERVICE_CATALOGUE.get(service_type_id)
    if catalog is None:
        return None

    if service_type_id == "commercial-nightly":
        try:
            return round(
                get_commercial_per_visit_rate(
                    client_id=client_id,
                    job_date=issue_date,
                    service_type_id=service_type_id,
                ),
                2,
            )
        except Exception as exc:
            logger.warning(
                "commercial-nightly rate unavailable for %s (%s): %s",
                client_id, issue_date, exc,
            )
            return None

    base_price = catalog.get("base_price")
    return round(base_price, 2) if base_price is not None else None


# ---------------------------------------------------------------------------
# Candidate query
# ---------------------------------------------------------------------------

def _fetch_candidates(
    db,
    since: Optional[str],
    until: Optional[str],
    service_types: Optional[list[str]],
    limit: Optional[int],
) -> list[MispricedInvoice]:
    conditions = [
        "i.job_id IS NOT NULL",
        "j.service_type_id IS NOT NULL",
        "j.service_type_id = ANY(%s)",
    ]
    params: list = [list(service_types or sorted(CANONICAL_SERVICE_IDS))]

    if since:
        conditions.append("i.issue_date >= %s")
        params.append(since)
    if until:
        conditions.append("i.issue_date <= %s")
        params.append(until)

    sql = f"""
        SELECT
            i.id            AS invoice_id,
            i.client_id     AS client_id,
            i.job_id        AS job_id,
            j.service_type_id AS service_type_id,
            i.issue_date    AS issue_date,
            i.status        AS status,
            i.paid_date     AS paid_date,
            i.amount        AS current_amount,
            (
                SELECT m.tool_specific_id
                FROM cross_tool_mapping m
                WHERE m.canonical_id = i.id AND m.tool_name = 'quickbooks'
            ) AS qbo_invoice_id
        FROM invoices i
        JOIN jobs j ON j.id = i.job_id
        WHERE {' AND '.join(conditions)}
        ORDER BY i.issue_date, i.id
    """
    if limit is not None:
        sql += " LIMIT %s"
        params.append(limit)

    rows = db.execute(sql, tuple(params)).fetchall()

    candidates: list[MispricedInvoice] = []
    for row in rows:
        record = dict(row)
        expected = _expected_amount(
            service_type_id=record["service_type_id"],
            client_id=record["client_id"],
            issue_date=str(record["issue_date"]),
        )
        if expected is None:
            continue

        current = round(float(record["current_amount"]), 2)
        if abs(current - expected) < 0.01:
            continue  # already priced correctly

        candidates.append(
            MispricedInvoice(
                invoice_id=record["invoice_id"],
                client_id=record["client_id"],
                job_id=record["job_id"],
                service_type_id=record["service_type_id"],
                issue_date=str(record["issue_date"]),
                status=record["status"],
                paid_date=record["paid_date"],
                current_amount=current,
                expected_amount=expected,
                qbo_invoice_id=record["qbo_invoice_id"],
            )
        )
    return candidates


# ---------------------------------------------------------------------------
# QBO update
# ---------------------------------------------------------------------------

def _qbo_request(method: str, path: str, **kwargs) -> dict:
    url = f"{get_base_url()}{path}"
    headers = get_quickbooks_headers()
    for attempt in (1, 2):
        QUICKBOOKS.wait()
        response = requests.request(method, url, headers=headers, timeout=30, **kwargs)
        if response.status_code == 401 and attempt == 1:
            headers = get_quickbooks_headers()
            continue
        response.raise_for_status()
        data = response.json()
        if "Fault" in data:
            error = (data["Fault"].get("Error") or [{}])[0]
            raise RuntimeError(
                f"QBO fault [{error.get('code', '')}] "
                f"{error.get('Message', 'Unknown error')}: {error.get('Detail', '')}"
            )
        return data
    raise RuntimeError(f"QuickBooks {method.upper()} {path} failed after token refresh retry")


def _fetch_qbo_invoice(qbo_invoice_id: str) -> dict:
    data = _qbo_request(
        "get",
        f"/invoice/{qbo_invoice_id}",
        params={"minorversion": "65"},
    )
    invoice = data.get("Invoice")
    if not invoice:
        raise RuntimeError(f"QBO invoice {qbo_invoice_id} not found")
    return invoice


def _sparse_update_line_amount(
    qbo_invoice: dict,
    expected_amount: float,
    qbo_item_id: Optional[str],
) -> dict:
    lines = qbo_invoice.get("Line") or []
    # Only touch the SalesItemLine rows; QBO surfaces a SubTotalLine at the
    # end which should not be repriced.
    sales_lines = [line for line in lines if line.get("DetailType") == "SalesItemLineDetail"]
    if len(sales_lines) != 1:
        raise RuntimeError(
            f"Invoice {qbo_invoice.get('Id')} has {len(sales_lines)} sales lines; "
            "manual review required rather than automated reprice."
        )

    target = sales_lines[0]
    updated_line = dict(target)
    updated_line["Amount"] = expected_amount
    detail = dict(updated_line.get("SalesItemLineDetail") or {})
    if qbo_item_id:
        item_ref = dict(detail.get("ItemRef") or {})
        item_ref["value"] = qbo_item_id
        detail["ItemRef"] = item_ref
    # Preserve Qty if present; update UnitPrice to the new amount.
    detail["UnitPrice"] = expected_amount
    updated_line["SalesItemLineDetail"] = detail

    rebuilt_lines: list[dict] = []
    for line in lines:
        if line.get("DetailType") == "SalesItemLineDetail" and line is target:
            rebuilt_lines.append(updated_line)
        else:
            rebuilt_lines.append(line)

    return {
        "Id":            qbo_invoice["Id"],
        "SyncToken":     qbo_invoice["SyncToken"],
        "sparse":        True,
        "Line":          rebuilt_lines,
    }


def _apply_qbo_reprice(
    qbo_invoice_id: str,
    expected_amount: float,
    qbo_item_id: Optional[str],
) -> None:
    qbo_invoice = _fetch_qbo_invoice(qbo_invoice_id)
    patch_body = _sparse_update_line_amount(qbo_invoice, expected_amount, qbo_item_id)
    _qbo_request(
        "post",
        "/invoice",
        json=patch_body,
        params={"minorversion": "65", "operation": "update"},
    )


# ---------------------------------------------------------------------------
# Local update
# ---------------------------------------------------------------------------

def _apply_local_reprice(db, invoice_id: str, expected_amount: float) -> None:
    with db:
        db.execute(
            "UPDATE invoices SET amount = %s WHERE id = %s",
            (expected_amount, invoice_id),
        )


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def _summarize(
    candidates: list[MispricedInvoice],
    skipped_paid: list[MispricedInvoice],
) -> None:
    if not candidates and not skipped_paid:
        logger.info("No mispriced invoices found for the requested scope.")
        return

    buckets: dict[str, dict[str, float]] = defaultdict(
        lambda: {"count": 0, "current_total": 0.0, "expected_total": 0.0, "delta_total": 0.0}
    )
    for cand in candidates:
        bucket = buckets[cand.service_type_id]
        bucket["count"] += 1
        bucket["current_total"] += cand.current_amount
        bucket["expected_total"] += cand.expected_amount
        bucket["delta_total"] += cand.delta

    logger.info("Per-service delta (candidates eligible for repricing):")
    logger.info(
        "  %-20s %6s %14s %14s %14s",
        "service_type_id", "count", "current_total", "expected_total", "delta_total",
    )
    for service_type_id, bucket in sorted(buckets.items()):
        logger.info(
            "  %-20s %6d %14.2f %14.2f %14.2f",
            service_type_id,
            bucket["count"],
            bucket["current_total"],
            bucket["expected_total"],
            bucket["delta_total"],
        )

    if skipped_paid:
        logger.info(
            "Skipped %d paid invoice(s) per forward-only policy. "
            "Use --reprice-paid only after finance sign-off.",
            len(skipped_paid),
        )


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def remediate(
    db,
    *,
    dry_run: bool,
    since: Optional[str],
    until: Optional[str],
    service_types: Optional[list[str]],
    reprice_paid: bool,
    skip_qbo: bool,
    limit: Optional[int],
) -> dict[str, int]:
    stats = {
        "candidates":       0,
        "skipped_paid":     0,
        "local_updated":    0,
        "qbo_updated":      0,
        "failed":           0,
    }

    all_candidates = _fetch_candidates(db, since, until, service_types, limit)
    stats["candidates"] = len(all_candidates)

    eligible: list[MispricedInvoice] = []
    skipped_paid: list[MispricedInvoice] = []
    for cand in all_candidates:
        if cand.is_paid and not reprice_paid:
            skipped_paid.append(cand)
            continue
        if cand.is_paid and reprice_paid:
            logger.warning(
                "Repricing PAID invoice %s (%s, $%.2f → $%.2f). Confirm finance sign-off.",
                cand.invoice_id,
                cand.service_type_id,
                cand.current_amount,
                cand.expected_amount,
            )
        eligible.append(cand)

    stats["skipped_paid"] = len(skipped_paid)
    _summarize(eligible, skipped_paid)

    if dry_run:
        logger.info("[DRY RUN] %d invoice(s) would be repriced.", len(eligible))
        return stats

    for index, cand in enumerate(eligible, start=1):
        catalog = SERVICE_CATALOGUE.get(cand.service_type_id) or {}
        qbo_item_id = catalog.get("qbo_item_id")

        try:
            if cand.qbo_invoice_id and not skip_qbo:
                _apply_qbo_reprice(cand.qbo_invoice_id, cand.expected_amount, qbo_item_id)
                stats["qbo_updated"] += 1
            elif not cand.qbo_invoice_id:
                logger.warning(
                    "Invoice %s has no cross_tool_mapping to QuickBooks; "
                    "updating local only.",
                    cand.invoice_id,
                )

            _apply_local_reprice(db, cand.invoice_id, cand.expected_amount)
            stats["local_updated"] += 1

            if index % 25 == 0 or index == len(eligible):
                logger.info(
                    "Progress %d/%d (local=%d, qbo=%d, failed=%d)",
                    index,
                    len(eligible),
                    stats["local_updated"],
                    stats["qbo_updated"],
                    stats["failed"],
                )
        except Exception as exc:
            stats["failed"] += 1
            logger.error(
                "Invoice %s (%s, %s → %s) reprice failed: %s",
                cand.invoice_id,
                cand.service_type_id,
                cand.current_amount,
                cand.expected_amount,
                exc,
            )

    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_date(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    # Validate format without converting to avoid surprises with timezone.
    date.fromisoformat(text)
    return text


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Apply repricing locally and in QuickBooks. Omit for dry-run mode.",
    )
    parser.add_argument(
        "--since",
        type=_parse_date,
        default=None,
        help="Only consider invoices with issue_date >= SINCE (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--until",
        type=_parse_date,
        default=None,
        help="Only consider invoices with issue_date <= UNTIL (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--service-types",
        nargs="+",
        default=None,
        help="Restrict to these canonical service_type_ids.",
    )
    parser.add_argument(
        "--reprice-paid",
        action="store_true",
        help=(
            "Override the forward-only default and reprice paid invoices too. "
            "Use only after finance sign-off; the policy reason is documented "
            "in the script docstring."
        ),
    )
    parser.add_argument(
        "--skip-qbo",
        action="store_true",
        help="Update the local invoices table only; do not call QuickBooks.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional cap on the number of candidates to consider.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    )

    if args.service_types:
        invalid = [s for s in args.service_types if s not in CANONICAL_SERVICE_IDS]
        if invalid:
            parser.error(
                f"Unknown service_type_ids: {invalid}. "
                f"Valid: {sorted(CANONICAL_SERVICE_IDS)}"
            )

    dry_run = not args.execute
    logger.info(
        "Starting mispriced-invoice remediation (%s; service_types=%s; since=%s; until=%s)",
        "dry-run" if dry_run else "execute",
        args.service_types or "ALL",
        args.since or "n/a",
        args.until or "n/a",
    )

    db = get_connection()
    try:
        stats = remediate(
            db,
            dry_run=dry_run,
            since=args.since,
            until=args.until,
            service_types=args.service_types,
            reprice_paid=args.reprice_paid,
            skip_qbo=args.skip_qbo,
            limit=args.limit,
        )
    finally:
        db.close()

    logger.info("Remediation summary:")
    for key, value in stats.items():
        logger.info("  %s=%s", key, value)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
