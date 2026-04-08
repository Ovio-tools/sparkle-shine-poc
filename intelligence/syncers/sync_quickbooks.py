"""
QuickBooks Online syncer -- pulls invoices, payments, and balance sheet data.

Uses the QBO REST API (query endpoint + Balance Sheet report).
Cash position metrics are stored in daily_metrics_snapshot.raw_json.
"""
import json
import time
from datetime import date, datetime
from typing import Optional

import requests

from auth import get_client
from auth.quickbooks_auth import get_base_url
from database.mappings import get_canonical_id, register_mapping, generate_id
from intelligence.syncers.base_syncer import BaseSyncer, SyncResult
from seeding.utils.throttler import QUICKBOOKS


class QuickBooksSyncer(BaseSyncer):
    tool_name = "quickbooks"

    def sync(self, since: Optional[datetime] = None) -> SyncResult:
        is_incremental = since is not None
        start = time.monotonic()
        errors: list[str] = []
        total = 0

        self.logger.info(
            "Starting %s QuickBooks sync (since=%s)",
            "incremental" if is_incremental else "full",
            since,
        )

        try:
            headers = get_client("quickbooks")
            base_url = get_base_url()
        except Exception as exc:
            errors.append(f"Auth failed: {exc}")
            self.update_sync_state(0, error=str(exc))
            return SyncResult(
                tool_name=self.tool_name,
                records_synced=0,
                errors=errors,
                duration_seconds=time.monotonic() - start,
                is_incremental=is_incremental,
            )

        since_str = since.strftime("%Y-%m-%dT%H:%M:%S") if since else "1970-01-01T00:00:00"

        total += self._sync_invoices(headers, base_url, since_str, errors)
        total += self._sync_payments(headers, base_url, since_str, errors)
        self._sync_balance_sheet(headers, base_url, errors)

        self.update_sync_state(total, error=errors[0] if errors else None)
        duration = time.monotonic() - start
        self.logger.info("QuickBooks sync complete: %d records in %.1fs", total, duration)
        return SyncResult(
            tool_name=self.tool_name,
            records_synced=total,
            errors=errors,
            duration_seconds=duration,
            is_incremental=is_incremental,
        )

    # ------------------------------------------------------------------ #
    # QBO query helper
    # ------------------------------------------------------------------ #

    def _qbo_query(self, headers: dict, base_url: str, query: str) -> list:
        """Execute a QBO SQL-like query and return the entity list."""
        QUICKBOOKS.wait()
        resp = requests.get(
            f"{base_url}/query",
            headers=headers,
            params={"query": query},
            timeout=30,
        )
        resp.raise_for_status()
        qr = resp.json().get("QueryResponse", {})
        # The entity type is the only non-metadata key
        for key, value in qr.items():
            if isinstance(value, list):
                return value
        return []

    # ------------------------------------------------------------------ #
    # Invoices
    # ------------------------------------------------------------------ #

    def _sync_invoices(self, headers, base_url, since_str, errors) -> int:
        count = 0
        try:
            rows = self._qbo_query(
                headers,
                base_url,
                f"SELECT * FROM Invoice WHERE MetaData.LastUpdatedTime > '{since_str}' MAXRESULTS 1000",
            )
        except Exception as exc:
            errors.append(f"invoices fetch error: {exc}")
            return 0

        today = date.today().isoformat()
        for inv in rows:
            try:
                qbo_id = str(inv["Id"])
                canonical_id = get_canonical_id(
                    "quickbooks", qbo_id, entity_type="INV", db_path=self.db_path
                )

                amount = float(inv.get("TotalAmt", 0))
                balance = float(inv.get("Balance", 0))
                issue_date = inv.get("TxnDate") or today
                due_date = inv.get("DueDate")

                if balance == 0:
                    status = "paid"
                    paid_date = inv.get("MetaData", {}).get("LastUpdatedTime", "")[:10] or today
                elif due_date and due_date < today:
                    status = "overdue"
                    paid_date = None
                else:
                    status = "sent"
                    paid_date = None

                days_outstanding = None
                if status in ("sent", "overdue"):
                    try:
                        days_outstanding = (
                            date.today() - date.fromisoformat(issue_date)
                        ).days
                    except ValueError:
                        pass

                if canonical_id is None:
                    client_ref = str((inv.get("CustomerRef") or {}).get("value", ""))
                    client_canonical = (
                        get_canonical_id(
                            "quickbooks", client_ref, entity_type="CLIENT", db_path=self.db_path
                        )
                        if client_ref else None
                    )
                    if client_canonical is None:
                        continue  # Can't link to a known client -- skip

                    canonical_id = generate_id("INV", self.db_path)
                    with self.db:
                        self.db.execute(
                            """
                            INSERT INTO invoices
                                (id, client_id, amount, status, issue_date,
                                 due_date, paid_date, days_outstanding)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT DO NOTHING
                            """,
                            (
                                canonical_id, client_canonical, amount, status,
                                issue_date, due_date, paid_date, days_outstanding,
                            ),
                        )
                    register_mapping(canonical_id, "quickbooks", qbo_id, db_path=self.db_path)
                else:
                    with self.db:
                        self.db.execute(
                            """
                            UPDATE invoices
                            SET status           = %s,
                                paid_date        = COALESCE(%s, paid_date),
                                days_outstanding = %s
                            WHERE id = %s
                            """,
                            (status, paid_date, days_outstanding, canonical_id),
                        )

                count += 1
            except Exception as exc:
                errors.append(f"invoice {inv.get('Id')}: {exc}")

        self.logger.debug("Synced %d invoices", count)
        return count

    # ------------------------------------------------------------------ #
    # Payments
    # ------------------------------------------------------------------ #

    def _sync_payments(self, headers, base_url, since_str, errors) -> int:
        count = 0
        try:
            rows = self._qbo_query(
                headers,
                base_url,
                f"SELECT * FROM Payment WHERE MetaData.LastUpdatedTime > '{since_str}' MAXRESULTS 1000",
            )
        except Exception as exc:
            errors.append(f"payments fetch error: {exc}")
            return 0

        for pay in rows:
            try:
                qbo_id = str(pay["Id"])
                # Skip if already mapped
                if get_canonical_id(
                    "quickbooks", qbo_id, entity_type="PAY", db_path=self.db_path
                ) is not None:
                    continue

                amount = float(pay.get("TotalAmt", 0))
                payment_date = pay.get("TxnDate") or date.today().isoformat()
                payment_method = (pay.get("PaymentMethodRef") or {}).get("name")

                # Resolve the linked invoice
                inv_qbo_id = None
                for line in (pay.get("Line") or []):
                    for lt in (line.get("LinkedTxn") or []):
                        if lt.get("TxnType") == "Invoice":
                            inv_qbo_id = str(lt["TxnId"])
                            break
                    if inv_qbo_id:
                        break

                inv_canonical = (
                    get_canonical_id(
                        "quickbooks", inv_qbo_id, entity_type="INV", db_path=self.db_path
                    )
                    if inv_qbo_id else None
                )
                if inv_canonical is None:
                    continue

                row = self.db.execute(
                    "SELECT client_id FROM invoices WHERE id = %s", (inv_canonical,)
                ).fetchone()
                if row is None:
                    continue

                canonical_id = generate_id("PAY", self.db_path)
                with self.db:
                    self.db.execute(
                        """
                        INSERT INTO payments
                            (id, invoice_id, client_id, amount, payment_method, payment_date)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                        """,
                        (
                            canonical_id, inv_canonical, row["client_id"],
                            amount, payment_method, payment_date,
                        ),
                    )
                register_mapping(canonical_id, "quickbooks", qbo_id, db_path=self.db_path)
                count += 1
            except Exception as exc:
                errors.append(f"payment {pay.get('Id')}: {exc}")

        self.logger.debug("Synced %d payments", count)
        return count

    # ------------------------------------------------------------------ #
    # Balance sheet (cash position)
    # ------------------------------------------------------------------ #

    def _sync_balance_sheet(self, headers, base_url, errors) -> None:
        try:
            QUICKBOOKS.wait()
            today = date.today().isoformat()
            resp = requests.get(
                f"{base_url}/reports/BalanceSheet",
                headers=headers,
                params={"date": today},
                timeout=30,
            )
            resp.raise_for_status()
            report = resp.json()

            bank_balance = self._find_report_value(report, "Total Bank Accounts")
            total_ar = self._find_report_value(report, "Total Accounts Receivable (A/R)")
            total_ap = self._find_report_value(report, "Total Accounts Payable (A/P)")

            self._merge_snapshot(today, {
                "bank_balance": bank_balance,
                "total_ar": total_ar,
                "total_ap": total_ap,
            })
            self.logger.debug(
                "Balance sheet: bank=%.2f  AR=%.2f  AP=%.2f",
                bank_balance or 0, total_ar or 0, total_ap or 0,
            )
        except Exception as exc:
            errors.append(f"balance sheet error: {exc}")

    def _find_report_value(self, report: dict, label: str) -> Optional[float]:
        """Recursively walk QBO report rows to find a numeric value by label."""
        for row in (report.get("Rows") or {}).get("Row", []):
            result = self._search_row(row, label)
            if result is not None:
                return result
        return None

    def _search_row(self, row: dict, label: str) -> Optional[float]:
        summary = row.get("Summary") or {}
        col_data = summary.get("ColData") or []
        for i, col in enumerate(col_data):
            if col.get("value", "").strip() == label and i + 1 < len(col_data):
                try:
                    return float(col_data[i + 1].get("value") or 0)
                except (ValueError, TypeError):
                    return None
        # Recurse into sub-rows
        for sub in (row.get("Rows") or {}).get("Row", []):
            result = self._search_row(sub, label)
            if result is not None:
                return result
        return None

    def _merge_snapshot(self, snapshot_date: str, new_data: dict) -> None:
        """Merge new_data into daily_metrics_snapshot.raw_json for snapshot_date."""
        row = self.db.execute(
            "SELECT raw_json FROM daily_metrics_snapshot WHERE snapshot_date = %s",
            (snapshot_date,),
        ).fetchone()
        existing = json.loads(row["raw_json"]) if (row and row["raw_json"]) else {}
        existing.update(new_data)
        with self.db:
            self.db.execute(
                """
                INSERT INTO daily_metrics_snapshot (snapshot_date, raw_json)
                VALUES (%s, %s)
                ON CONFLICT(snapshot_date) DO UPDATE SET raw_json = EXCLUDED.raw_json
                """,
                (snapshot_date, json.dumps(existing)),
            )


# ------------------------------------------------------------------ #
# CLI entry point
# ------------------------------------------------------------------ #

if __name__ == "__main__":
    import argparse
    import os
    import sys

    parser = argparse.ArgumentParser(description="Sync QuickBooks invoices/payments/balance-sheet into SQLite")
    parser.add_argument("--dry-run", action="store_true", help="Auth check + sample fetch; no DB writes")
    parser.add_argument("--since", metavar="YYYY-MM-DD", help="Incremental sync from this date")
    parser.add_argument("--db", default="sparkle_shine.db", help="Path to SQLite database")
    args = parser.parse_args()

    db_path = os.path.abspath(args.db)
    since = datetime.strptime(args.since, "%Y-%m-%d") if args.since else None

    syncer = QuickBooksSyncer(db_path)
    last_sync = syncer.get_last_sync_time()

    print(f"\n[quickbooks] DB:        {db_path}")
    print(f"[quickbooks] Last sync: {last_sync or 'never'}")
    print(f"[quickbooks] Mode:      {'DRY RUN' if args.dry_run else 'LIVE'}")
    if since:
        print(f"[quickbooks] Since:     {since.date()}")

    if args.dry_run:
        print("\n[quickbooks] --- Auth check ---")
        try:
            headers = get_client("quickbooks")
            base_url = get_base_url()
            print(f"[quickbooks] Auth OK — base URL: {base_url}")
        except Exception as exc:
            print(f"[quickbooks] Auth FAILED: {exc}")
            syncer.close()
            sys.exit(1)

        print("\n[quickbooks] --- Sample fetch (first 3 invoices, no DB writes) ---")
        try:
            rows = syncer._qbo_query(headers, base_url, "SELECT * FROM Invoice MAXRESULTS 3")
            for inv in rows:
                status = "paid" if float(inv.get("Balance", 1)) == 0 else "open"
                print(
                    f"  [#{inv['Id']}] {inv.get('CustomerRef', {}).get('name', '?')} — "
                    f"${inv.get('TotalAmt', 0):.2f} ({status}) due {inv.get('DueDate', '?')}"
                )
            if not rows:
                print("  (no invoices returned)")

            print("\n[quickbooks] --- Balance sheet preview ---")
            QUICKBOOKS.wait()
            resp = requests.get(
                f"{base_url}/reports/BalanceSheet",
                headers=headers,
                params={"date": date.today().isoformat()},
                timeout=30,
            )
            resp.raise_for_status()
            print(f"  Balance sheet fetched OK ({len(resp.content)} bytes)")
            print(f"\n[quickbooks] Would sync invoices, payments, and cash position.")
            print(f"[quickbooks] Run without --dry-run to apply changes.")
        except Exception as exc:
            print(f"[quickbooks] Sample fetch failed: {exc}")

        syncer.close()
        sys.exit(0)

    result = syncer.sync(since=since)
    syncer.close()

    print(f"\n[quickbooks] Synced {result.records_synced} records in {result.duration_seconds:.1f}s")
    if result.errors:
        print(f"[quickbooks] {len(result.errors)} error(s):")
        for err in result.errors[:10]:
            print(f"  - {err}")
    sys.exit(1 if result.errors else 0)
