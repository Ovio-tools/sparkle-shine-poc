"""
simulation/generators/payments.py

Records QuickBooks payments against outstanding invoices with realistic
timing distributions based on per-client payment profiles.

Type 3 generator: reacts to existing invoice records created by automations.
Dry-run convention: reads always allowed; QBO API + SQLite writes skipped.
"""
from __future__ import annotations

import random
import sqlite3
from dataclasses import dataclass
from datetime import date, timedelta
from time import sleep
from typing import Optional

import requests

from auth import get_client
from auth.quickbooks_auth import get_base_url
from database.mappings import generate_id, get_tool_id, register_mapping
from intelligence.logging_config import setup_logging
from seeding.utils.throttler import QUICKBOOKS as throttler
from simulation.exceptions import TokenExpiredError

logger = setup_logging("simulation.payments")


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------

@dataclass
class GeneratorResult:
    success: bool
    message: str = ""


# ---------------------------------------------------------------------------
# Payment profile configuration
# ---------------------------------------------------------------------------

# Profile weights per client type: [on_time, slow, very_slow, non_payer]
_PROFILE_WEIGHTS = {
    "residential": [0.77, 0.15, 0.07, 0.01],
    "commercial":  [0.65, 0.22, 0.10, 0.03],
}

_PROFILES = ["on_time", "slow", "very_slow", "non_payer"]

# Payment window (days after invoice issue_date): (min, max)
# non_payer has no window — invoice is never paid
_PAYMENT_WINDOWS = {
    "on_time":   (3, 15),
    "slow":      (15, 45),
    "very_slow": (45, 75),
    "non_payer": None,
}

_WRITE_OFF_DAYS = 90

_qbo_base_url_cache: Optional[str] = None


def _qbo_base() -> str:
    global _qbo_base_url_cache
    if _qbo_base_url_cache is None:
        _qbo_base_url_cache = get_base_url()
    return _qbo_base_url_cache


# ---------------------------------------------------------------------------
# Per-client profile assignment (deterministic, no extra DB column needed)
# ---------------------------------------------------------------------------

def _assign_profile(client_id: str, client_type: str) -> str:
    """Deterministically assign a payment profile from the client's canonical ID.

    Uses a seeded RNG so the same client always gets the same profile across
    simulation ticks without storing the profile in SQLite.
    """
    weights = _PROFILE_WEIGHTS.get(client_type, _PROFILE_WEIGHTS["residential"])
    rng = random.Random(hash(client_id) & 0xFFFFFFFF)
    return rng.choices(_PROFILES, weights=weights, k=1)[0]


def _target_payment_date(profile: str, issue_date: date) -> Optional[date]:
    """Return the day the client will pay, or None for non-payers.

    People procrastinate and pay near the end of their window: uses a
    beta(2, 1) distribution skewed toward the high end.
    """
    window = _PAYMENT_WINDOWS[profile]
    if window is None:
        return None
    lo, hi = window
    # beta(2, 1) skews toward 1.0 (end of window)
    fraction = random.betavariate(2, 1)
    days = lo + int(fraction * (hi - lo))
    return issue_date + timedelta(days=days)


# ---------------------------------------------------------------------------
# Main generator
# ---------------------------------------------------------------------------

class PaymentGenerator:
    """
    Records QuickBooks payments against outstanding invoices.

    On each execute_one() call:
      - Scans the oldest unpaid invoices (up to 20) for the next actionable one
      - On-time / slow / very_slow: creates a QBO payment when target date arrives
      - non_payer at 90+ days: marks invoice as overdue + logs bad_debt, no payment
    """

    name = "payments"

    def __init__(self, db_path: str = "sparkle_shine.db"):
        self.db_path = db_path
        self.logger = logger

    def execute(self, dry_run: bool = False) -> GeneratorResult:
        """Synchronous entry point called by the simulation engine dispatch loop."""
        import asyncio
        return asyncio.run(self.execute_one(dry_run=dry_run))

    async def execute_one(self, dry_run: bool = False) -> GeneratorResult:
        db = sqlite3.connect(self.db_path)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys = ON")
        today = date.today()

        try:
            candidates = self._get_unpaid_invoices(db)
            if not candidates:
                return GeneratorResult(success=True, message="No unpaid invoices")

            for invoice in candidates:
                result = self._try_process(invoice, db, today)
                if result is not None:
                    return result

            return GeneratorResult(success=True, message="No invoices ready for payment today")

        except Exception as e:
            db.rollback()
            self.logger.error("PaymentGenerator.execute_one failed: %s", e)
            raise

        finally:
            db.close()

    # -------------------------------------------------------------------------
    # Per-invoice processing
    # -------------------------------------------------------------------------

    def _try_process(
        self,
        invoice: sqlite3.Row,
        db: sqlite3.Connection,
        today: date,
    ) -> Optional[GeneratorResult]:
        """Attempt to process one invoice. Returns a GeneratorResult if action
        was taken, or None to signal 'skip and try the next invoice'."""

        invoice_id = invoice["id"]
        client_id = invoice["client_id"]
        amount = invoice["amount"]
        issue_date = date.fromisoformat(invoice["issue_date"])
        days_outstanding = (today - issue_date).days

        # Resolve client type for profile weighting
        client_row = db.execute(
            "SELECT client_type FROM clients WHERE id = ?", (client_id,)
        ).fetchone()
        client_type = client_row["client_type"] if client_row else "residential"
        profile = _assign_profile(client_id, client_type)

        # ── Non-payer write-off at 90+ days ──────────────────────────────────
        if profile == "non_payer":
            if days_outstanding >= _WRITE_OFF_DAYS:
                db.execute(
                    "UPDATE invoices SET status = 'overdue', days_outstanding = ? WHERE id = ?",
                    (days_outstanding, invoice_id),
                )
                db.commit()
                self.logger.warning(
                    "bad_debt: %s written off at %d days (client %s, $%.2f)",
                    invoice_id, days_outstanding, client_id, amount,
                )
                return GeneratorResult(
                    success=True,
                    message=f"bad_debt written off: {invoice_id} at {days_outstanding} days",
                )
            # Non-payer not yet at write-off threshold — update days and skip
            db.execute(
                "UPDATE invoices SET days_outstanding = ? WHERE id = ?",
                (days_outstanding, invoice_id),
            )
            db.commit()
            return None  # try the next invoice

        # ── Check whether the client's target payment date has arrived ────────
        target_date = _target_payment_date(profile, issue_date)
        if target_date is None or today < target_date:
            # Update days_outstanding but don't pay yet; try the next invoice
            db.execute(
                "UPDATE invoices SET days_outstanding = ? WHERE id = ?",
                (days_outstanding, invoice_id),
            )
            db.commit()
            return None

        # ── Look up QBO IDs ───────────────────────────────────────────────────
        qbo_invoice_id = get_tool_id(invoice_id, "quickbooks", self.db_path)
        qbo_customer_id = get_tool_id(client_id, "quickbooks", self.db_path)

        if not qbo_invoice_id or not qbo_customer_id:
            self.logger.warning(
                "Skipping %s: missing QBO mapping (invoice=%s, customer=%s)",
                invoice_id, qbo_invoice_id, qbo_customer_id,
            )
            return None  # skip, try next

        # ── Create QBO payment ────────────────────────────────────────────────
        qbo_payment_id = self._create_qbo_payment(
            qbo_customer_id, qbo_invoice_id, amount, today
        )

        # ── Write to SQLite ───────────────────────────────────────────────────
        payment_canonical = generate_id("PAY", self.db_path)
        paid_date_str = today.isoformat()

        db.execute(
            """
            INSERT INTO payments (id, invoice_id, client_id, amount, payment_method, payment_date)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (payment_canonical, invoice_id, client_id, amount, "online", paid_date_str),
        )
        db.execute(
            """
            UPDATE invoices
            SET status = 'paid', paid_date = ?, days_outstanding = ?
            WHERE id = ?
            """,
            (paid_date_str, days_outstanding, invoice_id),
        )
        db.commit()
        register_mapping(payment_canonical, "quickbooks", str(qbo_payment_id), db_path=self.db_path)

        self.logger.info(
            "Payment recorded: %s → QBO %s | $%.2f | invoice %s | %s | %d days",
            payment_canonical, qbo_payment_id, amount, invoice_id, profile, days_outstanding,
        )
        return GeneratorResult(
            success=True,
            message=(
                f"Paid ${amount:.0f} for {invoice_id} "
                f"(profile={profile}, {days_outstanding} days)"
            ),
        )

    # -------------------------------------------------------------------------
    # SQLite helpers
    # -------------------------------------------------------------------------

    def _get_unpaid_invoices(self, db: sqlite3.Connection) -> list[sqlite3.Row]:
        """Return the 20 oldest unpaid sent/overdue invoices."""
        cursor = db.execute(
            """
            SELECT id, client_id, amount, issue_date, days_outstanding
            FROM invoices
            WHERE status IN ('sent', 'overdue')
              AND paid_date IS NULL
            ORDER BY issue_date ASC
            LIMIT 20
            """
        )
        return cursor.fetchall()

    # -------------------------------------------------------------------------
    # QBO API call
    # -------------------------------------------------------------------------

    def _create_qbo_payment(
        self,
        qbo_customer_id: str,
        qbo_invoice_id: str,
        amount: float,
        payment_date: date,
    ) -> str:
        """POST /payment to QuickBooks. Returns the QBO payment Id string."""
        headers = get_client("quickbooks")
        url = f"{_qbo_base()}/payment"
        payload = {
            "CustomerRef": {"value": qbo_customer_id},
            "TotalAmt": amount,
            "TxnDate": payment_date.isoformat(),
            "Line": [
                {
                    "Amount": amount,
                    "LinkedTxn": [
                        {"TxnId": qbo_invoice_id, "TxnType": "Invoice"}
                    ],
                }
            ],
        }

        throttler.wait()
        resp = requests.post(url, headers=headers, json=payload, timeout=30)

        if resp.status_code in (200, 201):
            body = resp.json()
            return body["Payment"]["Id"]

        if resp.status_code == 401:
            raise TokenExpiredError(f"QBO token expired: {resp.text[:200]}")

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 5))
            sleep(retry_after)
            return self._create_qbo_payment(
                qbo_customer_id, qbo_invoice_id, amount, payment_date
            )

        raise RuntimeError(f"QBO payment API {resp.status_code}: {resp.text[:300]}")
