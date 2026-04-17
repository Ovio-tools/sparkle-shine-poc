"""
tests/test_remediate_mispriced_invoices.py

Unit tests for scripts/remediate_mispriced_invoices.py. Covers:
  * _sparse_update_line_amount — QBO patch-body construction
  * _expected_amount           — residential catalog + commercial resolver paths
  * remediate()                — dry-run, execute, paid-policy, skip-qbo,
                                 QBO update invocation, per-candidate failure isolation
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from scripts.remediate_mispriced_invoices import (
    MispricedInvoice,
    _expected_amount,
    _sparse_update_line_amount,
    remediate,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _candidate(**overrides) -> MispricedInvoice:
    defaults = dict(
        invoice_id="SS-INV-0100",
        client_id="SS-CLIENT-0001",
        job_id="SS-JOB-0100",
        service_type_id="deep-clean",
        issue_date="2026-04-10",
        status="sent",
        paid_date=None,
        current_amount=150.00,
        expected_amount=275.00,
        qbo_invoice_id="qbo-inv-42",
    )
    defaults.update(overrides)
    return MispricedInvoice(**defaults)


def _qbo_invoice(*, sales_lines: int = 1, include_subtotal: bool = True) -> dict:
    """Build a minimal QBO Invoice body with `sales_lines` SalesItemLine rows."""
    lines = []
    for i in range(sales_lines):
        lines.append({
            "DetailType": "SalesItemLineDetail",
            "Amount": 150.00,
            "SalesItemLineDetail": {
                "ItemRef": {"value": "19", "name": "Standard Residential Clean"},
                "Qty": 1,
                "UnitPrice": 150.00,
            },
        })
    if include_subtotal:
        lines.append({"DetailType": "SubTotalLine", "Amount": 150.00 * sales_lines})
    return {"Id": "qbo-inv-42", "SyncToken": "3", "Line": lines}


# ─────────────────────────────────────────────────────────────────────────────
# _sparse_update_line_amount — pure function
# ─────────────────────────────────────────────────────────────────────────────

def test_sparse_update_sets_sparse_flag_and_preserves_id_and_synctoken():
    invoice = _qbo_invoice()
    patch_body = _sparse_update_line_amount(invoice, 275.00, "20")
    assert patch_body["sparse"] is True
    assert patch_body["Id"] == "qbo-inv-42"
    assert patch_body["SyncToken"] == "3"


def test_sparse_update_overwrites_amount_unitprice_and_item():
    invoice = _qbo_invoice()
    patch_body = _sparse_update_line_amount(invoice, 275.00, "20")
    sales_line = next(
        line for line in patch_body["Line"]
        if line["DetailType"] == "SalesItemLineDetail"
    )
    assert sales_line["Amount"] == 275.00
    assert sales_line["SalesItemLineDetail"]["UnitPrice"] == 275.00
    assert sales_line["SalesItemLineDetail"]["ItemRef"]["value"] == "20"


def test_sparse_update_preserves_subtotal_line_untouched():
    invoice = _qbo_invoice(include_subtotal=True)
    patch_body = _sparse_update_line_amount(invoice, 275.00, "20")
    subtotal_lines = [
        line for line in patch_body["Line"] if line["DetailType"] == "SubTotalLine"
    ]
    # The subtotal must not be repriced; QBO recomputes it server-side.
    assert len(subtotal_lines) == 1
    assert subtotal_lines[0]["Amount"] == 150.00


def test_sparse_update_raises_when_multiple_sales_lines():
    invoice = _qbo_invoice(sales_lines=2)
    with pytest.raises(RuntimeError, match="manual review required"):
        _sparse_update_line_amount(invoice, 275.00, "20")


def test_sparse_update_preserves_qty_on_existing_line():
    invoice = _qbo_invoice()
    # Bump qty to simulate a multi-visit invoice line.
    invoice["Line"][0]["SalesItemLineDetail"]["Qty"] = 4
    patch_body = _sparse_update_line_amount(invoice, 275.00, "20")
    sales_line = next(
        line for line in patch_body["Line"]
        if line["DetailType"] == "SalesItemLineDetail"
    )
    assert sales_line["SalesItemLineDetail"]["Qty"] == 4


def test_sparse_update_without_item_id_keeps_existing_itemref():
    invoice = _qbo_invoice()
    patch_body = _sparse_update_line_amount(invoice, 275.00, qbo_item_id=None)
    sales_line = next(
        line for line in patch_body["Line"]
        if line["DetailType"] == "SalesItemLineDetail"
    )
    # ItemRef is untouched when no qbo_item_id is passed.
    assert sales_line["SalesItemLineDetail"]["ItemRef"]["value"] == "19"


# ─────────────────────────────────────────────────────────────────────────────
# _expected_amount — pricing resolution
# ─────────────────────────────────────────────────────────────────────────────

def test_expected_amount_residential_uses_catalog_base_price():
    # deep-clean is 275 in config.business.SERVICE_TYPES.
    assert _expected_amount("deep-clean", "SS-CLIENT-0001", "2026-04-10") == 275.00


def test_expected_amount_unknown_service_returns_none():
    assert _expected_amount("ceramic-tile-restore", "SS-CLIENT-0001", "2026-04-10") is None


@patch("scripts.remediate_mispriced_invoices.get_commercial_per_visit_rate")
def test_expected_amount_commercial_uses_runtime_resolver(mock_rate):
    mock_rate.return_value = 512.34
    assert _expected_amount(
        "commercial-nightly", "SS-CLIENT-0050", "2026-04-10"
    ) == 512.34
    mock_rate.assert_called_once_with(
        client_id="SS-CLIENT-0050",
        job_date="2026-04-10",
        service_type_id="commercial-nightly",
    )


@patch("scripts.remediate_mispriced_invoices.get_commercial_per_visit_rate")
def test_expected_amount_commercial_unresolvable_returns_none(mock_rate):
    mock_rate.side_effect = ValueError("no agreement")
    # When the per-visit resolver fails we return None rather than guessing;
    # the candidate is then quietly dropped from the remediation set.
    assert _expected_amount(
        "commercial-nightly", "SS-CLIENT-0050", "2026-04-10"
    ) is None


# ─────────────────────────────────────────────────────────────────────────────
# remediate() — orchestration, policy, dry-run gate
# ─────────────────────────────────────────────────────────────────────────────

@patch("scripts.remediate_mispriced_invoices._fetch_candidates")
def test_remediate_dry_run_does_not_mutate_local_or_qbo(mock_fetch):
    mock_fetch.return_value = [_candidate()]
    db = MagicMock()

    with patch(
        "scripts.remediate_mispriced_invoices._apply_qbo_reprice"
    ) as mock_qbo, patch(
        "scripts.remediate_mispriced_invoices._apply_local_reprice"
    ) as mock_local:
        stats = remediate(
            db, dry_run=True, since=None, until=None,
            service_types=None, reprice_paid=False, skip_qbo=False, limit=None,
        )

    mock_qbo.assert_not_called()
    mock_local.assert_not_called()
    assert stats == {
        "candidates":    1,
        "skipped_paid":  0,
        "local_updated": 0,
        "qbo_updated":   0,
        "failed":        0,
    }


@patch("scripts.remediate_mispriced_invoices._fetch_candidates")
def test_remediate_execute_updates_local_and_qbo(mock_fetch):
    mock_fetch.return_value = [_candidate()]
    db = MagicMock()

    with patch(
        "scripts.remediate_mispriced_invoices._apply_qbo_reprice"
    ) as mock_qbo, patch(
        "scripts.remediate_mispriced_invoices._apply_local_reprice"
    ) as mock_local:
        stats = remediate(
            db, dry_run=False, since=None, until=None,
            service_types=None, reprice_paid=False, skip_qbo=False, limit=None,
        )

    mock_qbo.assert_called_once_with("qbo-inv-42", 275.00, "20")
    mock_local.assert_called_once_with(db, "SS-INV-0100", 275.00)
    assert stats["local_updated"] == 1
    assert stats["qbo_updated"]   == 1
    assert stats["failed"]        == 0


@patch("scripts.remediate_mispriced_invoices._fetch_candidates")
def test_remediate_skips_paid_by_default(mock_fetch):
    mock_fetch.return_value = [
        _candidate(invoice_id="SS-INV-0100", status="sent", paid_date=None),
        _candidate(invoice_id="SS-INV-0101", status="paid", paid_date="2026-04-12"),
    ]
    db = MagicMock()

    with patch(
        "scripts.remediate_mispriced_invoices._apply_qbo_reprice"
    ) as mock_qbo, patch(
        "scripts.remediate_mispriced_invoices._apply_local_reprice"
    ) as mock_local:
        stats = remediate(
            db, dry_run=False, since=None, until=None,
            service_types=None, reprice_paid=False, skip_qbo=False, limit=None,
        )

    # Only the unpaid invoice should have been repriced.
    assert mock_local.call_count == 1
    assert mock_qbo.call_count   == 1
    assert stats["candidates"]   == 2
    assert stats["skipped_paid"] == 1
    assert stats["local_updated"] == 1
    assert stats["qbo_updated"]  == 1


@patch("scripts.remediate_mispriced_invoices._fetch_candidates")
def test_remediate_reprice_paid_opts_paid_records_in(mock_fetch):
    mock_fetch.return_value = [
        _candidate(invoice_id="SS-INV-0100", status="sent", paid_date=None),
        _candidate(invoice_id="SS-INV-0101", status="paid", paid_date="2026-04-12"),
    ]
    db = MagicMock()

    with patch(
        "scripts.remediate_mispriced_invoices._apply_qbo_reprice"
    ) as mock_qbo, patch(
        "scripts.remediate_mispriced_invoices._apply_local_reprice"
    ) as mock_local:
        stats = remediate(
            db, dry_run=False, since=None, until=None,
            service_types=None, reprice_paid=True, skip_qbo=False, limit=None,
        )

    assert mock_local.call_count == 2
    assert mock_qbo.call_count   == 2
    assert stats["skipped_paid"] == 0


@patch("scripts.remediate_mispriced_invoices._fetch_candidates")
def test_remediate_skip_qbo_bypasses_quickbooks_call(mock_fetch):
    mock_fetch.return_value = [_candidate()]
    db = MagicMock()

    with patch(
        "scripts.remediate_mispriced_invoices._apply_qbo_reprice"
    ) as mock_qbo, patch(
        "scripts.remediate_mispriced_invoices._apply_local_reprice"
    ) as mock_local:
        stats = remediate(
            db, dry_run=False, since=None, until=None,
            service_types=None, reprice_paid=False, skip_qbo=True, limit=None,
        )

    mock_qbo.assert_not_called()
    mock_local.assert_called_once()
    assert stats["qbo_updated"]   == 0
    assert stats["local_updated"] == 1


@patch("scripts.remediate_mispriced_invoices._fetch_candidates")
def test_remediate_no_qbo_mapping_updates_local_only(mock_fetch):
    # Invoice never synced to QBO → no qbo_invoice_id. Local update still
    # runs so the simulation DB matches the canonical catalogue.
    mock_fetch.return_value = [_candidate(qbo_invoice_id=None)]
    db = MagicMock()

    with patch(
        "scripts.remediate_mispriced_invoices._apply_qbo_reprice"
    ) as mock_qbo, patch(
        "scripts.remediate_mispriced_invoices._apply_local_reprice"
    ) as mock_local:
        stats = remediate(
            db, dry_run=False, since=None, until=None,
            service_types=None, reprice_paid=False, skip_qbo=False, limit=None,
        )

    mock_qbo.assert_not_called()
    mock_local.assert_called_once()
    assert stats["qbo_updated"]   == 0
    assert stats["local_updated"] == 1


@patch("scripts.remediate_mispriced_invoices._fetch_candidates")
def test_remediate_per_candidate_failure_is_isolated(mock_fetch):
    # First candidate's QBO call raises; the loop must still process the
    # second candidate and record stats["failed"] == 1.
    mock_fetch.return_value = [
        _candidate(invoice_id="SS-INV-0100", qbo_invoice_id="qbo-inv-A"),
        _candidate(invoice_id="SS-INV-0101", qbo_invoice_id="qbo-inv-B"),
    ]
    db = MagicMock()

    def _qbo_side_effect(qbo_id, amount, item):
        if qbo_id == "qbo-inv-A":
            raise RuntimeError("QBO 500 on first record")

    with patch(
        "scripts.remediate_mispriced_invoices._apply_qbo_reprice",
        side_effect=_qbo_side_effect,
    ), patch(
        "scripts.remediate_mispriced_invoices._apply_local_reprice"
    ) as mock_local:
        stats = remediate(
            db, dry_run=False, since=None, until=None,
            service_types=None, reprice_paid=False, skip_qbo=False, limit=None,
        )

    # The first candidate fails before local update; the second succeeds.
    assert stats["failed"]       == 1
    assert stats["local_updated"] == 1
    assert stats["qbo_updated"]  == 1
    mock_local.assert_called_once_with(db, "SS-INV-0101", 275.00)


@patch("scripts.remediate_mispriced_invoices._fetch_candidates")
def test_remediate_propagates_filters_to_fetch(mock_fetch):
    mock_fetch.return_value = []
    db = MagicMock()

    remediate(
        db, dry_run=True,
        since="2026-04-01", until="2026-04-15",
        service_types=["deep-clean", "recurring-weekly"],
        reprice_paid=False, skip_qbo=False, limit=50,
    )

    mock_fetch.assert_called_once_with(
        db, "2026-04-01", "2026-04-15",
        ["deep-clean", "recurring-weekly"], 50,
    )
