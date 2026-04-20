#!/usr/bin/env python3
"""Snapshot and quarantine the residual orphan-invoice bucket.

This is the Track B "quarantine, don't relink" path for orphan invoices that:

- are still orphaned (``invoices.job_id IS NULL``)
- are not obvious duplicate-shaped rows
- have no same-day completed job for the same client

The script is intentionally additive:

- it never updates ``invoices.job_id``
- it never writes to QuickBooks
- it records quarantine state in ``invoice_quarantine``
- it can emit a CSV evidence snapshot for operators

Usage:
    python scripts/quarantine_residual_orphan_invoices.py
    python scripts/quarantine_residual_orphan_invoices.py --apply
    python scripts/quarantine_residual_orphan_invoices.py --apply --snapshot-path /tmp/quarantine.csv
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
from datetime import date
from pathlib import Path
from typing import Iterable, TextIO

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from database.connection import table_exists
from database.schema import get_connection

logger = logging.getLogger("quarantine_residual_orphan_invoices")

AUTO_QUARANTINE = "auto_quarantine"
MANUAL_REVIEW = "manual_review"
DEFAULT_SOURCE = "track_b_residual_orphan_quarantine_2026_04_19"
DEFAULT_REVIEWED_BY = "operations-human-review-complete"

CSV_FIELDS = [
    "invoice_id",
    "client_id",
    "issue_date",
    "amount",
    "qbo_invoice_id",
    "quarantine_lane",
    "reason_code",
    "nearby_completed_job_count_3d",
    "nearby_job_ids",
    "reason_detail",
    "source",
    "snapshot_date",
    "reviewed_by",
]


def _ensure_invoice_quarantine_table(db) -> None:
    if not table_exists(db, "invoice_quarantine"):
        db.execute(
            """
            CREATE TABLE invoice_quarantine (
                invoice_id       TEXT PRIMARY KEY REFERENCES invoices(id),
                client_id        TEXT NOT NULL REFERENCES clients(id),
                issue_date       TEXT NOT NULL,
                amount           REAL NOT NULL,
                qbo_invoice_id   TEXT,
                quarantine_lane  TEXT NOT NULL
                                    CHECK(quarantine_lane IN ('auto_quarantine','manual_review')),
                reason_code      TEXT NOT NULL,
                reason_detail    TEXT,
                source           TEXT NOT NULL,
                snapshot_date    TEXT NOT NULL,
                reviewed_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                reviewed_by      TEXT NOT NULL,
                released_at      TIMESTAMP,
                released_by      TEXT
            )
            """
        )
    db.execute(
        "CREATE INDEX IF NOT EXISTS idx_invoice_quarantine_lane "
        "ON invoice_quarantine(quarantine_lane)"
    )
    db.execute(
        "CREATE INDEX IF NOT EXISTS idx_invoice_quarantine_reason "
        "ON invoice_quarantine(reason_code)"
    )


def _fetch_residual_no_same_day_candidates(db, nearby_window_days: int) -> list[dict]:
    rows = db.execute(
        """
        WITH orphan_base AS (
            SELECT
                i.id AS invoice_id,
                i.client_id,
                i.issue_date,
                i.amount,
                m.tool_specific_id AS qbo_invoice_id
            FROM invoices i
            LEFT JOIN cross_tool_mapping m
              ON m.canonical_id = i.id
             AND m.tool_name = 'quickbooks'
            WHERE i.job_id IS NULL
        ),
        residual AS (
            SELECT o.*
            FROM orphan_base o
            WHERE NOT EXISTS (
                SELECT 1
                FROM invoices i2
                WHERE i2.client_id = o.client_id
                  AND i2.issue_date = o.issue_date
                  AND i2.amount = o.amount
                  AND i2.job_id IS NOT NULL
            )
        ),
        residual_no_same_day AS (
            SELECT r.*
            FROM residual r
            WHERE NOT EXISTS (
                SELECT 1
                FROM jobs j
                WHERE j.client_id = r.client_id
                  AND j.status = 'completed'
                  AND (
                        COALESCE(NULLIF(j.completed_at, '')::date, j.scheduled_date::date) = r.issue_date::date
                     OR j.scheduled_date::date = r.issue_date::date
                  )
            )
        )
        SELECT
            r.invoice_id,
            r.client_id,
            r.issue_date,
            r.amount,
            r.qbo_invoice_id,
            COALESCE(nearby.nearby_completed_job_count_3d, 0) AS nearby_completed_job_count_3d,
            COALESCE(nearby.nearby_job_ids, '') AS nearby_job_ids
        FROM residual_no_same_day r
        LEFT JOIN LATERAL (
            SELECT
                COUNT(*)::int AS nearby_completed_job_count_3d,
                string_agg(
                    j.id,
                    ',' ORDER BY COALESCE(NULLIF(j.completed_at, ''), j.scheduled_date), j.id
                ) AS nearby_job_ids
            FROM jobs j
            WHERE j.client_id = r.client_id
              AND j.status = 'completed'
              AND ABS(
                    COALESCE(NULLIF(j.completed_at, '')::date, j.scheduled_date::date)
                    - r.issue_date::date
                  ) <= %s
        ) nearby ON TRUE
        ORDER BY r.issue_date DESC, r.invoice_id
        """,
        (nearby_window_days,),
    ).fetchall()
    return [dict(row) for row in rows]


def _classify_candidate(
    row: dict,
    source: str,
    reviewed_by: str,
    snapshot_date: str,
) -> dict:
    nearby_count = int(row.get("nearby_completed_job_count_3d") or 0)
    nearby_job_ids = row.get("nearby_job_ids") or ""
    qbo_invoice_id = row.get("qbo_invoice_id")

    if nearby_count > 0:
        lane = MANUAL_REVIEW
        reason_code = "MANUAL_REVIEW_NEARBY_JOB"
    elif not qbo_invoice_id:
        lane = AUTO_QUARANTINE
        reason_code = "NO_QBO_MAPPING"
    else:
        lane = AUTO_QUARANTINE
        reason_code = "NO_NEARBY_COMPLETED_JOB_3D"

    detail = (
        "Residual orphan with no same-day completed job; "
        f"nearby_completed_job_count_3d={nearby_count}"
    )
    if nearby_job_ids:
        detail += f"; nearby_job_ids={nearby_job_ids}"

    return {
        "invoice_id": row["invoice_id"],
        "client_id": row["client_id"],
        "issue_date": row["issue_date"],
        "amount": round(float(row["amount"]), 2),
        "qbo_invoice_id": qbo_invoice_id,
        "quarantine_lane": lane,
        "reason_code": reason_code,
        "nearby_completed_job_count_3d": nearby_count,
        "nearby_job_ids": nearby_job_ids,
        "reason_detail": detail,
        "source": source,
        "snapshot_date": snapshot_date,
        "reviewed_by": reviewed_by,
    }


def _summarize(rows: list[dict]) -> dict:
    summary = {
        "total": len(rows),
        "auto_quarantine": 0,
        "manual_review": 0,
        "no_qbo_mapping": 0,
        "by_issue_date": {},
    }
    for row in rows:
        lane = row["quarantine_lane"]
        summary[lane] += 1
        if row["reason_code"] == "NO_QBO_MAPPING":
            summary["no_qbo_mapping"] += 1
        day = row["issue_date"]
        day_summary = summary["by_issue_date"].setdefault(day, {"count": 0, "amount": 0.0})
        day_summary["count"] += 1
        day_summary["amount"] += float(row["amount"])
    return summary


def _assert_expected_counts(
    summary: dict,
    expected_total: int | None,
    expected_auto: int | None,
    expected_manual: int | None,
) -> None:
    mismatches = []
    if expected_total is not None and summary["total"] != expected_total:
        mismatches.append(f"total={summary['total']} (expected {expected_total})")
    if expected_auto is not None and summary["auto_quarantine"] != expected_auto:
        mismatches.append(
            f"auto_quarantine={summary['auto_quarantine']} (expected {expected_auto})"
        )
    if expected_manual is not None and summary["manual_review"] != expected_manual:
        mismatches.append(
            f"manual_review={summary['manual_review']} (expected {expected_manual})"
        )
    if mismatches:
        raise RuntimeError("Quarantine candidate counts changed: " + ", ".join(mismatches))


def _write_snapshot(rows: list[dict], handle: TextIO) -> None:
    writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
    writer.writeheader()
    for row in rows:
        writer.writerow({field: row.get(field) for field in CSV_FIELDS})


def _released_conflicts(db, invoice_ids: Iterable[str]) -> list[str]:
    ids = list(invoice_ids)
    if not ids:
        return []
    rows = db.execute(
        """
        SELECT invoice_id
        FROM invoice_quarantine
        WHERE invoice_id = ANY(%s)
          AND released_at IS NOT NULL
        ORDER BY invoice_id
        """,
        (ids,),
    ).fetchall()
    return [row["invoice_id"] for row in rows]


def _upsert_quarantine_rows(db, rows: list[dict]) -> None:
    db.executemany(
        """
        INSERT INTO invoice_quarantine (
            invoice_id,
            client_id,
            issue_date,
            amount,
            qbo_invoice_id,
            quarantine_lane,
            reason_code,
            reason_detail,
            source,
            snapshot_date,
            reviewed_at,
            reviewed_by
        )
        VALUES (
            %(invoice_id)s,
            %(client_id)s,
            %(issue_date)s,
            %(amount)s,
            %(qbo_invoice_id)s,
            %(quarantine_lane)s,
            %(reason_code)s,
            %(reason_detail)s,
            %(source)s,
            %(snapshot_date)s,
            CURRENT_TIMESTAMP,
            %(reviewed_by)s
        )
        ON CONFLICT (invoice_id) DO UPDATE
        SET client_id = EXCLUDED.client_id,
            issue_date = EXCLUDED.issue_date,
            amount = EXCLUDED.amount,
            qbo_invoice_id = EXCLUDED.qbo_invoice_id,
            quarantine_lane = EXCLUDED.quarantine_lane,
            reason_code = EXCLUDED.reason_code,
            reason_detail = EXCLUDED.reason_detail,
            source = EXCLUDED.source,
            snapshot_date = EXCLUDED.snapshot_date,
            reviewed_at = CURRENT_TIMESTAMP,
            reviewed_by = EXCLUDED.reviewed_by
        WHERE invoice_quarantine.released_at IS NULL
        """,
        rows,
    )


def run_quarantine(
    db,
    *,
    apply: bool,
    nearby_window_days: int,
    source: str,
    reviewed_by: str,
    expected_total: int | None,
    expected_auto: int | None,
    expected_manual: int | None,
) -> tuple[list[dict], dict]:
    snapshot_date = str(date.today())
    candidates = _fetch_residual_no_same_day_candidates(db, nearby_window_days)
    classified = [
        _classify_candidate(row, source, reviewed_by, snapshot_date)
        for row in candidates
    ]
    summary = _summarize(classified)
    _assert_expected_counts(summary, expected_total, expected_auto, expected_manual)

    logger.info(
        "Residual no-same-day orphan candidates: total=%d auto_quarantine=%d manual_review=%d no_qbo_mapping=%d",
        summary["total"],
        summary["auto_quarantine"],
        summary["manual_review"],
        summary["no_qbo_mapping"],
    )
    for day, stats in sorted(summary["by_issue_date"].items(), reverse=True):
        logger.info(
            "  %s: count=%d amount=$%s",
            day,
            stats["count"],
            f"{stats['amount']:,.2f}",
        )

    manual_rows = [row for row in classified if row["quarantine_lane"] == MANUAL_REVIEW]
    if manual_rows:
        logger.info("Manual-review holdouts:")
        for row in manual_rows:
            logger.info(
                "  %s client=%s issue_date=%s amount=$%.2f nearby_jobs=%s",
                row["invoice_id"],
                row["client_id"],
                row["issue_date"],
                float(row["amount"]),
                row["nearby_job_ids"] or "(none)",
            )

    if not apply:
        return classified, summary

    _ensure_invoice_quarantine_table(db)
    conflicts = _released_conflicts(db, [row["invoice_id"] for row in classified])
    if conflicts:
        raise RuntimeError(
            "Refusing to overwrite released quarantine rows: "
            + ", ".join(conflicts[:10])
            + (" ..." if len(conflicts) > 10 else "")
        )

    _upsert_quarantine_rows(db, classified)
    logger.info("Applied quarantine upsert for %d invoice(s)", len(classified))
    return classified, summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Write invoice_quarantine rows.")
    parser.add_argument(
        "--nearby-window-days",
        type=int,
        default=3,
        help="Completed-job lookaround window for manual-review holdouts.",
    )
    parser.add_argument(
        "--snapshot-path",
        default=None,
        help="Optional CSV output path. Use '-' to write the snapshot CSV to stdout.",
    )
    parser.add_argument("--source", default=DEFAULT_SOURCE)
    parser.add_argument("--reviewed-by", default=DEFAULT_REVIEWED_BY)
    parser.add_argument("--expected-total", type=int, default=None)
    parser.add_argument("--expected-auto", type=int, default=None)
    parser.add_argument("--expected-manual", type=int, default=None)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    )

    db = get_connection()
    try:
        with db:
            rows, _summary = run_quarantine(
                db,
                apply=args.apply,
                nearby_window_days=args.nearby_window_days,
                source=args.source,
                reviewed_by=args.reviewed_by,
                expected_total=args.expected_total,
                expected_auto=args.expected_auto,
                expected_manual=args.expected_manual,
            )
    finally:
        db.close()

    if args.snapshot_path:
        if args.snapshot_path == "-":
            _write_snapshot(rows, sys.stdout)
        else:
            output_path = Path(args.snapshot_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with output_path.open("w", newline="") as handle:
                _write_snapshot(rows, handle)
            logger.info("Wrote snapshot CSV to %s", output_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
