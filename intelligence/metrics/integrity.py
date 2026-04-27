"""
intelligence/metrics/integrity.py

Data-integrity metrics surfaced in every daily briefing so silent drift
between our source-of-truth tables and their derived records cannot sit
undetected the way the 2026-04-09 orphan-invoice spike did.

Three counts are reported:
  1. Orphan invoices       — invoices.job_id IS NULL
  2. Payments missing link — payments whose invoice_id points at a row that
                             no longer exists in invoices (defensive: the
                             schema enforces this via FK, but a future
                             migration or manual write could violate it).
  3. Stale completed jobs  — jobs.status='completed' older than 24h that
                             have no matching invoices.job_id. Track B's
                             automation runner creates these invoices within
                             5 minutes under normal operation; anything
                             older than 24h means the pipeline stalled.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from database.connection import table_exists


# Anything over this many "still missing an invoice" is treated as critical.
# The automation runner should keep this at 0; a handful may appear while
# jobs complete late in the day and the 5-min sync cycle catches up.
_STALE_JOB_CRITICAL_THRESHOLD = 5
_ORPHAN_INVOICE_CRITICAL_THRESHOLD = 50

# SQL fragment excluding invoices that were intentionally quarantined by
# scripts/quarantine_residual_orphan_invoices.py. A quarantined row is an
# operator-classified orphan that we have deliberately decided not to
# relink — it should not drive the alert. Released rows (released_at IS
# NOT NULL) flow back into the count so a mistaken quarantine resurfaces.
_QUARANTINE_EXCLUSION = """
    AND NOT EXISTS (
        SELECT 1
        FROM invoice_quarantine q
        WHERE q.invoice_id = invoices.id
          AND q.released_at IS NULL
    )
"""


def compute(db, briefing_date: str) -> dict:
    """Return integrity metrics for the briefing date.

    Args:
        db: intelligence.database.Connection (Postgres wrapper).
        briefing_date: ISO date ("YYYY-MM-DD").

    Returns:
        dict with:
          - orphan_invoices: {count, amount, sample: [{id, client_id, amount, issue_date}]}
          - payments_missing_invoice_link: {count, amount, sample: [...]}
          - stale_completed_jobs: {count, oldest_age_hours, sample: [...]}
          - alerts: list[str]
    """
    today = date.fromisoformat(briefing_date)
    cutoff_completed_at = (datetime.combine(today, datetime.min.time(),
                                            tzinfo=timezone.utc)
                           - timedelta(hours=24))

    # ────────────────────────────────────────────────────────────────
    # 1. Orphan invoices — job_id IS NULL, minus operator-classified
    #    quarantine rows so the alert reflects only the unaddressed
    #    bucket. The quarantine table is created by
    #    scripts/quarantine_residual_orphan_invoices.py, so environments
    #    that haven't run it (local dev, fresh test DBs) simply fall
    #    back to the legacy count.
    # ────────────────────────────────────────────────────────────────
    quarantine_filter = _QUARANTINE_EXCLUSION if table_exists(db, "invoice_quarantine") else ""

    orphan_row = db.execute(
        f"""
        SELECT COUNT(*) AS cnt, COALESCE(SUM(amount), 0.0) AS total
        FROM invoices
        WHERE job_id IS NULL
          {quarantine_filter}
        """
    ).fetchone()
    orphan_count = orphan_row["cnt"] or 0
    orphan_amount = float(orphan_row["total"] or 0.0)

    orphan_sample = [
        dict(r) for r in db.execute(
            f"""
            SELECT id, client_id, amount, issue_date
            FROM invoices
            WHERE job_id IS NULL
              {quarantine_filter}
            ORDER BY issue_date DESC, id DESC
            LIMIT 5
            """
        ).fetchall()
    ]

    quarantined_count = 0
    quarantined_amount = 0.0
    if quarantine_filter:
        quarantined_row = db.execute(
            """
            SELECT COUNT(*) AS cnt, COALESCE(SUM(i.amount), 0.0) AS total
            FROM invoice_quarantine q
            JOIN invoices i ON i.id = q.invoice_id
            WHERE q.released_at IS NULL
              AND i.job_id IS NULL
            """
        ).fetchone()
        quarantined_count = quarantined_row["cnt"] or 0
        quarantined_amount = float(quarantined_row["total"] or 0.0)

    # ────────────────────────────────────────────────────────────────
    # 2. Payments whose invoice_id no longer exists in invoices.
    #    The FK should prevent this, but we audit anyway — a future
    #    migration that drops/recreates FKs (or a manual patch) could
    #    leave dangling references undetected without this check.
    # ────────────────────────────────────────────────────────────────
    dangling_row = db.execute(
        """
        SELECT COUNT(*) AS cnt, COALESCE(SUM(p.amount), 0.0) AS total
        FROM payments p
        LEFT JOIN invoices i ON i.id = p.invoice_id
        WHERE i.id IS NULL
        """
    ).fetchone()
    dangling_count = dangling_row["cnt"] or 0
    dangling_amount = float(dangling_row["total"] or 0.0)

    dangling_sample = [
        dict(r) for r in db.execute(
            """
            SELECT p.id, p.invoice_id, p.client_id, p.amount, p.payment_date
            FROM payments p
            LEFT JOIN invoices i ON i.id = p.invoice_id
            WHERE i.id IS NULL
            ORDER BY p.payment_date DESC, p.id DESC
            LIMIT 5
            """
        ).fetchall()
    ]

    # ────────────────────────────────────────────────────────────────
    # 3. Completed jobs older than 24h with no linked invoice.
    #    completed_at is stored as ISO text; cast to timestamp for
    #    comparison and fall back to scheduled_date if completed_at is
    #    missing (pre-Phase-3 seed data may not have it).
    # ────────────────────────────────────────────────────────────────
    stale_row = db.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM jobs j
        LEFT JOIN invoices i ON i.job_id = j.id
        WHERE j.status = 'completed'
          AND i.id IS NULL
          AND COALESCE(j.completed_at, j.scheduled_date)::timestamp < %s
        """,
        (cutoff_completed_at.replace(tzinfo=None),),
    ).fetchone()
    stale_count = stale_row["cnt"] or 0

    stale_sample = [
        dict(r) for r in db.execute(
            """
            SELECT j.id, j.client_id, j.crew_id,
                   COALESCE(j.completed_at, j.scheduled_date) AS completed_at,
                   j.scheduled_date
            FROM jobs j
            LEFT JOIN invoices i ON i.job_id = j.id
            WHERE j.status = 'completed'
              AND i.id IS NULL
              AND COALESCE(j.completed_at, j.scheduled_date)::timestamp < %s
            ORDER BY COALESCE(j.completed_at, j.scheduled_date) ASC
            LIMIT 5
            """,
            (cutoff_completed_at.replace(tzinfo=None),),
        ).fetchall()
    ]

    oldest_age_hours = 0.0
    if stale_sample:
        oldest = stale_sample[0]["completed_at"]
        try:
            oldest_dt = datetime.fromisoformat(str(oldest).replace("Z", "+00:00"))
            if oldest_dt.tzinfo is None:
                oldest_dt = oldest_dt.replace(tzinfo=timezone.utc)
            oldest_age_hours = round(
                (datetime.now(timezone.utc) - oldest_dt).total_seconds() / 3600.0,
                1,
            )
        except (ValueError, TypeError):
            oldest_age_hours = 0.0

    # ────────────────────────────────────────────────────────────────
    # Alerts — route to #operations via _ALERT_CHANNEL_MAP.
    # "critical" appears in the text when any threshold is breached so
    # intelligence.runner._is_critical picks it up.
    # ────────────────────────────────────────────────────────────────
    alerts: list[str] = []

    if stale_count > 0:
        severity = "critical" if stale_count >= _STALE_JOB_CRITICAL_THRESHOLD else "warning"
        alerts.append(
            f"{stale_count} completed job(s) have been sitting without an invoice "
            f"for 24h+ (oldest: {oldest_age_hours:.1f}h) — the job→invoice "
            f"automation may have stalled [{severity}]"
        )

    if orphan_count > 0:
        severity = "critical" if orphan_count >= _ORPHAN_INVOICE_CRITICAL_THRESHOLD else "warning"
        quarantine_note = (
            f" (excludes {quarantined_count} quarantined, "
            f"${quarantined_amount:,.0f})"
            if quarantined_count > 0
            else ""
        )
        alerts.append(
            f"{orphan_count} unaddressed invoice(s) totalling ${orphan_amount:,.0f} "
            f"are not linked to any job (job_id IS NULL) — these are excluded "
            f"from booked revenue and need Track B remediation"
            f"{quarantine_note} [{severity}]"
        )

    if dangling_count > 0:
        alerts.append(
            f"{dangling_count} payment(s) totalling ${dangling_amount:,.0f} "
            f"reference an invoice_id that no longer exists — database "
            f"integrity violation, investigate immediately [critical]"
        )

    return {
        "orphan_invoices": {
            "count": orphan_count,
            "amount": round(orphan_amount, 2),
            "sample": orphan_sample,
            "quarantined_count": quarantined_count,
            "quarantined_amount": round(quarantined_amount, 2),
        },
        "payments_missing_invoice_link": {
            "count": dangling_count,
            "amount": round(dangling_amount, 2),
            "sample": dangling_sample,
        },
        "stale_completed_jobs": {
            "count": stale_count,
            "oldest_age_hours": oldest_age_hours,
            "sample": stale_sample,
        },
        "alerts": alerts,
    }
