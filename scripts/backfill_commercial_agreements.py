"""Backfill recurring_agreements rows for active commercial clients.

Reads:
  - clients (client_type='commercial', status='active')
  - seeding.generators.gen_clients._COMMERCIAL_CLIENTS for authoritative schedule
  - clients.notes as fallback schedule hint
  - get_commercial_per_visit_rate for price_per_visit

Writes:
  - recurring_agreements rows, one per (client, service_type_id)
    (nightly_plus_saturday clients get two rows: commercial-nightly Mon-Fri
    plus deep-clean Saturday)

Idempotency: re-running is safe. An existing active agreement for the same
(client_id, service_type_id) pair causes the row to be skipped.

Usage:
    python -m scripts.backfill_commercial_agreements --dry-run
    python -m scripts.backfill_commercial_agreements --execute
"""
from __future__ import annotations

import argparse
import logging
from datetime import date
from typing import Optional

from database.connection import get_connection, get_column_names
from database.mappings import generate_id

logger = logging.getLogger(__name__)

# Schedule key → list of (service_type_id, day_of_week csv) agreements.
# Keys cover both the seed-data strings (underscored, e.g. "3x_weekly") and
# the _commercial_scope output strings (space-separated, e.g. "3x weekly").
_SCHEDULE_TO_AGREEMENTS: dict[str, list[tuple[str, str]]] = {
    "nightly":              [("commercial-nightly", "monday,tuesday,wednesday,thursday,friday")],
    "nightly_weekdays":     [("commercial-nightly", "monday,tuesday,wednesday,thursday,friday")],
    "5x weekly":            [("commercial-nightly", "monday,tuesday,wednesday,thursday,friday")],
    "5x_weekly":            [("commercial-nightly", "monday,tuesday,wednesday,thursday,friday")],
    "daily":                [("commercial-nightly", "monday,tuesday,wednesday,thursday,friday,saturday")],
    "3x weekly":            [("commercial-nightly", "monday,wednesday,friday")],
    "3x_weekly":            [("commercial-nightly", "monday,wednesday,friday")],
    "2x weekly":            [("commercial-nightly", "tuesday,thursday")],
    "2x_weekly":            [("commercial-nightly", "tuesday,thursday")],
    "nightly_plus_saturday": [
        ("commercial-nightly", "monday,tuesday,wednesday,thursday,friday"),
        ("deep-clean",         "saturday"),
    ],
}

_DEFAULT_AGREEMENTS: list[tuple[str, str]] = [
    ("commercial-nightly", "monday,wednesday,friday"),
]


def _seed_schedule_by_company() -> dict[str, str]:
    """Return a {company_name: schedule} lookup from gen_clients seed data."""
    try:
        from seeding.generators.gen_clients import _COMMERCIAL_CLIENTS
    except Exception as e:
        logger.warning("could not import _COMMERCIAL_CLIENTS; falling back to notes only (%s)", e)
        return {}
    return {c["company_name"]: c.get("schedule", "") for c in _COMMERCIAL_CLIENTS}


def _per_visit_rate(client_id: str, service_type_id: str) -> Optional[float]:
    try:
        from seeding.generators.gen_clients import get_commercial_per_visit_rate
        return get_commercial_per_visit_rate(
            client_id, service_type_id=service_type_id
        )
    except Exception as e:
        logger.warning(
            "could not resolve per-visit rate for %s (%s): %s",
            client_id, service_type_id, e,
        )
        return None


def _infer_schedule(
    company_name: Optional[str],
    notes: Optional[str],
    seed_map: dict[str, str],
) -> str:
    """Return a schedule key usable as a _SCHEDULE_TO_AGREEMENTS lookup.

    Seed data (authoritative) wins over notes inference.
    """
    seed = seed_map.get(company_name or "")
    if seed:
        return seed
    from simulation.generators.operations import _commercial_scope
    return _commercial_scope(notes)


def _ensure_client_type_column(conn) -> None:
    cols = get_column_names(conn, "recurring_agreements")
    if "client_type" not in cols:
        conn.execute(
            "ALTER TABLE recurring_agreements ADD COLUMN client_type "
            "TEXT DEFAULT 'residential'"
        )
        conn.commit()


def _existing_active_agreements(conn, client_id: str) -> set[str]:
    rows = conn.execute(
        """
        SELECT service_type_id FROM recurring_agreements
        WHERE client_id = %s AND status = 'active'
        """,
        (client_id,),
    ).fetchall()
    return {r["service_type_id"] for r in rows}


def backfill(dry_run: bool) -> dict:
    conn = get_connection()
    created: list[dict] = []
    skipped: list[dict] = []
    failed: list[dict] = []

    try:
        _ensure_client_type_column(conn)
        seed_map = _seed_schedule_by_company()
        today = date.today().isoformat()

        clients = conn.execute(
            """
            SELECT id, company_name, notes
            FROM clients
            WHERE client_type = 'commercial' AND status = 'active'
            ORDER BY company_name
            """
        ).fetchall()
        clients = [dict(c) for c in clients]

        for client in clients:
            schedule_key = _infer_schedule(
                client.get("company_name"), client.get("notes"), seed_map
            )
            agreements = _SCHEDULE_TO_AGREEMENTS.get(schedule_key, _DEFAULT_AGREEMENTS)
            existing = _existing_active_agreements(conn, client["id"])

            for service_type_id, day_of_week in agreements:
                if service_type_id in existing:
                    skipped.append({
                        "client_id": client["id"],
                        "company_name": client.get("company_name"),
                        "service_type_id": service_type_id,
                        "reason": "active agreement already exists",
                    })
                    continue

                price = _per_visit_rate(client["id"], service_type_id)
                if price is None:
                    failed.append({
                        "client_id": client["id"],
                        "company_name": client.get("company_name"),
                        "service_type_id": service_type_id,
                        "reason": "could not resolve per-visit rate",
                    })
                    continue

                record = {
                    "client_id": client["id"],
                    "company_name": client.get("company_name"),
                    "service_type_id": service_type_id,
                    "price_per_visit": price,
                    "day_of_week": day_of_week,
                    "schedule_key": schedule_key,
                }

                if dry_run:
                    created.append({**record, "id": "DRY-RUN"})
                    continue

                try:
                    agreement_id = generate_id("RECUR")
                    conn.execute(
                        """
                        INSERT INTO recurring_agreements
                        (id, client_id, service_type_id, crew_id, frequency,
                         price_per_visit, start_date, status, day_of_week,
                         client_type)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        (
                            agreement_id, client["id"], service_type_id,
                            "crew-d", "weekly", price, today,
                            "active", day_of_week, "commercial",
                        ),
                    )
                    created.append({**record, "id": agreement_id})
                except Exception as e:
                    logger.exception(
                        "insert failed for %s / %s",
                        client["id"], service_type_id,
                    )
                    failed.append({**record, "reason": str(e)})

        if not dry_run:
            conn.commit()
        return {
            "dry_run": dry_run,
            "created": created,
            "skipped": skipped,
            "failed": failed,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true")
    group.add_argument("--execute", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    result = backfill(dry_run=args.dry_run)

    print(
        f"created={len(result['created'])} "
        f"skipped={len(result['skipped'])} "
        f"failed={len(result['failed'])} "
        f"dry_run={result['dry_run']}"
    )
    for row in result["created"]:
        print(
            f"  created {row['id']} {row['client_id']} {row['service_type_id']} "
            f"({row['day_of_week']}) @ ${row['price_per_visit']:.2f}"
        )
    for row in result["failed"]:
        print(
            f"  FAILED {row['client_id']} {row['service_type_id']}: {row['reason']}"
        )
    return 0 if not result["failed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
