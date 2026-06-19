#!/usr/bin/env python3
"""Repair duplicate client rows created by the HubSpot intelligence sync.

This script targets a specific production artifact pattern:

1. A client row exists with no HubSpot mapping.
2. A lead row exists with the same email address.
3. The lead row holds the HubSpot mapping.

Those artifacts split into two safe remediation paths:

- Reassign the HubSpot mapping from the lead to the client when the client row
  already has downstream dependencies or other tool mappings.
- Delete the duplicate client row when it is dependency-free and has no tool
  mappings at all. The original lead row remains canonical.

Default mode is dry-run. Use --execute to apply the changes.
"""
from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env")

from database.connection import get_connection


@dataclass
class Candidate:
    client_id: str
    client_email: str
    lead_id: str
    hubspot_id: str
    has_jobs: bool
    has_recurring: bool
    has_invoices: bool
    has_payments: bool
    has_reviews: bool
    has_tasks: bool
    has_marketing: bool
    has_calendar_events: bool
    has_proposals: bool
    has_jobber: bool
    has_quickbooks: bool
    has_pipedrive: bool

    @property
    def has_dependencies(self) -> bool:
        return any(
            (
                self.has_jobs,
                self.has_recurring,
                self.has_invoices,
                self.has_payments,
                self.has_reviews,
                self.has_tasks,
                self.has_marketing,
                self.has_calendar_events,
                self.has_proposals,
                self.has_jobber,
                self.has_quickbooks,
                self.has_pipedrive,
            )
        )

    @property
    def action(self) -> str:
        return "reassign_hubspot_mapping" if self.has_dependencies else "delete_duplicate_client"


def _load_candidates(limit: int | None = None) -> list[Candidate]:
    conn = get_connection()
    try:
        sql = """
            WITH duplicate_clients AS (
                SELECT
                    c.id AS client_id,
                    c.email AS client_email,
                    l.id AS lead_id,
                    hm.tool_specific_id AS hubspot_id,
                    EXISTS (SELECT 1 FROM jobs j WHERE j.client_id = c.id) AS has_jobs,
                    EXISTS (
                        SELECT 1 FROM recurring_agreements r WHERE r.client_id = c.id
                    ) AS has_recurring,
                    EXISTS (SELECT 1 FROM invoices i WHERE i.client_id = c.id) AS has_invoices,
                    EXISTS (SELECT 1 FROM payments p WHERE p.client_id = c.id) AS has_payments,
                    EXISTS (SELECT 1 FROM reviews r WHERE r.client_id = c.id) AS has_reviews,
                    EXISTS (SELECT 1 FROM tasks t WHERE t.client_id = c.id) AS has_tasks,
                    EXISTS (
                        SELECT 1
                        FROM marketing_interactions mi
                        WHERE mi.client_id = c.id
                    ) AS has_marketing,
                    EXISTS (
                        SELECT 1
                        FROM calendar_events ce
                        WHERE ce.related_client_id = c.id
                    ) AS has_calendar_events,
                    EXISTS (
                        SELECT 1
                        FROM commercial_proposals cp
                        WHERE cp.client_id = c.id
                    ) AS has_proposals,
                    EXISTS (
                        SELECT 1 FROM cross_tool_mapping m
                        WHERE m.canonical_id = c.id AND m.tool_name = 'jobber'
                    ) AS has_jobber,
                    EXISTS (
                        SELECT 1 FROM cross_tool_mapping m
                        WHERE m.canonical_id = c.id
                          AND m.tool_name IN ('quickbooks', 'quickbooks_customer')
                    ) AS has_quickbooks,
                    EXISTS (
                        SELECT 1 FROM cross_tool_mapping m
                        WHERE m.canonical_id = c.id
                          AND m.tool_name IN ('pipedrive', 'pipedrive_person')
                    ) AS has_pipedrive
                FROM clients c
                JOIN leads l
                  ON l.email = c.email
                JOIN cross_tool_mapping hm
                  ON hm.canonical_id = l.id
                 AND hm.tool_name = 'hubspot'
                WHERE COALESCE(c.email, '') <> ''
                  AND NOT EXISTS (
                        SELECT 1
                        FROM cross_tool_mapping m
                        WHERE m.canonical_id = c.id
                          AND m.tool_name = 'hubspot'
                  )
            )
            SELECT *
            FROM duplicate_clients
            ORDER BY client_id
        """
        params = None
        if limit is not None:
            sql += " LIMIT %s"
            params = (limit,)

        rows = conn.execute(sql, params).fetchall()
        return [Candidate(**dict(row)) for row in rows]
    finally:
        conn.close()


def _reassign_hubspot_mapping(conn, candidate: Candidate) -> None:
    existing_client_mapping = conn.execute(
        """
        SELECT tool_specific_id
        FROM cross_tool_mapping
        WHERE canonical_id = %s AND tool_name = 'hubspot'
        """,
        (candidate.client_id,),
    ).fetchone()
    if existing_client_mapping is not None:
        raise RuntimeError(
            f"{candidate.client_id} already has HubSpot mapping "
            f"{existing_client_mapping['tool_specific_id']}"
        )

    updated = conn.execute(
        """
        UPDATE cross_tool_mapping
        SET canonical_id = %s,
            entity_type = 'CLIENT',
            synced_at = CURRENT_TIMESTAMP
        WHERE canonical_id = %s
          AND tool_name = 'hubspot'
          AND tool_specific_id = %s
        """,
        (candidate.client_id, candidate.lead_id, candidate.hubspot_id),
    ).rowcount
    if updated != 1:
        raise RuntimeError(
            f"Expected to move one HubSpot mapping for {candidate.client_id}, moved {updated}"
        )


def _delete_duplicate_client(conn, candidate: Candidate) -> None:
    if candidate.has_dependencies:
        raise RuntimeError(
            f"{candidate.client_id} has downstream dependencies and cannot be deleted safely"
        )

    updated = conn.execute(
        "DELETE FROM clients WHERE id = %s",
        (candidate.client_id,),
    ).rowcount
    if updated != 1:
        raise RuntimeError(f"Expected to delete one client row for {candidate.client_id}, deleted {updated}")


def remediate(candidates: list[Candidate], execute: bool) -> dict[str, int]:
    stats = {
        "candidates": len(candidates),
        "would_reassign": 0,
        "would_delete": 0,
        "reassigned": 0,
        "deleted": 0,
        "failed": 0,
    }

    conn = get_connection()
    try:
        for candidate in candidates:
            if candidate.action == "reassign_hubspot_mapping":
                stats["would_reassign"] += 1
            else:
                stats["would_delete"] += 1

            if not execute:
                continue

            try:
                with conn:
                    if candidate.action == "reassign_hubspot_mapping":
                        _reassign_hubspot_mapping(conn, candidate)
                        stats["reassigned"] += 1
                    else:
                        _delete_duplicate_client(conn, candidate)
                        stats["deleted"] += 1
            except Exception:
                stats["failed"] += 1
                raise
    finally:
        conn.close()

    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Apply the remediation. Omit for dry-run mode.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional cap on candidate rows.",
    )
    args = parser.parse_args()

    candidates = _load_candidates(limit=args.limit)
    if not candidates:
        print("No duplicate HubSpot-sync client artifacts found.")
        return 0

    stats = remediate(candidates, execute=args.execute)

    mode = "EXECUTE" if args.execute else "DRY RUN"
    print(f"{mode}: {stats['candidates']} candidate client artifact(s)")
    for candidate in candidates:
        dep_bits = []
        if candidate.has_jobs:
            dep_bits.append("jobs")
        if candidate.has_recurring:
            dep_bits.append("recurring")
        if candidate.has_invoices:
            dep_bits.append("invoices")
        if candidate.has_payments:
            dep_bits.append("payments")
        if candidate.has_reviews:
            dep_bits.append("reviews")
        if candidate.has_tasks:
            dep_bits.append("tasks")
        if candidate.has_marketing:
            dep_bits.append("marketing")
        if candidate.has_calendar_events:
            dep_bits.append("calendar")
        if candidate.has_proposals:
            dep_bits.append("proposals")
        if candidate.has_jobber:
            dep_bits.append("jobber")
        if candidate.has_quickbooks:
            dep_bits.append("quickbooks")
        if candidate.has_pipedrive:
            dep_bits.append("pipedrive")
        dep_label = ",".join(dep_bits) if dep_bits else "none"
        print(
            f"{candidate.action.upper():26} "
            f"{candidate.client_id} <- {candidate.lead_id} "
            f"(email={candidate.client_email}, deps={dep_label}, hubspot={candidate.hubspot_id})"
        )

    print(
        "\nSummary: "
        f"would_reassign={stats['would_reassign']}, "
        f"would_delete={stats['would_delete']}, "
        f"reassigned={stats['reassigned']}, "
        f"deleted={stats['deleted']}, "
        f"failed={stats['failed']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
