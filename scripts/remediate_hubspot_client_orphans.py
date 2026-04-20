#!/usr/bin/env python3
"""Repair client rows that were created by the HubSpot sync but never got
a ``cross_tool_mapping`` entry, and where no sibling lead row holds the
HubSpot mapping either.

This is the sibling variant of ``remediate_hubspot_client_sync_artifacts.py``:

- ``remediate_hubspot_client_sync_artifacts.py`` covers clients whose HubSpot
  mapping landed on a duplicate *lead* row (move mapping / delete duplicate).
- This script covers clients with **no** tool mapping on either the client or
  a sibling lead. Those typically come from ``sync_hubspot.py::_upsert_contact``
  cases where the client ``INSERT`` committed but ``register_mapping`` raised
  before it could write the mapping row (now fixed: the two operations share
  one transaction, but historical artifacts remain).

Remediation paths per candidate:

1. ``relink_hubspot`` - HubSpot has exactly one contact for the client's email.
   Register the existing HubSpot contact ID as the client's HubSpot mapping.
2. ``delete_orphan_client`` - HubSpot has no matching contact AND the client
   row has no downstream dependencies. Safe to delete.
3. ``manual_review`` - HubSpot returns multiple matches, or has none while the
   client still has downstream dependencies. Print details and take no action.

Default mode is dry-run. Use ``--execute`` to apply relinks and deletes.
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env")

from auth import get_client  # noqa: E402
from database.connection import get_connection  # noqa: E402
from database.mappings import register_mapping_on_conn  # noqa: E402
from seeding.utils.throttler import HUBSPOT as HUBSPOT_THROTTLER  # noqa: E402


@dataclass
class Candidate:
    client_id: str
    client_email: str
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
    hubspot_matches: list[str] = field(default_factory=list)
    action: str = "manual_review"
    note: str = ""

    @property
    def has_dependencies(self) -> bool:
        return any(
            (
                self.has_jobs, self.has_recurring, self.has_invoices,
                self.has_payments, self.has_reviews, self.has_tasks,
                self.has_marketing, self.has_calendar_events, self.has_proposals,
                self.has_jobber, self.has_quickbooks, self.has_pipedrive,
            )
        )

    @property
    def dep_label(self) -> str:
        bits = []
        if self.has_jobs: bits.append("jobs")
        if self.has_recurring: bits.append("recurring")
        if self.has_invoices: bits.append("invoices")
        if self.has_payments: bits.append("payments")
        if self.has_reviews: bits.append("reviews")
        if self.has_tasks: bits.append("tasks")
        if self.has_marketing: bits.append("marketing")
        if self.has_calendar_events: bits.append("calendar")
        if self.has_proposals: bits.append("proposals")
        if self.has_jobber: bits.append("jobber")
        if self.has_quickbooks: bits.append("quickbooks")
        if self.has_pipedrive: bits.append("pipedrive")
        return ",".join(bits) if bits else "none"


_CANDIDATE_SQL = """
    SELECT
        c.id AS client_id,
        c.email AS client_email,
        EXISTS (SELECT 1 FROM jobs j WHERE j.client_id = c.id) AS has_jobs,
        EXISTS (SELECT 1 FROM recurring_agreements r WHERE r.client_id = c.id) AS has_recurring,
        EXISTS (SELECT 1 FROM invoices i WHERE i.client_id = c.id) AS has_invoices,
        EXISTS (SELECT 1 FROM payments p WHERE p.client_id = c.id) AS has_payments,
        EXISTS (SELECT 1 FROM reviews r WHERE r.client_id = c.id) AS has_reviews,
        EXISTS (SELECT 1 FROM tasks t WHERE t.client_id = c.id) AS has_tasks,
        EXISTS (SELECT 1 FROM marketing_interactions mi WHERE mi.client_id = c.id) AS has_marketing,
        EXISTS (SELECT 1 FROM calendar_events ce WHERE ce.related_client_id = c.id) AS has_calendar_events,
        EXISTS (SELECT 1 FROM commercial_proposals cp WHERE cp.client_id = c.id) AS has_proposals,
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
    WHERE NOT EXISTS (
        SELECT 1 FROM cross_tool_mapping m WHERE m.canonical_id = c.id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM leads l
        JOIN cross_tool_mapping hm
          ON hm.canonical_id = l.id
         AND hm.tool_name = 'hubspot'
        WHERE COALESCE(c.email, '') <> ''
          AND l.email = c.email
    )
    ORDER BY c.id
"""


def _load_candidates(limit: Optional[int]) -> list[Candidate]:
    conn = get_connection()
    try:
        sql = _CANDIDATE_SQL
        params: Optional[tuple] = None
        if limit is not None:
            sql += " LIMIT %s"
            params = (limit,)
        rows = conn.execute(sql, params).fetchall()
        return [Candidate(**dict(row)) for row in rows]
    finally:
        conn.close()


def _hubspot_matches_for(email: str) -> list[str]:
    """Return HubSpot contact IDs matching ``email`` (exact, EQ search)."""
    if not email:
        return []

    from hubspot.crm.contacts import PublicObjectSearchRequest  # type: ignore

    HUBSPOT_THROTTLER.wait()
    client = get_client("hubspot")
    req = PublicObjectSearchRequest(
        filter_groups=[{
            "filters": [{
                "propertyName": "email",
                "operator": "EQ",
                "value": email,
            }]
        }],
        properties=["email", "hs_object_id"],
        limit=5,
    )
    resp = client.crm.contacts.search_api.do_search(public_object_search_request=req)
    return [str(c.id) for c in (resp.results or [])]


def _classify(candidate: Candidate) -> None:
    if not candidate.client_email:
        candidate.action = "manual_review"
        candidate.note = "client has no email; cannot resolve via HubSpot"
        return

    try:
        matches = _hubspot_matches_for(candidate.client_email)
    except Exception as exc:
        candidate.action = "manual_review"
        candidate.note = f"HubSpot lookup failed: {exc}"
        return

    candidate.hubspot_matches = matches

    if len(matches) == 1:
        candidate.action = "relink_hubspot"
        candidate.note = f"single HubSpot contact {matches[0]}"
    elif len(matches) == 0 and not candidate.has_dependencies:
        candidate.action = "delete_orphan_client"
        candidate.note = "no HubSpot contact; client row has no downstream dependencies"
    elif len(matches) == 0:
        candidate.action = "manual_review"
        candidate.note = f"no HubSpot contact but client has deps ({candidate.dep_label})"
    else:
        candidate.action = "manual_review"
        candidate.note = f"{len(matches)} HubSpot contacts match email: {','.join(matches)}"


def _relink(conn, candidate: Candidate) -> None:
    hs_id = candidate.hubspot_matches[0]
    # register_mapping_on_conn raises ValueError if hs_id is already pointed
    # at a different canonical_id. Surface that as a failure rather than
    # silently overwriting a valid mapping elsewhere.
    with conn:
        register_mapping_on_conn(conn, candidate.client_id, "hubspot", hs_id)


def _delete_orphan(conn, candidate: Candidate) -> None:
    if candidate.has_dependencies:
        raise RuntimeError(
            f"{candidate.client_id} has downstream dependencies "
            f"({candidate.dep_label}); refusing to delete"
        )
    with conn:
        updated = conn.execute(
            "DELETE FROM clients WHERE id = %s",
            (candidate.client_id,),
        ).rowcount
    if updated != 1:
        raise RuntimeError(
            f"Expected to delete one client row for {candidate.client_id}, deleted {updated}"
        )


def remediate(candidates: list[Candidate], execute: bool) -> dict[str, int]:
    stats = {
        "candidates": len(candidates),
        "would_relink": 0,
        "would_delete": 0,
        "would_review": 0,
        "relinked": 0,
        "deleted": 0,
        "reviewed": 0,
        "failed": 0,
    }

    conn = get_connection()
    try:
        for candidate in candidates:
            _classify(candidate)

            if candidate.action == "relink_hubspot":
                stats["would_relink"] += 1
            elif candidate.action == "delete_orphan_client":
                stats["would_delete"] += 1
            else:
                stats["would_review"] += 1

            if not execute:
                continue

            try:
                if candidate.action == "relink_hubspot":
                    _relink(conn, candidate)
                    stats["relinked"] += 1
                elif candidate.action == "delete_orphan_client":
                    _delete_orphan(conn, candidate)
                    stats["deleted"] += 1
                else:
                    stats["reviewed"] += 1
            except Exception as exc:
                stats["failed"] += 1
                candidate.note = f"{candidate.note} | execute failed: {exc}"
    finally:
        conn.close()

    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Apply relinks and deletes. Omit for dry-run mode.",
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
        print("No orphan-client HubSpot artifacts found.")
        return 0

    stats = remediate(candidates, execute=args.execute)

    mode = "EXECUTE" if args.execute else "DRY RUN"
    print(f"{mode}: {stats['candidates']} candidate client artifact(s)")
    for candidate in candidates:
        print(
            f"{candidate.action.upper():22} "
            f"{candidate.client_id} "
            f"(email={candidate.client_email or '<none>'}, "
            f"deps={candidate.dep_label}) -- {candidate.note}"
        )

    print(
        "\nSummary: "
        f"would_relink={stats['would_relink']}, "
        f"would_delete={stats['would_delete']}, "
        f"would_review={stats['would_review']}, "
        f"relinked={stats['relinked']}, "
        f"deleted={stats['deleted']}, "
        f"reviewed={stats['reviewed']}, "
        f"failed={stats['failed']}"
    )
    return 1 if stats["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
