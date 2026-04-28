"""
Repair SS-PROP-XXXX rows in cross_tool_mapping that were registered for
non-Pipedrive tools (jobber, hubspot, mailchimp, quickbooks, ...).

Background
----------
new_client_onboarding._get_or_create_canonical_id had a fallback bug: when the
won Pipedrive deal mapped to a SS-PROP-* and the proposal had neither client_id
nor lead_id linked, the function returned the proposal id, and the automation
then registered every downstream tool mapping under SS-PROP-*. This breaks the
Jobber syncer's INSERT INTO jobs (FK violation: jobs.client_id → clients.id).

This script repairs the existing damage. The code-side fix is already in
new_client_onboarding (mint a SS-CLIENT instead of falling through).

Two repair paths
----------------
1. Proposal already linked to a client (cp.client_id IS NOT NULL): the
   non-Pipedrive mappings on SS-PROP-* are leftover duplicates. For each tool,
   if the linked SS-CLIENT-* already has a mapping for that tool, DELETE the
   SS-PROP-* row. Otherwise, repoint the SS-PROP-* row to the SS-CLIENT-*.

2. Proposal status='won' but no linked client (the active bug case): mint a
   fresh SS-CLIENT-XXXX, insert a clients row using a name parsed from the
   proposal title and a placeholder email keyed on the proposal id (so we
   don't collide with another email), set cp.client_id, then run path (1).

Proposals with status != 'won' and no client_id are left alone; they should
not own non-Pipedrive mappings at all but cleaning them up is out of scope.

Usage
-----
    python scripts/repair_ss_prop_mappings.py --dry-run
    python scripts/repair_ss_prop_mappings.py --apply
    python scripts/repair_ss_prop_mappings.py --apply --proposal SS-PROP-0147
"""
from __future__ import annotations

import argparse
import re
import sys
from typing import Optional

from database.connection import get_connection


# Tools whose mappings should live on SS-CLIENT-*, not SS-PROP-*. Pipedrive is
# excluded because the Pipedrive deal IS the proposal; that mapping is correct.
_NON_PROP_TOOLS = (
    "jobber",
    "jobber_property",
    "hubspot",
    "mailchimp",
    "quickbooks",
    "quickbooks_customer",
)


def _next_client_canonical(conn, reserved: set[str]) -> str:
    """Mint the next sequential SS-CLIENT-NNNN, looking at both clients and
    cross_tool_mapping (mirrors new_client_onboarding._get_or_create_canonical_id).

    `reserved` is a caller-owned set of IDs already minted in this run that
    have not yet been committed/visible to subsequent SELECTs (e.g. dry-run
    mode). Pass an empty set if not needed."""
    row_c = conn.execute(
        "SELECT id FROM clients WHERE id LIKE 'SS-CLIENT-%' "
        "ORDER BY id DESC LIMIT 1"
    ).fetchone()
    row_m = conn.execute(
        "SELECT canonical_id FROM cross_tool_mapping "
        "WHERE canonical_id LIKE 'SS-CLIENT-%' ORDER BY canonical_id DESC LIMIT 1"
    ).fetchone()

    candidates = []
    if row_c:
        candidates.append(row_c["id"])
    if row_m:
        candidates.append(row_m["canonical_id"])
    candidates.extend(reserved)

    next_n = (int(max(candidates).split("-")[-1]) + 1) if candidates else 1
    return f"SS-CLIENT-{next_n:04d}"


_NAME_FROM_TITLE = re.compile(r"^\s*([^—\-]+?)\s*[—\-]")


def _name_from_proposal(title: Optional[str]) -> tuple[str, str]:
    """Best-effort parse a 'Firstname Lastname — Qualified Lead (...)' style
    proposal title. Returns ("", "") on failure."""
    if not title:
        return "", ""
    match = _NAME_FROM_TITLE.match(title)
    if not match:
        return "", ""
    parts = match.group(1).strip().split(" ", 1)
    first = parts[0] if parts else ""
    last = parts[1] if len(parts) > 1 else ""
    return first, last


def _placeholder_email(proposal_id: str) -> str:
    """Stable, unique email for a recovered client. Real email can be
    backfilled later from HubSpot if needed."""
    return f"recovered+{proposal_id.lower()}@orphan.invalid"


def _mint_client_for_proposal(
    conn, proposal_id: str, dry_run: bool, reserved: set[str]
) -> str:
    """Allocate SS-CLIENT-XXXX for a won proposal that has no client_id /
    lead_id, insert the clients row, and link cp.client_id."""
    row = conn.execute(
        "SELECT id, title, status, client_id, lead_id "
        "FROM commercial_proposals WHERE id = %s",
        (proposal_id,),
    ).fetchone()
    if row is None:
        raise RuntimeError(f"{proposal_id}: proposal row not found")
    if row["client_id"]:
        return row["client_id"]
    if row["lead_id"]:
        raise RuntimeError(
            f"{proposal_id}: proposal has lead_id={row['lead_id']}, "
            f"this script only handles fully-orphaned proposals; "
            f"resolve via new_client_onboarding._promote_lead_to_client first"
        )
    if row["status"] != "won":
        raise RuntimeError(
            f"{proposal_id}: status={row['status']!r}, refusing to mint a "
            f"client for a non-won proposal"
        )

    first, last = _name_from_proposal(row["title"])
    email = _placeholder_email(proposal_id)
    new_client_id = _next_client_canonical(conn, reserved)
    reserved.add(new_client_id)

    if dry_run:
        print(
            f"  [dry] would INSERT clients id={new_client_id} "
            f"first={first!r} last={last!r} email={email!r}"
        )
        print(f"  [dry] would UPDATE commercial_proposals SET client_id={new_client_id} WHERE id={proposal_id}")
        return new_client_id

    conn.execute(
        """
        INSERT INTO clients (id, client_type, first_name, last_name, email, status)
        VALUES (%s, 'residential', %s, %s, %s, 'active')
        ON CONFLICT (id) DO NOTHING
        """,
        (new_client_id, first, last, email),
    )
    conn.execute(
        "UPDATE commercial_proposals SET client_id = %s WHERE id = %s",
        (new_client_id, proposal_id),
    )
    print(
        f"  minted {new_client_id} for orphan {proposal_id} "
        f"({first} {last}, {email})"
    )
    return new_client_id


def _repair_mappings(
    conn, proposal_id: str, client_id: str, dry_run: bool
) -> tuple[int, int]:
    """Re-point or delete each non-Pipedrive SS-PROP-* mapping. Returns
    (repointed_count, deleted_count)."""
    bad_rows = conn.execute(
        """
        SELECT tool_name, tool_specific_id, tool_specific_url
        FROM cross_tool_mapping
        WHERE canonical_id = %s
          AND tool_name = ANY(%s)
        ORDER BY tool_name
        """,
        (proposal_id, list(_NON_PROP_TOOLS)),
    ).fetchall()

    repointed = 0
    deleted = 0
    for bad in bad_rows:
        tool = bad["tool_name"]
        # Does the SS-CLIENT-* already have a mapping for this tool?
        existing = conn.execute(
            "SELECT tool_specific_id FROM cross_tool_mapping "
            "WHERE canonical_id = %s AND tool_name = %s",
            (client_id, tool),
        ).fetchone()

        if existing is not None:
            # SS-CLIENT-* already owns this tool — drop the duplicate SS-PROP-* row.
            if dry_run:
                print(
                    f"  [dry] would DELETE ({proposal_id}, {tool}) "
                    f"[duplicate of {client_id}/{tool}={existing['tool_specific_id']}]"
                )
            else:
                conn.execute(
                    "DELETE FROM cross_tool_mapping "
                    "WHERE canonical_id = %s AND tool_name = %s",
                    (proposal_id, tool),
                )
                print(f"  deleted ({proposal_id}, {tool}) [dup]")
            deleted += 1
        else:
            # SS-CLIENT-* has no mapping for this tool yet — repoint.
            if dry_run:
                print(
                    f"  [dry] would UPDATE ({proposal_id}, {tool}) "
                    f"→ ({client_id}, CLIENT)"
                )
            else:
                conn.execute(
                    """
                    UPDATE cross_tool_mapping
                    SET canonical_id = %s,
                        entity_type  = 'CLIENT',
                        synced_at    = CURRENT_TIMESTAMP
                    WHERE canonical_id = %s AND tool_name = %s
                    """,
                    (client_id, proposal_id, tool),
                )
                print(f"  repointed ({proposal_id}, {tool}) → {client_id}")
            repointed += 1

    return repointed, deleted


def _affected_proposals(conn, only: Optional[str]) -> list[dict]:
    if only:
        rows = conn.execute(
            """
            SELECT cp.id, cp.title, cp.status, cp.client_id, cp.lead_id
            FROM commercial_proposals cp
            WHERE cp.id = %s
            """,
            (only,),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT cp.id, cp.title, cp.status, cp.client_id, cp.lead_id
            FROM commercial_proposals cp
            WHERE cp.id IN (
                SELECT DISTINCT canonical_id FROM cross_tool_mapping
                WHERE canonical_id LIKE 'SS-PROP-%%'
                  AND tool_name = ANY(%s)
            )
            ORDER BY cp.id
            """,
            (list(_NON_PROP_TOOLS),),
        ).fetchall()
    return [dict(r) for r in rows]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true",
                       help="Print actions without modifying any rows")
    group.add_argument("--apply", action="store_true",
                       help="Apply the repair")
    parser.add_argument("--proposal", metavar="SS-PROP-XXXX",
                        help="Only repair this single proposal")
    args = parser.parse_args()

    dry_run = not args.apply
    conn = get_connection()
    try:
        proposals = _affected_proposals(conn, args.proposal)
        if not proposals:
            print("No affected proposals found.")
            return 0

        print(f"Found {len(proposals)} affected proposal(s):")
        for p in proposals:
            print(f"  {p['id']}  status={p['status']:<12}  "
                  f"client_id={p['client_id'] or '—':<14}  "
                  f"lead_id={p['lead_id'] or '—'}")
        print()

        skipped = 0
        repointed_total = 0
        deleted_total = 0
        minted_total = 0
        reserved_client_ids: set[str] = set()

        for prop in proposals:
            pid = prop["id"]
            print(f"--- {pid} ({prop['status']}) ---")

            if prop["status"] != "won" and not prop["client_id"]:
                print(f"  SKIP: status={prop['status']!r} and no client_id; "
                      "manual cleanup required.")
                skipped += 1
                continue

            if prop["lead_id"] and not prop["client_id"]:
                print(f"  SKIP: lead_id={prop['lead_id']!r} present; this script "
                      "doesn't promote leads. Re-run new_client_onboarding for "
                      "this deal or call _promote_lead_to_client manually.")
                skipped += 1
                continue

            try:
                if prop["client_id"]:
                    client_id = prop["client_id"]
                else:
                    client_id = _mint_client_for_proposal(
                        conn, pid, dry_run, reserved_client_ids
                    )
                    minted_total += 1

                rep, dele = _repair_mappings(conn, pid, client_id, dry_run)
                repointed_total += rep
                deleted_total += dele
            except Exception as exc:
                conn.rollback()
                print(f"  FAILED: {exc}")
                continue

        if dry_run:
            print()
            print(f"[dry-run] would mint:    {minted_total}")
            print(f"[dry-run] would repoint: {repointed_total}")
            print(f"[dry-run] would delete:  {deleted_total}")
            print(f"[dry-run] skipped:       {skipped}")
            print()
            print("Re-run with --apply to commit.")
            conn.rollback()
        else:
            conn.commit()
            print()
            print(f"minted:    {minted_total}")
            print(f"repointed: {repointed_total}")
            print(f"deleted:   {deleted_total}")
            print(f"skipped:   {skipped}")

        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
