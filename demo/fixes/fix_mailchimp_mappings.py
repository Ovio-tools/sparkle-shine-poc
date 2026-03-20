"""
Populate Mailchimp cross_tool_mapping entries for all clients and leads.

The Mailchimp pusher syncs contacts via a batch endpoint but does NOT register
individual member mappings in cross_tool_mapping (it only registers campaigns).
This script fetches all members from the Mailchimp audience and back-fills the
mapping table using email as the natural key to resolve canonical IDs.

Run:  python -m demo.fixes.fix_mailchimp_mappings
"""

from __future__ import annotations

import hashlib
import json
import os
import sys

_PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from auth.simple_clients import get_mailchimp_client
from database.schema import get_connection
from database.mappings import register_mapping

_DB_PATH = os.path.join(_PROJECT_ROOT, "sparkle_shine.db")

with open(os.path.join(_PROJECT_ROOT, "config", "tool_ids.json")) as _f:
    _AUDIENCE_ID = json.load(_f)["mailchimp"]["audience_id"]


def _subscriber_hash(email: str) -> str:
    return hashlib.md5(email.strip().lower().encode()).hexdigest()


def fix_mailchimp() -> None:
    mc = get_mailchimp_client()
    conn = get_connection(_DB_PATH)

    # Build email → canonical_id map from SQLite (clients + leads)
    email_to_canonical: dict[str, str] = {}
    for row in conn.execute("SELECT id, email FROM clients WHERE email IS NOT NULL").fetchall():
        email_to_canonical[row["email"].strip().lower()] = row["id"]
    for row in conn.execute("SELECT id, email FROM leads WHERE email IS NOT NULL").fetchall():
        email_to_canonical[row["email"].strip().lower()] = row["id"]

    print(f"[Mailchimp] Email→canonical map: {len(email_to_canonical)} entries")

    # Check existing mappings to skip already-registered ones
    already = set()
    for row in conn.execute(
        "SELECT canonical_id FROM cross_tool_mapping WHERE tool_name = 'mailchimp'"
    ).fetchall():
        already.add(row["canonical_id"])
    print(f"[Mailchimp] Already mapped: {len(already)} entries")

    # Fetch all audience members (paginated, 1000 per page)
    registered = skipped = unresolved = 0
    offset = 0
    page_size = 1000

    while True:
        result = mc.lists.get_list_members_info(
            _AUDIENCE_ID,
            count=page_size,
            offset=offset,
        )
        members = result.get("members") or []
        total = result.get("total_items", 0)

        for m in members:
            email = (m.get("email_address") or "").strip().lower()
            mc_id = m.get("id") or _subscriber_hash(email)
            canonical_id = email_to_canonical.get(email)
            if not canonical_id:
                unresolved += 1
                continue
            if canonical_id in already:
                skipped += 1
                continue
            register_mapping(canonical_id, "mailchimp", mc_id, db_path=_DB_PATH)
            already.add(canonical_id)
            registered += 1

        offset += len(members)
        print(f"  Fetched {offset}/{total} members so far ...")
        if offset >= total or not members:
            break

    conn.close()
    print(
        f"\n[Done] {registered} mappings registered, "
        f"{skipped} already existed, {unresolved} emails unresolved."
    )


if __name__ == "__main__":
    fix_mailchimp()
