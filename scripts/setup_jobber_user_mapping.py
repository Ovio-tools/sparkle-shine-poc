#!/usr/bin/env python3
"""Discover Jobber users and persist them as a flat assignment pool.

Writes `config/tool_ids.json["jobber"]` with:
    user_pool             list of Jobber user IDs that jobCreate may assign
    users_seen            full discovery snapshot for review/debug
    crew_size_tiers       duration tier thresholds (1 / 2 / 3 users)
    endAt_jitter_minutes  visible duration jitter on the Jobber calendar

Run once after inviting new team members to Jobber, and re-run any time the
roster changes. Idempotent — only `users_seen` and `user_pool` move when
Jobber's user list changes; thresholds + jitter are preserved.

Usage:
    python scripts/setup_jobber_user_mapping.py [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from auth import get_client

_TOOL_IDS_PATH = os.path.join(PROJECT_ROOT, "config", "tool_ids.json")
_JOBBER_GQL_URL = "https://api.getjobber.com/api/graphql"
_JOBBER_VERSION_HEADER = {"X-JOBBER-GRAPHQL-VERSION": "2026-03-10"}

# Heuristic patterns matched (case-insensitive) against each Jobber user's
# name and email. Any user matching one of these is excluded from the
# default user_pool — it's almost certainly a service account, automation
# user, or shared inbox, not a field cleaner. Operators can still add the
# ID back manually by editing tool_ids.json.
_NON_FIELD_PATTERNS = (
    "tools", "service", "automation", "bot", "api",
    "noreply", "no-reply", "support", "admin",
)

_USERS_QUERY = """
query Users {
  users(first: 100) {
    nodes {
      id
      name { full first last }
      email { raw }
    }
  }
}
"""


def _query_users(session) -> list[dict]:
    resp = session.post(
        _JOBBER_GQL_URL,
        json={"query": _USERS_QUERY},
        headers=_JOBBER_VERSION_HEADER,
        timeout=30,
    )
    resp.raise_for_status()
    body = resp.json()
    errs = body.get("errors")
    if errs:
        raise RuntimeError(f"Jobber users query errors: {errs}")
    nodes = (
        body.get("data", {}).get("users", {}).get("nodes", []) or []
    )
    flat = []
    for n in nodes:
        full = (n.get("name") or {}).get("full") or ""
        email = (n.get("email") or {}).get("raw") or ""
        flat.append({"id": n["id"], "name": full, "email": email})
    return flat


def _load_existing() -> dict:
    if not os.path.exists(_TOOL_IDS_PATH):
        return {}
    with open(_TOOL_IDS_PATH) as f:
        return json.load(f)


def _looks_like_non_field_user(user: dict) -> str | None:
    """Return the matched pattern if a user looks like a service/automation
    account, else None. Case-insensitive substring match on name + email."""
    haystack = f"{user.get('name', '')} {user.get('email', '')}".lower()
    for pat in _NON_FIELD_PATTERNS:
        if pat in haystack:
            return pat
    return None


def _build_jobber_block(users: list[dict], existing_jobber: dict) -> dict:
    pool: list[str] = []
    excluded: list[dict] = []
    for u in users:
        pat = _looks_like_non_field_user(u)
        if pat:
            excluded.append({**u, "excluded_reason": f"matched '{pat}'"})
        else:
            pool.append(u["id"])

    return {
        # Default pool excludes anything matching _NON_FIELD_PATTERNS (service
        # accounts, automation users). Operators can move IDs between
        # user_pool and excluded_from_pool by hand if a heuristic misfires.
        "user_pool": pool,
        "excluded_from_pool": excluded,
        "users_seen": users,
        "crew_size_tiers": existing_jobber.get(
            "crew_size_tiers", {"small_max": 90, "medium_max": 150},
        ),
        "endAt_jitter_minutes": existing_jobber.get("endAt_jitter_minutes", 5),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print the proposed jobber block without writing tool_ids.json",
    )
    args = parser.parse_args()

    session = get_client("jobber")
    users = _query_users(session)
    if not users:
        print("[setup_jobber_user_mapping] No Jobber users returned. Aborting.")
        sys.exit(1)

    existing = _load_existing()
    existing_jobber = existing.get("jobber") or {}
    new_jobber = _build_jobber_block(users, existing_jobber)

    print(f"[setup_jobber_user_mapping] Discovered {len(users)} Jobber users:")
    for u in users:
        pat = _looks_like_non_field_user(u)
        marker = f"  [EXCLUDED: '{pat}']" if pat else ""
        print(f"  {u['id']}  {u['name']:30s}  {u['email']}{marker}")
    print(
        f"\nDefault pool: {len(new_jobber['user_pool'])} field-staff users  "
        f"({len(new_jobber['excluded_from_pool'])} excluded by heuristic)."
    )

    if args.dry_run:
        print("\n[dry-run] Would write the following 'jobber' block:")
        print(json.dumps(new_jobber, indent=2))
        return

    existing["jobber"] = new_jobber
    with open(_TOOL_IDS_PATH, "w") as f:
        json.dump(existing, f, indent=2)
        f.write("\n")

    print(
        f"\nWrote {len(users)} Jobber users to {_TOOL_IDS_PATH} "
        f"({len(new_jobber['user_pool'])} in user_pool, "
        f"{len(new_jobber['excluded_from_pool'])} in excluded_from_pool). "
        f"If a heuristic misfired, move IDs between the two lists by hand."
    )


if __name__ == "__main__":
    main()
