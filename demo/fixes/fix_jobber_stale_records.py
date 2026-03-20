"""
Delete stale Jobber client + recurring-agreement mappings from cross_tool_mapping.

A 'stale' mapping is one where the stored Jobber GID returns null data from
the GraphQL API (record was deleted on the Jobber side, e.g. after a sandbox
reset or manual cleanup).

After running this script, re-run the Jobber pusher:
    python seeding/pushers/push_jobber.py

The pusher is idempotent: it skips records already in cross_tool_mapping and
only creates the ones that were just cleared.

Run:  python -m demo.fixes.fix_jobber_stale_records
"""

from __future__ import annotations

import os
import sys

_PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import requests

from auth.jobber_auth import get_jobber_session
from database.schema import get_connection
from seeding.utils.throttler import JOBBER

_DB_PATH = os.path.join(_PROJECT_ROOT, "sparkle_shine.db")
_JOBBER_GQL = "https://api.getjobber.com/api/graphql"

_CLIENT_QUERY = """
query CheckClient($id: EncodedId!) {
  client(id: $id) { id }
}
"""

_RECURRING_QUERY = """
query CheckRecurring($id: EncodedId!) {
  recurringJob(id: $id) { id }
}
"""

_JOB_QUERY = """
query CheckJob($id: EncodedId!) {
  job(id: $id) { id }
}
"""


def _gql(session: requests.Session, query: str, variables: dict) -> dict:
    JOBBER.wait()
    resp = session.post(
        _JOBBER_GQL,
        json={"query": query, "variables": variables},
        timeout=30,
    )
    if resp.status_code == 401:
        new_session = get_jobber_session()
        session.headers.update(new_session.headers)
        JOBBER.wait()
        resp = session.post(
            _JOBBER_GQL,
            json={"query": query, "variables": variables},
            timeout=30,
        )
    resp.raise_for_status()
    return resp.json()


def _is_stale(session: requests.Session, query: str, key: str, gid: str) -> bool:
    """Return True if the Jobber record no longer exists."""
    try:
        data = _gql(session, query, {"id": gid})
        return not (data.get("data") or {}).get(key)
    except Exception:
        return True


def delete_stale_mappings() -> None:
    session = get_jobber_session()
    conn = get_connection(_DB_PATH)

    stale: list[tuple[str, str]] = []  # (canonical_id, entity_type)

    # ---- Check CLIENT mappings ----
    client_rows = conn.execute("""
        SELECT canonical_id, tool_specific_id
        FROM cross_tool_mapping
        WHERE tool_name = 'jobber' AND entity_type = 'CLIENT'
        ORDER BY canonical_id
    """).fetchall()

    print(f"[Jobber] Checking {len(client_rows)} CLIENT mappings ...")
    for row in client_rows:
        cid, gid = row["canonical_id"], row["tool_specific_id"]
        if _is_stale(session, _CLIENT_QUERY, "client", gid):
            stale.append((cid, "CLIENT"))
            print(f"  [STALE] {cid} → {gid}")

    # ---- Check RECUR mappings ----
    recur_rows = conn.execute("""
        SELECT canonical_id, tool_specific_id
        FROM cross_tool_mapping
        WHERE tool_name = 'jobber' AND entity_type = 'RECUR'
        ORDER BY canonical_id
    """).fetchall()

    print(f"\n[Jobber] Checking {len(recur_rows)} RECUR mappings ...")
    for row in recur_rows:
        cid, gid = row["canonical_id"], row["tool_specific_id"]
        if _is_stale(session, _RECURRING_QUERY, "recurringJob", gid):
            stale.append((cid, "RECUR"))
            print(f"  [STALE] {cid} → {gid}")

    if not stale:
        print("\n[Done] No stale Jobber mappings found.")
        conn.close()
        return

    print(f"\n[Summary] {len(stale)} stale mappings to delete:")
    for cid, etype in stale:
        print(f"  {etype}: {cid}")

    for cid, _ in stale:
        conn.execute(
            "DELETE FROM cross_tool_mapping WHERE canonical_id = ? AND tool_name = 'jobber'",
            (cid,),
        )
    conn.commit()
    conn.close()

    print(f"\n[Done] Deleted {len(stale)} stale mappings.")
    print("\nNext step:  python seeding/pushers/push_jobber.py")
    print("(Only the unmapped clients + recurring agreements will be created.)")


if __name__ == "__main__":
    delete_stale_mappings()
