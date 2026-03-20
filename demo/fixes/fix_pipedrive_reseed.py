"""
Delete all Pipedrive mappings from cross_tool_mapping, then re-push.

The Pipedrive sandbox was reset (all persons and deals returned 404).
Clearing the stale mappings and re-running the pusher creates fresh records.

Run:  python -m demo.fixes.fix_pipedrive_reseed
"""

from __future__ import annotations

import os
import sys

_PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from database.schema import get_connection

_DB_PATH = os.path.join(_PROJECT_ROOT, "sparkle_shine.db")


def delete_pipedrive_mappings() -> int:
    conn = get_connection(_DB_PATH)
    row = conn.execute(
        "SELECT COUNT(*) AS cnt FROM cross_tool_mapping WHERE tool_name = 'pipedrive'"
    ).fetchone()
    count = row["cnt"] if row else 0
    conn.execute("DELETE FROM cross_tool_mapping WHERE tool_name = 'pipedrive'")
    conn.commit()
    conn.close()
    return count


def reseed_pipedrive() -> None:
    print("[Pipedrive] Deleting stale mappings ...")
    deleted = delete_pipedrive_mappings()
    print(f"[Pipedrive] Deleted {deleted} stale mappings from cross_tool_mapping")

    print("\n[Pipedrive] Re-running pusher ...")
    from seeding.pushers.push_pipedrive import main as _pipedrive_main
    _pipedrive_main(dry_run=False)


if __name__ == "__main__":
    reseed_pipedrive()
