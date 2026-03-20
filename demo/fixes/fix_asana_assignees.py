"""
Fix Asana task assignees that were seeded with the 'tools' fallback account
instead of the named employee.

When the Jobber pusher originally ran, most employees weren't in the Asana
workspace, so all tasks fell back to the service-account GID. This script:
  1. Fetches all workspace users to build a name → GID map.
  2. For every Asana-mapped task in SQLite, resolves the expected employee GID.
  3. Fetches the task's current assignee from Asana.
  4. Updates the assignee if it doesn't match.

Run:  python -m demo.fixes.fix_asana_assignees
"""

from __future__ import annotations

import json
import os
import sys

_PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import asana

from auth.simple_clients import get_asana_client
from database.schema import get_connection
from seeding.utils.throttler import ASANA

_DB_PATH = os.path.join(_PROJECT_ROOT, "sparkle_shine.db")

with open(os.path.join(_PROJECT_ROOT, "config", "tool_ids.json")) as _f:
    _WORKSPACE_GID = json.load(_f)["asana"]["workspace_gid"]


def _resolve_workspace_users() -> dict[str, str]:
    """Return {name_lower: gid} for all users in the Asana workspace."""
    client = get_asana_client()
    users_api = asana.UsersApi(client)
    ASANA.wait()
    users = list(
        users_api.get_users_for_workspace(
            _WORKSPACE_GID,
            opts={"opt_fields": "gid,name,email"},
        )
    )
    print(f"[Asana] Workspace users ({len(users)}):")
    for u in users:
        print(f"  - {u['name']!r}  ({u['gid']})")
    return {u["name"].strip().lower(): u["gid"] for u in users}


def fix_task_assignees() -> None:
    """Update every Asana task whose current assignee differs from SQLite."""
    asana_client = get_asana_client()
    tasks_api = asana.TasksApi(asana_client)

    name_to_gid = _resolve_workspace_users()
    if not name_to_gid:
        print("[ERROR] No workspace users found — aborting.")
        return

    conn = get_connection(_DB_PATH)
    task_rows = conn.execute("""
        SELECT
            t.id,
            t.title,
            t.assignee_employee_id,
            e.first_name || ' ' || e.last_name AS employee_name,
            m.tool_specific_id                 AS asana_gid
        FROM tasks t
        JOIN cross_tool_mapping m ON m.canonical_id = t.id AND m.tool_name = 'asana'
        JOIN employees e          ON e.id = t.assignee_employee_id
        ORDER BY t.id
    """).fetchall()
    conn.close()

    print(f"\n[Asana] Checking {len(task_rows)} mapped tasks ...")

    updated = skipped = not_in_workspace = already_correct = 0

    for row in task_rows:
        task_id = row["id"]
        asana_gid = row["asana_gid"]
        employee_name = row["employee_name"].strip().lower()

        expected_gid = name_to_gid.get(employee_name)
        if not expected_gid:
            not_in_workspace += 1
            continue  # employee not in Asana workspace — nothing to do

        # Fetch current assignee from Asana
        try:
            ASANA.wait()
            task_data = tasks_api.get_task(
                asana_gid,
                opts={"opt_fields": "assignee.gid,assignee.name"},
            )
        except Exception as exc:
            print(f"  [WARN] Cannot fetch {task_id} ({asana_gid}): {exc}")
            skipped += 1
            continue

        current = task_data.get("assignee") or {}
        current_gid = current.get("gid", "")

        if current_gid == expected_gid:
            already_correct += 1
            continue

        # Update assignee
        try:
            ASANA.wait()
            tasks_api.update_task(
                {"data": {"assignee": expected_gid}},
                asana_gid,
                opts={},
            )
            print(
                f"  [FIXED] {task_id} ({row['title'][:40]!r}): "
                f"assignee → {row['employee_name']!r} ({expected_gid})"
            )
            updated += 1
        except Exception as exc:
            print(f"  [WARN] Cannot update {task_id}: {exc}")
            skipped += 1

    print(
        f"\n[Done] {updated} updated, {already_correct} already correct, "
        f"{skipped} skipped, {not_in_workspace} employees not in workspace"
    )


if __name__ == "__main__":
    fix_task_assignees()
