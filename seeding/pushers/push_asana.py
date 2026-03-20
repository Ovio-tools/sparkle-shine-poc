"""Push Sparkle & Shine task records to Asana.

Full run:  python seeding/pushers/push_asana.py
Dry run:   python seeding/pushers/push_asana.py --dry-run

Push order:
  Phase 1 — Resolve user GIDs  (workspace user lookup; fallback to Tools GID)
  Phase 2 — Push tasks         (POST /api/1.0/tasks + section assignment)
  Phase 3 — Verification       (task counts per project + overdue task report)

Auth: auth.get_client("asana") validates credentials on startup.
      ASANA_ACCESS_TOKEN is used as a Bearer token on every request.

Rate limiting: ASANA throttler at 2.2 req/sec (firm).
  Each task requires 2 API calls (create + section assignment).
  500 tasks × 2 calls ≈ 1000 calls → ~455 seconds runtime (~7.5 minutes).

Known limitation — completed_at backdating:
  Asana automatically sets completed_at to the current time when a task is
  created with completed=True. There is no API parameter to backdate it.
  Seeded completed tasks will therefore show a completed_at of the push date,
  not the historical completed_date from the database.

Section mapping:
  Client Success      → all tasks → "Onboarding"
  Admin & Operations  → not_started→"To Do", in_progress→"In Progress",
                        overdue→"Waiting", completed→"Done"
  Sales Pipeline Tasks→ "Initial outreach"→"New Leads",
                        "Proposal follow-up"→"Follow-Up",
                        "Follow-up call"→"Closed", "Contract review"→"Closed"
"""

from __future__ import annotations

import json
import os
import sys
from datetime import date
from typing import Optional

import asana

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from auth import get_client                                               # noqa: E402
from database.schema import get_connection                                # noqa: E402
from database.mappings import register_mapping, get_tool_id               # noqa: E402
from seeding.utils.throttler import ASANA                                 # noqa: E402

_DB_PATH = os.path.join(_PROJECT_ROOT, "sparkle_shine.db")

# ---------------------------------------------------------------------------
# Tool IDs (loaded once at import time)
# ---------------------------------------------------------------------------

with open(os.path.join(_PROJECT_ROOT, "config", "tool_ids.json")) as _f:
    _TOOL_IDS = json.load(_f)

_ASANA_CFG     = _TOOL_IDS["asana"]
_WORKSPACE_GID = _ASANA_CFG["workspace_gid"]
_PROJECT_GIDS  = _ASANA_CFG["projects"]   # {project_name: gid}
_SECTION_GIDS  = _ASANA_CFG["sections"]   # {project_name: {section_name: gid}}

_TODAY = date.today().isoformat()

# Expected task counts per project for verification
_EXPECTED_COUNTS = {
    "Client Success":      370,
    "Admin & Operations":  80,
    "Sales Pipeline Tasks": 50,
}

# ---------------------------------------------------------------------------
# Asana API helpers (initialized in main after auth)
# ---------------------------------------------------------------------------

_tasks_api: asana.TasksApi     = None  # type: ignore[assignment]
_sections_api: asana.SectionsApi = None  # type: ignore[assignment]
_users_api: asana.UsersApi     = None  # type: ignore[assignment]

# ---------------------------------------------------------------------------
# Section resolution
# ---------------------------------------------------------------------------

def _resolve_section_gid(task: dict) -> str:
    """Return the Asana section GID for the given task record."""
    project = task["project_name"]
    status  = task["status"]
    title   = task["title"]

    if project == "Client Success":
        return _SECTION_GIDS["Client Success"]["Onboarding"]

    if project == "Admin & Operations":
        if status == "completed":
            return _SECTION_GIDS["Admin & Operations"]["Done"]
        if status == "in_progress":
            return _SECTION_GIDS["Admin & Operations"]["In Progress"]
        if status == "overdue":
            return _SECTION_GIDS["Admin & Operations"]["Waiting"]
        return _SECTION_GIDS["Admin & Operations"]["To Do"]  # not_started

    if project == "Sales Pipeline Tasks":
        if title.startswith("Initial outreach"):
            return _SECTION_GIDS["Sales Pipeline Tasks"]["New Leads"]
        if title.startswith("Proposal follow-up"):
            return _SECTION_GIDS["Sales Pipeline Tasks"]["Follow-Up"]
        if title.startswith("Follow-up call") or title.startswith("Contract review"):
            return _SECTION_GIDS["Sales Pipeline Tasks"]["Closed"]
        return _SECTION_GIDS["Sales Pipeline Tasks"]["New Leads"]

    raise ValueError(f"Unknown project_name: {project!r}")


# ---------------------------------------------------------------------------
# Phase 1 — Resolve user GIDs
# ---------------------------------------------------------------------------

def resolve_user_gids() -> dict[str, str]:
    """
    Fetch workspace users and build employee_id → Asana user GID mapping.

    Most employees (cleaners) won't have Asana accounts, so the workspace
    may contain only a single API/admin user. All employees without a name
    match fall back to the first workspace user's GID (Tools account).

    Returns:
        {employee_id: asana_user_gid}
    """
    ASANA.wait()
    ASANA.track_call("users.get_users_for_workspace")
    workspace_users = list(
        _users_api.get_users_for_workspace(
            _WORKSPACE_GID,
            opts={"opt_fields": "gid,name,email"},
        )
    )

    if not workspace_users:
        raise RuntimeError("No users found in Asana workspace")

    # Build name → GID lookup (case-insensitive)
    name_to_gid: dict[str, str] = {u["name"].lower(): u["gid"] for u in workspace_users}
    fallback_gid = workspace_users[0]["gid"]

    # Employee name → GID mapping from DB
    conn = get_connection(_DB_PATH)
    employees = conn.execute(
        "SELECT id, first_name, last_name FROM employees ORDER BY id"
    ).fetchall()
    conn.close()

    mapping: dict[str, str] = {}
    for emp in employees:
        full_name = f"{emp['first_name']} {emp['last_name']}".lower()
        gid = name_to_gid.get(full_name, fallback_gid)
        mapping[emp["id"]] = gid

    matched = sum(1 for gid in mapping.values() if gid != fallback_gid)
    print(
        f"  Resolved {len(mapping)} employee GIDs "
        f"({matched} matched by name, {len(mapping) - matched} using fallback "
        f"GID {fallback_gid!r})"
    )
    return mapping


# ---------------------------------------------------------------------------
# Phase 2 — Push tasks
# ---------------------------------------------------------------------------

def _fetch_tasks(conn) -> list[dict]:
    rows = conn.execute(
        """SELECT id, title, description, project_name, assignee_employee_id,
                  due_date, status
           FROM tasks
           ORDER BY id"""
    ).fetchall()
    return [dict(r) for r in rows]


def push_tasks(user_gids: dict[str, str], dry_run: bool = False) -> int:
    """
    Create all tasks in Asana and register mappings.

    Each task requires two API calls:
      1. POST /api/1.0/tasks          (create)
      2. POST /sections/{gid}/addTask (move to section)

    Idempotent: tasks already in cross_tool_mapping are skipped.

    Returns number of tasks created.
    """
    conn = get_connection(_DB_PATH)
    tasks = _fetch_tasks(conn)
    conn.close()

    created = skipped = errors = 0

    for task in tasks:
        task_id = task["id"]

        # Idempotency check
        existing_gid = get_tool_id(task_id, "asana", db_path=_DB_PATH)
        if existing_gid:
            skipped += 1
            continue

        project_name = task["project_name"]
        project_gid  = _PROJECT_GIDS.get(project_name)
        if not project_gid:
            print(f"  [WARN] Unknown project {project_name!r} for {task_id} — skipping")
            errors += 1
            continue

        assignee_gid = user_gids.get(task["assignee_employee_id"])
        if not assignee_gid:
            print(f"  [WARN] No user GID for {task['assignee_employee_id']} — skipping {task_id}")
            errors += 1
            continue

        is_completed = task["status"] == "completed"

        if dry_run:
            print(
                f"  [DRY] {task_id}: {task['title'][:50]!r} "
                f"→ {project_name} | completed={is_completed}"
            )
            created += 1
            continue

        # --- Create task ---
        ASANA.wait()
        ASANA.track_call("tasks.create_task")
        try:
            result = _tasks_api.create_task(
                {
                    "data": {
                        "name":      task["title"],
                        "notes":     task["description"] or "",
                        "due_on":    task["due_date"],
                        "projects":  [project_gid],
                        "assignee":  assignee_gid,
                        "completed": is_completed,
                        "workspace": _WORKSPACE_GID,
                    }
                },
                opts={},
            )
        except Exception as exc:
            print(f"  [ERROR] create_task {task_id}: {exc}")
            errors += 1
            continue

        task_gid = result["gid"]
        register_mapping(task_id, "asana", task_gid, db_path=_DB_PATH)

        # --- Move to section ---
        try:
            section_gid = _resolve_section_gid(task)
        except ValueError as exc:
            print(f"  [WARN] {task_id}: {exc}")
            created += 1
            continue

        ASANA.wait()
        ASANA.track_call("sections.add_task_for_section")
        try:
            _sections_api.add_task_for_section(
                section_gid,
                {"body": {"data": {"task": task_gid}}},
            )
        except Exception as exc:
            print(f"  [WARN] section assignment for {task_id} ({task_gid}): {exc}")
            # Not fatal — task was created, just not in the right section

        created += 1

        if created % 50 == 0:
            print(f"  ... {created} tasks created so far")

    print(
        f"  Tasks: {created} created, {skipped} skipped (already mapped), "
        f"{errors} errors"
    )
    return created


# ---------------------------------------------------------------------------
# Phase 3 — Verification
# ---------------------------------------------------------------------------

def verify_tasks() -> None:
    """
    Verify task counts per project and overdue task statistics.

    Per-project verification:
      Counts tasks per project from cross_tool_mapping (authoritative — only
      tasks we pushed). Also fetches Asana's total to detect pre-existing tasks.
      Pre-existing tasks in a project show as a positive delta and are expected
      when the project was set up with sample data before the seeding run.

    User task report:
      Fetches all tasks for the workspace user and reports total count
      plus how many have a past due_on date and are incomplete (overdue
      in Asana's UI sense). Since all employees fall back to the same
      Asana user, this reflects the full pushed task set.
    """
    print("\n--- Verification ---")

    # 1. Per-project task counts (DB-side, cross_tool_mapping authoritative)
    conn = get_connection(_DB_PATH)
    all_ok = True
    for project_name in _EXPECTED_COUNTS:
        project_gid = _PROJECT_GIDS.get(project_name)
        if not project_gid:
            continue

        # Count our mapped tasks for this project
        mapped_count = conn.execute(
            """SELECT COUNT(*) FROM cross_tool_mapping ctm
               JOIN tasks t ON t.id = ctm.canonical_id
               WHERE ctm.tool_name = 'asana' AND t.project_name = ?""",
            (project_name,),
        ).fetchone()[0]

        # Also fetch Asana's count for informational purposes
        ASANA.wait()
        ASANA.track_call("tasks.get_tasks_for_project")
        try:
            asana_tasks = list(
                _tasks_api.get_tasks_for_project(
                    project_gid,
                    opts={"opt_fields": "gid", "limit": 100},
                )
            )
            asana_count = len(asana_tasks)
        except Exception as exc:
            print(f"  [ERROR] get_tasks_for_project({project_name}): {exc}")
            asana_count = -1
            all_ok = False

        expected = _EXPECTED_COUNTS[project_name]
        status   = "[OK]" if mapped_count == expected else "[MISMATCH]"
        if mapped_count != expected:
            all_ok = False

        extra = asana_count - mapped_count if asana_count >= 0 else 0
        extra_note = f" (+{extra} pre-existing)" if extra > 0 else ""
        print(
            f"  {status} {project_name}: {mapped_count} mapped tasks "
            f"(expected {expected}){extra_note}"
        )

    conn.close()

    # 2. User task report (overdue check)
    # Fetch tasks for the workspace user (fallback GID used for all employees).
    ASANA.wait()
    ASANA.track_call("tasks.get_tasks")
    try:
        user_tasks = list(
            _tasks_api.get_tasks(
                opts={
                    "assignee": "me",
                    "workspace": _WORKSPACE_GID,
                    "opt_fields": "gid,completed,due_on",
                    "limit": 100,
                }
            )
        )
    except Exception as exc:
        print(f"  [ERROR] get_tasks for user: {exc}")
        return

    total         = len(user_tasks)
    overdue_count = sum(
        1 for t in user_tasks
        if not t.get("completed")
        and t.get("due_on")
        and t["due_on"] < _TODAY
    )
    overdue_pct = (overdue_count / total * 100) if total else 0

    print(f"\n  User (Tools) task report:")
    print(f"    Total tasks assigned : {total}")
    print(f"    Incomplete + past due : {overdue_count}  ({overdue_pct:.0f}% of total)")
    print(f"    Note: ~40% overdue expected for Maria's tasks (SS-EMP-001).")
    print(f"          All employees share one Asana user, so this reflects the full set.")

    if all_ok:
        print("\n  [OK] All project task counts match.")
    else:
        print("\n  [WARN] Some counts differ — check output above.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(dry_run: bool = False) -> None:
    print("=== push_asana.py ===")
    if dry_run:
        print("DRY RUN — no data will be written to Asana\n")

    # Auth
    print("Authenticating...")
    client = get_client("asana")

    global _tasks_api, _sections_api, _users_api
    _tasks_api    = asana.TasksApi(client)
    _sections_api = asana.SectionsApi(client)
    _users_api    = asana.UsersApi(client)
    print("  Auth OK\n")

    # Phase 1
    print("Phase 1 — Resolving user GIDs...")
    user_gids = resolve_user_gids()
    print()

    # Phase 2
    print("Phase 2 — Pushing tasks...")
    print(
        "  Note: completed tasks will show today's date as completed_at.\n"
        "  Asana does not support backdating completed_at via the API.\n"
    )
    push_tasks(user_gids, dry_run=dry_run)
    print()

    # Phase 3
    if not dry_run:
        verify_tasks()

    print("\nDone.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Push Sparkle & Shine tasks to Asana")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be pushed without making API calls")
    args = parser.parse_args()

    main(dry_run=args.dry_run)
