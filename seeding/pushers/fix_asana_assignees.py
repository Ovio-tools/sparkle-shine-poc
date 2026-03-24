"""
fix_asana_assignees.py

One-time remediation: find all unassigned tasks in Client Success → Onboarding
and assign them based on the task title suffix (matching the logic in
automations/new_client_onboarding.py _build_task_list).

Usage:
    cd sparkle-shine-poc
    python seeding/pushers/fix_asana_assignees.py [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from auth import get_client   # noqa: E402
import asana                  # noqa: E402

# ---------------------------------------------------------------------------
# Known GIDs (from workspace member lookup)
# ---------------------------------------------------------------------------

_OFFICE_MANAGER_GID = "1213739372258981"   # Patricia Nguyen
_CREW_LEAD_GID      = "1213739618284641"   # Travis Coleman

# Task title suffixes that belong to the crew lead; everything else → office manager.
_CREW_LEAD_SUFFIXES = {
    "Schedule first visit",
    "Pre-service walkthrough with crew lead",
    "Order specialized supplies",
    "First service visit",
    "Quality inspection",
    "Complete service",
}


def _assignee_for(task_name: str) -> str:
    """Return the correct user GID based on the task title suffix."""
    for suffix in _CREW_LEAD_SUFFIXES:
        if task_name.endswith(suffix):
            return _CREW_LEAD_GID
    return _OFFICE_MANAGER_GID


def main(dry_run: bool) -> None:
    with open(os.path.join(_PROJECT_ROOT, "config", "tool_ids.json")) as f:
        tool_ids = json.load(f)

    project_gid = tool_ids["asana"]["projects"]["Client Success"]
    section_gid = tool_ids["asana"]["sections"]["Client Success"]["Onboarding"]

    client = get_client("asana")
    tasks_api = asana.TasksApi(client)

    print(f"Fetching tasks in Client Success → Onboarding (section {section_gid})...")

    opts = {"opt_fields": "gid,name,assignee"}
    all_tasks = list(tasks_api.get_tasks_for_section(section_gid, opts))

    unassigned = [t for t in all_tasks if not t.get("assignee")]
    print(f"  Total tasks:      {len(all_tasks)}")
    print(f"  Unassigned tasks: {len(unassigned)}")
    print()

    if not unassigned:
        print("Nothing to fix.")
        return

    fixed = 0
    for task in unassigned:
        gid  = task["gid"]
        name = task["name"]
        assignee_gid = _assignee_for(name)
        assignee_label = (
            "Patricia Nguyen (office manager)"
            if assignee_gid == _OFFICE_MANAGER_GID
            else "Travis Coleman (crew lead)"
        )

        if dry_run:
            print(f"  [DRY RUN] Would assign '{name}' → {assignee_label}")
        else:
            tasks_api.update_task(
                {"data": {"assignee": assignee_gid}},
                gid,
                {},
            )
            print(f"  Fixed: '{name}' → {assignee_label}")
            fixed += 1
            time.sleep(0.45)   # Asana: 150 req/min on free plan

    print()
    if dry_run:
        print(f"Dry run complete. Would have fixed {len(unassigned)} task(s).")
    else:
        print(f"Done. Fixed {fixed} task(s).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    main(dry_run=args.dry_run)
