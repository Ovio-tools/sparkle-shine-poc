"""
Asana syncer -- pulls tasks from all 4 projects into SQLite.

Maps Asana assignees to employees by name.
Computes is_overdue from due_on vs. today.
"""
import json
import time
from datetime import date, datetime
from typing import Optional

import asana

from auth import get_client
from database.mappings import get_canonical_id, register_mapping, generate_id
from intelligence.syncers.base_syncer import BaseSyncer, SyncResult
from seeding.utils.throttler import ASANA

# Project GIDs from tool_ids.json
_PROJECTS = {
    "Sales Pipeline Tasks":  "1213719393240330",
    "Marketing Calendar":    "1213719401725621",
    "Admin & Operations":    "1213719394454339",
    "Client Success":        "1213719346640011",
}

_OPT_FIELDS = ",".join([
    "name",
    "completed",
    "completed_at",
    "due_on",
    "assignee.name",
    "notes",
    "memberships.section.name",
    "modified_at",
    "permalink_url",
])


class AsanaSyncer(BaseSyncer):
    tool_name = "asana"

    def __init__(self, db_path: str):
        super().__init__(db_path)
        # Cache employee name → id to avoid per-task DB lookups
        self._employee_cache: dict[str, str] = {}
        self._load_employee_cache()

    def _load_employee_cache(self) -> None:
        rows = self.db.execute(
            "SELECT id, first_name, last_name FROM employees"
        ).fetchall()
        for row in rows:
            full_name = f"{row['first_name']} {row['last_name']}".strip()
            self._employee_cache[full_name.lower()] = row["id"]

    def sync(self, since: Optional[datetime] = None) -> SyncResult:
        is_incremental = since is not None
        start = time.monotonic()
        errors: list[str] = []
        total = 0

        self.logger.info(
            "Starting %s Asana sync (since=%s)",
            "incremental" if is_incremental else "full",
            since,
        )

        try:
            client = get_client("asana")
        except Exception as exc:
            errors.append(f"Auth failed: {exc}")
            self.update_sync_state(0, error=str(exc))
            return SyncResult(
                tool_name=self.tool_name,
                records_synced=0,
                errors=errors,
                duration_seconds=time.monotonic() - start,
                is_incremental=is_incremental,
            )

        since_iso = since.isoformat() if since else None

        for project_name, project_gid in _PROJECTS.items():
            synced = self._sync_project(client, project_name, project_gid, since_iso, errors)
            total += synced
            self.logger.debug("Project '%s': %d tasks synced", project_name, synced)

        self.update_sync_state(total, error=errors[0] if errors else None)
        duration = time.monotonic() - start
        self.logger.info("Asana sync complete: %d tasks in %.1fs", total, duration)
        return SyncResult(
            tool_name=self.tool_name,
            records_synced=total,
            errors=errors,
            duration_seconds=duration,
            is_incremental=is_incremental,
        )

    def _sync_project(
        self,
        client,
        project_name: str,
        project_gid: str,
        since_iso: Optional[str],
        errors: list,
    ) -> int:
        count = 0
        tasks_api = asana.TasksApi(client)

        opts = {
            "opt_fields": _OPT_FIELDS,
            "limit": 100,
        }
        if since_iso:
            opts["modified_since"] = since_iso

        try:
            ASANA.wait()
            task_iter = tasks_api.get_tasks_for_project(project_gid, opts=opts)
        except Exception as exc:
            errors.append(f"project {project_name} fetch error: {exc}")
            return 0

        for task in task_iter:
            ASANA.wait()
            try:
                self._upsert_task(task, project_name)
                count += 1
            except Exception as exc:
                errors.append(f"task {task.get('gid', '?')}: {exc}")

        return count

    def _upsert_task(self, task: dict, project_name: str) -> None:
        asana_gid = task.get("gid") or task.get("id") or ""
        canonical_id = get_canonical_id("asana", asana_gid, self.db_path)

        title = task.get("name") or "Untitled"
        description = task.get("notes") or None
        completed = bool(task.get("completed"))
        completed_at = (task.get("completed_at") or "")[:10] or None
        due_on = task.get("due_on")  # "YYYY-MM-DD" or None

        # Determine status
        today = date.today().isoformat()
        if completed:
            status = "completed"
        elif due_on and due_on < today:
            status = "overdue"
        elif due_on:
            status = "not_started"
        else:
            status = "not_started"

        # Resolve assignee
        assignee_name = ((task.get("assignee") or {}).get("name") or "").lower()
        assignee_id = self._employee_cache.get(assignee_name)

        # Section name (first membership)
        memberships = task.get("memberships") or []
        section_name = None
        for m in memberships:
            section = m.get("section") or {}
            section_name = section.get("name")
            if section_name:
                break

        if canonical_id is None:
            canonical_id = generate_id("TASK", self.db_path)
            with self.db:
                self.db.execute(
                    """
                    INSERT OR IGNORE INTO tasks
                        (id, title, description, project_name,
                         assignee_employee_id, due_date, completed_date, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        canonical_id, title, description, project_name,
                        assignee_id, due_on, completed_at, status,
                    ),
                )
            register_mapping(canonical_id, "asana", asana_gid, db_path=self.db_path)
        else:
            with self.db:
                self.db.execute(
                    """
                    UPDATE tasks
                    SET status              = ?,
                        completed_date      = COALESCE(?, completed_date),
                        due_date            = COALESCE(?, due_date),
                        assignee_employee_id = COALESCE(?, assignee_employee_id)
                    WHERE id = ?
                    """,
                    (status, completed_at, due_on, assignee_id, canonical_id),
                )


# ------------------------------------------------------------------ #
# CLI entry point
# ------------------------------------------------------------------ #

if __name__ == "__main__":
    import argparse
    import os
    import sys

    parser = argparse.ArgumentParser(description="Sync Asana tasks from all 4 projects into SQLite")
    parser.add_argument("--dry-run", action="store_true", help="Auth check + sample fetch; no DB writes")
    parser.add_argument("--since", metavar="YYYY-MM-DD", help="Incremental sync — only tasks modified after this date")
    parser.add_argument("--db", default="sparkle_shine.db", help="Path to SQLite database")
    args = parser.parse_args()

    db_path = os.path.abspath(args.db)
    since = datetime.strptime(args.since, "%Y-%m-%d") if args.since else None

    syncer = AsanaSyncer(db_path)
    last_sync = syncer.get_last_sync_time()

    print(f"\n[asana] DB:        {db_path}")
    print(f"[asana] Last sync: {last_sync or 'never'}")
    print(f"[asana] Mode:      {'DRY RUN' if args.dry_run else 'LIVE'}")
    if since:
        print(f"[asana] Since:     {since.date()}")

    print(f"\n[asana] Projects to sync:")
    for name, gid in _PROJECTS.items():
        print(f"  [{gid}] {name}")

    if args.dry_run:
        print("\n[asana] --- Auth check ---")
        try:
            client = get_client("asana")
            print("[asana] Auth OK")
        except Exception as exc:
            print(f"[asana] Auth FAILED: {exc}")
            syncer.close()
            sys.exit(1)

        print("\n[asana] --- Sample fetch (first 3 tasks from 'Admin & Operations', no DB writes) ---")
        try:
            tasks_api = asana.TasksApi(client)
            ASANA.wait()
            sample_gid = _PROJECTS["Admin & Operations"]
            task_iter = tasks_api.get_tasks_for_project(
                sample_gid,
                opts={
                    "opt_fields": "name,completed,due_on,assignee.name",
                    "limit": 3,
                },
            )
            shown = 0
            for task in task_iter:
                ASANA.wait()
                assignee = (task.get("assignee") or {}).get("name") or "unassigned"
                due = task.get("due_on") or "no due date"
                done = "✓" if task.get("completed") else "○"
                print(f"  {done} [{task.get('gid')}] {task.get('name', '?')[:50]} — due {due} — {assignee}")
                shown += 1
                if shown >= 3:
                    break
            if shown == 0:
                print("  (no tasks returned)")
            print(f"\n[asana] Would sync tasks from all 4 projects → tasks table.")
            print(f"[asana] Run without --dry-run to apply changes.")
        except Exception as exc:
            print(f"[asana] Sample fetch failed: {exc}")

        syncer.close()
        sys.exit(0)

    result = syncer.sync(since=since)
    syncer.close()

    print(f"\n[asana] Synced {result.records_synced} tasks in {result.duration_seconds:.1f}s")
    if result.errors:
        print(f"[asana] {len(result.errors)} error(s):")
        for err in result.errors[:10]:
            print(f"  - {err}")
    sys.exit(1 if result.errors else 0)
