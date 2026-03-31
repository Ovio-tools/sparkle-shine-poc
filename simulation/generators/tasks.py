"""
simulation/generators/tasks.py

Completes Asana tasks over time with realistic per-assignee rates.
Maria Gonzalez completes tasks at 15%/day (preserves the overdue pattern
visible in the intelligence layer). All other assignees complete at 30%/day.

Type 2 generator: progresses existing task records.
Dry-run convention: reads always allowed; Asana API + SQLite writes skipped.
"""
from __future__ import annotations

import random
import sqlite3
from dataclasses import dataclass
from datetime import date
from time import sleep
from typing import Optional

import asana
from asana.rest import ApiException as AsanaApiException

from auth import get_client
from database.mappings import get_tool_id
from intelligence.logging_config import setup_logging
from seeding.utils.throttler import ASANA as throttler
from simulation.config import DAILY_VOLUMES
from simulation.exceptions import TokenExpiredError

logger = setup_logging("simulation.tasks")


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------

@dataclass
class GeneratorResult:
    success: bool
    message: str = ""


# ---------------------------------------------------------------------------
# Completion rate configuration
# ---------------------------------------------------------------------------

_MARIA_RATE    = DAILY_VOLUMES["task_completion"]["maria_completion_rate"]   # 0.15
_DEFAULT_RATE  = DAILY_VOLUMES["task_completion"]["daily_completion_rate"]    # 0.30
_OVERDUE_BOOST     = 1.5
_ONBOARDING_BOOST  = 2.0

# The first 3 onboarding tasks in sequence (order matters for dependencies)
_ONBOARDING_TITLES = [
    "Create Jobber profile",
    "Schedule first visit",
    "Send welcome email",
]

# Dependency map: task title → title of blocker task (same client, same project)
# "Schedule first visit" cannot complete before "Create Jobber profile"
_DEPENDENCIES: dict[str, str] = {
    "Schedule first visit": "Create Jobber profile",
}


# ---------------------------------------------------------------------------
# Main generator
# ---------------------------------------------------------------------------

class TaskCompletionGenerator:
    """
    Probabilistically completes Asana tasks on each simulation tick.

    Completion rates:
      - Maria Gonzalez:    15%/day  (preserves ~40% overdue pattern)
      - All other assignees: 30%/day

    Boosts applied multiplicatively (capped at 1.0):
      - Overdue tasks:            × 1.5  (people eventually catch up)
      - First 3 onboarding tasks: × 2.0  (they block downstream Jobber setup)

    When the last onboarding task for a client completes:
      - Sets client.status = 'active' in SQLite
      - Logs "Onboarding complete for {client_name}"
      - Signals Step 4 (operations generator) that the client is ready
    """

    name = "tasks"

    def __init__(self, db_path: str = "sparkle_shine.db"):
        self.db_path = db_path
        self.logger = logger
        self._maria_id: Optional[str] = None  # cached after first lookup

    def execute(self, dry_run: bool = False) -> GeneratorResult:
        """Synchronous entry point called by the simulation engine dispatch loop."""
        import asyncio
        return asyncio.run(self.execute_one(dry_run=dry_run))

    async def execute_one(self, dry_run: bool = False) -> GeneratorResult:
        db = sqlite3.connect(self.db_path)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys = ON")
        today = date.today()

        try:
            maria_id = self._get_maria_id(db)
            candidates = self._get_incomplete_tasks(db)

            if not candidates:
                return GeneratorResult(success=True, message="No incomplete tasks")

            # Pick the first task that passes its probability roll
            task = self._pick_task(candidates, maria_id, today)
            if task is None:
                return GeneratorResult(success=True, message="No task selected this tick")

            task_id    = task["id"]
            title      = task["title"]
            client_id  = task["client_id"]

            # ── Check task dependency ────────────────────────────────────────
            blocker_title = _DEPENDENCIES.get(title)
            if blocker_title and client_id:
                blocker_done = db.execute(
                    """
                    SELECT 1 FROM tasks
                    WHERE client_id = ? AND title = ? AND status = 'completed'
                    LIMIT 1
                    """,
                    (client_id, blocker_title),
                ).fetchone()
                if not blocker_done:
                    return GeneratorResult(
                        success=True,
                        message=(
                            f"Blocked: '{title}' waiting for "
                            f"'{blocker_title}' (client {client_id})"
                        ),
                    )

            # ── Look up Asana GID ────────────────────────────────────────────
            asana_gid = get_tool_id(task_id, "asana", self.db_path)
            if not asana_gid:
                self.logger.warning(
                    "No Asana GID for task %s ('%s')", task_id, title
                )
                return GeneratorResult(
                    success=False,
                    message=f"Missing Asana GID for {task_id}",
                )

            # ── Complete in Asana ────────────────────────────────────────────
            self._complete_asana_task(asana_gid)

            # ── Update SQLite ────────────────────────────────────────────────
            completed_date_str = today.isoformat()
            db.execute(
                """
                UPDATE tasks
                SET status = 'completed', completed_date = ?
                WHERE id = ?
                """,
                (completed_date_str, task_id),
            )
            db.commit()

            self.logger.info("Task completed: %s ('%s')", task_id, title)

            # ── Check if onboarding is now fully complete ────────────────────
            if client_id:
                self._check_onboarding_complete(db, client_id)

            return GeneratorResult(
                success=True,
                message=f"Completed: '{title}' ({task_id})",
            )

        except Exception as e:
            db.rollback()
            self.logger.error("TaskCompletionGenerator.execute_one failed: %s", e)
            raise

        finally:
            db.close()

    # -------------------------------------------------------------------------
    # Selection helpers
    # -------------------------------------------------------------------------

    def _get_maria_id(self, db: sqlite3.Connection) -> Optional[str]:
        if self._maria_id is not None:
            return self._maria_id
        row = db.execute(
            """
            SELECT id FROM employees
            WHERE first_name = 'Maria' AND last_name = 'Gonzalez'
            LIMIT 1
            """
        ).fetchone()
        if row:
            self._maria_id = row["id"]
        return self._maria_id

    def _get_incomplete_tasks(self, db: sqlite3.Connection) -> list[dict]:
        """Return all incomplete tasks ordered oldest-first."""
        cursor = db.execute(
            """
            SELECT id, title, assignee_employee_id, client_id,
                   due_date, status, project_name
            FROM tasks
            WHERE status IN ('not_started', 'in_progress', 'overdue')
            ORDER BY created_at ASC
            """
        )
        return [dict(row) for row in cursor.fetchall()]

    def _pick_task(
        self,
        candidates: list[dict],
        maria_id: Optional[str],
        today: date,
    ) -> Optional[dict]:
        """Iterate oldest-first; return the first task that passes its probability roll."""
        for task in candidates:
            prob = self._completion_probability(task, maria_id, today)
            if random.random() < prob:
                return task
        return None

    def _completion_probability(
        self,
        task: dict,
        maria_id: Optional[str],
        today: date,
    ) -> float:
        base = _MARIA_RATE if task["assignee_employee_id"] == maria_id else _DEFAULT_RATE

        # Onboarding boost: first 3 tasks block downstream work
        if task["title"] in _ONBOARDING_TITLES:
            base *= _ONBOARDING_BOOST

        # Overdue boost: people eventually catch up on late tasks
        if task["status"] == "overdue" or self._is_overdue(task, today):
            base *= _OVERDUE_BOOST

        return min(base, 1.0)

    def _is_overdue(self, task: dict, today: date) -> bool:
        due_str = task.get("due_date")
        if not due_str:
            return False
        try:
            return date.fromisoformat(due_str) < today
        except ValueError:
            return False

    # -------------------------------------------------------------------------
    # Asana API call
    # -------------------------------------------------------------------------

    def _complete_asana_task(self, asana_gid: str) -> None:
        """Mark a task completed via the Asana Python SDK."""
        api_client = get_client("asana")
        tasks_api = asana.TasksApi(api_client)

        throttler.wait()
        try:
            tasks_api.update_task({"data": {"completed": True}}, asana_gid, {})
        except AsanaApiException as exc:
            if exc.status == 401:
                raise TokenExpiredError(f"Asana token expired: {exc}") from exc
            if exc.status == 429:
                retry_after = int(exc.headers.get("Retry-After", 5))
                sleep(retry_after)
                return self._complete_asana_task(asana_gid)
            raise RuntimeError(f"Asana API {exc.status}: {str(exc.body)[:300]}") from exc

    # -------------------------------------------------------------------------
    # Onboarding completion check
    # -------------------------------------------------------------------------

    def _check_onboarding_complete(self, db: sqlite3.Connection, client_id: str) -> None:
        """If all onboarding tasks for this client are complete, activate the
        client and log the handoff signal for the operations generator (Step 4).
        """
        total_row = db.execute(
            """
            SELECT COUNT(*) AS n FROM tasks
            WHERE client_id = ?
              AND project_name = 'Client Success'
              AND title IN (?, ?, ?)
            """,
            (client_id, *_ONBOARDING_TITLES),
        ).fetchone()
        total = total_row["n"] if total_row else 0

        if total == 0:
            return  # this client has no onboarding tasks tracked here

        completed_row = db.execute(
            """
            SELECT COUNT(*) AS n FROM tasks
            WHERE client_id = ?
              AND project_name = 'Client Success'
              AND title IN (?, ?, ?)
              AND status = 'completed'
            """,
            (client_id, *_ONBOARDING_TITLES),
        ).fetchone()
        completed = completed_row["n"] if completed_row else 0

        if completed < total:
            return  # still pending tasks

        # All onboarding tasks done — activate client
        client_row = db.execute(
            "SELECT first_name, last_name, company_name, status FROM clients WHERE id = ?",
            (client_id,),
        ).fetchone()

        if client_row and client_row["status"] != "active":
            client_name = (
                client_row["company_name"]
                or f"{client_row['first_name']} {client_row['last_name']}"
            )
            db.execute(
                "UPDATE clients SET status = 'active' WHERE id = ?",
                (client_id,),
            )
            db.commit()
            self.logger.info("Onboarding complete for %s", client_name)
