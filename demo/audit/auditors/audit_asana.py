"""
Asana auditor -- compares Asana tasks against SQLite canonical records.

Overdue consistency check: the count of overdue tasks in Asana MUST match
SQLite for the briefing's task metrics to be credible.
"""

from __future__ import annotations

import datetime
import os
import sys
import time
from typing import Optional

_PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import asana
import asana.rest

from auth.simple_clients import get_asana_client
from seeding.utils.throttler import ASANA
from demo.audit.cross_tool_audit import AuditFinding, AuditSample, ToolAuditResult

_OPT_FIELDS = "name,completed,due_on,assignee.name,memberships.section.name"

# SQLite task status → Asana completed flag
_COMPLETED_MAP = {
    "completed": True,
    "not_started": False,
    "in_progress": False,
    "overdue": False,
}


def _normalize_str(s) -> str:
    return str(s or "").strip().lower()


def _compare(
    entity_type: str,
    canonical_id: str,
    tool_id: str,
    field: str,
    expected: str,
    actual: str,
) -> AuditFinding:
    if expected == actual:
        return AuditFinding(
            severity="match",
            entity_type=entity_type,
            canonical_id=canonical_id,
            tool_id=tool_id,
            field=field,
            expected=expected,
            actual=actual,
            message=f"{canonical_id}: {field} matches ({expected!r})",
        )
    return AuditFinding(
        severity="mismatch",
        entity_type=entity_type,
        canonical_id=canonical_id,
        tool_id=tool_id,
        field=field,
        expected=expected,
        actual=actual,
        message=(
            f"{canonical_id}: {field} mismatch. "
            f"SQLite={expected!r}, Asana={actual!r}"
        ),
    )


class AsanaAuditor:
    def __init__(self, db_path: str):
        self.db_path = db_path

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def audit(self, sample: AuditSample) -> ToolAuditResult:
        """Check Asana tasks against SQLite."""
        start = time.time()
        findings: list[AuditFinding] = []
        records_checked = 0

        asana_client = get_asana_client()
        tasks_api = asana.TasksApi(asana_client)

        # ---- Individual task checks ----
        for t in sample.tasks:
            asana_gid = t.get("asana_gid")
            if not asana_gid:
                findings.append(AuditFinding(
                    severity="missing",
                    entity_type="task",
                    canonical_id=t["id"],
                    message=f"Task {t['id']} has no Asana mapping",
                ))
                continue

            try:
                ASANA.wait()
                task_data = tasks_api.get_task(
                    asana_gid,
                    opts={"opt_fields": _OPT_FIELDS},
                )
            except asana.rest.ApiException as exc:
                if exc.status == 404:
                    findings.append(AuditFinding(
                        severity="missing",
                        entity_type="task",
                        canonical_id=t["id"],
                        tool_id=asana_gid,
                        message=f"Task {t['id']} (Asana {asana_gid}) not found in Asana",
                    ))
                else:
                    findings.append(AuditFinding(
                        severity="missing",
                        entity_type="task",
                        canonical_id=t["id"],
                        tool_id=asana_gid,
                        message=f"Task {t['id']}: Asana API error {exc.status}: {exc.reason}",
                    ))
                continue
            except Exception as exc:
                findings.append(AuditFinding(
                    severity="missing",
                    entity_type="task",
                    canonical_id=t["id"],
                    tool_id=asana_gid,
                    message=f"Task {t['id']}: Asana API error: {exc}",
                ))
                continue

            records_checked += 1

            # name
            expected_name = _normalize_str(t.get("title", ""))
            actual_name = _normalize_str(task_data.get("name", ""))
            findings.append(_compare("task", t["id"], asana_gid, "name",
                                     expected_name, actual_name))

            # completed status
            expected_completed = str(_COMPLETED_MAP.get(
                _normalize_str(t.get("status", "")), False
            )).lower()
            actual_completed = str(task_data.get("completed", False)).lower()
            findings.append(_compare("task", t["id"], asana_gid, "completed",
                                     expected_completed, actual_completed))

            # due date
            expected_due = _normalize_str(t.get("due_date", ""))
            actual_due = _normalize_str(task_data.get("due_on", ""))
            if expected_due or actual_due:
                findings.append(_compare("task", t["id"], asana_gid, "due_on",
                                         expected_due, actual_due))

            # assignee name -- compare against SQLite employee name
            expected_assignee = self._resolve_assignee_name(t.get("assignee_employee_id"))
            actual_assignee_obj = task_data.get("assignee")
            actual_assignee = _normalize_str(
                actual_assignee_obj.get("name", "") if actual_assignee_obj else ""
            )
            if expected_assignee or actual_assignee:
                findings.append(_compare("task", t["id"], asana_gid, "assignee",
                                         expected_assignee, actual_assignee))

        # ---- Overdue consistency check ----
        overdue_findings = self._check_overdue_count(tasks_api)
        findings.extend(overdue_findings)

        return ToolAuditResult(
            tool_name="asana",
            records_checked=records_checked,
            findings=findings,
            duration_seconds=time.time() - start,
        )

    def _resolve_assignee_name(self, employee_id: Optional[str]) -> str:
        if not employee_id:
            return ""
        try:
            from database.schema import get_connection
            conn = get_connection(self.db_path)
            row = conn.execute(
                "SELECT first_name, last_name FROM employees WHERE id = ?",
                (employee_id,),
            ).fetchone()
            conn.close()
            if row:
                return _normalize_str(f"{row['first_name']} {row['last_name']}".strip())
        except Exception:
            pass
        return ""

    def _check_overdue_count(self, tasks_api: asana.TasksApi) -> list[AuditFinding]:
        """Count overdue tasks in Asana and compare against SQLite.

        These numbers MUST match for the briefing's task metrics to be credible.
        Queries all projects from config/tool_ids.json.
        """
        from database.schema import get_connection
        import json as _json

        findings: list[AuditFinding] = []
        today = datetime.date.today().isoformat()

        # SQLite overdue count
        try:
            conn = get_connection(self.db_path)
            row = conn.execute("""
                SELECT COUNT(*) AS cnt FROM tasks
                WHERE status != 'completed'
                  AND due_date < date('now')
                  AND due_date IS NOT NULL
            """).fetchone()
            conn.close()
            sqlite_overdue = row["cnt"] if row else 0
        except Exception as exc:
            findings.append(AuditFinding(
                severity="missing",
                entity_type="task_aggregate",
                canonical_id="AGGREGATE",
                message=f"Could not count SQLite overdue tasks: {exc}",
            ))
            return findings

        # Asana overdue count -- query each project and count incomplete tasks past due
        try:
            tool_ids_path = os.path.join(_PROJECT_ROOT, "config", "tool_ids.json")
            with open(tool_ids_path) as fh:
                tool_ids = _json.load(fh)
            project_gids = list(tool_ids.get("asana", {}).get("projects", {}).values())
        except Exception:
            project_gids = []

        asana_overdue = 0
        for gid in project_gids:
            try:
                ASANA.wait()
                tasks_page = tasks_api.get_tasks_for_project(
                    gid,
                    opts={
                        "opt_fields": "completed,due_on",
                        "completed_since": "now",  # incomplete tasks only
                    },
                )
                for task in tasks_page:
                    due = task.get("due_on") or ""
                    if not task.get("completed") and due and due < today:
                        asana_overdue += 1
            except Exception:
                # Skip project on error; don't fail the whole check
                continue

        if asana_overdue == sqlite_overdue:
            findings.append(AuditFinding(
                severity="match",
                entity_type="task_aggregate",
                canonical_id="AGGREGATE",
                field="overdue_count",
                expected=str(sqlite_overdue),
                actual=str(asana_overdue),
                message=(
                    f"Overdue task count matches: "
                    f"SQLite={sqlite_overdue}, Asana={asana_overdue}"
                ),
            ))
        else:
            findings.append(AuditFinding(
                severity="mismatch",
                entity_type="task_aggregate",
                canonical_id="AGGREGATE",
                field="overdue_count",
                expected=str(sqlite_overdue),
                actual=str(asana_overdue),
                message=(
                    f"Overdue task count mismatch: "
                    f"SQLite={sqlite_overdue}, Asana={asana_overdue}. "
                    f"Briefing task metrics will be inaccurate."
                ),
            ))

        return findings

    def fix_mismatch(self, finding: AuditFinding) -> bool:
        """Attempt to fix a simple field mismatch in Asana."""
        if not finding.tool_id or not finding.expected:
            return False

        asana_client = get_asana_client()
        tasks_api = asana.TasksApi(asana_client)

        if finding.entity_type != "task":
            return False

        if finding.field == "assignee":
            try:
                from auth.simple_clients import get_asana_client as _get_client
                import json as _json
                with open(os.path.join(_PROJECT_ROOT, "config", "tool_ids.json")) as _f:
                    _workspace_gid = _json.load(_f)["asana"]["workspace_gid"]
                users_api = asana.UsersApi(asana_client)
                ASANA.wait()
                workspace_users = list(
                    users_api.get_users_for_workspace(
                        _workspace_gid,
                        opts={"opt_fields": "gid,name"},
                    )
                )
                name_to_gid = {u["name"].strip().lower(): u["gid"] for u in workspace_users}
                expected_gid = name_to_gid.get(finding.expected.strip().lower())
                if not expected_gid:
                    return False  # Employee not in Asana workspace
                ASANA.wait()
                tasks_api.update_task(
                    {"data": {"assignee": expected_gid}},
                    finding.tool_id,
                    opts={},
                )
                return True
            except Exception:
                return False

        if finding.field == "name":
            try:
                ASANA.wait()
                tasks_api.update_task(
                    {"data": {"name": finding.expected}},
                    finding.tool_id,
                    opts={},
                )
                return True
            except Exception:
                return False

        if finding.field == "due_on":
            try:
                ASANA.wait()
                tasks_api.update_task(
                    {"data": {"due_on": finding.expected or None}},
                    finding.tool_id,
                    opts={},
                )
                return True
            except Exception:
                return False

        if finding.field == "completed":
            try:
                completed_val = finding.expected.lower() == "true"
                ASANA.wait()
                tasks_api.update_task(
                    {"data": {"completed": completed_val}},
                    finding.tool_id,
                    opts={},
                )
                return True
            except Exception:
                return False

        return False
