"""seeding/generators/gen_tasks_events.py

Generates tasks, reviews, and calendar events for the Sparkle & Shine POC.
Also bootstraps employees and crews, and plants five analytical discovery patterns.

Run:
    python seeding/generators/gen_tasks_events.py
"""
from __future__ import annotations

import random
import sqlite3
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

DB_PATH = ROOT / "sparkle_shine.db"
TODAY = date(2026, 3, 17)

_RNG = random.Random(42)

from config.business import EMPLOYEES, CREWS
from seeding.utils.text_generator import generate_review_text


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _get_conn(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _table_count(cur: sqlite3.Cursor, table: str) -> int:
    return cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


# ---------------------------------------------------------------------------
# Step 0 — Bootstrap employees and crews
# ---------------------------------------------------------------------------

def bootstrap_employees_and_crews(conn: sqlite3.Connection) -> tuple[int, int]:
    """Insert all 18 employees and 4 crews if tables are empty. Returns (emp_count, crew_count).

    Handles circular FK: employees.crew_id → crews.id AND crews.lead_employee_id → employees.id.
    Strategy: insert crews with lead=NULL first, then employees, then update crew lead IDs.
    """
    cur = conn.cursor()

    emp_seeded = 0
    crew_seeded = 0

    if _table_count(cur, "employees") == 0:
        # Insert crews first with lead_employee_id=NULL to avoid circular FK violation
        for crew in CREWS:
            cur.execute(
                "INSERT OR IGNORE INTO crews (id, name, zone, lead_employee_id) VALUES (?,?,?,?)",
                (crew["id"], crew["name"], crew["zone"], None),
            )
        crew_seeded = len(CREWS)

        # Now insert all employees (crew_id references crews which now exist)
        for emp in EMPLOYEES:
            crew_id = None
            if emp.get("crew"):
                crew_letter = emp["crew"].lower()
                crew_id = f"crew-{crew_letter}"
            cur.execute(
                """INSERT OR IGNORE INTO employees
                   (id, first_name, last_name, role, crew_id, hire_date, status, hourly_rate)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (
                    emp["id"],
                    emp["first_name"],
                    emp["last_name"],
                    emp["role"],
                    crew_id,
                    emp["hire_date"],
                    emp["status"],
                    emp["hourly_rate"],
                ),
            )
        emp_seeded = len(EMPLOYEES)

        # Now update crews with the correct lead_employee_id
        for crew in CREWS:
            if crew["lead_id"]:
                cur.execute(
                    "UPDATE crews SET lead_employee_id=? WHERE id=?",
                    (crew["lead_id"], crew["id"]),
                )

        conn.commit()
    else:
        emp_seeded = _table_count(cur, "employees")
        crew_seeded = _table_count(cur, "crews")

    return emp_seeded, crew_seeded


# ---------------------------------------------------------------------------
# Step 1 — Plant Barton Creek upsell signals
# ---------------------------------------------------------------------------

def plant_barton_creek_upsell_signals(conn: sqlite3.Connection) -> int:
    """Update 20% of Barton Creek Medical Group jobs to include upsell signal in notes."""
    cur = conn.cursor()
    rows = cur.execute(
        "SELECT id, notes FROM jobs WHERE client_id = 'SS-CLIENT-0311' ORDER BY id"
    ).fetchall()

    n_to_update = max(1, round(len(rows) * 0.20))
    selected = _RNG.sample(rows, n_to_update)

    for row in selected:
        job_id = row["id"]
        existing_notes = row["notes"]
        if existing_notes:
            new_notes = existing_notes + " | add-on service requested"
        else:
            new_notes = "add-on service requested"
        cur.execute("UPDATE jobs SET notes=? WHERE id=?", (new_notes, job_id))

    conn.commit()
    return n_to_update


# ---------------------------------------------------------------------------
# Step 2 — Plant Aug-Sep 2025 complaint cluster
# ---------------------------------------------------------------------------

def plant_complaint_cluster(conn: sqlite3.Connection) -> list[str]:
    """Flag 3 completed Aug-Sep 2025 jobs with complaint notes. Returns their IDs."""
    cur = conn.cursor()
    rows = cur.execute(
        """SELECT id, notes FROM jobs
           WHERE status='completed'
             AND scheduled_date BETWEEN '2025-08-01' AND '2025-09-30'
           ORDER BY id"""
    ).fetchall()

    selected = _RNG.sample(rows, 3)
    complaint_job_ids = []

    for row in selected:
        job_id = row["id"]
        existing_notes = row["notes"]
        if existing_notes:
            new_notes = existing_notes + " | client complaint noted"
        else:
            new_notes = "client complaint noted"
        cur.execute("UPDATE jobs SET notes=? WHERE id=?", (new_notes, job_id))
        complaint_job_ids.append(job_id)

    conn.commit()
    return complaint_job_ids


# ---------------------------------------------------------------------------
# Step 3 — Plant Westlake cancellation cluster
# ---------------------------------------------------------------------------

def plant_westlake_cancellations(conn: sqlite3.Connection, rng: random.Random | None = None) -> int:
    """Cancel 3 active Westlake recurring agreements within a 14-day window."""
    if rng is None:
        rng = _RNG

    cur = conn.cursor()
    rows = cur.execute(
        """SELECT c.id AS client_id, ra.id AS agreement_id
           FROM clients c
           JOIN recurring_agreements ra ON ra.client_id = c.id
           WHERE c.neighborhood = 'Westlake'
             AND ra.status = 'active'
           ORDER BY c.id"""
    ).fetchall()

    selected = rng.sample(rows, 3)

    # Three cancellation dates within a 14-day span (Feb 10–24, 2026)
    cancel_dates = [
        date(2026, 2, 10),
        date(2026, 2, 14),
        date(2026, 2, 21),
    ]

    for i, row in enumerate(selected):
        cancel_date = cancel_dates[i]
        cancel_str = cancel_date.isoformat()

        cur.execute(
            "UPDATE recurring_agreements SET status='cancelled', end_date=? WHERE id=?",
            (cancel_str, row["agreement_id"]),
        )
        cur.execute(
            """UPDATE clients
               SET status='churned',
                   last_service_date=?,
                   notes = COALESCE(notes || ' | ', '') || 'Client cancelled recurring service — reason unclear'
               WHERE id=?""",
            (cancel_str, row["client_id"]),
        )

    conn.commit()
    return 3


# ---------------------------------------------------------------------------
# Step 4 — Plant referral proposal value premium
# ---------------------------------------------------------------------------

def plant_referral_proposal_premium(conn: sqlite3.Connection) -> int:
    """Increase monthly_value by 30% for non-won referral commercial proposals."""
    cur = conn.cursor()
    cur.execute(
        """UPDATE commercial_proposals
           SET monthly_value = ROUND(monthly_value * 1.30, 2)
           WHERE lead_id IN (
               SELECT id FROM leads
               WHERE source='referral' AND lead_type='commercial'
           )
             AND status != 'won'
             AND client_id IS NULL"""
    )
    n_updated = cur.rowcount
    conn.commit()
    return n_updated


# ---------------------------------------------------------------------------
# Step 5 — Generate Asana tasks
# ---------------------------------------------------------------------------

_COMMERCIAL_CHECKLIST = [
    ("Send welcome packet and signed contract copy", "high", "SS-EMP-001"),
    ("Complete facility walkthrough and cleaning assessment", "high", "SS-EMP-001"),
    ("Set up client portal access and credentials", "high", "SS-EMP-004"),
    ("Obtain facility access cards and alarm codes", "medium", "SS-EMP-004"),
    ("Assign and brief crew team lead", "medium", "SS-EMP-001"),
    ("Order specialized cleaning supplies", "medium", "SS-EMP-004"),
    ("Complete initial deep-clean walkthrough with client", "high", "SS-EMP-001"),
    ("Review and sign cleaning standards documentation", "high", "SS-EMP-001"),
    ("Configure recurring invoice and billing schedule", "medium", "SS-EMP-004"),
    ("Schedule 30-day check-in call", "medium", "SS-EMP-004"),
]

_RESIDENTIAL_CHECKLIST = [
    ("Send welcome email with service details", "SS-EMP-004"),
    ("Confirm first appointment date and time", "SS-EMP-004"),
    ("Add client to Mailchimp mailing list", "SS-EMP-004"),
    ("Create client profile in job management system", "SS-EMP-004"),
    ("Assign crew and neighborhood zone", "SS-EMP-001"),
    ("Verify address and access instructions", "SS-EMP-001"),
    ("Set up recurring service schedule", "SS-EMP-001"),
    ("Process first payment method on file", "SS-EMP-004"),
    ("Send post-first-service follow-up", "SS-EMP-004"),
]


def _make_task(
    task_id: str,
    title: str,
    project_name: str,
    assignee_id: str,
    client_id: str | None,
    due_date: str,
    status: str,
    priority: str,
    created_at: str,
    completed_date: str | None = None,
) -> dict:
    return {
        "id": task_id,
        "title": title,
        "description": None,
        "project_name": project_name,
        "assignee_employee_id": assignee_id,
        "client_id": client_id,
        "due_date": due_date,
        "completed_date": completed_date,
        "status": status,
        "priority": priority,
        "created_at": created_at,
    }


def _insert_tasks(conn: sqlite3.Connection, tasks: list[dict]) -> None:
    cur = conn.cursor()
    cur.executemany(
        """INSERT OR IGNORE INTO tasks
           (id, title, description, project_name, assignee_employee_id, client_id,
            due_date, completed_date, status, priority, created_at)
           VALUES (:id, :title, :description, :project_name, :assignee_employee_id,
                   :client_id, :due_date, :completed_date, :status, :priority, :created_at)""",
        tasks,
    )
    conn.commit()


def generate_commercial_onboarding_tasks(
    conn: sqlite3.Connection, task_counter_start: int
) -> tuple[list[dict], int]:
    """Generate 100 commercial onboarding tasks (10 clients × 10 items)."""
    cur = conn.cursor()
    commercial_clients = cur.execute(
        "SELECT id, first_service_date FROM clients WHERE client_type='commercial' ORDER BY id"
    ).fetchall()

    tasks = []
    task_counter = task_counter_start

    for client in commercial_clients:
        client_id = client["id"]
        first_service_date = date.fromisoformat(client["first_service_date"])
        days_since_win = (TODAY - first_service_date).days

        # Determine how many tasks are completed
        if days_since_win > 90:
            n_completed = 10
        elif days_since_win >= 30:
            n_completed = 8
        else:
            n_completed = 4

        created_at_date = first_service_date - timedelta(days=3)
        created_at = f"{created_at_date.isoformat()} 08:00:00"

        for idx, (title, priority, assignee_id) in enumerate(_COMMERCIAL_CHECKLIST):
            item_num = idx + 1
            task_id = f"SS-TASK-{task_counter:04d}"
            task_counter += 1

            # Due dates: items 1-5 → +30 days, items 6-10 → +60 days
            if item_num <= 5:
                due_date = (first_service_date + timedelta(days=30)).isoformat()
            else:
                due_date = (first_service_date + timedelta(days=60)).isoformat()

            if idx < n_completed:
                status = "completed"
                offset_days = _RNG.randint(2, 14)
                completed_date = (created_at_date + timedelta(days=offset_days)).isoformat()
            elif n_completed < 10:
                status = _RNG.choice(["not_started", "in_progress"])
                completed_date = None
            else:
                status = "completed"
                offset_days = _RNG.randint(2, 14)
                completed_date = (created_at_date + timedelta(days=offset_days)).isoformat()

            tasks.append(
                _make_task(
                    task_id=task_id,
                    title=title,
                    project_name="Client Success",
                    assignee_id=assignee_id,
                    client_id=client_id,
                    due_date=due_date,
                    status=status,
                    priority=priority,
                    created_at=created_at,
                    completed_date=completed_date,
                )
            )

    return tasks, task_counter


def generate_residential_onboarding_tasks(
    conn: sqlite3.Connection, task_counter_start: int
) -> tuple[list[dict], int]:
    """Generate residential onboarding tasks for the 30 most recent active residential clients."""
    cur = conn.cursor()
    recent_clients = cur.execute(
        """SELECT id, first_service_date FROM clients
           WHERE client_type='residential' AND status='active'
           ORDER BY first_service_date DESC
           LIMIT 30"""
    ).fetchall()

    tasks = []
    task_counter = task_counter_start

    for client in recent_clients:
        client_id = client["id"]
        first_service_date = date.fromisoformat(client["first_service_date"])
        days_since = (TODAY - first_service_date).days

        if days_since >= 14:
            n_completed = 9
        else:
            n_completed = _RNG.randint(3, 6)

        created_at_date = first_service_date - timedelta(days=1)
        created_at = f"{created_at_date.isoformat()} 08:00:00"

        for idx, (title, assignee_id) in enumerate(_RESIDENTIAL_CHECKLIST):
            task_id = f"SS-TASK-{task_counter:04d}"
            task_counter += 1

            due_date = (first_service_date + timedelta(days=14)).isoformat()

            if idx < n_completed:
                status = "completed"
                offset_days = _RNG.randint(1, 7)
                completed_date = (created_at_date + timedelta(days=offset_days)).isoformat()
            else:
                status = _RNG.choice(["not_started", "in_progress"])
                completed_date = None

            tasks.append(
                _make_task(
                    task_id=task_id,
                    title=title,
                    project_name="Client Success",
                    assignee_id=assignee_id,
                    client_id=client_id,
                    due_date=due_date,
                    status=status,
                    priority="medium",
                    created_at=created_at,
                    completed_date=completed_date,
                )
            )

    return tasks, task_counter


def generate_back_office_tasks(
    conn: sqlite3.Connection, task_counter_start: int
) -> tuple[list[dict], int]:
    """Generate 80 back-office administrative tasks."""
    tasks = []
    task_counter = task_counter_start

    months_apr_2025_to_mar_2026 = [
        date(2025 if m >= 4 else 2026, m if m >= 4 else m, 1)
        for m in list(range(4, 13)) + list(range(1, 4))
    ]

    month_names = [
        "April 2025", "May 2025", "June 2025", "July 2025",
        "August 2025", "September 2025", "October 2025", "November 2025",
        "December 2025", "January 2026", "February 2026", "March 2026",
    ]

    def make_back_office(
        title: str,
        assignee_id: str,
        due_date: str,
        priority: str = "medium",
        created_at: str | None = None,
    ) -> dict:
        nonlocal task_counter
        task_id = f"SS-TASK-{task_counter:04d}"
        task_counter += 1
        if created_at is None:
            due_d = date.fromisoformat(due_date)
            created_d = due_d - timedelta(days=_RNG.randint(14, 45))
            created_at = f"{created_d.isoformat()} 08:00:00"
        return _make_task(
            task_id=task_id,
            title=title,
            project_name="Admin & Operations",
            assignee_id=assignee_id,
            client_id=None,
            due_date=due_date,
            status="not_started",
            priority=priority,
            created_at=created_at,
        )

    # 12 monthly supply orders (Patricia)
    for i, (month_date, month_name) in enumerate(zip(months_apr_2025_to_mar_2026, month_names)):
        due = (month_date + timedelta(days=5)).isoformat()
        tasks.append(make_back_office(f"Monthly supply order: {month_name}", "SS-EMP-004", due))

    # 4 vehicle maintenance tasks (Patricia, quarterly)
    crew_labels = ["A", "B", "C", "D"]
    vehicle_dates = ["2025-06-15", "2025-09-15", "2025-12-15", "2026-03-15"]
    for crew_label, v_date in zip(crew_labels, vehicle_dates):
        tasks.append(make_back_office(f"Vehicle maintenance — Crew {crew_label} van", "SS-EMP-004", v_date))

    # Insurance renewals (Maria, high priority)
    tasks.append(make_back_office("Insurance renewal — general liability", "SS-EMP-001", "2026-04-15", priority="high"))
    tasks.append(make_back_office("Insurance renewal — workers comp", "SS-EMP-001", "2026-04-30", priority="high"))

    # Business license (Maria)
    tasks.append(make_back_office("Business license renewal — City of Austin", "SS-EMP-001", "2026-05-01", priority="high"))

    # 8 equipment repair tasks (Patricia or Maria)
    equipment_types = [
        "commercial vacuum Crew A",
        "steam cleaner Crew B",
        "floor buffer Crew C",
        "pressure washer",
        "industrial mop system",
        "HEPA filter replacement Crew A",
        "carpet extractor",
        "auto-scrubber Crew D",
    ]
    equip_dates = [
        "2025-06-01", "2025-07-15", "2025-08-20", "2025-09-10",
        "2025-10-05", "2025-11-12", "2025-12-01", "2026-01-20",
    ]
    equip_assignees = [
        "SS-EMP-004", "SS-EMP-001", "SS-EMP-004", "SS-EMP-001",
        "SS-EMP-004", "SS-EMP-004", "SS-EMP-001", "SS-EMP-004",
    ]
    for etype, edate, eassignee in zip(equipment_types, equip_dates, equip_assignees):
        tasks.append(make_back_office(f"Equipment repair: {etype}", eassignee, edate))

    # 4 quarterly sales tax filings (Sandra / Patricia alternating)
    quarters = [
        ("Q1 2025 (Jan–Mar)", "2025-04-30", "SS-EMP-006"),
        ("Q2 2025 (Apr–Jun)", "2025-07-31", "SS-EMP-004"),
        ("Q3 2025 (Jul–Sep)", "2025-10-31", "SS-EMP-006"),
        ("Q4 2025 (Oct–Dec)", "2026-01-31", "SS-EMP-004"),
    ]
    for q_label, q_due, q_assignee in quarters:
        tasks.append(make_back_office(f"Sales tax filing — {q_label}", q_assignee, q_due))

    # 12 payroll processing tasks (Patricia, monthly)
    for i, (month_date, month_name) in enumerate(zip(months_apr_2025_to_mar_2026, month_names)):
        # Payroll processed on the 15th
        pay_date = date(month_date.year, month_date.month, 15)
        tasks.append(make_back_office(f"Payroll processing — {month_name}", "SS-EMP-004", pay_date.isoformat()))

    # Additional tasks to reach 80 total
    # After 43 tasks above, we need 37 more misc tasks
    misc_tasks = [
        ("Crew uniforms reorder", "SS-EMP-004", "2025-05-01"),
        ("Background check renewals — Crew A", "SS-EMP-004", "2025-07-01"),
        ("Background check renewals — Crew B", "SS-EMP-004", "2025-07-08"),
        ("Background check renewals — Crew C", "SS-EMP-004", "2025-07-15"),
        ("Background check renewals — Crew D", "SS-EMP-004", "2025-07-22"),
        ("Google Business profile update", "SS-EMP-001", "2025-06-01"),
        ("Yelp review response follow-up", "SS-EMP-001", "2025-08-01"),
        ("Yelp review response follow-up", "SS-EMP-001", "2025-10-01"),
        ("Yelp review response follow-up", "SS-EMP-001", "2026-01-01"),
        ("OSHA compliance training — Crew A", "SS-EMP-001", "2025-05-15"),
        ("OSHA compliance training — Crew B", "SS-EMP-001", "2025-05-22"),
        ("OSHA compliance training — Crew C", "SS-EMP-001", "2025-05-29"),
        ("OSHA compliance training — Crew D", "SS-EMP-001", "2025-06-05"),
        ("Update employee handbook 2025", "SS-EMP-001", "2025-09-01"),
        ("Vendor contract review — Austin Janitorial Supply", "SS-EMP-004", "2025-10-01"),
        ("Vendor contract review — Hill Country Equipment Rental", "SS-EMP-004", "2025-10-08"),
        ("Vendor contract review — Lone Star Chemical Distributors", "SS-EMP-004", "2025-10-15"),
        ("Workers comp annual audit prep", "SS-EMP-006", "2026-02-01"),
        ("Annual team performance reviews", "SS-EMP-001", "2026-03-01"),
        ("Q1 2026 budget review", "SS-EMP-001", "2026-04-10"),
        ("Q2 2025 crew performance check", "SS-EMP-001", "2025-06-30"),
        ("Q3 2025 crew performance check", "SS-EMP-001", "2025-09-30"),
        ("Q4 2025 crew performance check", "SS-EMP-001", "2025-12-31"),
        ("Q1 2026 crew performance check", "SS-EMP-001", "2026-03-31"),
        ("Update Jobber client records", "SS-EMP-004", "2025-07-01"),
        ("Update Jobber client records", "SS-EMP-004", "2025-10-01"),
        ("Update Jobber client records", "SS-EMP-004", "2026-01-05"),
        ("Crew scheduling review — summer 2025", "SS-EMP-004", "2025-05-20"),
        ("Crew scheduling review — fall 2025", "SS-EMP-004", "2025-08-20"),
        ("Crew scheduling review — winter 2026", "SS-EMP-004", "2025-11-20"),
        ("Annual safety audit", "SS-EMP-001", "2025-11-01"),
        ("Renew cleaning industry certification", "SS-EMP-001", "2025-12-01"),
        ("Finalize FY2026 growth plan", "SS-EMP-001", "2026-01-15"),
        ("Process year-end tax documents", "SS-EMP-006", "2026-01-31"),
        ("New hire onboarding — Q3 2025 checklist", "SS-EMP-004", "2025-08-01"),
        ("Client satisfaction survey — Q3 2025", "SS-EMP-005", "2025-10-15"),
        ("Client satisfaction survey — Q4 2025", "SS-EMP-005", "2026-01-15"),
    ]
    for title, assignee_id, due in misc_tasks:
        if len(tasks) >= 80:
            break
        tasks.append(make_back_office(title, assignee_id, due))

    # Post-process: apply overdue rates
    # Separate by assignee
    maria_tasks = [t for t in tasks if t["assignee_employee_id"] == "SS-EMP-001"]
    patricia_tasks = [t for t in tasks if t["assignee_employee_id"] == "SS-EMP-004"]
    other_tasks = [t for t in tasks if t["assignee_employee_id"] not in ("SS-EMP-001", "SS-EMP-004")]

    # Mark 40% of Maria's tasks as overdue
    n_maria_overdue = max(1, round(len(maria_tasks) * 0.40))
    maria_overdue_candidates = [
        t for t in maria_tasks
        if t["due_date"] < TODAY.isoformat()
    ]
    if len(maria_overdue_candidates) >= n_maria_overdue:
        maria_overdue_selected = _RNG.sample(maria_overdue_candidates, n_maria_overdue)
    else:
        maria_overdue_selected = maria_overdue_candidates

    for t in maria_overdue_selected:
        # Assign an overdue due date in the past range
        overdue_date = date(2025, _RNG.randint(6, 12), _RNG.randint(1, 28))
        t["due_date"] = overdue_date.isoformat()
        t["status"] = "overdue"

    # Mark 10% of Patricia's tasks as overdue
    n_patricia_overdue = max(1, round(len(patricia_tasks) * 0.10))
    patricia_overdue_candidates = [
        t for t in patricia_tasks
        if t["due_date"] < TODAY.isoformat()
    ]
    if len(patricia_overdue_candidates) >= n_patricia_overdue:
        patricia_overdue_selected = _RNG.sample(patricia_overdue_candidates, n_patricia_overdue)
    else:
        patricia_overdue_selected = patricia_overdue_candidates

    for t in patricia_overdue_selected:
        overdue_date = date(2025, _RNG.randint(6, 12), _RNG.randint(1, 28))
        t["due_date"] = overdue_date.isoformat()
        t["status"] = "overdue"

    # Mark 5% of other assignees' tasks as overdue
    n_other_overdue = max(1, round(len(other_tasks) * 0.05))
    other_overdue_candidates = [
        t for t in other_tasks
        if t["due_date"] < TODAY.isoformat()
    ]
    if len(other_overdue_candidates) >= n_other_overdue:
        other_overdue_selected = _RNG.sample(other_overdue_candidates, n_other_overdue)
    else:
        other_overdue_selected = other_overdue_candidates

    for t in other_overdue_selected:
        overdue_date = date(2025, _RNG.randint(6, 12), _RNG.randint(1, 28))
        t["due_date"] = overdue_date.isoformat()
        t["status"] = "overdue"

    # Mark completed tasks that are in the past (non-overdue) as completed
    for t in tasks:
        if t["status"] == "not_started" and t["due_date"] < TODAY.isoformat():
            t["status"] = "completed"
            due_d = date.fromisoformat(t["due_date"])
            completed_d = due_d - timedelta(days=_RNG.randint(0, 3))
            t["completed_date"] = completed_d.isoformat()

    return tasks[:80], task_counter


def generate_sales_pipeline_tasks(
    conn: sqlite3.Connection, task_counter_start: int
) -> tuple[list[dict], int]:
    """Generate 50 sales pipeline tasks."""
    cur = conn.cursor()
    tasks = []
    task_counter = task_counter_start

    def make_sales_task(
        title: str,
        assignee_id: str,
        due_date: str,
        status: str,
        client_id: str | None = None,
        completed_date: str | None = None,
        priority: str = "medium",
    ) -> dict:
        nonlocal task_counter
        task_id = f"SS-TASK-{task_counter:04d}"
        task_counter += 1
        due_d = date.fromisoformat(due_date)
        created_d = due_d - timedelta(days=_RNG.randint(7, 30))
        created_at = f"{created_d.isoformat()} 08:00:00"
        return _make_task(
            task_id=task_id,
            title=title,
            project_name="Sales Pipeline Tasks",
            assignee_id=assignee_id,
            client_id=client_id,
            due_date=due_date,
            status=status,
            priority=priority,
            created_at=created_at,
            completed_date=completed_date,
        )

    # 10 follow-up calls for lost proposals
    lost_proposals = cur.execute(
        """SELECT p.id, p.title, l.company_name, p.decision_date
           FROM commercial_proposals p
           LEFT JOIN leads l ON l.id = p.lead_id
           WHERE p.status = 'lost'
           ORDER BY p.id
           LIMIT 10"""
    ).fetchall()

    for prop in lost_proposals:
        company = prop["company_name"] or prop["title"]
        decision_date = prop["decision_date"] or "2025-12-01"
        due_date = (date.fromisoformat(decision_date) + timedelta(days=14)).isoformat()
        completed_date = due_date
        tasks.append(make_sales_task(
            title=f"Follow-up call — {company}",
            assignee_id="SS-EMP-005",
            due_date=due_date,
            status="completed",
            completed_date=completed_date,
        ))

    # 15 proposal follow-up tasks for sent/negotiating proposals
    active_proposals = cur.execute(
        """SELECT p.id, p.title, l.company_name, p.sent_date
           FROM commercial_proposals p
           LEFT JOIN leads l ON l.id = p.lead_id
           WHERE p.status IN ('sent', 'negotiating')
           ORDER BY p.id"""
    ).fetchall()

    # Pad with draft proposals if needed
    if len(active_proposals) < 15:
        draft_proposals = cur.execute(
            """SELECT p.id, p.title, l.company_name, p.sent_date
               FROM commercial_proposals p
               LEFT JOIN leads l ON l.id = p.lead_id
               WHERE p.status = 'draft'
               ORDER BY p.id
               LIMIT ?""",
            (15 - len(active_proposals),),
        ).fetchall()
        active_proposals = list(active_proposals) + list(draft_proposals)

    for prop in active_proposals[:15]:
        company = prop["company_name"] or prop["title"] or "Prospect"
        sent_date = prop["sent_date"] or TODAY.isoformat()
        due_date = (date.fromisoformat(sent_date) + timedelta(days=7)).isoformat()
        status = _RNG.choice(["not_started", "in_progress"])
        tasks.append(make_sales_task(
            title=f"Proposal follow-up — {company}",
            assignee_id="SS-EMP-005",
            due_date=due_date,
            status=status,
        ))

    # 15 initial outreach tasks for new/qualified leads
    new_leads = cur.execute(
        """SELECT id, company_name, status
           FROM leads
           WHERE lead_type='commercial' AND status IN ('new', 'qualified', 'contacted')
           ORDER BY id
           LIMIT 15"""
    ).fetchall()

    for lead in new_leads[:15]:
        company = lead["company_name"] or "Prospect"
        due_date = (TODAY + timedelta(days=_RNG.randint(3, 21))).isoformat()
        status = _RNG.choice(["not_started", "in_progress"])
        tasks.append(make_sales_task(
            title=f"Initial outreach — {company}",
            assignee_id="SS-EMP-005",
            due_date=due_date,
            status=status,
        ))

    # 10 contract review tasks for won proposals
    won_proposals = cur.execute(
        """SELECT p.id, p.title, p.client_id, p.decision_date
           FROM commercial_proposals p
           WHERE p.status = 'won'
           ORDER BY p.id
           LIMIT 10"""
    ).fetchall()

    for prop in won_proposals:
        decision_date = prop["decision_date"] or "2025-11-01"
        due_date = (date.fromisoformat(decision_date) + timedelta(days=7)).isoformat()
        completed_date = due_date
        tasks.append(make_sales_task(
            title="Contract review",
            assignee_id="SS-EMP-005",
            due_date=due_date,
            status="completed",
            client_id=prop["client_id"],
            completed_date=completed_date,
        ))

    return tasks[:50], task_counter


# ---------------------------------------------------------------------------
# Step 6 — Generate reviews
# ---------------------------------------------------------------------------

_CREW_RATING_WEIGHTS: dict[str, list[float]] = {
    "crew-a": [0.01, 0.01, 0.05, 0.13, 0.80],  # [1★, 2★, 3★, 4★, 5★]
    "crew-b": [0.03, 0.05, 0.12, 0.30, 0.50],
    "crew-c": [0.03, 0.05, 0.12, 0.30, 0.50],
    "crew-d": [0.02, 0.03, 0.08, 0.22, 0.65],
}


def _sample_rating(crew_id: str, weekday: int) -> int:
    """Sample a 1–5 star rating for the given crew, with Tue/Wed adjustment."""
    weights = list(_CREW_RATING_WEIGHTS.get(crew_id, _CREW_RATING_WEIGHTS["crew-b"]))
    ratings = [1, 2, 3, 4, 5]

    # Tue (weekday=1) or Wed (weekday=2): reduce 1★ and 2★ by 30%
    if weekday in (1, 2):
        weights[0] *= 0.70  # 1-star
        weights[1] *= 0.70  # 2-star
        total = sum(weights)
        weights = [w / total for w in weights]

    return _RNG.choices(ratings, weights=weights, k=1)[0]


def generate_reviews(
    conn: sqlite3.Connection,
    complaint_job_ids: list[str],
) -> list[dict]:
    """Generate reviews for ~17% of completed jobs."""
    cur = conn.cursor()

    completed_jobs = cur.execute(
        """SELECT id, client_id, crew_id, service_type_id, scheduled_date
           FROM jobs
           WHERE status='completed'
           ORDER BY id"""
    ).fetchall()

    n_total_completed = len(completed_jobs)
    n_reviews = round(n_total_completed * 0.17)

    selected_jobs = _RNG.sample(list(completed_jobs), n_reviews)

    # Ensure all 3 complaint jobs are in the review set
    selected_ids = {j["id"] for j in selected_jobs}
    complaint_jobs_to_add = []
    for cjid in complaint_job_ids:
        if cjid not in selected_ids:
            match = cur.execute("SELECT * FROM jobs WHERE id=?", (cjid,)).fetchone()
            if match:
                complaint_jobs_to_add.append(match)

    if complaint_jobs_to_add:
        # Replace last N entries to maintain target count
        selected_jobs = list(selected_jobs)
        for i, cj in enumerate(complaint_jobs_to_add):
            selected_jobs[-(i + 1)] = cj

    review_text_cache: dict[tuple[int, str], str | None] = {}

    def get_review_text(rating: int, service_type: str) -> str | None:
        key = (rating, service_type)
        if key in review_text_cache:
            return review_text_cache[key]

        if rating in (1, 2):
            text = generate_review_text(rating, service_type)
        elif rating in (4, 5):
            if _RNG.random() < 0.20:
                text = generate_review_text(rating, service_type)
            else:
                text = None
        else:  # 3-star: never generate text
            text = None

        review_text_cache[key] = text
        return text

    reviews = []
    complaint_id_set = set(complaint_job_ids)
    platforms = ["Google", "Yelp", "internal"]
    platform_weights = [60, 30, 10]

    for i, job in enumerate(selected_jobs):
        job_id = job["id"]
        client_id = job["client_id"]
        crew_id = job["crew_id"] or "crew-b"
        service_type = job["service_type_id"]
        scheduled_date = date.fromisoformat(job["scheduled_date"])
        weekday = scheduled_date.weekday()

        # Force 1-star for complaint jobs
        if job_id in complaint_id_set:
            rating = 1
        else:
            rating = _sample_rating(crew_id, weekday)

        review_text = get_review_text(rating, service_type)

        platform = _RNG.choices(platforms, weights=platform_weights, k=1)[0]

        review_date = scheduled_date + timedelta(days=_RNG.randint(1, 3))

        review_id = f"SS-REV-{i + 1:04d}"
        reviews.append({
            "id": review_id,
            "client_id": client_id,
            "job_id": job_id,
            "rating": rating,
            "review_text": review_text,
            "platform": platform,
            "review_date": review_date.isoformat(),
            "response_text": None,
            "response_date": None,
        })

    # Mark review_requested=1 on all reviewed jobs
    reviewed_job_ids = [job["id"] for job in selected_jobs]
    cur.executemany(
        "UPDATE jobs SET review_requested=1 WHERE id=?",
        [(jid,) for jid in reviewed_job_ids],
    )
    conn.commit()

    return reviews, n_total_completed


def _insert_reviews(conn: sqlite3.Connection, reviews: list[dict]) -> None:
    cur = conn.cursor()
    cur.executemany(
        """INSERT OR IGNORE INTO reviews
           (id, client_id, job_id, rating, review_text, platform, review_date, response_text, response_date)
           VALUES (:id, :client_id, :job_id, :rating, :review_text, :platform,
                   :review_date, :response_text, :response_date)""",
        reviews,
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Step 7 — Generate calendar events
# ---------------------------------------------------------------------------

def _all_employee_names() -> str:
    """Return comma-separated list of all employee full names."""
    names = [f"{e['first_name']} {e['last_name']}" for e in EMPLOYEES]
    return ",".join(names)


def _crew_lead_name(crew_letter: str) -> str:
    """Return the full name of the crew lead for the given letter (A/B/C/D)."""
    crew_id = f"crew-{crew_letter.lower()}"
    for crew in CREWS:
        if crew["id"] == crew_id and crew["lead_id"]:
            for emp in EMPLOYEES:
                if emp["id"] == crew["lead_id"]:
                    return f"{emp['first_name']} {emp['last_name']}"
    return ""


def _first_monday_on_or_after(d: date) -> date:
    """Return the first Monday on or after the given date."""
    days_ahead = (0 - d.weekday()) % 7
    return d + timedelta(days=days_ahead)


def _first_tuesday_of_month(year: int, month: int) -> date:
    """Return the first Tuesday of the given month."""
    d = date(year, month, 1)
    days_ahead = (1 - d.weekday()) % 7
    return d + timedelta(days=days_ahead)


def _last_friday_of_month(year: int, month: int) -> date:
    """Return the last Friday of the given month."""
    # Go to last day of month
    if month == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, month + 1, 1)
    last_day = next_month - timedelta(days=1)
    # Find last Friday
    days_back = (last_day.weekday() - 4) % 7
    return last_day - timedelta(days=days_back)


def generate_calendar_events(conn: sqlite3.Connection) -> list[dict]:
    """Generate all 7 series of calendar events."""
    cur = conn.cursor()
    events = []
    cal_counter = 1

    def make_event(
        title: str,
        event_type: str,
        start_dt: str,
        end_dt: str,
        attendees: str,
        related_client_id: str | None = None,
        notes: str | None = None,
    ) -> dict:
        nonlocal cal_counter
        event_id = f"SS-CAL-{cal_counter:04d}"
        cal_counter += 1
        return {
            "id": event_id,
            "title": title,
            "event_type": event_type,
            "start_datetime": start_dt,
            "end_datetime": end_dt,
            "attendees": attendees,
            "related_client_id": related_client_id,
            "notes": notes,
        }

    # ------------------------------------------------------------------ #
    # Series 1 — Weekly 1:1: Maria & Patricia (52 events, every Monday)
    # ------------------------------------------------------------------ #
    series1_start = date(2025, 4, 7)  # First Monday in April 2025
    series1_end = date(2026, 3, 16)   # Last Monday before TODAY (Mar 17, 2026)

    current = series1_start
    while current <= series1_end:
        if current.weekday() == 0:  # Monday
            date_str = current.isoformat()
            events.append(make_event(
                title="Weekly 1:1 — Maria & Patricia",
                event_type="internal_meeting",
                start_dt=f"{date_str} 09:00:00",
                end_dt=f"{date_str} 09:30:00",
                attendees="Maria Gonzalez,Patricia Nguyen",
            ))
        current += timedelta(days=7)

    # ------------------------------------------------------------------ #
    # Series 2 — All-Hands Team Meeting (4 events, quarterly)
    # ------------------------------------------------------------------ #
    all_hands_dates = [
        date(2025, 5, 15),
        date(2025, 8, 14),
        date(2025, 11, 13),
        date(2026, 2, 12),
    ]
    all_employee_names = _all_employee_names()

    for d in all_hands_dates:
        date_str = d.isoformat()
        events.append(make_event(
            title="All-Hands Team Meeting",
            event_type="team_meeting",
            start_dt=f"{date_str} 10:00:00",
            end_dt=f"{date_str} 11:30:00",
            attendees=all_employee_names,
        ))

    # ------------------------------------------------------------------ #
    # Series 3 — Commercial site visits (30 events)
    # ------------------------------------------------------------------ #
    # Take proposals that have a sent_date (won, lost, sent)
    proposals_with_sent_date = cur.execute(
        """SELECT id, title, client_id, sent_date
           FROM commercial_proposals
           WHERE sent_date IS NOT NULL
             AND status IN ('won', 'lost', 'sent')
           ORDER BY id
           LIMIT 30"""
    ).fetchall()

    for prop in proposals_with_sent_date:
        sent_d = date.fromisoformat(prop["sent_date"])
        site_visit_date = sent_d - timedelta(days=7)
        date_str = site_visit_date.isoformat()
        title = prop["title"] or "Commercial Site Visit"
        events.append(make_event(
            title=f"Site Visit: {title}",
            event_type="site_visit",
            start_dt=f"{date_str} 10:00:00",
            end_dt=f"{date_str} 11:30:00",
            attendees="Maria Gonzalez,Kevin Okafor",
            related_client_id=prop["client_id"],
        ))

    # ------------------------------------------------------------------ #
    # Series 4 — Supplier/vendor meetings (15 events: 3 per supplier)
    # ------------------------------------------------------------------ #
    suppliers = [
        "Austin Janitorial Supply Co.",
        "Hill Country Equipment Rental",
        "Lone Star Chemical Distributors",
        "Central Texas Fleet Services",
        "Eco-Clean Products Austin",
    ]

    # Spread 3 meetings per supplier across Apr 2025 – Jan 2026 (10 months)
    supplier_meeting_dates_per_supplier = [
        # Supplier 1
        [date(2025, 4, 10), date(2025, 8, 12), date(2025, 12, 9)],
        # Supplier 2
        [date(2025, 5, 8), date(2025, 9, 11), date(2026, 1, 8)],
        # Supplier 3
        [date(2025, 6, 12), date(2025, 10, 9), date(2026, 1, 22)],
        # Supplier 4
        [date(2025, 7, 10), date(2025, 11, 13), date(2026, 1, 29)],
        # Supplier 5
        [date(2025, 4, 24), date(2025, 8, 28), date(2025, 12, 18)],
    ]

    for supplier, meeting_dates in zip(suppliers, supplier_meeting_dates_per_supplier):
        for md in meeting_dates:
            date_str = md.isoformat()
            events.append(make_event(
                title=f"Supplier Meeting — {supplier}",
                event_type="supplier_meeting",
                start_dt=f"{date_str} 14:00:00",
                end_dt=f"{date_str} 15:00:00",
                attendees="Patricia Nguyen",
            ))

    # ------------------------------------------------------------------ #
    # Series 5 — New hire onboarding sessions (6 events)
    # ------------------------------------------------------------------ #
    new_hires = [
        ("Kevin", "Okafor", date(2023, 4, 17)),
        ("Vanessa", "Reyes", date(2025, 6, 16)),
        ("Isaiah", "Patterson", date(2025, 7, 7)),
        # 2 replacement hires Oct 2025
        ("Jordan", "Mitchell", date(2025, 10, 6)),
        ("Taylor", "Brooks", date(2025, 10, 13)),
        # One more 2025 hire
        ("Marcus", "Thompson", date(2024, 1, 8)),
    ]

    for first_name, last_name, hire_date in new_hires:
        date_str = hire_date.isoformat()
        events.append(make_event(
            title=f"New Employee Orientation — {first_name} {last_name}",
            event_type="onboarding",
            start_dt=f"{date_str} 08:00:00",
            end_dt=f"{date_str} 11:00:00",
            attendees=f"Maria Gonzalez,Patricia Nguyen,{first_name} {last_name}",
        ))

    # ------------------------------------------------------------------ #
    # Series 6 — Holiday closing notices (2 events)
    # ------------------------------------------------------------------ #
    events.append(make_event(
        title="Office Closed — Thanksgiving Week 2025",
        event_type="holiday_closure",
        start_dt="2025-11-27 00:00:00",
        end_dt="2025-11-27 23:59:59",
        attendees="All Staff",
    ))
    events.append(make_event(
        title="Office Closed — Christmas Week 2025",
        event_type="holiday_closure",
        start_dt="2025-12-25 00:00:00",
        end_dt="2025-12-25 23:59:59",
        attendees="All Staff",
    ))

    # ------------------------------------------------------------------ #
    # Series 7 — Monthly crew performance check-ins (48 events)
    # ------------------------------------------------------------------ #
    crew_letters = ["A", "B", "C", "D"]
    crew_lead_names = {letter: _crew_lead_name(letter) for letter in crew_letters}

    months_list = []
    for m in range(4, 13):
        months_list.append((2025, m))
    for m in range(1, 4):
        months_list.append((2026, m))

    for year, month in months_list:
        first_tue = _first_tuesday_of_month(year, month)
        for crew_letter in crew_letters:
            lead_name = crew_lead_names[crew_letter]
            date_str = first_tue.isoformat()
            attendees = f"Maria Gonzalez,{lead_name}" if lead_name else "Maria Gonzalez"
            events.append(make_event(
                title=f"Crew {crew_letter} Monthly Check-in",
                event_type="crew_checkin",
                start_dt=f"{date_str} 11:00:00",
                end_dt=f"{date_str} 11:30:00",
                attendees=attendees,
            ))

    # ------------------------------------------------------------------ #
    # Series 8 — Monthly financial review (12 events)
    # ------------------------------------------------------------------ #
    for year, month in months_list:
        last_fri = _last_friday_of_month(year, month)
        date_str = last_fri.isoformat()
        events.append(make_event(
            title="Monthly Financial Review",
            event_type="financial_review",
            start_dt=f"{date_str} 14:00:00",
            end_dt=f"{date_str} 15:00:00",
            attendees="Maria Gonzalez,Sandra Flores,Patricia Nguyen",
        ))

    return events


def _insert_calendar_events(conn: sqlite3.Connection, events: list[dict]) -> None:
    cur = conn.cursor()
    cur.executemany(
        """INSERT OR IGNORE INTO calendar_events
           (id, title, event_type, start_datetime, end_datetime, attendees, related_client_id, notes)
           VALUES (:id, :title, :event_type, :start_datetime, :end_datetime,
                   :attendees, :related_client_id, :notes)""",
        events,
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Print summary
# ---------------------------------------------------------------------------

def _print_summary(
    emp_seeded: int,
    crew_seeded: int,
    n_barton_creek: int,
    complaint_job_ids: list[str],
    n_westlake: int,
    n_referral_proposals: int,
    all_tasks: list[dict],
    commercial_tasks: list[dict],
    residential_tasks: list[dict],
    back_office_tasks: list[dict],
    sales_tasks: list[dict],
    reviews: list[dict],
    n_total_completed: int,
    calendar_events: list[dict],
) -> None:
    # Compute per-crew review stats
    crew_ratings: dict[str, list[int]] = {}
    for rev in reviews:
        # We need the crew from the review — look up in the reviews list we built
        pass

    print()
    print("=" * 66)
    print("  SPARKLE & SHINE — TASKS / REVIEWS / EVENTS GENERATION RESULTS")
    print("=" * 66)
    print(f"  Employees seeded       : {emp_seeded}")
    print(f"  Crews seeded           : {crew_seeded}")
    print(f"  Planted patterns       :")
    print(f"    Barton Creek upsell signals : {n_barton_creek} jobs updated")
    print(f"    Aug-Sep complaint jobs      : {len(complaint_job_ids)} jobs flagged")
    print(f"    Westlake cancellations      : {n_westlake} agreements cancelled")
    print(f"    Referral proposal premium   : {n_referral_proposals} proposals updated (+30%)")
    print(f"  Asana tasks            : {len(all_tasks)} total")
    print(f"    Commercial onboarding: {len(commercial_tasks)}")
    print(f"    Residential onboarding: {len(residential_tasks)}")
    print(f"    Back-office          : {len(back_office_tasks)}")
    print(f"    Sales pipeline       : {len(sales_tasks)}")
    n_reviews = len(reviews)
    pct = round(n_reviews / n_total_completed * 100) if n_total_completed else 0
    print(f"  Reviews generated      : {n_reviews} ({pct}% of {n_total_completed} completed jobs)")
    print(f"  Calendar events        : {len(calendar_events)} total")
    print("=" * 66)
    print()


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main(db_path: Path = DB_PATH) -> None:
    conn = _get_conn(db_path)

    # --- Step 0: Bootstrap employees and crews ---
    print("Step 0: Bootstrapping employees and crews...")
    emp_seeded, crew_seeded = bootstrap_employees_and_crews(conn)
    print(f"  {emp_seeded} employees, {crew_seeded} crews ready.")

    # --- Step 1: Barton Creek upsell signals ---
    print("Step 1: Planting Barton Creek upsell signals...")
    n_barton_creek = plant_barton_creek_upsell_signals(conn)
    print(f"  {n_barton_creek} jobs updated.")

    # --- Step 2: Aug-Sep complaint cluster ---
    print("Step 2: Planting Aug-Sep 2025 complaint cluster...")
    complaint_job_ids = plant_complaint_cluster(conn)
    print(f"  Flagged jobs: {complaint_job_ids}")

    # --- Step 3: Westlake cancellation cluster ---
    print("Step 3: Planting Westlake cancellation cluster...")
    n_westlake = plant_westlake_cancellations(conn)
    print(f"  {n_westlake} agreements cancelled.")

    # --- Step 4: Referral proposal premium ---
    print("Step 4: Planting referral proposal value premium...")
    n_referral_proposals = plant_referral_proposal_premium(conn)
    print(f"  {n_referral_proposals} proposals updated.")

    # --- Step 5: Generate tasks ---
    print("Step 5: Generating Asana tasks...")

    task_counter = 1

    if _table_count(conn.cursor(), "tasks") > 0:
        print("  Tasks table already has data — skipping task generation.")
        commercial_tasks, residential_tasks, back_office_tasks, sales_tasks = [], [], [], []
        all_tasks = []
    else:
        print("  Generating commercial onboarding tasks...")
        commercial_tasks, task_counter = generate_commercial_onboarding_tasks(conn, task_counter)
        print(f"    {len(commercial_tasks)} commercial onboarding tasks.")

        print("  Generating residential onboarding tasks...")
        residential_tasks, task_counter = generate_residential_onboarding_tasks(conn, task_counter)
        print(f"    {len(residential_tasks)} residential onboarding tasks.")

        print("  Generating back-office tasks...")
        back_office_tasks, task_counter = generate_back_office_tasks(conn, task_counter)
        print(f"    {len(back_office_tasks)} back-office tasks.")

        print("  Generating sales pipeline tasks...")
        sales_tasks, task_counter = generate_sales_pipeline_tasks(conn, task_counter)
        print(f"    {len(sales_tasks)} sales pipeline tasks.")

        all_tasks = commercial_tasks + residential_tasks + back_office_tasks + sales_tasks
        print(f"  Inserting {len(all_tasks)} tasks...")
        _insert_tasks(conn, all_tasks)

    # --- Step 6: Generate reviews ---
    print("Step 6: Generating reviews...")
    if _table_count(conn.cursor(), "reviews") > 0:
        print("  Reviews table already has data — skipping review generation.")
        reviews = []
        n_total_completed = 0
    else:
        reviews, n_total_completed = generate_reviews(conn, complaint_job_ids)
        print(f"  Generated {len(reviews)} reviews.")
        print("  Inserting reviews...")
        _insert_reviews(conn, reviews)

    # --- Step 7: Generate calendar events ---
    print("Step 7: Generating calendar events...")
    if _table_count(conn.cursor(), "calendar_events") > 0:
        print("  Calendar events table already has data — skipping.")
        calendar_events = []
    else:
        calendar_events = generate_calendar_events(conn)
        print(f"  Generated {len(calendar_events)} calendar events.")
        print("  Inserting calendar events...")
        _insert_calendar_events(conn, calendar_events)

    conn.close()

    _print_summary(
        emp_seeded=emp_seeded,
        crew_seeded=crew_seeded,
        n_barton_creek=n_barton_creek,
        complaint_job_ids=complaint_job_ids,
        n_westlake=n_westlake,
        n_referral_proposals=n_referral_proposals,
        all_tasks=all_tasks,
        commercial_tasks=commercial_tasks,
        residential_tasks=residential_tasks,
        back_office_tasks=back_office_tasks,
        sales_tasks=sales_tasks,
        reviews=reviews,
        n_total_completed=n_total_completed,
        calendar_events=calendar_events,
    )


if __name__ == "__main__":
    import os
    db = os.environ.get("SS_DB_PATH", str(DB_PATH))
    main(Path(db))
