import sqlite3

CREATE_TABLES = [
    # ------------------------------------------------------------------ #
    # 1. clients
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS clients (
        id                  TEXT PRIMARY KEY,          -- SS-CLIENT-NNNN
        client_type         TEXT NOT NULL CHECK(client_type IN ('residential','commercial')),
        first_name          TEXT,
        last_name           TEXT,
        company_name        TEXT,
        email               TEXT UNIQUE NOT NULL,
        phone               TEXT,
        address             TEXT,
        neighborhood        TEXT,
        zone                TEXT,
        status              TEXT NOT NULL DEFAULT 'active'
                                CHECK(status IN ('active','churned','occasional','lead')),
        acquisition_source  TEXT,
        first_service_date  TEXT,
        last_service_date   TEXT,
        lifetime_value      REAL DEFAULT 0.0,
        notes               TEXT,
        created_at          TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """,

    # ------------------------------------------------------------------ #
    # 2. leads
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS leads (
        id                  TEXT PRIMARY KEY,          -- SS-LEAD-NNNN
        first_name          TEXT,
        last_name           TEXT,
        company_name        TEXT,
        email               TEXT,
        phone               TEXT,
        lead_type           TEXT NOT NULL CHECK(lead_type IN ('residential','commercial')),
        source              TEXT,
        status              TEXT NOT NULL DEFAULT 'new'
                                CHECK(status IN ('new','contacted','qualified','lost')),
        estimated_value     REAL DEFAULT 0.0,
        created_at          TEXT NOT NULL DEFAULT (datetime('now')),
        last_activity_at    TEXT,
        notes               TEXT
    )
    """,

    # ------------------------------------------------------------------ #
    # 3. employees
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS employees (
        id                  TEXT PRIMARY KEY,          -- SS-EMP-NNN
        first_name          TEXT NOT NULL,
        last_name           TEXT NOT NULL,
        role                TEXT NOT NULL,
        crew_id             TEXT REFERENCES crews(id),
        hire_date           TEXT NOT NULL,
        termination_date    TEXT,
        status              TEXT NOT NULL DEFAULT 'active'
                                CHECK(status IN ('active','terminated')),
        hourly_rate         REAL NOT NULL DEFAULT 0.0,
        email               TEXT,
        notes               TEXT
    )
    """,

    # ------------------------------------------------------------------ #
    # 4. crews
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS crews (
        id                  TEXT PRIMARY KEY,          -- crew-a through crew-d
        name                TEXT NOT NULL,
        zone                TEXT,
        lead_employee_id    TEXT REFERENCES employees(id)
    )
    """,

    # ------------------------------------------------------------------ #
    # 5. jobs
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS jobs (
        id                          TEXT PRIMARY KEY,  -- SS-JOB-NNNN
        client_id                   TEXT NOT NULL REFERENCES clients(id),
        crew_id                     TEXT REFERENCES crews(id),
        service_type_id             TEXT NOT NULL,
        scheduled_date              TEXT NOT NULL,
        scheduled_time              TEXT,
        duration_minutes_actual     INTEGER,
        status                      TEXT NOT NULL DEFAULT 'scheduled'
                                        CHECK(status IN ('scheduled','completed','cancelled','no-show')),
        address                     TEXT,
        notes                       TEXT,
        review_requested            INTEGER NOT NULL DEFAULT 0 CHECK(review_requested IN (0,1)),
        completed_at                TEXT
    )
    """,

    # ------------------------------------------------------------------ #
    # 6. recurring_agreements
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS recurring_agreements (
        id                  TEXT PRIMARY KEY,          -- SS-RECUR-NNNN
        client_id           TEXT NOT NULL REFERENCES clients(id),
        service_type_id     TEXT NOT NULL,
        crew_id             TEXT REFERENCES crews(id),
        frequency           TEXT NOT NULL CHECK(frequency IN ('weekly','biweekly','monthly')),
        price_per_visit     REAL NOT NULL,
        start_date          TEXT NOT NULL,
        end_date            TEXT,
        status              TEXT NOT NULL DEFAULT 'active'
                                CHECK(status IN ('active','paused','cancelled')),
        day_of_week         TEXT
    )
    """,

    # ------------------------------------------------------------------ #
    # 7. commercial_proposals
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS commercial_proposals (
        id                  TEXT PRIMARY KEY,          -- SS-PROP-NNNN
        lead_id             TEXT REFERENCES leads(id),
        client_id           TEXT REFERENCES clients(id),
        title               TEXT NOT NULL,
        square_footage      REAL,
        service_scope       TEXT,
        price_per_visit     REAL,
        frequency           TEXT,
        monthly_value       REAL,
        status              TEXT NOT NULL DEFAULT 'draft'
                                CHECK(status IN ('draft','sent','negotiating','won','lost','expired')),
        sent_date           TEXT,
        decision_date       TEXT,
        notes               TEXT
    )
    """,

    # ------------------------------------------------------------------ #
    # 8. invoices
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS invoices (
        id                  TEXT PRIMARY KEY,          -- SS-INV-NNNN
        client_id           TEXT NOT NULL REFERENCES clients(id),
        job_id              TEXT REFERENCES jobs(id),
        amount              REAL NOT NULL,
        status              TEXT NOT NULL DEFAULT 'draft'
                                CHECK(status IN ('draft','sent','paid','overdue')),
        issue_date          TEXT NOT NULL,
        due_date            TEXT,
        paid_date           TEXT,
        days_outstanding    INTEGER
    )
    """,

    # ------------------------------------------------------------------ #
    # 9. payments
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS payments (
        id                  TEXT PRIMARY KEY,          -- SS-PAY-NNNN
        invoice_id          TEXT NOT NULL REFERENCES invoices(id),
        client_id           TEXT NOT NULL REFERENCES clients(id),
        amount              REAL NOT NULL,
        payment_method      TEXT,
        payment_date        TEXT NOT NULL
    )
    """,

    # ------------------------------------------------------------------ #
    # 10. marketing_campaigns
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS marketing_campaigns (
        id                  TEXT PRIMARY KEY,          -- SS-CAMP-NNNN
        name                TEXT NOT NULL,
        platform            TEXT NOT NULL DEFAULT 'mailchimp',
        campaign_type       TEXT,
        subject_line        TEXT,
        send_date           TEXT,
        recipient_count     INTEGER DEFAULT 0,
        open_rate           REAL DEFAULT 0.0,
        click_rate          REAL DEFAULT 0.0,
        conversion_count    INTEGER DEFAULT 0
    )
    """,

    # ------------------------------------------------------------------ #
    # 11. marketing_interactions
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS marketing_interactions (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id           TEXT REFERENCES clients(id),
        lead_id             TEXT REFERENCES leads(id),
        campaign_id         TEXT NOT NULL REFERENCES marketing_campaigns(id),
        interaction_type    TEXT NOT NULL CHECK(interaction_type IN ('open','click','conversion')),
        interaction_date    TEXT NOT NULL
    )
    """,

    # ------------------------------------------------------------------ #
    # 12. reviews
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS reviews (
        id                  TEXT PRIMARY KEY,          -- SS-REV-NNNN
        client_id           TEXT NOT NULL REFERENCES clients(id),
        job_id              TEXT REFERENCES jobs(id),
        rating              INTEGER NOT NULL CHECK(rating BETWEEN 1 AND 5),
        review_text         TEXT,
        platform            TEXT,
        review_date         TEXT NOT NULL,
        response_text       TEXT,
        response_date       TEXT
    )
    """,

    # ------------------------------------------------------------------ #
    # 13. tasks
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS tasks (
        id                      TEXT PRIMARY KEY,      -- SS-TASK-NNNN
        title                   TEXT NOT NULL,
        description             TEXT,
        project_name            TEXT,
        assignee_employee_id    TEXT REFERENCES employees(id),
        client_id               TEXT REFERENCES clients(id),
        due_date                TEXT,
        completed_date          TEXT,
        status                  TEXT NOT NULL DEFAULT 'not_started'
                                    CHECK(status IN ('not_started','in_progress','completed','overdue')),
        priority                TEXT NOT NULL DEFAULT 'medium'
                                    CHECK(priority IN ('low','medium','high')),
        created_at              TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """,

    # ------------------------------------------------------------------ #
    # 14. calendar_events
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS calendar_events (
        id                  TEXT PRIMARY KEY,          -- SS-CAL-NNNN
        title               TEXT NOT NULL,
        event_type          TEXT,
        start_datetime      TEXT NOT NULL,
        end_datetime        TEXT,
        attendees           TEXT,
        related_client_id   TEXT REFERENCES clients(id),
        notes               TEXT
    )
    """,

    # ------------------------------------------------------------------ #
    # 15. documents
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS documents (
        id                  TEXT PRIMARY KEY,          -- SS-DOC-NNNN
        title               TEXT NOT NULL,
        doc_type            TEXT NOT NULL CHECK(doc_type IN ('sop','contract','template','spreadsheet')),
        platform            TEXT NOT NULL CHECK(platform IN ('google_docs','google_sheets')),
        google_file_id      TEXT,
        content_text        TEXT,
        keywords            TEXT,
        last_indexed_at     TEXT
    )
    """,

    # ------------------------------------------------------------------ #
    # 16. cross_tool_mapping
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS cross_tool_mapping (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        canonical_id        TEXT NOT NULL,             -- SS-TYPE-NNNN
        entity_type         TEXT NOT NULL,
        tool_name           TEXT NOT NULL,
        tool_specific_id    TEXT NOT NULL,
        tool_specific_url   TEXT,
        synced_at           TEXT NOT NULL DEFAULT (datetime('now')),
        UNIQUE(canonical_id, tool_name)
    )
    """,

    # ------------------------------------------------------------------ #
    # 17. daily_metrics_snapshot
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS daily_metrics_snapshot (
        snapshot_date           TEXT PRIMARY KEY UNIQUE,
        total_revenue_mtd       REAL DEFAULT 0.0,
        jobs_completed          INTEGER DEFAULT 0,
        jobs_scheduled          INTEGER DEFAULT 0,
        jobs_cancelled          INTEGER DEFAULT 0,
        active_clients          INTEGER DEFAULT 0,
        new_leads               INTEGER DEFAULT 0,
        open_invoices_value     REAL DEFAULT 0.0,
        overdue_invoices_value  REAL DEFAULT 0.0,
        pipeline_value          REAL DEFAULT 0.0,
        raw_json                TEXT
    )
    """,

    # ------------------------------------------------------------------ #
    # 18. document_index
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS document_index (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        doc_id      TEXT NOT NULL REFERENCES documents(id),
        chunk_text  TEXT NOT NULL,
        keywords    TEXT,
        source_title TEXT,
        indexed_at  TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """,

    # ------------------------------------------------------------------ #
    # Indexes
    # ------------------------------------------------------------------ #
    "CREATE INDEX IF NOT EXISTS idx_clients_email        ON clients(email)",
    "CREATE INDEX IF NOT EXISTS idx_clients_status       ON clients(status)",
    "CREATE INDEX IF NOT EXISTS idx_leads_status         ON leads(status)",
    "CREATE INDEX IF NOT EXISTS idx_leads_email          ON leads(email)",
    "CREATE INDEX IF NOT EXISTS idx_jobs_client_id       ON jobs(client_id)",
    "CREATE INDEX IF NOT EXISTS idx_jobs_scheduled_date  ON jobs(scheduled_date)",
    "CREATE INDEX IF NOT EXISTS idx_jobs_status          ON jobs(status)",
    "CREATE INDEX IF NOT EXISTS idx_invoices_client_id   ON invoices(client_id)",
    "CREATE INDEX IF NOT EXISTS idx_invoices_status      ON invoices(status)",
    "CREATE INDEX IF NOT EXISTS idx_payments_invoice_id  ON payments(invoice_id)",
    "CREATE INDEX IF NOT EXISTS idx_recurring_client_id  ON recurring_agreements(client_id)",
    "CREATE INDEX IF NOT EXISTS idx_recurring_status     ON recurring_agreements(status)",
    "CREATE INDEX IF NOT EXISTS idx_proposals_status     ON commercial_proposals(status)",
    "CREATE INDEX IF NOT EXISTS idx_tasks_status         ON tasks(status)",
    "CREATE INDEX IF NOT EXISTS idx_tasks_assignee       ON tasks(assignee_employee_id)",
    "CREATE INDEX IF NOT EXISTS idx_mktg_inter_client    ON marketing_interactions(client_id)",
    "CREATE INDEX IF NOT EXISTS idx_mktg_inter_campaign  ON marketing_interactions(campaign_id)",
    "CREATE INDEX IF NOT EXISTS idx_cross_tool_canonical ON cross_tool_mapping(canonical_id)",
    "CREATE INDEX IF NOT EXISTS idx_doc_index_doc_id     ON document_index(doc_id)",

    # ------------------------------------------------------------------ #
    # 19. poll_state — tracks last-processed state per tool per entity type
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS poll_state (
        tool_name                TEXT NOT NULL,
        entity_type              TEXT NOT NULL,
        last_processed_id        TEXT,
        last_processed_timestamp TEXT,
        last_poll_at             TEXT NOT NULL,
        PRIMARY KEY (tool_name, entity_type)
    )
    """,

    # ------------------------------------------------------------------ #
    # 20. automation_log — audit trail for every action in every automation
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS automation_log (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id          TEXT NOT NULL,
        automation_name TEXT NOT NULL,
        trigger_source  TEXT,
        trigger_detail  TEXT,
        action_name     TEXT NOT NULL,
        action_target   TEXT,
        status          TEXT NOT NULL CHECK(status IN ('success','failed','skipped')),
        error_message   TEXT,
        created_at      TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """,

    # ------------------------------------------------------------------ #
    # 21. pending_actions — delayed / scheduled actions
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS pending_actions (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        automation_name TEXT NOT NULL,
        action_name     TEXT NOT NULL,
        trigger_context TEXT NOT NULL,
        execute_after   TEXT NOT NULL,
        status          TEXT NOT NULL DEFAULT 'pending'
                            CHECK(status IN ('pending','executed','failed')),
        created_at      TEXT NOT NULL DEFAULT (datetime('now')),
        executed_at     TEXT
    )
    """,

    "CREATE INDEX IF NOT EXISTS idx_automation_log_run_id   ON automation_log(run_id)",
    "CREATE INDEX IF NOT EXISTS idx_automation_log_status   ON automation_log(status)",
    "CREATE INDEX IF NOT EXISTS idx_pending_actions_status  ON pending_actions(status)",
    "CREATE INDEX IF NOT EXISTS idx_pending_actions_execute ON pending_actions(execute_after)",
]

# Table name → human label (for __main__ summary only)
_TABLE_NAMES = [
    "clients", "leads", "employees", "crews", "jobs",
    "recurring_agreements", "commercial_proposals", "invoices", "payments",
    "marketing_campaigns", "marketing_interactions", "reviews", "tasks",
    "calendar_events", "documents", "cross_tool_mapping",
    "daily_metrics_snapshot", "document_index",
    "poll_state", "automation_log", "pending_actions",
]


def get_connection(db_path: str = "sparkle_shine.db") -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(db_path: str = "sparkle_shine.db") -> None:
    conn = get_connection(db_path)
    with conn:
        for statement in CREATE_TABLES:
            conn.execute(statement)
    conn.close()


if __name__ == "__main__":
    import os

    db_path = "sparkle_shine.db"
    init_db(db_path)

    conn = get_connection(db_path)
    print(f"\nDatabase initialised: {os.path.abspath(db_path)}\n")
    print(f"{'Table':<30} {'Columns':>7}")
    print("-" * 40)

    for table in _TABLE_NAMES:
        cursor = conn.execute(f"PRAGMA table_info({table})")
        cols = cursor.fetchall()
        print(f"  {table:<28} {len(cols):>7}")

    cursor = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'"
    )
    index_count = cursor.fetchone()[0]
    print("-" * 40)
    print(f"  {'TOTAL TABLES':<28} {len(_TABLE_NAMES):>7}")
    print(f"  {'TOTAL INDEXES':<28} {index_count:>7}")
    conn.close()
    print()
