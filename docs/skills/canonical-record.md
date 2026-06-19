# Skill: Canonical Record Pattern

**When to read:** Before writing any code that creates, updates, or looks up records in the PostgreSQL database or across tools.

Every entity in the Sparkle & Shine POC has a canonical record in PostgreSQL and cross-tool mapping entries that link it to its IDs in each SaaS tool. This skill doc covers the exact workflow for creating, linking, and looking up records.

---

## The Golden Rule

**PostgreSQL is the source of truth.** The SaaS tools are mirrors. If there's a conflict between what the database says and what HubSpot says, the database wins. Every new record starts in PostgreSQL, gets a canonical ID, then gets pushed to tools. Never create a tool record without also creating the database record and mapping.

All database access goes through `from database.connection import get_connection` (requires `DATABASE_URL`). Parameter placeholders are `%s`, rows are `RealDictRow` dicts (`row["column_name"]`, never `row[0]`). See "Database Patterns (PostgreSQL)" in CLAUDE.md.

---

## Canonical ID Format

```
SS-{TYPE}-{NNNN}
```

Examples:
- `SS-CLIENT-0047` -- a client (residential or commercial)
- `SS-LEAD-0312` -- a lead or prospect (not yet converted)
- `SS-JOB-8201` -- a single job visit
- `SS-INVOICE-8201` -- an invoice
- `SS-PAYMENT-7500` -- a payment
- `SS-PROPOSAL-0025` -- a commercial proposal/deal
- `SS-TASK-0150` -- an Asana task
- `SS-EMPLOYEE-0014` -- an employee

The sequential number auto-increments per type. Use `database.mappings.generate_id()` to get the next available ID. Never hardcode IDs.

---

## The Mappings Module

**Location:** `database/mappings.py`

All functions accept a legacy `db_path` keyword argument that is passed through to `get_connection()`; new code can omit it (the PostgreSQL connection comes from `DATABASE_URL`).

```python
from database.mappings import (
    generate_id, register_mapping, get_tool_id, get_tool_url,
    get_canonical_id, find_unmapped, bulk_register,
)

# Generate the next canonical ID for a type.
# Checks BOTH the entity table AND cross_tool_mapping to avoid colliding
# with IDs allocated by automations.
canonical_id = generate_id("CLIENT")
# Returns: "SS-CLIENT-0311" (next available)

# Link a canonical ID to a tool-specific ID (insert-or-update).
# Raises ValueError if the tool ID is already mapped to a DIFFERENT
# canonical_id -- guards against cross-contaminated mappings.
register_mapping("SS-CLIENT-0311", "hubspot", "12345678")
register_mapping("SS-CLIENT-0311", "pipedrive", "456", tool_specific_url="https://yourco.pipedrive.com/person/456")

# Look up a tool-specific ID from a canonical ID
hubspot_id = get_tool_id("SS-CLIENT-0311", "hubspot")
# Returns: "12345678" or None if not mapped

# Look up the stored deep-link URL for a record
url = get_tool_url("SS-CLIENT-0311", "pipedrive")

# Reverse lookup: find canonical ID from a tool-specific ID
canonical_id = get_canonical_id("hubspot", "12345678")
# Returns: "SS-CLIENT-0311" or None

# Find canonical IDs that exist in the entity table but lack a mapping
# for a tool. NOTE the argument order: entity_type FIRST, then tool_name.
unmapped = find_unmapped("CLIENT", "jobber")
# Returns: ["SS-CLIENT-0311", "SS-CLIENT-0312"] -- clients with no Jobber ID

# Insert many mappings in one transaction.
# Each item: (canonical_id, tool_name, tool_specific_id)
bulk_register([("SS-JOB-8201", "jobber", "gid://jobber/Job/991")])
```

For code that already holds an open connection, `register_mapping_on_conn()` and `get_canonical_id_on_conn()` variants exist.

---

## cross_tool_mapping Table

Actual DDL (see `database/schema.py` for the authoritative version):

```sql
CREATE TABLE IF NOT EXISTS cross_tool_mapping (
    id                  SERIAL PRIMARY KEY,
    canonical_id        TEXT NOT NULL,             -- SS-TYPE-NNNN
    entity_type         TEXT NOT NULL,
    tool_name           TEXT NOT NULL,
    tool_specific_id    TEXT NOT NULL,
    tool_specific_url   TEXT,
    synced_at           TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(canonical_id, tool_name),
    UNIQUE(entity_type, tool_name, tool_specific_id)
);
```

A single client might have 5 rows in this table:

| canonical_id | tool_name | tool_specific_id |
|-------------|-----------|------------------|
| SS-CLIENT-0311 | hubspot | 12345678 |
| SS-CLIENT-0311 | pipedrive | 456 |
| SS-CLIENT-0311 | jobber | gid://jobber/Client/789 |
| SS-CLIENT-0311 | quickbooks | 101 |
| SS-CLIENT-0311 | mailchimp | abc123def456 |

---

## Complete Workflow: Creating a New Client

Here's the full sequence for creating a client that ends up in HubSpot, Pipedrive, Jobber, QuickBooks, and Mailchimp. Not every tool gets a record immediately. Some are created by automations.

### Phase 1: Contact Generator Creates HubSpot Contact

```python
# 1. Generate canonical ID
from database.mappings import generate_id, register_mapping
canonical_id = generate_id("LEAD")  # SS-LEAD-0313

# 2. Insert into the leads table
from datetime import datetime
from database.connection import get_connection

db = get_connection()
try:
    db.execute("""
        INSERT INTO leads (
            canonical_id, first_name, last_name, email, phone,
            address, city, state, zip, neighborhood,
            client_type, lead_source, service_interest,
            lifecycle_stage, status, created_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        canonical_id, "Sarah", "Chen", "sarah.chen@example.com",
        "(512) 555-0147", "2401 Westlake Dr", "Austin", "TX", "78746",
        "Westlake/Tarrytown", "residential", "referral",
        "biweekly_recurring", "sales_qualified_lead", "active",
        datetime.utcnow().isoformat(),
    ))
    db.commit()
finally:
    db.close()

# 3. Create in HubSpot using the unified auth interface
from auth import get_client
session = get_client("hubspot")
# (see tool-api-patterns.md for the POST body format)
hubspot_id = create_in_hubspot(session, profile, lifecycle_stage)

# 4. Register the HubSpot mapping
register_mapping(canonical_id, "hubspot", hubspot_id)

# *** CRITICAL: Do NOT register a Pipedrive mapping here. ***
# The automation runner detects new SQLs by finding HubSpot contacts
# with NO Pipedrive entry in cross_tool_mapping.
# If you call register_mapping(canonical_id, "pipedrive", ...) here,
# the runner will never pick up this SQL and no deal will be created.

# 5. Embed canonical ID in HubSpot record (for traceability)
# This is done by including a note or custom property:
# "SS-ID: SS-LEAD-0313"
```

### Phase 2: Automation Runner Creates Pipedrive Deal

The automation runner polls HubSpot for new SQLs. When it finds SS-LEAD-0313:

```python
# The automation runner handles this -- you don't write this code.
# But the runner should:
# 1. Create Pipedrive person + deal
# 2. Call register_mapping(canonical_id, "pipedrive", pipedrive_deal_id)
# 3. Optionally promote the record from "leads" to "clients" table
#    OR update the leads table with the Pipedrive deal reference
```

### Phase 3: Deal Won -- Simulation Fills In Details

When the deal generator marks a deal as "Won":

```python
# 1. Update Pipedrive deal with contract details
# 2. Update PostgreSQL:
db.execute("""
    UPDATE commercial_proposals
    SET status = 'won', won_date = %s, contract_value = %s,
        service_frequency = %s, start_date = %s
    WHERE canonical_id = %s
""", (won_date, value, frequency, start_date, canonical_id))
db.commit()
```

### Phase 4: Automation Creates Asana Tasks

The automation runner detects the won deal and creates onboarding tasks:

```python
# Automation handles this. It should:
# For each onboarding task:
#   task_id = generate_id("TASK")
#   create task in Asana
#   register_mapping(task_id, "asana", asana_gid)
```

### Phase 5: Operations Generator Creates Jobber Client + Jobs

After onboarding tasks are mostly done:

```python
# 1. Look up canonical_id for the client
# 2. Create Jobber client via the unified auth interface
from auth import get_client
session = get_client("jobber")
# (see tool-api-patterns.md for the GraphQL mutation)
jobber_id = create_jobber_client(session, client_data)
register_mapping(canonical_id, "jobber", jobber_id)

# 3. Create the first job or recurring schedule.
# NOTE: Jobber has no crew objects. Crews exist only in config/business.py.
# Jobber jobs are assigned per job via assignedUsers from the 7-user
# field-staff pool -- avoid time overlaps for the same user.
job_canonical = generate_id("JOB")
jobber_job_id = create_jobber_job(session, jobber_id, job_data)
register_mapping(job_canonical, "jobber", jobber_job_id)

# 4. Insert job into the jobs table
db.execute("""
    INSERT INTO jobs (
        canonical_id, client_id, crew_id, service_type,
        scheduled_date, expected_duration_min, amount,
        status, created_at
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
""", (job_canonical, canonical_id, crew_id, ...))
db.commit()
```

### Phase 5b: Operations Generator Marks Job Complete

After the scheduled duration elapses (with +/- 15% variance):

```python
# Mark the job complete in Jobber's API
# This triggers the automation runner to create a QBO invoice
session = get_client("jobber")
complete_jobber_job(session, jobber_job_id, actual_duration)

# Update PostgreSQL
db.execute("""
    UPDATE jobs SET status = 'completed', actual_duration_min = %s,
    completed_at = %s, rating = %s WHERE canonical_id = %s
""", (actual_duration, completion_time, rating, job_canonical))
db.commit()

# *** Do NOT create a QBO invoice here. ***
# The automation runner detects completed Jobber jobs via poll_state
# and creates the invoice automatically within 5 minutes.
```

### Phase 6: Automation Creates Invoice (Automatic)

The automation runner handles this. The simulation does NOT write invoice code.
The reconciliation engine checks for completed jobs older than 24 hours with
no matching invoice, and flags missing invoices in #automation-failure.

### Phase 7: Payment Recorded

```python
payment_canonical = generate_id("PAYMENT")
session = get_client("quickbooks")
qbo_payment_id = create_qbo_payment(session, invoice)
register_mapping(payment_canonical, "quickbooks", qbo_payment_id)

db.execute("""
    INSERT INTO payments (
        canonical_id, invoice_id, client_id, amount,
        payment_date, created_at
    ) VALUES (%s, %s, %s, %s, %s, %s)
""", (payment_canonical, invoice_canonical, canonical_id, amount, ...))

# Update the invoice status
db.execute("""
    UPDATE invoices SET status = 'paid', amount_paid = %s, paid_date = %s
    WHERE canonical_id = %s
""", (amount, payment_date, invoice_canonical))
db.commit()
```

---

## Churn Cascade (Touching Multiple Tools)

When a client churns, use `cross_tool_mapping` to find all their tool IDs:

```python
from database.mappings import get_tool_id

canonical_id = "SS-CLIENT-0311"

# Find all tool IDs for this client
hubspot_id = get_tool_id(canonical_id, "hubspot")
pipedrive_id = get_tool_id(canonical_id, "pipedrive")
jobber_id = get_tool_id(canonical_id, "jobber")
mailchimp_email = "sarah.chen@example.com"  # used as hash for Mailchimp
# Note: Mailchimp doesn't use a numeric ID. Use the email hash.

# Update each tool (see tool-api-patterns.md for API calls)
# 1. Jobber: cancel recurring schedule
# 2. HubSpot: set lifecycle to "other", add churn_date property
# 3. Pipedrive: add activity note, mark person inactive
# 4. Mailchimp: unsubscribe, add "churned" tag
# 5. Asana: create retention follow-up task

# Update PostgreSQL last
db.execute("""
    UPDATE clients
    SET status = 'churned', churn_date = %s, churn_reason = %s,
        lifetime_value = %s
    WHERE canonical_id = %s
""", (churn_date, reason, ltv, canonical_id))
db.commit()
```

---

## Key Tables Quick Reference

| Table | Primary Key | Key Columns |
|-------|------------|-------------|
| `clients` | `canonical_id` | first_name, last_name, email, client_type, status, neighborhood, lead_source |
| `leads` | `canonical_id` | first_name, last_name, email, lifecycle_stage, lead_source |
| `jobs` | `canonical_id` | client_id, crew_id, service_type, scheduled_date, status, amount |
| `invoices` | `canonical_id` | client_id, job_id, amount, due_date, status, amount_paid |
| `payments` | `canonical_id` | invoice_id, client_id, amount, payment_date |
| `commercial_proposals` | `canonical_id` | client_id, status (open/won/lost), value, stage |
| `tasks` | `canonical_id` | title, assignee, project, completed, due_date |
| `recurring_agreements` | `canonical_id` | client_id, service_type, frequency, amount_per_visit |
| `cross_tool_mapping` | `(canonical_id, tool_name)` unique | entity_type, tool_specific_id, tool_specific_url |

For the full schema with all columns, read `database/schema.py`.

---

## Anti-Patterns to Avoid

**Never create a tool record without a database record and mapping:**
```python
# BAD
hubspot_id = create_in_hubspot(profile)
# Done! (no database record, no mapping -- orphaned record)

# GOOD
canonical_id = generate_id("LEAD")
insert_into_db(canonical_id, profile)
hubspot_id = create_in_hubspot(profile)
register_mapping(canonical_id, "hubspot", hubspot_id)
```

**Never hardcode tool IDs:**
```python
# BAD
pipedrive_stage_id = 5  # what if stages get reordered?

# GOOD
import json
with open("config/tool_ids.json") as f:
    tool_ids = json.load(f)
pipedrive_stage_id = tool_ids["pipedrive"]["stages"]["negotiation"]
```

**Never skip the duplicate check:**
```python
# BAD
create_in_hubspot(profile)  # might already exist from a prior run

# GOOD
from database.mappings import get_tool_id
existing = get_tool_id(canonical_id, "hubspot")
if existing:
    logger.info(f"Already mapped to HubSpot {existing}, skipping")
    return existing
hubspot_id = create_in_hubspot(profile)
register_mapping(canonical_id, "hubspot", hubspot_id)
```

**Always embed the canonical ID in the tool record:**
```python
# In job notes, invoice memos, Asana task descriptions, etc.:
"SS-ID: SS-CLIENT-0312"
# This makes records traceable without needing the mapping table
```
