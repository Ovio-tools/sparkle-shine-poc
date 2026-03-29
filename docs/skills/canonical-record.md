# Skill: Canonical Record Pattern

**When to read:** Before writing any code that creates, updates, or looks up records in the SQLite database or across tools.

Every entity in the Sparkle & Shine POC has a canonical record in SQLite and cross-tool mapping entries that link it to its IDs in each SaaS tool. This skill doc covers the exact workflow for creating, linking, and looking up records.

---

## The Golden Rule

**SQLite is the source of truth.** The SaaS tools are mirrors. If there's a conflict between what SQLite says and what HubSpot says, SQLite wins. Every new record starts in SQLite, gets a canonical ID, then gets pushed to tools. Never create a tool record without also creating the SQLite record and mapping.

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

**Key functions:**

```python
from database.mappings import generate_id, link, lookup, reverse_lookup, find_unmapped

# Generate the next canonical ID for a type
canonical_id = generate_id("CLIENT")
# Returns: "SS-CLIENT-0311" (next available)

# Link a canonical ID to a tool-specific ID
link(canonical_id="SS-CLIENT-0311", tool_name="hubspot", tool_id="12345678")
link(canonical_id="SS-CLIENT-0311", tool_name="pipedrive", tool_id="456")
link(canonical_id="SS-CLIENT-0311", tool_name="quickbooks", tool_id="789")

# Look up a tool-specific ID from a canonical ID
hubspot_id = lookup("SS-CLIENT-0311", "hubspot")
# Returns: "12345678" or None if not mapped

# Reverse lookup: find canonical ID from a tool-specific ID
canonical_id = reverse_lookup("hubspot", "12345678")
# Returns: "SS-CLIENT-0311" or None

# Find canonical IDs that are missing a mapping for a specific tool
unmapped = find_unmapped("jobber", "CLIENT")
# Returns: ["SS-CLIENT-0311", "SS-CLIENT-0312"] -- clients with no Jobber ID
```

---

## cross_tool_mapping Table

```sql
CREATE TABLE cross_tool_mapping (
    canonical_id TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    tool_id TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (canonical_id, tool_name)
);
```

A single client might have 5 rows in this table:

| canonical_id | tool_name | tool_id |
|-------------|-----------|---------|
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
from database.mappings import generate_id, link
canonical_id = generate_id("LEAD")  # SS-LEAD-0313

# 2. Insert into SQLite leads table
import sqlite3
db = sqlite3.connect("sparkle_shine.db")
db.execute("""
    INSERT INTO leads (
        canonical_id, first_name, last_name, email, phone,
        address, city, state, zip, neighborhood,
        client_type, lead_source, service_interest,
        lifecycle_stage, status, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""", (
    canonical_id, "Sarah", "Chen", "sarah.chen@example.com",
    "(512) 555-0147", "2401 Westlake Dr", "Austin", "TX", "78746",
    "Westlake/Tarrytown", "residential", "referral",
    "biweekly_recurring", "sales_qualified_lead", "active",
    datetime.utcnow().isoformat()
))
db.commit()

# 3. Create in HubSpot using the unified auth interface
from auth import get_client
session = get_client("hubspot")
# (see tool-api-patterns.md for the POST body format)
hubspot_id = create_in_hubspot(session, profile, lifecycle_stage)

# 4. Register the HubSpot mapping
link(canonical_id, "hubspot", hubspot_id)

# *** CRITICAL: Do NOT register a Pipedrive mapping here. ***
# The automation runner detects new SQLs by finding HubSpot contacts
# with NO Pipedrive entry in cross_tool_mapping.
# If you call link(canonical_id, "pipedrive", ...) here, the runner
# will never pick up this SQL and no deal will be created.

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
# 2. Call link(canonical_id, "pipedrive", pipedrive_deal_id)
# 3. Optionally promote the record from "leads" to "clients" table
#    OR update the leads table with the Pipedrive deal reference
```

### Phase 3: Deal Won -- Simulation Fills In Details

When the deal generator marks a deal as "Won":

```python
# 1. Update Pipedrive deal with contract details
# 2. Update SQLite:
db.execute("""
    UPDATE commercial_proposals
    SET status = 'won', won_date = ?, contract_value = ?,
        service_frequency = ?, start_date = ?
    WHERE canonical_id = ?
""", (won_date, value, frequency, start_date, canonical_id))
```

### Phase 4: Automation Creates Asana Tasks

The automation runner detects the won deal and creates onboarding tasks:

```python
# Automation handles this. It should:
# For each onboarding task:
#   task_id = generate_id("TASK")
#   create task in Asana
#   link(task_id, "asana", asana_gid)
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
link(canonical_id, "jobber", jobber_id)

# 3. Create the first job or recurring schedule
job_canonical = generate_id("JOB")
jobber_job_id = create_jobber_job(session, jobber_id, job_data)
link(job_canonical, "jobber", jobber_job_id)

# 4. Insert job into SQLite jobs table
db.execute("""
    INSERT INTO jobs (
        canonical_id, client_id, crew_id, service_type,
        scheduled_date, expected_duration_min, amount,
        status, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
""", (job_canonical, canonical_id, crew_id, ...))
```

### Phase 5b: Operations Generator Marks Job Complete

After the scheduled duration elapses (with +/- 15% variance):

```python
# Mark the job complete in Jobber's API
# This triggers the automation runner to create a QBO invoice
session = get_client("jobber")
complete_jobber_job(session, jobber_job_id, actual_duration)

# Update SQLite
db.execute("""
    UPDATE jobs SET status = 'completed', actual_duration_min = ?,
    completed_at = ?, rating = ? WHERE canonical_id = ?
""", (actual_duration, completion_time, rating, job_canonical))

# *** Do NOT create a QBO invoice here. ***
# The automation runner detects completed Jobber jobs via poll_state
# and creates the invoice automatically within 5 minutes.
```

### Phase 6: Automation Creates Invoice (Automatic)

The automation runner handles this. The simulation does NOT write invoice code.
The reconciliation engine (Step 7) checks for completed jobs older than 24
hours with no matching invoice, and flags missing invoices in #automation-failure.

### Phase 7: Payment Recorded

```python
payment_canonical = generate_id("PAYMENT")
session = get_client("quickbooks")
qbo_payment_id = create_qbo_payment(session, invoice)
link(payment_canonical, "quickbooks", qbo_payment_id)

db.execute("""
    INSERT INTO payments (
        canonical_id, invoice_id, client_id, amount,
        payment_date, created_at
    ) VALUES (?, ?, ?, ?, ?, ?)
""", (payment_canonical, invoice_canonical, canonical_id, amount, ...))

# Update the invoice status
db.execute("""
    UPDATE invoices SET status = 'paid', amount_paid = ?, paid_date = ?
    WHERE canonical_id = ?
""", (amount, payment_date, invoice_canonical))
```

---

## Churn Cascade (Touching Multiple Tools)

When a client churns, use `cross_tool_mapping` to find all their tool IDs:

```python
from database.mappings import lookup

canonical_id = "SS-CLIENT-0311"

# Find all tool IDs for this client
hubspot_id = lookup(canonical_id, "hubspot")
pipedrive_id = lookup(canonical_id, "pipedrive")
jobber_id = lookup(canonical_id, "jobber")
mailchimp_email = "sarah.chen@example.com"  # used as hash for Mailchimp
# Note: Mailchimp doesn't use a numeric ID. Use the email hash.

# Update each tool (see tool-api-patterns.md for API calls)
# 1. Jobber: cancel recurring schedule
# 2. HubSpot: set lifecycle to "other", add churn_date property
# 3. Pipedrive: add activity note, mark person inactive
# 4. Mailchimp: unsubscribe, add "churned" tag
# 5. Asana: create retention follow-up task

# Update SQLite last
db.execute("""
    UPDATE clients
    SET status = 'churned', churn_date = ?, churn_reason = ?,
        lifetime_value = ?
    WHERE canonical_id = ?
""", (churn_date, reason, ltv, canonical_id))
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
| `cross_tool_mapping` | `(canonical_id, tool_name)` | tool_id |

For the full schema with all columns, read `database/schema.py`.

---

## Anti-Patterns to Avoid

**Never create a tool record without a SQLite record and mapping:**
```python
# BAD
hubspot_id = create_in_hubspot(profile)
# Done! (no SQLite record, no mapping -- orphaned record)

# GOOD
canonical_id = generate_id("LEAD")
insert_into_sqlite(canonical_id, profile)
hubspot_id = create_in_hubspot(profile)
link(canonical_id, "hubspot", hubspot_id)
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
from database.mappings import lookup
existing = lookup(canonical_id, "hubspot")
if existing:
    logger.info(f"Already mapped to HubSpot {existing}, skipping")
    return existing
hubspot_id = create_in_hubspot(profile)
link(canonical_id, "hubspot", hubspot_id)
```

**Always embed the canonical ID in the tool record:**
```python
# In job notes, invoice memos, Asana task descriptions, etc.:
"SS-ID: SS-CLIENT-0312"
# This makes records traceable without needing the mapping table
```
