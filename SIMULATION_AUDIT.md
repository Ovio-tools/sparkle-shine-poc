# SIMULATION_AUDIT.md

Pre-simulation audit of `sparkle-shine-poc` before adding the simulation engine.
Conducted: 2026-03-27.

---

## 1. Root Script Dependencies

### Scripts surveyed
| Script | Self-references (in own docstring) | External importers |
|--------|------------------------------------|--------------------|
| `create_hubspot_contact.py` | yes (usage example in docstring) | **0** |
| `create_sql_contact.py` | yes | **0** |
| `create_contact_derek_okafor.py` | yes | **0** |
| `create_contact_marcus_webb.py` | yes | **0** |
| `create_contact_nadia_chen.py` | yes | **0** |
| `create_contact_priya_nair.py` | yes | **0** |
| `create_contact_marco_delgado.py` | yes | **0** |

Grep command run (across `.py`, `.sh`, `.md`):
```
grep -r "create_hubspot_contact|create_sql_contact|create_contact_derek|
         create_contact_marcus|create_contact_nadia|create_contact_priya|
         Create_Hubspot_Contact" --include="*.py" --include="*.sh" --include="*.md" .
```

**Findings:**
- No `.sh` files reference these scripts.
- The only `.md` match is `Create_Hubspot_Contact/skill.md`, which refers to
  `create_hubspot_contact.py` as a human-operated command in usage instructions
  — not a programmatic import.
- No `.py` file outside each script itself imports any of them.

**Verdict: all seven root scripts are safe to move** (e.g., to `scripts/` or
`seeding/pushers/`). The `Create_Hubspot_Contact/` directory is a skills doc;
it can stay or move to `docs/skills/` without affecting runtime behaviour.

---

## 2. Import Path Findings

### `from db.` / `import db.`
```
grep -r "from db\.|import db\." --include="*.py" .
→ No matches found.
```

### `from database.` / `import database.`
Matches found in **60+ files** across every layer of the project
(seeding, automations, intelligence, tests, demo). Representative sample:

```
from database.schema import get_connection
from database.schema import init_db, CREATE_TABLES
from database.mappings import register_mapping, get_tool_id, find_unmapped
from database.mappings import get_canonical_id, generate_id, bulk_register
from database.mappings import get_all_mappings
```

**Canonical import path: `database.schema` and `database.mappings`.**

> ⚠️ **CLAUDE.md discrepancy:** The project structure section in CLAUDE.md lists
> `db/schema.py` and `db/mappings.py`, but the actual directory is `database/`.
> All code uses `from database.*`. The CLAUDE.md entry is stale and should be
> corrected to `database/`.

---

## 3. Auth Pattern Findings

Two complementary layers exist and are **both in active use**.

### Layer 1 — `credentials.py` (root-level)
Provides `get_credential(key: str) -> str`, which reads from `.env` / environment
variables. Used directly by:
- All root `create_contact_*.py` scripts
- `auth/simple_clients.py`, `auth/jobber_auth.py`, `auth/quickbooks_auth.py`,
  `auth/google_auth.py` (internally, to fetch tokens)
- Some seeding pushers (`push_hubspot.py`, `push_pipedrive.py`, `push_mailchimp.py`,
  `push_asana.py`, `push_quickbooks.py`, `add_hubspot_contact.py`,
  `add_pipedrive_entry.py`, `live_test_won_deal.py`)
- `setup/configure_tools.py`, `setup/populate_workspace.py`
- `demo/audit/auditors/audit_hubspot.py`, `demo/fixes/fix_hubspot_deal_mappings.py`

### Layer 2 — `auth/` module (unified client factory)
`auth/__init__.py` exposes `get_client(tool_name: str) -> Any`, which wraps
credentials into ready-to-use SDK clients. Used by:
- All six automation modules and `automations/runner.py`
- All intelligence syncers (`sync_*.py`)
- `tests/smoke_test_phase3.py` and integration tests
- `scripts/backfill_pipedrive_orgs.py`, `scripts/setup_hubspot_properties.py`
- `setup/configure_tools.py` (uses both layers)

### Relationship
`get_credential()` is the **low-level env-var reader**. `get_client()` is the
**high-level authenticated-client factory** that calls `get_credential()`
internally. They are complementary, not competing.

### Convention for simulation code
- **Use `auth.get_client(tool_name)`** for all API interactions in the simulation
  engine.
- `credentials.get_credential()` is acceptable for direct token access in
  seeding-style one-off scripts.
- Never import tool-specific auth modules directly (e.g., `auth.simple_clients`,
  `auth.jobber_auth`) — only use the `get_client()` public interface.

---

## 4. Automation Runner Interface Summary

**Entry point:** `python -m automations.runner [--poll] [--scheduled] [--pending] [--all] [--dry-run]`

### Mode 1: Poll (`run_poll`)
Queries external APIs for new events since the last poll. State is persisted in
the SQLite `poll_state` table (`automations.state.get_last_poll` /
`update_last_poll`).

| Trigger function | Source | Detection mechanism | Watermark key |
|---|---|---|---|
| `poll_pipedrive_won_deals` | Pipedrive REST `GET /v1/deals?status=won` | `won_time > last_processed_timestamp` | `("pipedrive", "deal_won")` |
| `poll_jobber_completed_jobs` | Jobber GraphQL `CompletedJobs` | `status=requires_invoicing` + `completedAt > since` | `("jobber", "completed_job")` |
| `poll_quickbooks_payments` | QBO SQL `WHERE MetaData.LastUpdatedTime > since` | `LastUpdatedTime > last_processed_timestamp` | `("quickbooks", "payment")` |
| `poll_sheets_negative_reviews` | Google Sheets `GET spreadsheetId/values/A:G` | row index > `last_processed_id` AND `rating <= 2` | `("google_sheets", "negative_review")` |

### Mode 2: Scheduled (`run_scheduled`)
Time-gated automations, no external trigger event.

| Automation | Cadence | Detection |
|---|---|---|
| `HubSpotQualifiedSync` | Every runner invocation (every ~5 min via cron) | Fetches HubSpot contacts with `lifecyclestage=salesqualifiedlead` (last 90 days), filters out those already present in `cross_tool_mapping` with a Pipedrive deal |
| `LeadLeakDetection` | At most once per 24 h (sentinel: `logs/.lead_leak_last_run`) | Finds HubSpot contacts that lack a Pipedrive person entry in `cross_tool_mapping` |
| `OverdueInvoiceEscalation` | Mondays only (`weekday() == 0`) | Queries QBO for invoices past due |

### Mode 3: Pending (`run_pending`)
Reads `pending_actions` table for rows with `status='pending' AND execute_after <= now`.
Currently handles one action type:

| `action_name` | What it does |
|---|---|
| `send_review_request` | Adds Mailchimp `review-requested` tag to a subscriber (deferred 7 days after job completion by `JobCompletionFlow`) |

### Record creations the runner owns
The following cross-tool writes are initiated by the runner. The simulation must
not independently create the same records, or they will be duplicated:

| Automation | Records created |
|---|---|
| `NewClientOnboarding` | HubSpot contact (lifecycle → customer), Mailchimp subscriber, Asana onboarding task, Slack message to `#new-clients` |
| `JobCompletionFlow` | QBO invoice, pending `send_review_request` row, Slack message to `#operations` |
| `PaymentReceived` | Pipedrive deal stage update, Slack message to `#operations` |
| `NegativeReviewResponse` | Asana task (complaint follow-up), Slack message to `#operations` |
| `HubSpotQualifiedSync` | Pipedrive person + deal at "Qualified" stage, `cross_tool_mapping` entries |
| `LeadLeakDetection` | Slack alert to `#sales` |
| `OverdueInvoiceEscalation` | Slack alert to `#operations` |
| Pending `send_review_request` | Mailchimp tag `review-requested` |

### SQLite state tables used by the runner
- `poll_state` — watermarks for all four poll triggers
- `pending_actions` — deferred actions queue
- `cross_tool_mapping` — used by `HubSpotQualifiedSync` to determine which
  HubSpot SQLs are already synced to Pipedrive

---

## 5. Recommended Actions

### A. Root scripts — move, don't delete
All seven `create_contact_*.py` and `create_sql_contact.py` scripts have zero
importers. Move them to `scripts/` (they are operational one-off scripts, not
seeding pushers). Update `Create_Hubspot_Contact/skill.md` path reference if
moved.

### B. Fix CLAUDE.md project structure
CLAUDE.md lists `db/schema.py` and `db/mappings.py` but the real directory is
`database/`. Update the structure diagram to show `database/` to avoid
confusing future sessions.

### C. Simulation engine — safe injection points
To trigger an automation without bypassing the runner's deduplication logic,
the simulation should inject data at the **source tool**, not into SQLite state:

| To simulate… | Inject here | Runner detects via |
|---|---|---|
| A Pipedrive won deal | Set deal `status=won` in Pipedrive | `poll_pipedrive_won_deals` watermark |
| A HubSpot SQL | Set `lifecyclestage=salesqualifiedlead` on HubSpot contact | `HubSpotQualifiedSync` `cross_tool_mapping` absence check |
| A completed Jobber job | Set job status to `requires_invoicing` in Jobber | `poll_jobber_completed_jobs` `completedAt` filter |
| A QBO payment | Create payment record in QuickBooks | `poll_quickbooks_payments` `LastUpdatedTime` filter |
| A negative review | Append row with `rating <= 2` to reviews Google Sheet | `poll_sheets_negative_reviews` row index advance |

### D. Do NOT write to `poll_state` from simulation code
Directly updating watermarks in `poll_state` would desync the runner and cause
it to skip or re-process real events. The simulation must only write to the
source tool APIs and let the runner's normal polling discover the change.

### E. Auth pattern for simulation code
Use `from auth import get_client` exclusively. Do not import
`credentials.get_credential` directly in the simulation engine — wrap any raw
token needs inside `auth/simple_clients.py` additions if necessary.
