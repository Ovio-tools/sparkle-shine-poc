# HubSpot-to-Pipedrive Sync Collision Fix

**Date:** 2026-04-06
**Status:** Approved
**Scope:** `automations/hubspot_qualified_sync.py`, `database/schema.py`, `scripts/repair_sync_collisions.py`

---

## Problem

The HubSpot-to-Pipedrive sync automation (`HubSpotQualifiedSync`) has three structural problems that cause recurring failures:

1. **No email-level deduplication.** `_get_or_create_person` searches Pipedrive by email, finds an existing person, and returns it without checking if that person is already mapped to a different canonical ID. This causes a collision in `_register_mappings`.

2. **Non-atomic allocation.** `_allocate_lead_id()` commits the HubSpot mapping independently before the Pipedrive step. If Pipedrive fails, the HubSpot mapping is orphaned. The simulation's `register_mapping` (which uses `ON CONFLICT DO UPDATE`) can then overwrite it.

3. **No circuit breaker.** Failed contacts retry every 5 minutes indefinitely. Three stuck contacts generated 329 failure log entries over 2 days.

### Impact

- 3 HubSpot contacts permanently stuck with no Pipedrive mapping
- Person 220 mapped to both SS-CLIENT-0328 and SS-LEAD-0051 (integrity violation)
- 329 wasted automation_log entries from futile retries

---

## Solution

### Section 1: Email-Level Dedup (Merge Flow)

**Files:** `hubspot_qualified_sync.py`

**Changes to `_get_or_create_person`:**
- Returns a tuple: `(person_id, is_new, existing_canonical_id)`
- When a Pipedrive person is found, queries `cross_tool_mapping` for the canonical ID that owns it

**New helper `_check_person_ownership`:**
```sql
SELECT canonical_id FROM cross_tool_mapping
WHERE tool_name = 'pipedrive_person' AND tool_specific_id = :person_id
```

**Changes to `_create_pipedrive_records`:**
- When `existing_canonical_id` differs from the freshly allocated one:
  - Register the new HubSpot contact ID under the existing canonical ID (merge)
  - Discard the freshly allocated canonical ID (not yet committed, so no cleanup needed)
  - Log: `"Merged HubSpot {hs_id} into {existing_canonical_id} (shared email: {email})"`
  - Create a deal if the existing canonical ID doesn't have one

### Section 2: Atomic Allocation

**Files:** `hubspot_qualified_sync.py`

**Changes to `_allocate_lead_id()`:**
- Remove the `INSERT INTO cross_tool_mapping` for the HubSpot mapping
- Remove the independent `COMMIT` after HubSpot mapping
- Keep `SELECT ... FOR UPDATE` + entity table insert for serialized ID allocation
- Return only the canonical ID

**Changes to `_register_mappings()`:**
- Add HubSpot mapping registration alongside Pipedrive mappings
- All three mappings (hubspot, pipedrive_person, pipedrive deal) written in a single transaction
- If any collision is detected, entire transaction rolls back — no orphaned mappings

**Merge flow exception:** When merging, skip `_allocate_lead_id()` entirely. Register HubSpot mapping under existing canonical ID. Pipedrive person mapping already exists. Only create/register deal if missing.

### Section 3: Circuit Breaker

**Files:** `hubspot_qualified_sync.py`, `database/schema.py`

**New table `sync_skip_list`:**
```sql
CREATE TABLE IF NOT EXISTS sync_skip_list (
    tool_name TEXT NOT NULL,
    tool_specific_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    detail TEXT,
    skipped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tool_name, tool_specific_id)
);
```

**Skip check in `_filter_new_contacts()`:**
- Before classifying a contact, check `sync_skip_list` for its HubSpot ID
- If present, exclude silently and count in `permanently_skipped` summary counter

**When to add to skip list:**
- After 3 consecutive failures for the same HubSpot contact ID (counted via `automation_log`)
- On unresolvable collision (merge flow itself fails)
- Reason codes: `"collision_limit"`, `"unresolvable_merge"`, `"mapping_integrity"`

**Failure count query:**
```sql
SELECT COUNT(*) AS cnt FROM automation_log
WHERE automation_name = 'HubSpotQualifiedSync'
  AND action_name = 'sync_contact_to_pipedrive'
  AND status = 'error'
  AND action_target = :hubspot_id
```

**Slack visibility:** Summary adds `"N contact(s) permanently skipped (see sync_skip_list)"` when applicable.

**Manual override:** Delete the row from `sync_skip_list` to retry.

### Section 4: Repair Flow Alignment

**Files:** `hubspot_qualified_sync.py`

- Unify the truly_new and repair (`needs_pipedrive`) flows through `_register_mappings`
- Remove direct `register_mapping` calls from `id_resolver.py` in this automation
- Merge detection and circuit breaker apply equally to both flows
- Only difference: truly_new calls `_allocate_lead_id()`, repair uses existing canonical ID

### Section 5: Data Repair (Standalone Script)

**Files:** `scripts/repair_sync_collisions.py`

One-time script to fix existing collision damage:

1. For each of the 3 stuck HubSpot contacts (`464805888705`, `464850822862`, `464848180951`):
   - Look up email in Pipedrive
   - Find the canonical ID that owns the Pipedrive person
   - Register the HubSpot ID under that canonical ID (merge)
   - Create a deal if missing

2. For person 220 dual-mapping: keep `SS-CLIENT-0328` mapping, remove `SS-LEAD-0051` mapping.

3. Log all changes to `automation_log` for audit trail.

---

## Migration

A migration script (`scripts/add_sync_skip_list.py`) applies `CREATE TABLE IF NOT EXISTS sync_skip_list` to the live PostgreSQL database. Also add the DDL to `database/schema.py` so fresh deployments include it.

---

## Files Changed

| File | Change |
|------|--------|
| `automations/hubspot_qualified_sync.py` | Sections 1-4: dedup, atomic allocation, circuit breaker, unified flow |
| `database/schema.py` | Add `sync_skip_list` DDL |
| `scripts/add_sync_skip_list.py` | Migration: create table on live DB |
| `scripts/repair_sync_collisions.py` | One-time data repair |

## Files NOT Changed

| File | Why |
|------|-----|
| `automations/utils/id_resolver.py` | No changes needed — we stop calling it from this automation |
| `database/mappings.py` | `ON CONFLICT DO UPDATE` stays — it's correct for the simulation's use case |
| `automations/runner.py` | No changes — dispatches HubSpotQualifiedSync the same way |
