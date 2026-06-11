# Alert Suppression & Invoice Self-Healing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop duplicate Slack alert spam, cap Google API hang time, and make the Jobber→QuickBooks invoice pipeline self-healing — plus heal the two concretely stuck jobs (SS-JOB-5981, SS-JOB-6058).

**Architecture:** Alert suppression state lives in a new `error_alert_state` PostgreSQL table keyed on `(tool_name, context)` because the automation-runner is a fresh cron container every 5 minutes — in-memory state cannot work. Invoice retries reuse the existing `pending_actions` table + the runner's PENDING mode; the reconciler enqueues retries instead of only alerting; `JobCompletionFlow` gains a `run_invoice_retry()` entry point shared by the pending handler and the one-off backfill script.

**Tech Stack:** Python 3.11, psycopg2 via `database.connection.get_connection`, pytest (`tests/test_automations/` with the PG test DB), Railway for live verification.

**Constraint:** The repo has unrelated uncommitted WIP in `CLAUDE.md`, `database/mappings.py`, `database/schema.py`, `intelligence/syncers/sync_jobber.py`, `scripts/backfill_jobber_job_enrichment.py`, `scripts/migrate_jobber_job_enrichment.py`, `tests/test_jobber_utils.py`, `tests/test_sync_jobber.py`. **Never edit or stage those files.** The `error_alert_state` DDL therefore goes in `automations/migrate.py` (where `pending_actions` DDL also lives) + a live migration script — NOT in `database/schema.py` until that WIP lands.

**Root causes being fixed (from the 2026-06-11 investigation):**
1. Google Sheets poll timed out 19:16–20:06 UTC (transient Google slowness) → 10 identical Slack alerts, 60s hang per cycle.
2. SS-JOB-6058: client SS-CLIENT-0521 has no QBO customer (onboarding's `create_quickbooks_customer` failed; verify step logged it; nothing retried).
3. SS-JOB-5981: Jobber client 142428287 is mapped by BOTH SS-LEAD-0335 (stale) and SS-CLIENT-0519; `reverse_resolve` picked the lead (no QBO mapping). `_promote_lead_to_client` only re-points hubspot/pipedrive mappings.
4. Systemic: a failed `create_quickbooks_invoice` is never retried (poll watermark advances at fetch time); the reconciler only alerts.

---

### Task 1: `error_alert_state` table (migrate.py + live migration script)

**Files:**
- Modify: `automations/migrate.py` (add DDL to `_MIGRATIONS`)
- Create: `scripts/migrate_error_alert_state.py`

DDL (used in both files):

```sql
CREATE TABLE IF NOT EXISTS error_alert_state (
    tool_name        TEXT NOT NULL,
    context_key      TEXT NOT NULL,
    first_seen       TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen        TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_posted_at   TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    suppressed_count INTEGER NOT NULL DEFAULT 0,
    episode_count    INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (tool_name, context_key)
)
```

Migration script pattern: same shape as existing `scripts/migrate_*.py` — `from database.connection import get_connection`, execute DDL, print confirmation, `--dry-run` prints only. Run later against Railway with `DATABASE_URL=<railway public url>`.

- [ ] Add DDL to `_MIGRATIONS` in `automations/migrate.py` and update its summary print line
- [ ] Write `scripts/migrate_error_alert_state.py`
- [ ] Run `python3 scripts/migrate_error_alert_state.py --dry-run` locally → prints DDL, no errors
- [ ] Commit: `feat: add error_alert_state table for Slack alert suppression`

### Task 2: Alert suppression + recovery in error_reporter

**Files:**
- Modify: `simulation/error_reporter.py`
- Modify: `automations/runner.py` (recovery wiring)
- Test: `tests/test_automations/test_error_suppression.py` (new; uses `mock_db` fixture so `_MIGRATIONS` creates the table)

Design:
- `ALERT_SUPPRESSION_WINDOW_MINUTES = 30` (matches `ESCALATION_WINDOW_MINUTES`; covers 6 runner cron cycles — the June 11 incident would have posted 2 alerts instead of 10).
- Key = `(tool_name, context[:500])`. Category intentionally NOT in the key so a poll alternating timeout/server_error stays one alert stream.
- `_suppression_decision(conn, tool_name, key, window_minutes) -> (should_post, suppressed_count)`: row missing → insert, post. Window elapsed (SQL `CURRENT_TIMESTAMP - last_posted_at >= make_interval(mins => %s)`) → reset `last_posted_at`, `suppressed_count=0`, `episode_count+=1`, post with prior suppressed_count. Else → `suppressed_count+=1`, `last_seen=now`, `episode_count+=1`, suppress.
- `_check_suppression(tool_name, key)` wraps it with its own `get_connection()`; **fail-open** (post) on any DB error so report_error never breaks and DB-less tests/local runs behave as before.
- In `report_error()`: after translation, before posting, when `not dry_run`: call `_check_suppression`; if suppressed → log INFO and return True (treated as handled). If posting and prior `suppressed_count > 0`, append `" (occurred N more times since the last alert)"` to `what_happened`.
- `report_recovery(tool_name, context, dry_run=False) -> bool`: look up the row; if missing → return False (no episode). DELETE the row; post the one-liner only when `episode_count >= 2` (a single blip + recovery message would double noise). Message: `:white_check_mark: Recovered: {tool} — {context} (failed N times since first failure)` posted via plain `chat_postMessage` text. Never raises.
- Runner wiring: after each of the 4 poll successes in `run_poll` call `report_recovery(<tool>, <same context string as the except branch>, dry_run=dry_run)`.

Tests (in `tests/test_automations/test_error_suppression.py`, using `mock_db`):
- first call posts, second within window suppresses, count increments (drive `_suppression_decision` directly with the test conn; backdate `last_posted_at` with an UPDATE to test window expiry)
- `report_recovery` returns False with no row; posts and deletes row when `episode_count >= 2` (patch `simulation.error_reporter.get_client` and `setup_channel`)
- `_check_suppression` fail-open: point at a context with DB patched to raise → returns `(True, 0)`

- [ ] Write failing tests
- [ ] Implement suppression + recovery in `error_reporter.py`
- [ ] Wire `report_recovery` into `run_poll` for all four polls
- [ ] `python3 -m pytest tests/test_automations/test_error_suppression.py -v` → PASS; `python3 tests/test_error_reporter.py -v` still PASS
- [ ] Commit: `feat: suppress duplicate Slack alerts and post recovery notices`

### Task 3: Google Sheets client socket timeout

**Files:**
- Modify: `auth/google_auth.py`

```python
GOOGLE_HTTP_TIMEOUT_SECONDS = 20
# ESTIMATED — reasoning: healthy Sheets reads complete in ~1s (Railway runner logs);
# worst observed healthy read was 8.5s during the 2026-06-11 Google slowness window.
# The library default (60s socket timeout) stretched each failing cron cycle to 69s.
# 20s keeps >2x headroom over the worst healthy read while capping wasted time.

def _authorized_http(credentials, timeout: int = GOOGLE_HTTP_TIMEOUT_SECONDS):
    """AuthorizedHttp with an explicit socket timeout (build() default is 60s)."""
    import httplib2
    from google_auth_httplib2 import AuthorizedHttp
    return AuthorizedHttp(credentials, http=httplib2.Http(timeout=timeout))
```

`get_sheets_service()` becomes `build("sheets", "v4", http=_authorized_http(get_google_credentials()))` (when `http=` is passed, `credentials=` must be omitted). Scope: sheets only, per the approved fix.

- [ ] Implement; verify `python3 -c "from auth.google_auth import _authorized_http"` imports
- [ ] Commit: `feat: cap Google Sheets API socket timeout at 20s`

### Task 4: JobCompletionFlow — retry entry point + graceful degradation

**Files:**
- Modify: `automations/job_completion_flow.py`
- Test: `tests/test_automations/test_job_completion_flow.py`

Refactor (behavior-preserving for `run()`):
- Extract the local-record block of Action 1 into `_record_local_invoice(self, ctx, invoice_id, invoice_amount) -> Optional[str]` (the `invoices` INSERT + `register_mapping`, returning the `SS-INV-*` id; dry_run returns `"dry-run-local-invoice-id"`).
- New `_maybe_queue_invoice_retry(self, ctx, exc)`: only for `MappingNotFoundError` with a known `canonical_job_id`; dedupe `pending_actions` (`action_name='create_invoice' AND status='pending' AND trigger_context LIKE '%"<job>"%'`); `schedule_delayed_action("create_invoice", {"canonical_job_id": ...}, delay_hours=1)` — 1h gives the onboarding `create_qbo_customer` retry (Task 6) time to land first; then `report_error(str(exc), tool_name="quickbooks", context=f"creating invoice for {client} (job {id}) — retry queued", severity="warning")` inside try/except so alerting can never break the flow.
- `run()` Action 1 `except` branch now also calls `_maybe_queue_invoice_retry(ctx, exc)`.
- New `run_invoice_retry(self, trigger_event) -> Optional[str]`: build ctx; if an `invoices` row already exists for `canonical_job_id` → log + return its id (idempotent); else `_action_quickbooks_invoice` (raises on failure so pending mode marks the action failed) → `_record_local_invoice` → `log_action` success with `trigger_source=f"retry:create_invoice:{job}"` → best-effort Jobber writeback (Action 1b, try/except + log).

Tests:
- missing QBO mapping (client with jobber mapping, no quickbooks) → `pending_actions` row queued, automation_log failed row, `report_error` patched + asserted
- `run_invoice_retry` on SS-JOB-0001 fixtures creates QBO invoice (patched `requests.post`), `invoices` row, INV mapping
- `run_invoice_retry` skips when invoice already exists (no HTTP call)

- [ ] Write failing tests; run → FAIL
- [ ] Implement refactor + new methods
- [ ] `python3 -m pytest tests/test_automations/test_job_completion_flow.py -v` → PASS (old tests intact)
- [ ] Commit: `feat: queue invoice retries and alert at failure time in JobCompletionFlow`

### Task 5: Runner pending handlers (`create_invoice`, `create_qbo_customer`) + onboarding retry method

**Files:**
- Modify: `automations/runner.py` (`_dispatch_pending` + two handlers)
- Modify: `automations/new_client_onboarding.py` (`retry_quickbooks_customer`)
- Test: `tests/test_automations/test_runner.py`, `tests/test_automations/test_new_client_onboarding.py`

`_handle_create_invoice(clients, db, context, dry_run)`:
- require `canonical_job_id`; load the jobs row; if status != 'completed' → log + return; resolve `jobber` mappings for job and client (job mapping required — refuse to invoice a job we can't tie back to Jobber); build event `{job_id, client_id, completed_at, service_type: None → DB-driven, is_recurring: False}`; `JobCompletionFlow(clients, db, dry_run).run_invoice_retry(event)`.

`_handle_create_qbo_customer(clients, db, context, dry_run)`:
- require `canonical_id`; `NewClientOnboarding(clients, db, dry_run).retry_quickbooks_customer(canonical_id)`.

`NewClientOnboarding.retry_quickbooks_customer(canonical_id)`:
- if `quickbooks` mapping exists → backfill missing `quickbooks_customer` alias and return existing id (idempotent)
- else load `clients` row (`first_name, last_name, company_name, email, phone, client_type` — all columns exist), build minimal ctx, call existing `_action_quickbooks(ctx)` (already handles QBO 6240 duplicate-name lookup), register both mappings, `log_action` success/failed with `trigger_source=f"retry:create_qbo_customer:{canonical_id}"`, raise on failure.

Tests:
- `_dispatch_pending` routes `create_invoice` / `create_qbo_customer` (patch the flow classes, assert calls)
- `retry_quickbooks_customer` creates customer (patched `requests.post` → `{"Customer": {"Id": "555"}}`) and registers both mappings; second call returns existing without HTTP

- [ ] Write failing tests; run → FAIL
- [ ] Implement handlers + retry method
- [ ] `python3 -m pytest tests/test_automations/test_runner.py tests/test_automations/test_new_client_onboarding.py -v` → PASS
- [ ] Commit: `feat: pending-action handlers for invoice and QBO customer retries`

### Task 6: Onboarding hardening — verify failure enqueues QBO customer retry

**Files:**
- Modify: `automations/new_client_onboarding.py` (`_action_verify_mappings`)
- Test: `tests/test_automations/test_new_client_onboarding.py`

When `missing ∩ {quickbooks, quickbooks_customer}` and not dry_run: dedupe `pending_actions` (`action_name='create_qbo_customer' AND status='pending' AND trigger_context LIKE '%"<canonical>"%'`) then `schedule_delayed_action("create_qbo_customer", {"canonical_id": canonical_id}, delay_hours=1)` — 1h because the original failure was seconds ago (likely a transient QBO error); immediate retry would usually re-fail and failed pending actions are not auto-retried. Slack message gains "Automatic retry queued."

- [ ] Write failing test (verify with missing QBO mapping → pending row; second run no duplicate)
- [ ] Implement
- [ ] `python3 -m pytest tests/test_automations/test_new_client_onboarding.py -v` → PASS
- [ ] Commit: `feat: onboarding verify failure queues QuickBooks customer retry`

### Task 7: Identity resolution — prefer CLIENT, migrate mappings on promotion

**Files:**
- Modify: `automations/utils/id_resolver.py` (`reverse_resolve`)
- Modify: `automations/new_client_onboarding.py` (`_promote_lead_to_client`)
- Test: `tests/test_automations/test_id_resolver.py` (new), `tests/test_automations/test_new_client_onboarding.py`

`reverse_resolve` without explicit `entity_type` gets deterministic preference (currently arbitrary row when one tool ID maps to multiple canonicals — the SS-LEAD-0335 bug):

```sql
ORDER BY CASE entity_type
             WHEN 'CLIENT' THEN 0
             WHEN 'PROP'   THEN 1
             WHEN 'LEAD'   THEN 2
             ELSE 3
         END,
         canonical_id DESC
LIMIT 1
```

(`PROP` confirmed as the proposal entity_type in `database.mappings._ENTITY_META`.)

`_promote_lead_to_client`: extend the re-point tool list from `('hubspot','pipedrive','pipedrive_person')` to also include `'jobber','jobber_property','mailchimp','quickbooks','quickbooks_customer'` — a Jobber/QBO record created pre-conversion belongs to the client after promotion; leaving it on the lead is exactly what stranded SS-JOB-5981's invoice. No uniqueness risk: the client canonical is freshly minted, so it has no existing rows. Update the docstring.

- [ ] Write failing tests (dual mapping LEAD+CLIENT → reverse_resolve returns CLIENT; promotion re-points a seeded jobber mapping)
- [ ] Implement both changes
- [ ] `python3 -m pytest tests/test_automations/test_id_resolver.py tests/test_automations/test_new_client_onboarding.py -v` → PASS
- [ ] Commit: `fix: prefer CLIENT records in reverse ID resolution; migrate all tool mappings on lead promotion`

### Task 8: Reconciler healer — enqueue invoice retries

**Files:**
- Modify: `simulation/reconciliation/reconciler.py` (`run_automation_health_check`)
- Test: `tests/test_automations/test_reconciler_healer.py` (new; `Reconciler()` works against the test DB because `get_connection` ignores `db_path` and `DATABASE_URL` is set by conftest)

After collecting uninvoiced job IDs and before posting to Slack: when not dry_run, for each job enqueue `pending_actions (automation_name='ReconciliationHealer', action_name='create_invoice', trigger_context=json {"canonical_job_id": id}, execute_after=now UTC "%Y-%m-%dT%H:%M:%SZ")` with the same LIKE-based dedupe against `status='pending'`. Wrap the whole enqueue block in try/except (the reconciler must never crash on a missing table). Append `"{queued} queued for automatic retry."` to the Slack details. Known acceptable behavior: permanently un-invoiceable jobs (e.g. unresolved commercial pricing) re-queue daily and fail — identical cadence to today's daily alert, but now self-heals the moment the blocker clears.

- [ ] Write failing test (completed job >24h, no invoice → pending row; re-run → no duplicate; patch `report_reconciliation_issue`)
- [ ] Implement
- [ ] `python3 -m pytest tests/test_automations/test_reconciler_healer.py -v` → PASS
- [ ] Commit: `feat: reconciler queues invoice-creation retries instead of only alerting`

### Task 9: Backfill script + live healing run

**Files:**
- Create: `scripts/backfill_missing_invoices_20260611.py`

Hardcoded incident constants (documented in the docstring with the investigation summary):
`STALE_LEAD = "SS-LEAD-0335"`, `PROMOTED_CLIENT = "SS-CLIENT-0519"`, `QBO_CUSTOMER_NEEDED = "SS-CLIENT-0521"`, `JOBS = ["SS-JOB-5981", "SS-JOB-6058"]`.

Steps (each idempotent, `--dry-run` supported):
1. **Retire stale lead mappings:** for `jobber`/`jobber_property` rows on SS-LEAD-0335 — if SS-CLIENT-0519 already has the same tool mapping, DELETE the lead row; otherwise UPDATE it to the client (entity_type='CLIENT').
2. **Ensure QBO customer for SS-CLIENT-0521:** `NewClientOnboarding(get_client, db, dry_run).retry_quickbooks_customer("SS-CLIENT-0521")` (checks `cross_tool_mapping` first — the required duplicate guard).
3. **Create the two invoices:** reuse the runner handler: `_handle_create_invoice(get_client, db, {"canonical_job_id": job_id}, dry_run)` — which checks for an existing invoice, then creates QBO invoice + local row + INV mapping + Jobber writeback with the job's real completion date.

Safety guard: when not `--dry-run`, abort unless `JOBBER_TOKEN_KEEPER_ENABLED=1` (Jobber refresh tokens are single-use and owned by the Railway token-keeper; a local self-refresh would rotate the token and break production auth).

Live run sequence:
- [ ] Write the script
- [ ] `DATABASE_URL=<railway> python3 scripts/migrate_error_alert_state.py` (apply Task 1 live)
- [ ] `DATABASE_URL=<railway> JOBBER_TOKEN_KEEPER_ENABLED=1 python3 scripts/backfill_missing_invoices_20260611.py --dry-run` → review output
- [ ] Run for real; verify: `invoices` rows exist for both jobs, mappings registered, SS-LEAD-0335 has no `jobber` mapping, SS-CLIENT-0521 has `quickbooks` mapping
- [ ] Commit: `fix: backfill invoices for SS-JOB-5981/SS-JOB-6058 and retire stale lead mapping`

### Task 10: Full test pass + deploy

- [ ] `python3 -m pytest tests/test_automations/ -v` → all PASS
- [ ] `python3 tests/test_error_reporter.py -v` → PASS
- [ ] Confirm nothing from the unrelated WIP files is staged (`git status`)
- [ ] Push to main → Railway auto-deploys; spot-check next runner cycle logs
