# Railway Pause State — 2026-05-01

This file is the runbook for **resuming** the Sparkle & Shine POC after a temporary pause that began on 2026-05-01. Do not delete it during the pause. Move it to `docs/operations/archive/` only after the project is fully resumed and verified.

> **Status (2026-09-01 review):** git history shows continuous production changes from 2026-05-25 onward (PR #13 merged 2026-05-26, simulation/Jobber/alerting fixes through June), so the project was evidently resumed around 2026-05-25/26 following [docs/superpowers/plans/2026-05-25-railway-resume.md](../../superpowers/plans/2026-05-25-railway-resume.md). The post-pause and post-resume verification checklists below were never filled in at the time; live verification was done on 2026-09-01 (see [Resume outcome](#resume-outcome-verified-2026-09-01) at the end of this file) and the file was archived the same day. The 2026-09-01 review also corrected factual errors in the resume and contingency sections — follow the corrected procedure for the next pause; the checked-off pause checklist is left as the historical record of what was actually done. Reusable lessons are summarised in [docs/railway.md](../../railway.md#pausing-and-resuming-the-project).

> ⚠️ **Read the [Incident: 2026-05-01 to 2026-05-04 worker auto-redeploy](#incident-2026-05-01-to-2026-05-04-worker-auto-redeploy) section before resuming.** The original pause plan missed Railway's "Auto Deploy on Push" hook. Pushing the snapshot file to `main` resurrected `simulation-engine` and `token-keeper` ~16-25 minutes after they were stopped, and they ran for ~54 hours generating data and Slack alerts before being caught. The fix (disconnecting the GitHub repo from those two services) means resume now requires **reconnecting the repo** before redeploying — not just clicking redeploy.

## Pause metadata

| Field | Value |
|-------|-------|
| Pause start | 2026-05-01 |
| Expected duration | 1-3 months |
| Reason | Project temporarily paused (owner request) |
| Approach | Railway-native pause (no code changes) |
| Plan reference | Local Claude Code plan file (outside the repo; no longer available). The resume procedure was later formalised in [docs/superpowers/plans/2026-05-25-railway-resume.md](../../superpowers/plans/2026-05-25-railway-resume.md) |

**Token expiry watch dates:**
- QuickBooks refresh token (100-day lifetime): assume expired after **~2026-08-09** (100 days from pause start)
- Jobber refresh token: rotates per refresh; assume re-OAuth required for any pause longer than ~30 days
- HubSpot Private App Token: never expires
- Pipedrive API token: never expires
- Asana PAT: never expires
- Mailchimp API key: never expires
- Slack Bot OAuth Token: never expires

## Branch state at pause

- Branch: `fix/hubspot-syncer-proposal-linkage` (clean, in sync with origin)
- `briefings/` folder: clean (all reports committed)
- `config/tool_ids.json`: present, embedded below

## Service inventory (as captured 2026-05-01)

Captured via `railway service status --all` (since deprecated — use `railway service list`) with project linked to `sparkle-shine-poc / production`.

| # | Service | Service ID | Type | Status at pause | Custom Config | Start Command | Cron Schedule (UTC) |
|---|---------|------------|------|-----------------|---------------|---------------|---------------------|
| 1 | sales-outreach | `bfa5bd57-6186-43ed-bed4-82874861d8db` | cron | STOPPED (between firings) | `/railway.toml` | `bash scripts/start_sales_outreach.sh` | `*/30 * * * *` |
| 2 | automation-runner | `8c7624fd-0587-4ad6-b4d8-a16f52af3fb3` | cron | SUCCESS | `/railway.toml` | `python -m automations.runner --all` | `*/5 * * * *` |
| 3 | intelligence-daily | `62097fee-8ec7-49d1-9320-2df0825331d7` | cron | STOPPED (between firings) | `/railway.toml` | `python -m intelligence.runner --report-type daily` | `0 11 * * 1-5` (6 AM CDT M-F) |
| 4 | intelligence-weekly | `e551aeec-bbdd-49a3-b2eb-8bf345f122dc` | cron | SUCCESS | `/railway.toml` | `python -m intelligence.runner --report-type weekly` | `0 13 * * 0` (8 AM CDT Sun) |
| 5 | simulation-engine | `a0a4fc19-9f4c-4fa9-8270-f964d85ce182` | always-on worker | SUCCESS | `/railway.worker.toml` | `python -m simulation.engine` | n/a |
| 6 | token-keeper | `0b1fad75-56c8-4d08-91a6-5c42c782be7b` | always-on worker | SUCCESS | `/railway.worker.toml` | `bash scripts/start_token_keeper.sh` | n/a |
| — | Postgres | `03fbdbf8-9af0-4b8c-a0ee-be6f658159f4` | database | **REMAINS RUNNING** | n/a | n/a | n/a |

### Wrapper script contents (for reference)

`scripts/start_sales_outreach.sh`:
```bash
#!/bin/bash
exec python -m automations.automation_07_sales_outreach --live
```

`scripts/start_token_keeper.sh`:
```bash
#!/bin/bash
exec python -m services.token_keeper
```

### Railway config files

`railway.toml` (cron services):
```toml
[build]
builder = "nixpacks"

[deploy]
restartPolicyType = "ON_FAILURE"
restartPolicyMaxRetries = 3
```

`railway.worker.toml` (always-on workers):
```toml
[build]
builder = "nixpacks"

[deploy]
restartPolicyType = "ALWAYS"
```

## Inline copy of `config/tool_ids.json` at pause time

Embedded so resume is decoupled from git history. If the new accounts produce different IDs at resume time, the originals are still here for reference.

```json
{
  "pipedrive": {
    "pipelines": {"Cleaning Services Sales": 2, "Lost/Inactive": 3},
    "stages": {
      "New Lead": 7, "Qualified": 8, "Site Visit Scheduled": 9,
      "Proposal Sent": 10, "Negotiation": 11, "Closed Won": 12, "Closed Lost": 13
    },
    "deal_fields": {
      "Client Type": "0c33b3b00286f14e71a0e0845a2180d6b524dd39",
      "Service Type": "29d12ce12832b01642ca5b6b764fed836201ae88",
      "Estimated Monthly Value": "f25efe3a76061b039c0aeb9482e22ea8a276e6e2",
      "Lead Source": "a44f485b9f59b407da74b048ed7e09c67852c447"
    },
    "person_fields": {
      "HubSpot Contact ID": "a70495529a73cf3473d1a10528cf7052e56d217e",
      "Jobber Client ID": "f62df7e3465c734b05eea342e1db1a424a88489a",
      "Acquisition Source": "d021197a6120bd6de2d5dc329ce66e06b300d311",
      "Neighborhood": "c522a9fe547842f66319659855399dd086763f9d"
    }
  },
  "mailchimp": {
    "audience_id": "92f05d2d65",
    "merge_fields": {"PHONE": 4, "NEIGHBORHD": 5, "CLIENTTYPE": 6, "SVCTYPE": 7, "LEADSOURCE": 8},
    "segments": {
      "Active Residential Clients": 18732, "Active Commercial Clients": 18733,
      "Churned Clients": 18734, "High-Value Clients": 18735
    }
  },
  "asana": {
    "workspace_gid": "1213704231015587",
    "team_gid": "1213704231015589",
    "projects": {
      "Sales Pipeline Tasks": "1213719393240330",
      "Marketing Calendar": "1213719401725621",
      "Admin & Operations": "1213719394454339",
      "Client Success": "1213719346640011"
    }
  },
  "slack": {
    "channels": {
      "daily-briefing": "C0AML3Q8PSM",
      "operations": "C0AM76H9K34",
      "sales": "C0ALRNT2Z8F",
      "new-clients": "C0AN1EFM1A4",
      "reviews-and-feedback": "C0ANYMAEELC"
    }
  },
  "google": {
    "docs_count": 8,
    "sheets_count": 5,
    "calendar_events_count": 15
  }
}
```

Full file is at [config/tool_ids.json](../../../config/tool_ids.json) — abbreviated here for readability; full IDs (Asana sections, all Google Drive file IDs, all Calendar event IDs, all QuickBooks items/accounts) are in the JSON.

## Pre-pause audits (run while `token-keeper` is still deployed)

These were not run from this session because production database access requires explicit per-command approval. Run them yourself, paste results back into this file under each heading.

The Section A command executes inside the `token-keeper` container via `railway ssh`, so it only works while that service still has a live deployment — run it before pause step 6 removes the deployment, or on resume after Step 3 brings it back.

### A. cross_tool_mapping completeness

```bash
# From repo root, with railway linked to sparkle-shine-poc / production
railway ssh --service token-keeper --environment production "cd /app && python3 -c \"
import json
from database.mappings import find_unmapped, _ENTITY_META
TOOLS = ['hubspot','pipedrive','jobber','quickbooks','mailchimp','asana']
result = {}
for entity in _ENTITY_META.keys():
    result[entity] = {tool: len(find_unmapped(entity, tool)) for tool in TOOLS}
print(json.dumps(result, indent=2))
\""
```

> Note: `find_unmapped(entity_type, tool_name)` — entity type first, then tool (matches [docs/skills/canonical-record.md](../../skills/canonical-record.md)). `_ENTITY_META` in [database/mappings.py](../../../database/mappings.py) covers all 14 canonical entity types (CLIENT, LEAD, EMP, CREW, JOB, RECUR, PROP, INV, PAY, CAMP, REV, TASK, CAL, DOC), which is why the results below have 14 rows.

**Results (captured 2026-05-01, post-pause):**

```json
{
  "CLIENT":  {"hubspot":     8, "pipedrive":   459, "jobber":   158, "quickbooks":   159, "mailchimp":   161, "asana":   494},
  "LEAD":    {"hubspot":     7, "pipedrive":   228, "jobber":   339, "quickbooks":   339, "mailchimp":   199, "asana":   339},
  "EMP":     {"hubspot":    18, "pipedrive":    18, "jobber":    18, "quickbooks":    18, "mailchimp":    18, "asana":    18},
  "CREW":    {"hubspot":     4, "pipedrive":     4, "jobber":     4, "quickbooks":     4, "mailchimp":     4, "asana":     4},
  "JOB":     {"hubspot":  5766, "pipedrive":  5766, "jobber":     2, "quickbooks":  5766, "mailchimp":  5766, "asana":  5766},
  "RECUR":   {"hubspot":   251, "pipedrive":   251, "jobber":   251, "quickbooks":   251, "mailchimp":   251, "asana":   251},
  "PROP":    {"hubspot":   271, "pipedrive":   109, "jobber":   272, "quickbooks":   272, "mailchimp":   272, "asana":   272},
  "INV":     {"hubspot":  7139, "pipedrive":  7139, "jobber":  7139, "quickbooks":     2, "mailchimp":  7139, "asana":  7139},
  "PAY":     {"hubspot":  4396, "pipedrive":  4396, "jobber":  4396, "quickbooks":     0, "mailchimp":  4396, "asana":  4396},
  "CAMP":    {"hubspot":     5, "pipedrive":     5, "jobber":     5, "quickbooks":     5, "mailchimp":     0, "asana":     5},
  "REV":     {"hubspot": 13166, "pipedrive": 13166, "jobber": 13166, "quickbooks": 13166, "mailchimp": 13166, "asana": 13166},
  "TASK":    {"hubspot":  1731, "pipedrive":  1731, "jobber":  1731, "quickbooks":  1731, "mailchimp":  1731, "asana":   130},
  "CAL":     {"hubspot":   169, "pipedrive":   169, "jobber":   169, "quickbooks":   169, "mailchimp":   169, "asana":   169},
  "DOC":     {"hubspot":     8, "pipedrive":     8, "jobber":     8, "quickbooks":     8, "mailchimp":     8, "asana":     8}
}
```

**Interpretation — most counts are by design, three are worth a follow-up:**

The number reported is *unmapped canonical records* — records in the Postgres canonical table that have no row in `cross_tool_mapping` for that tool. Many of these gaps are intentional:
- `EMP`, `CREW`, `REV` (reviews), `CAL` (Google Calendar events), `DOC` (Google Drive docs) are not mirrored across most tools — those numbers should be high.
- `LEAD` not mapped to Jobber/QuickBooks/Asana is correct (leads only flow to HubSpot/Pipedrive/Mailchimp).
- `JOB`, `RECUR`, `PROP` not mapped to most tools is correct (jobs live in Jobber/QBO; recurring agreements in Jobber; proposals in Pipedrive).
- `INV` and `PAY` are managed in QuickBooks only.
- `CAMP` is Mailchimp only.
- `CLIENT` not mapped to Pipedrive (459) is the **deliberate lead-leak detector** per [CLAUDE.md](../../../CLAUDE.md): "NEVER register a Pipedrive mapping for SQL contacts. Only register the HubSpot mapping. The missing Pipedrive mapping is what triggers the automation runner's lead-leak detection." Expected.

Three findings worth noting (not blockers — re-seed pushers handle drift):

| Finding | Count | Significance |
|---------|-------|--------------|
| `JOB.jobber` | 2 unmapped | 2 jobs in Postgres without a Jobber mapping. Either pre-Jobber records, or a sync miss. Investigate if jobs are missing from Jobber UI on resume. |
| `INV.quickbooks` | 2 unmapped | 2 invoices in Postgres without a QBO mapping. Same — investigate if invoices are missing in QBO on resume. |
| `TASK.asana` | 130 unmapped | 130 canonical tasks without an Asana mapping. Likely tasks created by automations that failed to push, or tasks intentionally not mirrored. Worth a spot-check on resume. |

`PAY.quickbooks = 0` and `CAMP.mailchimp = 0` confirm that payments and campaigns are fully mirrored to their authoritative tools. Good signal for re-seed readiness.

### B. oauth_tokens table freshness

```bash
railway connect Postgres
# Then in psql:
SELECT tool_name, created_at, updated_at,
       NOW() - updated_at AS age_since_refresh
FROM oauth_tokens
ORDER BY tool_name;
```

**Results (captured 2026-05-01, post-pause):**

```
 tool_name  |         updated_at         | age_since_refresh
------------+----------------------------+-------------------
 google     | 2026-05-01 11:04:46.445476 | 10:07:24.98883
 jobber     | 2026-05-01 20:38:04.78988  | 00:34:06.644426
 quickbooks | 2026-05-01 21:01:49.848017 | 00:10:21.586289
```

All three OAuth-based tokens (Google, Jobber, QuickBooks) have a recent `updated_at` — well inside the 24-hour freshness window. **At resume, treat these timestamps as the start of the clock**:
- QuickBooks: 100-day refresh-token lifetime → **expires ~2026-08-09** (count from `2026-05-01 21:01`)
- Jobber: rotating refresh; assume re-OAuth required if pause exceeds ~30 days from `2026-05-01 20:38`
- Google: refresh tokens issued in "Testing" mode expire in 7 days; if the GCP app is published, no expiry → confirm publication status before pause exceeds 7 days from `2026-05-01 11:04`

HubSpot, Pipedrive, Asana, Mailchimp, Slack are absent from `oauth_tokens` because they use static API tokens (no OAuth refresh flow). That is correct — the table is only populated for OAuth-based tools.

### C. Service log silence baseline

Before pausing, capture last activity timestamps so post-pause verification has a reference:
```bash
railway logs --service simulation-engine --environment production -n 3
railway logs --service token-keeper --environment production -n 3
railway logs --service automation-runner --environment production -n 3
```

Last activity (captured 2026-05-01 ~21:10 UTC, before the pause checklist was executed):

| Service | Last activity | Notes |
|---|---|---|
| simulation-engine | 06:00 UTC 2026-05-01 | Daily reconciliation output, then silence until the 21:21 SIGTERM |
| token-keeper | 20:38 UTC 2026-05-01 | Jobber refresh (previous refresh 19:53) — on schedule |
| automation-runner | last cron run before pause | Run summary: 0 failures, 23.5 s. `-n 3` only shows the summary tail, so no exact timestamp |

## Pause execution checklist (Railway dashboard)

Pause in this exact order. Postgres stays running.

- [X] **1. sales-outreach** → Settings → Deploy → clear "Cron Schedule" field (currently `*/30 * * * *`). Save.
- [X] **2. automation-runner** → Settings → Deploy → clear "Cron Schedule" field (currently `*/5 * * * *`). Save.
- [X] **3. intelligence-daily** → Settings → Deploy → clear "Cron Schedule" field (currently `0 11 * * 1-5`). Save.
- [X] **4. intelligence-weekly** → Settings → Deploy → clear "Cron Schedule" field (currently `0 13 * * 0`). Save.
- [X] **5. simulation-engine** → Deployments → latest deployment (`a0a4fc19-...`) → "Remove" (this triggers SIGTERM; `SimulationEngine.handle_shutdown()` in [simulation/engine.py](../../../simulation/engine.py) saves `simulation/checkpoint.json` — but that file lives on the container filesystem and is destroyed with the deployment, so the save has no effect on resume; see Resume Step 5).
- [X] **6. token-keeper** → Deployments → latest deployment (`0b1fad75-...`) → "Remove". (Last to stop because it underpins Jobber auth for everything else.)

**Do not** delete services, do not change service environment variables, do not modify custom config file paths. The pause should leave each service definition fully intact so resume is a one-field paste.

- [X] **7. Disconnect GitHub repo from `simulation-engine` and `token-keeper`** (added 2026-05-04 after the auto-redeploy incident — see incident section below). Settings → Source → "Disconnect Repo". This is required because Railway has no toggle to disable "Auto Deploy" while keeping the repo connected. Without this step, any push to `main` will redeploy the workers and revive the simulation. **Required for any future pause (upgraded from "recommended" in the 2026-09-01 review):** also disconnect the 4 cron services for the same reason — a push to `main` triggers a one-shot run on redeploy (which is how the cron services briefly fired on 2026-05-01 21:27/21:29 even though their schedules were cleared). Alternative that avoids re-entering the custom config file on resume: switch each service's Source branch to a frozen branch (e.g. `paused`) instead of disconnecting the repo.

### Corrected order for future pauses

The order above is what was done on 2026-05-01. Two problems with it: (1) clearing `automation-runner` (step 2) three steps before stopping `simulation-engine` (step 5) leaves a window where the engine completes Jobber jobs that nothing invoices, and the engine's own daily reconciliation sweep then posts "N completed jobs have no invoices" to `#automation-failure`; (2) the repo disconnect (step 7) came *after* the push that resurrected the workers. Use this order next time:

1. Disconnect the GitHub source (or switch to a frozen branch) on **all 6** compute services. Push nothing to `main` until this is done.
2. `simulation-engine` → remove the latest deployment (stop producing upstream events first).
3. Let `automation-runner` fire one or two more ticks so completed jobs get invoiced and SQLs get their deals.
4. Clear the cron schedules: `automation-runner`, `intelligence-daily`, `intelligence-weekly`, `sales-outreach`.
5. `token-keeper` → **leaving it running is the better default.** It only refreshes the Jobber token (it ran harmlessly for 54 hours during the incident), and keeping it up means Jobber's rotating refresh token never goes stale, so Resume Step 1 shrinks to a QuickBooks/Google check. Remove its deployment only if the pause must be zero-cost.
6. Commit and push the snapshot file.

## Post-pause verification (~30 minutes after step 6)

> These boxes were never filled in at pause time. The incident section below is the de-facto record: the workers were *not* quiet — Railway auto-redeployed them within ~25 minutes of the push.

- [ ] `railway service list` — `simulation-engine` and `token-keeper` show no active deployment. The 4 cron services keep their last SUCCESS/STOPPED deployment (only the schedule was cleared), so they will not change here; confirm their "Cron Schedule" field is empty in the dashboard instead.
- [ ] `railway logs --service simulation-engine --environment production` — no log lines after pause completion timestamp
- [ ] `railway logs --service token-keeper --environment production` — no log lines after pause completion timestamp (skip if token-keeper was deliberately left running)
- [ ] Slack `#automation-failure` quiet (no alarms from interrupted runs)
- [ ] `railway connect Postgres` still works and a sample SELECT returns data
- [ ] Every compute service's GitHub source is disconnected (step 7) **before** the next item
- [ ] This file committed and pushed to `main` — pushing before step 7 is exactly what caused the 2026-05-01 incident

## Resume runbook

Because the pause is expected to last 1-3 months, **assume both Jobber and QuickBooks OAuth tokens need to be re-issued** before any compute service runs. A stale token will spam `#automation-failure` on the first cron tick.

### Step 1 — Re-OAuth Jobber and QuickBooks first (and Google if the GCP app is in "Testing")

If `token-keeper` was left running during the pause (see "Corrected order for future pauses"), Jobber is already fresh — check with the query below and skip its flow.

The OAuth flows write through [auth/token_store.py](../../../auth/token_store.py), which upserts into `oauth_tokens` on **whatever `DATABASE_URL` points at**. Run them with `DATABASE_URL` set to the Railway Postgres *public* URL (`DATABASE_PUBLIC_URL` on the Postgres service in the Railway dashboard); otherwise the new token lands in local Postgres and Railway keeps the stale one. `JOBBER_TOKEN_KEEPER_ENABLED` must be unset locally.

```bash
export DATABASE_PUBLIC_URL='postgresql://...'   # from the Railway Postgres service

# Jobber — has a __main__ entry point (browser callback on localhost:8019)
DATABASE_URL="$DATABASE_PUBLIC_URL" python -m auth.jobber_auth

# QuickBooks — no __main__; call run_initial_auth() directly (browser callback on localhost:8020)
DATABASE_URL="$DATABASE_PUBLIC_URL" python -c "from auth.quickbooks_auth import run_initial_auth; run_initial_auth()"

# Google — only if the refresh token is dead (GCP app in "Testing" mode => 7-day expiry).
# get_google_credentials() raises on a failed refresh instead of re-consenting, so clear the
# stored token first (DB row, token.json, and any GOOGLE_REFRESH_TOKEN env var), then:
python scripts/railway_db.py clear-token google
rm -f token.json
DATABASE_URL="$DATABASE_PUBLIC_URL" python -c "from auth.google_auth import get_google_credentials; get_google_credentials()"
```

Verify with `python scripts/railway_db.py tokens` (or `railway connect Postgres` and):
```sql
SELECT tool_name, updated_at FROM oauth_tokens WHERE tool_name IN ('jobber','quickbooks','google');
```
`updated_at` for every tool you re-authorised must be within the last hour.

### Step 2 — Reconnect GitHub repo to the disconnected services

Per the 2026-05-04 incident, `simulation-engine` and `token-keeper` had their GitHub source disconnected to prevent auto-redeploy on push. Before they can deploy, reconnect:
- `simulation-engine` → Settings → Source → "Connect Repo" → select `Ovio-tools/sparkle-shine-poc` → branch `main` → custom config file `/railway.worker.toml`
- `token-keeper` → same as above

Also reconnect any cron services you disconnected during the pause. Their custom config file is `/railway.toml`. (If you used the frozen-branch approach instead, switch each service's Source branch back to `main`.)

### Step 3 — Resume token-keeper first

Trigger a redeploy from the latest commit on `main` (Railway will auto-deploy once the repo is connected, but verify it did). Watch logs:
```bash
railway logs --service token-keeper --environment production -f
```
You should see one full Jobber refresh tick within the first few minutes. If it 401s, the token push from Step 1 didn't take. Fix and re-verify before continuing.

### Step 4 — Restore the `automation-runner` cron BEFORE the simulation engine

`automation-runner` → Settings → Deploy → "Cron Schedule" → `*/5 * * * *`. Wait for the first tick and confirm the run summary shows 0 failures and no Jobber/QBO 401s.

Why before the engine: the runner is what turns completed Jobber jobs into QBO invoices. If the engine comes up first, every completed job it generates sits un-invoiced until the runner is restored, and the engine's daily reconciliation sweep (`SimulationEngine.run()` → `Reconciler.run_daily_sweep()`) posts "N completed jobs have no invoices" to `#automation-failure` — the same alert pattern seen during the incident. Don't manufacture it during a clean resume.

### Step 5 — Resume simulation-engine

Trigger a redeploy (or wait for the auto-deploy from Step 2 to complete).

**What to expect in the logs — there is no checkpoint to resume from.** The engine's checkpoint is `simulation/checkpoint.json` (`CHECKPOINT_FILE` / `save_checkpoint()` / `load_checkpoint()` in [simulation/engine.py](../../../simulation/engine.py)), written to the container's own filesystem. It is gitignored and not on a Railway volume, so pause step 5 (removing the deployment) destroyed it. On start-up `load_checkpoint()` finds no file and the engine begins a fresh day at `date.today()` — you will **not** see a "Resumed from checkpoint" line, and the period between pause and resume is not backfilled. That gap is intentional (see the resume plan's "Out of scope"). The `poll_state` table is the automation runner's polling watermark and has nothing to do with engine state.

Watch for the first daily plan to build and at least one generator to complete without errors.

### Step 6 — Restore the remaining cron schedules

Restore in this order (least-to-most external-facing):
1. intelligence-daily → set cron to `0 11 * * 1-5`
2. intelligence-weekly → set cron to `0 13 * * 0`
3. sales-outreach → set cron to `*/30 * * * *` (last — this resumes outbound activity; `*/30`, not `*/5`, per the cron cadence history in [docs/railway.md](../../railway.md))

### Step 7 — Verify

- [ ] First automation-runner tick (Step 4) completed without 401s or QBO errors
- [ ] Next morning's daily briefing posts to Slack `#daily-briefing` (channel ID `C0AML3Q8PSM` per pause-time tool_ids.json)
- [ ] `cross_tool_mapping` audit (Section A above) returns the same numbers as at pause-time, plus any normal growth

### Step 8 — Archive this file

Once Step 7 passes for two consecutive days:
```bash
mkdir -p docs/operations/archive
git mv docs/operations/2026-05-01-railway-pause-state.md docs/operations/archive/
git commit -m "ops: archive 2026-05-01 Railway pause runbook after successful resume"
```

## Resume contingency: a tool account is lost

If during the pause one or more SaaS tool accounts becomes inaccessible and a new account has to be created, follow this path **for that tool only** (other tools resume normally per the runbook above):

1. Create the new tool account; complete OAuth/API-key setup. Update `.env` and Railway env vars with new credentials.
2. Run the tool setup so the new account has the right pipelines/fields/projects/audiences/channels:
   ```bash
   python -m setup.configure_tools
   ```
   There is **no per-tool flag** — `main()` in [setup/configure_tools.py](../../../setup/configure_tools.py) ignores arguments and runs all six configurators (Pipedrive, HubSpot, Asana, Mailchimp, QuickBooks, Slack). Each is find-or-create, so tools that still exist are reported as "already existed", but the script rewrites the whole `config/tool_ids.json` — `git diff` it before committing. It does **not** cover Jobber or Google Workspace (see the tool-specific notes below).
3. **Wipe stale `cross_tool_mapping` rows for that tool only** (the old IDs are dead and would cause duplicate-detection logic to skip records):
   ```sql
   DELETE FROM cross_tool_mapping WHERE tool_name = '<tool_name>';
   ```
4. **For Google Workspace specifically**, also run:
   ```bash
   python -m setup.populate_workspace
   ```
   to regenerate the 8 Docs / 5 Sheets / 15 Calendar events on the new account. Drive and Calendar IDs will differ.
5. Run the corresponding pusher with `--dry-run` first to confirm the record count, then live (pushers exist for `asana`, `hubspot`, `jobber`, `mailchimp`, `pipedrive`, `quickbooks`):
   ```bash
   python seeding/pushers/push_<tool_name>.py --dry-run
   python seeding/pushers/push_<tool_name>.py
   ```
6. Verify `cross_tool_mapping` row counts for that tool match the canonical record counts.
7. Continue with the standard resume runbook above.

### Tool-specific re-seed notes

- **Slack** — `configure_slack()` (run by `python -m setup.configure_tools`) finds-or-creates the 5 channels (`#daily-briefing`, `#operations`, `#sales`, `#new-clients`, `#reviews-and-feedback`); invite the bot to each. `#automation-failure` is deliberately absent from `tool_ids.json` — [simulation/error_reporter.py](../../../simulation/error_reporter.py) creates it on the first alert, so do not hand-create it. Slack uses a static bot token (`SLACK_BOT_TOKEN`), not `oauth_tokens`. There is no data pusher; briefing history can be replayed by re-posting from [briefings/](../../../briefings/) if desired.
- **Google Workspace** — not covered by `configure_tools`; no pusher. Re-consent per Resume Step 1 (clear the stored token, then call `get_google_credentials()`), then `python -m setup.populate_workspace` regenerates content via LLM. Drive file IDs and Calendar event IDs will all be new — no system code currently hardcodes them outside `config/tool_ids.json`.
- **Jobber** — not covered by `configure_tools` (it has no Jobber step). Re-OAuth with `python -m auth.jobber_auth`, wipe the `jobber` mapping rows, then re-seed with [seeding/pushers/push_jobber.py](../../../seeding/pushers/push_jobber.py) (clients → recurring agreements → jobs, ~8,500 records). Also regenerate `jobber.user_pool` in `config/tool_ids.json` from the new account's user roster with [scripts/setup_jobber_user_mapping.py](../../../scripts/setup_jobber_user_mapping.py) before starting the simulation engine.
- **QuickBooks** — re-OAuth per Resume Step 1; `configure_quickbooks()` regenerates items/accounts in `tool_ids.json`; then re-seed with [seeding/pushers/push_quickbooks.py](../../../seeding/pushers/push_quickbooks.py) (customers → invoices → payments, ~15,000 records).
- **HubSpot, Pipedrive, Asana, Mailchimp** — pushers exist; the re-seed is mechanical once `tool_ids.json` is regenerated.

## Incident: 2026-05-01 to 2026-05-04 worker auto-redeploy

**Summary:** The original pause plan didn't account for Railway's GitHub auto-deploy hook. Pushing this snapshot file to `main` resurrected `simulation-engine` and `token-keeper` minutes after the pause checklist was completed. They ran undetected for ~54 hours, generating data and `#automation-failure` alerts.

### Timeline (UTC)

| Time | Event |
|---|---|
| 2026-05-01 21:21:03 | `simulation-engine` SIGTERM — graceful "Engine stopped." |
| 2026-05-01 21:21:49 | `token-keeper` SIGTERM — "Stopping Container" |
| 2026-05-01 21:24-21:29 | All 4 cron services completed their final scheduled or in-flight runs (intelligence-daily/weekly each posted one final briefing to Slack) |
| 2026-05-01 ~21:35 | Snapshot file (`docs/operations/2026-05-01-railway-pause-state.md`) committed and pushed to `origin/main` (commit `50fdb5c`) |
| 2026-05-01 21:37:37 | **`token-keeper` auto-redeployed** by Railway in response to the GitHub push |
| 2026-05-01 21:46:36 | **`simulation-engine` auto-redeployed** |
| 2026-05-01 → 2026-05-04 | Both workers ran continuously. `simulation-engine` generated jobs, contacts, payments etc. for 54 hours. `automation-runner` was correctly paused so the Jobber→QuickBooks invoice-creation pipeline didn't run. The simulation's own daily reconciliation sweep (`SimulationEngine.run()` in [simulation/engine.py](../../../simulation/engine.py)) detected this gap and posted "13 completed jobs have no invoices" alerts to `#automation-failure` daily. |
| 2026-05-04 ~03:35 | User noticed the alerts and re-stopped both workers; **disconnected the GitHub repo** from `simulation-engine` and `token-keeper` to prevent another auto-redeploy |

### Root cause

Railway services connected to GitHub auto-deploy on every push to the watched branch (`main` here). The pause plan removed the deployments but left the GitHub source connection intact. Railway has no UI toggle to disable auto-deploy while keeping the repo connected, so the only way to stop it is to disconnect the source entirely.

This affected the workers visibly because they're long-running. It also affected the **cron services** invisibly: each cron service got a new deployment ID after the push, and Railway's first-deploy behavior runs the service's start command once on deploy. That's why `intelligence-daily` and `intelligence-weekly` each fired once at 21:27 and 21:29 even though their schedules were cleared, posting one final daily and weekly briefing to Slack. They didn't continue firing because the cron schedule (cleared) is the gate for subsequent firings.

### Data impact

`simulation-engine` ran for 54 hours generating new SS-* records (contacts, jobs, payments, tasks). The simulation is designed to be continuous and reproducible (random.seed(42) per CLAUDE.md), so these records are valid simulated data — just produced during a window the user thought was paused. No corruption, no manual cleanup required. Resume will pick up from the latest checkpoint, not 2026-05-01.

The `#automation-failure` alerts were correct given the state — the simulation was running and the automation-runner wasn't, so completed jobs genuinely lacked invoices. They were not false positives.

### Fix applied

User disconnected the GitHub repo from both `simulation-engine` and `token-keeper` on 2026-05-04. Cron services were NOT disconnected as of this writing — they remain at risk of one-shot firings if `main` is pushed during the pause. Recommendation in pause checklist Step 7: disconnect them too as a precaution.

### Resume implications

The Resume runbook above already accounts for this — see Step 2 ("Reconnect GitHub repo"). Without that step, redeploying via Railway dashboard will fail because there's no source to deploy from.

### Lesson for future pauses

Railway's pause story has two layers: deployment state (controlled by removing deployments) **and** auto-deploy triggers (controlled by source/GitHub connection). Pausing without addressing both layers leaves a back door. A future pause plan should disconnect the repo from every compute service before pushing any commits, not just the always-on workers.

Additional lessons from the 2026-09-01 review of this runbook:

- **Disconnect (or branch-freeze) every compute service first, then push.** Not just the workers — see "Corrected order for future pauses" above.
- **Stop the engine before the runner, and restore the runner before the engine.** Otherwise the engine's reconciliation sweep manufactures "completed jobs have no invoices" alerts during a perfectly healthy pause or resume.
- **Consider leaving `token-keeper` running.** It is the cheapest service, it only refreshes the Jobber token, and it removes the Jobber re-OAuth step (the riskiest part of resume) for any pause shorter than QuickBooks' 100-day refresh-token lifetime.
- **The engine checkpoint is not durable on Railway.** `simulation/checkpoint.json` lives in the container; removing a deployment discards it and the engine restarts fresh at today's date. Don't plan around "resume from checkpoint" unless a volume is mounted or the checkpoint moves into Postgres.
- **Local OAuth flows only reach Railway if `DATABASE_URL` points there.** `auth/token_store.py` upserts into whatever database `DATABASE_URL` names; use the Postgres service's `DATABASE_PUBLIC_URL` when re-authorising for production.
- **Not yet implemented — a kill switch would make all of this a variable flip.** None of `simulation.engine`, `automations.runner`, `intelligence.runner`, or `automation_07_sales_outreach` checks an env var before running. A `SIMULATION_PAUSED=1` / `AUTOMATIONS_PAUSED=1` guard at those entry points would pause the project without removing deployments or disconnecting repos, closing the auto-deploy back door entirely.

## Out-of-scope during pause

- No code changes to entry points, config files, or schema
- Postgres database is **NOT** paused (preserves all canonical data)
- Briefings folder remains in git (historical archive)
- Daily/weekly insight history (`weekly_reports/insight_history.json` and DB equivalents) preserved

## Resume outcome (verified 2026-09-01)

Live state captured with the Railway CLI linked to `sparkle-shine-poc / production`. **Every service ID — including Postgres — differs from the inventory table above, and all services now report region EU West:** the project was evidently rebuilt after the pause with the data carried over (8,725 invoices, 7,353 jobs, pre-pause history intact). Treat the inventory table as historical only.

| Service | Status 2026-09-01 | Cron (UTC) | Repo connected | Service ID (current) |
|---|---|---|---|---|
| simulation-engine | Online — daily summary 2026-09-01: 117 events, 0 errors | — | yes | `efa349ab-cb79-4b1e-b3b4-368d75dca140` |
| token-keeper | Online — Jobber refresh HTTP 200 every ~45 min | — | yes | `94b30b64-6f6d-4da8-8426-6329c35e7aa1` |
| automation-runner | Completed — 0 failures, ~10 s per tick | `*/5 * * * *` | yes | `87cea803-1b78-4207-a551-f84b985a08ab` |
| intelligence-daily | **Crashed** 2026-09-01 11:11 UTC (see below) | `0 11 * * 1-5` | yes | `68b60281-30bd-4792-b87d-c273ec3e9760` |
| intelligence-weekly | Completed — 2026-08-30 report posted to `#weekly-briefing` | `0 13 * * 0` | yes | `4ab27615-e7ff-4b4b-aeef-15dfd9641d65` |
| sales-outreach | Completed — 13:00 run, 0 contacts to process | `*/30 * * * *` | yes | `39d08b95-e409-4af6-a518-115ddcc4a606` |
| Postgres | Online — `postgres-volume` 201 MB / 500 MB | — | — | `253b39fd-db52-479f-9baa-a5a2b30a9f3e` |

**`oauth_tokens` freshness (13:12 UTC):** google `11:01` (refreshed by the intelligence-daily preflight), jobber `12:59` (token-keeper), quickbooks `13:10` (automation-runner). All three well inside their windows — the QuickBooks 100-day watch date (~2026-08-09) was moot because the token has been refreshing continuously since the late-May resume.

**Section A audit** — `railway ssh` was unavailable (no SSH key registered on the workstation), so `find_unmapped` was run locally with `DATABASE_URL` set to the Postgres service's `DATABASE_PUBLIC_URL` (read-only). Notable rows, pause → 2026-09-01:

| Entity.tool | 2026-05-01 | 2026-09-01 | Reading |
|---|---|---|---|
| `JOB.jobber` | 2 | 2 | Same two orphans as at pause; still open |
| `INV.quickbooks` | 2 | 2 | Same two orphans; still open |
| `TASK.asana` | 130 | 130 | Same 130 unmapped tasks; ~3,900 new tasks since are all mapped |
| `JOB.*` (non-Jobber cols) | 5,766 | 7,353 | +1,587 jobs — normal growth |
| `INV.*` (non-QBO cols) | 7,139 | 8,725 | +1,586 invoices — normal growth |
| `INV.jobber` | 7,139 | 7,482 | Jobber invoice writeback (2026-05-30) is registering mappings for new invoices |
| `CLIENT.jobber` / `.quickbooks` / `.mailchimp` | 158 / 159 / 161 | 156 / 157 / 159 | *Unmapped* count fell → two clients gained mappings (healing, not loss) |
| `CLIENT.hubspot`, `LEAD.hubspot` | 8, 7 | 106, 405 | Expected: the HubSpot contact pruner (2026-06-19) deletes `hubspot` mapping rows to stay under the free-tier cap |
| `PAY.*` | 4,396 | 4,396 | **Unchanged — see data-integrity note** |

Note on the heuristic in the resume plan: the audit counts *unmapped* records, so a falling number is a mapping being added, not data loss. Growth shows up in the columns for tools that never map that entity type.

### intelligence-daily crash — Anthropic credits exhausted

The 2026-09-01 daily run completed sync (8/8 tools, watermarks advanced) and metrics, then died in Stage 4 with `anthropic.BadRequestError 400 — "Your credit balance is too low to access the Anthropic API"`. The 2026-08-31 run had succeeded. This is billing, not code, and not pause-related. Blast radius: `intelligence-daily` (every weekday), `intelligence-weekly` (next fire Sunday 2026-09-06), and the `sales-outreach` agents (`automations/agents/*` — only when it has contacts to process). Not affected: `simulation-engine` (its only Anthropic use is an offline build step in `simulation/generators/contacts.py`), `automation-runner`, `token-keeper`. Fix: top up credits; no redeploy needed.

### Data-integrity observation (pre-dates the pause; surfaced by this verification)

`SELECT COUNT(*), MAX(payment_date) FROM payments` → **4,396 rows, last payment 2026-04-15, zero rows since** — while `simulation-engine` logs ~20 `payments` events per day and the invoice count grew by 1,586 (8,725 total; 4,385 paid, 120 written off). The generator's insert path (`INSERT INTO payments` in `simulation/generators/payments.py`) has not fired in 4½ months, and `automation-runner` reports 0 new QuickBooks payments every tick. Consistent with the engine's daily `bad_debt` warnings re-processing the same invoice, and with the two chronic critical alerts the intelligence layer posts daily ("11 completed jobs without an invoice for 24h+", "918 unaddressed invoices totalling $137,700 not linked to any job"). Recommend a separate investigation; nothing here was changed.

### Archive decision

Resume Step 7's criteria have held continuously for ~3 months (daily briefings posted through 2026-08-31, runner at 0 failures, all tokens fresh). Archived 2026-09-01.
