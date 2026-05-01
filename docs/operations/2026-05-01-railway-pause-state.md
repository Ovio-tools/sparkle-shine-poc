# Railway Pause State — 2026-05-01

This file is the runbook for **resuming** the Sparkle & Shine POC after a temporary pause that began on 2026-05-01. Do not delete it during the pause. Move it to `docs/operations/archive/` only after the project is fully resumed and verified.

## Pause metadata

| Field | Value |
|-------|-------|
| Pause start | 2026-05-01 |
| Expected duration | 1-3 months |
| Reason | Project temporarily paused (owner request) |
| Approach | Railway-native pause (no code changes) |
| Plan reference | `~/.claude/plans/i-would-like-to-gentle-hammock.md` |

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

Captured via `railway service status --all` with project linked to `sparkle-shine-poc / production`.

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

Full file is at [config/tool_ids.json](../../config/tool_ids.json) — abbreviated here for readability; full IDs (Asana sections, all Google Drive file IDs, all Calendar event IDs, all QuickBooks items/accounts) are in the JSON.

## Pre-pause audits (to run before clearing schedules)

These were not run from this session because production database access requires explicit per-command approval. Run them yourself, paste results back into this file under each heading.

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

> Note: `find_unmapped(entity_type, tool_name)` — the canonical-record skill doc has the args reversed. Use the order above. The `_ENTITY_META` dict only contains CLIENT, LEAD, JOB, TASK; INVOICE / PAYMENT / PROPOSAL are not in the entity helper but their canonical IDs do live in `cross_tool_mapping` and can be queried directly with SQL if you need them.

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
- `CLIENT` not mapped to Pipedrive (459) is the **deliberate lead-leak detector** per [CLAUDE.md](../../CLAUDE.md): "NEVER register a Pipedrive mapping for SQL contacts. Only register the HubSpot mapping. The missing Pipedrive mapping is what triggers the automation runner's lead-leak detection." Expected.

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

Last activity timestamps:
```
simulation-engine — last log entry at 06:00 UTC (~15h ago). Just reconciliation output, then silence. Matches the "pause state" doc you have open: this service appears stopped.

token-keeper — healthy and active. Last Jobber refresh at 20:38 UTC (the 34m age we saw earlier). Earlier 19:53 refresh visible too — it's running on schedule.

automation-runner — only the tail of a cron-run summary banner is visible (0 failures, 23.5s duration). The -n 3 window caught the very end of one run. No timestamp on these lines so I can't tell when the run ended, but a clean exit with 0 failures is a healthy signal.
TBD
```

## Pause execution checklist (Railway dashboard)

Pause in this exact order. Postgres stays running.

- [ ] **1. sales-outreach** → Settings → Deploy → clear "Cron Schedule" field (currently `*/30 * * * *`). Save.
- [ ] **2. automation-runner** → Settings → Deploy → clear "Cron Schedule" field (currently `*/5 * * * *`). Save.
- [ ] **3. intelligence-daily** → Settings → Deploy → clear "Cron Schedule" field (currently `0 11 * * 1-5`). Save.
- [ ] **4. intelligence-weekly** → Settings → Deploy → clear "Cron Schedule" field (currently `0 13 * * 0`). Save.
- [ ] **5. simulation-engine** → Deployments → latest deployment (`a0a4fc19-...`) → "Remove" (this triggers SIGTERM; the engine handles checkpoint save in `simulation/engine.py:100-101`).
- [ ] **6. token-keeper** → Deployments → latest deployment (`0b1fad75-...`) → "Remove". (Last to stop because it underpins Jobber auth for everything else.)

**Do not** delete services, do not change service environment variables, do not modify custom config file paths. The pause should leave each service definition fully intact so resume is a one-field paste.

## Post-pause verification (~30 minutes after step 6)

- [ ] `railway service status --all` — the 6 services no longer show SUCCESS deployments
- [ ] `railway logs --service simulation-engine --environment production` — no log lines after pause completion timestamp
- [ ] `railway logs --service token-keeper --environment production` — no log lines after pause completion timestamp
- [ ] Slack `#automation-failure` quiet (no alarms from interrupted runs)
- [ ] `railway connect Postgres` still works and a sample SELECT returns data
- [ ] This file committed and pushed to `main` (or current branch)

## Resume runbook

Because the pause is expected to last 1-3 months, **assume both Jobber and QuickBooks OAuth tokens need to be re-issued** before any compute service runs. A stale token will spam `#automation-failure` on the first cron tick.

### Step 1 — Re-OAuth Jobber and QuickBooks first

Run the OAuth flows locally (using existing `auth/jobber_auth.py` and `auth/quickbooks_auth.py` flows), then push the refreshed tokens to Railway via the `oauth_tokens` table. Verify with:
```bash
railway connect Postgres
SELECT tool_name, updated_at FROM oauth_tokens WHERE tool_name IN ('jobber','quickbooks');
```
Both `updated_at` values must be within the last hour.

### Step 2 — Resume token-keeper first

Trigger a redeploy from the latest commit on `main` (or the long-lived branch). Watch logs:
```bash
railway logs --service token-keeper --environment production -f
```
You should see one full Jobber refresh tick within the first few minutes. If it 401s, the token push from Step 1 didn't take. Fix and re-verify before continuing.

### Step 3 — Resume simulation-engine

Trigger a redeploy. Confirm in logs that the engine resumes from its last checkpoint rather than starting from scratch. The checkpoint is preserved in Postgres (per `simulation/checkpoint.py` and the `poll_state` discipline).

### Step 4 — Restore cron schedules

Restore in this order (least-to-most external-facing):
1. automation-runner → set cron to `*/5 * * * *`
2. intelligence-daily → set cron to `0 11 * * 1-5`
3. intelligence-weekly → set cron to `0 13 * * 0`
4. sales-outreach → set cron to `*/30 * * * *` (last — this resumes outbound activity)

### Step 5 — Verify

- [ ] First automation-runner tick (within 5 minutes of restoring its cron) completes without 401s or QBO errors
- [ ] Next morning's daily briefing posts to Slack `#daily-briefing` (channel ID `C0AML3Q8PSM` per pause-time tool_ids.json)
- [ ] `cross_tool_mapping` audit (Section A above) returns the same numbers as at pause-time, plus any normal growth

### Step 6 — Archive this file

Once Step 5 passes for two consecutive days:
```bash
git mv docs/operations/2026-05-01-railway-pause-state.md docs/operations/archive/
git commit -m "ops: archive 2026-05-01 Railway pause runbook after successful resume"
```

## Resume contingency: a tool account is lost

If during the pause one or more SaaS tool accounts becomes inaccessible and a new account has to be created, follow this path **for that tool only** (other tools resume normally per the runbook above):

1. Create the new tool account; complete OAuth/API-key setup. Update `.env` and Railway env vars with new credentials.
2. Run the per-tool setup so the new account has the right pipelines/fields/projects/audiences/channels:
   ```bash
   python -m setup.configure_tools --tool <tool_name>
   ```
   This regenerates the relevant section of `config/tool_ids.json`.
3. **Wipe stale `cross_tool_mapping` rows for that tool only** (the old IDs are dead and would cause duplicate-detection logic to skip records):
   ```sql
   DELETE FROM cross_tool_mapping WHERE tool_name = '<tool_name>';
   ```
4. **For Google Workspace specifically**, also run:
   ```bash
   python -m setup.populate_workspace
   ```
   to regenerate the 8 Docs / 5 Sheets / 15 Calendar events on the new account. Drive and Calendar IDs will differ.
5. Run the corresponding pusher with `--dry-run` first to confirm the record count, then live:
   ```bash
   python -m seeding.pushers.push_<tool_name> --dry-run
   python -m seeding.pushers.push_<tool_name>
   ```
6. Verify `cross_tool_mapping` row counts for that tool match the canonical record counts.
7. Continue with the standard resume runbook above.

### Tool-specific re-seed notes

- **Slack** — no pusher exists. Manually recreate the 5 channels (`#daily-briefing`, `#operations`, `#sales`, `#new-clients`, `#reviews-and-feedback`), invite the bot, update its OAuth in `oauth_tokens`. Briefing history can be replayed by re-posting from [briefings/](../../briefings/) if desired.
- **Google Workspace** — no pusher. `setup/populate_workspace.py` regenerates content via LLM. Drive file IDs and Calendar event IDs will all be new — no system code currently hardcodes them outside `config/tool_ids.json`.
- **Jobber and QuickBooks** — OAuth-based. Full reconnect flow + `setup/configure_tools.py` is sufficient.
- **HubSpot, Pipedrive, Asana, Mailchimp** — pushers exist; the re-seed is mechanical once `tool_ids.json` is regenerated.

## Out-of-scope during pause

- No code changes to entry points, config files, or schema
- Postgres database is **NOT** paused (preserves all canonical data)
- Briefings folder remains in git (historical archive)
- Daily/weekly insight history (`weekly_reports/insight_history.json` and DB equivalents) preserved
