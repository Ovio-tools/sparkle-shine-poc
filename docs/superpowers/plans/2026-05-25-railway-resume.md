# Railway Resume Implementation Plan — 2026-05-25

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring the Sparkle & Shine POC out of the 2026-05-01 Railway pause and back to full production, with a staged rollout that limits blast radius if a token has expired or a service comes up dirty.

**Architecture:** Operational restart, not code change. Follows the resume runbook in [docs/operations/2026-05-01-railway-pause-state.md](../../operations/archive/2026-05-01-railway-pause-state.md), adapted for: (a) 24-day pause (Jobber near, but under, the ~30-day re-OAuth threshold), (b) unknown Google OAuth publication status, (c) unknown GitHub-disconnect state for cron services, (d) staged 24–48h resume cadence (workers + automation-runner today; intelligence + sales-outreach after a clean soak).

> **Status (2026-09-01):** This plan was evidently executed around 2026-05-25/26 (see git history), but "Phase 0 findings" was never filled in and Phase 8 (archive the pause runbook) was never done. Factual corrections applied 2026-09-01: checkpoint expectation and `#automation-failure` channel ID in Task 3.2, OAuth entry points and `DATABASE_URL` target in Phase 1, deprecated CLI command. For any future resume, the pause runbook's corrected order (restore `automation-runner` **before** redeploying `simulation-engine`) supersedes the Phase 3 → Phase 4 order here.

**Tech Stack:** Railway CLI (`railway`), Railway dashboard (manual UI steps for cron schedules and GitHub source), Postgres via `railway connect Postgres`, local OAuth flows in `auth/jobber_auth.py` / `auth/quickbooks_auth.py` / `auth/google_auth.py`, Slack monitoring (`#automation-failure`, `#daily-briefing`).

**Pre-flight assumptions to verify in Phase 0:**
- 24 days have elapsed since pause start (`2026-05-01` → `2026-05-25`).
- QBO refresh token (100-day lifetime) should still be valid (~76 days remaining).
- Jobber rotating refresh token is within the ~30-day "should still work" window but close enough that we must attempt refresh before assuming success.
- Google refresh token: definitely expired if GCP app is in "Testing" mode (7-day cap); valid otherwise.
- HubSpot / Pipedrive / Asana / Mailchimp / Slack static tokens never expire.

---

## File Structure

This plan is operational; no source files are created or modified. Artifacts produced:

- **Modify (later, in Phase 8):** [docs/operations/2026-05-01-railway-pause-state.md](../../operations/archive/2026-05-01-railway-pause-state.md) — moved to `docs/operations/archive/` once resume is verified for 2 consecutive days.
- **Read-only references:**
  - [docs/operations/2026-05-01-railway-pause-state.md](../../operations/archive/2026-05-01-railway-pause-state.md) — the canonical runbook this plan adapts
  - [CLAUDE.md](../../../CLAUDE.md) — Railway services table, troubleshooting source-of-truth rules
  - [auth/jobber_auth.py](../../../auth/jobber_auth.py), [auth/quickbooks_auth.py](../../../auth/quickbooks_auth.py), [auth/google_auth.py](../../../auth/google_auth.py) — OAuth flows
  - [config/tool_ids.json](../../../config/tool_ids.json) — Slack channel IDs for monitoring
  - [simulation/engine.py](../../../simulation/engine.py) — checkpoint behaviour: `simulation/checkpoint.json` is container-local and does not survive a removed deployment, so there is nothing to resume from (see pause doc Resume Step 5)

**Why no code:** Per the [Simulation Data Integrity Rule (L5)](../../skills/project-conventions.md), the simulation engine starts a fresh day from `date.today()` on redeploy (its file checkpoint does not survive a removed deployment) and there is no regeneration step. Stale simulated data from the 2026-05-01 to 2026-05-04 auto-redeploy incident is documented and intentionally left in place.

---

## Phase 0 — Verify current state (no changes)

**Why first:** The pause-state doc was authored 2026-05-01; today is 2026-05-25. Three things may have drifted: (a) Railway service status (manual changes since pause), (b) `oauth_tokens` freshness, (c) which services still have a GitHub source connected. Per [CLAUDE.md troubleshooting source-of-truth rules](../../../CLAUDE.md), Railway is the canonical answer for runtime state — verify before acting.

### Task 0.1: Confirm Railway link and inventory services

**Files:** none (read-only Railway commands).

- [ ] **Step 1: Confirm Railway CLI is linked to the right project**

```bash
railway status
```

Expected: project `sparkle-shine-poc`, environment `production`. If not linked, run `railway link` and select `sparkle-shine-poc / production`.

- [ ] **Step 2: Capture service inventory**

```bash
railway service list   # `railway service status --all` is deprecated
```

Expected: 6 services + Postgres. Compare against the table in [docs/operations/2026-05-01-railway-pause-state.md#service-inventory-as-captured-2026-05-01](../../operations/archive/2026-05-01-railway-pause-state.md). Note any service whose status has drifted from the pause snapshot.

- [ ] **Step 3: Tail the most recent logs on each compute service**

```bash
railway logs --service simulation-engine --environment production -n 5
railway logs --service token-keeper --environment production -n 5
railway logs --service automation-runner --environment production -n 5
railway logs --service intelligence-daily --environment production -n 5
railway logs --service intelligence-weekly --environment production -n 5
railway logs --service sales-outreach --environment production -n 5
```

Expected: silence after `2026-05-04` for `simulation-engine` and `token-keeper`; cron services silent since pause (cron schedule cleared, no firings).

If any service has log entries dated after 2026-05-04, **stop**. Something restarted unnoticed — investigate before continuing (likely another auto-deploy from a `main` push or a manual redeploy).

### Task 0.2: Check `oauth_tokens` freshness in production Postgres

**Files:** none (read-only SQL).

- [ ] **Step 1: Connect to production Postgres**

```bash
railway connect Postgres
```

- [ ] **Step 2: Inspect token ages**

```sql
SELECT tool_name, updated_at, NOW() - updated_at AS age_since_refresh
FROM oauth_tokens
ORDER BY tool_name;
```

Expected: three rows (`google`, `jobber`, `quickbooks`) with `age_since_refresh` near 24 days. Record actual `updated_at` for each — they become the inputs to Phase 1.

- [ ] **Step 3: Exit psql**

```sql
\q
```

### Task 0.3: Determine GitHub source state per service

**Files:** none (Railway dashboard inspection — CLI does not expose this reliably).

- [ ] **Step 1: Open Railway dashboard → `sparkle-shine-poc` → `production`**

For each of the 6 compute services, click into Settings → Source and record whether a GitHub repo is connected.

Expected per the 2026-05-04 incident fix: `simulation-engine` and `token-keeper` are disconnected. The 4 cron services may or may not be disconnected — runbook recommended disconnecting them as a precaution but the user is unsure whether that was done.

- [ ] **Step 2: Record findings in this plan**

Append the actual state to the bottom of this file under a "## Phase 0 findings" heading (e.g., "simulation-engine: disconnected; token-keeper: disconnected; automation-runner: connected; …"). Phases 2 and 3 read this to decide which services need reconnection.

### Task 0.4: Determine Google OAuth app publication status

**Files:** none (GCP console).

- [ ] **Step 1: Open Google Cloud Console → APIs & Services → OAuth consent screen**

The project is the one whose client credentials back `auth/google_auth.py`. Look at "Publishing status".

- [ ] **Step 2: Record in Phase 0 findings**

If "Testing": Google refresh token has expired (24 days > 7-day cap). Plan Phase 1.3 must run a full re-OAuth flow.
If "In production": Google refresh token should still work. Plan Phase 1.3 only verifies via a token refresh.

---

## Phase 1 — Restore OAuth tokens (local + push to Railway)

**Why before reconnecting GitHub or redeploying:** Per runbook Step 1, a stale OAuth token will spam `#automation-failure` the moment a compute service starts. Refresh and verify locally, push the new tokens into `oauth_tokens`, then redeploy.

### Task 1.1: Refresh QuickBooks token

**Files:**
- Read: [auth/quickbooks_auth.py](../../../auth/quickbooks_auth.py)

- [ ] **Step 1: Test current QBO token from local**

```bash
python3 -c "
from auth import get_client
s = get_client('quickbooks')
import os
company_id = os.environ.get('QBO_COMPANY_ID') or os.environ.get('QUICKBOOKS_COMPANY_ID')
url = f'https://sandbox-quickbooks.api.intuit.com/v3/company/{company_id}/companyinfo/{company_id}'
r = s.get(url, timeout=20)
print(r.status_code, r.text[:200])
"
```

Expected: `200` and a JSON snippet with the company name. If `401`, the access token expired but the refresh token (100-day) should auto-refresh transparently inside `get_client("quickbooks")`. A second 401 means the refresh token itself died and a full re-OAuth is needed.

- [ ] **Step 2: If a full re-OAuth is required, run the local QBO flow**

```bash
DATABASE_URL="$DATABASE_PUBLIC_URL" python -c "from auth.quickbooks_auth import run_initial_auth; run_initial_auth()"
```

`auth/quickbooks_auth.py` has no `__main__` — call `run_initial_auth()` directly; the browser callback lands on `localhost:8020`. The token store dual-writes to `.quickbooks_tokens.json` and to `oauth_tokens` on **whatever `DATABASE_URL` points at** — set it to the Railway Postgres public URL (`DATABASE_PUBLIC_URL` on the Postgres service) or the token never reaches production.

- [ ] **Step 3: Verify token landed in production Postgres**

```bash
railway connect Postgres
```

```sql
SELECT tool_name, updated_at FROM oauth_tokens WHERE tool_name = 'quickbooks';
```

Expected: `updated_at` within the last few minutes.

- [ ] **Step 4: Exit psql, commit nothing** — `.quickbooks_tokens.json` is gitignored.

### Task 1.2: Refresh Jobber token

**Files:**
- Read: [auth/jobber_auth.py](../../../auth/jobber_auth.py)

- [ ] **Step 1: Test current Jobber token from local**

```bash
python3 -c "
from auth import get_client
s = get_client('jobber')
q = {'query': '{ account { name } }'}
r = s.post('https://api.getjobber.com/api/graphql', json=q, timeout=20)
print(r.status_code, r.text[:200])
"
```

Expected: `200` with `{"data": {"account": {"name": "..."}}}`. If `401`, the refresh token rotated past validity (24 days is under the 30-day soft threshold but rotating refresh tokens are unforgiving — full re-OAuth needed).

- [ ] **Step 2: If 401, run the local Jobber OAuth flow**

```bash
DATABASE_URL="$DATABASE_PUBLIC_URL" python -m auth.jobber_auth
```

Follow the browser flow (callback on `localhost:8019`); the flow writes new tokens to `.jobber_tokens.json` and to `oauth_tokens` on whatever `DATABASE_URL` points at — use the Railway public URL. `JOBBER_TOKEN_KEEPER_ENABLED` must be unset locally.

- [ ] **Step 3: Verify in production Postgres**

```bash
railway connect Postgres
```

```sql
SELECT tool_name, updated_at FROM oauth_tokens WHERE tool_name = 'jobber';
```

Expected: `updated_at` within the last few minutes.

### Task 1.3: Handle Google OAuth

**Files:**
- Read: [auth/google_auth.py](../../../auth/google_auth.py)

- [ ] **Step 1: Branch on Phase 0.4 finding**

If "In production": go to Step 2.
If "Testing": skip to Step 3 (refresh token is dead — full re-auth required).

- [ ] **Step 2: Test current Google token from local**

```bash
python3 -c "
from auth import get_client
creds = get_client('google')
from googleapiclient.discovery import build
drive = build('drive', 'v3', credentials=creds)
about = drive.about().get(fields='user(emailAddress)').execute()
print(about)
"
```

Expected: a dict containing the OAuth account email. If it raises a `RefreshError`, fall through to Step 3.

- [ ] **Step 3: Run the local Google OAuth flow**

```bash
python scripts/railway_db.py clear-token google   # needs DATABASE_PUBLIC_URL exported
rm -f token.json
DATABASE_URL="$DATABASE_PUBLIC_URL" python -c "from auth.google_auth import get_google_credentials; get_google_credentials()"
```

`auth/google_auth.py` has no `__main__`, and `get_google_credentials()` raises on a failed refresh rather than falling through to the consent screen — so the stored token (DB row, `token.json`, and any `GOOGLE_REFRESH_TOKEN` env var) must be cleared first. Follow the browser consent (callback on `localhost:8025`); new tokens land in `token.json` and `oauth_tokens`.

- [ ] **Step 4: Verify in production Postgres**

```sql
SELECT tool_name, updated_at FROM oauth_tokens WHERE tool_name = 'google';
```

Expected: `updated_at` within the last few minutes.

- [ ] **Step 5 (if "Testing" today): Note for follow-up** — append to Phase 0 findings: "GCP app remains in Testing mode; Google refresh tokens will die again in 7 days. Recommend publishing the app before next pause."

---

## Phase 2 — Reconnect GitHub sources (conditional)

**Why now:** Workers will not redeploy from the dashboard without a connected source. Cron services can survive without one but will not pick up future code changes.

### Task 2.1: Reconnect `token-keeper`

**Files:** none (Railway dashboard).

- [ ] **Step 1: Settings → Source → "Connect Repo" → select `Ovio-tools/sparkle-shine-poc`, branch `main`**

- [ ] **Step 2: Confirm Custom Config File field = `/railway.worker.toml`**

If empty or set to `/railway.toml`, change it to `/railway.worker.toml` and save. The worker config sets `restartPolicyType = "ALWAYS"`; the cron config does not.

- [ ] **Step 3: Do NOT redeploy yet** — Phase 3 controls the restart order.

### Task 2.2: Reconnect `simulation-engine`

**Files:** none (Railway dashboard).

- [ ] **Step 1: Same as Task 2.1.1, for `simulation-engine`**

- [ ] **Step 2: Same as Task 2.1.2 — Custom Config File must be `/railway.worker.toml`**

- [ ] **Step 3: Do NOT redeploy yet**

### Task 2.3: Reconnect cron services if Phase 0 found them disconnected

**Files:** none (Railway dashboard).

- [ ] **Step 1: For each cron service Phase 0.3 flagged as disconnected (`automation-runner`, `intelligence-daily`, `intelligence-weekly`, `sales-outreach`):**

  - Settings → Source → "Connect Repo" → `Ovio-tools/sparkle-shine-poc`, branch `main`
  - Custom Config File = `/railway.toml` (cron services use the root config, not the worker config)

- [ ] **Step 2: Auto-deploy will trigger** because reconnecting the source counts as a deployment event. **This is the same back door that caused the 2026-05-01 incident.** Mitigation: cron services with their schedule cleared run their start command exactly once on first deploy (per the incident analysis), then sit idle until the cron field is repopulated. So a one-shot run is acceptable; subsequent runs require Phase 6 schedule restoration.

- [ ] **Step 3: Watch the one-shot run if it happens**

```bash
railway logs --service automation-runner --environment production -f
```

If the run errors, fix before restoring the cron schedule in Phase 6.

---

## Phase 3 — Resume workers (today)

**Why workers first:** `token-keeper` underpins Jobber auth for every other service. `simulation-engine` produces the upstream events that `automation-runner` will need.

### Task 3.1: Resume `token-keeper`

**Files:** none.

- [ ] **Step 1: Trigger a redeploy from Railway dashboard**

Service → Deployments → "Redeploy" (or wait for the auto-deploy triggered by Task 2.1's source reconnect to land — if it already succeeded, skip this step).

- [ ] **Step 2: Tail logs**

```bash
railway logs --service token-keeper --environment production -f
```

Expected within ~2 minutes: a "Refreshing Jobber token" line, followed by a successful refresh and a sleep-until-next-tick message. The keeper refresh cadence is set inside [services/token_keeper.py](../../../services/token_keeper.py).

- [ ] **Step 3: If a 401 appears, Phase 1.2 did not actually land**

Re-verify the Postgres `oauth_tokens.jobber` row, redo Phase 1.2 if needed, then redeploy.

- [ ] **Step 4: Confirm a successful refresh tick**

After the first successful refresh, stop tailing (Ctrl-C) and verify in Postgres:

```sql
SELECT tool_name, updated_at FROM oauth_tokens WHERE tool_name = 'jobber';
```

`updated_at` should now be inside the last 2 minutes — proving the keeper, not the local OAuth flow, is the most recent writer.

### Task 3.2: Resume `simulation-engine`

**Files:** none.

- [ ] **Step 1: Trigger a redeploy** (same pattern as 3.1.1).

- [ ] **Step 2: Tail logs**

```bash
railway logs --service simulation-engine --environment production -f
```

Expected: **no** "Resumed from checkpoint" line. The checkpoint (`simulation/checkpoint.json`) lived in the container that was removed on 2026-05-04, so `load_checkpoint()` finds nothing and the engine starts a fresh day at today's date, then normal tick output. The gap since 2026-05-04 is not backfilled (see Out of scope). A checkpoint message referencing 2026-05-04 would actually be a surprise worth investigating.

- [ ] **Step 3: Watch for ~5 minutes**

Confirm at least one full tick cycle completes without errors. If `#automation-failure` lights up, stop the service and diagnose before continuing.

- [ ] **Step 4: Spot-check `#automation-failure` in Slack**

`#automation-failure` is not in `tool_ids.json` — it is created on demand by [simulation/error_reporter.py](../../../simulation/error_reporter.py); find it by name in Slack. (`C0AML3Q8PSM` is `#daily-briefing`, not this channel.) New alerts since the redeploy should be zero.

---

## Phase 4 — Resume `automation-runner` (today)

**Why before intelligence:** The automation runner clears the queue of completed Jobber jobs awaiting QBO invoices and the new SQLs awaiting Pipedrive deals. The simulation will be generating those upstream events from the moment Phase 3.2 succeeded — every 5-minute delay in starting the runner is a small backlog being added.

### Task 4.1: Restore `automation-runner` cron schedule

**Files:** none (Railway dashboard).

- [ ] **Step 1: `automation-runner` → Settings → Deploy → "Cron Schedule"**

Set to `*/5 * * * *`. Save.

- [ ] **Step 2: Wait for the next 5-minute boundary**

The cron will fire automatically. No manual trigger needed.

- [ ] **Step 3: Tail the first run**

```bash
railway logs --service automation-runner --environment production -f
```

Expected: a summary banner with `0 failures` and a duration under ~30 seconds. If failures > 0, identify the failing module and fix before any further phase.

- [ ] **Step 4: Confirm no Jobber 401s or QBO 401s in the log**

Either of these means Phase 1 didn't fully land. Stop and re-verify token state.

---

## Phase 5 — 24-hour soak

**Why:** Per the staged-resume cadence selected at planning time, intelligence and sales-outreach do not come back until one full automation-runner cycle proves clean. This is the cheapest insurance against a cascade of bad daily-briefing content or outbound sales emails on top of a broken pipeline.

### Task 5.1: Monitor for 24h

**Files:** none.

- [ ] **Step 1: Set a reminder for `2026-05-26` ~same time as Phase 4 completed**

- [ ] **Step 2: After 24h, run the soak checklist**

```bash
railway service list
railway logs --service automation-runner --environment production -n 50
railway logs --service simulation-engine --environment production -n 50
railway logs --service token-keeper --environment production -n 20
```

Pass criteria:
- `automation-runner` ran every 5 minutes with 0 failures
- `simulation-engine` ticked continuously, no error_reporter messages
- `token-keeper` refreshed Jobber on its normal cadence with no 401s
- `#automation-failure` Slack channel has zero new alerts in the 24h window

- [ ] **Step 3: If any criterion fails, do not advance to Phase 6**

Fix the root cause first. The intelligence and sales-outreach services will only amplify any underlying issue (briefings citing broken data, outbound emails sent against stale leads).

---

## Phase 6 — Resume intelligence + sales-outreach (next day, after soak passes)

### Task 6.1: Restore `intelligence-daily` cron

**Files:** none.

- [ ] **Step 1: Settings → Deploy → "Cron Schedule" → `0 11 * * 1-5`** (6 AM CDT, Mon-Fri)

Save.

- [ ] **Step 2: Wait until the next scheduled fire**

If resumed mid-week, this is the next weekday morning. If resumed Saturday, it's Monday 06:00 CDT.

- [ ] **Step 3: Watch the first briefing land in Slack `#daily-briefing` (channel `C0AML3Q8PSM`)**

Pass criteria: the briefing posts, contains current numbers (not stale pre-pause data), and citations resolve to live tool URLs.

### Task 6.2: Restore `intelligence-weekly` cron

**Files:** none.

- [ ] **Step 1: Settings → Deploy → "Cron Schedule" → `0 13 * * 0`** (8 AM CDT, Sundays)

Save.

- [ ] **Step 2: First fire is the next Sunday — note the date**

If today is Wednesday 2026-05-27 when resumed, the first weekly is 2026-05-31 08:00 CDT.

- [ ] **Step 3: Manually trigger a dry run today to validate the path without posting**

```bash
railway ssh --service intelligence-weekly --environment production "cd /app && python -m intelligence.runner --skip-sync --date $(date -u +%F) --report-type weekly --dry-run"
```

Expected: a weekly report renders to stdout with the correct structure (6 sections per [docs/skills/weekly-report.md](../../skills/weekly-report.md)) and no exceptions. Failures here are usually a stale `weekly_reports/insight_history.json` — investigate before the real Sunday fire.

### Task 6.3: Restore `sales-outreach` cron LAST

**Why last:** Sales-outreach is the only service in the stack that sends **outbound** messages (emails to real-feeling addresses on the simulated lead list, plus Slack posts about outreach activity). Per the cron cadence history in [docs/railway.md](../../railway.md#cron-cadence-history), this service was previously throttled from `*/5` to `*/30` to mitigate topic-spam during a different incident. Restore at `*/30`, not `*/5`.

- [ ] **Step 1: Settings → Deploy → "Cron Schedule" → `*/30 * * * *`**

Save.

- [ ] **Step 2: Tail the first run**

```bash
railway logs --service sales-outreach --environment production -f
```

Expected: outreach decisions logged, any sent emails accounted for, no Slack flood.

- [ ] **Step 3: Spot-check Slack `#sales` (channel `C0ALRNT2Z8F`) for normal cadence**

---

## Phase 7 — Two-day verification

### Task 7.1: Verify on day +1 and day +2

**Files:** none.

- [ ] **Step 1: At end of each day after Phase 6 completes, run the soak checklist from Task 5.1.2**

- [ ] **Step 2: Re-run the `cross_tool_mapping` audit from [the pause runbook's Section A](../../operations/archive/2026-05-01-railway-pause-state.md#a-cross_tool_mapping-completeness)**

Expected: counts match the pause-time snapshot plus normal growth (a handful of new clients, jobs, invoices reflecting two days of fresh simulation activity).

> **Correction (2026-09-01):** the audit reports *unmapped* records per tool, so a count that **drops** means records gained a mapping (healing), not data loss. Growth in the columns for tools that never map an entity type (e.g. `JOB.hubspot`, `INV.asana`) is the proxy for canonical-table growth. `CLIENT.hubspot` / `LEAD.hubspot` rising is expected once the HubSpot contact pruner (2026-06-19) is live — it deletes `hubspot` mapping rows by design. `railway ssh` needs a registered SSH key; the audit also runs locally with `DATABASE_URL` set to the Postgres service's `DATABASE_PUBLIC_URL` (read-only SELECTs).

- [ ] **Step 3: Confirm `#daily-briefing` posted both mornings (or the appropriate weekday count)**

- [ ] **Step 4: Confirm `#automation-failure` has zero new alerts across both days**

---

## Phase 8 — Archive the pause runbook

### Task 8.1: Move the runbook to the archive

**Files:**
- Move: `docs/operations/2026-05-01-railway-pause-state.md` → `docs/operations/archive/2026-05-01-railway-pause-state.md`

- [ ] **Step 1: Confirm Phase 7 passed for two consecutive days**

If not, do not run this step. The runbook stays in place until verification is clean.

- [ ] **Step 2: Move and commit**

```bash
mkdir -p docs/operations/archive
git mv docs/operations/2026-05-01-railway-pause-state.md docs/operations/archive/
git commit -m "ops: archive 2026-05-01 Railway pause runbook after successful resume"
```

- [ ] **Step 3: Push**

```bash
git push origin main
```

This push will trigger auto-deploys on any service whose GitHub source was reconnected in Phase 2. Workers will get a fresh deployment (acceptable — they're long-running and idempotent on restart). Cron services will fire their start command once (also acceptable per the incident analysis — schedule-gated thereafter).

---

## Phase 0 findings

_Populated during Phase 0 execution. Phases 1-3 read from here._

- Railway service status drift vs. pause snapshot: _TBD_
- `oauth_tokens` updated_at per tool: _TBD_
- GitHub source connected per service: _TBD_
- Google OAuth publication status: _TBD_

---

## Out of scope

- Code changes (none required for resume per [Simulation Data Integrity Rule L5](../../skills/project-conventions.md))
- Database migrations
- Backfilling the simulation gap between 2026-05-04 and 2026-05-25 — engine resumes from checkpoint; the date gap is preserved as a known characteristic of this dataset, not a bug to fix
- Reseed of any tool — only required under the [resume contingency](../../operations/archive/2026-05-01-railway-pause-state.md#resume-contingency-a-tool-account-is-lost) if a tool account was lost; not the case today per the user

## Rollback

If Phase 3 or 4 reveals a fundamental break (e.g., Jobber API contract changed, simulation engine crashes on checkpoint resume), the rollback is to re-pause:

1. Clear cron schedules on any service that was restored
2. Remove the latest deployment on workers (per pause-doc Step 5/6)
3. Disconnect GitHub source again on workers (per pause-doc Step 7)
4. Note findings at the bottom of this plan; the pause runbook remains in place for the next attempt
