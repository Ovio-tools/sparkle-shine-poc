# Railway Pause State — 2026-09-04

This file is the runbook for **resuming** the Sparkle & Shine POC after the pause that began on 2026-09-04. Do not delete it during the pause. Move it to `docs/operations/archive/` only after the project is fully resumed and verified.

This pause follows the **"Corrected order for future pauses"** from the previous (2026-05-01) pause, and its resume follows that runbook's corrected Resume Steps 1–8 with the deltas listed below: [docs/operations/archive/2026-05-01-railway-pause-state.md](archive/2026-05-01-railway-pause-state.md).

## Pause metadata

| Field | Value |
|-------|-------|
| Pause start | 2026-09-04 21:45 UTC (execution began) — **fully quiet from 2026-09-08 16:39 UTC** (see Execution timeline) |
| Expected duration | Unspecified |
| Reason | Owner request |
| Approach | Frozen-branch source freeze + Railway-native stop (no code changes) |
| Snapshot commit branch | `chore/docs-pg-first-and-archive-scripts` |

**Source freeze method:** a `paused` branch was pushed to GitHub at the then-current `origin/main` tip. Each compute service's Railway Source branch is switched `main` → `paused`, so pushes to `main` cannot redeploy anything. This replaces the 2026-05-01 "Disconnect Repo" approach; resume is "switch the branch back", with no custom-config-file re-entry.

**Token-keeper decision:** left **running** (runbook default). It keeps the Jobber rotating refresh token fresh, so resume skips the Jobber re-OAuth entirely. Postgres also remains running.

## Token expiry watch dates

- **Jobber**: token-keeper keeps refreshing (~every 45 min) — no expiry while it runs. If it is ever stopped during the pause, assume re-OAuth needed ~30 days after its last refresh.
- **QuickBooks** (100-day refresh-token lifetime): last refresh `2026-09-08 16:39 UTC` (final automation-runner one-shot) → assume expired after **~2026-12-17**.
- **Google**: last refresh `2026-09-08 16:18 UTC` (final sales-outreach one-shot). Nothing refreshes it during the pause. If the GCP OAuth app is in "Testing" mode the refresh token dies in 7 days (**~2026-09-15**); if published, no expiry. Publication status is still unrecorded — check it at resume (or now) per the archived runbook's Task 0.4.
- **HubSpot / Pipedrive / Asana / Mailchimp / Slack**: static tokens, never expire.

## Service inventory (captured 2026-09-04, pre-pause)

Project `sparkle-shine-poc` (`7354daed-ac44-424b-abdc-4e857d5b553c`), environment `production` (`f2baa6eb-c452-4d73-9574-f4fec7cec45d`), all services region EU West. Captured via `railway service list`.

| # | Service | Service ID | Type | Status at capture | Cron (UTC) | Start command (dashboard) |
|---|---------|------------|------|-------------------|------------|---------------------------|
| 1 | simulation-engine | `efa349ab-cb79-4b1e-b3b4-368d75dca140` | worker (`/railway.worker.toml`) | Online | — | `python -m simulation.engine` |
| 2 | token-keeper | `94b30b64-6f6d-4da8-8426-6329c35e7aa1` | worker (`/railway.worker.toml`) | Online | — | `bash scripts/start_token_keeper.sh` |
| 3 | automation-runner | `87cea803-1b78-4207-a551-f84b985a08ab` | cron (`/railway.toml`) | Completed | `*/5 * * * *` | `python -m automations.runner --all` |
| 4 | intelligence-daily | `68b60281-30bd-4792-b87d-c273ec3e9760` | cron (`/railway.toml`) | **Crashed** (see status notes) | `0 11 * * 1-5` | `python -m intelligence.runner --report-type daily` |
| 5 | intelligence-weekly | `4ab27615-e7ff-4b4b-aeef-15dfd9641d65` | cron (`/railway.toml`) | Completed | `0 13 * * 0` | `python -m intelligence.runner --report-type weekly` |
| 6 | sales-outreach | `39d08b95-e409-4af6-a518-115ddcc4a606` | cron (`/railway.toml`) | Completed | `*/30 * * * *` | `bash scripts/start_sales_outreach.sh` |
| — | Postgres | `253b39fd-db52-479f-9baa-a5a2b30a9f3e` | database | **REMAINS RUNNING** — `postgres-volume` 203 MB / 500 MB | — | — |

`config/tool_ids.json` is unchanged by the pause and tracked in git (retrieve any historical version with `git show <commit>:config/tool_ids.json`). Slack channels for monitoring: `#daily-briefing` `C0AML3Q8PSM`, `#operations` `C0AM76H9K34`, `#sales` `C0ALRNT2Z8F`; `#automation-failure` is auto-created by `simulation/error_reporter.py` and not in `tool_ids.json`.

## Status at pause — known issues (pre-existing, NOT caused by the pause)

1. **intelligence-daily AND intelligence-weekly crash** with `TypeError: Messages.create() got an unexpected keyword argument 'temperature'` — daily on 2026-09-04, 09-07, 09-08; weekly on Sunday 2026-09-06. Its deployment ID changed between 2026-09-01 and 2026-09-04 (`7fc2c856…` → `ef55c68b…`), so a rebuild likely picked up a newer, unpinned `anthropic` SDK. Before that (2026-09-01) it failed with an exhausted-Anthropic-credits 400. **Resume must fix/verify both**: pin or adapt to the SDK, and confirm credit balance.
2. **Payments stagnation**: no rows inserted into `payments` since `2026-04-15` (4,396 rows) despite the engine logging ~20 payment events/day. Under separate investigation; documented in the archived 2026-05-01 runbook's "Resume outcome".
3. **Proposal linkage error**: `RuntimeError: Proposal SS-PROP-0141 has no client_id or lead_id linkage` (`simulation/generators/operations.py`), seen repeatedly before the engine stopped.
4. **Chronic daily critical alerts**: "11 completed job(s) from yesterday have no invoices" (reconciliation queues 11 `create_invoice` retries every day — the same 11) and "918 unaddressed invoices ($137,700) not linked to any job".

## Pre-pause audits (captured 2026-09-04)

Run locally against Railway Postgres (`DATABASE_PUBLIC_URL` from the Postgres service; `railway ssh` has no registered key on this workstation).

### A. cross_tool_mapping completeness (unmapped canonical records per tool)

```json
{
 "CLIENT": {"hubspot": 106, "pipedrive": 459, "jobber": 156, "quickbooks": 157, "mailchimp": 159, "asana": 521},
 "LEAD":   {"hubspot": 412, "pipedrive": 643, "jobber": 931, "quickbooks": 955, "mailchimp": 815, "asana": 955},
 "EMP":    {"hubspot": 18, "pipedrive": 18, "jobber": 18, "quickbooks": 18, "mailchimp": 18, "asana": 18},
 "CREW":   {"hubspot": 4, "pipedrive": 4, "jobber": 4, "quickbooks": 4, "mailchimp": 4, "asana": 4},
 "JOB":    {"hubspot": 7401, "pipedrive": 7401, "jobber": 2, "quickbooks": 7401, "mailchimp": 7401, "asana": 7401},
 "RECUR":  {"hubspot": 251, "pipedrive": 251, "jobber": 251, "quickbooks": 251, "mailchimp": 251, "asana": 251},
 "PROP":   {"hubspot": 271, "pipedrive": 111, "jobber": 272, "quickbooks": 274, "mailchimp": 274, "asana": 274},
 "INV":    {"hubspot": 8775, "pipedrive": 8775, "jobber": 7482, "quickbooks": 2, "mailchimp": 8775, "asana": 8775},
 "PAY":    {"hubspot": 4396, "pipedrive": 4396, "jobber": 4396, "quickbooks": 0, "mailchimp": 4396, "asana": 4396},
 "CAMP":   {"hubspot": 5, "pipedrive": 5, "jobber": 5, "quickbooks": 5, "mailchimp": 0, "asana": 5},
 "REV":    {"hubspot": 14793, "pipedrive": 14793, "jobber": 14793, "quickbooks": 14793, "mailchimp": 14793, "asana": 14793},
 "TASK":   {"hubspot": 5626, "pipedrive": 5626, "jobber": 5626, "quickbooks": 5626, "mailchimp": 5626, "asana": 130},
 "CAL":    {"hubspot": 169, "pipedrive": 169, "jobber": 169, "quickbooks": 169, "mailchimp": 169, "asana": 169},
 "DOC":    {"hubspot": 8, "pipedrive": 8, "jobber": 8, "quickbooks": 8, "mailchimp": 8, "asana": 8}
}
```

Reading (see the archived runbook for the full interpretation): most gaps are by design; `CLIENT.pipedrive = 459` is the deliberate lead-leak detector; falling numbers mean healing, not loss; `CLIENT/LEAD.hubspot` reflect the HubSpot contact pruner. Standing follow-ups carried over: `JOB.jobber = 2`, `INV.quickbooks = 2`, `TASK.asana = 130`.

### B. oauth_tokens freshness

```
 tool_name  |         updated_at         | age at capture
------------+----------------------------+---------------
 google     | 2026-09-04 11:02:12.479784 | 10:33
 jobber     | 2026-09-04 21:20:42.434487 | 00:14
 quickbooks | 2026-09-04 20:55:33.683812 | 00:39
```

### C. Service log baseline (last activity before pause)

| Service | Last activity (UTC) | Notes |
|---|---|---|
| simulation-engine | 2026-09-04 06:00 | Daily summary + reconciliation sweep (the daily 11-invoice warning), then sleeping until midnight |
| token-keeper | 2026-09-04 21:20 | Jobber refresh HTTP 200, on ~45-min cadence — stays running |
| automation-runner | ~21:35 | Clean tick: processed 1, 0 failures, 8.8 s |
| intelligence-daily | 2026-09-04 ~11:12 | Crashed (TypeError — see status notes) |
| intelligence-weekly | 2026-08-30 13:14 | Last Sunday run, posted normally |
| sales-outreach | 2026-09-04 21:32 | Clean run, 0 contacts |

## Pause execution checklist (corrected order)

Executed 2026-09-04. Owner column: **dashboard** = Railway dashboard (user), **CLI** = this workstation.

- [X] **1. Freeze sources (dashboard):** *(done 2026-09-04 ~21:45 UTC; the switch redeployed every service once at the identical commit — cron services each ran their known one-shot)* for each of the 6 compute services → Settings → Source → change branch `main` → `paused`. Push nothing to `main` until this is done. (A branch switch may trigger one redeploy of the identical commit — harmless while everything is still scheduled/running.)
- [X] **2. Stop simulation-engine (CLI):** *(done 2026-09-04 21:54 UTC — SIGTERM logged, graceful daily summary, deployment `821ccce8` REMOVED)* `railway down --service simulation-engine --environment production` (SIGTERM; stops producing upstream events first).
- [X] **3. Drain automation-runner (CLI, wait):** *(verified through 2026-09-05 08:45 UTC — every tick since the engine stopped shows `failed=0`, 0 pending actions due)* let it fire 1–2 more `*/5` ticks after the engine stops; confirm a clean summary (`0 failures`).
- [X] **4. Clear cron schedules (dashboard):** *(done 2026-09-08 ~16:17 UTC — saving Deploy settings redeployed each cron service, so each ran its start command once: automation-runner and sales-outreach clean at 16:19, intelligence-daily crashed again on the known TypeError. Expected one-shot-on-redeploy behaviour.)* `automation-runner` (`*/5 * * * *`), `intelligence-daily` (`0 11 * * 1-5`), `intelligence-weekly` (`0 13 * * 0`), `sales-outreach` (`*/30 * * * *`) → Settings → Deploy → clear "Cron Schedule" → Save. Record the current values (they are in the inventory table above) so resume is a paste.
- [X] **5. token-keeper:** intentionally left running — still refreshing Jobber every ~45 min (last seen 2026-09-09 14:10 UTC) (see metadata). If it must be stopped later: `railway down --service token-keeper --environment production`, and note the date — Jobber re-OAuth will be needed at resume.
- [X] **6. Commit and push this snapshot file** *(2026-09-09)* (safe at any point after step 1; pushed to the `chore/docs-pg-first-and-archive-scripts` branch, which no service watches — and after step 1 not even `main` deploys anything).

**Do not** delete services, change env vars, or modify custom config file paths. Postgres stays running.

## Execution timeline (actual)

| When (UTC) | Event |
|---|---|
| 2026-09-04 ~21:45 | Step 1 — all 6 service Source branches switched `main` → `paused`. The switch redeployed each service once at the identical commit. |
| 2026-09-04 21:46 | Step 2 — `railway down --service simulation-engine`; deployment `821ccce8` REMOVED. |
| 2026-09-04 21:54 | Engine logged `Shutdown signal received (signal 15)`, wrote its daily summary, `Stopping Container`. **Last engine activity of any kind.** |
| 2026-09-04 → 2026-09-08 | Step 3 drain verified clean, but **the cron schedules stayed live for ~4 more days** (step 4 was executed on 09-08, not 09-04). During this window `automation-runner` ran every 5 min (every tick `failed=0`) and `sales-outreach` every 30 min (0 contacts). `intelligence-daily` fired 09-07 and 09-08 at 11:01 and **crashed** both times on the known TypeError; `intelligence-weekly` fired Sunday 09-06 13:00 and crashed the same way. |
| 2026-09-08 ~16:17 | Step 4 — cron schedules cleared. Saving triggered one redeploy + one-shot run per cron service. |
| 2026-09-08 16:19 / 16:39 | Final `sales-outreach` and `automation-runner` runs, both clean. **Last compute activity in the project apart from token-keeper.** |
| 2026-09-09 14:10 | `token-keeper` still refreshing Jobber on its ~45-min cadence, as intended. |

**Was the 4-day tail harmful?** No. With the engine down there were no new jobs, contacts, or payments to process — the runner had nothing to do and reported `failed=0` on every tick, and it kept the QuickBooks and Google tokens fresh (which is why the watch dates above start on 09-08). The only cost was two `intelligence-daily` crashes and one `intelligence-weekly` crash on the pre-existing SDK bug. Nothing needs remediation. The ordering guarantee that matters — engine stopped **before** the runner went quiet — held.

**Also observed during the step-2 window** (pre-existing, add to known issues): `RuntimeError: Proposal SS-PROP-0141 has no client_id or lead_id linkage` from `simulation/generators/operations.py`.

## Post-pause verification (~30 minutes after step 4)

Verified 2026-09-09 ~14:50 UTC — roughly 22 hours after the last cron activity.

- [X] `railway service list` — `simulation-engine` has no live deployment (its only listed deployment, `821ccce8`, is REMOVED; the service reads "Failed", which is how a removed always-restart worker presents). `token-keeper` Online. Cron services show their final one-shot deployment from 09-08 and have not run since.
- [X] `railway logs --service simulation-engine` — **zero log lines after 2026-09-04 21:54**. Independently corroborated by row counts: `jobs = 7401`, `invoices = 8775`, identical to the pre-pause audit, so the engine generated nothing.
- [X] `railway logs --service automation-runner` — last run finished 2026-09-08 16:19:51 UTC, `failed=0`. Silent since.
- [ ] Slack `#automation-failure` quiet — **not verified from here** (no Slack read access in this session). Low risk: the recurring alerts came from the engine's reconciliation sweep, which has been down since 09-04, and every runner tick reported `failed=0`. Worth a glance in the Slack UI.
- [X] Postgres reachable — `SELECT` returned `invoices 8775 | jobs 7401 | payments 4396`.
- [X] token-keeper still refreshing Jobber (`HTTP 200`, last 2026-09-09 14:10 UTC).

## Resume runbook

Follow the archived runbook's corrected **Resume Steps 1–8** ([archive/2026-05-01-railway-pause-state.md](archive/2026-05-01-railway-pause-state.md)) with these deltas:

1. **Step 1 (tokens):** Jobber needs nothing (token-keeper kept it fresh). QuickBooks needs re-OAuth only if the pause exceeded ~2026-12-13. Google: if the GCP app is in "Testing" and the pause exceeded 7 days, re-consent per Step 1's Google block.
2. **Step 2 (sources):** instead of reconnecting repos, switch each service's Source branch `paused` → `main` (custom config files were never lost). Delete the `paused` branch afterwards: `git push origin :paused`.
3. **Step 3 (token-keeper):** already running — skip.
4. **Steps 4–6:** unchanged — restore `automation-runner` cron first, then redeploy `simulation-engine`, then the remaining crons (sales-outreach last, at `*/30`).
5. **Before restoring intelligence crons:** fix the `intelligence-daily` TypeError (pin/adapt the `anthropic` SDK) and confirm Anthropic credit balance — otherwise the first morning run crashes again.
6. **Step 7 (verify) and Step 8 (archive this file):** unchanged. Fill in the checklists this time.

## Out-of-scope during pause

- No code changes to entry points, config files, or schema
- Postgres is **NOT** paused (preserves all canonical data)
- The payments-stagnation investigation and the intelligence-daily SDK fix are deferred to resume (or a separate session), not blockers for pausing
