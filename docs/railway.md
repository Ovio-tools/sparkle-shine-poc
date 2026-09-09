# Railway Deployment Notes

Moved out of CLAUDE.md. Read this before changing any Railway service config,
start command, or `railway.*.toml` file. The service table (schedules and start
commands) stays in CLAUDE.md.

## Nixpacks 1.38.0 workaround

If a Railway service build fails with `Found argument '-m'`, use a wrapper
script (`scripts/start_*.sh`) as the dashboard start command instead of
`python -m ...` directly. Do NOT set the `NIXPACKS_START_CMD` env var alongside
a dashboard start command — the combination causes build failures.

## Config files per service type

- **Cron services** (`automation-runner`, `sales-outreach`, `intelligence-daily`,
  `intelligence-weekly`): use the shared root `railway.toml`.
- **Always-on workers** (`simulation-engine`, `token-keeper`): point Railway's
  "Custom Config File" setting at `railway.worker.toml` so they get
  `restartPolicyType = "ALWAYS"` without affecting the cron jobs.
- `railway.simulation.toml` remains only as a compatibility alias for older
  `simulation-engine` setups.

## Cron cadence history

- `automation-runner` runs every 5 minutes (`*/5 * * * *`).
- `sales-outreach` was moved from `*/5` to `*/30` as a topic-spam mitigation.
  The root cause has since been fixed; the slower cadence is the intended
  current state. Restoring `*/5` is a deliberate decision, not a cleanup.

## Pausing and resuming the project

> **The project is PAUSED as of 2026-09-04 (fully quiet 2026-09-08).** Only
> `token-keeper` and `Postgres` are running; the 6 compute services are stopped
> or have empty cron schedules, and every service's Source branch is frozen at
> `paused` so pushes to `main` cannot redeploy. To resume, follow
> `docs/operations/2026-09-04-railway-pause-state.md`.


Full runbook, including the 2026-05-01 auto-redeploy incident write-up:
`docs/operations/archive/2026-05-01-railway-pause-state.md`. Rules learned from it:

- A Railway pause has two layers: deployment state (remove worker deployments,
  clear cron schedules) **and** auto-deploy triggers (the GitHub source
  connection). Any push to `main` redeploys every connected service — workers
  come back to life and cron services run their start command once. Disconnect
  the source (or switch the Source branch to a frozen branch) on **all six**
  compute services *before* pushing anything.
- Stop `simulation-engine` before clearing the `automation-runner` schedule, and
  restore `automation-runner` before redeploying `simulation-engine`. Otherwise
  the engine's daily reconciliation sweep alerts on un-invoiced jobs.
- Prefer leaving `token-keeper` running through a pause: it only refreshes the
  Jobber token, and keeping it up avoids the Jobber re-OAuth on resume.
- `simulation/checkpoint.json` is container-local. Removing a deployment
  discards it and the engine restarts fresh at today's date — there is no
  "resume from checkpoint" on Railway.
- OAuth flows run locally write to whatever `DATABASE_URL` points at. Set it to
  the Postgres service's `DATABASE_PUBLIC_URL` when re-authorising for Railway.
- No kill-switch env var exists yet. Adding one to the four entry points
  (`simulation.engine`, `automations.runner`, `intelligence.runner`,
  `automation_07_sales_outreach`) would turn a pause into a variable flip.
