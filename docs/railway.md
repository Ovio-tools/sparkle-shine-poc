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
