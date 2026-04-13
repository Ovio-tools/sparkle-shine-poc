# Agent.md

Read this file before executing any prompt in this repository.

## Purpose

This file is the operational preflight guide for working in `sparkle-shine-poc`.
Use it to decide:

- which systems are source-of-truth
- how to access each tool safely
- when to use GitHub, Railway, local files, or live containers

## Access Order

When a prompt requires tool access, prefer sources in this order:

1. GitHub-tracked files in this repo
2. Railway project/runtime state
3. Railway service container access via `railway ssh`
4. Railway database access via `railway connect`
5. Local untracked auth files only as a last resort

Do not assume local `.env`, `token.json`, `.jobber_tokens.json`, or `.quickbooks_tokens.json` are the right source of truth if Railway or GitHub can answer the question.

## General Rules

- Prefer GitHub-tracked configuration for structure, names, scripts, and intended setup.
- Prefer Railway for live credentials, runtime behavior, deployed service state, and logs.
- Prefer `railway ssh` over `railway run` when private-network access or production-like execution matters.
- Treat `railway run` as local execution with Railway environment variables injected.
- Use `railway connect` for Railway Postgres access instead of assuming local connectivity from `railway run`.
- Never expose or paste secrets into responses, commits, or docs.
- If a command can mutate production state, verify the linked Railway project/environment first.

## GitHub Access

Use GitHub for:

- tracked code
- tracked docs
- repo metadata
- branch, PR, issue, and Actions workflows

Useful commands:

- `git remote -v`
- `gh repo view`
- `gh auth status`
- `gh api repos/Ovio-tools/sparkle-shine-poc`

Notes:

- The repo tracks examples like `.env.example` and `.env.railway.example`, not live secrets.
- Railway deploys from GitHub state, not uncommitted local changes.

## Railway Access

Use Railway for:

- deployed services
- runtime logs
- live environment variables
- live container execution
- production-linked auth and token behavior

Useful commands:

- `railway status`
- `railway environment list --json`
- `railway service status --all`
- `railway logs --service <name> --environment production --lines 100`
- `railway ssh --service <name> --environment production`
- `railway connect Postgres`

Important behavior:

- `railway run` is local-process execution with Railway env vars.
- `railway ssh` executes inside the running Railway container.
- For production-like verification, prefer `railway ssh`.
- For database shells, prefer `railway connect`.

## Tool Access By System

### GitHub

- Source of truth: GitHub repo + local git checkout
- Read: yes
- Write: yes, if the task requires repo changes
- Use for code, PRs, issues, branches, pushes, and workflow inspection

### Railway

- Source of truth: Railway project `sparkle-shine-poc`, environment `production`
- Read: yes
- Write: yes, but be deliberate
- Use for service status, logs, runtime env, service shells, and deployments

### Jobber

- Source of truth: Railway runtime + token keeper + DB-backed token storage
- Do not treat local Jobber token files as authoritative
- `token-keeper` is the sole owner of refresh-token rotation
- Other services should consume the current token state, not invent their own refresh flow
- If Jobber auth looks broken locally, check Railway logs before assuming production is broken

Useful commands:

- `railway logs --service token-keeper --environment production --lines 100`
- `railway ssh --service simulation-engine --environment production`

### QuickBooks

- Source of truth: Railway runtime and DB-backed token/auth flow
- Prefer Railway runtime over local token files
- Verify behavior from deployed services or logs when possible

### Google Workspace

- Source of truth: Railway env for deployed runtime, repo code for scopes and auth flow
- Includes Drive, Docs, Sheets, Calendar, and Gmail
- If production-like behavior matters, verify from Railway runtime

### HubSpot

- Source of truth: Railway env + repo integration code
- Prefer Railway-backed auth for live checks

### Pipedrive

- Source of truth: Railway env + repo integration code
- Prefer Railway-backed auth for live checks

### Slack

- Source of truth: Railway env + repo integration code
- Use Railway-backed auth for live verification

### Asana

- Source of truth: Railway env + repo integration code
- Use Railway-backed auth for live verification

### Mailchimp

- Source of truth: Railway env + repo integration code
- Use Railway-backed auth for live verification

### Local Auth Files

Examples:

- `.env`
- `.env.railway`
- `token.json`
- `.jobber_tokens.json`
- `.quickbooks_tokens.json`
- `credentials.json`

Rules:

- These are fallback inputs, not default truth, unless the prompt is explicitly about local development auth.
- Do not rely on them before checking GitHub-tracked docs and Railway runtime state.

## Recommended Preflight

Before acting on a prompt that touches tools:

1. Read `CLAUDE.md`
2. Confirm repo context with `git remote -v` and `gh auth status`
3. Confirm Railway context with `railway status`
4. If production behavior matters, use `railway service status --all`
5. If runtime verification matters, use `railway ssh --service <name> --environment production`
6. If DB verification matters, use `railway connect Postgres`

## Railway Deployment

This project deploys to Railway. The Railway CLI is authenticated via
the `RAILWAY_API_TOKEN` environment variable.

### Before deploying

- Always push changes to GitHub before triggering a Railway deployment.
  Railway deploys from the GitHub repo, not local files.
- Run `railway status` to confirm which project/environment is linked.

### Four services

- simulation-engine
- automation-runner
- daily-intelligence-runner
- weekly-intelligence-runner

### Common commands

- `railway up` - deploy from current directory
- `railway logs --service <name>` - check service logs
- `railway variables` - list environment variables
- `railway variables --set KEY=VALUE` - set a variable
- `railway ssh` - shell into a running container
- `railway status` - show linked project/environment

## Extra Railway Notes

- In current CLI behavior, `railway variable` is the actual command family. If `railway variables` is unavailable, use `railway variable list` and `railway variable set`.
- Validate exact service names with `railway service status --all` before acting. Historical docs and user notes may use slightly different labels than the current Railway project.
- If native SSH is needed, verify Railway login, local SSH key presence, and Railway SSH key registration before assuming `railway ssh --native` will work.
