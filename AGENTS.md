# AGENTS.md

Read this file before executing prompts in this repository.

## Source Of Truth

For investigation and troubleshooting in this repo, default to sources in this order:

1. GitHub-tracked repository state for code, config, scripts, migrations, docs, and intended behavior
2. Railway production state for database contents, environment variables, deployed service runtime, logs, auth state, and token behavior
3. Railway service/container access for production-like verification
4. Railway Postgres access for live data verification
5. Local untracked files only as a last resort

Do not start by trusting the local database, local `.env` files, or local token JSON files unless the prompt explicitly says the issue is local-only, test-only, seeding-only, or offline-tooling-only.

## Working Rules

- Prefer GitHub-tracked files for repository structure, service names, scripts, migrations, and intended setup.
- Prefer Railway for live credentials, runtime behavior, deployed service state, logs, and DB verification.
- During troubleshooting, treat Railway as authoritative for DB state, env vars, auth state, and live tool behavior.
- Use `railway ssh` for production-like runtime verification when container context matters.
- Use `railway connect` for Railway Postgres access when live database verification matters.
- Treat `railway run` as local execution with Railway environment variables injected, not as proof of deployed runtime behavior.
- Never expose secrets in responses, commits, or docs.

## Local Files Are Fallbacks, Not Truth

Examples of local fallback files:

- `.env`
- `.env.railway`
- `token.json`
- `.jobber_tokens.json`
- `.quickbooks_tokens.json`
- `credentials.json`

Use these only when Railway or GitHub cannot answer the question, or when the prompt is explicitly about local development.

## Troubleshooting Preflight

1. Read `CLAUDE.md`
2. Confirm repo context from git/GitHub state
3. Confirm linked Railway project/environment
4. If the issue touches runtime behavior, logs, auth, env vars, or data mismatches, verify Railway before drawing conclusions from local state
5. If database truth matters, verify Railway Postgres before trusting any local database snapshot
