# CLAUDE.md -- Sparkle & Shine POC

## What This Project Is

An internal proof-of-concept for OVIO Digital that demonstrates workflow automation and AI-driven business intelligence for small/medium service businesses. It simulates a fictional Austin-based cleaning company called **Sparkle & Shine Cleaning Co.** across 8 real SaaS platforms plus Google Workspace. Cross-tool automations connect the platforms. A live simulation engine generates daily business activity. An intelligence layer powered by Claude (Anthropic API) delivers daily and weekly reports via Slack.

This is NOT a customer-facing product. It is an internal asset to prove the concept works before selling to real clients.

**Fictional company:** Sparkle & Shine Cleaning Co., Austin TX, ~$2M/year, 18 employees (12 cleaners in 4 crews, 2 team leads, 1 office manager, 1 sales/estimator, 1 bookkeeper, owner Maria Gonzalez). Residential cleaning (recurring + one-time) and commercial cleaning (contract-based). See `config/business.py` for full company profile and `config/narrative.py` for the 12-month timeline.

## Tech Stack

- **Language:** Python 3.11+ (Railway/Nixpacks default is 3.11; avoid macOS system Python 3.9 for local dev)
- **Database:** PostgreSQL via psycopg2 (production/simulation/automations/intelligence), SQLite (seeding, setup, tests)
- **No middleware:** All integrations are direct API calls (no Zapier/Make)
- **LLM:** Anthropic API (`claude-sonnet-4-6` for daily briefings, `claude-opus-4-6` for weekly analysis)
- **Deployment:** Railway (`railway.toml` in repo root, 6 services — start commands configured per-service on dashboard via wrapper scripts; always-on workers should use `/railway.worker.toml` as their custom config file)

### Railway Services

| Service | Type | Schedule (UTC) | Start Command (Dashboard) |
|---------|------|---------------|---------------------------|
| simulation-engine | Long-lived | — | `python -m simulation.engine` |
| automation-runner | Cron | `*/5 * * * *` | `python -m automations.runner --all` |
| sales-outreach | Cron | `*/30 * * * *` | `bash scripts/start_sales_outreach.sh` |
| intelligence-daily | Cron | `0 11 * * 1-5` (6 AM CDT) | `python -m intelligence.runner --report-type daily` |
| intelligence-weekly | Cron | `0 13 * * 0` (8 AM CDT Sun) | `python -m intelligence.runner --report-type weekly` |
| token-keeper | Worker (always-on) | — | `bash scripts/start_token_keeper.sh` |

**Nixpacks 1.38.0 workaround:** If a Railway service build fails with `Found argument '-m'`, use a wrapper script (`scripts/start_*.sh`) as the dashboard start command instead of `python -m ...` directly. Do NOT set `NIXPACKS_START_CMD` env var alongside a dashboard start command — the combination causes build failures.
- **Worker config:** Keep the shared root [`railway.toml`](/Users/ovieoghor/Documents/Claude%20Code%20Exercises/Simulation%20Exercise/sparkle-shine-poc/railway.toml) for cron services. Point always-on workers (`simulation-engine`, `token-keeper`) at [`railway.worker.toml`](/Users/ovieoghor/Documents/Claude%20Code%20Exercises/Simulation%20Exercise/sparkle-shine-poc/railway.worker.toml) in Railway's "Custom Config File" setting so they use `restartPolicyType = "ALWAYS"` without affecting the cron jobs. [`railway.simulation.toml`](/Users/ovieoghor/Documents/Claude%20Code%20Exercises/Simulation%20Exercise/sparkle-shine-poc/railway.simulation.toml) remains as a compatibility alias for older `simulation-engine` setups.
- **Local dev:** macOS, Postgres.app, Python 3.11+ recommended. The macOS system `python3` (3.9 + LibreSSL) works for basic scripts but emits Google client EOL warnings and `urllib3` LibreSSL warnings during tests.

## Tool Stack

| Tool | Function | Auth |
|------|----------|------|
| Jobber | Operations and scheduling | OAuth 2.0 |
| Pipedrive | Sales pipeline | API token (`x-api-token` header) |
| HubSpot | Marketing and contact database | Private App Token (Bearer) |
| Mailchimp | Email marketing | API key (Basic Auth) |
| QuickBooks Online | Finance | OAuth 2.0 |
| Asana | Back-office tasks | Personal Access Token (Bearer) |
| Slack | Internal comms and briefings | Bot User OAuth Token (Bearer) |
| Google Workspace | Docs, Sheets, Calendar, Gmail, Drive | OAuth 2.0 |

OAuth tokens are stored via `auth/token_store.py` (four-tier: PostgreSQL DB -> JSON file fallback -> env vars -> empty dict). JSON fallback files: `.jobber_tokens.json`, `.quickbooks_tokens.json`, `token.json` (Google).

**Jobber Token Keeper:** Jobber uses single-use rotating refresh tokens. The `token-keeper` Railway service (`services/token_keeper.py`) is the sole owner of Jobber token refresh. All other services are read-only consumers. Set `JOBBER_TOKEN_KEEPER_ENABLED=1` on all Railway services except token-keeper itself. Local dev uses legacy self-refresh (env var unset).

Pipedrive and HubSpot overlap intentionally. Pipedrive owns the active sales process (deals, proposals). HubSpot owns marketing and the full contact database. The overlap mirrors real SMB operations and is a feature for the intelligence layer to exploit.

See @docs/skills/tool-api-patterns.md for rate limits, endpoints, headers, and error codes.

## IMPORTANT: Common Mistakes

These rules exist because of real bugs. Follow them strictly.

- When troubleshooting runtime issues, auth issues, missing records, or data mismatches, treat Railway as the default source of truth for DB state, env vars, token state, and live tool behavior. Do not start by trusting the local DB or local token files unless the prompt is explicitly about local development, SQLite seeding, or tests.
- NEVER `import sqlite3` or call `sqlite3.connect()` in new production code. Use `from database.connection import get_connection`.
- NEVER use integer indexing on database rows (`row[0]`, `fetchone()[0]`). Rows are `RealDictRow` dicts. Always use `row["column_name"]`.
- NEVER use `?` as a parameter placeholder in PostgreSQL code. Use `%s`.
- NEVER use `PRAGMA` or `sqlite_master`. Use helpers from `database.connection`: `column_exists()`, `table_exists()`, `get_column_names()`.
- NEVER write to `poll_state` directly. The automation runner owns that watermark system.
- NEVER import tool-specific auth modules directly. Use `auth.get_client(tool_name)` exclusively.
- NEVER create an API record without first checking `cross_tool_mapping` for duplicates.
- NEVER hardcode rate limit delays. Import pre-configured `Throttler` instances from `seeding/utils/throttler.py`.
- NEVER register a Pipedrive mapping for SQL contacts. Only register the HubSpot mapping. The missing Pipedrive mapping is what triggers the automation runner's lead-leak detection.
- NEVER remove simulation invoicing logic without confirming the automation runner handles QBO invoice creation for completed Jobber jobs.
- NEVER change `database/schema.py` without a corresponding migration script in `scripts/` that applies the same change to the live PostgreSQL database via ALTER statements.

## Database Patterns (PostgreSQL)

IMPORTANT: All new code must use PostgreSQL via psycopg2. Some legacy modules (`simulation/reconciliation/`, `seeding/`, `setup/`, `demo/`, some `scripts/`, `tests/`) still use SQLite directly. Do not extend that pattern.

### Troubleshooting Source Of Truth
- For production or production-like diagnosis, verify Railway first: Railway Postgres for data, Railway env for config, Railway logs/runtime for auth and tool behavior.
- Use local PostgreSQL or SQLite only for local-dev reproduction, tests, seeding flows, or when the prompt is explicitly about local state.
- If local state disagrees with Railway, assume local drift until Railway proves otherwise.
- Preferred sequence for diagnosis: `railway status` -> `railway service status --all` -> `railway logs --service <name> --environment production` -> `railway ssh --service <name> --environment production` or `railway connect Postgres`, depending on whether runtime or DB verification is needed.

### Connection
- Preferred: `from database.connection import get_connection`
- Also valid: `from database.schema import get_connection` (delegates to `connection.py`, accepts but ignores `db_path`)
- `database.connection` also exports: `column_exists()`, `table_exists()`, `get_column_names()`, `date_subtract_sql()`
- Requires `DATABASE_URL` in environment (e.g., `postgresql://localhost/sparkle_shine`)

### SQL Patterns
- Parameter placeholders: `%s` (not `?`)
- Current timestamp: `CURRENT_TIMESTAMP` (not `datetime('now')`)
- Current date: `CURRENT_DATE` (not `date('now')`)
- Date arithmetic: `CURRENT_DATE - INTERVAL '60 days'` or `date_subtract_sql(60)` from `database.connection`
- Upsert: `INSERT INTO ... ON CONFLICT ... DO NOTHING` or `DO UPDATE SET` (not `INSERT OR IGNORE` or `INSERT OR REPLACE`)

### Row Access
- Rows are `RealDictRow` objects (dict-like). Always: `row["column_name"]`
- For scalar queries: `SELECT COUNT(*) AS cnt ...` then `row["cnt"]`

## Cross-Tool Identity System

Every entity gets a canonical ID: `SS-{TYPE}-{NNNN}` (e.g., `SS-CLIENT-0047`). The `cross_tool_mapping` table links each canonical ID to its tool-specific IDs across all platforms. Email is the primary natural key for residential clients. Commercial clients use a compound key (company name + billing contact email).

Use `database/mappings.py` for all ID operations: `generate_id()`, `register_mapping()`, `get_tool_id()`, `get_canonical_id()`, `find_unmapped()`, `get_tool_url()`, `bulk_register()`.

### Writing to APIs
- Check `cross_tool_mapping` before creating any record to avoid duplicates.
- After every successful API create, immediately register the returned ID in `cross_tool_mapping`.
- Include the canonical ID in the record's notes/metadata field (e.g., `"SS-ID: SS-CLIENT-0047"`).
- Save checkpoints every 25-100 records (varies by tool).

## Project Structure

```
sparkle-shine-poc/
├── config/          # business.py, narrative.py, tool_ids.json
├── auth/            # Unified auth: get_client(tool_name), token_store.py (3-tier OAuth storage)
├── database/        # connection.py (PostgreSQL), schema.py (DDL + legacy get_connection), mappings.py
├── simulation/      # Live simulation engine, 6 generators (contacts, deals, operations, payments, churn, tasks),
│                    #   reconciliation/, error_reporter.py, deep_links.py, variation.py
├── automations/     # 8 workflow modules + runner.py, triggers.py, base.py, state.py, utils/
├── intelligence/    # syncers/, metrics/ (6 modules), context_builder, briefing_generator, weekly_report,
│                    #   slack_publisher, runner.py, documents/doc_search.py
├── seeding/         # generators/ (data into SQLite) + pushers/ (SQLite to APIs) + utils/
├── services/        # token_keeper.py (Jobber OAuth refresh — sole owner of rotating refresh tokens)
├── setup/           # configure_tools.py, populate_workspace.py (one-time tool provisioning)
├── demo/            # audit/, fixes/, hardening/, scenarios/, tuning/, walkthrough/, smoke_test.py
├── scripts/         # migrate_to_postgres.py, extract_railway_env.py, backfill_pipedrive_orgs.py, etc.
├── tests/           # test_phase{1,2,4}.py, test_phase5_operations.py, test_simulation.py, test_deals.py,
│                    #   test_error_reporter.py, smoke_test_phase3.py, test_automations/ (7 module tests)
├── docs/skills/     # 5 skill docs for Claude Code sessions
├── docs/superpowers/# plans/ and specs/ from Claude Code directive sessions
├── briefings/       # Archived daily and weekly report outputs + context docs
└── weekly_reports/  # insight_history.json (repetition prevention for weekly insights)
```

## Intelligence Layer

The pipeline follows: **Sync -> Metrics -> Context -> Generate -> Publish**

Two report types: daily (6 AM Mon-Fri, 175-450 words) and weekly (Sunday evening, 700-1400 words). Both post to Slack via Block Kit.

See @docs/skills/weekly-report.md for report structure, section specs, confidence levels, citation formatting, and insight repetition rules. See `intelligence/config.py` for thresholds, targets, and prompt templates.

## Conventions

- **Reproducibility:** Use `random.seed(42)` for all data generation. Identical inputs must produce identical outputs.
- **Error isolation:** Wrap each tool interaction in try/except. One tool failing should never cascade.
- **Checkpoint/resume:** Every API push uses `seeding/utils/checkpoint.py`. Interrupted pushes resume where they left off.
- **Dry run:** All runners and pushers support `--dry-run`.
- **Config over inline:** Magic numbers live in `config/business.py`, `config/narrative.py`, `intelligence/config.py`, or `simulation/config.py`.
- **Tool IDs:** Pipeline stage IDs, custom field IDs, project GIDs all live in `config/tool_ids.json`.
- **Design specs are the source of truth.** When Claude Code surfaces conflicts between a plan verification command and a design spec, the design spec wins.

### Testing
- Phase tests: `tests/test_phase{1,2,4}.py`, `test_phase5_operations.py`
- Simulation tests: `tests/test_simulation.py`, `tests/test_deals.py`, `tests/test_error_reporter.py`
- Automation tests: `tests/test_automations/` (one test module per automation + `test_runner.py`)
- Smoke tests: `tests/smoke_test_phase3.py`, `demo/smoke_test.py`
- Integration tests requiring live APIs are gated behind `@unittest.skipUnless(os.getenv("RUN_INTEGRATION"), ...)`.

## Common Tasks

```bash
# Tests
python tests/test_phase1.py -v
python tests/test_phase2.py -v
python tests/test_phase4.py -v -k "not live and not slack_channel"
RUN_INTEGRATION=1 python tests/test_phase4.py -v
python -m pytest tests/test_automations/ -v
python tests/test_simulation.py -v

# Intelligence layer
python -m intelligence.runner --skip-sync --date 2026-03-17
python -m intelligence.runner --skip-sync --date 2026-03-17 --report-type weekly
python -m intelligence.runner --skip-sync --date 2026-03-17 --dry-run

# Simulation engine
python -m simulation.engine
python -m simulation.engine --dry-run

# Data validation
python seeding/utils/validator.py
python seeding/generators/gen_anomalies.py
python -c "from database.mappings import find_unmapped; print(find_unmapped('jobber', 'CLIENT'))"

# PostgreSQL migration
python scripts/migrate_to_postgres.py
```

Resume an interrupted push by re-running the pusher. Checkpoints handle it.

## On Compaction

When compacting, always preserve: the database pattern rules (PostgreSQL vs. SQLite boundary), the auth convention (`get_client` only), the common mistakes list, the list of modified files in this session, and current test status.

## Key Files to Read First

1. `CLAUDE.md` (this file)
2. `config/business.py` -- company profile, employees, crews, service types
3. `config/narrative.py` -- the 12-month timeline as a data structure
4. `config/tool_ids.json` -- all tool-specific IDs created during Phase 1 setup
5. `database/schema.py` -- DDL and legacy connection wrapper
6. `database/connection.py` -- PostgreSQL connection, helpers (column_exists, table_exists, etc.)
7. `intelligence/config.py` -- thresholds, targets, and prompt templates
8. `simulation/config.py` -- simulation engine configuration

## Skills Reference

Read the relevant skill doc before building new modules:

- @docs/skills/project-conventions.md -- Import paths, naming rules, testing patterns, error handling. Read at session start.
- @docs/skills/tool-api-patterns.md -- Auth patterns, endpoints, headers, rate limits, error codes. Read before any API call.
- @docs/skills/canonical-record.md -- How to create records and register cross-tool mappings. Read before writing to the database.
- @docs/skills/generator-template.md -- Boilerplate class for simulation generators. Copy and fill in.
- @docs/skills/weekly-report.md -- Report structure, data analysis standards, confidence levels, citations, insight repetition rules.

## Reference Documents

- `sparkle-shine-context-transfer.md` -- Comprehensive narrative context: 12-month business story, data volumes, planted discovery patterns, commercial bid-to-contract workflow, Asana onboarding automation, seeding approach (sections 1-17)
- `docs/automation-lessons-learned.md` -- Battle-tested rules (L1-L21) from building the automations
- `docs/superpowers/plans/` -- Claude Code directive plans (simulation engine, generators, weekly report, PostgreSQL migration)
- `docs/superpowers/specs/` -- Design specs (error reporter, simulation framework, deals, operations, weekly report)

## Environment

API keys and secrets are in `.env` (never commit). `DATABASE_URL` must also be set (e.g., `postgresql://localhost/sparkle_shine`; Railway provides this automatically). OAuth tokens are managed by `auth/token_store.py` with JSON file fallback. See `.env.example` for local dev variables and `.env.railway.example` for the full Railway deployment template (auto-generated via `python scripts/extract_railway_env.py`).
