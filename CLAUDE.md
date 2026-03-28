# CLAUDE.md -- Sparkle & Shine POC

## What This Project Is

An internal proof-of-concept for OVIO Digital (an AI consultancy) that demonstrates workflow automation and AI-driven business intelligence for small/medium service businesses. It simulates a fictional Austin-based cleaning company called **Sparkle & Shine Cleaning Co.** across 8 real SaaS platforms plus Google Workspace, pre-seeded with 12 months of realistic operational data. Cross-tool automations connect the platforms. An intelligence layer powered by Claude (Anthropic API) delivers a daily morning briefing via Slack.

This is NOT a customer-facing product. It is an internal asset to prove the concept works before selling it to real clients.

## The Fictional Company

- **Name:** Sparkle & Shine Cleaning Co.
- **Owner:** Maria Gonzalez (fictional)
- **Location:** Austin, TX
- **Revenue:** ~$2M/year
- **Employees:** 18 total (12 cleaners in 4 crews, 2 team leads, 1 office manager, 1 sales/estimator, 1 bookkeeper, Maria)
- **Services:** Residential cleaning (recurring + one-time) and commercial cleaning (contract-based)
- **Crew zones:** A = Westlake/Tarrytown, B = East Austin/Mueller, C = South Austin/Zilker, D = Round Rock/Cedar Park (commercial)

## Tech Stack

- **Language:** Python 3
- **Database:** SQLite (`sparkle_shine.db`) -- the single source of truth for all cross-tool data
- **No middleware:** All integrations are direct API calls (no Zapier/Make)
- **LLM:** Anthropic API (`claude-sonnet-4-6` for data generation and daily briefings, `claude-opus-4-6` reserved for complex analysis)
- **Platform:** macOS

## Tool Stack (8 SaaS Platforms + Google Workspace)

| Tool | Function | Auth |
|------|----------|------|
| Jobber | Operations and scheduling | OAuth 2.0 (refresh token in `.jobber_tokens.json`) |
| Pipedrive | Sales pipeline | API token (`x-api-token` header) |
| HubSpot | Marketing and contact database | Private App Token (Bearer) |
| Mailchimp | Email marketing | API key (Basic Auth) |
| QuickBooks Online | Finance | OAuth 2.0 (refresh token in `.quickbooks_tokens.json`) |
| Asana | Back-office tasks | Personal Access Token (Bearer) |
| Slack | Internal comms and briefings | Bot User OAuth Token (Bearer) |
| Google Workspace | Docs, Sheets, Calendar, Gmail, Drive | OAuth 2.0 (token in `token.json`) |

Pipedrive and HubSpot overlap intentionally -- Pipedrive owns the active sales process (deals, proposals), HubSpot owns marketing and the full contact database. The overlap mirrors real SMB operations and is a feature for the intelligence layer to exploit.

## Project Structure

```
sparkle-shine-poc/
├── CLAUDE.md                    # This file
├── sparkle-shine-context-transfer.md  # Full narrative context document
├── sparkle_shine.db             # SQLite database (source of truth)
├── .env                         # API keys and tokens (never commit)
├── requirements.txt
├── config/
│   ├── business.py              # Company profile, employees, crews, services, neighborhoods
│   ├── narrative.py             # 12-month business timeline as data structure
│   └── tool_ids.json            # Tool-specific IDs (pipeline stages, custom field IDs, project GIDs, etc.)
├── auth/
│   ├── simple_clients.py        # Pipedrive, HubSpot, Mailchimp, Asana, Slack
│   ├── jobber_auth.py           # OAuth 2.0 with auto-refresh
│   ├── quickbooks_auth.py       # OAuth 2.0 with auto-refresh
│   ├── google_auth.py           # OAuth 2.0 Desktop app flow
│   └── get_client(tool_name)    # Unified auth interface
├── db/
│   ├── schema.py                # 18 tables: 15 entity + cross_tool_mapping + daily_metrics_snapshot + document_index
│   └── mappings.py              # Cross-tool ID system: generate_id(), link(), lookup(), find_unmapped()
├── seeding/
│   ├── utils/
│   │   ├── throttler.py         # Rate limiting (pre-configured per tool)
│   │   ├── checkpoint.py        # Resume-on-failure for API pushes
│   │   ├── text_generator.py    # Claude API wrapper for realistic text generation
│   │   ├── faker_austin.py      # Deterministic Austin-specific data generator (seed=42)
│   │   └── validator.py         # Pre-push data validation
│   ├── generators/              # Stage A: generate data into SQLite (no API calls)
│   │   ├── gen_clients.py       # 310 residential + 10 commercial + 160 leads
│   │   ├── gen_jobs.py          # ~8,200 jobs + ~220 recurring agreements
│   │   ├── gen_financials.py    # Invoices + payments with lag distributions
│   │   ├── gen_marketing.py     # Mailchimp campaigns + Pipedrive proposals + HubSpot lifecycle
│   │   ├── gen_tasks_events.py  # Asana tasks + Calendar events + reviews
│   │   └── gen_anomalies.py     # Verifies 7 planted discovery patterns
│   └── pushers/                 # Stage B: push from SQLite to each tool's API
│       ├── push_jobber.py       # Longest push (~2.5-3 hours)
│       ├── push_quickbooks.py
│       ├── push_hubspot.py
│       ├── push_mailchimp.py
│       ├── push_pipedrive.py
│       └── push_asana.py
├── automations/                 # Phase 3: 6 cross-tool automation workflows
│   ├── runner.py                # Polling-based runner with --dry-run support
│   └── (6 automation modules)
├── intelligence/                # Phase 4: Daily briefing pipeline
│   ├── config.py                # Thresholds, targets, prompt templates
│   ├── logging_config.py
│   ├── syncers/                 # Pull fresh data from all 8 tools into SQLite
│   │   ├── base_syncer.py
│   │   └── sync_{tool}.py       # One per tool
│   ├── metrics/                 # Deterministic business metric computations
│   │   ├── revenue.py
│   │   ├── operations.py
│   │   ├── sales.py
│   │   ├── financial_health.py
│   │   ├── marketing.py
│   │   └── tasks.py
│   ├── documents/
│   │   └── doc_search.py        # Keyword search over document_index table
│   ├── context_builder.py       # Assembles structured context doc for Claude
│   ├── briefing_generator.py    # Anthropic API call to produce the briefing
│   ├── slack_publisher.py       # Posts to Slack via Block Kit API
│   └── runner.py                # CLI orchestrator: sync -> metrics -> context -> generate -> publish
├── tests/
│   ├── test_phase1.py           # 15 integration checks
│   ├── test_phase2.py           # 20 volume + mapping + pattern checks
│   └── test_phase4.py           # 22 unit + integration + discovery pattern checks
├── briefings/                   # Archived briefing outputs (briefing_{date}.md)
├── logs/                        # Daily log files (intelligence_{date}.log)
└── checkpoints/                 # Pusher checkpoint files (auto-deleted on completion)
```

## Cross-Tool Identity System

Every entity gets a canonical ID in the format `SS-{TYPE}-{NNNN}` (e.g., `SS-CLIENT-0047`, `SS-JOB-0001`). The `cross_tool_mapping` table links each canonical ID to its tool-specific IDs across all platforms.

- Email is the primary natural key for residential clients
- Commercial clients use compound key (company name + billing contact email)
- Use `db/mappings.py` for all ID operations: `generate_id()`, `link()`, `lookup()`, `reverse_lookup()`, `find_unmapped()`

## Data Volumes

| Entity | Count |
|--------|-------|
| Residential clients | 310 (~180 active, ~60 churned, ~40 occasional, ~30 quick churn) |
| Commercial clients | 10 (8 active, 2 churned) |
| Unconverted leads | 160 |
| Jobs | ~8,200 |
| Invoices | ~8,200 |
| Payments | ~7,900 |
| Commercial proposals | 48 (10 won, 23 lost, 15 open) |
| Recurring agreements | ~220 |
| Asana tasks | ~260 |
| Mailchimp contacts | ~320 |
| Reviews | ~1,400 |
| Calendar events | ~180 |

## 12-Month Business Narrative (Apr 2025 -- Mar 2026)

The seeded data tells a specific story. When generating or modifying data, preserve these narrative beats:

1. **Apr-May 2025:** Steady operations. 140 residential clients, 7 commercial contracts, ~$140K/month.
2. **Jun-Jul 2025:** Summer surge. Deep clean spike, 2 new hires, "Spring into summer" email campaign (25 leads, 18 convert). 2 commercial contracts churn (relocations).
3. **Aug-Sep 2025:** Rough patch. 2 cleaners quit, scheduling gets tight, 3 negative reviews, Maria raises rates +$10/visit causing 5 more cancellations.
4. **Oct-Nov 2025:** Stabilization. Replacement hires, big commercial win (Barton Creek Medical Group, $4,500/month), referral program launches (15 new clients), holiday pre-bookings.
5. **Dec-Jan 2026:** Holiday peak then January dip. Revenue peaks in December, 2 commercial clients pay late (50-60 days on net-30 terms), cash flow tightens.
6. **Feb-Mar 2026:** Recovery. Spring pipeline building, 12 active commercial proposals, "New year fresh start" Mailchimp campaign.

## 7 Planted Discovery Patterns

These are deliberately embedded in the data for the intelligence layer to detect. Do not remove or flatten them:

1. **Crew speed vs. quality:** Crew A takes 20% longer but has the highest satisfaction ratings (avg 4.7 vs. 4.2).
2. **Referral retention advantage:** Referral clients have 2x the retention rate of Google Ads clients.
3. **Day-of-week complaint rate:** Tuesday/Wednesday jobs have 30% fewer 1-2 star reviews.
4. **Commercial upsell signal:** Barton Creek Medical Group has "add-on" or "additional service" markers in 20% of job notes.
5. **Westlake cancellation cluster:** 3 Westlake-neighborhood cancellations within a 14-day window in Feb-Mar 2026 (possible competitor signal).
6. **Maria delegation insight:** Maria's Asana overdue rate is ~40% vs. the office manager's ~10%.
7. **Referral program contract value premium:** Referral-sourced commercial proposals have 30% higher estimated monthly values.

## Rate Limits (Built Into Every API Pusher)

| Tool | Safe Rate | Delay | Notes |
|------|-----------|-------|-------|
| Jobber | 10 req/sec | 0.15s | GraphQL mutations count as 1 each |
| QuickBooks | 500 req/min | 0.15s | Sandbox is more lenient but match production |
| HubSpot | 100 req/10sec | 0.12s | Use batch endpoints (up to 100 records) |
| Mailchimp | 10 req/sec | 0.15s | Batch subscribe up to 500 at once |
| Pipedrive | 80 req/10sec | 0.15s | Sequential per deal to preserve activity order |
| Asana | 150 req/min | 0.45s | Free plan limit, no batch endpoint |
| Slack | 1 msg/sec | 1.1s | Only for notifications, not bulk data |

Pre-configured `Throttler` instances are in `seeding/utils/throttler.py`. Import and use them; do not hardcode delays.

## Key API Endpoints

**Jobber:** GraphQL at `https://api.getjobber.com/api/graphql` with header `X-JOBBER-GRAPHQL-VERSION: 2026-03-10`

**QuickBooks Sandbox:** `https://sandbox-quickbooks.api.intuit.com/v3/company/{COMPANY_ID}`

**HubSpot:** REST at `https://api.hubapi.com`. Batch create contacts via `POST /crm/v3/objects/contacts/batch/create`.

**Pipedrive:** REST at `https://api.pipedrive.com/v1`

**Asana:** REST at `https://app.asana.com/api/1.0`

**Mailchimp:** REST at `https://{server_prefix}.api.mailchimp.com/3.0`

**Slack:** REST at `https://slack.com/api`. Bot needs scopes: `chat:write`, `channels:read`, `channels:join`, `channels:history`, `im:write`.

## Phased Build Plan

| Phase | Name | Status |
|-------|------|--------|
| 0 | Day-Zero API Verification | COMPLETE |
| 1 | Foundation and Company Setup | COMPLETE (15 integration tests passing) |
| 2 | Data Seeding | Plan complete, execution via Claude Code |
| 3 | Cross-Tool Automations | Plan complete (6 automations, 41 unit tests, 11 smoke tests) |
| 4 | Intelligence Layer (Daily Briefing) | Plan complete (9 steps, ~22 tests) |
| 5 | Polish and Demo Prep | Not yet planned |
| 6 | Enhanced Document Intelligence (RAG, Gemini Embedding 2) | Future, parked |

## Conventions and Patterns

### General
- **Reproducibility:** Use `random.seed(42)` for all data generation. Identical inputs must produce identical outputs.
- **Error isolation:** Wrap each tool interaction in try/except. One tool failing should never cascade.
- **Checkpoint/resume:** Every API push operation uses `seeding/utils/checkpoint.py` so interrupted pushes resume where they left off.
- **Dry run:** All runners and pushers support a `--dry-run` flag that logs what would happen without making API calls.

### Code Style
- Config and magic numbers live in dedicated config files (`config/business.py`, `config/narrative.py`, `intelligence/config.py`), not inline.
- Tool-specific IDs (pipeline stage IDs, custom field IDs, project GIDs) live in `config/tool_ids.json`.
- Auth is accessed through the unified `get_client(tool_name)` interface, never by importing tool-specific auth modules directly.
- Canonical IDs follow the `SS-{TYPE}-{NNNN}` format. Always use `db/mappings.py` to generate and look up IDs.
- SQLite is the local state layer for everything: generated data, cross-tool mappings, sync state, metrics snapshots, document index.

### Testing
- Phase tests live in `tests/test_phase{N}.py`.
- Integration tests that require live API access are gated behind `@unittest.skipUnless(os.getenv("RUN_INTEGRATION"), "Skipping integration tests")`.
- Each phase's tests verify both correctness (are the right records present?) and narrative consistency (do the numbers match the 12-month story?).

### Writing to APIs
- Always check `cross_tool_mapping` before creating a record to avoid duplicates.
- After every successful API create, immediately register the returned ID in `cross_tool_mapping`.
- Include the canonical ID in the record's notes or metadata field (e.g., `"SS-ID: SS-CLIENT-0047"`) so records are traceable back to the master database.
- Save checkpoints every 25-100 records (varies by tool).

## Intelligence Layer Pipeline (Phase 4)

The pipeline supports two report types, both following: **Sync -> Metrics -> Context -> Generate -> Publish**

1. **Sync** (optional, skippable for demos): Pull fresh data from all 8 tools into SQLite.
2. **Metrics:** 6 deterministic modules (revenue, operations, sales, financial health, marketing, tasks) compute all numbers from SQLite.
3. **Context:** Assemble a structured text document with all metrics, alerts, and relevant document excerpts.
4. **Generate:** Send the context document to Claude (Sonnet) to produce the report.
5. **Publish:** Post to Slack via Block Kit. Post critical alerts to `#operations` or `#sales`.

### Daily Report (6 sections, 175–450 words, delivered 6 AM Mon–Fri)
Answers: *"What do I need to act on today?"*
1. **Yesterday's Numbers** — 3 lines only: jobs completed, revenue, completion rate (+ 1-sentence flag if notably off)
2. **Today's Operations Snapshot** — crew assignments, utilization, gaps/overloaded flags, overnight cancellations
3. **Cash That Needs Chasing** — invoices that crossed the 30-day or 60-day overdue threshold today only
4. **Deals That Need a Nudge** — stale deals (14+ days) + proposals in "Proposal Sent" for 7+ days; max 3 items by value
5. **Overdue High-Priority Tasks** — count by project/category only (e.g., "3 Sales Pipeline tasks, 1 Operations task")
6. **One Action Item** — single most important suggestion for today, by urgency and dollar impact

Excluded from daily: campaign performance, review summaries (except 1-star overnight), revenue trend comparisons, conversion rates by source.

Run: `python -m intelligence.runner --skip-sync --date 2026-03-17`

### Weekly Report (8 sections, 700–1400 words, delivered Sunday evening or Monday 6 AM)
Answers: *"What patterns should I be paying attention to, and where is the business heading?"*
1. **Week in Review** — revenue vs. target, WoW trend, jobs, cancellations, narrative sentence
2. **Crew Performance Scorecard** — each crew ranked by efficiency + quality, speed-vs-quality call-outs
3. **Cash Flow and AR Health** — full AR aging, DSO trend (4 weeks), deteriorating payers
4. **Sales Pipeline Movement** — entered/moved/won/lost, pipeline value, cycle length, conversion by source
5. **Marketing and Reputation** — campaign metrics, reviews, audience, day-of-week complaint patterns
6. **Task and Delegation Health** — overdue rates by person, delegation gap (Maria vs. office manager)
7. **Neighborhood and Segment Trends** — cancellations by neighborhood (28 days), new client acquisitions by area
8. **One Big Opportunity** — highest-impact strategic insight with quantified upside

Run: `python -m intelligence.runner --skip-sync --date 2026-03-17 --report-type weekly`

## Key Files to Read First

If you are picking up this project in a new session:

1. `CLAUDE.md` (this file) -- project overview and conventions
2. `config/business.py` -- company profile, employees, crews, service types
3. `config/narrative.py` -- the 12-month timeline as a data structure
4. `config/tool_ids.json` -- all tool-specific IDs created during Phase 1 setup
5. `db/schema.py` -- the 18-table SQLite schema
6. `intelligence/config.py` -- thresholds, targets, and the system prompt template (Phase 4)

## Reference Documents

- `sparkle-shine-context-transfer.md` -- comprehensive narrative context (sections 1-17)
- `sparkle-shine-project-plan.docx` -- the original 11-section project plan
- `sparkle-shine-phase4-plan.md` -- Phase 4 execution plan with Claude Code prompts for all 9 steps

## Environment Variables (in .env)

```
ANTHROPIC_API_KEY=
PIPEDRIVE_API_TOKEN=
HUBSPOT_ACCESS_TOKEN=
MAILCHIMP_API_KEY=
MAILCHIMP_SERVER_PREFIX=
ASANA_ACCESS_TOKEN=
SLACK_BOT_TOKEN=
JOBBER_CLIENT_ID=
JOBBER_CLIENT_SECRET=
QUICKBOOKS_CLIENT_ID=
QUICKBOOKS_CLIENT_SECRET=
QUICKBOOKS_COMPANY_ID=
GOOGLE_CLIENT_ID=
GOOGLE_CLIENT_SECRET=
```

OAuth tokens for Jobber, QuickBooks, and Google are stored in their respective JSON files (`.jobber_tokens.json`, `.quickbooks_tokens.json`, `token.json`), not in `.env`.

## Common Tasks

**Run all Phase 1 tests:** `python tests/test_phase1.py -v`

**Run all Phase 2 tests:** `python tests/test_phase2.py -v`

**Run Phase 4 unit tests (no API calls):** `python tests/test_phase4.py -v -k "not live and not slack_channel"`

**Run Phase 4 with integration tests:** `RUN_INTEGRATION=1 python tests/test_phase4.py -v`

**Generate a briefing for a specific date:** `python -m intelligence.runner --skip-sync --date 2026-03-17`

**Dry-run the briefing (inspect context document without API call):** `python -m intelligence.runner --skip-sync --date 2026-03-17 --dry-run`

**Validate seeded data:** `python seeding/utils/validator.py`

**Verify planted discovery patterns:** `python seeding/generators/gen_anomalies.py`

**Check cross-tool mapping gaps:** `python -c "from db.mappings import find_unmapped; print(find_unmapped('jobber', 'CLIENT'))"`

**Resume an interrupted push:** Just re-run the pusher. The checkpoint system picks up where it left off.

## Skills Reference

When building new modules, read the relevant skill doc first:

- `docs/skills/tool-api-patterns.md` -- Auth patterns, endpoints, headers, rate limits, and error codes for all 8 tools. Read before writing any API call.
- `docs/skills/canonical-record.md` -- How to create SQLite records and register cross-tool mappings. Read before writing to the database.
- `docs/skills/generator-template.md` -- Boilerplate class for simulation generators. Copy and fill in. Read before creating a new generator.
- `docs/skills/project-conventions.md` -- Import paths, naming rules, testing patterns, and error handling. Read at session start.
- `docs/skills/weekly-report.md` -- Report structure, data analysis standards, confidence levels, citation formatting, and insight repetition rules. Read before building or modifying the weekly report generator.