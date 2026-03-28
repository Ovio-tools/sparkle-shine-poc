# Automation POC — Lessons Learned

**Project:** Sparkle & Shine POC (OVIO Digital)
**Date:** 2026-03-26
**Scope:** Applicable to any multi-tool automation or SaaS integration project

This document captures lessons learned from building a full-stack automation POC that connected 8 SaaS platforms with a seeded SQLite database, cross-tool automation workflows, and an AI-powered reporting pipeline. Use this as a checklist and reference for future automation projects.

---

## 1. Data Modeling & Spec Design

### L1 — Benchmark domain values before writing config or generation code

When configuring financial models, pricing tiers, or any domain-specific values, do not rely on intuition. Commercial cleaning contracts were priced at $75–$200/visit when realistic market rates are $450–$1,100/visit — a 5–10× underestimate that propagated through every downstream calculation.

**Rule:** For any pricing or financial model, gather 3–5 real-world reference data points *before* writing config values. Document the source inline in the config file.

---

### L2 — Verify spec metrics are mathematically achievable given the data distributions you also require

A success metric of "referral LTV ≥ 1.5× Google Ads LTV" failed because the same spec distributed 60% of referral clients to the final 5 months of a 12-month window (short tenure), while Google Ads clients were weighted toward the first 3 months (long tenure). Tenure dominates LTV. No code fix can resolve a self-contradiction in the spec.

**Rule:** Before finalising any success metric, trace through the math end-to-end. If the metric is a function of a distribution, confirm that the required distribution produces the required metric value. If they conflict, fix the spec — not the code.

---

### L3 — Align performance targets with the lifecycle stage being simulated

Revenue targets were written to match a fully-mature book of business ($135K–$185K/month), but the narrative begins with 60 clients ramping to 240+ over 12 months. The result was 11/12 months failing targets — not a code bug, but a mismatch between the ramp-up model and static absolute targets.

**Rule:** Write separate "ramp-up" and "mature" target bands when simulating business growth. Alternatively, express targets as a percentage of theoretical maximum capacity rather than absolute values.

---

## 2. Data Generation & Seeding Architecture

### L4 — Separate data generation (Stage A) from API pushing (Stage B)

Using SQLite as an intermediate layer between generation scripts and API pushers enables full data validation before any external calls are made. Pricing errors, volume discrepancies, and pattern failures are all catchable at this layer — cheaply, without involving external rate limits or API state.

**Rule:** Gen scripts write to the local database only. Push scripts read from the local database only. Never mix these concerns. Run your full validation suite against the database before starting any push.

---

### L5 — Fix root causes in generation code, not just in the database

Direct SQL patches are acceptable as a temporary diagnostic or emergency repair. They are not the canonical fix. If you patch the database without fixing the generator, the next regeneration reintroduces the bug.

**Rule:** The canonical fix lives in the generation code. Direct SQL patches are technical debt. After applying a SQL patch, immediately plan the source fix and regenerate to confirm. Document both in a status file.

---

### L6 — Verify planted patterns after every significant data change

Invoice recalculation changed lifetime value fields for hundreds of clients. Two of the seven planted discovery patterns degraded or reversed as a side effect — not visible without running the anomaly verification script.

**Rule:** After any data regeneration or bulk update, run the pattern verification script (equivalent of `gen_anomalies.py`) as a required checklist step. Treat pattern degradation as a test failure, not a cosmetic issue.

---

### L7 — Use deterministic seeds for all random data generation

`random.seed(42)` at the entry point of every generator, combined with a seeded faker instance, ensures that identical inputs always produce identical outputs. Regenerations are reproducible and debuggable.

**Rule:** Set global random seeds at the entry point of every generator. Document the seed value in your master context file. Never use `random` or `uuid` without a seed in generation code.

---

## 3. API Integration Architecture

### L8 — Build rate limiting infrastructure before writing any API pusher

Each tool has a different safe request rate (e.g., Jobber: ~10 req/sec, Asana: 150 req/min, Slack: 1 msg/sec). Hardcoding delays inline scatters magic numbers across every file. A centralized `Throttler` class with per-tool presets keeps this maintainable and prevents mid-push 429 errors.

**Rule:** Create `utils/throttler.py` with per-tool safe rates before writing the first pusher. All pushers import and use the pre-configured throttler for their tool. Never hardcode `time.sleep()` with a raw number.

---

### L9 — Cross-tool ID registration must be atomic with record creation

In polling-based automations, a race condition exists: if a record is created in Tool A but the local mapping table is not updated before the next poll cycle, the automation will attempt to create it again, minting a duplicate canonical ID.

**Rule:** The invariant is: (1) reserve canonical ID, (2) create record in external tool, (3) register mapping in local database — all within a single try/except block. Never defer step 3 to a later function or a cleanup pass.

---

### L10 — Implement checkpoint/resume before running any long API push

A multi-hour push (e.g., 2.5–3 hours for Jobber) that fails at record 800 of 1,000 should restart from record 801 — not record 1. Without checkpointing, failures waste hours and risk creating partial state in the external tool.

**Rule:** `checkpoint.py` must exist and be wired into every pusher before the first real push runs. Save checkpoints every 25–100 records (tune to the tool's volatility). A re-run of the pusher should always be idempotent.

---

### L11 — Design a unified auth interface before writing any integration code

Each tool uses a different auth mechanism: API key headers, OAuth 2.0 with auto-refresh, and Desktop OAuth flows. Code that imports tool-specific auth modules directly becomes tightly coupled and breaks when tokens rotate.

**Rule:** Write a `get_client(tool_name)` abstraction on Day 1 that returns an authenticated session or client for any tool. All integration code calls `get_client()`. No file outside the auth layer should import a tool-specific auth module.

---

### L12 — OAuth tokens belong in mutable JSON files, not in .env

OAuth tokens include refresh tokens and expiry timestamps that are updated programmatically on each refresh. `.env` files are static. Storing mutable token state in `.env` causes race conditions and makes token refresh impossible without side effects.

**Rule:** `.env` holds client IDs, client secrets, and static API keys only. OAuth token state (access token, refresh token, expiry) lives in `.{tool}_tokens.json` files that are git-ignored and updated in place by the auth layer.

---

## 4. Automation Design

### L13 — Automations must fail gracefully at the individual tool level

In a multi-tool automation, a failure in one tool (e.g., Asana task creation) must not prevent the workflow from completing steps in other tools (e.g., Pipedrive deal update, Slack notification). Unhandled exceptions that bubble up from tool-specific code halt the entire automation.

**Rule:** Wrap each external tool call in its own try/except block. On failure: log the error with tool name, entity ID, and error message, then continue. One tool being down should never take down a workflow step for a different tool.

---

### L14 — Always check the mapping table before creating a record in any external tool

Polling-based automations re-examine the same trigger conditions repeatedly. Without a prior-creation check, each poll cycle risks creating duplicate records in external tools.

**Rule:** Every automation creation path must start with: query mapping table for existing entry → if found, skip creation and proceed → if not found, create and register. "Create or skip" is the invariant. This check must happen inside the automation, not in the trigger condition.

---

### L15 — Build --dry-run mode before building live execution mode

`--dry-run` allows inspecting what an automation would do — logging decisions, context documents, trigger conditions — without making any API calls or database mutations. This is the primary tool for verifying automation logic before enabling live execution.

**Rule:** `--dry-run` is not optional polish. Build it into every runner and pusher as the default mode. Only enable live execution after dry-run output has been reviewed and confirmed correct.

---

## 5. Scheduling & Deployment

### L16 — Test automated schedulers in their exact execution context

Scheduled jobs (launchd, cron, systemd) run in a stripped-down environment with a different working directory, limited PATH, and fewer environment variables than an interactive shell. A script that works fine when run manually may fail silently when launched by a scheduler.

**Rule:** Test schedulers using their actual launch mechanism — not by running the script manually. Use absolute paths everywhere in scheduled scripts. Capture stderr to a log file. After first deployment, verify the log file to confirm the scheduled run succeeded.

---

## 6. Testing Strategy

### L17 — Gate integration tests behind an environment variable flag

Live API integration tests are slow, consume rate limit quota, and can mutate external state. Mixing them into the default test run breaks fast feedback loops.

**Rule:** Use `@unittest.skipUnless(os.getenv("RUN_INTEGRATION"), "Skipping")` for all tests that make real API calls. Default `pytest` run = unit tests only (fast, offline). `RUN_INTEGRATION=1 pytest` = full suite. Document both commands in the master context file.

---

### L18 — Structure tests per project phase, not per domain entity

Organizing test files by phase (auth, seeding, automations, intelligence) makes it easy to validate each phase independently as the project grows. A single monolithic test file becomes unmanageable and slow.

**Rule:** Each build phase gets its own test file: `test_phase1.py`, `test_phase2.py`, etc. Each file verifies both correctness (are the right records present?) and narrative consistency (do the counts and values match the spec?).

---

## 7. Documentation & Project Memory

### L19 — Write your master context file before writing any implementation code

The master context file (`CLAUDE.md`) is the contract between all sessions working on the project. It should contain: project purpose, tool stack with auth method per tool, rate limits, key API endpoints, cross-tool ID format, code conventions, common task commands, and the build phase map. Without it, each new session rediscovers the same conventions independently.

**Rule:** Create `CLAUDE.md` (or equivalent) before the first line of implementation code. Update it whenever a convention changes. It is not a one-time artifact — it is a living contract. Any new session or contributor reads it first.

---

### L20 — Embed the canonical ID in every record you create in an external tool

When debugging a multi-tool system, the most common question is: "Which database record does this external record correspond to?" If the canonical ID is embedded in the record's notes or a custom field at creation time, this question is answered instantly without a database join.

**Rule:** When creating records via API, always write the canonical ID into the record's `notes`, `description`, or a dedicated custom field (e.g., `"SS-ID: SS-CLIENT-0047"`). This costs one field per record and eliminates tracing ambiguity in every future debugging, audit, or migration session.

---

## Quick Reference Checklist

Use this before starting any new automation project:

**Spec Review**
- [ ] All domain-specific values benchmarked against real-world data
- [ ] All success metrics traced through to confirm they are mathematically achievable
- [ ] Target bands defined per lifecycle stage (ramp-up vs. mature)

**Architecture Setup (before writing integration code)**
- [ ] `utils/throttler.py` created with per-tool safe rates
- [ ] `utils/checkpoint.py` created and wired into all pushers
- [ ] Auth abstraction (`get_client(tool_name)`) written and tested
- [ ] `.env` vs. token JSON file split established and git-ignore verified
- [ ] Local database schema defined as the single source of truth
- [ ] Cross-tool canonical ID format defined (e.g., `PROJECT-TYPE-NNNN`)

**Generation Pipeline**
- [ ] Random seeds set at entry point of every generator and documented
- [ ] Pattern verification script exists and runs clean before any push
- [ ] Validation suite runs against local database before any API push

**Automation Design**
- [ ] `--dry-run` mode implemented before live execution mode
- [ ] Every automation creation path includes a mapping table lookup
- [ ] Each tool call wrapped in its own try/except with structured logging

**Testing**
- [ ] Integration tests gated behind `RUN_INTEGRATION` flag
- [ ] Test file per build phase, covering both correctness and narrative consistency

**Documentation**
- [ ] `CLAUDE.md` written and covers: purpose, tool stack, auth, rate limits, ID format, conventions, common commands
- [ ] Canonical ID embedded in notes/metadata field for every external record created
