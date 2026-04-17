# Revenue Remediation Plan

Date: 2026-04-16
Scope: Revenue reporting mismatch, invoice data integrity, billing normalization, payment timing, and commercial scheduling gaps in production.

## Executive Summary

The April 2026 revenue shortfall is being overstated by the current reporting stack.

There are four distinct issues:

1. The intelligence layer compares monthly revenue goals to cash collected, not revenue earned.
2. Production invoice data is polluted by orphan QuickBooks invoice imports, especially a large spike on 2026-04-09.
3. The job completion invoicing flow is mispricing some service types because it keys off free-text labels instead of canonical service IDs.
4. Production has 40 active commercial clients but 0 active commercial recurring agreements, which means commercial work is being scheduled through a fallback path instead of the primary recurring scheduling model.

The plan below is organized into immediate containment, data repair, logic fixes, and structural commercial scheduling remediation.

## Current State Snapshot

- April 15, 2026 cash collected: $1,650
- April 1-15 cash collected: $23,905
- April 1-15 completed jobs: 320
- April 1-15 job-linked invoiced amount: $53,574.90
- April 1-15 invoices in production: 2,077
- April 1-15 invoices with no `job_id`: 1,384
- April 9, 2026 invoices: 1,783 totaling $269,038.47
- Active commercial clients: 40
- Active commercial recurring agreements: 0

## Root Causes

### 1. Metric Semantics

`intelligence/metrics/revenue.py` uses the `payments` table for daily, weekly, and MTD "revenue". This is actually cash collection, not earned revenue.

Impact:
- Daily and MTD pacing look far worse than actual booked work.
- Commercial net-30 behavior is treated like lost revenue rather than expected collection lag.

### 2. Invoice Data Integrity

`intelligence/syncers/sync_quickbooks.py` can materialize local invoices without linking them to jobs when it sees invoices in QuickBooks that do not already have local mappings.

Impact:
- Invoice-based analytics are inflated by orphan records.
- The 2026-04-09 spike strongly suggests a reconciliation/import event inserted many invoices without `job_id`.

### 3. Billing Normalization Bug

`automations/job_completion_flow.py` prices invoices using a free-text `service_type` string from the Jobber trigger event. The rest of the system primarily uses canonical IDs like `deep-clean`, `recurring-weekly`, and `recurring-monthly`.

Impact:
- `deep-clean` jobs are being billed at the fallback $150 instead of $275.
- Most `recurring-weekly` jobs are being billed at $150 instead of $135.
- Most `recurring-monthly` jobs are being billed at $150 instead of $165.

### 4. Commercial Scheduling Architecture Gap

`simulation/generators/operations.py` explicitly bypasses `recurring_agreements` for commercial clients and schedules them from `clients.notes` instead.

Impact:
- Production has no active commercial recurring agreements even though commercial clients are active.
- Commercial scheduling depends on a fragile notes-driven fallback path.
- Commercial capacity and forecastability are weak because the recurring workload is not represented in the same canonical scheduling model as residential recurring work.

## Remediation Tracks

## Track A: Reporting and KPI Semantics

Priority: P0
Owner: Intelligence / Analytics

### Goals

- Stop labeling cash as revenue.
- Add side-by-side booked revenue and cash collected metrics.
- Compare monthly targets against booked revenue by default.

### Actions

1. Rename current `revenue` outputs sourced from `payments` to `cash_collected`.
2. Add `booked_revenue` metrics sourced from job-linked invoices or completed jobs.
3. Update daily and weekly briefings to show:
   - jobs completed
   - booked revenue
   - cash collected
   - AR / overdue totals
4. Change pacing logic so monthly targets compare to booked revenue, not payments.
5. Add a separate cash collection pacing section for finance-specific monitoring.
6. Migrate all downstream consumers of the old `revenue` key in one pass and gate the rename behind a compatibility shim until every consumer is updated. Known consumers:
   - `intelligence/context_builder.py`
   - `intelligence/briefing_generator.py`
   - `intelligence/weekly_report.py`
   - `intelligence/metrics/__init__.py` exports
   - Any template, snapshot, or fixture that reads `revenue` as a top-level key
   Ship the shim and the rename in the same PR so stale consumers cannot silently read cash as booked revenue, or vice versa.

### Acceptance Criteria

- Daily briefing no longer says "revenue collected" when it means cash.
- MTD target pacing uses booked revenue.
- Cash and earned revenue can be compared side by side for the same date range.
- No consumer of `metrics.revenue` reads the legacy key after the rename lands; grep for `"revenue"` in intelligence modules returns only the renamed usages or the compatibility shim.

## Track B: Production Data Containment and Repair

Priority: P0
Owner: Data Integrity / Backend

### Goals

- Prevent orphan invoices from distorting analytics.
- Repair or quarantine existing orphan invoice rows.

### Actions

1. Immediately exclude `invoices.job_id IS NULL` rows from intelligence booked-revenue analytics.
2. Investigate the 2026-04-09 invoice spike and identify the exact import or sync run that generated it.
3. Extend the existing `scripts/remediate_reconciliation_invoices.py` (already committed; do not create a new script) to:
   - relink orphan local invoices to jobs where possible
   - create missing local invoice links when a QBO invoice already exists
   - preserve ambiguity safeguards when multiple candidates match
4. Add a one-time audit query/report for:
   - orphan invoices by day
   - orphan invoice amount by day
   - invoices with QBO mapping but no job
   - invoices linked to clients with no corresponding completed job on the issue date
5. Add a recurring integrity check in CI or daily ops reporting:
   - count of invoices with null `job_id`
   - count of payments with missing invoice link
   - count of jobs completed with no linked invoice after 24 hours

### Acceptance Criteria

- Orphan invoice count is reduced to near zero for recent production dates.
- The 2026-04-09 anomaly is either repaired or explicitly quarantined from metrics.
- Booked revenue dashboards exclude unlinked invoices unless explicitly requested.

## Track C: Billing and Service-Type Normalization

Priority: P0
Owner: Backend / Automations

### Goals

- Ensure invoices use canonical service IDs for pricing.
- Eliminate fallback pricing for known service types.

### Actions

1. Refactor `automations/job_completion_flow.py` so invoice pricing is driven by canonical `service_type_id`, not free-text service labels.
2. Add a normalization layer that maps Jobber event titles to canonical IDs before invoice pricing.
3. Reuse the canonical mappings already present in the sync layer where possible.
4. Add tests covering all standard service types:
   - `std-residential`
   - `deep-clean`
   - `move-in-out`
   - `recurring-weekly`
   - `recurring-biweekly`
   - `recurring-monthly`
   - `commercial-nightly`
5. Add a guardrail that logs and alerts whenever fallback pricing is used for a recognized service category.
6. Repair recent mispriced invoices via a committed migration script under `scripts/` (per project rule L5 — never patch simulation-generated data with one-off SQL). The script must:
   - Identify mispriced invoices by canonical service ID and issue date window
   - Update both the local `invoices` row and the QBO invoice via API, keyed on `cross_tool_mapping`
   - Skip invoices already paid in full (see policy below)
   - Support `--dry-run` and print a per-service-type delta summary before applying
7. Define an explicit policy for already-paid mispriced invoices. The plan assumes forward-only repair is always safe; it is not. Choose one per service type and document the decision inline:
   - Forward-only: accept the historical underbilling, do not re-invoice paid records
   - Re-bill: issue a supplemental invoice for the delta
   - Write-off: accept the loss and log it for finance reporting

   **Decision (2026-04-16):** All seven canonical service types are **forward-only** by default. Rationale:
   - Residential (std-residential, deep-clean, move-in-out, recurring-weekly/biweekly/monthly): re-billing a paid residential customer creates a support burden that dwarfs the recovered delta ($15-125 per visit).
   - Commercial (commercial-nightly): contract rates are per-agreement; any paid-invoice delta is a contract-interpretation issue, not a data-entry issue, and should go to finance before any re-bill.

   Operators can override the default on a per-run basis via `scripts/remediate_mispriced_invoices.py --reprice-paid`, which repricess paid invoices locally and in QBO and logs a WARNING per affected record. That override should only be used after finance has approved the specific invoice set. The policy is enforced in code by the `_PAID_POLICY` dict in the remediation script; update that dict (not just this doc) if the default changes.
8. Add a fallback-only test for `commercial-nightly`. Commercial pricing is per-contract and has no base price in the catalogue; the test should assert that completion events for `commercial-nightly` use the contract rate from `recurring_agreements`, never the fallback constant.

### Acceptance Criteria

- New deep-clean invoices are priced at $275.
- New recurring-weekly invoices are priced at $135.
- New recurring-monthly invoices are priced at $165.
- Fallback pricing is only used for truly unknown service types.
- Mispriced-invoice repair runs from a committed `scripts/remediate_*.py` file, not ad-hoc SQL.
- Historical paid invoices are handled per the documented policy, not silently rewritten.

## Track D: Payment Timing and Cash Realism

Priority: P1
Owner: Simulation / Finance Logic

### Goals

- Align payment timing with invoice terms and briefing language.

### Actions

1. Revisit the payment timing windows in `simulation/generators/payments.py`.
2. For residential "due on receipt" invoices, move most payments to same-day or 0-3 day windows.
3. Keep slower behavior for commercial net-30 clients.
4. Update docs and briefing language so operational users understand:
   - booked revenue
   - cash collected
   - overdue receivables

### Sequencing Note

Track D must land after Track A's booked-revenue split is in production and validated for at least one full week of briefings. Tightening payment timing before the metric rename ships will move the `revenue` number upward for the wrong reason — it will look like Track A's fix worked, when in reality the payment distribution shifted underneath. Hold Track D until cash and booked revenue are reporting side-by-side with stable values.

### Acceptance Criteria

- Same-day residential work produces materially higher same-week cash realization.
- Cash pacing is still meaningfully distinct from booked revenue, but not misleadingly depressed.
- Track A has been live for at least 5 business days before Track D changes are merged.

## Track E: Commercial Recurring Agreement Remediation

Priority: P0
Owner: Operations Simulation / Data Integrity

### Goals

- Fix the production state where 40 active commercial clients have 0 active commercial recurring agreements.
- Move commercial scheduling onto the same canonical recurring model used elsewhere, or maintain a durable mirrored agreement table if the fallback path must remain.

### Actions

1. Add a production audit for commercial clients:
   - active commercial clients
   - active commercial recurring agreements
   - latest scheduled/completed job date
   - notes-derived schedule scope
   - QBO customer mapping
   - Jobber client/property mapping
2. Create a backfill script that generates `recurring_agreements` rows for active commercial clients based on canonical schedule rules.
   Inputs should include:
   - company
   - schedule type from notes or known seeded commercial profile
   - per-visit rate
   - crew assignment
   - day-of-week or cadence
   - client type = `commercial`

   The script must define an explicit uniqueness key to prevent duplicate agreements on rerun or partial rollout. Recommended key: `(client_canonical_id, service_type_id, day_of_week, active=true)`. Enforce it with a DB-level unique index where possible, or a pre-insert `SELECT` guard otherwise. Without this, a second run — or a notes-based path running alongside — will silently duplicate agreements and double-schedule jobs.
3. Preserve existing scheduling behavior during rollout by:
   - feature-flagging commercial scheduling source
   - preventing duplicate jobs if both notes-based and agreement-based paths are active
4. Refactor `simulation/generators/operations.py` so commercial recurring work reads from `recurring_agreements` first.
5. Keep the notes-based scheduling path only as a temporary compatibility fallback.
6. Add a reconciliation script to identify commercial clients that are:
   - active but unscheduled
   - scheduled via notes but missing an agreement
   - represented in agreements but have no recent jobs
7. Add a regression test asserting that active commercial clients should not drop to zero active recurring agreements after setup/sync flows.

### Acceptance Criteria

- Active commercial clients in production have corresponding active commercial recurring agreements unless explicitly marked one-time.
- Commercial job scheduling can run from canonical agreement records.
- The notes-based fallback is either removed or clearly secondary.

## Recommended Execution Order

### Phase 0: Immediate Containment

1. Rename/report cash vs booked revenue correctly.
2. Exclude orphan invoices from booked-revenue analytics.
3. Add visibility for commercial clients missing recurring agreements.
4. **Blocker before Phase 1:** Diagnose the 2026-04-09 invoice spike. Identify the exact sync/import run, the source QBO query, and whether the spike represents real historical invoices or a reconciliation artifact. Remediation strategy in Phase 1 depends on this answer — relinking is appropriate for real historical invoices, quarantine or deletion is appropriate for an import bug. Do not proceed with Track B repair actions until this diagnosis is complete and documented.

### Phase 1: Repair and Guardrails

1. Run invoice-link remediation on production.
2. Add integrity monitors for orphan invoices and missing job links.
3. Fix service-type normalization and pricing tests.

### Phase 2: Structural Commercial Fix

1. Backfill commercial recurring agreements.
2. Switch commercial scheduling to canonical agreements behind a flag.
3. Validate no duplicate job creation.

### Phase 3: Calibration

1. Recalibrate payment timing.
2. Reassess April-May revenue targets once metrics and commercial scheduling are trustworthy.

## Risks

- Repairing orphan invoices without strong matching rules could create incorrect job links.
- Backfilling commercial recurring agreements without a rollout flag could double-schedule jobs.
- Repricing logic changes may expose historical invoice inconsistencies that need business review.

## Validation Queries to Run After Each Phase

1. Count of invoices with `job_id IS NULL`
2. Count of completed jobs older than 24h with no linked invoice
3. Count of active commercial clients with no active commercial recurring agreement
4. Booked revenue vs cash collected by day for the current month
5. Invoice amount distribution by `service_type_id`

## Ownership, Target Dates, and Rollback

Each track needs a named owner, a target merge date, and a rollback plan. Fill this in before starting execution; a plan without these is a wish list.

| Track | Owner | Target Merge | Rollback |
|-------|-------|--------------|----------|
| A — Reporting / KPI Semantics | TBD | TBD | Revert metric rename PR; compatibility shim keeps legacy consumers functional |
| B — Data Containment / Repair | TBD | TBD | Remediation script is additive (relinks rows); restore from pre-run snapshot of `invoices` if relinking is wrong |
| C — Billing Normalization | TBD | TBD | Revert `_SERVICE_CATALOGUE` / `_SERVICE_ALIASES` changes; repriced invoices stay repriced (no auto-rollback of financial records) |
| D — Payment Timing | TBD | TBD | Revert `simulation/generators/payments.py` distribution change |
| E — Commercial Recurring Agreements | TBD | TBD | Feature flag off; backfill script dry-run output retained; agreements can be soft-deleted via `active=false` |

Rollback for financial records (Tracks B and C) is inherently partial — once an invoice is repriced in QBO, undoing it requires a credit memo, not a revert. Plan accordingly.

## Definition of Done

The remediation is complete when:

- reporting distinguishes booked revenue from cash collected
- orphan QuickBooks invoice imports no longer distort production metrics
- invoice pricing matches the service catalog
- active commercial clients are represented by active commercial recurring agreements
- commercial jobs are scheduled from canonical agreement data
- monthly target pacing reflects operational reality instead of data/model artifacts
