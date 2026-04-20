# Production Quarantine Plan for Residual Orphan Invoices

**Date:** 2026-04-19
**Scope:** Railway production orphan invoices that remain after removing the
obvious duplicate-shaped cohort from the 2026-04-09 spike.

## Goal

Safely quarantine the residual orphan-invoice bucket without:

- deleting invoice rows
- writing back to QuickBooks
- re-linking invoices to the wrong jobs
- changing booked-revenue analytics behavior

This plan is intentionally conservative. The residual bucket does not match
the current relinker's job-link assumptions, so the safest move is to freeze
it operationally, document it, and keep it excluded from booked revenue until
there is stronger identity evidence.

## Current Prod Facts

From the 2026-04-19 Railway production audit:

- Total orphan invoices: `1,393` totaling `$208,800`
- Likely duplicate-shaped orphans:
  `854` totaling `$128,100`
  Definition: same `client_id + issue_date + amount` as at least one
  job-linked invoice.
- Residual orphan bucket:
  `539` totaling `$80,700`
  Definition: orphan invoices with no job-linked invoice on the same
  `client_id + issue_date + amount`.

The residual `539` then splits into:

- `64` residuals with a same-day completed job whose job is already invoiced
- `475` residuals with no same-day completed job

This document covers the `475` bucket only.

### 475-Bucket Breakdown

- `465` on `2026-04-09` totaling `$69,750`
- `9` on `2026-03-19` totaling `$1,215`
- `1` on `2026-04-14` totaling `$135`

Other properties:

- `473` of `475` have a QuickBooks mapping
- `2` do not have a QuickBooks mapping:
  `SS-INV-4625`, `SS-INV-4626`
- `470` of `475` have no completed job within `3` days
- only `5` of `475` have any completed job within `3` days

Interpretation:
the bucket is overwhelmingly "no obvious local job counterpart" rather than
"missing backlink to an eligible completed job."

## Quarantine Definition

An invoice is considered **quarantined** when all of the following are true:

1. The invoice row remains in `invoices` with `job_id IS NULL`.
2. No remediation script is allowed to auto-link it to a job.
3. The row remains excluded from booked revenue by the existing
   `revenue.py` defensive filters.
4. The invoice ID is recorded in a durable quarantine artifact so operators
   do not repeatedly revisit it as a fresh relink candidate.
5. Any future promotion out of quarantine requires stronger evidence than
   `(client_id, issue_date, amount)` alone.

Important:
for this codebase, quarantine is an **operational classification**, not a
destructive database action. We are not deleting the invoice, voiding it in
QBO, or changing historical analytics beyond the exclusion that already
exists.

## Safety Rules

The quarantine run must obey all of these constraints:

- Never update `invoices.job_id` for the `475` bucket.
- Never delete `invoices` rows.
- Never void, delete, or patch QBO invoices as part of quarantine.
- Never change booked-revenue query logic; it is already safe.
- Never reuse the current `--mode orphans --execute` script against this
  bucket unless a stronger match key lands first.
- Always capture a pre-run snapshot of the candidate IDs before creating any
  durable quarantine artifact.

## Recommended Quarantine Artifact

The safest durable implementation is an additive artifact, not a mutation to
existing finance tables.

Preferred options, in order:

1. **Repo-tracked evidence file**
   Store the candidate list and rationale in a dated CSV or markdown appendix.
   This is the safest immediate option because it is read-only from the
   application's point of view.

2. **New additive table**
   If durable DB state is needed, add a dedicated table such as
   `invoice_quarantine` with fields:
   - `invoice_id`
   - `reason_code`
   - `reason_detail`
   - `reviewed_at`
   - `reviewed_by`
   - `source`
   - `released_at`
   - `released_by`

   This table should reference `invoices(id)` and must not alter
   `invoices.job_id`.

3. **Temporary export only**
   Acceptable for same-day ops coordination, but weaker than option 1 or 2
   because future operators may not discover it.

## Reason Codes

Use the following reason codes for this bucket:

- `NO_SAME_DAY_COMPLETED_JOB`
  No same-day completed job exists for the same client.
- `NO_NEARBY_COMPLETED_JOB_3D`
  Stronger quarantine signal; no completed job exists within `3` days.
- `NO_QBO_MAPPING`
  Residual orphan has no QuickBooks mapping and should be treated as a local
  anomaly.
- `MANUAL_REVIEW_NEARBY_JOB`
  Residual orphan has no same-day completed job but does have a nearby
  completed job within `3` days; hold for manual review before finalizing.

## Execution Plan

### Phase 0: Freeze the relink path for this bucket

- Mark the `475` bucket as "do not relink with current tooling."
- Do not run `scripts/remediate_reconciliation_invoices.py --mode orphans
  --execute` against this set.
- Continue using the integrity alert as visibility, not as a signal to
  relink this bucket automatically.

### Phase 1: Snapshot the candidate set

Export the exact candidate IDs from prod and archive them.

Selection query:

```sql
WITH residual_no_same_day AS (
  SELECT o.id, o.client_id, o.issue_date, o.amount,
         m.tool_specific_id AS qbo_invoice_id
  FROM invoices o
  LEFT JOIN cross_tool_mapping m
    ON m.canonical_id = o.id
   AND m.tool_name = 'quickbooks'
  WHERE o.job_id IS NULL
    AND NOT EXISTS (
      SELECT 1
      FROM invoices i
      WHERE i.client_id = o.client_id
        AND i.issue_date = o.issue_date
        AND i.amount = o.amount
        AND i.job_id IS NOT NULL
    )
    AND NOT EXISTS (
      SELECT 1
      FROM jobs j
      WHERE j.client_id = o.client_id
        AND j.status = 'completed'
        AND (
             j.completed_at::date = o.issue_date::date
          OR j.scheduled_date::date = o.issue_date::date
        )
    )
)
SELECT *
FROM residual_no_same_day
ORDER BY issue_date DESC, id;
```

Expected count on 2026-04-19: `475`.

### Phase 2: Split into auto-quarantine and manual-review lanes

Sub-bucket A: **Auto-quarantine**

- Criteria:
  - no same-day completed job
  - no completed job within `3` days
- Expected count: `470`

Sub-bucket B: **Manual-review holdout**

- Criteria:
  - no same-day completed job
  - at least one completed job within `3` days
- Expected count: `5`

Manual-review holdouts observed on 2026-04-19:

- `SS-INV-6458` / `SS-CLIENT-0071` / `2026-04-14` / `$135`
- `SS-INV-4698` / `SS-CLIENT-0139` / `2026-04-09` / `$150`
- `SS-INV-5075` / `SS-CLIENT-0139` / `2026-04-09` / `$150`
- `SS-INV-5446` / `SS-CLIENT-0139` / `2026-04-09` / `$150`
- `SS-INV-5805` / `SS-CLIENT-0139` / `2026-04-09` / `$150`

These five should not be auto-relinked, but they are the only residuals that
justify a second pass if stronger evidence later becomes available.

### Phase 3: Record the quarantine

If using a DB table:

- insert one row per invoice into `invoice_quarantine`
- include `reason_code`
- include `reviewed_by` and `reviewed_at`
- preserve the export file used to create the rows

If using a repo-tracked artifact:

- store a dated CSV with:
  - `invoice_id`
  - `client_id`
  - `issue_date`
  - `amount`
  - `qbo_invoice_id`
  - `reason_code`
  - `notes`

Recommended reason-code assignment:

- `470` rows: `NO_NEARBY_COMPLETED_JOB_3D`
- `3` rows for client `SS-CLIENT-0139` plus `1` more duplicate row and
  `SS-INV-6458`: `MANUAL_REVIEW_NEARBY_JOB`
- `SS-INV-4625`, `SS-INV-4626`: `NO_QBO_MAPPING`

### Phase 4: Operator communication

Update the diagnosis doc and ops channel with:

- quarantined count
- manual-review holdout count
- explicit note that booked revenue was already protected before quarantine
- explicit note that no QBO writes were performed

### Phase 5: Ongoing handling

For future daily ops:

- treat quarantined IDs as known residuals, not fresh relink targets
- keep them visible in the integrity appendix
- do not escalate them as a new revenue distortion incident unless the count
  changes materially

## Rollback

Rollback should be trivial because quarantine is additive.

If using a repo artifact:

- remove the file or replace it with a corrected export

If using a DB table:

- delete rows from `invoice_quarantine` for the affected `invoice_id`s

No rollback should ever require touching:

- `invoices.job_id`
- QBO invoices
- booked-revenue query logic

## Acceptance Criteria

The quarantine plan is complete when:

- the `475`-invoice candidate set is snapshotted and reproducible
- the `470` auto-quarantine rows are durably recorded
- the `5` manual-review holdouts are separately listed
- no invoice rows are deleted
- no orphan rows are relinked as part of quarantine
- booked-revenue analytics remain unchanged

## Recommended Next Step

Implement quarantine as a durable additive artifact first, then decide
whether the `5` manual-review holdouts justify a stronger relink workflow.
Do **not** spend engineering time broadening the current relinker for the
other `470` rows unless new identity signals are discovered.
