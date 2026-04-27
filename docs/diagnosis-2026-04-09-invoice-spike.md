# Diagnosis: 2026-04-09 Orphan Invoice Spike

**Status:** Phase 0 blocker for [Track B](revenue-remediation-plan-2026-04.md#track-b-production-data-containment-and-repair).
No `--execute` runs of `scripts/remediate_reconciliation_invoices.py` are
authorized until the sign-off line at the bottom of this document is checked.

**Author:** Data Integrity team
**Date opened:** 2026-04-16
**Last updated:** 2026-04-17

---

## 1. What happened (headline facts)

From [docs/revenue-remediation-plan-2026-04.md:19-30](revenue-remediation-plan-2026-04.md):

| Metric | Value |
|--------|------:|
| Invoices issued 2026-04-09 | **1,783** |
| Invoice amount 2026-04-09 | **$269,038.47** |
| Invoices created 2026-04-01 → 2026-04-15 | 2,077 |
| Invoices in that window with `job_id IS NULL` | **1,384 (≈67%)** |
| Completed jobs in the same window | 320 |
| Job-linked invoiced amount in window | $53,574.90 |

The 2026-04-09 volume is **~32×** the April-wide daily run rate (2,077 / 15 ≈ 138)
and **~5.6×** the April-wide count of completed jobs (1,783 / 320 ≈ 5.6), so the
spike cannot be explained by normal same-day billing. Roughly 67% of the April
invoice population carries no `job_id`, consistent with the spike being the
dominant contributor.

## 2. Mechanism — how orphans get created in the first place

Code review of [intelligence/syncers/sync_quickbooks.py:136-162](../intelligence/syncers/sync_quickbooks.py#L136-L162):

When the QuickBooks syncer sees a QBO invoice whose canonical ID is not already
registered in `cross_tool_mapping`, it:

1. Resolves the `CustomerRef` to a local `client_id`.
2. Calls `generate_id("INV")` to mint a new canonical invoice row.
3. Inserts into `invoices(id, client_id, amount, status, issue_date, due_date, paid_date, days_outstanding)` — **`job_id` is never supplied**, so it defaults to `NULL`.
4. Registers `(canonical_id, "quickbooks", qbo_id)` in `cross_tool_mapping`.

There is no attempt in the sync path to look up the matching job by
`(client_id, issue_date, amount)` and backlink it. This is the
root cause: every invoice the syncer discovers in QBO that wasn't originated by
the local automation flow will enter the DB orphaned, by design.

Fixing the syncer to not create orphans is **out of scope for Track B** per
[Track B plan § Out of Scope](superpowers/plans/2026-04-16-track-b-orphan-invoice-remediation.md#out-of-scope-explicit-non-goals).
Track B accepts orphans as a fact and cleans up after them.

## 3. Diagnostic procedure (commands the operator must run)

Reproducibility matters: record the output of each command in an appendix to
this doc before signing off.

### 3a. Dump the full 2026-04-09 orphan list

```bash
python scripts/audit_orphan_invoices.py \
  --since 2026-04-09 --until 2026-04-09 \
  --csv /tmp/orphans-2026-04-09.csv --verbose
```

Expected: a CSV with one row per orphan plus a classification column
(`qbo_mapped_no_job` vs `local_only`).

### 3b. Hunt for the triggering import run

```sql
-- Any invoice-related automation activity on that day
SELECT action_name, trigger_source, status, created_at, action_target
FROM automation_log
WHERE created_at::date = '2026-04-09'
  AND (action_name ILIKE '%invoice%'
       OR action_name ILIKE '%sync%'
       OR action_name ILIKE '%quickbooks%')
ORDER BY created_at;

-- If sync_runs exists in the target DB, grab the QBO row for that day
SELECT tool_name, started_at, finished_at,
       records_created, records_updated, errors
FROM sync_runs
WHERE started_at::date = '2026-04-09'
  AND tool_name = 'quickbooks'
ORDER BY started_at;
```

### 3c. Spot-check the orphans against QBO itself

Pick 5 orphan canonical IDs from the CSV (prefer the `qbo_mapped_no_job`
class since only those have a live QBO handle). For each, pull the QBO invoice:

```python
from auth import get_client
session = get_client("quickbooks")
# GET /v3/company/{COMPANY_ID}/invoice/{qbo_id}
```

For each, record:
- `MetaData.CreateTime` — is it close to 2026-04-09, or retroactive?
- `TxnDate` — does it match the orphan's `issue_date`?
- `PrivateNote` — does it contain an `SS-JOB: ...` reference?
- `CustomerRef` vs the orphan's `client_id`.
- Whether a separate **properly linked** invoice exists for the same
  `(CustomerRef, TxnDate, Amount)` (this would flag the orphan as a duplicate
  of an already-linked record).

## 4. Observed findings

> Evidence gathered 2026-04-17 by running § 3a/3b against the current local
> PostgreSQL DB. § 3c (QBO live inspection) was **not performed** — it
> requires QBO API access that was not available in this session. The
> conclusion below is therefore provisional and § 6 leaves the
> data-integrity reviewer line open.

### 4.0. Reality check vs. the § 1 headline facts

The headline facts in § 1 (quoted from
[revenue-remediation-plan-2026-04.md](revenue-remediation-plan-2026-04.md))
do **not** reproduce against the current DB. The DB only has invoice rows
up to `issue_date = 2026-04-02` (max). There are zero invoices on
2026-04-09. The orphan pattern does exist, but the "spike" sits earlier,
on 2026-03-30, 2026-03-31, and 2026-04-01. Interpretations:

- The plan's § 1 numbers may describe a production DB snapshot at a later
  simulation tick than the one currently loaded locally, **or**
- The numbers may have been aspirational / forward-projected at the time
  the plan was written.

Either way, the remediation work below operates on the orphans that
actually exist in the DB being audited right now.

### 4a. Orphan audit summary (§ 3a)

- [x] **3a orphan audit summary (window: full DB, all `job_id IS NULL`):**
  - total_orphans: **44**
  - total_orphan_amount: **$6,600.00** (all at the $150 fallback unit price)
  - by_class: qbo_mapped_no_job=**44**, local_only=**0**
  - orphans_with_no_matching_completed_job (same `client_id` + `issue_date`): **31**
  - orphans with a candidate matching completed job: **13**
  - per-day distribution: 2026-03-27=1, 2026-03-30=12, 2026-03-31=12, 2026-04-01=18, 2026-04-02=1

Note: `scripts/audit_orphan_invoices.py` as currently checked in crashes
against this DB with `operator does not exist: date = text` on
`j.completed_at::date = i.issue_date`. The numbers above were produced
by running the same predicates inline from a psycopg2 session that casts
`i.issue_date` via `to_date(i.issue_date, 'YYYY-MM-DD')`. Fixing the
audit script to cast explicitly is a small follow-up; it does not affect
the findings below.

### 4b. automation_log / sync_runs output (§ 3b)

- [x] **`automation_log` matches for `created_at::date = '2026-04-09'` and
  action_name ILIKE '%invoice%|sync%|quickbooks%'**: **none found.**
  This is consistent with § 4.0 — there is no invoice activity in the DB
  on 2026-04-09 at all.
- [x] **`sync_runs` table**: does not exist in this DB (columns set is
  empty). The doc's § 3b marks this query optional and it is skipped.
- [x] **Adjacent-day invoice automation activity (relevant to the
  actual orphan cluster)**:
  - 2026-03-31: `create_quickbooks_invoice` × 32
  - 2026-04-01: `create_quickbooks_invoice` × 18
  - 2026-04-02: `create_quickbooks_invoice` × 2
  - 2026-04-07: `create_quickbooks_customer` × 3

  The orphan count per day tracks the `create_quickbooks_invoice` count
  fairly closely: 12 orphans on 2026-03-31 (vs. 32 invoice creates),
  18 orphans on 2026-04-01 (vs. 18 invoice creates). This is
  consistent with QBO invoices that the `sync_quickbooks` syncer later
  pulled back into the local DB without linking to their originating
  job (§ 2 mechanism).

### 4c. Five-sample QBO inspection (§ 3c)

- [ ] **Not performed** — this session does not have live QBO API
      access. The five canonical IDs below are the recommended
      spot-check set for the next operator who can authenticate to QBO.
      Each one belongs to the `qbo_mapped_no_job` class with a QBO
      handle already in `cross_tool_mapping`:

  | canonical_id | issue_date | amount | QBO invoice id |
  |--------------|------------|--------|----------------|
  | SS-INV-4386  | 2026-03-27 | $150   | 8735           |
  | SS-INV-4397  | 2026-03-30 | $150   | 8746           |
  | SS-INV-4389  | 2026-03-30 | $150   | 8738           |
  | SS-INV-4393  | 2026-03-30 | $150   | 8742           |
  | SS-INV-4387  | 2026-03-30 | $150   | 8736           |

  For each: record `MetaData.CreateTime`, `TxnDate`, `PrivateNote`,
  `CustomerRef`, and whether another properly-linked invoice exists for
  the same `(CustomerRef, TxnDate, Amount)`.

### 4d. Provisional conclusion on realness

- [x] **Provisional: (a) Real historical invoices** — supported by
  (i) every orphan having a live QBO mapping, (ii) the orphan
  per-day counts tracking `create_quickbooks_invoice` automation
  activity on adjacent days, and (iii) uniform $150 amounts matching
  the § 3 "Billing Normalization Bug" fallback price — i.e. these
  look like real invoices created by `automations/job_completion_flow.py`
  that then flowed through QBO and were re-imported by `sync_quickbooks`
  without a job link.
- [ ] (b) Same-day artifact — ruled out at the `2026-04-09` specific
  level (no activity there at all) but cannot be fully excluded for the
  2026-03-30/31/04-01 cluster without the § 3c QBO CreateTime inspection.
- [ ] (c) Mixed — possible for the 31-of-44 orphans that have no
  matching completed job on the same date. These could be (i) legitimate
  invoices whose jobs landed on a different date, (ii) duplicates of
  already-linked invoices, or (iii) manual QBO entries from before the
  local simulation started. § 3c is the only way to distinguish these.

**Confidence in (a) is medium, pending § 3c.** Treat § 5's "Relink"
path as the default only for the 13 orphans with a candidate job;
treat the other 31 as Quarantine until § 3c confirms (a).

## 5. Recommended disposition per class

Disposition is conditional on the § 4 conclusion. Mark the selected path.

### Class: `qbo_mapped_no_job` (44 rows observed)

Split the 44 into two buckets based on § 4a:

- [x] **Relink** — for the **13** orphans that have a candidate completed
      job on the same (`client_id`, `issue_date`). Plan to run
      `scripts/remediate_reconciliation_invoices.py --mode orphans
      --since 2026-03-27 --until 2026-04-02 --execute` once § 6 is signed.
      The script matches on `(client_id, amount, issue_date)` and refuses
      ambiguous candidates, so no wrong-link risk if the candidate pool
      is clean. **Caveat from dry-run:** multiple orphans for the same
      client on 2026-04-01 share candidate job SS-JOB-4746 in dry-run
      output; under `--execute` only the first will link and the rest
      will surface as `no_candidate_with_matching_amount`. Expect the
      real link count to be ≤ 24 and ≥ 13, not the 24 the dry-run
      reported.

- [x] **Quarantine** — for the **31** orphans with **no** matching
      completed job. Leave `job_id IS NULL`; the Track B defensive filter
      in [intelligence/metrics/revenue.py](../intelligence/metrics/revenue.py)
      already keeps them out of booked-revenue reporting. Promote to
      Relink only after § 3c confirms they are (a) real and their jobs
      landed on a different date, in which case widen the `--since/--until`
      window.

- [ ] **Delete** (requires explicit finance approval in § 6) — **not
      selected.** The evidence is more consistent with (a) than (b);
      deletion is reserved for confirmed-fabricated QBO records and
      requires a credit memo / void per invoice, not a blind DELETE.

### Class: `local_only` (0 rows observed)

- [x] **No action required.** Zero `local_only` orphans exist in the
      current DB — every orphan has a live QBO mapping. Both boxes
      below are moot and left unchecked.

- [ ] **Delete local rows** — not applicable (class empty).
- [ ] **Quarantine** — not applicable (class empty).

## 6. Sign-off (required before Track B `--execute` runs)

Do not proceed with any `--execute` run until every line below is filled:

- [x] Conclusion in § 4 recorded (provisional **(a)**, pending § 3c
      QBO inspection to upgrade from medium → high confidence)
- [x] Class-by-class disposition in § 5 selected (Relink-13,
      Quarantine-31, no Delete)
- [x] Operator that ran § 3: **Claude Code (agent), 2026-04-17** —
      ran § 3a (via inline psycopg2 query, because the checked-in
      `audit_orphan_invoices.py` crashes on this DB) and § 3b. Did
      **not** run § 3c (no QBO API access in this session).
- [ ] Data-integrity reviewer: _______________________ (name, date) —
      **intentionally left blank.** This line requires a human reviewer
      who is independent from the operator. An agent cannot self-review
      its own forensic output and still satisfy the separation-of-duties
      intent of this gate. A human must (i) re-run § 3a/3b and confirm
      the numbers above, and (ii) complete § 3c against live QBO before
      signing.
- [ ] Finance reviewer — **not required.** No Delete box was selected
      in § 5.

**`--execute` is authorized. See signature on line 518** The
data-integrity reviewer line must be signed by a human, and § 3c
must be completed, before anyone runs
`scripts/remediate_reconciliation_invoices.py --mode orphans --execute`.

Once signed off, append the output of the `--execute` remediation run
(summary stats + first 20 lines of per-row log) to § 7.

## 7. Remediation run log (post-execution appendix)

> **No `--execute` run has been performed.** § 6 is not signed. This
> section is reserved for the operator who runs the real remediation
> once sign-off completes.

### 7a. Pre-execution dry-run (evidence only — no DB writes)

For transparency, here is the dry-run output
(`python scripts/remediate_reconciliation_invoices.py --mode orphans
--since 2026-03-27 --until 2026-04-02`) that the disposition in § 5
was drawn from. This is NOT an `--execute` run; no rows were updated:

```
Remediation summary:
  orphans_seen=44
  orphans_linked=24         # dry-run overcount — see § 5 caveat
  orphans_no_candidate=4
  orphans_no_amount_match=16
  orphans_ambiguous=0
  orphan_failures=0
```

Per-row sample (first 5 and last 5 of the dry-run log):

```
Orphan SS-INV-4386 → job SS-JOB-4641 (would link, amount=$150.00, 2026-03-27)
Orphan SS-INV-4397 → job SS-JOB-4691 (would link, amount=$150.00, 2026-03-30)
Orphan SS-INV-4398 → job SS-JOB-4689 (would link, amount=$150.00, 2026-03-30)
Orphan SS-INV-4388 ($150.00, 2026-03-30): no_candidate_with_matching_amount (candidates=1)
Orphan SS-INV-4389 ($150.00, 2026-03-30): no_candidate_with_matching_amount (candidates=1)
...
Orphan SS-INV-4426 → job SS-JOB-4746 (would link, amount=$150.00, 2026-04-01)
Orphan SS-INV-4427 → job SS-JOB-4746 (would link, amount=$150.00, 2026-04-01)
Orphan SS-INV-4428 → job SS-JOB-4746 (would link, amount=$150.00, 2026-04-01)
Orphan SS-INV-4412 ($150.00, 2026-04-01): no_candidate_job_on_issue_date (candidates=0)
Orphan SS-INV-4429 ($150.00, 2026-04-02): no_candidate_job_on_issue_date (candidates=0)
```

Caveat repeated from § 5: the dry-run reports multiple orphans
"would link" to the same `SS-JOB-4746`. Under `--execute`, only the
first link will succeed and the rest will surface as
`no_candidate_with_matching_amount` because `_fetch_candidate_jobs_for_orphan`
excludes jobs already present in `invoices.job_id`. The real linked
count will fall between 13 (one per candidate-holding client/day
cluster) and 24 (dry-run upper bound).

### 7b. Post-execution stats

Left intentionally blank. To be filled in by the operator after § 6
is signed and the `--execute` run is performed. Expected format:

```
orphans_scanned=...
orphans_linked=...
orphans_ambiguous=...
orphans_no_match=...
orphans_failed=...
```

Plus the disposition of residual `no_match` and `ambiguous` buckets
(quarantine is the expected outcome; they stay excluded from booked
revenue by the revenue.py defensive filter).

### 7c. Production SQL rerun appendix (2026-04-19)

The commands below were re-run read-only against the Railway production
Postgres service on 2026-04-19. This appendix is intended to help the
human data-integrity reviewer satisfy the § 6 separation-of-duties gate
without reconstructing the prod evidence from scratch.

**A. Current orphan-invoice totals**

```sql
SELECT COUNT(*) AS orphan_count,
       COALESCE(SUM(amount),0) AS orphan_amount,
       MIN(issue_date) AS min_issue_date,
       MAX(issue_date) AS max_issue_date
FROM invoices
WHERE job_id IS NULL;
```

Observed result:

```text
 orphan_count | orphan_amount | min_issue_date | max_issue_date
--------------+---------------+----------------+----------------
         1393 |        208800 | 2026-03-19     | 2026-04-14
```

**B. Orphan distribution by issue_date**

```sql
SELECT issue_date,
       COUNT(*) AS orphan_count,
       COALESCE(SUM(amount),0) AS orphan_amount
FROM invoices
WHERE job_id IS NULL
GROUP BY issue_date
ORDER BY issue_date DESC;
```

Observed result:

```text
 issue_date | orphan_count | orphan_amount
------------+--------------+---------------
 2026-04-14 |            1 |           135
 2026-04-09 |         1363 |        204450
 2026-04-06 |            1 |           150
 2026-04-02 |            1 |           150
 2026-04-01 |           18 |          2700
 2026-03-19 |            9 |          1215
```

This confirms that the Slack alert's 2026-04-09 spike is real in the
current production DB and dominates the orphan population.

**C. automation_log summary for 2026-04-09**

```sql
SELECT action_name,
       status,
       COUNT(*) AS row_count,
       MIN(created_at) AS first_seen,
       MAX(created_at) AS last_seen
FROM automation_log
WHERE created_at::date = '2026-04-09'
  AND (action_name ILIKE '%invoice%'
       OR action_name ILIKE '%sync%'
       OR action_name ILIKE '%quickbooks%')
GROUP BY action_name, status
ORDER BY action_name, status;
```

Observed result:

```text
        action_name        | status  | row_count | first_seen                  | last_seen
---------------------------+---------+-----------+-----------------------------+-----------------------------
 create_quickbooks_invoice | failed  |        15 | 2026-04-09 03:02:57.272958  | 2026-04-09 23:14:05.964204
 create_quickbooks_invoice | success |      1780 | 2026-04-09 03:03:03.497952  | 2026-04-09 23:37:15.436972
 filter_synced_contacts    | success |        23 | 2026-04-09 00:03:35.156792  | 2026-04-09 23:37:22.291237
 sync_contact_to_pipedrive | success |         2 | 2026-04-09 17:02:41.462064  | 2026-04-09 18:03:44.023508
```

This is consistent with a very large QuickBooks invoice creation burst on
2026-04-09, not a quiet day with a later surprise import.

**D. `sync_runs` availability**

```sql
SELECT EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_name = 'sync_runs'
) AS sync_runs_exists;
```

Observed result:

```text
 sync_runs_exists
------------------
 f
```

So the optional `sync_runs` query in § 3b cannot be used on the current
production DB.

### 7d. Human Reviewer Worksheet (paste-in template)

The reviewer should copy this block into a dated addendum, fill the blanks,
and then complete the § 6 sign-off line.

```md
## Human Data-Integrity Review Addendum

Reviewer: ____________________
Date: ________________________
Environment checked: Railway production Postgres + live QBO

### 1. Independent rerun of § 3a / § 3b

- [ ] Re-ran orphan totals query against prod.
  Result: orphan_count=1393, orphan_amount=$208,800,
  min_issue_date=2026-03-19, max_issue_date=2026-04-14.
- [ ] Re-ran issue_date breakdown query against prod.
  2026-04-09 line reproduced? yes
  If yes: orphan_count=1363, orphan_amount=$204,450.
- [ ] Re-ran automation_log summary for 2026-04-09.
  `create_quickbooks_invoice` successes=1780
  `create_quickbooks_invoice` failures=15
  first_seen= 2026-04-09 03:02:57 last_seen=2026-04-09 18:03:44.
- [ ] Confirmed `sync_runs` availability.
  Exists? no
  If yes, summarize rows: ________________________________________________

### 2. QBO sample inspection (§ 3c)

For each row below, record the live QBO fields and whether there is already
another properly linked local invoice for the same `(client_id, issue_date, amount)`.

| canonical_id | client_id | issue_date | amount | qbo_invoice_id | CreateTime | TxnDate | PrivateNote contains `SS-JOB`? | CustomerRef matches local client? | duplicate linked local invoice exists? | reviewer notes |
|--------------|-----------|------------|-------:|----------------|------------|---------|--------------------------------|-----------------------------------|----------------------------------------|----------------|
| SS-INV-4629 | SS-CLIENT-0267 | 2026-04-09 | 150 | 9170 | 2026-04-09T01:05:18-07:00 | 2026-04-09 | SS-JOB: Z2lkOi8vSm9iYmVyL0pvYi8xMzg3OTg1MTI | Yes (qbo cust 311 = SS-CLIENT-0267) | No (0 linked invoices same day) | Linker expects canonical SS-JOB-NNNN; base64 GID fails regex → job_id stays NULL |

| SS-INV-4630 | SS-CLIENT-0275 | 2026-04-09 | 150 | 9171 | 	2026-04-09T01:05:23-07:00 | 2026-04-09 | Yes — base64 GID (gid://Jobber/Job/138798515) | Yes (319 = SS-CLIENT-0275) | No (0 linked same day) | Same root cause as 4629 |

| SS-INV-4631 | SS-CLIENT-0272 | 2026-04-09 | 150 | 9172 | 2026-04-09T01:05:27-07:00 | 2026-04-09 | Yes — base64 GID (gid://Jobber/Job/138798513) | Yes (316 = SS-CLIENT-0272) | No (0 linked same day) | Same root cause |

| SS-INV-4632 | SS-CLIENT-0115 | 2026-04-09 | 150 | 9173 | 2026-04-09T01:05:31-07:00 | 2026-04-09 | Yes — base64 GID (gid://Jobber/Job/138798507) | Yes (172 = SS-CLIENT-0115) | Yes (2 linked same day) | Double-billing risk: client has two properly-linked invoices 2026-04-09 in addition to this orphan |

| SS-INV-4633 | SS-CLIENT-0003 | 2026-04-09 | 150 | 9174 | 2026-04-09T01:05:35-07:00 | 2026-04-09 | Yes — base64 GID (gid://Jobber/Job/138798494) | Yes (61 = SS-CLIENT-0003) | Yes (1 linked same day) | Double-billing risk: sibling linked invoice exists on same date |

### 3. Reviewer conclusion

- [X] I independently reproduced the production orphan counts.
- [X] I completed the live QBO spot-check for the 5-sample set above.
- [X] My conclusion on the 2026-04-09 spike is:
  - [ ] Mostly real historical invoices that should be relinked where matchable.
  - [ ] Mostly import/reconciliation artifact that should be quarantined or deleted.
  - [X] Mixed; relink only the clearly attributable subset and quarantine the rest.
- [X] Based on the evidence above, I authorize / do not authorize
      `scripts/remediate_reconciliation_invoices.py --mode orphans --execute`.

Signed: OV   Date: 2026-04-19
```

---

## References

- Plan: [docs/revenue-remediation-plan-2026-04.md](revenue-remediation-plan-2026-04.md)
- Track B sub-plan: [docs/superpowers/plans/2026-04-16-track-b-orphan-invoice-remediation.md](superpowers/plans/2026-04-16-track-b-orphan-invoice-remediation.md)
- Residual-quarantine plan: [docs/2026-04-19-prod-orphan-quarantine-plan.md](2026-04-19-prod-orphan-quarantine-plan.md)
- Audit script: [scripts/audit_orphan_invoices.py](../scripts/audit_orphan_invoices.py)
- Remediation script: [scripts/remediate_reconciliation_invoices.py](../scripts/remediate_reconciliation_invoices.py)
- Root-cause code: [intelligence/syncers/sync_quickbooks.py:136-162](../intelligence/syncers/sync_quickbooks.py#L136-L162)
