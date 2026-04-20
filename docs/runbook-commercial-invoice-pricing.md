# Runbook: Populate commercial rate for clients flagged by `JobCompletionFlow`

**Applies to:** Slack alerts of the form

> Automation health check: N completed job(s) from yesterday have no invoices. The Jobber-to-QuickBooks automation may have missed them. Job IDs: SS-JOB-XXXX

> `Invoice creation skipped: pricing unresolved for service_type_id=commercial-nightly (canonical job SS-JOB-XXXX). A commercial-nightly job without a resolvable contract rate cannot be billed at the residential fallback; ops must resolve the rate before this job can be invoiced.`

**Why this happens.** `JobCompletionFlow` refuses to invoice a `commercial-nightly` job when `recurring_agreements` has no active row covering the job's `scheduled_date`. The refusal is intentional (commit `17bc842`): falling back to the residential rate would silently undercharge and misclassify the invoice. The unpaid job is left for ops to resolve.

**Two tools already exist.** Do not write new scripts. Use them in this order:

1. `scripts/backfill_commercial_agreements.py` -- (re)creates the missing `recurring_agreements` row.
2. `scripts/remediate_skipped_commercial_invoices.py` -- replays only the QBO invoice create for the specific job IDs.

---

## Step-by-step

All `python` invocations below assume you are running against **Railway production**. Local runs require `DATABASE_URL` set and valid OAuth for Jobber / QBO / HubSpot. Prefer `railway run` or `railway ssh` so you pick up the production env without copying secrets locally.

### 1. Identify affected jobs

```bash
railway run --service automation-runner python3 - <<'PY'
from database.connection import get_connection
conn = get_connection()
rows = conn.execute("""
    SELECT DISTINCT
           substring(error_message from 'SS-JOB-[0-9]+') AS job_id,
           min(created_at) AS first_failure,
           max(created_at) AS last_failure,
           count(*) AS attempts
    FROM automation_log
    WHERE automation_name = 'JobCompletionFlow'
      AND action_name    = 'create_quickbooks_invoice'
      AND status         = 'failed'
      AND error_message LIKE '%pricing unresolved%commercial-nightly%'
    GROUP BY 1
    ORDER BY min(created_at)
""").fetchall()
for r in rows:
    print(dict(r))
conn.close()
PY
```

Record each `SS-JOB-XXXX`. Then resolve each to its client and scheduled date:

```bash
railway run --service automation-runner python3 - <<'PY'
from database.connection import get_connection
JOB_IDS = ["SS-JOB-5553", "SS-JOB-5554", ...]  # paste from step 1
conn = get_connection()
for jid in JOB_IDS:
    row = conn.execute(
        "SELECT id, client_id, scheduled_date, service_type_id "
        "FROM jobs WHERE id = %s", (jid,),
    ).fetchone()
    print(dict(row) if row else f"{jid}: NOT FOUND in jobs")
conn.close()
PY
```

Keep this list -- you will need `(job_id, client_id, scheduled_date)` for steps 2 and 3.

### 2. Check whether an agreement already covers each job_date

```bash
railway run --service automation-runner python3 - <<'PY'
from database.connection import get_connection
CLIENT_IDS = ["SS-CLIENT-0313", "SS-CLIENT-0314", ...]  # from step 1
conn = get_connection()
rows = conn.execute("""
    SELECT client_id, frequency, price_per_visit,
           start_date, end_date, status
    FROM recurring_agreements
    WHERE service_type_id = 'commercial-nightly'
      AND client_id = ANY(%s)
    ORDER BY client_id, start_date
""", (CLIENT_IDS,)).fetchall()
for r in rows:
    print(dict(r))
conn.close()
PY
```

For each affected job, compare `scheduled_date` to the agreement's `start_date` / `end_date`:

| Situation | Outcome | Go to |
|-----------|---------|-------|
| Active agreement with `start_date <= scheduled_date` and (`end_date IS NULL` or `end_date >= scheduled_date`) | Rate is already resolvable -- the failure was transient or pre-dated the agreement row | Step 4 (retry the invoice) |
| No agreement, or the only agreement's `start_date` is later than `scheduled_date` | The rate cannot be resolved for that job_date | Step 3 (populate) |

### 3. Populate the missing agreement

**Preferred path: driven by the seed data.** `scripts/backfill_commercial_agreements.py` reads the authoritative schedule and per-visit rate from `seeding/generators/gen_clients._COMMERCIAL_CLIENTS` via `get_commercial_per_visit_rate`. Re-running is idempotent (skips rows that already exist for the same `client_id` + `service_type_id`).

```bash
# Dry run, single client
railway run --service automation-runner python3 -m scripts.backfill_commercial_agreements \
    --dry-run --client-id SS-CLIENT-0318

# Execute, single client
railway run --service automation-runner python3 -m scripts.backfill_commercial_agreements \
    --execute --client-id SS-CLIENT-0318

# Execute, all commercial clients (safe, idempotent)
railway run --service automation-runner python3 -m scripts.backfill_commercial_agreements --execute
```

**If the backfill script's `start_date` is still later than a failed job's `scheduled_date`** (for example, a job completed on 2026-04-17 but the seed schedule has the agreement starting 2026-04-18), decide with Maria:

- **Option A -- backdate the agreement.** Run a one-off UPDATE that moves `start_date` earlier so the historical job falls inside the active window. Only do this after confirming the client really was under contract at that rate on the earlier date -- undercharging retroactively is worse than leaving the job un-invoiced.
  ```sql
  UPDATE recurring_agreements
  SET start_date = :earlier_date_iso
  WHERE client_id = :client_id
    AND service_type_id = 'commercial-nightly'
    AND status = 'active';
  ```
- **Option B -- leave the pre-contract job unbilled.** If the job genuinely predates the contract, do not invoice it. Mark it out-of-scope in Jobber and clear the alert by closing the job in a non-invoiceable status.

### 4. Retry the invoice(s) for specific job IDs

Once the rate is resolvable, replay the invoice step only (not the full `JobCompletionFlow`):

```bash
# Dry run first
railway run --service automation-runner python3 -m scripts.remediate_skipped_commercial_invoices \
    --dry-run \
    --job-id SS-JOB-5560

# Execute when the dry run looks right
railway run --service automation-runner python3 -m scripts.remediate_skipped_commercial_invoices \
    --execute \
    --job-id SS-JOB-5553 \
    --job-id SS-JOB-5554 \
    --job-id SS-JOB-5555 \
    --job-id SS-JOB-5560 \
    --job-id SS-JOB-5580 \
    --job-id SS-JOB-5581 \
    --job-id SS-JOB-5582
```

`remediate_skipped_commercial_invoices.py` is intentionally narrower than a full replay: it creates the QBO invoice and increments HubSpot `outstanding_balance`, but it does NOT add a duplicate HubSpot note, bump `total_services_completed`, or schedule another review request. Safe to re-run if a subset fails.

### 5. Verify

```bash
railway run --service automation-runner python3 - <<'PY'
from database.connection import get_connection
JOB_IDS = ["SS-JOB-5553", "SS-JOB-5554", ...]
conn = get_connection()
rows = conn.execute("""
    SELECT j.id AS job_id, j.client_id, j.scheduled_date,
           i.id AS invoice_id, i.amount, i.issue_date, i.status
    FROM jobs j
    LEFT JOIN invoices i ON i.job_id = j.id
    WHERE j.id = ANY(%s)
    ORDER BY j.scheduled_date
""", (JOB_IDS,)).fetchall()
for r in rows:
    print(dict(r))
conn.close()
PY
```

Every target job should now have a non-null `invoice_id`, `amount`, and `issue_date`. Confirm in QuickBooks that the invoice was created with the correct customer and `commercial-nightly` item.

---

## Prevention checklist

Each new commercial-nightly deal closing in Pipedrive must produce a matching `recurring_agreements` row **before** the first service day. The onboarding automation is the correct place to write it (see `automations/new_client_onboarding.py`). When adding a new commercial client by hand:

1. Confirm `client_type = 'commercial'` in `clients`.
2. INSERT a row into `recurring_agreements` with `service_type_id = 'commercial-nightly'`, `status = 'active'`, the contract `price_per_visit`, and `start_date <=` first service day.
3. Do not rely on the residential $150 fallback -- `JobCompletionFlow` will refuse to bill a commercial-nightly job against it by design.
