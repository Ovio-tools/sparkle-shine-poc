# Operations Generator Design Spec
**Date:** 2026-03-28
**Status:** Approved — ready for implementation planning

---

## What We're Building

`simulation/generators/operations.py` — a mixed-type simulation generator that handles two
responsibilities:

1. **New client setup** (Type 1): When a won deal's contract start date arrives and no Jobber
   record exists yet, create the Jobber client and initial job schedule.
2. **Daily job scheduling and completion** (Type 2): For all active recurring clients, determine
   which jobs are due today, create them in Jobber, then fire completion events at realistic
   times throughout the day.

The file contains three separate generator classes, each registered with the engine independently.

---

## Data Layer Changes

### `won_deals` table (new)

Owned by `deals.py` — the deals generator writes first, the operations generator reads.
Created in `DealGenerator._ensure_schema()` so it exists before any won deal fires.

```sql
CREATE TABLE IF NOT EXISTS won_deals (
    canonical_id      TEXT PRIMARY KEY,   -- SS-PROP-NNNN or SS-LEAD-NNNN
    client_type       TEXT NOT NULL,      -- 'commercial' or 'residential'
    service_frequency TEXT NOT NULL,      -- 'weekly_recurring', 'nightly_clean', etc.
    contract_value    REAL,               -- per-visit dollar amount
    start_date        TEXT NOT NULL,      -- ISO date; set by deals generator on win
    crew_assignment   TEXT,               -- 'Crew A', 'Crew B', etc.
    pipedrive_deal_id INTEGER
)
```

`OR IGNORE` on all inserts — safe to replay without creating duplicates.

### `deals.py` patch (already applied)

`DealGenerator._complete_won_deal()` now inserts into `won_deals` for **all** won deals
(SS-PROP and SS-LEAD), unconditionally when `not dry_run` and `canonical_id is not None`.

- SS-PROP path: still updates `commercial_proposals` AND inserts into `won_deals`
- SS-LEAD path: previously had no SQLite write; now writes only to `won_deals`

`_ensure_schema()` runs `CREATE TABLE IF NOT EXISTS won_deals` before any deal processing.

### `churn_risk` column on `clients` (added defensively)

`JobCompletionGenerator._ensure_schema()` adds `churn_risk TEXT DEFAULT 'normal'` to the
`clients` table if not present. Text values: `'normal'`, `'elevated'`, `'high'`. The churn
generator (Step 6) reads this to apply a 3× churn probability multiplier for high-risk clients.

---

## Engine Extensions

### Timed event queue

The engine maintains a priority queue (`heapq`) of `TimedEvent` objects alongside the shuffled
day plan. `JobSchedulingGenerator` queues completion events after creating each job.

```python
from collections import namedtuple
TimedEvent = namedtuple("TimedEvent", ["fire_at", "generator_name", "kwargs"])
# fire_at: datetime — used for heapq ordering
```

**New engine attribute:**
```python
self._timed_queue: list[TimedEvent] = []  # heapq, sorted by fire_at
```

**New public method (called by generators):**
```python
def queue_timed_event(self, fire_at: datetime, generator_name: str, kwargs: dict) -> None:
    heapq.heappush(self._timed_queue, TimedEvent(fire_at, generator_name, kwargs))
```

**`run_once()` dispatch loop** — before popping the next shuffled event, drain any timed events
whose `fire_at` has passed:
```python
now = datetime.utcnow()
while self._timed_queue and self._timed_queue[0].fire_at <= now:
    timed = heapq.heappop(self._timed_queue)
    self.dispatch(GeneratorCall(timed.generator_name, timed.kwargs))
```

**Checkpoint serialization:** `save_checkpoint()` includes the timed queue as a list of
`(fire_at_iso, generator_name, kwargs)` tuples. `load_checkpoint()` restores it. This prevents
losing queued completion events across engine restarts mid-day.

### `queue_fn` parameter (not full engine reference)

Generators receive only the `queue_timed_event` function, not the full engine instance. This
avoids circular dependencies and restricts generator access to only what they need.

```python
# In engine._register_generators():
self.register(
    "job_scheduling",
    JobSchedulingGenerator(db_path, queue_fn=self.queue_timed_event)
)
```

`queue_fn=None` default allows tests to instantiate `JobSchedulingGenerator` without an engine
and verify queue calls via a mock or a list collector.

Only `JobSchedulingGenerator` receives `queue_fn`. `NewClientSetupGenerator` and
`JobCompletionGenerator` do not need it.

### `plan_day()` ordering

The day's event list is constructed as:

1. One `GeneratorCall("new_client_setup", {})` — placed first, before shuffling
2. One `GeneratorCall("job_scheduling", {})` — placed second, before shuffling
3. N `GeneratorCall("job_completion", {"job_id": ...})` events — queued dynamically by
   `JobSchedulingGenerator` at runtime; not in the initial plan

The shuffled pool (contacts, deals, churn, tasks, payments) follows items 1 and 2.
`job_completion` calls are driven by the timed queue, not the shuffled pool.

---

## `NewClientSetupGenerator`

**Type:** 1 (creates new records from scratch)
**Cadence:** Once per day (placed at front of plan, before shuffled pool)

### Trigger query

```sql
SELECT * FROM won_deals
WHERE start_date <= date('now')
  AND canonical_id NOT IN (
      SELECT canonical_id FROM cross_tool_mapping WHERE tool_name = 'jobber'
  )
```

Typically returns 0–3 rows per day.

### Per-client setup flow

All ready deals are processed in one `execute()` call. Each client is wrapped in its own
`try/except` so a single API failure does not abort the rest.

1. Look up client in SQLite (`clients` table via `canonical_id`) → name, email, phone, address
2. `clientCreate` GraphQL mutation → `jobber_client_id`
3. `register_mapping(canonical_id, "jobber", jobber_client_id)`
4. Branch on `service_frequency`:

**Recurring** (`weekly_recurring`, `biweekly_recurring`, `monthly_recurring`, `nightly_clean`,
`weekend_deep_clean`):
- `jobRecurrenceCreate` mutation with schedule derived from frequency
- `generate_id("AGREEMENT")` → insert into `recurring_agreements`:
  - `status='active'`, `frequency` mapped from frequency string
  - `price_per_visit = contract_value`
  - `start_date`, `crew_id` mapped from `crew_assignment`
  - `day_of_week` = first available weekday on or after `start_date`
- `register_mapping(recurring_agreement_id, "jobber", jobber_recurrence_id)`

  Note: verify the entity type prefix used in existing seeded recurring agreement IDs before
  implementing (`grep "SS-" sparkle_shine.db` or check `seeding/generators/gen_jobs.py`). Use
  whatever prefix the seeded data established so canonical IDs are consistent.

**One-time** (`one_time_standard`, `one_time_deep_clean`, `one_time_move_in_out`,
`one_time_project`):
- `jobCreate` mutation with `scheduled_date = start_date`
- `generate_id("JOB")` → insert into `jobs`:
  - `status='scheduled'`, `scheduled_date = start_date`
  - `service_type_id` mapped from frequency string
  - `crew_id` from `crew_assignment`
- `register_mapping(job_canonical_id, "jobber", jobber_job_id)`

5. Log Pipedrive activity note: `"SS-ID: {canonical_id} — Jobber client created, {frequency} schedule initialized"` (skipped in dry_run)

### Return pattern

```python
results = []
for deal in ready_deals:
    try:
        # steps 1-5
        results.append(("ok", deal["canonical_id"]))
    except Exception as e:
        logger.exception("Setup failed for %s", deal["canonical_id"])
        results.append(("failed", deal["canonical_id"], str(e)))

succeeded = sum(1 for r in results if r[0] == "ok")
failed = len(results) - succeeded
if failed:
    return GeneratorResult(success=False, message=f"setup {succeeded} clients, {failed} failed")
return GeneratorResult(success=True, message=f"setup {succeeded} new clients")
```

`GeneratorResult(success=True, message="no new clients ready")` when query returns empty.
Individual client failures are logged internally and summarized in the message; they do not
propagate as exceptions to the engine's dispatch handler.

---

## `JobSchedulingGenerator`

**Type:** 2 (progresses existing records)
**Cadence:** Once per day (placed second in plan, before shuffled pool)

### "Is a job due today?" logic

For each active row in `recurring_agreements`:

- **weekly**: due if `today.weekday() == day_of_week_int`
- **biweekly**: due if weekly condition holds AND `(today - start_date).days % 14 < 7`
- **monthly**: due if `today.day == start_date_day` (clamped to last day of month if
  `start_date_day > days_in_month`)

**Idempotent guard:** Skip if a `jobs` row already exists for this `client_id` on today's date.

### Job type selection (per `JOB_VARIETY` config)

**Residential recurring:**
1. Roll against `deep_clean_rate × seasonal_deep_clean_boost[current_month]` (default 1.0 for
   months not in the boost dict)
2. If not deep clean, roll against `add_on_rate`
3. Result: `regular`, `deep_clean`, or `add_on` (random choice from `add_on_options`)

**Commercial recurring:**
1. Roll against `extra_service_rate`
2. Result: `standard` or `extra_service` (random choice from `extra_service_options`)

### Scheduled time assignment (sequential routing)

Jobs for each crew are assigned start times sequentially, preserving the natural route pattern:

| Crew | Window start | Notes |
|------|-------------|-------|
| Crew A | 7:00 AM | Westlake/Tarrytown residential |
| Crew B | 7:30 AM | East Austin/Mueller residential |
| Crew C | 8:00 AM | South Austin/Zilker residential |
| Crew D | 5:00 PM | Round Rock/Cedar Park commercial (post-business-hours) |

Within each crew: `job_start = window_start + sum(prior_durations) + sum(travel_buffers)`.
Travel buffer per job: `random.randint(15, 30)` minutes. First job gets `window_start` exactly.

### Per-job flow

```python
for agreement in due_agreements:
    job_type = _pick_job_type(agreement, today)
    scheduled_time = _assign_scheduled_time(agreement.crew_id, prior_jobs_for_crew)
    expected_duration = _expected_duration(agreement.service_type_id, job_type)
    completion_time = scheduled_time + timedelta(minutes=expected_duration)
    completion_time += timedelta(minutes=random.uniform(
        -expected_duration * 0.15, expected_duration * 0.15
    ))

    job_id = generate_id("JOB")
    # jobCreate mutation → jobber_job_id
    # INSERT INTO jobs (status='scheduled', scheduled_date, scheduled_time, ...)
    # register_mapping(job_id, "jobber", jobber_job_id)

    if self._queue_timed_event:
        self._queue_timed_event(
            fire_at=completion_time,
            generator_name="job_completion",
            kwargs={"job_id": job_id},
        )
```

Line item pricing in Jobber: `price_per_visit` (×1.80 for deep clean, +add-on price for add-ons).

Expected duration source: `config/business.py` `SERVICE_TYPES[n]["duration_minutes"]`. Map
`service_type_id` → duration. If the field is absent for a type, default to 120 minutes.

### Already-scheduled jobs (rescheduled and one-time)

After processing `recurring_agreements`, run a second query to pick up any jobs that already exist
in SQLite with `status='scheduled'` for today — covering rescheduled jobs (created yesterday by
`JobCompletionGenerator`) and one-time jobs created by `NewClientSetupGenerator` on their
`start_date`:

```sql
SELECT * FROM jobs
WHERE status = 'scheduled'
  AND scheduled_date = date('now')
  AND id NOT IN (<job IDs created this run from recurring_agreements>)
```

For each result: **skip** `jobCreate` (the job already exists in Jobber), but **do** queue a timed
completion event based on the job's `scheduled_time` + `expected_duration` ± 15% variance.

This eliminates any need for special completion-event handling in `NewClientSetupGenerator` (the
one-time setup flow creates the job row; the next day's scheduling run picks it up and queues its
completion) and ensures rescheduled jobs are never abandoned in `status='scheduled'` permanently.

Same collected-results return pattern as `NewClientSetupGenerator`.

---

## `JobCompletionGenerator`

**Type:** 2 (progresses existing records)
**Cadence:** Fired by timed events queued by `JobSchedulingGenerator`; receives
`{"job_id": job_canonical_id}` in kwargs

### Outcome roll

From `DAILY_VOLUMES["job_completion"]`:

| Probability | Outcome | Action |
|------------|---------|--------|
| 92% | completed | Mark complete, record duration, insert review |
| 3% | cancelled | Mark cancelled, check churn risk |
| 2% | no-show | Mark no-show, check churn risk |
| 3% | rescheduled | Cancel original, create new job for next business day |

### `completed` path

1. `get_tool_id(job_id, "jobber")` → `jobber_job_id`
2. Jobber mutation to mark job complete
3. `actual_duration = expected_duration * random.uniform(0.85, 1.15)`
4. `UPDATE jobs SET status='completed', duration_minutes_actual=?, completed_at=? WHERE id=?`
5. Determine rating using `_adjusted_rating_distribution(crew_name, day_of_week)` → draw from
   distribution → `INSERT INTO reviews (id, client_id, job_id, rating, platform, review_date)`
   with `platform='internal'` and `review_date = today`

### Rating distributions

```
Base:              5★ 60%,  4★ 25%,  3★ 10%,  2★ 4%,   1★ 1%
Crew A only:       5★ 70%,  4★ 20%,  3★ 7%,   2★ 2.5%, 1★ 0.5%
Tue/Wed only:      5★ 65%,  4★ 23%,  3★ 8%,   2★ 3%,   1★ 1%
Crew A + Tue/Wed:  5★ 75%,  4★ 17%,  3★ 5%,   2★ 2.5%, 1★ 0.5%
```

Cap rule: 5★ weight never exceeds 80%; any excess redistributed into 4★.

Implemented as a pure function `_adjusted_rating_distribution(crew_name: str, day_of_week: int) -> list[tuple[int, float]]` — no mocking required in tests.

Rating insert skipped when `dry_run=True`.

### `cancelled` / `no-show` path

1. `UPDATE jobs SET status=? WHERE id=?`
2. Jobber mutation to cancel/update job status
3. Churn risk check:
   ```sql
   SELECT COUNT(*) FROM jobs
   WHERE client_id = ? AND status IN ('cancelled', 'no-show')
     AND scheduled_date >= date('now', '-60 days')
   ```
   If count ≥ 3: `UPDATE clients SET churn_risk='high' WHERE id=?`

### `rescheduled` path

1. `UPDATE jobs SET status='cancelled' WHERE id=?` (original slot)
2. Jobber: cancel original job
3. `generate_id("JOB")` → new job for `_add_business_days(today, 1)`, same crew/service/client
4. Insert into `jobs` with `status='scheduled'`
5. `jobCreate` mutation → new `jobber_job_id`
6. `register_mapping(new_job_canonical_id, "jobber", new_jobber_job_id)`

### Return

`GeneratorResult(success=True, message=f"job {job_id}: {outcome}")` on success.
`GeneratorResult(success=False, message=f"job {job_id}: {error}")` on API failure.

---

## Auth and Rate Limiting

- Auth: `auth.get_client("jobber")` exclusively
- Throttler: `from seeding.utils.throttler import JOBBER as throttler` (0.15s between calls)
- All GraphQL calls: `POST https://api.getjobber.com/api/graphql` with header
  `X-JOBBER-GRAPHQL-VERSION: 2026-03-10`
- Never write to `poll_state`
- Never create QBO invoices (automation runner handles downstream invoice creation)

---

## File and Registration

**File:** `simulation/generators/operations.py`

**Engine registration** (in `simulation/engine.py` `_register_generators()`):

```python
try:
    from simulation.generators.operations import (
        NewClientSetupGenerator,
        JobSchedulingGenerator,
        JobCompletionGenerator,
    )
    self.register("new_client_setup", NewClientSetupGenerator(db_path))
    self.register("job_scheduling",   JobSchedulingGenerator(db_path, queue_fn=self.queue_timed_event))
    self.register("job_completion",   JobCompletionGenerator(db_path))
except ImportError:
    logger.warning("OperationsGenerators not found — skipping")
```

`JobCompletionGenerator` is registered so the engine can dispatch timed events to it by name,
but it is never added to the shuffled plan directly.

---

## Testing Surface

Tests live in `tests/test_phase5_operations.py`.

### Pure unit tests (no mocks)

- **`_adjusted_rating_distribution` — all 4 combos:** Call with `(crew_name, day_of_week)` for
  base, Crew A only, Tue/Wed only, and Crew A + Tue/Wed. Assert the returned 5★ weight matches
  60%, 70%, 65%, and 75% respectively (within floating-point tolerance).
- **`_adjusted_rating_distribution` — 5★ cap:** Construct a scenario where combined boosts would
  exceed 80% for 5★; assert the returned 5★ weight is exactly 80% and the excess is redistributed
  into 4★.
- **Biweekly due-date logic:** Given a `start_date` of Monday, assert the job fires on weeks 0, 2,
  4 from `start_date` and returns `False` for weeks 1, 3, 5.
- **Monthly due-date clamping:** `start_date` with day 31; assert the due check returns `True` for
  February 28 (or 29 in a leap year) and `False` for February 1.
- **`_pick_job_type` distribution:** 200 rolls on a residential recurring agreement produce regular
  75–95%, deep clean 5–20%, add-on 1–12% (tolerance bands, not exact).
- **`_pick_job_type` seasonal boost:** Compare deep-clean rate for March (boost month) vs. August
  (no boost) over 500 rolls; assert March rate is statistically higher.
- **Sequential scheduling:** Create 4 agreements for Crew A. Assert the 4 returned `scheduled_time`
  values are strictly ascending and each gap is at least 15 minutes (minimum travel buffer).

### Mock tests

- **Per-client try/except isolation:** Three clients in `won_deals`. Mock the Jobber API to raise
  on the second client only. Assert `NewClientSetupGenerator.execute()` returns
  `GeneratorResult(success=False, message=...)` and the message reports 2 succeeded, 1 failed.
- **`queue_fn` call args:** Mock `queue_fn` as a list collector. Run `JobSchedulingGenerator` with
  2 due agreements. Assert `queue_fn` was called twice with `generator_name="job_completion"` and
  `fire_at` values in the future.
- **`completed` outcome:** Mock Jobber mutation and SQLite. Assert `jobs.status='completed'`,
  `duration_minutes_actual` is set, and a row is inserted into `reviews`.
- **`cancelled` outcome:** Assert `jobs.status='cancelled'` and Jobber cancel mutation was called.
- **`no-show` outcome:** Assert `jobs.status='no-show'` and Jobber mutation was called.
- **`rescheduled` outcome:** Assert original job has `status='cancelled'`, a new `jobs` row exists
  with `status='scheduled'` and `scheduled_date = _add_business_days(today, 1)`, and
  `register_mapping` was called for the new job.
- **Churn risk threshold — triggers:** 3 `cancelled` / `no-show` jobs for the same client within
  60 days → assert `clients.churn_risk='high'`.
- **Churn risk threshold — does not trigger:** 2 such events → assert `churn_risk` is unchanged.

### Integration tests (gated behind `RUN_INTEGRATION`)

- **Jobber `clientCreate` + `jobCreate` round-trip:** Call `NewClientSetupGenerator.execute()` with
  a real won deal; assert the returned Jobber IDs are non-null and mappings are registered.
- **Rescheduled job picked up next day:** Run `JobCompletionGenerator` with outcome forced to
  `rescheduled`; assert the new job row has tomorrow's date; then run `JobSchedulingGenerator` for
  that date and assert the job appears in its output (picked up by the second query).

---

## Constraints

- Never write to `poll_state` (automation runner owns those watermarks)
- Never create QBO invoices (runner creates them on completed job detection)
- Never create Pipedrive records (simulation injects at source tool only)
- Auth via `auth.get_client("jobber")` exclusively — never import `jobber_auth.py` directly
- Imports: `from database.mappings import ...` and `from database.schema import ...`
- All numeric config values from `simulation/config.py` (no inline magic numbers)
- `dry_run=True` skips all API calls, SQLite writes, and review inserts; timed events may still
  be queued in dry_run (they will dispatch to no-op calls)
