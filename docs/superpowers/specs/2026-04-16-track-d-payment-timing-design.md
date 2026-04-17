# Track D — Payment Timing and Cash Realism (Design Spec)

**Date:** 2026-04-16
**Scope:** Track D of `docs/revenue-remediation-plan-2026-04.md`
**Depends on:** Track A (booked-vs-cash metric split) live in production ≥ 5 business days before Track D timing flag is flipped on.

---

## Problem

The simulation's payment generator currently uses a single `_PAYMENT_WINDOWS` table shared across all client types. Every invoice — residential "due on receipt" or commercial "Net 30" — has its first payment opportunity at day 3 at earliest, and the beta(2, 1) draw skews payment to the late end of each window. The net effect is that cash collection lags booked revenue by far more than the narrative says it should, and the `collection_ratio = mtd_cash / mtd_booked` metric in `intelligence/metrics/revenue.py` sits around 0.45 in April 1–15 2026 instead of the 0.70 floor the Track A cash-pacing logic assumes.

Track D aligns the simulation's payment timing with the invoice terms actually stamped on invoices by `automations/job_completion_flow.py`, so that cash-collection metrics in briefings reflect a believable business instead of misleadingly depressed AR.

## Goals

- Residential invoices (due on receipt) realize cash within 0–3 days of issue for on-time payers, clustering at day 0–1.
- Commercial invoices (Net 30) realize cash around day 30 for on-time payers, with slow and very-slow profiles pushing further out.
- Same-week residential work produces materially higher same-week cash realization than today.
- `mtd_cash / mtd_booked` after Track D flips on sits inside the 0.70–1.05 band expected by the cash-pacing alert.
- Track D lands **code-complete but feature-flagged off**, so merging the PR does not violate the 5-business-day Track A validation gate.
- The cash-collection alert floor is tunable via config without a code change, so if the new model's steady-state ratio differs from 0.70 we can adjust without another release.

## Non-goals

- No changes to profile assignment weights (`_PROFILE_WEIGHTS`). Holding these constant keeps the before/after attribution on `collection_ratio` clean — any movement after the flag flip is attributable to timing, not to a compound timing + weights change.
- No changes to the QBO payment API call, the SQLite schema, the automation runner, or the intelligence metrics pipeline beyond moving two constants into config.
- No changes to non-payer behavior. The 1–3% non-payer share is preserved and still writes off at the threshold, just with a client-type-specific threshold.

## Architecture

Track D is a localized change to `simulation/generators/payments.py`, plus two constants moved from `intelligence/metrics/revenue.py` into `intelligence/config.py` and two new feature flags. No schema changes. No changes to automations. Nothing outside the simulation generator and the metrics config.

Two independent flags gate the rollout:

- `TRACK_D_PAYMENT_TIMING_ENABLED` — switches the payment generator from legacy windows to V2 windows and from the scalar write-off threshold to a per-client-type threshold.
- `CASH_COLLECTION_ALERT_ENABLED` — gates the alert in `revenue.py` (already exists, stays False through this PR).

The flags are deliberately decoupled because the alert is coupled to two things, not one: the new timing model being active **and** the 0.70 floor actually being calibrated for that new model. Wiring the alert directly to the timing flag would reintroduce the false-positive class we just suppressed, only under a newer model.

## Components

### 1. `intelligence/config.py`

Add:

```python
# Track D: payment timing realism. When True, PaymentGenerator uses
# client-type-specific windows and write-off thresholds instead of the
# blended legacy windows. Merge this flag False; flip True only after
# Track A's booked-vs-cash split has been live in production for at
# least 5 business days.
TRACK_D_PAYMENT_TIMING_ENABLED: bool = False

# Cash-collection alert floor. Moved from revenue.py so the threshold
# can be tuned without a code change once Track D's steady-state
# collection_ratio is observed.
EXPECTED_CASH_RATIO_LOW: float = 0.70
EXPECTED_CASH_RATIO_HIGH: float = 1.05
```

Keep `CASH_COLLECTION_ALERT_ENABLED: bool = False` as-is. Its flip is a separate, later decision gated on observing the post-Track-D ratio.

### 2. `simulation/generators/payments.py`

Add V2 windows alongside the existing legacy table. Legacy stays in-file to guarantee exact-equivalence fallback when the flag is False.

```python
_PAYMENT_WINDOWS_LEGACY = {
    "on_time":   (3, 15),
    "slow":      (15, 45),
    "very_slow": (45, 75),
    "non_payer": None,
}

_PAYMENT_WINDOWS_V2 = {
    "residential": {
        "on_time":   (0, 3),
        "slow":      (5, 15),
        "very_slow": (20, 45),
        "non_payer": None,
    },
    "commercial": {
        "on_time":   (25, 35),
        "slow":      (35, 55),
        "very_slow": (55, 85),
        "non_payer": None,
    },
}

_WRITE_OFF_DAYS_LEGACY = 90
_WRITE_OFF_DAYS_V2 = {"residential": 60, "commercial": 90}
```

Update `_target_payment_date` to take `client_type` and branch on the flag:

```python
def _target_payment_date(
    client_type: str,
    profile: str,
    issue_date: date,
) -> Optional[date]:
    from intelligence import config as intel_config

    if not getattr(intel_config, "TRACK_D_PAYMENT_TIMING_ENABLED", False):
        window = _PAYMENT_WINDOWS_LEGACY[profile]
        if window is None:
            return None
        lo, hi = window
        fraction = random.betavariate(2, 1)
        days = lo + int(fraction * (hi - lo))
        return issue_date + timedelta(days=days)

    windows = _PAYMENT_WINDOWS_V2.get(client_type, _PAYMENT_WINDOWS_V2["residential"])
    window = windows[profile]
    if window is None:
        return None
    lo, hi = window

    if client_type == "residential" and profile == "on_time":
        fraction = random.betavariate(1, 2)   # early-skewed
    else:
        fraction = random.betavariate(2, 1)   # late-skewed

    days = lo + int(fraction * (hi - lo))
    return issue_date + timedelta(days=days)
```

Add a helper for write-off threshold:

```python
def _write_off_threshold(client_type: str) -> int:
    from intelligence import config as intel_config

    if getattr(intel_config, "TRACK_D_PAYMENT_TIMING_ENABLED", False):
        return _WRITE_OFF_DAYS_V2.get(client_type, 90)
    return _WRITE_OFF_DAYS_LEGACY
```

Update `_try_process` callsites:

- `target_date = _target_payment_date(profile, issue_date)` → `target_date = _target_payment_date(client_type, profile, issue_date)`
- `if days_outstanding >= _WRITE_OFF_DAYS:` → `if days_outstanding >= _write_off_threshold(client_type):`

`client_type` is already resolved a few lines above in `_try_process`, so no new DB query is needed.

Delete the module-level `_WRITE_OFF_DAYS = 90` constant once all callsites use `_write_off_threshold`. Keep `_WRITE_OFF_DAYS_LEGACY` as the internal fallback.

### 3. `intelligence/metrics/revenue.py`

Replace the two local constants with imports:

```python
from intelligence.config import (
    ALERT_THRESHOLDS,
    EXPECTED_CASH_RATIO_LOW,
    EXPECTED_CASH_RATIO_HIGH,
    REVENUE_TARGETS,
)
```

Delete the inline `EXPECTED_CASH_RATIO_LOW = 0.70` and `EXPECTED_CASH_RATIO_HIGH = 1.05` inside `compute()`. All uses of the names in that function stay identical.

No other logic changes. The `CASH_COLLECTION_ALERT_ENABLED` gate is untouched.

## Data flow

```
Job completes
  → automations/job_completion_flow.py stamps invoice with
    "Net 30" (commercial) or "due on receipt" (residential)
  → invoice row written to SQLite with status='sent'

PaymentGenerator tick
  → _get_unpaid_invoices(db)                            [unchanged]
  → for each invoice, _try_process:
      resolve client_type                               [unchanged]
      _assign_profile(client_id, client_type)           [unchanged]
      non_payer branch: uses _write_off_threshold       [flag-gated]
      _target_payment_date(client_type, profile, ...)   [flag-gated]
      if today >= target_date: create QBO payment       [unchanged]
  → payments.payment_date feeds revenue.py cash metrics
```

The only shapes that change are the two flag-gated branches. Everything upstream (invoice creation, profile assignment) and downstream (QBO API, payments row, metrics aggregation) is untouched.

## Error handling

No new error paths. The existing try/except in `execute_one`, the 429/401/400 branches in `_create_qbo_payment`, and the `UPDATE invoices SET status='written_off'` recovery on QBO 400 all operate identically.

Edge cases:
- **Unknown `client_type`:** V2 windows fall back to residential defaults (`_PAYMENT_WINDOWS_V2.get(client_type, _PAYMENT_WINDOWS_V2["residential"])`); `_write_off_threshold` falls back to 90. This matches the fallback already in `_assign_profile` and keeps the generator from crashing on data drift.
- **Non-payer under V2:** window is `None`, so `_target_payment_date` returns `None`, which the existing non-payer branch already handles before ever calling `_target_payment_date`. Defensive check is retained.
- **Flag read at call time (not import time):** Using `getattr(intel_config, "TRACK_D_PAYMENT_TIMING_ENABLED", False)` inside each function means a config change does not require a worker restart to take effect for subsequent ticks. Matches the existing pattern used for `CASH_COLLECTION_ALERT_ENABLED`.

## Testing

Add to `tests/test_simulation.py`. Keeping the timing assertions with the existing simulation tests avoids new test plumbing and lets the beta-distribution sanity checks reuse the same fixtures the generator tests already rely on.

1. **Legacy path regression guard (flag=False).** `_target_payment_date("residential", "on_time", date(2026,4,1))` returns a date in `[2026-04-04, 2026-04-16]`. Same test with `"commercial"` returns the same range. 100 samples; all within bounds.

2. **V2 residential on_time early skew (flag=True).** 1000 samples of `_target_payment_date("residential", "on_time", issue)`. All in `[issue+0, issue+3]`. Mean offset ≤ 1.5 days — confirms beta(1, 2) delivers the early-cluster we designed for.

3. **V2 residential slow/very_slow late skew (flag=True).** `slow` samples fall in `[5, 15]` days; `very_slow` in `[20, 45]` days. Mean of each ≥ midpoint — confirms beta(2, 1) preserved.

4. **V2 commercial on_time late skew (flag=True).** 1000 samples in `[25, 35]`. Mean ≥ 30 — confirms Net 30 behavior.

5. **Write-off threshold.** `_write_off_threshold("residential")` returns 60 when flag=True, 90 when flag=False. `_write_off_threshold("commercial")` returns 90 in both. Unknown type returns 90.

6. **Non-payer preserved.** `_target_payment_date("residential", "non_payer", issue)` returns `None` in both flag states. `_target_payment_date("commercial", "non_payer", issue)` same.

7. **Callsite signature.** Import `_try_process` or exercise `PaymentGenerator.execute_one` end-to-end against a small fixture invoice and confirm no `TypeError` on the new 3-arg `_target_payment_date`.

Tests use `unittest.mock.patch` on `intelligence.config.TRACK_D_PAYMENT_TIMING_ENABLED` to flip the flag per-test without touching the module-level default.

## Rollout sequence

1. **PR1 (this spec).** All code above, both flags False. Behavior bit-for-bit identical to production today. Tests green in both flag states. Merge.
2. **Track A gate confirmation.** Before flipping Track D on, verify via Railway logs or deploy records that the Track A changes in `intelligence/metrics/revenue.py` have been running in production for at least 5 business days. Today is 2026-04-16; earliest Track D flip is therefore tied to when Track A was actually deployed, not to today's calendar.
3. **Flip `TRACK_D_PAYMENT_TIMING_ENABLED = True`.** One-line config change, deploy.
4. **Observe.** Watch 3–5 consecutive daily briefings. Confirm `collection_ratio` lifts from ~0.45 toward the 0.70–1.05 band. Confirm no runaway write-offs (residential 60-day threshold is tighter than legacy 90 — non-payer residentials will write off a month sooner, which is the intended behavior).
5. **Tune `EXPECTED_CASH_RATIO_LOW` if needed.** If steady-state ratio sits consistently above or below 0.70, adjust the config constant. No code change required.
6. **Flip `CASH_COLLECTION_ALERT_ENABLED = True`.** Only after the ratio has stabilized and the floor is validated. The alert will then fire only when cash genuinely lags booked revenue under the new model.

## Risks

- **Seeded payment schedules in-flight at flag flip.** Invoices issued before the flip had their target dates drawn from legacy windows. After the flip, `PaymentGenerator` re-draws `target_date` on every tick (it is not persisted), so those in-flight invoices will immediately snap to the V2 timing for their profile. This is the desired behavior — we want the change to take effect uniformly — but it does mean the first few days after the flip will show a bulge of "suddenly ready to pay" residential invoices that were previously waiting for day 3+. Operationally harmless; briefings will show a one-time cash uptick.
- **Residential 60-day write-off vs. historical AR.** Any existing non-payer residential invoice already aged 60–89 days at flip time will write off on the next tick. Confirm in pre-flip audit that the count is expected (handful, not dozens) before deploying.
- **Flag read at call time.** A malformed config load (e.g., a typo) would silently revert to legacy behavior since the default is False. Acceptable fail-safe, but worth a log line on PaymentGenerator startup stating which mode is active, so we can confirm the flip from logs.

## Acceptance criteria

- All tests pass with both flag states.
- Existing `tests/test_phase4.py`, `tests/test_automations/test_job_completion_flow.py`, and `tests/test_simulation.py` continue to pass without modification (modulo the `_target_payment_date` signature change, which only affects payment-related tests).
- With `TRACK_D_PAYMENT_TIMING_ENABLED = True` in a local run of the simulation engine over a fresh day, residential on_time payments land within 3 days of invoice issue and average ≤ 1.5 days.
- With `TRACK_D_PAYMENT_TIMING_ENABLED = True` over a simulated month, the briefing-day `collection_ratio` sits in the 0.70–1.05 band for at least 15 of 20 business days.
- `CASH_COLLECTION_ALERT_ENABLED` can be flipped True without retriggering the April 1–15 false-positive pattern we suppressed.
