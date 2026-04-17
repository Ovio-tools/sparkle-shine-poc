# Track D — Payment Timing and Cash Realism Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Align simulation payment timing with invoice terms (residential due-on-receipt → 0–3 days; commercial Net 30 → ~30 days) behind a feature flag, so `cash_collected / booked_revenue` trends toward the 0.70–1.05 band only after Track A has been validated in production for 5+ business days.

**Architecture:** Localized change to `simulation/generators/payments.py` (new V2 windows + V2 write-off thresholds, flag-gated at call time), two constants (`EXPECTED_CASH_RATIO_LOW/HIGH`) moved from `intelligence/metrics/revenue.py` into `intelligence/config.py` so the alert floor is tunable without a code change, and one new flag (`TRACK_D_PAYMENT_TIMING_ENABLED`) alongside the existing `CASH_COLLECTION_ALERT_ENABLED`. Merge with both flags False — behavior identical to today until the flip.

**Tech Stack:** Python 3.11, stdlib `unittest` + `unittest.mock.patch`, existing `random.betavariate` for distribution shaping, `psycopg2` (indirect — unchanged).

**Spec:** `docs/superpowers/specs/2026-04-16-track-d-payment-timing-design.md`

---

## File Structure

**Modified:**
- `intelligence/config.py` — add `TRACK_D_PAYMENT_TIMING_ENABLED`, `EXPECTED_CASH_RATIO_LOW`, `EXPECTED_CASH_RATIO_HIGH`
- `intelligence/metrics/revenue.py` — replace inline ratio constants with config imports
- `simulation/generators/payments.py` — add `_PAYMENT_WINDOWS_V2`, `_PAYMENT_WINDOWS_LEGACY`, `_WRITE_OFF_DAYS_V2`, `_WRITE_OFF_DAYS_LEGACY`; add `_write_off_threshold` helper; update `_target_payment_date` signature; update `_try_process` callsites; delete module-level `_WRITE_OFF_DAYS`
- `tests/test_simulation.py` — add `TestTrackDPaymentTiming` test class

**Not modified:** schema, automations, QBO API call, profile assignment weights.

---

## Task 1: Add Track D config flag and cash ratio constants

**Files:**
- Modify: `intelligence/config.py:65` (insert after the existing `CASH_COLLECTION_ALERT_ENABLED` block)

- [ ] **Step 1: Open `intelligence/config.py` and locate the Track A comment block ending at line 65**

The flag should live immediately after `CASH_COLLECTION_ALERT_ENABLED` because they form a paired pattern (both gate Track D rollout behavior) and the comment block already references Track D by name.

- [ ] **Step 2: Insert the Track D flag and ratio constants**

After the existing line `CASH_COLLECTION_ALERT_ENABLED: bool = False` (line 65), add:

```python

# Track D cash realism gate.
#
# When True, simulation.generators.payments uses client-type-specific
# windows (residential 0-3 due-on-receipt / commercial ~Net 30) and a
# client-type-specific write-off threshold (residential 60 / commercial 90
# days). When False, the generator uses the legacy blended windows and
# the flat 90-day write-off — behavior identical to pre-Track-D production.
#
# Merge this flag False. Flip True only after Track A's booked-vs-cash
# split has been live in production for at least 5 business days. The flag
# is read at call time via getattr, so a config change takes effect on
# the next PaymentGenerator tick without a worker restart.
TRACK_D_PAYMENT_TIMING_ENABLED: bool = False

# Cash-collection alert band (MTD cash / MTD booked).
#
# Moved out of intelligence/metrics/revenue.py so the floor can be
# tuned without a code change once Track D's steady-state ratio is
# observed in production. LOW is the floor below which CASH_COLLECTION_ALERT
# fires (when the alert is enabled); HIGH is the ceiling above which the
# cash_pacing bucket is marked "ahead" (prior-month AR collecting faster
# than usual, or repriced historical invoices landing this month).
EXPECTED_CASH_RATIO_LOW: float = 0.70
EXPECTED_CASH_RATIO_HIGH: float = 1.05
```

- [ ] **Step 3: Verify module imports cleanly**

Run: `python -c "from intelligence.config import TRACK_D_PAYMENT_TIMING_ENABLED, EXPECTED_CASH_RATIO_LOW, EXPECTED_CASH_RATIO_HIGH; assert TRACK_D_PAYMENT_TIMING_ENABLED is False; assert EXPECTED_CASH_RATIO_LOW == 0.70; assert EXPECTED_CASH_RATIO_HIGH == 1.05; print('OK')"`
Expected output: `OK`

- [ ] **Step 4: Commit**

```bash
git add intelligence/config.py
git commit -m "Add Track D timing flag and cash ratio config constants"
```

---

## Task 2: Move ratio constants from revenue.py to config

**Files:**
- Modify: `intelligence/metrics/revenue.py:14-16` (imports) and `:196-197` (delete inline constants)
- Regression check: `tests/test_phase4.py` (existing — do not modify)

This is a pure refactor with no behavior change. It isolates the constants move so Task 3+ can assume the config is the single source of truth.

- [ ] **Step 1: Update the imports in `intelligence/metrics/revenue.py`**

Current lines 14–16:
```python
from intelligence import config as intel_config
from intelligence.config import ALERT_THRESHOLDS, REVENUE_TARGETS
```

Replace with:
```python
from intelligence import config as intel_config
from intelligence.config import (
    ALERT_THRESHOLDS,
    EXPECTED_CASH_RATIO_HIGH,
    EXPECTED_CASH_RATIO_LOW,
    REVENUE_TARGETS,
)
```

- [ ] **Step 2: Delete the two inline constants inside `compute()`**

Current lines 196–197 (inside `compute()`):
```python
    EXPECTED_CASH_RATIO_LOW = 0.70
    EXPECTED_CASH_RATIO_HIGH = 1.05
```

Delete both lines. The surrounding comment block (above) can be shortened — keep the rationale paragraph but drop the `# Source: intelligence/config.py REVENUE_TARGETS trajectory...` line's claim that the constants live here. The uses further down (the `collection_ratio` comparisons and the return dict's `expected_ratio_low`/`expected_ratio_high`) will now resolve to the module-level imports.

Optional cleanup: tighten the comment block above the deleted constants to reflect that the values now live in config. Do not rewrite the full justification paragraph — it's valid context for why 0.70/1.05 were chosen.

- [ ] **Step 3: Run the revenue-metrics regression test**

Run: `python -m pytest tests/test_phase4.py -v -k "revenue or cash_pacing or cash_collection" 2>&1 | tail -40`
Expected: all tests pass. The `test_cash_collection_alert_*` tests patch `intelligence.config.CASH_COLLECTION_ALERT_ENABLED` and exercise the same code paths we just touched — they should continue to pass unchanged.

If any test fails with `NameError` for `EXPECTED_CASH_RATIO_LOW`, the import block in Step 1 was not applied. Re-check.

- [ ] **Step 4: Commit**

```bash
git add intelligence/metrics/revenue.py
git commit -m "Move cash ratio thresholds from revenue.py into intelligence.config"
```

---

## Task 3: Add V2 payment windows and write-off tables to payments.py

**Files:**
- Modify: `simulation/generators/payments.py:47-63` (replace the flat `_PAYMENT_WINDOWS` + `_WRITE_OFF_DAYS` with legacy + V2 tables)

This task adds the data structures only. `_target_payment_date` and `_try_process` still reference the legacy names, so behavior is unchanged at the end of this task.

- [ ] **Step 1: Replace the existing constants block**

Current lines 47–63 of `simulation/generators/payments.py`:

```python
# Profile weights per client type: [on_time, slow, very_slow, non_payer]
_PROFILE_WEIGHTS = {
    "residential": [0.77, 0.15, 0.07, 0.01],
    "commercial":  [0.65, 0.22, 0.10, 0.03],
}

_PROFILES = ["on_time", "slow", "very_slow", "non_payer"]

# Payment window (days after invoice issue_date): (min, max)
# non_payer has no window — invoice is never paid
_PAYMENT_WINDOWS = {
    "on_time":   (3, 15),
    "slow":      (15, 45),
    "very_slow": (45, 75),
    "non_payer": None,
}

_WRITE_OFF_DAYS = 90
```

Replace with:

```python
# Profile weights per client type: [on_time, slow, very_slow, non_payer]
_PROFILE_WEIGHTS = {
    "residential": [0.77, 0.15, 0.07, 0.01],
    "commercial":  [0.65, 0.22, 0.10, 0.03],
}

_PROFILES = ["on_time", "slow", "very_slow", "non_payer"]

# --------------------------------------------------------------------------- #
# Payment timing windows (days after invoice issue_date): (min, max)
# non_payer has no window — invoice is never paid.
#
# Legacy table: single blended window per profile. Used when
# TRACK_D_PAYMENT_TIMING_ENABLED is False (pre-Track-D behavior).
#
# V2 table: client-type-specific windows matching invoice terms stamped
# by automations/job_completion_flow.py (residential due-on-receipt,
# commercial Net 30). Used when TRACK_D_PAYMENT_TIMING_ENABLED is True.
# --------------------------------------------------------------------------- #
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

# Write-off threshold (days outstanding before a non-payer invoice is
# marked written_off). Legacy is a flat 90 days; V2 tightens residential
# to 60 days (aligns with ALERT_THRESHOLDS["overdue_invoice_days_critical"])
# while giving commercial the original 90-day headroom.
_WRITE_OFF_DAYS_LEGACY = 90
_WRITE_OFF_DAYS_V2 = {"residential": 60, "commercial": 90}
```

- [ ] **Step 2: Verify the module still imports cleanly**

Run: `python -c "from simulation.generators.payments import _PAYMENT_WINDOWS_LEGACY, _PAYMENT_WINDOWS_V2, _WRITE_OFF_DAYS_LEGACY, _WRITE_OFF_DAYS_V2; assert _PAYMENT_WINDOWS_LEGACY['on_time'] == (3, 15); assert _PAYMENT_WINDOWS_V2['residential']['on_time'] == (0, 3); assert _PAYMENT_WINDOWS_V2['commercial']['very_slow'] == (55, 85); assert _WRITE_OFF_DAYS_LEGACY == 90; assert _WRITE_OFF_DAYS_V2['residential'] == 60; print('OK')"`
Expected output: `OK`

- [ ] **Step 3: Confirm `_target_payment_date` and `_try_process` still reference the old name**

Run: `grep -n "_PAYMENT_WINDOWS\[" simulation/generators/payments.py`
Expected: one match inside `_target_payment_date` (the line `window = _PAYMENT_WINDOWS[profile]`). This reference will become a `NameError` — **expected** because `_PAYMENT_WINDOWS` no longer exists. Task 5 replaces this caller. Until then, the module will fail to execute payment logic.

Run: `grep -n "_WRITE_OFF_DAYS" simulation/generators/payments.py`
Expected: `_WRITE_OFF_DAYS_LEGACY` and `_WRITE_OFF_DAYS_V2` in the new block, plus one remaining bare `_WRITE_OFF_DAYS` inside `_try_process`. Task 6 replaces that caller.

**Do not commit yet.** The module is in a broken intermediate state. Proceed directly to Task 4.

---

## Task 4: Add `_write_off_threshold` helper with tests

**Files:**
- Create: new test class in `tests/test_simulation.py` (append at end of file before any `if __name__ == '__main__'` block)
- Modify: `simulation/generators/payments.py` (add helper function after the constants block, before the `_qbo_base_url_cache` line at approximately line 66)

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_simulation.py`:

```python
class TestTrackDWriteOffThreshold(unittest.TestCase):
    """Track D: per-client-type write-off threshold, flag-gated."""

    def test_legacy_threshold_residential(self):
        from simulation.generators.payments import _write_off_threshold
        with patch("intelligence.config.TRACK_D_PAYMENT_TIMING_ENABLED", False):
            self.assertEqual(_write_off_threshold("residential"), 90)

    def test_legacy_threshold_commercial(self):
        from simulation.generators.payments import _write_off_threshold
        with patch("intelligence.config.TRACK_D_PAYMENT_TIMING_ENABLED", False):
            self.assertEqual(_write_off_threshold("commercial"), 90)

    def test_v2_threshold_residential(self):
        from simulation.generators.payments import _write_off_threshold
        with patch("intelligence.config.TRACK_D_PAYMENT_TIMING_ENABLED", True):
            self.assertEqual(_write_off_threshold("residential"), 60)

    def test_v2_threshold_commercial(self):
        from simulation.generators.payments import _write_off_threshold
        with patch("intelligence.config.TRACK_D_PAYMENT_TIMING_ENABLED", True):
            self.assertEqual(_write_off_threshold("commercial"), 90)

    def test_v2_threshold_unknown_client_type(self):
        from simulation.generators.payments import _write_off_threshold
        with patch("intelligence.config.TRACK_D_PAYMENT_TIMING_ENABLED", True):
            # Unknown types fall back to 90 (matches _assign_profile's residential default)
            self.assertEqual(_write_off_threshold("industrial"), 90)

    def test_legacy_threshold_unknown_client_type(self):
        from simulation.generators.payments import _write_off_threshold
        with patch("intelligence.config.TRACK_D_PAYMENT_TIMING_ENABLED", False):
            self.assertEqual(_write_off_threshold("industrial"), 90)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_simulation.py::TestTrackDWriteOffThreshold -v 2>&1 | tail -30`
Expected: all 6 tests fail with `ImportError: cannot import name '_write_off_threshold' from 'simulation.generators.payments'` (or similar AttributeError).

- [ ] **Step 3: Implement `_write_off_threshold`**

In `simulation/generators/payments.py`, immediately after the constants block added in Task 3 (after the `_WRITE_OFF_DAYS_V2 = {...}` line) and before the `_qbo_base_url_cache: Optional[str] = None` line, add:

```python


def _write_off_threshold(client_type: str) -> int:
    """Return the write-off threshold in days for a given client type.

    Flag-gated: legacy returns 90 for everyone; V2 returns 60 for
    residential and 90 for commercial. Unknown types fall back to 90
    (matches the residential fallback used by _assign_profile).
    """
    from intelligence import config as intel_config

    if getattr(intel_config, "TRACK_D_PAYMENT_TIMING_ENABLED", False):
        return _WRITE_OFF_DAYS_V2.get(client_type, 90)
    return _WRITE_OFF_DAYS_LEGACY
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_simulation.py::TestTrackDWriteOffThreshold -v 2>&1 | tail -20`
Expected: all 6 tests pass.

- [ ] **Step 5: Commit**

Note: the module is still in a broken intermediate state (Task 3's removal of `_PAYMENT_WINDOWS` and `_WRITE_OFF_DAYS` left dangling references in `_target_payment_date` and `_try_process`). This commit bundles Task 3 + Task 4 because the module only returns to a runnable state after Task 5 and Task 6.

Do NOT commit here. Proceed to Task 5.

---

## Task 5: Extend `_target_payment_date` with V2 branch and tests

**Files:**
- Modify: `simulation/generators/payments.py:96-109` (`_target_payment_date` function)
- Modify: `tests/test_simulation.py` (append a second test class)

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_simulation.py` (after `TestTrackDWriteOffThreshold`):

```python
class TestTrackDTargetPaymentDate(unittest.TestCase):
    """Track D: _target_payment_date signature + V2 windows, flag-gated."""

    ISSUE = date(2026, 4, 1)

    # ----- Legacy path (flag=False): preserves pre-Track-D behavior -----

    def test_legacy_residential_on_time_in_window(self):
        from simulation.generators.payments import _target_payment_date
        with patch("intelligence.config.TRACK_D_PAYMENT_TIMING_ENABLED", False):
            for _ in range(200):
                d = _target_payment_date("residential", "on_time", self.ISSUE)
                offset = (d - self.ISSUE).days
                self.assertGreaterEqual(offset, 3)
                self.assertLessEqual(offset, 15)

    def test_legacy_commercial_on_time_same_as_residential(self):
        from simulation.generators.payments import _target_payment_date
        with patch("intelligence.config.TRACK_D_PAYMENT_TIMING_ENABLED", False):
            for _ in range(200):
                d = _target_payment_date("commercial", "on_time", self.ISSUE)
                offset = (d - self.ISSUE).days
                self.assertGreaterEqual(offset, 3)
                self.assertLessEqual(offset, 15)

    def test_legacy_non_payer_returns_none(self):
        from simulation.generators.payments import _target_payment_date
        with patch("intelligence.config.TRACK_D_PAYMENT_TIMING_ENABLED", False):
            self.assertIsNone(_target_payment_date("residential", "non_payer", self.ISSUE))
            self.assertIsNone(_target_payment_date("commercial", "non_payer", self.ISSUE))

    # ----- V2 path (flag=True): client-type-specific windows -----

    def test_v2_residential_on_time_in_window_and_early_skewed(self):
        from simulation.generators.payments import _target_payment_date
        with patch("intelligence.config.TRACK_D_PAYMENT_TIMING_ENABLED", True):
            offsets = []
            for _ in range(1000):
                d = _target_payment_date("residential", "on_time", self.ISSUE)
                offset = (d - self.ISSUE).days
                self.assertGreaterEqual(offset, 0)
                self.assertLessEqual(offset, 3)
                offsets.append(offset)
            mean = sum(offsets) / len(offsets)
            # beta(1, 2) mean is 1/3 of the window; with (0, 3) that's 1.0
            # day. Allow headroom to 1.5 so the test is not flaky.
            self.assertLessEqual(mean, 1.5,
                f"Residential on_time should cluster early, got mean offset {mean}")

    def test_v2_residential_slow_in_window(self):
        from simulation.generators.payments import _target_payment_date
        with patch("intelligence.config.TRACK_D_PAYMENT_TIMING_ENABLED", True):
            for _ in range(200):
                d = _target_payment_date("residential", "slow", self.ISSUE)
                offset = (d - self.ISSUE).days
                self.assertGreaterEqual(offset, 5)
                self.assertLessEqual(offset, 15)

    def test_v2_residential_very_slow_in_window(self):
        from simulation.generators.payments import _target_payment_date
        with patch("intelligence.config.TRACK_D_PAYMENT_TIMING_ENABLED", True):
            for _ in range(200):
                d = _target_payment_date("residential", "very_slow", self.ISSUE)
                offset = (d - self.ISSUE).days
                self.assertGreaterEqual(offset, 20)
                self.assertLessEqual(offset, 45)

    def test_v2_commercial_on_time_in_window_and_late_skewed(self):
        from simulation.generators.payments import _target_payment_date
        with patch("intelligence.config.TRACK_D_PAYMENT_TIMING_ENABLED", True):
            offsets = []
            for _ in range(1000):
                d = _target_payment_date("commercial", "on_time", self.ISSUE)
                offset = (d - self.ISSUE).days
                self.assertGreaterEqual(offset, 25)
                self.assertLessEqual(offset, 35)
                offsets.append(offset)
            mean = sum(offsets) / len(offsets)
            # beta(2, 1) mean is 2/3 of the window; with (25, 35) that's
            # ~31.7. Assert >= 30 for a safe late-skew check.
            self.assertGreaterEqual(mean, 30,
                f"Commercial on_time should cluster late, got mean offset {mean}")

    def test_v2_commercial_slow_in_window(self):
        from simulation.generators.payments import _target_payment_date
        with patch("intelligence.config.TRACK_D_PAYMENT_TIMING_ENABLED", True):
            for _ in range(200):
                d = _target_payment_date("commercial", "slow", self.ISSUE)
                offset = (d - self.ISSUE).days
                self.assertGreaterEqual(offset, 35)
                self.assertLessEqual(offset, 55)

    def test_v2_commercial_very_slow_in_window(self):
        from simulation.generators.payments import _target_payment_date
        with patch("intelligence.config.TRACK_D_PAYMENT_TIMING_ENABLED", True):
            for _ in range(200):
                d = _target_payment_date("commercial", "very_slow", self.ISSUE)
                offset = (d - self.ISSUE).days
                self.assertGreaterEqual(offset, 55)
                self.assertLessEqual(offset, 85)

    def test_v2_non_payer_returns_none(self):
        from simulation.generators.payments import _target_payment_date
        with patch("intelligence.config.TRACK_D_PAYMENT_TIMING_ENABLED", True):
            self.assertIsNone(_target_payment_date("residential", "non_payer", self.ISSUE))
            self.assertIsNone(_target_payment_date("commercial", "non_payer", self.ISSUE))

    def test_v2_unknown_client_type_falls_back_to_residential(self):
        from simulation.generators.payments import _target_payment_date
        with patch("intelligence.config.TRACK_D_PAYMENT_TIMING_ENABLED", True):
            for _ in range(100):
                d = _target_payment_date("industrial", "on_time", self.ISSUE)
                offset = (d - self.ISSUE).days
                self.assertGreaterEqual(offset, 0)
                self.assertLessEqual(offset, 3)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_simulation.py::TestTrackDTargetPaymentDate -v 2>&1 | tail -40`
Expected: all tests fail with `TypeError: _target_payment_date() takes 2 positional arguments but 3 were given`.

- [ ] **Step 3: Replace `_target_payment_date`**

Current `simulation/generators/payments.py:96-109`:

```python
def _target_payment_date(profile: str, issue_date: date) -> Optional[date]:
    """Return the day the client will pay, or None for non-payers.

    People procrastinate and pay near the end of their window: uses a
    beta(2, 1) distribution skewed toward the high end.
    """
    window = _PAYMENT_WINDOWS[profile]
    if window is None:
        return None
    lo, hi = window
    # beta(2, 1) skews toward 1.0 (end of window)
    fraction = random.betavariate(2, 1)
    days = lo + int(fraction * (hi - lo))
    return issue_date + timedelta(days=days)
```

Replace with:

```python
def _target_payment_date(
    client_type: str,
    profile: str,
    issue_date: date,
) -> Optional[date]:
    """Return the day the client will pay, or None for non-payers.

    Legacy (TRACK_D_PAYMENT_TIMING_ENABLED=False):
        Single blended window per profile, beta(2, 1) late-skewed draw.
        Residential and commercial share the same window; client_type is
        accepted but ignored so callers can be future-proof.

    V2 (TRACK_D_PAYMENT_TIMING_ENABLED=True):
        Client-type-specific windows matching invoice terms.
        Residential on_time uses beta(1, 2) to cluster near issue date
        (due on receipt). Everything else keeps beta(2, 1) so procrastination
        still skews draws toward the window's late edge.

    Unknown client_type falls back to residential windows (matches the
    fallback in _assign_profile).
    """
    from intelligence import config as intel_config

    if not getattr(intel_config, "TRACK_D_PAYMENT_TIMING_ENABLED", False):
        window = _PAYMENT_WINDOWS_LEGACY[profile]
        if window is None:
            return None
        lo, hi = window
        fraction = random.betavariate(2, 1)
        days = lo + int(fraction * (hi - lo))
        return issue_date + timedelta(days=days)

    windows = _PAYMENT_WINDOWS_V2.get(
        client_type, _PAYMENT_WINDOWS_V2["residential"]
    )
    window = windows[profile]
    if window is None:
        return None
    lo, hi = window

    if client_type == "residential" and profile == "on_time":
        # beta(1, 2) skews toward 0.0 — "due on receipt" pays near day 0-1
        fraction = random.betavariate(1, 2)
    else:
        # beta(2, 1) skews toward 1.0 — procrastination within window
        fraction = random.betavariate(2, 1)

    days = lo + int(fraction * (hi - lo))
    return issue_date + timedelta(days=days)
```

- [ ] **Step 4: Run the new tests to verify they pass**

Run: `python -m pytest tests/test_simulation.py::TestTrackDTargetPaymentDate -v 2>&1 | tail -40`
Expected: all 11 tests pass.

If the mean-offset assertions flake, re-run the full test file three times. beta draws are stochastic, but with N=1000 the headroom built into the bounds (1.5 and 30) should absorb normal variation. If a test flakes across three runs, that signals a real skew mismatch — recheck the betavariate parameters in Step 3.

- [ ] **Step 5: Verify the `_try_process` callsite is now broken**

Run: `python -m pytest tests/test_simulation.py -v -k "payments or Payment" 2>&1 | tail -20`
Expected: any tests that exercise `_try_process` end-to-end will fail because the callsite still passes 2 args. Task 6 fixes this. If no such tests exist, this step is informational only.

Run: `grep -n "_target_payment_date(profile" simulation/generators/payments.py`
Expected: one match at around line 231 — the stale 2-arg call inside `_try_process`. Confirms what Task 6 needs to fix.

**Do not commit yet.** Proceed to Task 6.

---

## Task 6: Wire new helpers into `_try_process` and remove legacy constant

**Files:**
- Modify: `simulation/generators/payments.py:208` (write-off callsite) and `:231` (payment-date callsite)

- [ ] **Step 1: Update the write-off threshold callsite inside `_try_process`**

Current line 208:
```python
            if days_outstanding >= _WRITE_OFF_DAYS:
```

Replace with:
```python
            if days_outstanding >= _write_off_threshold(client_type):
```

`client_type` is already resolved earlier in `_try_process` (at the line `client_type = client_row["client_type"] if client_row else "residential"`), so no extra lookup is needed.

- [ ] **Step 2: Update the payment-date callsite inside `_try_process`**

Current line 231:
```python
        target_date = _target_payment_date(profile, issue_date)
```

Replace with:
```python
        target_date = _target_payment_date(client_type, profile, issue_date)
```

- [ ] **Step 3: Confirm no bare `_WRITE_OFF_DAYS` constant or reference remains**

Run: `grep -n "_WRITE_OFF_DAYS\b" simulation/generators/payments.py`
Expected: no matches. The only matches should be `_WRITE_OFF_DAYS_LEGACY` and `_WRITE_OFF_DAYS_V2` (with suffixes).

If a bare `_WRITE_OFF_DAYS` still appears anywhere in the file, Task 3 Step 1 did not remove it cleanly. Delete it now.

- [ ] **Step 4: Confirm no bare `_PAYMENT_WINDOWS` reference remains**

Run: `grep -n "_PAYMENT_WINDOWS\b" simulation/generators/payments.py`
Expected: no matches. Only `_PAYMENT_WINDOWS_LEGACY` and `_PAYMENT_WINDOWS_V2` should remain.

- [ ] **Step 5: Run the full Track D test class**

Run: `python -m pytest tests/test_simulation.py::TestTrackDWriteOffThreshold tests/test_simulation.py::TestTrackDTargetPaymentDate -v 2>&1 | tail -30`
Expected: all 17 tests pass (6 write-off + 11 target-date).

- [ ] **Step 6: Run the full simulation test file to catch any regression**

Run: `python -m pytest tests/test_simulation.py -v 2>&1 | tail -30`
Expected: all tests pass. Any pre-existing tests that import `_PAYMENT_WINDOWS` or `_WRITE_OFF_DAYS` by the bare name will fail — if so, update them to the legacy-suffixed name. (Based on the grep in Task 3 Step 3, no such test imports existed before Track D.)

- [ ] **Step 7: Commit the full Track D bundle**

```bash
git add simulation/generators/payments.py tests/test_simulation.py
git commit -m "Add Track D client-type-specific payment timing (flag-gated)"
```

This commit includes the work from Task 3, Task 4, Task 5, and Task 6 together because the module is only in a runnable state after Task 6. Task 1 and Task 2 were committed independently and are safe-on-their-own.

---

## Task 7: Full regression sweep

**Files:** None modified. Verification only.

- [ ] **Step 1: Run the intelligence-layer tests**

Run: `python -m pytest tests/test_phase4.py -v 2>&1 | tail -40`
Expected: all tests pass. The `test_cash_collection_alert_*` suite should continue to pass because Task 2 preserved the identifier names in `revenue.py`.

- [ ] **Step 2: Run the automation tests**

Run: `python -m pytest tests/test_automations/ -v 2>&1 | tail -30`
Expected: all tests pass. Automations do not touch payment timing; any failure here is unrelated to Track D and should be investigated separately.

- [ ] **Step 3: Run the simulation tests**

Run: `python -m pytest tests/test_simulation.py tests/test_phase5_operations.py -v 2>&1 | tail -40`
Expected: all tests pass.

- [ ] **Step 4: Sanity-check both flag states end-to-end**

Run:
```bash
python -c "
from unittest.mock import patch
from datetime import date
from simulation.generators.payments import _target_payment_date, _write_off_threshold

issue = date(2026, 4, 1)

with patch('intelligence.config.TRACK_D_PAYMENT_TIMING_ENABLED', False):
    d = _target_payment_date('residential', 'on_time', issue)
    assert 3 <= (d - issue).days <= 15, f'legacy residential out of bounds: {d}'
    assert _write_off_threshold('residential') == 90
    assert _write_off_threshold('commercial') == 90
    print('legacy OK')

with patch('intelligence.config.TRACK_D_PAYMENT_TIMING_ENABLED', True):
    d = _target_payment_date('residential', 'on_time', issue)
    assert 0 <= (d - issue).days <= 3, f'V2 residential out of bounds: {d}'
    d = _target_payment_date('commercial', 'on_time', issue)
    assert 25 <= (d - issue).days <= 35, f'V2 commercial out of bounds: {d}'
    assert _write_off_threshold('residential') == 60
    assert _write_off_threshold('commercial') == 90
    print('V2 OK')
"
```
Expected output:
```
legacy OK
V2 OK
```

- [ ] **Step 5: No commit for this task**

This task is verification only. If all steps pass, the implementation is complete and ready for review. The branch is ready to open a PR with both flags False.

---

## Rollout (post-merge, not part of this plan)

Documented here as a reference. Each flag flip is a separate config-only PR.

1. Merge the PR from this plan. Both `TRACK_D_PAYMENT_TIMING_ENABLED` and `CASH_COLLECTION_ALERT_ENABLED` stay False. Behavior identical to today.
2. Verify Track A has been live in production for at least 5 business days. Check Railway deploy records or commit history of `intelligence/metrics/revenue.py` for the booked-vs-cash split landing date.
3. Config-only PR: flip `TRACK_D_PAYMENT_TIMING_ENABLED = True`. Deploy. Monitor 3–5 consecutive daily briefings for `collection_ratio` lift from ~0.45 toward the 0.70–1.05 band.
4. If the steady-state ratio sits consistently outside 0.70–1.05, adjust `EXPECTED_CASH_RATIO_LOW` or `EXPECTED_CASH_RATIO_HIGH` in the same config file. No code change.
5. Config-only PR: flip `CASH_COLLECTION_ALERT_ENABLED = True`. Deploy.

---

## Files touched summary

| File | Change | Task |
|------|--------|------|
| `intelligence/config.py` | +3 constants | 1 |
| `intelligence/metrics/revenue.py` | move 2 constants into imports | 2 |
| `simulation/generators/payments.py` | +2 window tables, +1 helper, 1 signature change, 2 callsites, −2 legacy constants | 3, 4, 5, 6 |
| `tests/test_simulation.py` | +2 test classes (17 tests total) | 4, 5 |
