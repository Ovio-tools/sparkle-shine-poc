# Simulation Engine Framework — Design Spec
**Date:** 2026-03-27
**Status:** Approved — ready for implementation planning

---

## What We're Building

A real-time simulation engine (`simulation/`) that generates forward-looking business events for Sparkle & Shine Cleaning Co. It runs continuously in the background, producing new contacts, deal progressions, completed jobs, payments, and churn events — injecting them into the same SaaS tools the automation runner watches. The automation runner then fires its cross-tool workflows in response, exactly as it would with real data.

This is Phase 5 of the POC. The seeding scripts (Phase 2) filled in 12 months of history. The simulation engine fills in the present and future, one day at a time.

---

## Files

| File | Action | Description |
|------|--------|-------------|
| `simulation/__init__.py` | Create | Empty — makes `simulation` a package |
| `simulation/config.py` | Create | All calibrated config values: daily volumes, churn rates, seasonal weights, job variety |
| `simulation/variation.py` | Create | Daily multiplier, adjusted volumes, probability checks, Poisson-like event timing |
| `simulation/engine.py` | Create | `SimulationEngine` class — full event loop implementation |
| `intelligence/config.py` | Update | Replace `REVENUE_TARGETS` to match the actual ramp-up trajectory |

---

## simulation/config.py

All numeric values carry inline source citations or `ESTIMATED -- reasoning:` comments (project convention L1). After any change, `config_math_trace()` must be run to verify the business is still growing at net +3 to +5 clients/month (L2).

Key config blocks:

- **`DAILY_VOLUMES`** — new contacts (base 3–8/day), SQL fraction (0.35), lifecycle distribution, deal stage advance probability (0.15), won probability from negotiation (0.40), loss rate per stage (0.03), churn rates (residential 2.5%/month, commercial 1.5%/month), task completion rates.
- **`JOB_VARIETY`** — residential recurring (85% regular, 10% deep clean, 5% add-on), commercial recurring (90% standard, 10% extras), seasonal deep clean boost by month.
- **`BUSINESS_HOURS`** — 7–18, peak 9–14.
- **`SEASONAL_WEIGHTS`** — monthly multipliers (summer high, Jan low).
- **`DAY_OF_WEEK_WEIGHTS`** — Mon high, weekend low.
- **`SERVICE_TYPE_WEIGHTS`, `COMMERCIAL_SERVICE_WEIGHTS`, `CREW_ASSIGNMENT_WEIGHTS`** — probability distributions for scheduling variety.

The `config_math_trace()` function prints expected monthly outcomes (contacts → SQLs → pipeline → wins − churn = net change) to verify calibration.

---

## simulation/variation.py

Four functions that apply seasonal + day-of-week + noise multipliers:

- `get_daily_multiplier(target_date)` — combines seasonal weight, day-of-week weight, and `random.uniform(0.85, 1.15)` noise. Called with a specific date, the RNG state is already seeded so this is reproducible.
- `get_adjusted_volume(base_min, base_max, target_date)` — picks a random base count and scales it by the daily multiplier. Minimum 0.
- `should_event_happen(probability, target_date)` — multiplies probability by daily multiplier, returns bool. Used for low-frequency events like churn or add-ons.
- `get_next_event_delay(target_date)` — returns seconds until the next event. Peak hours (9–14): 3–15 min. Business hours (7–18): 10–30 min. Off-hours: 45–120 min. Divided by daily multiplier (busier days = shorter delays). Minimum 30s.

---

## simulation/engine.py — SimulationEngine

### Initialization

```python
SimulationEngine(
    dry_run: bool = False,
    speed: float = 1.0,
    target_date: date | None = None,
    verbose: bool = False
)
```

**Checkpoint vs. `--date` interaction (critical):**
- If `target_date` is provided: skip `load_checkpoint()`, seed `random.seed(hash(str(target_date)))` (L7). The explicit date is the authority; a stale checkpoint would fight deterministic seeding.
- If `target_date` is None (continuous mode): call `load_checkpoint()` if `simulation/checkpoint.json` exists. No RNG seed (let the OS entropy drive it).

Set `self.running = True`. After resolving checkpoint/date: call `_register_generators()`. Register signal handlers for SIGTERM and SIGINT.

### `_register_generators()`

Called from `__init__`. Uses conditional imports so the engine runs cleanly before generator modules exist:

```python
try:
    from simulation.generators.contacts import ContactGenerator
    self.register("contacts", ContactGenerator())
except ImportError:
    logger.warning("ContactGenerator not found — skipping")
# ... repeat for each generator
```

Zero registered generators → log a warning, continue. This lets the framework be smoke-tested before any generators are written.

### `run()`

Continuous loop for production use. Checks `while self.running:` before each iteration.

1. Call `run_once(today)`.
2. Run the daily reconciliation hook (see below).
3. Sleep until midnight (checking `self.running` periodically so a shutdown signal isn't ignored during the sleep).
4. Repeat with tomorrow's date.

**Daily reconciliation hook** — runs after every `run_once()` completes, before sleeping:

```python
try:
    from simulation.reconciliation.reconciler import Reconciler
    reconciler = Reconciler(self.db_path)
    reconciler.daily_sweep()
except ImportError:
    pass  # Reconciler not yet built
except Exception as e:
    logger.error(f"Daily reconciliation failed: {e}")
    # Don't crash the engine over a failed sweep
```

The `ImportError` guard keeps the engine running cleanly until the reconciler is built in Step 7.

### `run_once(target_date) -> dict`

Runs exactly one full simulated day. Used by `--once` flag and directly in tests.

1. Call `plan_day(target_date)` → shuffled list of `GeneratorCall` objects.
2. Loop until list is empty **or `self.running` is False**:
   - Check `if not self.running: break` before each dispatch — allows a signal mid-day to exit cleanly with partial counts.
   - Pop the next `GeneratorCall` via `pick_next_generator()`.
   - Sleep `get_next_event_delay(target_date) / self.speed`.
   - Call `dispatch(generator_call)`.
3. Call `log_daily_summary()`.
4. Return event counts accumulated so far (partial counts if interrupted).

### `plan_day(target_date) -> list[GeneratorCall]`

Builds the day's event list from `DAILY_VOLUMES` and `get_adjusted_volume()`:

- Uses `get_adjusted_volume(base_min, base_max, target_date)` to determine how many new contacts to generate.
- Uses `should_event_happen()` for low-probability per-entity events (churn, add-ons).
- Each entry is a `GeneratorCall(generator_name: str, kwargs: dict)` namedtuple.
- **Shuffles the list before returning.** Events must be interleaved across the day (a contact at 8:15, a job completion at 9:40, a payment at 10:00), not batched by category. Without shuffling, the timing pattern looks nothing like a real business.

### `pick_next_generator(plan) -> GeneratorCall | None`

Simple `plan.pop(0)`. Returns `None` when the list is empty.

### `dispatch(generator_call)`

`self.event_count` increments on every attempt (tracks throughput). `self.counters[name]` increments only on success (tracks output). `self.error_count` tracks failures. All three are reported in the daily summary.

```python
def dispatch(self, generator_call):
    name = generator_call.generator_name
    generator = self._generators.get(name)
    if not generator:
        logger.warning(f"No generator registered for '{name}', skipping")
        return

    try:
        generator.execute(dry_run=self.dry_run, **generator_call.kwargs)
        self.counters[name] += 1
    except Exception as e:
        self.error_count += 1
        logger.exception(f"{name} failed: {e}")
        try:
            from simulation.error_reporter import report_error
            report_error(e, tool_name=name,
                context=f"running {name} generator",
                dry_run=self.dry_run)
        except Exception:
            pass  # already logged above

    self.event_count += 1
    if not self.dry_run and self.event_count % 10 == 0:
        self.save_checkpoint()
```

### `save_checkpoint()` / `load_checkpoint()`

- **`save_checkpoint()`** writes `simulation/checkpoint.json`:
  ```json
  {
      "date": "2026-03-27",
      "counters": {"contacts": 4, "deals": 1, "operations": 12},
      "last_event_time": "2026-03-27T14:23:00",
      "event_count": 17
  }
  ```
  **Skipped entirely when `dry_run=True`.** A dry-run that saves a checkpoint would cause the next real run to skip the day, since it would load a checkpoint saying the day is already partially done.

- **`load_checkpoint()`** reads the file if it exists, restores `self.counters`, `self.event_count`, and `self.current_date`. Returns `None` if no checkpoint exists.

### `log_daily_summary()`

Logs at INFO level: date, total events attempted, error count, and success breakdown by generator type. Example:
```
Daily summary 2026-03-27: 47 events (5 errors): contacts=5, deals=8, operations=22, payments=9, churn=2, tasks=1
```

### `handle_shutdown(signum, frame)`

Called on SIGTERM or SIGINT. Does **not** call `sys.exit()` — instead sets a flag and returns, allowing the main loop to exit naturally:

1. Set `self.running = False`.
2. If `not self.dry_run`: `save_checkpoint()` (L10).
3. `log_daily_summary()`.

`run_once()` and `run()` both check `self.running` and exit their loops on the next iteration. The `__main__` block calls `sys.exit(0)` after `run()` returns, if needed. This avoids messy state from calling `sys.exit()` mid-sleep or mid-API-call.

### CLI (`__main__` block)

```
python -m simulation.engine
    --dry-run       Log what would happen, no API calls or SQLite writes, no checkpoint saves
    --speed N       Time multiplier (2.0 = twice as fast, 0.5 = half speed)
    --date DATE     Simulate specific date; seeds RNG for reproducibility (L7); ignores checkpoint
    --once          Run one full day then exit (calls run_once() instead of run())
    --verbose       Set logging to DEBUG
```

The `__main__` block: parse args → construct `SimulationEngine` → call `run_once(target_date)` if `--once`, else `run()`.

---

## intelligence/config.py — REVENUE_TARGETS update

Replace the existing `REVENUE_TARGETS` dict with values that match the actual ramp-up trajectory (the existing targets assumed maturity from April 2025, which caused 11/12 months to fail). New values:

```python
REVENUE_TARGETS = {
    # Historical months (seeded data)
    (2025, 4): (18000, 30000),      # Ramp-up: minimal commercial
    (2025, 5): (35000, 55000),      # Ramp-up: growing
    (2025, 6): (65000, 85000),      # Summer surge starts
    (2025, 7): (75000, 95000),      # Summer peak
    (2025, 8): (75000, 95000),      # Post-summer
    (2025, 9): (90000, 110000),     # Approaching maturity
    (2025, 10): (105000, 125000),   # Big commercial win month
    (2025, 11): (110000, 130000),   # Stabilization
    (2025, 12): (120000, 145000),   # Holiday peak
    (2026, 1): (115000, 135000),    # January dip
    (2026, 2): (110000, 130000),    # Recovery
    (2026, 3): (115000, 135000),    # Recovery
    # Forward months (simulation era)
    (2026, 4): (125000, 150000),
    (2026, 5): (130000, 155000),
    (2026, 6): (145000, 175000),    # Summer surge
    (2026, 7): (150000, 180000),
    (2026, 8): (140000, 165000),
    (2026, 9): (125000, 150000),    # Seasonal dip
    (2026, 10): (135000, 160000),
    (2026, 11): (140000, 170000),
    (2026, 12): (155000, 185000),   # Holiday peak
}
```

All other constants in `intelligence/config.py` are unchanged.

---

## Constraints (from project-conventions.md)

- **Never write to `poll_state`** — the automation runner owns those watermarks.
- **Never create records the runner also creates** (Pipedrive deals from HubSpot SQLs, Asana onboarding tasks, QuickBooks invoices from completed jobs). The simulation injects at the source tool; the runner handles downstream creation.
- **Register mappings only for the tool you wrote to** — don't pre-register Pipedrive mappings when creating HubSpot contacts.
- **Auth via `auth.get_client(tool_name)`** exclusively. Never import `credentials.py` directly.
- **Imports**: `from database.schema import ...` and `from database.mappings import ...` (not `db.`).
- **Every numeric config value** must have a `# Source:` or `# ESTIMATED -- reasoning:` comment (L1).
- **`--dry-run`** skips API calls, SQLite writes, checkpoint saves, and Slack posts.

---

## What This Spec Does NOT Cover

The generator modules (`simulation/generators/contacts.py`, `churn.py`, etc.) are out of scope for this spec. This spec covers only the framework — the engine, config, variation math, and intelligence config update. Generators will be built in subsequent steps and registered into `_register_generators()`.
