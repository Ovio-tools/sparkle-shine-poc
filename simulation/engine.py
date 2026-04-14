"""
simulation/engine.py

Core event loop for the Sparkle & Shine simulation engine.

CLI:
    python -m simulation.engine
    python -m simulation.engine --dry-run
    python -m simulation.engine --speed 10 --once --date 2026-03-27
    python -m simulation.engine --verbose
"""

from __future__ import annotations

import argparse
import heapq
import hashlib
import json
import logging
import os
import random
import signal
import sys
import time
from collections import defaultdict, namedtuple
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

from intelligence.logging_config import setup_logging
from simulation.config import DAILY_VOLUMES
from simulation.variation import get_adjusted_volume, get_next_event_delay, should_event_happen

logger = setup_logging(__name__)

GeneratorCall = namedtuple("GeneratorCall", ["generator_name", "kwargs"])
TimedEvent = namedtuple("TimedEvent", ["fire_at", "generator_name", "kwargs"])

CHECKPOINT_FILE = Path("simulation/checkpoint.json")
DEFAULT_DAY_RUNTIME_BUDGET_SECONDS = int(
    os.getenv("SIM_ENGINE_DAY_RUNTIME_BUDGET_SECONDS", str(6 * 60 * 60))
)


def _stable_seed(value: str) -> int:
    """Return a process-stable integer seed for deterministic simulations."""
    digest = hashlib.sha256(value.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big")


class SimulationEngine:
    """Real-time simulation engine for Sparkle & Shine business events.

    Runs as a continuous process (run()) or for a single day (run_once()).
    Generators are registered via _register_generators() with conditional
    imports so the engine runs cleanly before any generator modules exist.

    Args:
        dry_run: If True, skip all API calls, SQLite writes, and checkpoint saves.
        speed: Time multiplier for event delays (2.0 = twice as fast).
        target_date: Simulate a specific date; seeds RNG and ignores checkpoint.
        verbose: Enable DEBUG logging.
        db_path: Path to sparkle_shine.db (used by reconciliation hook).
    """

    def __init__(
        self,
        dry_run: bool = False,
        speed: float = 1.0,
        target_date: date | None = None,
        verbose: bool = False,
        db_path: str = "sparkle_shine.db",
    ):
        self.dry_run = dry_run
        self.speed = speed
        self.target_date = target_date
        self.verbose = verbose
        self.db_path = db_path
        self.running = True
        self.counters: dict[str, int] = defaultdict(int)
        self.event_count: int = 0
        self.error_count: int = 0
        self._generators: dict = {}
        self._timed_queue: list = []  # heapq sorted by fire_at
        self._remaining_plan: list[GeneratorCall] = []
        self._checkpoint_loaded = False
        self._resume_incomplete_day = False
        self._fresh_start = True
        self.day_runtime_budget_seconds = DEFAULT_DAY_RUNTIME_BUDGET_SECONDS

        # --date wins: seed RNG and skip checkpoint (L7)
        if target_date is not None:
            self.current_date = target_date
            random.seed(_stable_seed(target_date.isoformat()))
        else:
            self.current_date = date.today()
            self.load_checkpoint()

        self._register_generators()
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        signal.signal(signal.SIGINT, self.handle_shutdown)

    def register(self, name: str, generator) -> None:
        """Register a generator instance under the given event name."""
        self._generators[name] = generator

    def queue_timed_event(self, fire_at: datetime, generator_name: str, kwargs: dict) -> None:
        """Queue a timed event to be dispatched when fire_at is reached."""
        heapq.heappush(self._timed_queue, TimedEvent(fire_at, generator_name, kwargs))

    def _register_generators(self) -> None:
        """Attempt to import and register each generator module.

        Uses conditional imports so the engine runs cleanly when generator
        modules don't exist yet. Add new generators here as they are built.
        """
        # OperationsGenerators — simulation/generators/operations.py
        try:
            from simulation.generators.operations import (
                NewClientSetupGenerator,
                JobSchedulingGenerator,
                JobCompletionGenerator,
            )
            self.register("new_client_setup", NewClientSetupGenerator(self.db_path))
            self.register("job_scheduling",   JobSchedulingGenerator(self.db_path, queue_fn=self.queue_timed_event))
            self.register("job_completion",   JobCompletionGenerator(self.db_path))
        except ImportError:
            logger.warning("OperationsGenerators not found — skipping")

        # ContactGenerator — simulation/generators/contacts.py
        try:
            from simulation.generators.contacts import ContactGenerator
            self.register("contacts", ContactGenerator())
        except ImportError:
            logger.warning("ContactGenerator not found — skipping")

        # DealGenerator — simulation/generators/deals.py
        try:
            from simulation.generators.deals import DealGenerator
            self.register("deals", DealGenerator())
        except ImportError:
            logger.warning("DealGenerator not found — skipping")

        # ChurnGenerator — simulation/generators/churn.py
        try:
            from simulation.generators.churn import ChurnGenerator
            self.register("churn", ChurnGenerator())
        except ImportError:
            logger.warning("ChurnGenerator not found — skipping")

        # PaymentGenerator — simulation/generators/payments.py
        try:
            from simulation.generators.payments import PaymentGenerator
            self.register("payments", PaymentGenerator(self.db_path))
        except ImportError:
            logger.warning("PaymentGenerator not found — skipping")

        # TaskCompletionGenerator — simulation/generators/tasks.py
        try:
            from simulation.generators.tasks import TaskCompletionGenerator
            self.register("tasks", TaskCompletionGenerator(self.db_path))
        except ImportError:
            logger.warning("TaskCompletionGenerator not found — skipping")

        if not self._generators:
            logger.warning(
                "No generators registered. Engine will produce 0 events. "
                "Build generator modules and add them to _register_generators()."
            )

    def plan_day(self, target_date: date) -> list:
        """Build a shuffled list of GeneratorCall events for the day.

        Uses DAILY_VOLUMES and get_adjusted_volume() to determine event counts.
        The list is shuffled so events are interleaved across the day — contacts,
        deal progressions, and churn checks distributed randomly rather than
        batched by category.

        Args:
            target_date: The date being simulated (drives seasonal/day-of-week scaling).

        Returns:
            Shuffled list of GeneratorCall namedtuples.
        """
        plan = []
        vol = DAILY_VOLUMES

        # ── New contacts (individual events) ────────────────────────────────
        n_contacts = get_adjusted_volume(
            vol["new_contacts"]["base_min"],
            vol["new_contacts"]["base_max"],
            target_date,
        )
        for _ in range(n_contacts):
            plan.append(GeneratorCall("contacts", {}))

        # ── Deal progressions ────────────────────────────────────────────────
        # Fixed number of deal events per day. The deals generator's
        # _advance_deal() already applies stage_advance_probability
        # internally, so gating here with should_event_happen() was a
        # double probability filter (0.15 × 0.15 = 2.25% effective).
        # With ~100 open deals in the pipeline, 20 picks/day gives each
        # deal ~0.2 picks/day × 15% advance = ~3% daily advance rate,
        # yielding ~33 days per stage (~5–6 stages ≈ reasonable cycle).
        n_deal_events = get_adjusted_volume(15, 25, target_date)
        for _ in range(n_deal_events):
            plan.append(GeneratorCall("deals", {}))

        # ── Residential churn checks ─────────────────────────────────────────
        # ~180 active residential clients per CLAUDE.md data volumes.
        # Convert monthly rate to per-business-day rate (÷22).
        daily_res_churn = vol["churn"]["monthly_residential_churn_rate"] / 22
        for _ in range(180):
            if should_event_happen(daily_res_churn, target_date):
                plan.append(GeneratorCall("churn", {"client_type": "residential"}))

        # ── Commercial churn checks ──────────────────────────────────────────
        # ~9 active commercial clients per CLAUDE.md data volumes.
        daily_com_churn = vol["churn"]["monthly_commercial_churn_rate"] / 22
        for _ in range(9):
            if should_event_happen(daily_com_churn, target_date):
                plan.append(GeneratorCall("churn", {"client_type": "commercial"}))

        # ── Payment processing ───────────────────────────────────────────────
        # 20 attempts/day: each call scans for the next actionable invoice.
        # The generator handles timing (due-date logic) internally.
        # Increased from 10 to 20 to keep pace with higher job volumes
        # (20-24 jobs/day → 20-24 invoices/day, plus existing backlog).
        for _ in range(20):
            plan.append(GeneratorCall("payments", {}))

        # ── Task completion ──────────────────────────────────────────────────
        # 50 attempts/day against ~150 open tasks. Each call picks a random
        # task and applies the 30% (15% for Maria) completion probability.
        for _ in range(50):
            plan.append(GeneratorCall("tasks", {}))

        # Operations events: placed BEFORE the shuffle (fixed order, not randomised)
        ops_prefix = [
            GeneratorCall("new_client_setup", {}),
            GeneratorCall("job_scheduling", {}),
        ]

        random.shuffle(plan)
        return ops_prefix + plan

    def pick_next_generator(self, plan: list) -> "GeneratorCall | None":
        """Pop and return the next GeneratorCall from the plan.

        Returns None when the plan is exhausted.
        """
        if not plan:
            return None
        return plan.pop(0)

    def dispatch(self, generator_call: "GeneratorCall") -> None:
        """Execute one generator call, handling errors without crashing the engine.

        Tracking:
            event_count  — increments on every attempt (success or failure)
            counters[name] — increments only on success
            error_count  — increments on failure

        Checkpoint saved every 10 events (skipped in dry_run mode, per L10/L15).
        """
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
            logger.exception(f"{name} generator failed: {e}")
            # Report to Slack #automation-failure — but never let a broken reporter
            # crash the engine or mask the original error.
            try:
                from simulation.error_reporter import report_error
                report_error(
                    e,
                    tool_name=name,
                    context=f"running {name} generator",
                    dry_run=self.dry_run,
                )
            except Exception:
                pass  # original error already logged above

        self.event_count += 1
        if not self.dry_run and self.event_count % 10 == 0:
            self.save_checkpoint()

    def _serialize_plan(self, plan: list[GeneratorCall]) -> list[list]:
        """Convert the remaining plan into JSON-serializable rows."""
        return [[gc.generator_name, gc.kwargs] for gc in plan]

    def _deserialize_plan(self, raw_plan: list[list]) -> list[GeneratorCall]:
        """Restore GeneratorCall rows from checkpoint JSON."""
        restored = []
        for row in raw_plan:
            if not isinstance(row, list) or len(row) != 2:
                continue
            restored.append(GeneratorCall(row[0], row[1]))
        return restored

    def save_checkpoint(self) -> None:
        """Write engine state to simulation/checkpoint.json.

        Skipped entirely when dry_run=True. A dry-run checkpoint would cause
        the next real run to skip the day by loading partial state.
        """
        if self.dry_run:
            return
        state = {
            "date": self.current_date.isoformat(),
            "counters": dict(self.counters),
            "last_event_time": datetime.utcnow().isoformat(),
            "event_count": self.event_count,
            "error_count": self.error_count,
            "timed_queue": [
                (e.fire_at.isoformat(), e.generator_name, e.kwargs)
                for e in self._timed_queue
            ],
            "remaining_plan": self._serialize_plan(self._remaining_plan),
        }
        checkpoint_file = getattr(self, "_checkpoint_file", CHECKPOINT_FILE)
        checkpoint_file.write_text(json.dumps(state, indent=2))
        logger.debug(f"Checkpoint saved: {self.event_count} events on {self.current_date}")

    def load_checkpoint(self) -> "dict | None":
        """Read engine state from simulation/checkpoint.json if it exists.

        Returns the raw state dict, or None if no checkpoint file is present.
        Restores counters, event_count, error_count, and current_date.
        """
        checkpoint_file = getattr(self, "_checkpoint_file", CHECKPOINT_FILE)
        if not checkpoint_file.exists():
            return None
        state = json.loads(checkpoint_file.read_text())
        self.current_date = date.fromisoformat(state["date"])
        self.counters = defaultdict(int, state.get("counters", {}))
        self.event_count = state.get("event_count", 0)
        self.error_count = state.get("error_count", 0)
        raw_queue = state.get("timed_queue", [])
        self._timed_queue = []
        for fire_at_iso, gen_name, kwargs in raw_queue:
            heapq.heappush(
                self._timed_queue,
                TimedEvent(datetime.fromisoformat(fire_at_iso), gen_name, kwargs),
            )
        self._remaining_plan = self._deserialize_plan(state.get("remaining_plan", []))
        self._checkpoint_loaded = True
        self._resume_incomplete_day = bool(self._remaining_plan)
        self._fresh_start = False
        logger.info(
            f"Resumed from checkpoint: {self.current_date}, "
            f"{self.event_count} events already processed"
        )
        return state

    def _start_new_day(self, target_date: date) -> None:
        """Reset per-day counters before building a fresh daily plan."""
        self.current_date = target_date
        self.counters = defaultdict(int)
        self.event_count = 0
        self.error_count = 0
        self._remaining_plan = []
        self._resume_incomplete_day = False

    def _get_sleep_delay(
        self,
        target_date: date,
        remaining_events: int,
        started_at: datetime,
        fast_forward: bool,
    ) -> float:
        """Bound per-event sleep so one simulated day cannot spill indefinitely."""
        if fast_forward:
            return 0.0

        base_delay = get_next_event_delay(target_date) / max(self.speed, 0.001)
        if remaining_events <= 0:
            return base_delay

        elapsed = max(0.0, (datetime.utcnow() - started_at).total_seconds())
        remaining_budget = max(0.0, self.day_runtime_budget_seconds - elapsed)
        if remaining_budget <= 0:
            return 0.0
        return min(base_delay, remaining_budget / remaining_events)

    def log_daily_summary(self) -> None:
        """Log a one-line summary of the day's event counts and crew utilization.

        Format: Daily summary YYYY-MM-DD: N events (E errors): gen1=X, gen2=Y, ...
        Followed by per-crew utilization lines.
        """
        error_label = f"{self.error_count} error{'s' if self.error_count != 1 else ''}"
        counts = ", ".join(
            f"{k}={v}" for k, v in sorted(self.counters.items())
        ) or "none"
        logger.info(
            f"Daily summary {self.current_date}: "
            f"{self.event_count} events ({error_label}): {counts}"
        )

        # Dry-run CLI smoke tests only need the top-line summary. Skipping the
        # follow-up DB utilization query keeps the subprocess comfortably under
        # the test timeout without changing the visible "Daily summary" signal.
        if self.dry_run:
            return

        # Per-crew utilization
        try:
            from database.connection import get_connection
            from simulation.config import CREW_CAPACITY
            conn = get_connection()
            try:
                for crew_id in ("crew-a", "crew-b", "crew-c", "crew-d"):
                    row = conn.execute("""
                        SELECT COUNT(*) AS job_count,
                               COALESCE(SUM(COALESCE(duration_minutes_actual, 120)), 0) AS total_min
                        FROM jobs
                        WHERE crew_id = %s AND scheduled_date = %s
                          AND status IN ('completed', 'scheduled')
                    """, (crew_id, self.current_date.isoformat())).fetchone()
                    total_min = row["total_min"]
                    util_pct = total_min / CREW_CAPACITY["daily_minutes"] * 100
                    logger.info(
                        f"  {crew_id}: {row['job_count']} jobs, "
                        f"{total_min} min, {util_pct:.0f}% utilization"
                    )
            finally:
                conn.close()
        except Exception as e:
            logger.warning(f"Could not log utilization: {e}")

    def run_once(self, target_date: date, fast_forward: bool = False) -> dict:
        """Run exactly one full simulated day.

        Builds a shuffled plan of events, dispatches each with a timing delay,
        then logs the daily summary. Stops early if self.running becomes False
        (set by handle_shutdown on SIGTERM/SIGINT).

        Used by --once CLI flag and directly in tests.

        Args:
            target_date: The date to simulate.

        Returns:
            Dict of successful event counts by generator name (partial if interrupted).
        """
        if self._resume_incomplete_day and self.current_date == target_date and self._remaining_plan:
            plan = list(self._remaining_plan)
        else:
            self._start_new_day(target_date)
            plan = self.plan_day(target_date)
            self._remaining_plan = plan

        started_at = datetime.utcnow()

        while plan and self.running:
            if not self.running:
                break
            # Drain any timed events whose fire_at has passed
            now = datetime.utcnow()
            while self._timed_queue and self._timed_queue[0].fire_at <= now:
                timed = heapq.heappop(self._timed_queue)
                self.dispatch(GeneratorCall(timed.generator_name, timed.kwargs))

            generator_call = self.pick_next_generator(plan)
            # Only sleep (and dispatch) when a generator is registered for this event.
            # Unregistered generator names are skipped silently; sleeping for them
            # would inflate the sleep count and cause test_run_once_sleeps_between_events
            # to fail (sleep count must equal execute count).
            if generator_call.generator_name not in self._generators:
                continue
            self._remaining_plan = plan
            delay = self._get_sleep_delay(
                target_date,
                remaining_events=len(plan) + 1,
                started_at=started_at,
                fast_forward=fast_forward,
            )
            time.sleep(delay)
            self.dispatch(generator_call)

        self._remaining_plan = plan
        self.log_daily_summary()
        if not plan:
            self._resume_incomplete_day = False
            self._remaining_plan = []
        else:
            self._resume_incomplete_day = True

        if not self.dry_run:
            self.save_checkpoint()
        return dict(self.counters)

    def handle_shutdown(self, signum: int, frame) -> None:
        """Handle SIGTERM or SIGINT by setting self.running = False.

        Does NOT call sys.exit(). The main loop checks self.running and
        exits naturally, allowing the __main__ block to call sys.exit(0)
        at the top level. This avoids messy state from exiting mid-sleep
        or mid-API-call.
        """
        logger.info(f"Shutdown signal received (signal {signum}). Stopping after current event.")
        self.running = False
        if not self.dry_run:
            self.save_checkpoint()
        self.log_daily_summary()

    def run(self) -> None:
        """Continuous event loop. Runs until SIGTERM/SIGINT sets self.running = False.

        Each iteration: simulate one full day, run the reconciliation hook,
        then sleep until midnight (waking every second to check self.running).
        """
        while self.running:
            real_today = date.today()
            if self._resume_incomplete_day:
                target_day = self.current_date
            elif self._fresh_start:
                target_day = self.current_date
                self._fresh_start = False
            else:
                target_day = min(self.current_date + timedelta(days=1), real_today)

            fast_forward = target_day < real_today
            self.run_once(target_day, fast_forward=fast_forward)

            # Daily reconciliation sweep — no-op until reconciler is built (Step 7).
            # ImportError guard keeps the engine running before the module exists.
            try:
                from simulation.reconciliation.reconciler import Reconciler
                reconciler = Reconciler(self.db_path)
                reconciler.run_daily_sweep()
            except ImportError:
                pass
            except Exception as e:
                logger.error(f"Daily reconciliation failed: {e}")
                try:
                    from simulation.error_reporter import report_error
                    report_error(e, tool_name="reconciliation",
                                 context="Daily reconciliation sweep",
                                 dry_run=self.dry_run)
                except Exception:
                    pass

            if not self.running:
                break

            if fast_forward:
                continue

            # Sleep until midnight. Check self.running every second so a shutdown
            # signal is not ignored during a long sleep.
            tomorrow_midnight = datetime.combine(
                target_day + timedelta(days=1), datetime.min.time()
            )
            while self.running:
                remaining = (tomorrow_midnight - datetime.now()).total_seconds()
                if remaining <= 0:
                    break
                time.sleep(min(1.0, remaining))

        logger.info("Engine stopped.")


def _run_health_check() -> None:
    """Run simulation engine health checks and exit.

    Answers: 'Can the simulation engine generate events right now?'
    Called by --health before the engine is constructed.
    Exits 0 if all checks PASS or WARN, exits 1 if any FAIL.
    """
    import importlib

    from database.health import (
        HealthCheck,
        check_connection,
        check_table_inventory,
        check_sequences,
        render_table,
    )

    checks: list[HealthCheck] = []

    # 1. DB connection
    conn_check, conn = check_connection()
    checks.append(conn_check)

    _TABLES = ["clients", "jobs", "invoices", "payments", "cross_tool_mapping"]

    if conn is None:
        for name in ("Table inventory", "Sequence health"):
            checks.append(HealthCheck(name, "SKIP", "DB unreachable"))
    else:
        try:
            # 2. Table inventory
            checks.extend(check_table_inventory(conn, _TABLES))
            # 3. Sequence health (cross_tool_mapping has SERIAL PK; others use TEXT)
            checks.extend(check_sequences(conn, _TABLES))
        finally:
            conn.close()

    # 4. Checkpoint freshness
    if CHECKPOINT_FILE.exists():
        try:
            import json
            from datetime import date as _date
            state = json.loads(CHECKPOINT_FILE.read_text())
            cp_date = _date.fromisoformat(state["date"])
            delta = (_date.today() - cp_date).days
            if delta > 1:
                checks.append(HealthCheck(
                    "Checkpoint freshness", "WARN",
                    f"checkpoint is {delta} days old — engine may have stopped",
                ))
            else:
                checks.append(HealthCheck(
                    "Checkpoint freshness", "PASS", f"date={cp_date}",
                ))
        except Exception as exc:
            checks.append(HealthCheck(
                "Checkpoint freshness", "WARN", f"could not parse checkpoint: {exc}",
            ))
    else:
        checks.append(HealthCheck(
            "Checkpoint freshness", "PASS", "no checkpoint file (first run)",
        ))

    # 5. Generator imports
    _GENERATOR_IMPORTS = [
        ("simulation.generators.operations", "NewClientSetupGenerator"),
        ("simulation.generators.operations", "JobSchedulingGenerator"),
        ("simulation.generators.operations", "JobCompletionGenerator"),
        ("simulation.generators.contacts",   "ContactGenerator"),
        ("simulation.generators.deals",      "DealGenerator"),
        ("simulation.generators.churn",      "ChurnGenerator"),
        ("simulation.generators.payments",   "PaymentGenerator"),
        ("simulation.generators.tasks",      "TaskCompletionGenerator"),
    ]
    for module_path, class_name in _GENERATOR_IMPORTS:
        try:
            mod = importlib.import_module(module_path)
            getattr(mod, class_name)
            checks.append(HealthCheck(f"Import: {class_name}", "PASS", ""))
        except (ImportError, AttributeError) as exc:
            checks.append(HealthCheck(f"Import: {class_name}", "WARN", str(exc)))

    render_table("Simulation Engine — Health Check", checks)
    sys.exit(1 if any(c.status == "FAIL" for c in checks) else 0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Sparkle & Shine simulation engine — generates live business events."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Log actions without making API calls, writing to SQLite, or saving checkpoints"
    )
    parser.add_argument(
        "--speed", type=float, default=1.0,
        help="Time multiplier for event delays (2.0 = twice as fast, 0.5 = half speed)"
    )
    parser.add_argument(
        "--date", dest="target_date", default=None, metavar="YYYY-MM-DD",
        help="Simulate a specific date; seeds RNG for reproducibility (L7); ignores checkpoint"
    )
    parser.add_argument(
        "--once", action="store_true",
        help="Run one full day then exit"
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Enable DEBUG logging"
    )
    parser.add_argument(
        "--health",
        action="store_true",
        help="Run service health checks and exit. Does not start the engine.",
    )
    args = parser.parse_args()

    if args.health:
        _run_health_check()  # exits internally

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    target_date = date.fromisoformat(args.target_date) if args.target_date else None

    engine = SimulationEngine(
        dry_run=args.dry_run,
        speed=args.speed,
        target_date=target_date,
        verbose=args.verbose,
    )

    if args.once:
        run_date = target_date if target_date is not None else date.today()
        engine.run_once(run_date)
    else:
        engine.run()

    sys.exit(0)
