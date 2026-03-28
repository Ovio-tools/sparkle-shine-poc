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
import json
import logging
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

        # --date wins: seed RNG and skip checkpoint (L7)
        if target_date is not None:
            self.current_date = target_date
            random.seed(hash(str(target_date)))
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
        # Estimate active pipeline: ~30 deals at any given time.
        # Derived from: sqls/month (~42) × avg cycle (~33 days / 30 days) ≈ 46;
        # using conservative 30 to avoid over-generating events on slow days.
        # The deals generator queries the database for actual open deals.
        deal_config = vol["deal_progression"]
        for _ in range(30):
            if should_event_happen(deal_config["stage_advance_probability"], target_date):
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
        logger.info(
            f"Resumed from checkpoint: {self.current_date}, "
            f"{self.event_count} events already processed"
        )
        return state

    def log_daily_summary(self) -> None:
        """Log a one-line summary of the day's event counts.

        Format: Daily summary YYYY-MM-DD: N events (E errors): gen1=X, gen2=Y, ...
        """
        error_label = f"{self.error_count} error{'s' if self.error_count != 1 else ''}"
        counts = ", ".join(
            f"{k}={v}" for k, v in sorted(self.counters.items())
        ) or "none"
        logger.info(
            f"Daily summary {self.current_date}: "
            f"{self.event_count} events ({error_label}): {counts}"
        )

    def run_once(self, target_date: date) -> dict:
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
        self.current_date = target_date
        plan = self.plan_day(target_date)

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
            delay = get_next_event_delay(target_date) / max(self.speed, 0.001)
            time.sleep(delay)
            self.dispatch(generator_call)

        self.log_daily_summary()
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
            today = date.today()
            self.run_once(today)

            # Daily reconciliation sweep — no-op until reconciler is built (Step 7).
            # ImportError guard keeps the engine running before the module exists.
            try:
                from simulation.reconciliation.reconciler import Reconciler
                reconciler = Reconciler(self.db_path)
                reconciler.daily_sweep()
            except ImportError:
                pass
            except Exception as e:
                logger.error(f"Daily reconciliation failed: {e}")

            if not self.running:
                break

            # Sleep until midnight. Check self.running every second so a shutdown
            # signal is not ignored during a long sleep.
            tomorrow_midnight = datetime.combine(
                today + timedelta(days=1), datetime.min.time()
            )
            while self.running:
                remaining = (tomorrow_midnight - datetime.now()).total_seconds()
                if remaining <= 0:
                    break
                time.sleep(min(1.0, remaining))

        logger.info("Engine stopped.")


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
    args = parser.parse_args()

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
