# Operations Generator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `simulation/generators/operations.py` with three generator classes (`NewClientSetupGenerator`, `JobSchedulingGenerator`, `JobCompletionGenerator`) plus the engine extensions (timed event queue, checkpoint serialization, plan ordering) they depend on.

**Architecture:** The engine gains a heapq-based timed event queue so `JobSchedulingGenerator` can fire completion events at realistic times during the day. `NewClientSetupGenerator` reads `won_deals` (written by `deals.py`) to set up new Jobber clients on their contract start date. `JobCompletionGenerator` is dispatched by timed events and applies outcome probabilities, recording results in SQLite.

**Tech Stack:** Python 3, SQLite (sqlite3), Jobber GraphQL API, Pipedrive REST API, heapq, unittest with unittest.mock

---

## File Map

| Action | Path | Responsibility |
|--------|------|----------------|
| Modify | `simulation/engine.py` | TimedEvent queue, checkpoint serialization, plan_day ordering, generator registration |
| Create | `simulation/generators/operations.py` | All three generator classes + module-level helpers |
| Create | `tests/test_phase5_operations.py` | All unit, mock, and integration tests |

---

## Key Constants (reference throughout)

```python
# Jobber GraphQL
_JOBBER_GQL_URL = "https://api.getjobber.com/api/graphql"
_JOBBER_VERSION_HEADER = {"X-JOBBER-GRAPHQL-VERSION": "2026-03-10"}

# Crew window starts (datetime.time objects, used in _assign_scheduled_time)
_CREW_WINDOW = {
    "crew-a": (7, 0),   # 7:00 AM
    "crew-b": (7, 30),  # 7:30 AM
    "crew-c": (8, 0),   # 8:00 AM
    "crew-d": (17, 0),  # 5:00 PM
}

# service_frequency → recurring_agreements.frequency
_FREQ_TO_RECUR_FREQ = {
    "weekly_recurring":  "weekly",
    "biweekly_recurring": "biweekly",
    "monthly_recurring": "monthly",
    "nightly_clean":     "weekly",   # commercial nightly modelled as weekly per-visit
    "weekend_deep_clean": "weekly",
}

# service_frequency → service_type_id (config/business.py)
_FREQ_TO_SERVICE_ID = {
    "weekly_recurring":     "recurring-weekly",
    "biweekly_recurring":   "recurring-biweekly",
    "monthly_recurring":    "recurring-monthly",
    "nightly_clean":        "commercial-nightly",
    "weekend_deep_clean":   "deep-clean",
    "one_time_standard":    "std-residential",
    "one_time_deep_clean":  "deep-clean",
    "one_time_move_in_out": "move-in-out",
    "one_time_project":     "commercial-nightly",  # closest available
}

# service_type_id → duration_minutes (from config/business.py SERVICE_TYPES)
_DURATION_MAP = {
    "std-residential":    120,
    "deep-clean":         210,
    "move-in-out":        240,
    "recurring-weekly":   120,
    "recurring-biweekly": 120,
    "recurring-monthly":  120,
    "commercial-nightly": 180,
}

# Jobber Recurrence config: recurring_agreements.frequency → jobCreate input
_JOBBER_FREQ_MAP = {
    "weekly":   {"type": "WEEKLY",  "interval": 1},
    "biweekly": {"type": "WEEKLY",  "interval": 2},
    "monthly":  {"type": "MONTHLY", "interval": 1},
}
```

---

## Task 1: Engine — TimedEvent queue

**Files:**
- Modify: `simulation/engine.py`
- Test: `tests/test_phase5_operations.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_phase5_operations.py` with:

```python
"""tests/test_phase5_operations.py

NOTE: These tests will be consolidated into tests/test_simulation.py during
Step 10 (Phase 5 polish). Whoever builds that step should look here first.
"""
import heapq
import json
import os
import random
import sqlite3
import unittest
from collections import namedtuple
from datetime import date, datetime, time, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch, call

# ── Engine tests ──────────────────────────────────────────────────────────────

class TestTimedEventQueue(unittest.TestCase):
    def setUp(self):
        from simulation.engine import SimulationEngine, TimedEvent
        self.engine = SimulationEngine(dry_run=True)
        self.TimedEvent = TimedEvent

    def test_timed_queue_initialized_empty(self):
        self.assertEqual(self.engine._timed_queue, [])

    def test_queue_timed_event_adds_to_heap(self):
        fire_at = datetime(2026, 3, 28, 10, 0)
        self.engine.queue_timed_event(fire_at, "job_completion", {"job_id": "SS-JOB-0001"})
        self.assertEqual(len(self.engine._timed_queue), 1)
        event = self.engine._timed_queue[0]
        self.assertEqual(event.fire_at, fire_at)
        self.assertEqual(event.generator_name, "job_completion")
        self.assertEqual(event.kwargs, {"job_id": "SS-JOB-0001"})

    def test_queue_maintains_heap_order(self):
        t1 = datetime(2026, 3, 28, 14, 0)
        t2 = datetime(2026, 3, 28, 9, 0)
        t3 = datetime(2026, 3, 28, 11, 30)
        self.engine.queue_timed_event(t1, "job_completion", {})
        self.engine.queue_timed_event(t2, "job_completion", {})
        self.engine.queue_timed_event(t3, "job_completion", {})
        # heapq smallest at index 0
        self.assertEqual(self.engine._timed_queue[0].fire_at, t2)
```

- [ ] **Step 2: Run — expect ImportError on `TimedEvent`**

```bash
cd sparkle-shine-poc && python -m pytest tests/test_phase5_operations.py::TestTimedEventQueue -v 2>&1 | head -30
```
Expected: `ImportError: cannot import name 'TimedEvent'`

- [ ] **Step 3: Add TimedEvent + queue to engine.py**

In `simulation/engine.py`, add after the `GeneratorCall` namedtuple (line 33):

```python
import heapq

TimedEvent = namedtuple("TimedEvent", ["fire_at", "generator_name", "kwargs"])
```

In `SimulationEngine.__init__`, after `self._generators: dict = {}`:

```python
self._timed_queue: list = []  # heapq sorted by fire_at
```

Add new method after `register()`:

```python
def queue_timed_event(self, fire_at: datetime, generator_name: str, kwargs: dict) -> None:
    """Queue a timed event to be dispatched when fire_at is reached."""
    heapq.heappush(self._timed_queue, TimedEvent(fire_at, generator_name, kwargs))
```

- [ ] **Step 4: Run — expect PASS**

```bash
python -m pytest tests/test_phase5_operations.py::TestTimedEventQueue -v
```
Expected: 3 tests PASS

- [ ] **Step 5: Commit**

```bash
git add simulation/engine.py tests/test_phase5_operations.py
git commit -m "feat: add TimedEvent queue to simulation engine"
```

---

## Task 2: Engine — checkpoint serialization for timed queue

**Files:**
- Modify: `simulation/engine.py`
- Test: `tests/test_phase5_operations.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/test_phase5_operations.py`:

```python
class TestTimedQueueCheckpoint(unittest.TestCase):
    def setUp(self):
        from simulation.engine import SimulationEngine
        self._tmp = Path("/tmp/test_checkpoint_ops.json")
        self.engine = SimulationEngine(dry_run=False)
        self.engine._checkpoint_file = self._tmp

    def tearDown(self):
        if self._tmp.exists():
            self._tmp.unlink()

    def test_save_checkpoint_includes_timed_queue(self):
        fire_at = datetime(2026, 3, 28, 14, 30)
        self.engine.queue_timed_event(fire_at, "job_completion", {"job_id": "SS-JOB-0001"})
        self.engine.save_checkpoint()
        state = json.loads(self._tmp.read_text())
        self.assertIn("timed_queue", state)
        self.assertEqual(len(state["timed_queue"]), 1)
        entry = state["timed_queue"][0]
        self.assertEqual(entry[0], fire_at.isoformat())
        self.assertEqual(entry[1], "job_completion")
        self.assertEqual(entry[2], {"job_id": "SS-JOB-0001"})

    def test_load_checkpoint_restores_timed_queue(self):
        fire_at = datetime(2026, 3, 28, 14, 30)
        state = {
            "date": "2026-03-28",
            "counters": {},
            "event_count": 0,
            "error_count": 0,
            "timed_queue": [[fire_at.isoformat(), "job_completion", {"job_id": "SS-JOB-0001"}]],
        }
        self._tmp.write_text(json.dumps(state))
        engine2 = SimulationEngine(dry_run=True)
        engine2._checkpoint_file = self._tmp
        engine2._timed_queue = []
        engine2.load_checkpoint()
        self.assertEqual(len(engine2._timed_queue), 1)
        self.assertEqual(engine2._timed_queue[0].generator_name, "job_completion")
        self.assertEqual(engine2._timed_queue[0].fire_at, fire_at)
```

- [ ] **Step 2: Run — expect FAIL**

```bash
python -m pytest tests/test_phase5_operations.py::TestTimedQueueCheckpoint -v 2>&1 | head -20
```
Expected: `AssertionError: 'timed_queue' not in {...}`

- [ ] **Step 3: Update save_checkpoint() and load_checkpoint()**

In `save_checkpoint()`, update the `state` dict:

```python
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
```

In `load_checkpoint()`, after restoring `error_count`, add:

```python
raw_queue = state.get("timed_queue", [])
self._timed_queue = []
for fire_at_iso, gen_name, kwargs in raw_queue:
    heapq.heappush(
        self._timed_queue,
        TimedEvent(datetime.fromisoformat(fire_at_iso), gen_name, kwargs),
    )
```

- [ ] **Step 4: Run — expect PASS**

```bash
python -m pytest tests/test_phase5_operations.py::TestTimedQueueCheckpoint -v
```
Expected: 2 tests PASS

- [ ] **Step 5: Commit**

```bash
git add simulation/engine.py tests/test_phase5_operations.py
git commit -m "feat: serialize timed_queue in engine checkpoints"
```

---

## Task 3: Engine — run_once drain loop + plan_day + registration

**Files:**
- Modify: `simulation/engine.py`
- Test: `tests/test_phase5_operations.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/test_phase5_operations.py`:

```python
class TestPlanDayOrdering(unittest.TestCase):
    def test_new_client_setup_is_first(self):
        from simulation.engine import SimulationEngine, GeneratorCall
        engine = SimulationEngine(dry_run=True, target_date=date(2026, 3, 28))
        plan = engine.plan_day(date(2026, 3, 28))
        self.assertEqual(plan[0], GeneratorCall("new_client_setup", {}))

    def test_job_scheduling_is_second(self):
        from simulation.engine import SimulationEngine, GeneratorCall
        engine = SimulationEngine(dry_run=True, target_date=date(2026, 3, 28))
        plan = engine.plan_day(date(2026, 3, 28))
        self.assertEqual(plan[1], GeneratorCall("job_scheduling", {}))


class TestRunOnceDrainsTimed(unittest.TestCase):
    def test_timed_event_fired_when_past_due(self):
        from simulation.engine import SimulationEngine
        engine = SimulationEngine(dry_run=True, target_date=date(2026, 3, 28))
        engine.speed = 9999  # skip sleep time
        fired = []

        class FakeGen:
            def execute(self, dry_run=False, **kwargs):
                fired.append(kwargs)

        engine.register("job_completion", FakeGen())
        past = datetime.utcnow() - timedelta(seconds=1)
        engine.queue_timed_event(past, "job_completion", {"job_id": "SS-JOB-TEST"})
        engine.run_once(date(2026, 3, 28))
        self.assertTrue(any(k.get("job_id") == "SS-JOB-TEST" for k in fired))
```

- [ ] **Step 2: Run — expect FAIL on plan ordering**

```bash
python -m pytest tests/test_phase5_operations.py::TestPlanDayOrdering -v 2>&1 | head -20
```
Expected: `AssertionError: GeneratorCall(generator_name='contacts', ...) != GeneratorCall(generator_name='new_client_setup', ...)`

- [ ] **Step 3: Update plan_day() to prepend ops events**

In `plan_day()`, replace the `random.shuffle(plan)` + return with:

```python
# Operations events: placed BEFORE the shuffle (fixed order, not randomised)
ops_prefix = [
    GeneratorCall("new_client_setup", {}),
    GeneratorCall("job_scheduling", {}),
]

random.shuffle(plan)
return ops_prefix + plan
```

- [ ] **Step 4: Update run_once() to drain timed queue**

In `run_once()`, inside the `while plan and self.running:` loop, replace:

```python
        generator_call = self.pick_next_generator(plan)
```

with:

```python
        # Drain any timed events whose fire_at has passed
        now = datetime.utcnow()
        while self._timed_queue and self._timed_queue[0].fire_at <= now:
            timed = heapq.heappop(self._timed_queue)
            self.dispatch(GeneratorCall(timed.generator_name, timed.kwargs))

        generator_call = self.pick_next_generator(plan)
```

- [ ] **Step 5: Update _register_generators() to add operations generators**

Append to `_register_generators()` before the `if not self._generators:` check:

```python
        # OperationsGenerators — simulation/generators/operations.py
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

- [ ] **Step 6: Run — expect PASS**

```bash
python -m pytest tests/test_phase5_operations.py::TestPlanDayOrdering tests/test_phase5_operations.py::TestRunOnceDrainsTimed -v
```
Expected: 3 tests PASS

- [ ] **Step 7: Confirm existing engine tests still pass**

```bash
python -m pytest tests/test_phase2.py tests/test_phase4.py -v --tb=short 2>&1 | tail -10
```
Expected: all previously passing tests still PASS

- [ ] **Step 8: Commit**

```bash
git add simulation/engine.py tests/test_phase5_operations.py
git commit -m "feat: engine timed-queue drain loop, plan_day ops prefix, generator registration"
```

---

## Task 4: operations.py scaffold and pure helper functions

**Files:**
- Create: `simulation/generators/operations.py`

- [ ] **Step 1: Create the file**

```python
"""
simulation/generators/operations.py

Three simulation generators for operations:
  NewClientSetupGenerator  — Type 1: creates Jobber client + schedule for won deals
  JobSchedulingGenerator   — Type 2: creates today's jobs for active recurring clients
  JobCompletionGenerator   — Type 2: fires at realistic times, records outcomes + reviews
"""
from __future__ import annotations

import calendar
import heapq
import random
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Callable, Optional

from auth import get_client
from config.business import SERVICE_TYPES
from database.mappings import generate_id, get_tool_id, register_mapping
from intelligence.logging_config import setup_logging
from seeding.utils.throttler import JOBBER as throttler
from simulation.config import DAILY_VOLUMES, JOB_VARIETY

logger = setup_logging("simulation.operations")


# ── Result type ───────────────────────────────────────────────────────────────

@dataclass
class GeneratorResult:
    success: bool
    message: str = ""


# ── Module-level constants ────────────────────────────────────────────────────

_JOBBER_GQL_URL = "https://api.getjobber.com/api/graphql"
_JOBBER_VERSION_HEADER = {"X-JOBBER-GRAPHQL-VERSION": "2026-03-10"}

_CREW_WINDOW = {
    "crew-a": time(7, 0),
    "crew-b": time(7, 30),
    "crew-c": time(8, 0),
    "crew-d": time(17, 0),
}

_FREQ_TO_RECUR_FREQ = {
    "weekly_recurring":   "weekly",
    "biweekly_recurring": "biweekly",
    "monthly_recurring":  "monthly",
    "nightly_clean":      "weekly",
    "weekend_deep_clean": "weekly",
}

_FREQ_TO_SERVICE_ID = {
    "weekly_recurring":     "recurring-weekly",
    "biweekly_recurring":   "recurring-biweekly",
    "monthly_recurring":    "recurring-monthly",
    "nightly_clean":        "commercial-nightly",
    "weekend_deep_clean":   "deep-clean",
    "one_time_standard":    "std-residential",
    "one_time_deep_clean":  "deep-clean",
    "one_time_move_in_out": "move-in-out",
    "one_time_project":     "commercial-nightly",
}

_DURATION_MAP = {st["id"]: st["duration_minutes"] for st in SERVICE_TYPES}

_JOBBER_FREQ_MAP = {
    "weekly":   {"type": "WEEKLY",  "interval": 1},
    "biweekly": {"type": "WEEKLY",  "interval": 2},
    "monthly":  {"type": "MONTHLY", "interval": 1},
}

_ONE_TIME_FREQS = {
    "one_time_standard", "one_time_deep_clean",
    "one_time_move_in_out", "one_time_project",
}

# ── GraphQL mutation strings ──────────────────────────────────────────────────

_CLIENT_CREATE = """
mutation ClientCreate($input: ClientCreateInput!) {
  clientCreate(input: $input) {
    client { id firstName lastName companyName }
    userErrors { message path }
  }
}
"""

_PROPERTY_CREATE = """
mutation PropertyCreate($clientId: EncodedId!, $input: PropertyCreateInput!) {
  propertyCreate(clientId: $clientId, input: $input) {
    properties { id }
    userErrors { message path }
  }
}
"""

_JOB_CREATE = """
mutation JobCreate($input: JobCreateAttributes!) {
  jobCreate(input: $input) {
    job { id title jobStatus }
    userErrors { message path }
  }
}
"""

_JOB_CLOSE = """
mutation CloseJob($jobId: EncodedId!, $input: JobCloseInput!) {
  jobClose(jobId: $jobId, input: $input) {
    job { id jobStatus }
    userErrors { message path }
  }
}
"""

_CLIENT_PROPERTIES_QUERY = """
query ClientProperties($id: EncodedId!) {
  client(id: $id) {
    id
    clientProperties { nodes { id } }
  }
}
"""

_JOB_CREATE_ATTRS_QUERY = """
query JobCreateAttrs {
  __type(name: "JobCreateAttributes") {
    inputFields { name }
  }
}
"""


# ── Module-level helpers ──────────────────────────────────────────────────────

def _gql(session, query: str, variables: dict) -> dict:
    """Execute a Jobber GraphQL call and return the parsed response dict."""
    throttler.wait()
    resp = session.post(
        _JOBBER_GQL_URL,
        json={"query": query, "variables": variables},
        headers=_JOBBER_VERSION_HEADER,
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    top_errors = data.get("errors", [])
    if top_errors:
        raise RuntimeError(f"Jobber GQL top-level errors: {top_errors}")
    return data


def _gql_user_errors(data: dict, mutation_key: str) -> list:
    """Extract userErrors from a Jobber mutation response."""
    return (
        data.get("data", {}).get(mutation_key, {}).get("userErrors", [])
        or data.get("errors", [])
    )


def _add_business_days(d: date, n: int) -> date:
    """Return d + n business days (skipping Saturday=5, Sunday=6)."""
    result = d
    while n > 0:
        result += timedelta(days=1)
        if result.weekday() < 5:
            n -= 1
    return result


def _expected_duration(service_type_id: str, job_type: str) -> int:
    """Return expected job duration in minutes.

    Source: config/business.py SERVICE_TYPES[n]["duration_minutes"].
    deep_clean uses the deep-clean service type duration (210 min) regardless
    of the base service_type_id. Default 120 if service_type_id not found.
    """
    if job_type == "deep_clean":
        return _DURATION_MAP.get("deep-clean", 210)
    return _DURATION_MAP.get(service_type_id, 120)


def _adjusted_rating_distribution(crew_name: str, day_of_week: int) -> list[tuple[int, float]]:
    """Return rating distribution adjusted for crew and day-of-week.

    Distributions (5★, 4★, 3★, 2★, 1★):
      Base:             60%, 25%, 10%, 4%, 1%
      Crew A only:      70%, 20%, 7%, 2.5%, 0.5%
      Tue/Wed only:     65%, 23%, 8%, 3%, 1%
      Crew A + Tue/Wed: 75%, 17%, 5%, 2.5%, 0.5%

    Cap: 5★ never exceeds 80%; excess redistributed into 4★.

    Args:
        crew_name: e.g. "Crew A" (matches clients.crew_assignment values)
        day_of_week: 0=Monday … 6=Sunday

    Returns:
        List of (rating, weight) tuples, weights summing to 1.0.
    """
    is_crew_a = (crew_name == "Crew A")
    is_tue_wed = (day_of_week in (1, 2))  # Tuesday=1, Wednesday=2

    if is_crew_a and is_tue_wed:
        dist = {5: 0.75, 4: 0.17, 3: 0.05, 2: 0.025, 1: 0.005}
    elif is_crew_a:
        dist = {5: 0.70, 4: 0.20, 3: 0.07, 2: 0.025, 1: 0.005}
    elif is_tue_wed:
        dist = {5: 0.65, 4: 0.23, 3: 0.08, 2: 0.03, 1: 0.01}
    else:
        dist = {5: 0.60, 4: 0.25, 3: 0.10, 2: 0.04, 1: 0.01}

    # Cap 5★ at 80%, redistribute excess into 4★
    if dist[5] > 0.80:
        excess = dist[5] - 0.80
        dist[5] = 0.80
        dist[4] = round(dist[4] + excess, 6)

    return sorted(dist.items(), reverse=True)


def _is_due_today(agreement: dict, today: date) -> bool:
    """Return True if a recurring agreement has a job due on `today`.

    Args:
        agreement: dict with keys: start_date (ISO str), day_of_week (str or None),
                   frequency ('weekly'|'biweekly'|'monthly')
        today: date to check
    """
    start = date.fromisoformat(agreement["start_date"])
    freq = agreement["frequency"]

    _DOW_MAP = {
        "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
        "friday": 4, "saturday": 5, "sunday": 6,
    }
    dow_str = (agreement.get("day_of_week") or "").lower()
    day_of_week_int = _DOW_MAP.get(dow_str, start.weekday())

    if freq == "weekly":
        return today.weekday() == day_of_week_int

    elif freq == "biweekly":
        return (
            today.weekday() == day_of_week_int
            and (today - start).days % 14 < 7
        )

    elif freq == "monthly":
        days_in_month = calendar.monthrange(today.year, today.month)[1]
        due_day = min(start.day, days_in_month)
        return today.day == due_day

    return False


def _pick_job_type(agreement: dict, today: date) -> str:
    """Return job type string for a given recurring agreement and date.

    Residential recurring: 'regular', 'deep_clean', or 'add_on'
    Commercial recurring: 'standard' or 'extra_service'

    Source: simulation/config.py JOB_VARIETY
    """
    is_commercial = (agreement.get("client_type") == "commercial")

    if is_commercial:
        cfg = JOB_VARIETY["commercial_recurring"]
        if random.random() < cfg["extra_service_rate"]:
            return "extra_service"
        return "standard"
    else:
        cfg = JOB_VARIETY["residential_recurring"]
        month = today.month
        boost = cfg["seasonal_deep_clean_boost"].get(month, 1.0)
        if random.random() < cfg["deep_clean_rate"] * boost:
            return "deep_clean"
        if random.random() < cfg["add_on_rate"]:
            return "add_on"
        return "regular"


def _assign_scheduled_time(crew_id: str, prior_jobs: list[dict], today: date) -> datetime:
    """Return the start datetime for the next job in a crew's route.

    Jobs are assigned sequentially: window_start + sum of (prior durations +
    travel buffers). First job in the day gets the window_start exactly.
    Travel buffer per job: random.randint(15, 30) minutes.

    Args:
        crew_id: e.g. "crew-a" (matches recurring_agreements.crew_id)
        prior_jobs: list of dicts, each with key "expected_duration" (int, minutes)
        today: the date being scheduled

    Returns:
        datetime for the job's scheduled start.
    """
    window_time = _CREW_WINDOW.get(crew_id, time(7, 0))
    base = datetime.combine(today, window_time)
    if not prior_jobs:
        return base
    offset = sum(
        j["expected_duration"] + random.randint(15, 30)
        for j in prior_jobs
    )
    return base + timedelta(minutes=offset)


# ── Recurrence field discovery (cached) ──────────────────────────────────────

_recurrence_field_cache: Optional[str] = None
_recurrence_field_checked: bool = False


def _get_recurrence_field(session) -> Optional[str]:
    """Introspect Jobber schema to find the recurrence input field name on JobCreateAttributes.

    Returns the field name (e.g. 'recurrences') or None if not found.
    Result is cached for the lifetime of the process.
    """
    global _recurrence_field_cache, _recurrence_field_checked
    if _recurrence_field_checked:
        return _recurrence_field_cache
    try:
        data = _gql(session, _JOB_CREATE_ATTRS_QUERY, {})
        fields = (
            data.get("data", {})
            .get("__type", {})
            .get("inputFields", [])
        )
        field_names = {f["name"] for f in fields}
        for candidate in ("recurrences", "repeat", "schedule", "recurrence"):
            if candidate in field_names:
                _recurrence_field_cache = candidate
                break
    except Exception:
        pass
    _recurrence_field_checked = True
    return _recurrence_field_cache


# ── Property ID helper (lazy fetch + register) ────────────────────────────────

def _get_or_fetch_property_id(canonical_id: str, session, db_path: str) -> Optional[str]:
    """Return the Jobber property ID for a client canonical_id.

    First checks cross_tool_mapping for a 'jobber_property' entry.
    If absent, queries Jobber via ClientProperties and registers the result.
    Returns None if the client has no Jobber mapping or no properties.
    """
    # Fast path: already registered
    prop_id = get_tool_id(canonical_id, "jobber_property", db_path=db_path)
    if prop_id:
        return prop_id

    # Slow path: query Jobber
    jobber_client_id = get_tool_id(canonical_id, "jobber", db_path=db_path)
    if not jobber_client_id:
        return None

    try:
        data = _gql(session, _CLIENT_PROPERTIES_QUERY, {"id": jobber_client_id})
        nodes = (
            data.get("data", {})
            .get("client", {})
            .get("clientProperties", {})
            .get("nodes", [])
        )
        if not nodes:
            return None
        prop_id = nodes[0]["id"]
        register_mapping(canonical_id, "jobber_property", prop_id, db_path=db_path)
        return prop_id
    except Exception:
        return None
```

- [ ] **Step 2: Verify importable**

```bash
cd sparkle-shine-poc && python -c "from simulation.generators.operations import _adjusted_rating_distribution, _is_due_today, _pick_job_type, _assign_scheduled_time; print('OK')"
```
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add simulation/generators/operations.py
git commit -m "feat: operations.py scaffold with pure helper functions"
```

---

## Task 5: Pure unit tests for all helpers

**Files:**
- Test: `tests/test_phase5_operations.py`

- [ ] **Step 1: Append pure unit tests**

Append to `tests/test_phase5_operations.py`:

```python
# ── Pure unit tests (no mocks, no DB) ────────────────────────────────────────

from simulation.generators.operations import (
    _adjusted_rating_distribution,
    _add_business_days,
    _assign_scheduled_time,
    _expected_duration,
    _is_due_today,
    _pick_job_type,
)


class TestAdjustedRatingDistribution(unittest.TestCase):
    def _weights(self, crew, dow):
        return dict(_adjusted_rating_distribution(crew, dow))

    def test_base_distribution(self):
        w = self._weights("Crew B", 0)  # Monday, not Crew A
        self.assertAlmostEqual(w[5], 0.60)
        self.assertAlmostEqual(w[4], 0.25)
        self.assertAlmostEqual(w[3], 0.10)

    def test_crew_a_distribution(self):
        w = self._weights("Crew A", 0)  # Monday
        self.assertAlmostEqual(w[5], 0.70)
        self.assertAlmostEqual(w[4], 0.20)

    def test_tue_wed_distribution(self):
        w = self._weights("Crew B", 1)  # Tuesday
        self.assertAlmostEqual(w[5], 0.65)
        self.assertAlmostEqual(w[4], 0.23)

    def test_crew_a_tue_wed_distribution(self):
        w = self._weights("Crew A", 2)  # Wednesday
        self.assertAlmostEqual(w[5], 0.75)
        self.assertAlmostEqual(w[4], 0.17)

    def test_five_star_never_exceeds_80_percent(self):
        for crew in ["Crew A", "Crew B", "Crew C", "Crew D"]:
            for dow in range(7):
                w = self._weights(crew, dow)
                self.assertLessEqual(w[5], 0.80 + 1e-9,
                    msg=f"5★ exceeded 80% for {crew}, dow={dow}")

    def test_weights_sum_to_one(self):
        for crew in ["Crew A", "Crew B"]:
            for dow in (0, 1, 2):
                w = self._weights(crew, dow)
                self.assertAlmostEqual(sum(w.values()), 1.0, places=5)


class TestIsDueToday(unittest.TestCase):
    def _agreement(self, start, dow, freq):
        return {"start_date": start, "day_of_week": dow, "frequency": freq}

    def test_weekly_fires_on_correct_day(self):
        a = self._agreement("2025-01-06", "monday", "weekly")  # Jan 6 = Monday
        self.assertTrue(_is_due_today(a, date(2025, 1, 13)))   # next Monday
        self.assertFalse(_is_due_today(a, date(2025, 1, 14)))  # Tuesday

    def test_biweekly_fires_weeks_0_2_4(self):
        a = self._agreement("2025-01-06", "monday", "biweekly")
        self.assertTrue(_is_due_today(a, date(2025, 1, 6)))    # week 0: due
        self.assertFalse(_is_due_today(a, date(2025, 1, 13)))  # week 1: skip
        self.assertTrue(_is_due_today(a, date(2025, 1, 20)))   # week 2: due
        self.assertFalse(_is_due_today(a, date(2025, 1, 27)))  # week 3: skip
        self.assertTrue(_is_due_today(a, date(2025, 2, 3)))    # week 4: due

    def test_monthly_fires_on_correct_day(self):
        a = self._agreement("2025-01-15", None, "monthly")
        self.assertTrue(_is_due_today(a, date(2025, 2, 15)))
        self.assertFalse(_is_due_today(a, date(2025, 2, 14)))

    def test_monthly_clamps_day_31_to_feb_28(self):
        a = self._agreement("2025-01-31", None, "monthly")
        self.assertTrue(_is_due_today(a, date(2025, 2, 28)))   # Feb has 28 days
        self.assertFalse(_is_due_today(a, date(2025, 2, 27)))


class TestPickJobType(unittest.TestCase):
    def _res_agreement(self):
        return {
            "service_type_id": "recurring-biweekly",
            "client_type": "residential",
            "start_date": "2025-01-06",
            "day_of_week": "monday",
            "frequency": "biweekly",
        }

    def test_residential_distribution_within_tolerance(self):
        random.seed(42)
        today = date(2025, 8, 1)  # August — no seasonal boost
        types = [_pick_job_type(self._res_agreement(), today) for _ in range(200)]
        regular_rate = types.count("regular") / 200
        deep_rate = types.count("deep_clean") / 200
        add_on_rate = types.count("add_on") / 200
        self.assertGreaterEqual(regular_rate, 0.75)
        self.assertLessEqual(regular_rate, 0.95)
        self.assertGreaterEqual(deep_rate, 0.04)
        self.assertLessEqual(deep_rate, 0.20)
        self.assertGreaterEqual(add_on_rate, 0.00)
        self.assertLessEqual(add_on_rate, 0.12)

    def test_seasonal_deep_clean_boost_march_vs_august(self):
        random.seed(0)
        march = date(2025, 3, 1)
        august = date(2025, 8, 1)
        a = self._res_agreement()
        march_deep = sum(
            1 for _ in range(500) if _pick_job_type(a, march) == "deep_clean"
        ) / 500
        august_deep = sum(
            1 for _ in range(500) if _pick_job_type(a, august) == "deep_clean"
        ) / 500
        self.assertGreater(march_deep, august_deep,
            msg="March deep-clean rate should exceed August (seasonal boost)")

    def test_commercial_returns_standard_or_extra(self):
        a = {
            "service_type_id": "commercial-nightly",
            "client_type": "commercial",
            "start_date": "2025-01-01",
            "day_of_week": "monday",
            "frequency": "weekly",
        }
        today = date(2025, 8, 1)
        random.seed(1)
        types = {_pick_job_type(a, today) for _ in range(50)}
        self.assertTrue(types.issubset({"standard", "extra_service"}))


class TestAssignScheduledTime(unittest.TestCase):
    def test_first_job_gets_window_start(self):
        result = _assign_scheduled_time("crew-a", [], date(2026, 3, 28))
        self.assertEqual(result, datetime(2026, 3, 28, 7, 0))

    def test_crew_d_starts_at_5pm(self):
        result = _assign_scheduled_time("crew-d", [], date(2026, 3, 28))
        self.assertEqual(result, datetime(2026, 3, 28, 17, 0))

    def test_sequential_jobs_are_ascending(self):
        random.seed(42)
        prior = []
        times = []
        for _ in range(4):
            t = _assign_scheduled_time("crew-a", prior, date(2026, 3, 28))
            times.append(t)
            prior.append({"expected_duration": 120})
        for i in range(1, len(times)):
            self.assertGreater(times[i], times[i - 1],
                msg=f"Job {i} start not after job {i-1}")

    def test_gap_between_jobs_at_least_15_minutes(self):
        random.seed(7)
        prior = [{"expected_duration": 120}]
        t0 = _assign_scheduled_time("crew-b", [], date(2026, 3, 28))
        t1 = _assign_scheduled_time("crew-b", prior, date(2026, 3, 28))
        gap = (t1 - t0).total_seconds() / 60
        # gap = 120 (duration) + travel buffer (15-30)
        self.assertGreaterEqual(gap, 135)  # 120 + 15 minimum
        self.assertLessEqual(gap, 152)     # 120 + 30 + small rounding


class TestAddBusinessDays(unittest.TestCase):
    def test_friday_plus_1_is_monday(self):
        friday = date(2026, 3, 27)
        result = _add_business_days(friday, 1)
        self.assertEqual(result, date(2026, 3, 30))  # Monday

    def test_thursday_plus_1_is_friday(self):
        result = _add_business_days(date(2026, 3, 26), 1)
        self.assertEqual(result, date(2026, 3, 27))
```

- [ ] **Step 2: Run pure unit tests**

```bash
python -m pytest tests/test_phase5_operations.py::TestAdjustedRatingDistribution tests/test_phase5_operations.py::TestIsDueToday tests/test_phase5_operations.py::TestPickJobType tests/test_phase5_operations.py::TestAssignScheduledTime tests/test_phase5_operations.py::TestAddBusinessDays -v
```
Expected: all PASS (26 tests)

- [ ] **Step 3: Commit**

```bash
git add tests/test_phase5_operations.py
git commit -m "test: pure unit tests for operations helper functions"
```

---

## Task 6: NewClientSetupGenerator — recurring path

**Files:**
- Modify: `simulation/generators/operations.py`
- Test: `tests/test_phase5_operations.py`

- [ ] **Step 1: Write failing mock tests**

Append to `tests/test_phase5_operations.py`:

```python
# ── NewClientSetupGenerator mock tests ───────────────────────────────────────

class TestNewClientSetupGeneratorRecurring(unittest.TestCase):
    """Tests for NewClientSetupGenerator recurring client path."""

    def _make_db(self):
        """Create an in-memory DB with the required tables."""
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript("""
            CREATE TABLE won_deals (
                canonical_id TEXT PRIMARY KEY,
                client_type TEXT NOT NULL,
                service_frequency TEXT NOT NULL,
                contract_value REAL,
                start_date TEXT NOT NULL,
                crew_assignment TEXT,
                pipedrive_deal_id INTEGER
            );
            CREATE TABLE leads (
                id TEXT PRIMARY KEY,
                first_name TEXT, last_name TEXT, company_name TEXT,
                email TEXT, phone TEXT, lead_type TEXT, source TEXT,
                status TEXT DEFAULT 'new', estimated_value REAL,
                created_at TEXT DEFAULT (datetime('now')),
                last_activity_at TEXT
            );
            CREATE TABLE clients (
                id TEXT PRIMARY KEY, client_type TEXT,
                first_name TEXT, last_name TEXT, company_name TEXT,
                email TEXT, phone TEXT, address TEXT,
                neighborhood TEXT, zone TEXT, status TEXT DEFAULT 'active',
                acquisition_source TEXT, first_service_date TEXT,
                last_service_date TEXT, lifetime_value REAL DEFAULT 0,
                notes TEXT, created_at TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE cross_tool_mapping (
                canonical_id TEXT, tool_name TEXT, tool_specific_id TEXT,
                tool_specific_url TEXT, synced_at TEXT,
                PRIMARY KEY (canonical_id, tool_name)
            );
            CREATE TABLE recurring_agreements (
                id TEXT PRIMARY KEY, client_id TEXT, service_type_id TEXT,
                crew_id TEXT, frequency TEXT, price_per_visit REAL,
                start_date TEXT, end_date TEXT, status TEXT DEFAULT 'active',
                day_of_week TEXT
            );
        """)
        return conn

    @patch("simulation.generators.operations.get_client")
    @patch("simulation.generators.operations.register_mapping")
    @patch("simulation.generators.operations.generate_id")
    @patch("simulation.generators.operations.get_tool_id")
    @patch("simulation.generators.operations._gql")
    def test_recurring_residential_creates_agreement_row(
        self, mock_gql, mock_get_tool_id, mock_gen_id, mock_reg_map, mock_get_client
    ):
        from simulation.generators.operations import NewClientSetupGenerator

        # Setup: one ready residential won deal
        conn = self._make_db()
        conn.execute(
            "INSERT INTO won_deals VALUES (?,?,?,?,?,?,?)",
            ("SS-LEAD-0001", "residential", "biweekly_recurring",
             150.0, "2026-03-01", "Crew A", 999),
        )
        conn.execute(
            "INSERT INTO leads VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            ("SS-LEAD-0001", "Alice", "Smith", None,
             "alice@example.com", "5125550001", "residential",
             "referral", "new", 150.0, "2025-01-01", None),
        )
        conn.commit()

        # No existing Jobber mapping → triggers setup
        mock_get_tool_id.return_value = None

        # _gql returns for: clientCreate, propertyCreate, jobCreate (recurring)
        mock_gql.side_effect = [
            {"data": {"clientCreate": {"client": {"id": "GQL-CLIENT-1"}, "userErrors": []}}},
            {"data": {"propertyCreate": {"properties": [{"id": "GQL-PROP-1"}], "userErrors": []}}},
            {"data": {"__type": {"inputFields": [{"name": "recurrences"}]}}},  # introspect
            {"data": {"jobCreate": {"job": {"id": "GQL-JOB-1"}, "userErrors": []}}},
        ]
        mock_gen_id.side_effect = ["SS-RECUR-9999"]

        gen = NewClientSetupGenerator(db_path=":memory:")
        # Patch the connection so it uses our in-memory DB
        with patch("sqlite3.connect", return_value=conn):
            result = gen.execute(dry_run=False)

        self.assertTrue(result.success)
        self.assertIn("1", result.message)  # "setup 1 new clients"

        # recurring_agreements row should exist
        row = conn.execute(
            "SELECT * FROM recurring_agreements WHERE id = ?", ("SS-RECUR-9999",)
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["frequency"], "biweekly")
        self.assertEqual(row["price_per_visit"], 150.0)
```

- [ ] **Step 2: Run — expect ImportError**

```bash
python -m pytest "tests/test_phase5_operations.py::TestNewClientSetupGeneratorRecurring" -v 2>&1 | head -20
```
Expected: `ImportError: cannot import name 'NewClientSetupGenerator'`

- [ ] **Step 3: Add NewClientSetupGenerator to operations.py**

Append to `simulation/generators/operations.py`:

```python
# ── NewClientSetupGenerator ───────────────────────────────────────────────────

class NewClientSetupGenerator:
    """Type 1 generator: creates Jobber client + schedule for won deals.

    Reads won_deals table for deals whose start_date <= today with no Jobber
    mapping yet. Processes all ready deals in one execute() call. Each client
    is wrapped in its own try/except so a single failure does not abort the rest.
    """

    def __init__(self, db_path: str = "sparkle_shine.db"):
        self.db_path = db_path

    def _ensure_schema(self, conn: sqlite3.Connection) -> None:
        """Add client_type column to recurring_agreements if not present."""
        cols = {row[1] for row in conn.execute("PRAGMA table_info(recurring_agreements)")}
        if "client_type" not in cols:
            conn.execute(
                "ALTER TABLE recurring_agreements ADD COLUMN client_type TEXT DEFAULT 'residential'"
            )
            conn.commit()

    def execute(self, dry_run: bool = False) -> GeneratorResult:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            self._ensure_schema(conn)
            ready_deals = conn.execute("""
                SELECT * FROM won_deals
                WHERE start_date <= date('now')
                  AND canonical_id NOT IN (
                      SELECT canonical_id FROM cross_tool_mapping
                      WHERE tool_name = 'jobber'
                  )
            """).fetchall()
            ready_deals = [dict(r) for r in ready_deals]

            if not ready_deals:
                return GeneratorResult(success=True, message="no new clients ready")

            session = get_client("jobber") if not dry_run else None
            results = []

            for deal in ready_deals:
                try:
                    if dry_run:
                        results.append(("ok", deal["canonical_id"]))
                        continue
                    self._setup_client(deal, session, conn)
                    results.append(("ok", deal["canonical_id"]))
                except Exception as e:
                    logger.exception("Setup failed for %s", deal["canonical_id"])
                    results.append(("failed", deal["canonical_id"], str(e)))

            conn.commit()
            succeeded = sum(1 for r in results if r[0] == "ok")
            failed = len(results) - succeeded
            if failed:
                return GeneratorResult(
                    success=False,
                    message=f"setup {succeeded} clients, {failed} failed",
                )
            return GeneratorResult(success=True, message=f"setup {succeeded} new clients")

        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _setup_client(self, deal: dict, session, conn: sqlite3.Connection) -> None:
        """Create a Jobber client + schedule for one won deal."""
        canonical_id = deal["canonical_id"]
        client_type = deal["client_type"]
        service_frequency = deal["service_frequency"]
        contract_value = deal["contract_value"] or 0.0
        start_date = date.fromisoformat(deal["start_date"])
        crew_assignment = deal["crew_assignment"] or "Crew A"
        pipedrive_deal_id = deal.get("pipedrive_deal_id")

        # 1. Look up client info from the appropriate table
        if client_type == "commercial":
            row = conn.execute(
                "SELECT * FROM clients WHERE id = ?", (canonical_id,)
            ).fetchone()
            if row is None:
                raise RuntimeError(f"No clients row for {canonical_id}")
            row = dict(row)
            name = row.get("company_name") or f"{row.get('first_name','')} {row.get('last_name','')}".strip()
            email = row.get("email", "")
            phone = row.get("phone", "")
            address = row.get("address", "Austin, TX")
        else:
            row = conn.execute(
                "SELECT * FROM leads WHERE id = ?", (canonical_id,)
            ).fetchone()
            if row is None:
                raise RuntimeError(f"No leads row for {canonical_id}")
            row = dict(row)
            name = f"{row.get('first_name','')} {row.get('last_name','')}".strip()
            email = row.get("email", "")
            phone = row.get("phone", "")
            address = "Austin, TX"  # leads table has no address column

        # 2. clientCreate → jobber_client_id
        client_input: dict = {}
        if client_type == "residential":
            parts = name.split(" ", 1)
            client_input["firstName"] = parts[0]
            client_input["lastName"] = parts[1] if len(parts) > 1 else ""
        else:
            client_input["companyName"] = name
        if email:
            client_input["emails"] = [{"description": "MAIN", "address": email}]
        if phone:
            client_input["phones"] = [{"description": "MAIN", "number": phone}]
        if address:
            parts = [p.strip() for p in address.split(",")]
            client_input["billingAddress"] = {
                "street1": parts[0],
                "city": parts[1] if len(parts) > 1 else "Austin",
                "province": "TX",
                "country": "US",
            }

        resp = _gql(session, _CLIENT_CREATE, {"input": client_input})
        errs = _gql_user_errors(resp, "clientCreate")
        if errs:
            raise RuntimeError(f"clientCreate errors: {errs}")
        jobber_client_id = resp["data"]["clientCreate"]["client"]["id"]

        # 3. Register client mapping
        register_mapping(canonical_id, "jobber", jobber_client_id, db_path=self.db_path)

        # 4. propertyCreate → jobber_property_id
        prop_parts = [p.strip() for p in address.split(",")]
        prop_input = {
            "properties": [{
                "address": {
                    "street1": prop_parts[0],
                    "city": prop_parts[1] if len(prop_parts) > 1 else "Austin",
                    "province": "TX",
                    "country": "US",
                }
            }]
        }
        resp2 = _gql(session, _PROPERTY_CREATE,
                     {"clientId": jobber_client_id, "input": prop_input})
        errs2 = _gql_user_errors(resp2, "propertyCreate")
        if errs2:
            raise RuntimeError(f"propertyCreate errors: {errs2}")
        props = resp2["data"]["propertyCreate"]["properties"]
        jobber_property_id = props[0]["id"] if props else None
        if jobber_property_id:
            register_mapping(canonical_id, "jobber_property", jobber_property_id,
                             db_path=self.db_path)

        # 5. Branch: recurring vs one-time
        is_one_time = service_frequency in _ONE_TIME_FREQS
        service_type_id = _FREQ_TO_SERVICE_ID.get(service_frequency, "std-residential")
        crew_id = "crew-" + crew_assignment.lower().replace("crew ", "")

        if is_one_time:
            self._setup_one_time(
                deal, canonical_id, service_type_id, service_frequency,
                start_date, crew_id, contract_value, jobber_property_id, session, conn,
            )
        else:
            self._setup_recurring(
                deal, canonical_id, service_type_id, service_frequency,
                start_date, crew_id, contract_value, jobber_property_id, session, conn,
            )

        # 6. Pipedrive activity note
        if pipedrive_deal_id:
            try:
                pd_session = get_client("pipedrive")
                pd_session.post(
                    "https://api.pipedrive.com/v1/activities",
                    json={
                        "subject": f"SS-ID: {canonical_id} — Jobber client created",
                        "deal_id": pipedrive_deal_id,
                        "type": "task",
                        "done": 1,
                        "note": (
                            f"SS-ID: {canonical_id} — Jobber client created, "
                            f"{service_frequency} schedule initialized"
                        ),
                    },
                    timeout=15,
                )
            except Exception:
                logger.warning("Pipedrive activity failed for %s", canonical_id)

    def _setup_recurring(
        self, deal, canonical_id, service_type_id, service_frequency,
        start_date, crew_id, contract_value, jobber_property_id, session, conn,
    ) -> None:
        recur_frequency = _FREQ_TO_RECUR_FREQ[service_frequency]
        recur_field = _get_recurrence_field(session)

        # Determine day_of_week: first available weekday on or after start_date
        day_of_week_int = start_date.weekday()  # 0=Mon … 4=Fri; clamp to weekday
        if day_of_week_int >= 5:  # Sat or Sun → move to Monday
            day_of_week_int = 0
            start_date = start_date + timedelta(days=(7 - start_date.weekday()))
        _DOW_NAMES = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        day_of_week_name = _DOW_NAMES[day_of_week_int]

        # jobCreate with recurrence
        job_input: dict = {
            "propertyId": jobber_property_id or "",
            "title": f"Recurring: {service_type_id.replace('-', ' ').title()}",
            "invoicing": {
                "invoicingType": "FIXED_PRICE",
                "invoicingSchedule": "ON_COMPLETION",
            },
            "timeframe": {"startAt": start_date.isoformat()},
        }
        freq_config = _JOBBER_FREQ_MAP.get(recur_frequency)
        if recur_field and freq_config:
            job_input[recur_field] = freq_config

        resp = _gql(session, _JOB_CREATE, {"input": job_input})
        errs = _gql_user_errors(resp, "jobCreate")
        if errs:
            raise RuntimeError(f"jobCreate (recurring) errors: {errs}")
        jobber_recurrence_id = resp["data"]["jobCreate"]["job"]["id"]

        # Insert into recurring_agreements
        recur_id = generate_id("RECUR", db_path=self.db_path)
        conn.execute("""
            INSERT OR IGNORE INTO recurring_agreements
            (id, client_id, service_type_id, crew_id, frequency,
             price_per_visit, start_date, status, day_of_week, client_type)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            recur_id, canonical_id, service_type_id, crew_id,
            recur_frequency, contract_value,
            start_date.isoformat(), "active", day_of_week_name,
            deal.get("client_type", "residential"),
        ))
        register_mapping(recur_id, "jobber", jobber_recurrence_id, db_path=self.db_path)

    def _setup_one_time(
        self, deal, canonical_id, service_type_id, service_frequency,
        start_date, crew_id, contract_value, jobber_property_id, session, conn,
    ) -> None:
        job_input: dict = {
            "propertyId": jobber_property_id or "",
            "title": service_type_id.replace("-", " ").title(),
            "invoicing": {
                "invoicingType": "FIXED_PRICE",
                "invoicingSchedule": "ON_COMPLETION",
            },
            "timeframe": {"startAt": start_date.isoformat()},
        }
        resp = _gql(session, _JOB_CREATE, {"input": job_input})
        errs = _gql_user_errors(resp, "jobCreate")
        if errs:
            raise RuntimeError(f"jobCreate (one-time) errors: {errs}")
        jobber_job_id = resp["data"]["jobCreate"]["job"]["id"]

        job_id = generate_id("JOB", db_path=self.db_path)
        conn.execute("""
            INSERT OR IGNORE INTO jobs
            (id, client_id, crew_id, service_type_id, scheduled_date, status)
            VALUES (?,?,?,?,?,?)
        """, (
            job_id, canonical_id, crew_id, service_type_id,
            start_date.isoformat(), "scheduled",
        ))
        register_mapping(job_id, "jobber", jobber_job_id, db_path=self.db_path)
```

- [ ] **Step 4: Run test**

```bash
python -m pytest "tests/test_phase5_operations.py::TestNewClientSetupGeneratorRecurring" -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add simulation/generators/operations.py tests/test_phase5_operations.py
git commit -m "feat: NewClientSetupGenerator recurring client setup path"
```

---

## Task 7: NewClientSetupGenerator — error isolation test

**Files:**
- Test: `tests/test_phase5_operations.py`

- [ ] **Step 1: Append error isolation test**

Append to `tests/test_phase5_operations.py`:

```python
class TestNewClientSetupErrorIsolation(unittest.TestCase):
    """Second of three clients fails; first and third still succeed."""

    @patch("simulation.generators.operations.get_client")
    @patch("simulation.generators.operations.register_mapping")
    @patch("simulation.generators.operations.generate_id")
    @patch("simulation.generators.operations.get_tool_id")
    @patch("simulation.generators.operations._gql")
    def test_one_failure_does_not_abort_others(
        self, mock_gql, mock_get_tool_id, mock_gen_id, mock_reg_map, mock_get_client
    ):
        from simulation.generators.operations import NewClientSetupGenerator

        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript("""
            CREATE TABLE won_deals (
                canonical_id TEXT PRIMARY KEY,
                client_type TEXT NOT NULL,
                service_frequency TEXT NOT NULL,
                contract_value REAL,
                start_date TEXT NOT NULL,
                crew_assignment TEXT,
                pipedrive_deal_id INTEGER
            );
            CREATE TABLE leads (
                id TEXT PRIMARY KEY, first_name TEXT, last_name TEXT,
                company_name TEXT, email TEXT, phone TEXT,
                lead_type TEXT, source TEXT, status TEXT DEFAULT 'new',
                estimated_value REAL, created_at TEXT, last_activity_at TEXT
            );
            CREATE TABLE cross_tool_mapping (
                canonical_id TEXT, tool_name TEXT, tool_specific_id TEXT,
                tool_specific_url TEXT, synced_at TEXT,
                PRIMARY KEY (canonical_id, tool_name)
            );
            CREATE TABLE recurring_agreements (
                id TEXT PRIMARY KEY, client_id TEXT, service_type_id TEXT,
                crew_id TEXT, frequency TEXT, price_per_visit REAL,
                start_date TEXT, end_date TEXT, status TEXT DEFAULT 'active',
                day_of_week TEXT
            );
        """)
        for i in (1, 2, 3):
            conn.execute(
                "INSERT INTO won_deals VALUES (?,?,?,?,?,?,?)",
                (f"SS-LEAD-{i:04d}", "residential", "biweekly_recurring",
                 150.0, "2026-03-01", "Crew A", None),
            )
            conn.execute(
                "INSERT INTO leads VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (f"SS-LEAD-{i:04d}", f"Client{i}", "Test", None,
                 f"c{i}@test.com", "5125550000", "residential",
                 "test", "new", 0.0, "2025-01-01", None),
            )
        conn.commit()

        mock_get_tool_id.return_value = None
        mock_gen_id.return_value = "SS-RECUR-0001"

        success_response = [
            {"data": {"clientCreate": {"client": {"id": "GQL-C-OK"}, "userErrors": []}}},
            {"data": {"propertyCreate": {"properties": [{"id": "GQL-P-OK"}], "userErrors": []}}},
            {"data": {"__type": {"inputFields": [{"name": "recurrences"}]}}},
            {"data": {"jobCreate": {"job": {"id": "GQL-J-OK"}, "userErrors": []}}},
        ]
        # Client 1: success (4 calls), Client 2: raises on clientCreate,
        # Client 3: success (4 calls)
        call_count = [0]
        def side_effect(*args, **kwargs):
            call_count[0] += 1
            # Calls 1-4: client 1 (success)
            if call_count[0] <= 4:
                return success_response[call_count[0] - 1]
            # Call 5: client 2 clientCreate → raises
            if call_count[0] == 5:
                raise RuntimeError("Simulated Jobber error for client 2")
            # Calls 6-9: client 3 (success, reusing success_response cyclically)
            return success_response[(call_count[0] - 6) % 4]

        mock_gql.side_effect = side_effect

        gen = NewClientSetupGenerator(db_path=":memory:")
        with patch("sqlite3.connect", return_value=conn):
            result = gen.execute(dry_run=False)

        # 2 succeeded, 1 failed → success=False with summary message
        self.assertFalse(result.success)
        self.assertIn("2", result.message)
        self.assertIn("1", result.message)
```

- [ ] **Step 2: Run test**

```bash
python -m pytest "tests/test_phase5_operations.py::TestNewClientSetupErrorIsolation" -v
```
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add tests/test_phase5_operations.py
git commit -m "test: NewClientSetupGenerator error isolation — one failure does not abort others"
```

---

## Task 8: JobSchedulingGenerator — recurring agreements loop

**Files:**
- Modify: `simulation/generators/operations.py`
- Test: `tests/test_phase5_operations.py`

- [ ] **Step 1: Write failing mock test**

Append to `tests/test_phase5_operations.py`:

```python
class TestJobSchedulingGeneratorQueueFn(unittest.TestCase):
    """queue_fn is called with correct fire_at times after creating each job."""

    @patch("simulation.generators.operations.get_client")
    @patch("simulation.generators.operations.register_mapping")
    @patch("simulation.generators.operations.generate_id")
    @patch("simulation.generators.operations.get_tool_id")
    @patch("simulation.generators.operations._gql")
    def test_queue_fn_called_for_each_due_job(
        self, mock_gql, mock_get_tool_id, mock_gen_id, mock_reg_map, mock_get_client
    ):
        from simulation.generators.operations import JobSchedulingGenerator

        # Monday 2026-03-30 — weekly agreement due
        today = date(2026, 3, 30)

        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript("""
            CREATE TABLE recurring_agreements (
                id TEXT PRIMARY KEY, client_id TEXT, service_type_id TEXT,
                crew_id TEXT, frequency TEXT, price_per_visit REAL,
                start_date TEXT, end_date TEXT, status TEXT DEFAULT 'active',
                day_of_week TEXT
            );
            CREATE TABLE jobs (
                id TEXT PRIMARY KEY, client_id TEXT, crew_id TEXT,
                service_type_id TEXT, scheduled_date TEXT, scheduled_time TEXT,
                duration_minutes_actual INTEGER,
                status TEXT DEFAULT 'scheduled', address TEXT, notes TEXT,
                review_requested INTEGER DEFAULT 0, completed_at TEXT
            );
            CREATE TABLE cross_tool_mapping (
                canonical_id TEXT, tool_name TEXT, tool_specific_id TEXT,
                tool_specific_url TEXT, synced_at TEXT,
                PRIMARY KEY (canonical_id, tool_name)
            );
        """)
        # Two weekly agreements both due on Monday
        conn.execute(
            "INSERT INTO recurring_agreements VALUES (?,?,?,?,?,?,?,?,?,?)",
            ("SS-RECUR-0001", "SS-LEAD-0001", "recurring-weekly",
             "crew-a", "weekly", 135.0, "2026-03-23", None, "active", "monday"),
        )
        conn.execute(
            "INSERT INTO recurring_agreements VALUES (?,?,?,?,?,?,?,?,?,?)",
            ("SS-RECUR-0002", "SS-LEAD-0002", "recurring-biweekly",
             "crew-b", "biweekly", 150.0, "2026-03-16", None, "active", "monday"),
        )
        conn.commit()

        # get_tool_id: return property IDs for both clients
        mock_get_tool_id.side_effect = lambda cid, tool, **kw: (
            "GQL-PROP-" + cid[-4:] if tool == "jobber_property" else None
        )
        mock_gen_id.side_effect = ["SS-JOB-9001", "SS-JOB-9002"]
        mock_gql.return_value = {
            "data": {"jobCreate": {"job": {"id": "GQL-JOB-X"}, "userErrors": []}}
        }

        queue_calls = []
        def fake_queue(fire_at, generator_name, kwargs):
            queue_calls.append((fire_at, generator_name, kwargs))

        gen = JobSchedulingGenerator(db_path=":memory:", queue_fn=fake_queue)
        with patch("sqlite3.connect", return_value=conn):
            with patch("simulation.generators.operations.date") as mock_date:
                mock_date.today.return_value = today
                mock_date.fromisoformat.side_effect = date.fromisoformat
                result = gen.execute(dry_run=False)

        self.assertTrue(result.success)
        # Two jobs queued (one per due agreement)
        self.assertEqual(len(queue_calls), 2)
        for fire_at, gen_name, kwargs in queue_calls:
            self.assertEqual(gen_name, "job_completion")
            self.assertIn("job_id", kwargs)
            self.assertIsInstance(fire_at, datetime)
            # fire_at should be after window start (7 AM on 2026-03-30)
            self.assertGreater(fire_at, datetime(2026, 3, 30, 7, 0))
```

- [ ] **Step 2: Run — expect ImportError**

```bash
python -m pytest "tests/test_phase5_operations.py::TestJobSchedulingGeneratorQueueFn" -v 2>&1 | head -15
```
Expected: `ImportError: cannot import name 'JobSchedulingGenerator'`

- [ ] **Step 3: Add JobSchedulingGenerator to operations.py**

Append to `simulation/generators/operations.py`:

```python
# ── JobSchedulingGenerator ────────────────────────────────────────────────────

class JobSchedulingGenerator:
    """Type 2 generator: creates today's jobs for active recurring clients.

    Run once per day (placed second in plan_day before shuffle).
    Also picks up already-scheduled jobs (rescheduled or one-time) that need
    completion events queued.

    Args:
        db_path: Path to sparkle_shine.db.
        queue_fn: Callable(fire_at, generator_name, kwargs) — injected by engine.
                  None in tests that don't need the queue.
    """

    def __init__(self, db_path: str = "sparkle_shine.db", queue_fn: Optional[Callable] = None):
        self.db_path = db_path
        self._queue_fn = queue_fn

    def execute(self, dry_run: bool = False) -> GeneratorResult:
        today = date.today()
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            session = get_client("jobber") if not dry_run else None
            results = []
            jobs_created_this_run: list[str] = []  # canonical job IDs created here

            # ── Pass 1: recurring_agreements ──────────────────────────────────
            agreements = conn.execute("""
                SELECT * FROM recurring_agreements WHERE status = 'active'
            """).fetchall()
            agreements = [dict(a) for a in agreements]

            # Track prior jobs per crew for sequential time assignment
            prior_jobs_by_crew: dict[str, list[dict]] = {}

            for agreement in agreements:
                if not _is_due_today(agreement, today):
                    continue

                # Idempotent guard: skip if job already exists for this client+date
                existing = conn.execute(
                    "SELECT id FROM jobs WHERE client_id = ? AND scheduled_date = ?",
                    (agreement["client_id"], today.isoformat()),
                ).fetchone()
                if existing:
                    continue

                try:
                    job_type = _pick_job_type(agreement, today)
                    crew_id = agreement.get("crew_id") or "crew-a"
                    prior = prior_jobs_by_crew.get(crew_id, [])
                    scheduled_dt = _assign_scheduled_time(crew_id, prior, today)
                    duration = _expected_duration(agreement["service_type_id"], job_type)
                    completion_dt = scheduled_dt + timedelta(minutes=duration)
                    completion_dt += timedelta(
                        minutes=random.uniform(-duration * 0.15, duration * 0.15)
                    )

                    job_id = generate_id("JOB", db_path=self.db_path)

                    if not dry_run:
                        property_id = _get_or_fetch_property_id(
                            agreement["client_id"], session, self.db_path
                        )
                        price = agreement["price_per_visit"]
                        if job_type == "deep_clean":
                            from simulation.config import JOB_VARIETY
                            price = price * JOB_VARIETY["residential_recurring"]["deep_clean_price_multiplier"]
                        elif job_type == "add_on":
                            from simulation.config import JOB_VARIETY
                            add_on = random.choice(JOB_VARIETY["residential_recurring"]["add_on_options"])
                            price = price + add_on["price"]
                        elif job_type == "extra_service":
                            from simulation.config import JOB_VARIETY
                            extra = random.choice(JOB_VARIETY["commercial_recurring"]["extra_service_options"])
                            price = price + extra["price"]

                        job_input = {
                            "propertyId": property_id or "",
                            "title": agreement["service_type_id"].replace("-", " ").title(),
                            "invoicing": {
                                "invoicingType": "FIXED_PRICE",
                                "invoicingSchedule": "ON_COMPLETION",
                            },
                            "timeframe": {"startAt": today.isoformat()},
                        }
                        resp = _gql(session, _JOB_CREATE, {"input": job_input})
                        errs = _gql_user_errors(resp, "jobCreate")
                        if errs:
                            raise RuntimeError(f"jobCreate errors: {errs}")
                        jobber_job_id = resp["data"]["jobCreate"]["job"]["id"]

                        conn.execute("""
                            INSERT OR IGNORE INTO jobs
                            (id, client_id, crew_id, service_type_id,
                             scheduled_date, scheduled_time, status)
                            VALUES (?,?,?,?,?,?,?)
                        """, (
                            job_id, agreement["client_id"], crew_id,
                            agreement["service_type_id"], today.isoformat(),
                            scheduled_dt.strftime("%H:%M"), "scheduled",
                        ))
                        register_mapping(job_id, "jobber", jobber_job_id, db_path=self.db_path)

                    if self._queue_fn:
                        self._queue_fn(
                            fire_at=completion_dt,
                            generator_name="job_completion",
                            kwargs={"job_id": job_id},
                        )

                    prior_jobs_by_crew.setdefault(crew_id, []).append(
                        {"expected_duration": duration}
                    )
                    jobs_created_this_run.append(job_id)
                    results.append(("ok", job_id))

                except Exception as e:
                    logger.exception("Job scheduling failed for agreement %s", agreement.get("id"))
                    results.append(("failed", agreement.get("id"), str(e)))

            # ── Pass 2: already-scheduled jobs (rescheduled + one-time) ──────
            already_scheduled = conn.execute("""
                SELECT * FROM jobs
                WHERE status = 'scheduled'
                  AND scheduled_date = ?
            """, (today.isoformat(),)).fetchall()
            already_scheduled = [dict(j) for j in already_scheduled]

            for job in already_scheduled:
                if job["id"] in jobs_created_this_run:
                    continue  # just created this run — already has a completion event
                try:
                    crew_id = job.get("crew_id") or "crew-a"
                    prior = prior_jobs_by_crew.get(crew_id, [])
                    if job.get("scheduled_time"):
                        try:
                            h, m = map(int, job["scheduled_time"].split(":"))
                            scheduled_dt = datetime.combine(today, time(h, m))
                        except Exception:
                            scheduled_dt = _assign_scheduled_time(crew_id, prior, today)
                    else:
                        scheduled_dt = _assign_scheduled_time(crew_id, prior, today)

                    duration = _expected_duration(job.get("service_type_id", "std-residential"), "regular")
                    completion_dt = scheduled_dt + timedelta(minutes=duration)
                    completion_dt += timedelta(
                        minutes=random.uniform(-duration * 0.15, duration * 0.15)
                    )

                    if self._queue_fn:
                        self._queue_fn(
                            fire_at=completion_dt,
                            generator_name="job_completion",
                            kwargs={"job_id": job["id"]},
                        )

                    prior_jobs_by_crew.setdefault(crew_id, []).append(
                        {"expected_duration": duration}
                    )
                    results.append(("ok_existing", job["id"]))
                except Exception as e:
                    logger.exception("Completion queuing failed for existing job %s", job.get("id"))

            conn.commit()
            succeeded = sum(1 for r in results if r[0] in ("ok", "ok_existing"))
            failed = sum(1 for r in results if r[0] == "failed")
            if failed:
                return GeneratorResult(
                    success=False,
                    message=f"scheduled {succeeded} jobs, {failed} failed",
                )
            return GeneratorResult(success=True, message=f"scheduled {succeeded} jobs")

        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
```

- [ ] **Step 4: Run test**

```bash
python -m pytest "tests/test_phase5_operations.py::TestJobSchedulingGeneratorQueueFn" -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add simulation/generators/operations.py tests/test_phase5_operations.py
git commit -m "feat: JobSchedulingGenerator — recurring loop and already-scheduled pass"
```

---

## Task 9: JobCompletionGenerator — schema + completed path

**Files:**
- Modify: `simulation/generators/operations.py`
- Test: `tests/test_phase5_operations.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/test_phase5_operations.py`:

```python
class TestJobCompletionGeneratorCompleted(unittest.TestCase):
    """completed outcome: jobs updated, review inserted."""

    def _make_db_with_job(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript("""
            CREATE TABLE jobs (
                id TEXT PRIMARY KEY, client_id TEXT, crew_id TEXT,
                service_type_id TEXT, scheduled_date TEXT, scheduled_time TEXT,
                duration_minutes_actual INTEGER,
                status TEXT DEFAULT 'scheduled', address TEXT, notes TEXT,
                review_requested INTEGER DEFAULT 0, completed_at TEXT
            );
            CREATE TABLE clients (
                id TEXT PRIMARY KEY, client_type TEXT,
                first_name TEXT, last_name TEXT, company_name TEXT,
                email TEXT, phone TEXT, address TEXT,
                neighborhood TEXT, zone TEXT, status TEXT DEFAULT 'active',
                acquisition_source TEXT, first_service_date TEXT,
                last_service_date TEXT, lifetime_value REAL DEFAULT 0,
                notes TEXT, created_at TEXT,
                churn_risk TEXT DEFAULT 'normal'
            );
            CREATE TABLE reviews (
                id TEXT PRIMARY KEY, client_id TEXT, job_id TEXT,
                rating INTEGER, review_text TEXT, platform TEXT,
                review_date TEXT, response_text TEXT, response_date TEXT
            );
            CREATE TABLE cross_tool_mapping (
                canonical_id TEXT, tool_name TEXT, tool_specific_id TEXT,
                tool_specific_url TEXT, synced_at TEXT,
                PRIMARY KEY (canonical_id, tool_name)
            );
        """)
        conn.execute(
            "INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            ("SS-JOB-0001", "SS-LEAD-0001", "crew-a", "recurring-weekly",
             "2026-03-30", "07:00", None, "scheduled", None, None, 0, None),
        )
        conn.execute(
            "INSERT INTO clients VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("SS-LEAD-0001", "residential", "Alice", "Smith", None,
             "a@test.com", None, None, None, None, "active",
             None, None, None, 0.0, None, "2025-01-01", "normal"),
        )
        conn.execute(
            "INSERT INTO cross_tool_mapping VALUES (?,?,?,?,?)",
            ("SS-JOB-0001", "jobber", "GQL-JOB-0001", None, "2026-03-30"),
        )
        conn.commit()
        return conn

    @patch("simulation.generators.operations.get_client")
    @patch("simulation.generators.operations.register_mapping")
    @patch("simulation.generators.operations.generate_id")
    @patch("simulation.generators.operations.get_tool_id")
    @patch("simulation.generators.operations._gql")
    def test_completed_updates_job_and_inserts_review(
        self, mock_gql, mock_get_tool_id, mock_gen_id, mock_reg_map, mock_get_client
    ):
        from simulation.generators.operations import JobCompletionGenerator

        conn = self._make_db_with_job()
        mock_get_tool_id.return_value = "GQL-JOB-0001"
        mock_gen_id.return_value = "SS-REV-0001"
        mock_gql.return_value = {
            "data": {"jobClose": {"job": {"id": "GQL-JOB-0001", "jobStatus": "COMPLETED"}, "userErrors": []}}
        }

        gen = JobCompletionGenerator(db_path=":memory:")
        with patch("sqlite3.connect", return_value=conn):
            with patch("random.random", return_value=0.01):  # force 'completed' outcome
                result = gen.execute(dry_run=False, job_id="SS-JOB-0001")

        self.assertTrue(result.success)

        job = dict(conn.execute("SELECT * FROM jobs WHERE id = ?", ("SS-JOB-0001",)).fetchone())
        self.assertEqual(job["status"], "completed")
        self.assertIsNotNone(job["duration_minutes_actual"])
        self.assertIsNotNone(job["completed_at"])

        review = conn.execute("SELECT * FROM reviews WHERE job_id = ?", ("SS-JOB-0001",)).fetchone()
        self.assertIsNotNone(review)
        self.assertEqual(dict(review)["platform"], "internal")
        self.assertIn(dict(review)["rating"], (1, 2, 3, 4, 5))

    def test_completed_skips_review_in_dry_run(self):
        from simulation.generators.operations import JobCompletionGenerator
        conn = self._make_db_with_job()

        gen = JobCompletionGenerator(db_path=":memory:")
        with patch("sqlite3.connect", return_value=conn):
            result = gen.execute(dry_run=True, job_id="SS-JOB-0001")

        # dry_run: no API calls, no writes, no review
        review = conn.execute("SELECT * FROM reviews WHERE job_id = ?", ("SS-JOB-0001",)).fetchone()
        self.assertIsNone(review)
```

- [ ] **Step 2: Run — expect ImportError**

```bash
python -m pytest "tests/test_phase5_operations.py::TestJobCompletionGeneratorCompleted" -v 2>&1 | head -15
```
Expected: `ImportError: cannot import name 'JobCompletionGenerator'`

- [ ] **Step 3: Add JobCompletionGenerator to operations.py**

Append to `simulation/generators/operations.py`:

```python
# ── JobCompletionGenerator ────────────────────────────────────────────────────

class JobCompletionGenerator:
    """Type 2 generator: fires at realistic times, records job outcomes + reviews.

    Dispatched by timed events queued by JobSchedulingGenerator.
    Receives job_id in kwargs.

    Outcome probabilities from DAILY_VOLUMES["job_completion"]:
      92% completed, 3% cancelled, 2% no-show, 3% rescheduled
    """

    def __init__(self, db_path: str = "sparkle_shine.db"):
        self.db_path = db_path

    def _ensure_schema(self, conn: sqlite3.Connection) -> None:
        """Add churn_risk column to clients if not present."""
        cols = {row[1] for row in conn.execute("PRAGMA table_info(clients)")}
        if "churn_risk" not in cols:
            conn.execute("ALTER TABLE clients ADD COLUMN churn_risk TEXT DEFAULT 'normal'")
            conn.commit()

    def execute(self, dry_run: bool = False, job_id: Optional[str] = None) -> GeneratorResult:
        if not job_id:
            return GeneratorResult(success=False, message="job_id required")

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            self._ensure_schema(conn)

            job = conn.execute(
                "SELECT * FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()
            if not job:
                return GeneratorResult(success=False, message=f"job {job_id}: not found")
            job = dict(job)

            if dry_run:
                return GeneratorResult(success=True, message=f"job {job_id}: dry_run skip")

            session = get_client("jobber")
            jobber_job_id = get_tool_id(job_id, "jobber", db_path=self.db_path)
            if not jobber_job_id:
                return GeneratorResult(success=False, message=f"job {job_id}: no jobber mapping")

            # Determine outcome
            cfg = DAILY_VOLUMES["job_completion"]
            roll = random.random()
            if roll < cfg["on_time_rate"]:
                outcome = "completed"
            elif roll < cfg["on_time_rate"] + cfg["cancellation_rate"]:
                outcome = "cancelled"
            elif roll < cfg["on_time_rate"] + cfg["cancellation_rate"] + cfg["no_show_rate"]:
                outcome = "no-show"
            else:
                outcome = "rescheduled"

            if outcome == "completed":
                self._handle_completed(job, job_id, jobber_job_id, session, conn)
            elif outcome in ("cancelled", "no-show"):
                self._handle_cancelled_or_noshow(job, job_id, jobber_job_id, outcome, session, conn)
            else:
                self._handle_rescheduled(job, job_id, jobber_job_id, session, conn)

            conn.commit()
            return GeneratorResult(success=True, message=f"job {job_id}: {outcome}")

        except Exception as e:
            conn.rollback()
            logger.exception("JobCompletionGenerator failed for %s", job_id)
            return GeneratorResult(success=False, message=f"job {job_id}: {e}")
        finally:
            conn.close()

    def _handle_completed(
        self, job: dict, job_id: str, jobber_job_id: str, session, conn: sqlite3.Connection
    ) -> None:
        service_type_id = job.get("service_type_id", "std-residential")
        expected = _expected_duration(service_type_id, "regular")
        actual = int(expected * random.uniform(0.85, 1.15))
        completed_at = datetime.utcnow().isoformat()

        # Jobber: close the job
        resp = _gql(session, _JOB_CLOSE, {
            "jobId": jobber_job_id,
            "input": {"modifyIncompleteVisitsBy": "COMPLETE_PAST_DESTROY_FUTURE"},
        })
        errs = _gql_user_errors(resp, "jobClose")
        if errs:
            raise RuntimeError(f"jobClose errors: {errs}")

        conn.execute("""
            UPDATE jobs
            SET status = 'completed', duration_minutes_actual = ?, completed_at = ?
            WHERE id = ?
        """, (actual, completed_at, job_id))

        # Insert review
        today = date.today()
        crew_id = job.get("crew_id") or ""
        crew_name = crew_id.replace("crew-", "Crew ").title()  # "crew-a" → "Crew A"
        dist = _adjusted_rating_distribution(crew_name, today.weekday())
        ratings = [r for r, _ in dist]
        weights = [w for _, w in dist]
        rating = random.choices(ratings, weights=weights, k=1)[0]

        review_id = generate_id("REV", db_path=self.db_path)
        conn.execute("""
            INSERT INTO reviews (id, client_id, job_id, rating, platform, review_date)
            VALUES (?, ?, ?, ?, 'internal', ?)
        """, (review_id, job["client_id"], job_id, rating, today.isoformat()))

    def _handle_cancelled_or_noshow(
        self, job: dict, job_id: str, jobber_job_id: str,
        outcome: str, session, conn: sqlite3.Connection
    ) -> None:
        conn.execute("UPDATE jobs SET status = ? WHERE id = ?", (outcome, job_id))

        _gql(session, _JOB_CLOSE, {
            "jobId": jobber_job_id,
            "input": {"modifyIncompleteVisitsBy": "COMPLETE_PAST_DESTROY_FUTURE"},
        })

        # Churn risk: 3+ cancelled/no-show in 60 days → high
        count_row = conn.execute("""
            SELECT COUNT(*) FROM jobs
            WHERE client_id = ?
              AND status IN ('cancelled', 'no-show')
              AND scheduled_date >= date('now', '-60 days')
        """, (job["client_id"],)).fetchone()
        if count_row and count_row[0] >= 3:
            conn.execute(
                "UPDATE clients SET churn_risk = 'high' WHERE id = ?",
                (job["client_id"],),
            )

    def _handle_rescheduled(
        self, job: dict, job_id: str, jobber_job_id: str, session, conn: sqlite3.Connection
    ) -> None:
        # Cancel original slot
        conn.execute("UPDATE jobs SET status = 'cancelled' WHERE id = ?", (job_id,))
        _gql(session, _JOB_CLOSE, {
            "jobId": jobber_job_id,
            "input": {"modifyIncompleteVisitsBy": "COMPLETE_PAST_DESTROY_FUTURE"},
        })

        # New job for next business day
        tomorrow = _add_business_days(date.today(), 1)
        new_job_id = generate_id("JOB", db_path=self.db_path)

        service_type_id = job.get("service_type_id", "std-residential")
        crew_id = job.get("crew_id") or "crew-a"

        # jobCreate for the rescheduled slot
        property_id = _get_or_fetch_property_id(job["client_id"], session, self.db_path)
        job_input = {
            "propertyId": property_id or "",
            "title": service_type_id.replace("-", " ").title(),
            "invoicing": {
                "invoicingType": "FIXED_PRICE",
                "invoicingSchedule": "ON_COMPLETION",
            },
            "timeframe": {"startAt": tomorrow.isoformat()},
        }
        resp = _gql(session, _JOB_CREATE, {"input": job_input})
        errs = _gql_user_errors(resp, "jobCreate")
        if errs:
            raise RuntimeError(f"jobCreate (rescheduled) errors: {errs}")
        new_jobber_job_id = resp["data"]["jobCreate"]["job"]["id"]

        conn.execute("""
            INSERT INTO jobs (id, client_id, crew_id, service_type_id, scheduled_date, status)
            VALUES (?, ?, ?, ?, ?, 'scheduled')
        """, (new_job_id, job["client_id"], crew_id, service_type_id, tomorrow.isoformat()))

        register_mapping(new_job_id, "jobber", new_jobber_job_id, db_path=self.db_path)
```

- [ ] **Step 4: Run tests**

```bash
python -m pytest "tests/test_phase5_operations.py::TestJobCompletionGeneratorCompleted" -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add simulation/generators/operations.py tests/test_phase5_operations.py
git commit -m "feat: JobCompletionGenerator — schema, completed path, dry_run guard"
```

---

## Task 10: JobCompletionGenerator — remaining outcome tests

**Files:**
- Test: `tests/test_phase5_operations.py`

- [ ] **Step 1: Append remaining outcome tests**

Append to `tests/test_phase5_operations.py`:

```python
class TestJobCompletionGeneratorOutcomes(unittest.TestCase):
    """cancelled, no-show, rescheduled outcome paths."""

    def _make_conn(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript("""
            CREATE TABLE jobs (
                id TEXT PRIMARY KEY, client_id TEXT, crew_id TEXT,
                service_type_id TEXT, scheduled_date TEXT, scheduled_time TEXT,
                duration_minutes_actual INTEGER,
                status TEXT DEFAULT 'scheduled', address TEXT, notes TEXT,
                review_requested INTEGER DEFAULT 0, completed_at TEXT
            );
            CREATE TABLE clients (
                id TEXT PRIMARY KEY, client_type TEXT,
                first_name TEXT, last_name TEXT, company_name TEXT,
                email TEXT, phone TEXT, address TEXT,
                neighborhood TEXT, zone TEXT, status TEXT DEFAULT 'active',
                acquisition_source TEXT, first_service_date TEXT,
                last_service_date TEXT, lifetime_value REAL DEFAULT 0,
                notes TEXT, created_at TEXT,
                churn_risk TEXT DEFAULT 'normal'
            );
            CREATE TABLE reviews (
                id TEXT PRIMARY KEY, client_id TEXT, job_id TEXT,
                rating INTEGER, review_text TEXT, platform TEXT,
                review_date TEXT, response_text TEXT, response_date TEXT
            );
            CREATE TABLE cross_tool_mapping (
                canonical_id TEXT, tool_name TEXT, tool_specific_id TEXT,
                tool_specific_url TEXT, synced_at TEXT,
                PRIMARY KEY (canonical_id, tool_name)
            );
        """)
        conn.execute(
            "INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            ("SS-JOB-0001", "SS-LEAD-0001", "crew-b", "recurring-biweekly",
             "2026-03-30", "08:00", None, "scheduled", None, None, 0, None),
        )
        conn.execute(
            "INSERT INTO clients VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("SS-LEAD-0001", "residential", "Bob", "Jones", None,
             "b@test.com", None, None, None, None, "active",
             None, None, None, 0.0, None, "2025-01-01", "normal"),
        )
        conn.execute(
            "INSERT INTO cross_tool_mapping VALUES (?,?,?,?,?)",
            ("SS-JOB-0001", "jobber", "GQL-JOB-0001", None, "2026-03-30"),
        )
        conn.commit()
        return conn

    @patch("simulation.generators.operations.get_client")
    @patch("simulation.generators.operations.get_tool_id")
    @patch("simulation.generators.operations._gql")
    def test_cancelled_sets_status(self, mock_gql, mock_get_tool_id, mock_get_client):
        from simulation.generators.operations import JobCompletionGenerator
        conn = self._make_conn()
        mock_get_tool_id.return_value = "GQL-JOB-0001"
        mock_gql.return_value = {
            "data": {"jobClose": {"job": {"id": "GQL-JOB-0001"}, "userErrors": []}}
        }
        gen = JobCompletionGenerator(db_path=":memory:")
        with patch("sqlite3.connect", return_value=conn):
            # roll lands in cancelled window: 0.92 < x < 0.95
            with patch("random.random", return_value=0.93):
                result = gen.execute(dry_run=False, job_id="SS-JOB-0001")
        self.assertTrue(result.success)
        job = dict(conn.execute("SELECT * FROM jobs WHERE id = ?", ("SS-JOB-0001",)).fetchone())
        self.assertEqual(job["status"], "cancelled")

    @patch("simulation.generators.operations.get_client")
    @patch("simulation.generators.operations.get_tool_id")
    @patch("simulation.generators.operations._gql")
    def test_noshow_sets_status(self, mock_gql, mock_get_tool_id, mock_get_client):
        from simulation.generators.operations import JobCompletionGenerator
        conn = self._make_conn()
        mock_get_tool_id.return_value = "GQL-JOB-0001"
        mock_gql.return_value = {
            "data": {"jobClose": {"job": {"id": "GQL-JOB-0001"}, "userErrors": []}}
        }
        gen = JobCompletionGenerator(db_path=":memory:")
        with patch("sqlite3.connect", return_value=conn):
            # no-show window: 0.95 < x < 0.97
            with patch("random.random", return_value=0.96):
                result = gen.execute(dry_run=False, job_id="SS-JOB-0001")
        self.assertTrue(result.success)
        job = dict(conn.execute("SELECT * FROM jobs WHERE id = ?", ("SS-JOB-0001",)).fetchone())
        self.assertEqual(job["status"], "no-show")

    @patch("simulation.generators.operations.get_client")
    @patch("simulation.generators.operations.register_mapping")
    @patch("simulation.generators.operations.generate_id")
    @patch("simulation.generators.operations.get_tool_id")
    @patch("simulation.generators.operations._gql")
    def test_rescheduled_creates_new_job_for_tomorrow(
        self, mock_gql, mock_get_tool_id, mock_gen_id, mock_reg_map, mock_get_client
    ):
        from simulation.generators.operations import JobCompletionGenerator
        conn = self._make_conn()
        mock_get_tool_id.side_effect = lambda cid, tool, **kw: (
            "GQL-JOB-0001" if tool == "jobber" else "GQL-PROP-0001"
        )
        mock_gen_id.return_value = "SS-JOB-9999"
        mock_gql.side_effect = [
            {"data": {"jobClose": {"job": {"id": "GQL-JOB-0001"}, "userErrors": []}}},
            {"data": {"jobCreate": {"job": {"id": "GQL-JOB-9999"}, "userErrors": []}}},
        ]
        gen = JobCompletionGenerator(db_path=":memory:")
        with patch("sqlite3.connect", return_value=conn):
            # rescheduled window: x >= 0.97
            with patch("random.random", return_value=0.98):
                result = gen.execute(dry_run=False, job_id="SS-JOB-0001")

        self.assertTrue(result.success)
        # Original job cancelled
        orig = dict(conn.execute("SELECT * FROM jobs WHERE id = ?", ("SS-JOB-0001",)).fetchone())
        self.assertEqual(orig["status"], "cancelled")
        # New job exists with status=scheduled
        new_job = conn.execute("SELECT * FROM jobs WHERE id = ?", ("SS-JOB-9999",)).fetchone()
        self.assertIsNotNone(new_job)
        self.assertEqual(dict(new_job)["status"], "scheduled")

    @patch("simulation.generators.operations.get_client")
    @patch("simulation.generators.operations.get_tool_id")
    @patch("simulation.generators.operations._gql")
    def test_churn_risk_set_to_high_after_3_cancellations(
        self, mock_gql, mock_get_tool_id, mock_get_client
    ):
        from simulation.generators.operations import JobCompletionGenerator
        conn = self._make_conn()
        # Pre-populate 2 existing cancellations in the last 60 days
        conn.execute("""
            INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, ("SS-JOB-OLD1", "SS-LEAD-0001", "crew-b", "recurring-biweekly",
              "2026-02-15", None, None, "cancelled", None, None, 0, None))
        conn.execute("""
            INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, ("SS-JOB-OLD2", "SS-LEAD-0001", "crew-b", "recurring-biweekly",
              "2026-03-01", None, None, "cancelled", None, None, 0, None))
        conn.execute(
            "INSERT INTO cross_tool_mapping VALUES (?,?,?,?,?)",
            ("SS-JOB-0001", "jobber", "GQL-JOB-0001", None, "2026-03-30"),
        )
        conn.commit()

        mock_get_tool_id.return_value = "GQL-JOB-0001"
        mock_gql.return_value = {
            "data": {"jobClose": {"job": {"id": "GQL-JOB-0001"}, "userErrors": []}}
        }
        gen = JobCompletionGenerator(db_path=":memory:")
        with patch("sqlite3.connect", return_value=conn):
            with patch("random.random", return_value=0.93):  # cancelled
                gen.execute(dry_run=False, job_id="SS-JOB-0001")

        client = dict(conn.execute(
            "SELECT churn_risk FROM clients WHERE id = ?", ("SS-LEAD-0001",)
        ).fetchone())
        self.assertEqual(client["churn_risk"], "high")

    @patch("simulation.generators.operations.get_client")
    @patch("simulation.generators.operations.get_tool_id")
    @patch("simulation.generators.operations._gql")
    def test_churn_risk_not_set_for_only_2_cancellations(
        self, mock_gql, mock_get_tool_id, mock_get_client
    ):
        from simulation.generators.operations import JobCompletionGenerator
        conn = self._make_conn()
        # Only 1 prior cancellation in 60 days (total will be 2 after this)
        conn.execute("""
            INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, ("SS-JOB-OLD1", "SS-LEAD-0001", "crew-b", "recurring-biweekly",
              "2026-03-01", None, None, "cancelled", None, None, 0, None))
        conn.commit()

        mock_get_tool_id.return_value = "GQL-JOB-0001"
        mock_gql.return_value = {
            "data": {"jobClose": {"job": {"id": "GQL-JOB-0001"}, "userErrors": []}}
        }
        gen = JobCompletionGenerator(db_path=":memory:")
        with patch("sqlite3.connect", return_value=conn):
            with patch("random.random", return_value=0.93):  # cancelled
                gen.execute(dry_run=False, job_id="SS-JOB-0001")

        client = dict(conn.execute(
            "SELECT churn_risk FROM clients WHERE id = ?", ("SS-LEAD-0001",)
        ).fetchone())
        self.assertNotEqual(client["churn_risk"], "high")
```

- [ ] **Step 2: Run outcome tests**

```bash
python -m pytest "tests/test_phase5_operations.py::TestJobCompletionGeneratorOutcomes" -v
```
Expected: all 5 PASS

- [ ] **Step 3: Commit**

```bash
git add tests/test_phase5_operations.py
git commit -m "test: JobCompletionGenerator outcome paths and churn risk"
```

---

## Task 11: Integration test stubs

**Files:**
- Test: `tests/test_phase5_operations.py`

- [ ] **Step 1: Append integration tests (gated)**

Append to `tests/test_phase5_operations.py`:

```python
# ── Integration tests (require RUN_INTEGRATION=1) ────────────────────────────

@unittest.skipUnless(os.getenv("RUN_INTEGRATION"), "Skipping integration tests")
class TestOperationsIntegration(unittest.TestCase):
    """Live API tests — require real Jobber credentials and sparkle_shine.db."""

    DB_PATH = "sparkle_shine.db"

    def test_jobber_client_create_and_job_create_roundtrip(self):
        """NewClientSetupGenerator creates a real Jobber client and job."""
        from simulation.generators.operations import NewClientSetupGenerator
        from database.mappings import get_tool_id
        import sqlite3

        # Find a ready won deal that has no Jobber mapping yet
        conn = sqlite3.connect(self.DB_PATH)
        conn.row_factory = sqlite3.Row
        ready = conn.execute("""
            SELECT canonical_id FROM won_deals
            WHERE start_date <= date('now')
              AND canonical_id NOT IN (
                  SELECT canonical_id FROM cross_tool_mapping WHERE tool_name = 'jobber'
              )
            LIMIT 1
        """).fetchone()
        conn.close()

        if not ready:
            self.skipTest("No ready won deals without Jobber mapping")

        canonical_id = ready["canonical_id"]
        gen = NewClientSetupGenerator(db_path=self.DB_PATH)
        result = gen.execute(dry_run=False)

        self.assertTrue(result.success, f"Setup failed: {result.message}")
        jobber_client_id = get_tool_id(canonical_id, "jobber", db_path=self.DB_PATH)
        self.assertIsNotNone(jobber_client_id)
        self.assertTrue(jobber_client_id.startswith("Z2lkOi"),
            "Jobber IDs are base64-encoded Global IDs starting with Z2lkOi")

    def test_rescheduled_job_picked_up_next_day(self):
        """Rescheduled job appears in JobSchedulingGenerator pass-2 on its new date."""
        from simulation.generators.operations import JobCompletionGenerator, JobSchedulingGenerator
        import sqlite3

        # Find a scheduled job today to reschedule
        conn = sqlite3.connect(self.DB_PATH)
        conn.row_factory = sqlite3.Row
        job = conn.execute("""
            SELECT j.id FROM jobs j
            JOIN cross_tool_mapping m ON j.id = m.canonical_id AND m.tool_name = 'jobber'
            WHERE j.status = 'scheduled'
              AND j.scheduled_date = date('now')
            LIMIT 1
        """).fetchone()
        conn.close()

        if not job:
            self.skipTest("No scheduled jobs today with Jobber mappings")

        job_id = job["id"]

        # Force rescheduled outcome
        with patch("random.random", return_value=0.98):
            completion_gen = JobCompletionGenerator(db_path=self.DB_PATH)
            result = completion_gen.execute(dry_run=False, job_id=job_id)

        self.assertTrue(result.success)
        self.assertIn("rescheduled", result.message)

        # Verify new job exists in SQLite for tomorrow
        from datetime import date, timedelta
        tomorrow = (date.today() + timedelta(days=1 if date.today().weekday() < 4 else 3)).isoformat()
        conn2 = sqlite3.connect(self.DB_PATH)
        conn2.row_factory = sqlite3.Row
        new_job = conn2.execute("""
            SELECT id FROM jobs
            WHERE client_id = (SELECT client_id FROM jobs WHERE id = ?)
              AND scheduled_date = ?
              AND status = 'scheduled'
              AND id != ?
        """, (job_id, tomorrow, job_id)).fetchone()
        conn2.close()
        self.assertIsNotNone(new_job, "Rescheduled job not found in SQLite for tomorrow")
```

- [ ] **Step 2: Run without RUN_INTEGRATION — verify skips**

```bash
python -m pytest "tests/test_phase5_operations.py::TestOperationsIntegration" -v
```
Expected: `2 tests SKIPPED`

- [ ] **Step 3: Run full test suite**

```bash
python -m pytest tests/test_phase5_operations.py -v --tb=short 2>&1 | tail -20
```
Expected: all non-integration tests PASS, integration tests SKIPPED

- [ ] **Step 4: Confirm engine + prior phase tests unaffected**

```bash
python -m pytest tests/ -v --tb=short -k "not Integration" 2>&1 | tail -15
```
Expected: all previously passing tests still PASS

- [ ] **Step 5: Final commit**

```bash
git add tests/test_phase5_operations.py
git commit -m "test: integration test stubs for operations generator (gated behind RUN_INTEGRATION)"
```

---

## Self-Review Checklist

**Spec coverage:**
- [x] `won_deals` trigger query (NewClientSetupGenerator trigger query) — Task 6
- [x] Recurring path: clientCreate + propertyCreate + jobCreate + recurring_agreements INSERT — Task 6
- [x] One-time path: clientCreate + propertyCreate + jobCreate + jobs INSERT — Task 6
- [x] Per-client try/except error isolation — Task 7
- [x] `queue_fn` parameter pattern, `queue_fn=None` default — Task 8 constructor
- [x] Engine TimedEvent namedtuple — Task 1
- [x] `_timed_queue` heapq attribute — Task 1
- [x] `queue_timed_event()` method — Task 1
- [x] Checkpoint serialization of timed queue — Task 2
- [x] `run_once()` drain loop — Task 3
- [x] `plan_day()` ops prefix ordering — Task 3
- [x] Generator registration in `_register_generators()` — Task 3
- [x] `_is_due_today()` weekly/biweekly/monthly/clamping — Task 4 + Task 5
- [x] `_pick_job_type()` residential + commercial + seasonal boost — Task 4 + Task 5
- [x] `_assign_scheduled_time()` sequential routing with travel buffer — Task 4 + Task 5
- [x] `_adjusted_rating_distribution()` 4 combos + cap — Task 4 + Task 5
- [x] `JobSchedulingGenerator` recurring loop + already-scheduled second pass — Task 8
- [x] `JobCompletionGenerator` completed path with jobClose + review insert — Task 9
- [x] `JobCompletionGenerator` cancelled/no-show + churn risk — Task 10
- [x] `JobCompletionGenerator` rescheduled path — Task 10
- [x] `dry_run=True` skips API + SQLite + reviews — Tasks 6, 9
- [x] `churn_risk` column added defensively — Task 9 `_ensure_schema()`
- [x] Integration tests gated behind `RUN_INTEGRATION` — Task 11
- [x] Pipedrive activity note — Task 6 `_setup_client()`
- [x] `generate_id("RECUR")` for recurring agreements — Task 6
- [x] `_add_business_days()` for rescheduled jobs — Task 4, Task 10

**No placeholders:** All steps contain actual code.

**Type consistency:**
- `GeneratorResult(success=bool, message=str)` used consistently across all three generators
- `_gql(session, query, variables)` → dict used in Tasks 6, 8, 9, 10
- `generate_id("RECUR")` in Task 6, `generate_id("JOB")` in Tasks 6, 10, `generate_id("REV")` in Task 9
- `_is_due_today(agreement: dict, today: date)` defined in Task 4, used in Task 8
- `_assign_scheduled_time(crew_id, prior_jobs, today)` defined in Task 4, used in Task 8
