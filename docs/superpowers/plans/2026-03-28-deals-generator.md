# DealGenerator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `simulation/generators/deals.py` — a Type 2 generator that advances existing Pipedrive deals through the sales pipeline, filling in contract details when a deal reaches Won.

**Architecture:** `DealGenerator.execute()` is a thin orchestrator: it picks one open deal uniformly at random, then calls `_advance_deal()` which rolls two independent dice (advance probability, then loss probability if advance misses). Private helpers handle each concern — picking deals, computing probability, building contracts, writing to Pipedrive and SQLite, and logging activity notes. All writes (API + SQLite) are skipped in dry_run mode; reads are always allowed (Type 2 generators need existing state to produce meaningful output).

**Tech Stack:** Python 3, `requests` (via `auth.get_client`), `sqlite3`, `unittest.mock`, Pipedrive REST API v1.

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `simulation/generators/deals.py` | **Create** | `DealGenerator` class — full implementation |
| `tests/test_deals.py` | **Create** | Unit + integration tests; no live API calls, no writes to `sparkle_shine.db` |

No other files are modified. `simulation/engine.py` already registers `DealGenerator` via a conditional import (lines 101–106).

---

## Task 1: Skeleton, `GeneratorResult`, and probability functions

**Files:**
- Create: `simulation/generators/deals.py`
- Create: `tests/test_deals.py`

- [ ] **Step 1: Write failing tests for both probability methods**

Create `tests/test_deals.py`:

```python
"""
tests/test_deals.py

Unit and integration tests for DealGenerator.
No live API calls. No writes to sparkle_shine.db.

Run:
    python tests/test_deals.py -v
    python tests/test_deals.py -v -k "Probability"
"""
from __future__ import annotations

import os
import random
import sqlite3
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta
from unittest.mock import MagicMock, patch

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


def _bare_gen(db_path=":memory:"):
    """Return a DealGenerator with __init__ bypassed; only db_path set."""
    from simulation.generators.deals import DealGenerator
    gen = object.__new__(DealGenerator)
    gen.db_path = db_path
    return gen


class TestProbabilities(unittest.TestCase):
    """Pure unit tests — no API calls, no SQLite writes.

    datetime.utcnow is pinned to 2026-03-28 12:00:00 so that
    stage_change_time offsets produce exact integer day values.
    """

    def setUp(self):
        self._patcher = patch("simulation.generators.deals.datetime")
        mock_dt = self._patcher.start()
        mock_dt.utcnow.return_value = datetime(2026, 3, 28, 12, 0, 0)
        mock_dt.fromisoformat = datetime.fromisoformat

    def tearDown(self):
        self._patcher.stop()

    def _deal(self, days_ago: int) -> dict:
        """Deal with stage_change_time exactly `days_ago` days before pinned now."""
        sct = (datetime(2026, 3, 28, 12, 0, 0) - timedelta(days=days_ago)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        return {"id": 1, "stage_change_time": sct, "update_time": sct}

    # ── calculate_advance_probability ────────────────────────────────────────

    def test_advance_probability_days_2(self):
        gen = _bare_gen()
        self.assertAlmostEqual(gen.calculate_advance_probability(self._deal(2)), 0.150, places=3)

    def test_advance_probability_days_5(self):
        gen = _bare_gen()
        self.assertAlmostEqual(gen.calculate_advance_probability(self._deal(5)), 0.225, places=3)

    def test_advance_probability_days_10(self):
        gen = _bare_gen()
        self.assertAlmostEqual(gen.calculate_advance_probability(self._deal(10)), 0.300, places=3)

    def test_advance_probability_days_20(self):
        gen = _bare_gen()
        self.assertAlmostEqual(gen.calculate_advance_probability(self._deal(20)), 0.075, places=3)

    # ── calculate_loss_probability ───────────────────────────────────────────

    def test_loss_probability_days_2(self):
        gen = _bare_gen()
        self.assertAlmostEqual(gen.calculate_loss_probability(self._deal(2)), 0.015, places=3)

    def test_loss_probability_days_5(self):
        gen = _bare_gen()
        self.assertAlmostEqual(gen.calculate_loss_probability(self._deal(5)), 0.030, places=3)

    def test_loss_probability_days_10(self):
        gen = _bare_gen()
        self.assertAlmostEqual(gen.calculate_loss_probability(self._deal(10)), 0.045, places=3)

    def test_loss_probability_days_20(self):
        gen = _bare_gen()
        self.assertAlmostEqual(gen.calculate_loss_probability(self._deal(20)), 0.075, places=3)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run tests — expect ImportError / NameError (FAIL)**

```bash
cd /Users/ovieoghor/Documents/Claude\ Code\ Exercises/Simulation\ Exercise/sparkle-shine-poc
python tests/test_deals.py -v 2>&1 | head -20
```

Expected: `ModuleNotFoundError: No module named 'simulation.generators.deals'`

- [ ] **Step 3: Create `simulation/generators/deals.py` skeleton with probability functions**

```python
"""
simulation/generators/deals.py

Advances existing Pipedrive deals through the sales pipeline.
Each execute() call picks one open deal and probabilistically
advances it, marks it lost, or leaves it unchanged.

Type 2 generator: progresses existing records (does not create new ones).
# Dry-run convention: reads always allowed; writes (API + SQLite) skipped.
"""
from __future__ import annotations

import json
import random
import sqlite3
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

from auth import get_client
from config.business import SERVICE_TYPES
from database.mappings import get_canonical_id
from intelligence.logging_config import setup_logging
from simulation.config import (
    COMMERCIAL_SERVICE_WEIGHTS,
    CREW_ASSIGNMENT_WEIGHTS,
    DAILY_VOLUMES,
    SERVICE_TYPE_WEIGHTS,
)

logger = setup_logging("simulation.deals")


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------

@dataclass
class GeneratorResult:
    success: bool
    message: str = ""


# ---------------------------------------------------------------------------
# Module-level price/range lookups (derived from config/business.py)
# ---------------------------------------------------------------------------

_SERVICE_ID_PRICES = {
    st["id"]: st["base_price"] for st in SERVICE_TYPES if st.get("base_price")
}

_FREQ_TO_SERVICE_ID = {
    "weekly_recurring":     "recurring-weekly",
    "biweekly_recurring":   "recurring-biweekly",
    "monthly_recurring":    "recurring-monthly",
    "one_time_standard":    "std-residential",
    "one_time_deep_clean":  "deep-clean",
    "one_time_move_in_out": "move-in-out",
}

_COMMERCIAL_RANGES = {
    "nightly_clean":      (1500, 4500),
    "weekend_deep_clean": (300, 800),
    "one_time_project":   (500, 2000),
}


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def _advance_age_weight(days: float) -> float:
    """Age bracket multiplier for advance probability."""
    if days <= 3:
        return 1.0
    elif days <= 7:
        return 1.5
    elif days <= 14:
        return 2.0
    else:
        return 0.5


def _loss_age_weight(days: float) -> float:
    """Age bracket multiplier for loss probability (inverted — stale deals bleed out)."""
    if days <= 3:
        return 0.5
    elif days <= 7:
        return 1.0
    elif days <= 14:
        return 1.5
    else:
        return 2.5


def _add_business_days(start: date, n: int) -> date:
    """Return the date that is `n` business days (Mon–Fri) after `start`."""
    d = start
    added = 0
    while added < n:
        d += timedelta(days=1)
        if d.weekday() < 5:
            added += 1
    return d


# ---------------------------------------------------------------------------
# DealGenerator
# ---------------------------------------------------------------------------

class DealGenerator:
    """
    Advances existing Pipedrive deals through the sales pipeline.

    Registered with the simulation engine as the "deals" generator.
    Engine calls execute(dry_run=...) on each tick.
    """

    def __init__(self, db_path: str = "sparkle_shine.db"):
        raise NotImplementedError("Task 2 — implement __init__")

    def execute(self, dry_run: bool = False) -> GeneratorResult:
        raise NotImplementedError("Task 6 — implement execute")

    def _pick_deal(self) -> Optional[dict]:
        raise NotImplementedError("Task 3 — implement _pick_deal")

    def calculate_advance_probability(self, deal: dict) -> float:
        """Return probability [0, 1] that this deal advances today."""
        base = DAILY_VOLUMES["deal_progression"]["stage_advance_probability"]
        age = self._days_in_stage(deal)
        return base * _advance_age_weight(age)

    def calculate_loss_probability(self, deal: dict) -> float:
        """Return probability [0, 1] that this deal is lost today."""
        base = DAILY_VOLUMES["deal_progression"]["lost_probability_per_stage"]
        age = self._days_in_stage(deal)
        return base * _loss_age_weight(age)

    def _advance_deal(self, deal: dict, dry_run: bool = False) -> GeneratorResult:
        raise NotImplementedError("Task 4 — implement _advance_deal")

    def _complete_won_deal(self, deal: dict, contract: dict, dry_run: bool = False) -> None:
        raise NotImplementedError("Task 5 — implement _complete_won_deal")

    def _log_activity(self, deal_id: int, note: str, dry_run: bool = False) -> None:
        raise NotImplementedError("Task 3 — implement _log_activity")

    def _ensure_schema(self, conn: sqlite3.Connection) -> None:
        raise NotImplementedError("Task 2 — implement _ensure_schema")

    # ── Internal helpers ────────────────────────────────────────────────────

    def _days_in_stage(self, deal: dict) -> float:
        """Days the deal has been in its current stage (3-level fallback)."""
        now = datetime.utcnow()

        # Level 1: Pipedrive native stage_change_time
        sct = deal.get("stage_change_time")
        if sct:
            return (now - datetime.fromisoformat(sct.replace(" ", "T"))).days

        # Level 2: SQLite commercial_proposals.stage_change_time (SS-PROP only)
        try:
            canonical_id = get_canonical_id("pipedrive", str(deal["id"]), self.db_path)
            if canonical_id and canonical_id.startswith("SS-PROP-"):
                with sqlite3.connect(self.db_path) as conn:
                    row = conn.execute(
                        "SELECT stage_change_time FROM commercial_proposals WHERE id = ?",
                        (canonical_id,),
                    ).fetchone()
                    if row and row[0]:
                        return (now - datetime.fromisoformat(row[0])).days
        except Exception:
            pass

        # Level 3: update_time as last-resort proxy
        update_time = deal.get("update_time", "")
        if update_time:
            return (now - datetime.fromisoformat(update_time.replace(" ", "T"))).days
        return 0.0

    def _pick_service_frequency(self, client_type: str, emv: float) -> str:
        raise NotImplementedError("Task 4 — implement _pick_service_frequency")

    def _build_contract(self, deal: dict) -> dict:
        raise NotImplementedError("Task 4 — implement _build_contract")

    @staticmethod
    def _get_contract_value(service_frequency: str) -> float:
        raise NotImplementedError("Task 4 — implement _get_contract_value")
```

- [ ] **Step 4: Run tests — expect PASS for all 8**

```bash
python tests/test_deals.py -v -k "Probability"
```

Expected output (8 tests):
```
test_advance_probability_days_10 ... ok
test_advance_probability_days_2 ... ok
test_advance_probability_days_20 ... ok
test_advance_probability_days_5 ... ok
test_loss_probability_days_10 ... ok
test_loss_probability_days_2 ... ok
test_loss_probability_days_20 ... ok
test_loss_probability_days_5 ... ok
----------------------------------------------------------------------
Ran 8 tests in 0.XXXs

OK
```

- [ ] **Step 5: Commit**

```bash
git add simulation/generators/deals.py tests/test_deals.py
git commit -m "feat: add DealGenerator skeleton with probability functions and tests"
```

---

## Task 2: `_ensure_schema` and `__init__`

**Files:**
- Modify: `tests/test_deals.py` (append `TestEnsureSchema` + `TestInit`)
- Modify: `simulation/generators/deals.py` (implement both methods)

- [ ] **Step 1: Append failing tests**

Add to the bottom of `tests/test_deals.py` (before `if __name__ == "__main__":`):

```python
class TestEnsureSchema(unittest.TestCase):

    def test_adds_missing_columns_to_commercial_proposals(self):
        from simulation.generators.deals import DealGenerator
        gen = _bare_gen()
        conn = sqlite3.connect(":memory:")
        conn.execute(
            "CREATE TABLE commercial_proposals "
            "(id TEXT PRIMARY KEY, title TEXT, status TEXT DEFAULT 'draft')"
        )
        gen._ensure_schema(conn)
        cols = {row[1] for row in conn.execute("PRAGMA table_info(commercial_proposals)")}
        self.assertIn("start_date", cols)
        self.assertIn("crew_assignment", cols)
        self.assertIn("stage_change_time", cols)
        conn.close()

    def test_idempotent_on_second_call(self):
        """Running _ensure_schema twice must not raise."""
        from simulation.generators.deals import DealGenerator
        gen = _bare_gen()
        conn = sqlite3.connect(":memory:")
        conn.execute(
            "CREATE TABLE commercial_proposals "
            "(id TEXT PRIMARY KEY, title TEXT, status TEXT DEFAULT 'draft')"
        )
        gen._ensure_schema(conn)
        gen._ensure_schema(conn)  # second call — should not raise
        conn.close()


class TestInit(unittest.TestCase):

    def setUp(self):
        self._fd, self._db_path = tempfile.mkstemp(suffix=".db")
        from database.schema import init_db
        init_db(self._db_path)

    def tearDown(self):
        os.close(self._fd)
        os.unlink(self._db_path)

    def test_loads_stage_ids_from_tool_ids_json(self):
        from simulation.generators.deals import DealGenerator
        gen = DealGenerator(db_path=self._db_path)
        # Values from config/tool_ids.json
        self.assertEqual(gen._won_stage_id, 12)
        self.assertEqual(gen._lost_stage_id, 13)
        self.assertEqual(len(gen._stage_order), 6)
        self.assertIn(7, gen._stage_order)   # New Lead
        self.assertIn(11, gen._stage_order)  # Negotiation
        self.assertEqual(gen._stage_order[-1], 12)  # Closed Won is last

    def test_schema_columns_added_on_init(self):
        from simulation.generators.deals import DealGenerator
        DealGenerator(db_path=self._db_path)
        conn = sqlite3.connect(self._db_path)
        cols = {row[1] for row in conn.execute("PRAGMA table_info(commercial_proposals)")}
        conn.close()
        self.assertIn("start_date", cols)
        self.assertIn("crew_assignment", cols)
        self.assertIn("stage_change_time", cols)
```

- [ ] **Step 2: Run to verify FAIL**

```bash
python tests/test_deals.py -v -k "Schema or Init"
```

Expected: `NotImplementedError: Task 2 — implement _ensure_schema`

- [ ] **Step 3: Implement `_ensure_schema` and `__init__` in `deals.py`**

Replace the `_ensure_schema` stub with:

```python
def _ensure_schema(self, conn: sqlite3.Connection) -> None:
    """Add missing columns to commercial_proposals (SQLite < 3.35 compatible)."""
    existing = {row[1] for row in conn.execute("PRAGMA table_info(commercial_proposals)")}
    for col_name, col_type in [
        ("start_date",        "TEXT"),
        ("crew_assignment",   "TEXT"),
        ("stage_change_time", "TEXT"),
    ]:
        if col_name not in existing:
            conn.execute(
                f"ALTER TABLE commercial_proposals ADD COLUMN {col_name} {col_type}"
            )
```

Replace the `__init__` stub with:

```python
def __init__(self, db_path: str = "sparkle_shine.db"):
    self.db_path = db_path
    tool_ids = json.loads(Path("config/tool_ids.json").read_text())
    stages = tool_ids["pipedrive"]["stages"]
    self._stage_order = [
        stages["New Lead"],
        stages["Qualified"],
        stages["Site Visit Scheduled"],
        stages["Proposal Sent"],
        stages["Negotiation"],
        stages["Closed Won"],
    ]
    self._won_stage_id  = stages["Closed Won"]
    self._lost_stage_id = stages["Closed Lost"]
    self._stage_names   = {v: k for k, v in stages.items()}

    fields = tool_ids["pipedrive"]["deal_fields"]
    self._client_type_field = fields["Client Type"]
    self._service_type_field = fields["Service Type"]
    self._emv_field          = fields["Estimated Monthly Value"]

    with sqlite3.connect(self.db_path) as conn:
        self._ensure_schema(conn)
```

- [ ] **Step 4: Run — expect PASS for all 4 new tests + 8 existing**

```bash
python tests/test_deals.py -v
```

Expected:
```
Ran 12 tests in 0.XXXs
OK
```

- [ ] **Step 5: Commit**

```bash
git add simulation/generators/deals.py tests/test_deals.py
git commit -m "feat: implement DealGenerator __init__ and _ensure_schema"
```

---

## Task 3: `_pick_deal` and `_log_activity`

**Files:**
- Modify: `tests/test_deals.py` (append `TestPickDeal` + `TestLogActivity`)
- Modify: `simulation/generators/deals.py` (implement both methods)

- [ ] **Step 1: Append failing tests**

```python
class TestPickDeal(unittest.TestCase):

    def setUp(self):
        self._fd, self._db_path = tempfile.mkstemp(suffix=".db")
        from database.schema import init_db
        init_db(self._db_path)
        from simulation.generators.deals import DealGenerator
        self.gen = DealGenerator(db_path=self._db_path)

    def tearDown(self):
        os.close(self._fd)
        os.unlink(self._db_path)

    def _mock_pipedrive(self, deals):
        mc = MagicMock()
        mc.get.return_value.raise_for_status = MagicMock()
        mc.get.return_value.json.return_value = {"data": deals}
        return mc

    def test_returns_none_when_no_open_deals(self):
        with patch("simulation.generators.deals.get_client", return_value=self._mock_pipedrive(None)):
            with patch("time.sleep"):
                result = self.gen._pick_deal()
        self.assertIsNone(result)

    def test_returns_one_deal_from_list(self):
        deals = [{"id": i, "stage_id": 8} for i in range(1, 6)]
        with patch("simulation.generators.deals.get_client", return_value=self._mock_pipedrive(deals)):
            with patch("time.sleep"):
                result = self.gen._pick_deal()
        self.assertIn(result, deals)

    def test_uniform_selection_over_500_trials(self):
        """Each of 5 deals should be picked ~100 times; assert within [50, 150]."""
        deals = [{"id": i, "stage_id": 8} for i in range(1, 6)]
        counts = {i: 0 for i in range(1, 6)}
        random.seed(0)
        with patch("simulation.generators.deals.get_client", return_value=self._mock_pipedrive(deals)):
            with patch("time.sleep"):
                for _ in range(500):
                    d = self.gen._pick_deal()
                    counts[d["id"]] += 1
        for deal_id, count in counts.items():
            self.assertGreater(count, 50, f"deal {deal_id} picked only {count} times")
            self.assertLess(count, 150, f"deal {deal_id} picked {count} times (too often)")


class TestLogActivity(unittest.TestCase):

    def setUp(self):
        self._fd, self._db_path = tempfile.mkstemp(suffix=".db")
        from database.schema import init_db
        init_db(self._db_path)
        from simulation.generators.deals import DealGenerator
        self.gen = DealGenerator(db_path=self._db_path)

    def tearDown(self):
        os.close(self._fd)
        os.unlink(self._db_path)

    def test_posts_note_activity_to_pipedrive(self):
        mc = MagicMock()
        mc.post.return_value.status_code = 201
        with patch("simulation.generators.deals.get_client", return_value=mc):
            with patch("time.sleep"):
                self.gen._log_activity(deal_id=42, note="Stage updated", dry_run=False)
        mc.post.assert_called_once_with(
            "https://api.pipedrive.com/v1/activities",
            json={
                "deal_id": 42,
                "subject": "Stage update",
                "type": "note",
                "note": "Stage updated",
                "done": 1,
            },
        )

    def test_dry_run_skips_post(self):
        mc = MagicMock()
        with patch("simulation.generators.deals.get_client", return_value=mc):
            self.gen._log_activity(deal_id=42, note="Stage updated", dry_run=True)
        mc.post.assert_not_called()
```

- [ ] **Step 2: Run to verify FAIL**

```bash
python tests/test_deals.py -v -k "PickDeal or LogActivity"
```

Expected: `NotImplementedError: Task 3`

- [ ] **Step 3: Implement `_pick_deal` and `_log_activity` in `deals.py`**

Replace the `_pick_deal` stub:

```python
def _pick_deal(self) -> Optional[dict]:
    """Fetch open deals from Pipedrive and return one at random (uniform)."""
    time.sleep(0.15)
    client = get_client("pipedrive")
    resp = client.get(
        "https://api.pipedrive.com/v1/deals",
        params={"status": "open", "sort": "update_time DESC", "limit": 100},
    )
    resp.raise_for_status()
    deals = resp.json().get("data") or []
    if not deals:
        return None
    return random.choices(deals, k=1)[0]
```

Replace the `_log_activity` stub:

```python
def _log_activity(self, deal_id: int, note: str, dry_run: bool = False) -> None:
    """POST a note activity to Pipedrive for the given deal."""
    if dry_run:
        logger.debug("[dry_run] Activity note for deal %s: %s", deal_id, note)
        return
    time.sleep(0.15)
    client = get_client("pipedrive")
    resp = client.post(
        "https://api.pipedrive.com/v1/activities",
        json={
            "deal_id": deal_id,
            "subject": "Stage update",
            "type": "note",
            "note": note,
            "done": 1,
        },
    )
    if resp.status_code not in (200, 201):
        logger.warning("POST activities for deal %s failed: %s", deal_id, resp.status_code)
```

- [ ] **Step 4: Run — expect PASS for all 5 new tests + 12 existing**

```bash
python tests/test_deals.py -v
```

Expected:
```
Ran 17 tests in 0.XXXs
OK
```

- [ ] **Step 5: Commit**

```bash
git add simulation/generators/deals.py tests/test_deals.py
git commit -m "feat: implement _pick_deal (uniform) and _log_activity"
```

---

## Task 4: `_advance_deal` and contract helpers

**Files:**
- Modify: `tests/test_deals.py` (append `TestAdvanceDeal`)
- Modify: `simulation/generators/deals.py` (implement `_advance_deal`, `_build_contract`, `_pick_service_frequency`, `_get_contract_value`)

- [ ] **Step 1: Append failing tests**

```python
class TestAdvanceDeal(unittest.TestCase):
    """Seed random, mock Pipedrive PUT. No SQLite fixture needed for these cases."""

    def setUp(self):
        self._fd, self._db_path = tempfile.mkstemp(suffix=".db")
        from database.schema import init_db
        init_db(self._db_path)
        from simulation.generators.deals import DealGenerator
        self.gen = DealGenerator(db_path=self._db_path)
        # Deal in Qualified (stage_id=8), 5 days in stage
        now = datetime.utcnow()
        sct = (now - timedelta(days=5)).strftime("%Y-%m-%d %H:%M:%S")
        self.deal = {"id": 100, "stage_id": 8, "stage_change_time": sct, "update_time": sct}

    def tearDown(self):
        os.close(self._fd)
        os.unlink(self._db_path)

    def _mock_client(self, put_status=200, post_status=201):
        mc = MagicMock()
        mc.put.return_value.status_code = put_status
        mc.post.return_value.status_code = post_status
        return mc

    def test_advance_fires_and_puts_next_stage(self):
        """random.random()=0.01 → advance fires → PUT with stage_id=9 (Site Visit)."""
        mc = self._mock_client()
        with patch("simulation.generators.deals.get_client", return_value=mc):
            with patch("time.sleep"):
                with patch("random.random", return_value=0.01):
                    result = self.gen._advance_deal(self.deal, dry_run=False)

        self.assertTrue(result.success)
        mc.put.assert_called_once_with(
            "https://api.pipedrive.com/v1/deals/100",
            json={"stage_id": 9},  # Site Visit Scheduled
        )

    def test_no_advance_no_loss_returns_no_change(self):
        """random.random()=0.99 → neither roll fires → no PUT, message='no change'."""
        mc = self._mock_client()
        with patch("simulation.generators.deals.get_client", return_value=mc):
            with patch("time.sleep"):
                with patch("random.random", return_value=0.99):
                    result = self.gen._advance_deal(self.deal, dry_run=False)

        self.assertTrue(result.success)
        self.assertEqual(result.message, "no change")
        mc.put.assert_not_called()

    def test_loss_fires_when_advance_misses(self):
        """First roll=0.99 misses advance, second roll=0.01 fires loss → PUT status=lost."""
        mc = self._mock_client()
        call_count = [0]

        def _alternating():
            call_count[0] += 1
            return 0.01 if call_count[0] == 2 else 0.99

        with patch("simulation.generators.deals.get_client", return_value=mc):
            with patch("time.sleep"):
                with patch("random.random", side_effect=_alternating):
                    result = self.gen._advance_deal(self.deal, dry_run=False)

        self.assertTrue(result.success)
        self.assertIn("lost", result.message)
        mc.put.assert_called_once_with(
            "https://api.pipedrive.com/v1/deals/100",
            json={"status": "lost", "stage_id": 13},  # Closed Lost
        )

    def test_advance_to_won_calls_complete_won_deal(self):
        """Deal in Negotiation (stage_id=11) + advance fires → _complete_won_deal called once."""
        now = datetime.utcnow()
        sct = (now - timedelta(days=5)).strftime("%Y-%m-%d %H:%M:%S")
        negotiation_deal = {
            "id": 200,
            "stage_id": 11,  # Negotiation
            "stage_change_time": sct,
            "update_time": sct,
            self.gen._client_type_field: "residential",
            self.gen._emv_field: None,
        }
        with patch.object(self.gen, "_complete_won_deal") as mock_won:
            with patch("random.random", return_value=0.01):
                result = self.gen._advance_deal(negotiation_deal, dry_run=False)

        mock_won.assert_called_once()
        self.assertTrue(result.success)
        self.assertIn("Won", result.message)
```

- [ ] **Step 2: Run to verify FAIL**

```bash
python tests/test_deals.py -v -k "AdvanceDeal"
```

Expected: `NotImplementedError: Task 4 — implement _advance_deal`

- [ ] **Step 3: Implement contract helpers in `deals.py`**

Replace `_pick_service_frequency`, `_build_contract`, and `_get_contract_value` stubs:

```python
def _pick_service_frequency(self, client_type: str, emv: float) -> str:
    """Pick service frequency from the pool matching client type and EMV."""
    if client_type == "commercial":
        return random.choices(
            list(COMMERCIAL_SERVICE_WEIGHTS.keys()),
            weights=list(COMMERCIAL_SERVICE_WEIGHTS.values()),
            k=1,
        )[0]
    if emv > 0:
        pool = ["weekly_recurring", "biweekly_recurring", "monthly_recurring"]
    else:
        pool = ["one_time_standard", "one_time_deep_clean", "one_time_move_in_out"]
    weights = [SERVICE_TYPE_WEIGHTS[k] for k in pool]
    return random.choices(pool, weights=weights, k=1)[0]

def _build_contract(self, deal: dict) -> dict:
    """Build the contract dict when a deal advances to Won."""
    client_type = deal.get(self._client_type_field) or "residential"
    emv_raw = deal.get(self._emv_field)
    try:
        emv = float(emv_raw) if emv_raw is not None else 0.0
    except (TypeError, ValueError):
        emv = 0.0

    service_frequency = self._pick_service_frequency(client_type, emv)
    contract_value    = self._get_contract_value(service_frequency)
    crew = random.choices(
        list(CREW_ASSIGNMENT_WEIGHTS.keys()),
        weights=list(CREW_ASSIGNMENT_WEIGHTS.values()),
        k=1,
    )[0]
    start_date = _add_business_days(date.today(), random.randint(5, 10))
    return {
        "contract_type":     client_type,
        "service_frequency": service_frequency,
        "contract_value":    contract_value,
        "start_date":        start_date,
        "crew_assignment":   crew,
    }

@staticmethod
def _get_contract_value(service_frequency: str) -> float:
    """Return contract value derived from service type base price."""
    service_id = _FREQ_TO_SERVICE_ID.get(service_frequency)
    if service_id:
        return _SERVICE_ID_PRICES.get(service_id, 150.00)
    lo, hi = _COMMERCIAL_RANGES.get(service_frequency, (500, 2000))
    return round(random.uniform(lo, hi), 2)
```

- [ ] **Step 4: Implement `_advance_deal` in `deals.py`**

Replace the `_advance_deal` stub:

```python
def _advance_deal(self, deal: dict, dry_run: bool = False) -> GeneratorResult:
    """Roll advance and loss dice; execute the winning action or return no-change."""
    deal_id = deal["id"]

    # ── Roll 1: advance ──────────────────────────────────────────────────────
    if random.random() < self.calculate_advance_probability(deal):
        current_stage_id = deal.get("stage_id")
        if current_stage_id not in self._stage_order:
            return GeneratorResult(
                success=False,
                message=f"deal {deal_id}: unknown stage {current_stage_id}",
            )
        idx = self._stage_order.index(current_stage_id)
        if idx >= len(self._stage_order) - 1:
            return GeneratorResult(success=True, message="no change")
        next_stage_id = self._stage_order[idx + 1]

        if next_stage_id == self._won_stage_id:
            contract = self._build_contract(deal)
            self._complete_won_deal(deal, contract, dry_run=dry_run)
            return GeneratorResult(success=True, message=f"deal {deal_id} advanced to Won")

        if not dry_run:
            time.sleep(0.15)
            client = get_client("pipedrive")
            resp = client.put(
                f"https://api.pipedrive.com/v1/deals/{deal_id}",
                json={"stage_id": next_stage_id},
            )
            if resp.status_code not in (200, 201):
                logger.warning("PUT deals/%s failed: %s", deal_id, resp.status_code)
                return GeneratorResult(success=False, message=f"PUT deals failed: {resp.status_code}")

            # Write stage_change_time to SQLite for SS-PROP deals only
            canonical_id = get_canonical_id("pipedrive", str(deal_id), self.db_path)
            if canonical_id and canonical_id.startswith("SS-PROP-"):
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute(
                        "UPDATE commercial_proposals SET stage_change_time = ? WHERE id = ?",
                        (datetime.utcnow().isoformat(), canonical_id),
                    )

            stage_name = self._stage_names.get(next_stage_id, str(next_stage_id))
            self._log_activity(deal_id, f"Deal advanced to {stage_name}.", dry_run=dry_run)
        else:
            logger.debug("[dry_run] Would advance deal %s to stage %s", deal_id, next_stage_id)

        return GeneratorResult(success=True, message=f"deal {deal_id} advanced to stage {next_stage_id}")

    # ── Roll 2: loss (only if advance did not fire) ──────────────────────────
    if random.random() < self.calculate_loss_probability(deal):
        if not dry_run:
            reason = random.choice(DAILY_VOLUMES["deal_progression"]["lost_reasons"])
            time.sleep(0.15)
            client = get_client("pipedrive")
            resp = client.put(
                f"https://api.pipedrive.com/v1/deals/{deal_id}",
                json={"status": "lost", "stage_id": self._lost_stage_id},
            )
            if resp.status_code not in (200, 201):
                logger.warning("PUT deals/%s (loss) failed: %s", deal_id, resp.status_code)
                return GeneratorResult(success=False, message=f"PUT deals failed: {resp.status_code}")
            self._log_activity(deal_id, f"Deal lost. Reason: {reason}", dry_run=dry_run)
        else:
            logger.debug("[dry_run] Would mark deal %s as lost", deal_id)
        return GeneratorResult(success=True, message=f"deal {deal_id} marked lost")

    return GeneratorResult(success=True, message="no change")
```

- [ ] **Step 5: Run — expect PASS for all 4 new tests + 17 existing**

```bash
python tests/test_deals.py -v
```

Expected:
```
Ran 21 tests in 0.XXXs
OK
```

- [ ] **Step 6: Commit**

```bash
git add simulation/generators/deals.py tests/test_deals.py
git commit -m "feat: implement _advance_deal with contract building helpers"
```

---

## Task 5: `_complete_won_deal` and service frequency branching

**Files:**
- Modify: `tests/test_deals.py` (append `TestCompleteWonDeal` + `TestServiceFrequencyBranching`)
- Modify: `simulation/generators/deals.py` (implement `_complete_won_deal`)

- [ ] **Step 1: Append failing tests**

```python
class TestCompleteWonDeal(unittest.TestCase):
    """Mock Pipedrive PUT+POST, real SQLite in a temp file."""

    def setUp(self):
        self._fd, self._db_path = tempfile.mkstemp(suffix=".db")
        from database.schema import init_db
        from database.mappings import register_mapping
        init_db(self._db_path)
        from simulation.generators.deals import DealGenerator
        self.gen = DealGenerator(db_path=self._db_path)

        # Insert SS-PROP-0001 in commercial_proposals (status column allows 'negotiating')
        conn = sqlite3.connect(self._db_path)
        conn.execute(
            "INSERT INTO commercial_proposals (id, title, status) "
            "VALUES ('SS-PROP-0001', 'Test Proposal', 'negotiating')"
        )
        conn.commit()
        conn.close()
        # Register pipedrive deal 200 → SS-PROP-0001
        register_mapping("SS-PROP-0001", "pipedrive", "200", db_path=self._db_path)

        # Insert SS-LEAD-0001 in leads and register deal 201
        conn = sqlite3.connect(self._db_path)
        conn.execute(
            "INSERT INTO leads (id, first_name, last_name, lead_type, status) "
            "VALUES ('SS-LEAD-0001', 'Jane', 'Doe', 'residential', 'qualified')"
        )
        conn.commit()
        conn.close()
        register_mapping("SS-LEAD-0001", "pipedrive", "201", db_path=self._db_path)

        self.contract = {
            "contract_type":     "commercial",
            "service_frequency": "nightly_clean",
            "contract_value":    2500.00,
            "start_date":        date(2026, 4, 10),
            "crew_assignment":   "Crew A",
        }

    def tearDown(self):
        os.close(self._fd)
        os.unlink(self._db_path)

    def _mc(self, put_status=200):
        mc = MagicMock()
        mc.put.return_value.status_code = put_status
        mc.post.return_value.status_code = 201
        return mc

    def _deal(self, deal_id, client_type="commercial"):
        return {
            "id": deal_id,
            "stage_id": 11,
            self.gen._client_type_field: client_type,
            self.gen._emv_field: 2500,
        }

    def test_ss_prop_updates_commercial_proposals(self):
        with patch("simulation.generators.deals.get_client", return_value=self._mc()):
            with patch("time.sleep"):
                self.gen._complete_won_deal(self._deal(200), self.contract, dry_run=False)

        conn = sqlite3.connect(self._db_path)
        row = conn.execute(
            "SELECT status, start_date, crew_assignment "
            "FROM commercial_proposals WHERE id = 'SS-PROP-0001'"
        ).fetchone()
        conn.close()
        self.assertEqual(row[0], "won")
        self.assertEqual(row[1], "2026-04-10")
        self.assertEqual(row[2], "Crew A")

    def test_ss_lead_does_not_write_sqlite(self):
        """Residential deal (SS-LEAD): activity POST fires but no SQLite update."""
        residential_contract = dict(self.contract, contract_type="residential")
        mc = self._mc()
        with patch("simulation.generators.deals.get_client", return_value=mc):
            with patch("time.sleep"):
                self.gen._complete_won_deal(
                    self._deal(201, client_type="residential"),
                    residential_contract,
                    dry_run=False,
                )
        # Activity note was sent
        mc.post.assert_called_once()
        # leads table has no 'status' column change (no update attempted)
        conn = sqlite3.connect(self._db_path)
        lead_row = conn.execute(
            "SELECT status FROM leads WHERE id = 'SS-LEAD-0001'"
        ).fetchone()
        conn.close()
        self.assertEqual(lead_row[0], "qualified")  # unchanged

    def test_none_canonical_id_logs_warning_and_skips_sqlite(self):
        """Deal with no cross_tool_mapping entry: logs warning, skips SQLite."""
        with patch("simulation.generators.deals.get_client", return_value=self._mc()):
            with patch("time.sleep"):
                with self.assertLogs("simulation.deals", level="WARNING") as cm:
                    self.gen._complete_won_deal(self._deal(999), self.contract, dry_run=False)
        self.assertTrue(
            any("no canonical ID mapping" in line for line in cm.output),
            msg=f"Expected warning not found in: {cm.output}",
        )

    def test_dry_run_skips_put_post_and_sqlite(self):
        mc = MagicMock()
        with patch("simulation.generators.deals.get_client", return_value=mc):
            self.gen._complete_won_deal(self._deal(200), self.contract, dry_run=True)

        mc.put.assert_not_called()
        mc.post.assert_not_called()

        conn = sqlite3.connect(self._db_path)
        row = conn.execute(
            "SELECT status FROM commercial_proposals WHERE id = 'SS-PROP-0001'"
        ).fetchone()
        conn.close()
        self.assertNotEqual(row[0], "won")


class TestServiceFrequencyBranching(unittest.TestCase):
    """3 cases × 20 iterations — assert no crossover between pools."""

    def setUp(self):
        self._fd, self._db_path = tempfile.mkstemp(suffix=".db")
        from database.schema import init_db
        init_db(self._db_path)
        from simulation.generators.deals import DealGenerator
        self.gen = DealGenerator(db_path=self._db_path)

    def tearDown(self):
        os.close(self._fd)
        os.unlink(self._db_path)

    def test_residential_recurring_pool_when_emv_positive(self):
        """Client Type=residential, EMV > 0 → only weekly/biweekly/monthly."""
        pool = {"weekly_recurring", "biweekly_recurring", "monthly_recurring"}
        random.seed(0)
        for i in range(20):
            result = self.gen._pick_service_frequency("residential", 200.0)
            self.assertIn(result, pool,
                f"Iteration {i}: got '{result}' for residential+EMV>0, expected pool={pool}")

    def test_residential_one_time_pool_when_emv_zero(self):
        """Client Type=residential, EMV = 0 → only one_time_* services."""
        pool = {"one_time_standard", "one_time_deep_clean", "one_time_move_in_out"}
        random.seed(0)
        for i in range(20):
            result = self.gen._pick_service_frequency("residential", 0.0)
            self.assertIn(result, pool,
                f"Iteration {i}: got '{result}' for residential+EMV=0, expected pool={pool}")

    def test_commercial_pool(self):
        """Client Type=commercial → only nightly_clean/weekend_deep_clean/one_time_project."""
        pool = {"nightly_clean", "weekend_deep_clean", "one_time_project"}
        random.seed(0)
        for i in range(20):
            result = self.gen._pick_service_frequency("commercial", 0.0)
            self.assertIn(result, pool,
                f"Iteration {i}: got '{result}' for commercial, expected pool={pool}")
```

- [ ] **Step 2: Run to verify FAIL**

```bash
python tests/test_deals.py -v -k "CompleteWonDeal or ServiceFrequency"
```

Expected: `NotImplementedError: Task 5 — implement _complete_won_deal`

- [ ] **Step 3: Implement `_complete_won_deal` in `deals.py`**

Replace the `_complete_won_deal` stub:

```python
def _complete_won_deal(self, deal: dict, contract: dict, dry_run: bool = False) -> None:
    """Write won-deal contract details to Pipedrive and SQLite."""
    deal_id = deal["id"]

    if not dry_run:
        time.sleep(0.15)
        client = get_client("pipedrive")
        resp = client.put(
            f"https://api.pipedrive.com/v1/deals/{deal_id}",
            json={
                "stage_id": self._won_stage_id,
                "status": "won",
                self._client_type_field:  contract["contract_type"],
                self._service_type_field: contract["service_frequency"],
                self._emv_field:          contract["contract_value"],
            },
        )
        if resp.status_code not in (200, 201):
            logger.warning("PUT deals/%s (won) failed: %s", deal_id, resp.status_code)
            return
    else:
        logger.debug("[dry_run] Would mark deal %s as won", deal_id)

    # SQLite: commercial deals (SS-PROP) only
    canonical_id = get_canonical_id("pipedrive", str(deal_id), self.db_path)
    if canonical_id is None:
        logger.warning(
            "Won deal %s has no canonical ID mapping — skipping SQLite update", deal_id
        )
    elif canonical_id.startswith("SS-PROP-"):
        if not dry_run:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "UPDATE commercial_proposals "
                    "SET status='won', start_date=?, crew_assignment=? "
                    "WHERE id=?",
                    (
                        contract["start_date"].isoformat(),
                        contract["crew_assignment"],
                        canonical_id,
                    ),
                )
    # SS-LEAD: no SQLite write

    start = contract["start_date"].isoformat()
    crew  = contract["crew_assignment"]
    svc   = contract["service_frequency"]
    val   = contract["contract_value"]
    note = (
        f"Deal won. Contract details:\n"
        f"Start date: {start}\n"
        f"Crew: {crew}\n"
        f"Service: {svc}, ${val:.2f}/visit"
    )
    self._log_activity(deal_id, note, dry_run=dry_run)
```

- [ ] **Step 4: Run — expect PASS for all 7 new tests + 21 existing**

```bash
python tests/test_deals.py -v
```

Expected:
```
Ran 28 tests in 0.XXXs
OK
```

- [ ] **Step 5: Commit**

```bash
git add simulation/generators/deals.py tests/test_deals.py
git commit -m "feat: implement _complete_won_deal and service frequency branching"
```

---

## Task 6: `execute()` integration and final test run

**Files:**
- Modify: `tests/test_deals.py` (append `TestExecute`)
- Modify: `simulation/generators/deals.py` (implement `execute`)

- [ ] **Step 1: Append failing tests**

```python
class TestExecute(unittest.TestCase):

    def setUp(self):
        self._fd, self._db_path = tempfile.mkstemp(suffix=".db")
        from database.schema import init_db
        init_db(self._db_path)
        from simulation.generators.deals import DealGenerator
        self.gen = DealGenerator(db_path=self._db_path)

    def tearDown(self):
        os.close(self._fd)
        os.unlink(self._db_path)

    def _mock_pipedrive(self, deals):
        mc = MagicMock()
        mc.get.return_value.raise_for_status = MagicMock()
        mc.get.return_value.json.return_value = {"data": deals}
        return mc

    def test_returns_failure_when_no_open_deals(self):
        from simulation.generators.deals import GeneratorResult
        with patch("simulation.generators.deals.get_client", return_value=self._mock_pipedrive(None)):
            with patch("time.sleep"):
                result = self.gen.execute(dry_run=False)
        self.assertIsInstance(result, GeneratorResult)
        self.assertFalse(result.success)
        self.assertIn("no open deals", result.message)

    def test_dry_run_reads_deals_but_skips_put_and_post(self):
        """dry_run=True with open deals → GET called, no PUT/POST regardless of roll."""
        now = datetime.utcnow()
        sct = (now - timedelta(days=5)).strftime("%Y-%m-%d %H:%M:%S")
        deals = [{"id": 1, "stage_id": 8, "stage_change_time": sct, "update_time": sct}]
        mc = self._mock_pipedrive(deals)
        with patch("simulation.generators.deals.get_client", return_value=mc):
            with patch("time.sleep"):
                with patch("random.random", return_value=0.01):  # force advance roll
                    result = self.gen.execute(dry_run=True)
        self.assertTrue(result.success)
        mc.get.assert_called_once()
        mc.put.assert_not_called()
        mc.post.assert_not_called()

    def test_returns_failure_on_pipedrive_fetch_error(self):
        mc = MagicMock()
        mc.get.return_value.raise_for_status.side_effect = Exception("Connection refused")
        with patch("simulation.generators.deals.get_client", return_value=mc):
            with patch("time.sleep"):
                result = self.gen.execute(dry_run=False)
        self.assertFalse(result.success)
        self.assertIn("pipedrive fetch failed", result.message)
```

- [ ] **Step 2: Run to verify FAIL**

```bash
python tests/test_deals.py -v -k "Execute"
```

Expected: `NotImplementedError: Task 6 — implement execute`

- [ ] **Step 3: Implement `execute` in `deals.py`**

Replace the `execute` stub:

```python
def execute(self, dry_run: bool = False) -> GeneratorResult:
    """Pick one open deal and advance, lose, or leave it unchanged."""
    try:
        deal = self._pick_deal()
    except Exception as e:
        return GeneratorResult(success=False, message=f"pipedrive fetch failed: {e}")

    if deal is None:
        return GeneratorResult(success=False, message="no open deals")

    return self._advance_deal(deal, dry_run=dry_run)
```

- [ ] **Step 4: Run full test suite — expect PASS for all 31 tests**

```bash
python tests/test_deals.py -v
```

Expected:
```
Ran 31 tests in 0.XXXs
OK
```

- [ ] **Step 5: Run the engine smoke test to verify DealGenerator registers cleanly**

```bash
python -m simulation.engine --dry-run --once --date 2026-03-28 --speed 999999 2>&1 | tail -5
```

Expected: no `DealGenerator not found` warning; `Daily summary` line appears in output.

- [ ] **Step 6: Commit**

```bash
git add simulation/generators/deals.py tests/test_deals.py
git commit -m "feat: implement execute() and complete DealGenerator — 31 tests passing"
```

---

## Self-Review Checklist

**Spec coverage:**
- [x] `calculate_advance_probability` — 4 unit tests (Task 1)
- [x] `calculate_loss_probability` — 4 unit tests (Task 1)
- [x] `_ensure_schema` adds 3 columns, idempotent (Task 2)
- [x] `__init__` loads stage IDs from `tool_ids.json` (Task 2)
- [x] `_pick_deal` returns None on empty, uniform selection (Task 3)
- [x] `_log_activity` POST payload correct, dry_run skips (Task 3)
- [x] `_advance_deal`: advance fires (PUT next stage), no change, loss fires (PUT lost), advance-to-won calls `_complete_won_deal` (Task 4)
- [x] `_complete_won_deal`: SS-PROP SQLite update, SS-LEAD no SQLite, None canonical_id warning, dry_run skips all (Task 5)
- [x] Service frequency branching: 3 pools × 20 iterations, no crossover (Task 5)
- [x] `execute`: no open deals returns failure, dry_run reads only, fetch error caught (Task 6)
- [x] Engine smoke test verifies registration (Task 6)

**Type consistency:**
- `GeneratorResult` is defined locally in `deals.py` with `success: bool, message: str = ""` — matches all usages in `execute`, `_advance_deal`, and test assertions.
- `_pick_service_frequency(client_type: str, emv: float)` — called from `_build_contract` with `float(emv_raw)`, tested directly in `TestServiceFrequencyBranching`.
- `_stage_order` is a `list[int]` — `index()` in `_advance_deal` is compatible.
- `contract["start_date"]` is a `date` object — `.isoformat()` called correctly in `_complete_won_deal`.
