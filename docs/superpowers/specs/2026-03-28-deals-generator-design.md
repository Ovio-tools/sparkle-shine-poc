# DealGenerator Design Spec
**Date:** 2026-03-28
**Status:** Approved — ready for implementation planning

---

## What We're Building

`simulation/generators/deals.py` — a generator that advances existing Pipedrive deals through the sales pipeline. Each `execute()` call picks one open deal, rolls probability dice weighted by how long the deal has been in its current stage, and either advances it to the next stage, marks it lost, or leaves it unchanged. When a deal reaches "Closed Won," the generator fills in contract details (custom fields in Pipedrive, SQLite row for commercial deals) so the downstream Pipedrive-to-Asana automation has complete data.

This is a Type 2 generator (progresses existing records), unlike the Type 1 `ContactGenerator` (creates from scratch). Dry-run skips all writes (API and SQLite). Reads are allowed because Type 2 generators need existing state to produce meaningful test output.

---

## File

`simulation/generators/deals.py` — registered as `DealGenerator` in `simulation/engine.py`.

---

## Class Shape

```python
class DealGenerator:
    def __init__(self, db_path="sparkle_shine.db"): ...
    def execute(self, dry_run=False) -> GeneratorResult: ...
    def _pick_deal(self) -> dict | None: ...
    def calculate_advance_probability(self, deal: dict) -> float: ...
    def calculate_loss_probability(self, deal: dict) -> float: ...
    def _advance_deal(self, deal: dict, dry_run=False) -> GeneratorResult: ...
    def _complete_won_deal(self, deal: dict, contract: dict, dry_run=False) -> None: ...
    def _log_activity(self, deal_id: int, note: str, dry_run=False) -> None: ...
    def _ensure_schema(self, conn) -> None: ...
```

---

## Initialization

```python
def __init__(self, db_path="sparkle_shine.db"):
    self.db_path = db_path
    tool_ids = json.loads(Path("config/tool_ids.json").read_text())
    stages = tool_ids["pipedrive"]["stages"]
    self._stage_order = [
        stages["New Lead"], stages["Qualified"], stages["Site Visit Scheduled"],
        stages["Proposal Sent"], stages["Negotiation"], stages["Closed Won"],
    ]
    self._won_stage_id  = stages["Closed Won"]
    self._lost_stage_id = stages["Closed Lost"]
    # Run schema migration once on startup
    with sqlite3.connect(self.db_path) as conn:
        self._ensure_schema(conn)
```

Stage IDs are loaded from `config/tool_ids.json` at startup rather than hardcoded. If the Pipedrive pipeline is recreated or the sandbox changes, only `tool_ids.json` needs updating.

---

## `_pick_deal()`

`GET /v1/deals?status=open&sort=update_time%20DESC&limit=100`

Age = days in current stage, resolved via three-level fallback per deal:

1. `deal["stage_change_time"]` — Pipedrive native field, use if non-null
2. `commercial_proposals.stage_change_time` for matching canonical ID — populated by this generator on prior advances
3. `deal["update_time"]` — last-resort proxy

**Selection weighting:** uniform — every open deal has equal pick probability. The advance and loss probability functions already encode age-dependent behavior; applying the same age weights here would double-count them (a deal at 10 days would get 2× pick chance AND 2× advance probability = effectively 4× the daily advance rate of a fresh deal).

`random.choices(deals, k=1)[0]` — one uniformly random pick per `execute()` call. Returns `None` if the API returns no open deals.

---

## `calculate_advance_probability(deal)`

Pure function. Takes a deal dict, returns a float. No API calls.

```python
base = DAILY_VOLUMES["deal_progression"]["stage_advance_probability"]  # 0.15
age_days = days_in_stage(deal)  # from stage_change_time resolution above
weight = age_bracket_weight(age_days)  # 1.0 / 1.5 / 2.0 / 0.5
return base * weight
```

Expected values:
- `days_in_stage=2`  → `0.15 × 1.0 = 0.150`
- `days_in_stage=5`  → `0.15 × 1.5 = 0.225`
- `days_in_stage=10` → `0.15 × 2.0 = 0.300`
- `days_in_stage=20` → `0.15 × 0.5 = 0.075`

---

## `calculate_loss_probability(deal)`

Pure function. Same structure as `calculate_advance_probability`, but with inverted age weights — stale deals bleed out faster.

| Days in stage | Loss weight |
|---|---|
| 0–3  | 0.5x (grace period) |
| 4–7  | 1.0x |
| 8–14 | 1.5x |
| 15+  | 2.5x (stale deals bleed out) |

```python
base = DAILY_VOLUMES["deal_progression"]["lost_probability_per_stage"]  # 0.03
return base * loss_age_bracket_weight(age_days)
```

Expected values:
- `days_in_stage=2`  → `0.03 × 0.5 = 0.015`
- `days_in_stage=5`  → `0.03 × 1.0 = 0.030`
- `days_in_stage=10` → `0.03 × 1.5 = 0.045`
- `days_in_stage=20` → `0.03 × 2.5 = 0.075`

---

## `_advance_deal(deal, dry_run=False)`

Two independent rolls:

1. **Advance roll**: `random.random() < calculate_advance_probability(deal)`
   - Determine next stage from `_stage_order`
   - If next stage is `_won_stage_id` → build `contract` dict and call `_complete_won_deal(deal, contract, dry_run)`
   - Otherwise → `PUT /v1/deals/{id}` with new `stage_id`, write `stage_change_time` to SQLite (SS-PROP deals only), log activity note

2. **Loss roll** (only if no advance): `random.random() < calculate_loss_probability(deal)`
   - `PUT /v1/deals/{id}` with `status=lost`, `stage_id=_lost_stage_id`
   - Log activity note with a random reason from `DAILY_VOLUMES["deal_progression"]["lost_reasons"]`

3. **No change**: neither roll fires → return `GeneratorResult(success=True, message="no change")`, no API calls, no log.

Activity note logged on advance and loss; skipped on no-change.

### Contract dict (built when advancing to Won)

```python
contract = {
    "contract_type":      <from deal's "Client Type" custom field>,
    "service_frequency":  <picked from correct weight pool — see below>,
    "contract_value":     <derived from service type base price>,
    "start_date":         <5–10 business days from today>,
    "crew_assignment":    <random.choices from CREW_ASSIGNMENT_WEIGHTS>,
}
```

**Service frequency branching** by client type:

| Condition | Pool |
|---|---|
| Client Type = residential AND Estimated Monthly Value > 0 | `weekly_recurring`, `biweekly_recurring`, `monthly_recurring` |
| Client Type = residential AND EMV is null/zero | `one_time_standard`, `one_time_deep_clean`, `one_time_move_in_out` |
| Client Type = commercial | `nightly_clean`, `weekend_deep_clean`, `one_time_project` |

"Estimated Monthly Value > 0" means the deal's `Estimated Monthly Value` custom field (key from `tool_ids.json["pipedrive"]["deal_fields"]`) is non-null and greater than zero.

---

## `_complete_won_deal(deal, contract, dry_run=False)`

**Pipedrive write** — `PUT /v1/deals/{deal_id}`:
```json
{
  "stage_id": <won_stage_id>,
  "status": "won",
  "<Client Type field key>":             "<residential|commercial>",
  "<Service Type field key>":            "<service_frequency>",
  "<Estimated Monthly Value field key>": <contract_value>
}
```

**SQLite write** — resolve canonical ID first:
```python
canonical_id = get_canonical_id("pipedrive", str(deal["id"]), self.db_path)
```

- `canonical_id is None` → log warning `"Won deal {deal_id} has no canonical ID mapping — skipping SQLite update"` and skip SQLite write. Pipedrive custom fields and activity note are already written; the reconciler (Step 7) will catch the missing mapping later.
- `SS-PROP-NNNN` → commercial deal → update `commercial_proposals WHERE id = canonical_id`
  - Set `status='won'`, `start_date`, `crew_assignment`
- `SS-LEAD-NNNN` → residential deal → no SQLite write; activity note only

**Activity note** (logged via `_log_activity`):
```
Deal won. Contract details:
Start date: 2026-04-08
Crew: Crew A (Westlake/Tarrytown)
Service: Biweekly standard clean, $150/visit
```

---

## `_log_activity(deal_id, note, dry_run=False)`

`POST /v1/activities`:
```json
{
  "deal_id": <deal_id>,
  "subject": "Stage update",
  "type": "note",
  "note": <note>,
  "done": 1
}
```

0.15s throttle before every Pipedrive API call (GET, PUT, POST).

Skipped when `dry_run=True` — logs the note text at DEBUG level instead.

---

## `_ensure_schema(conn)`

Runs once in `__init__`. Checks existing columns via `PRAGMA table_info(commercial_proposals)` before each `ALTER TABLE` — compatible with SQLite versions older than 3.35.

Adds to `commercial_proposals` if missing:
- `start_date TEXT`
- `crew_assignment TEXT`
- `stage_change_time TEXT`

---

## SQLite Writes Summary

| When | Table | Columns written |
|---|---|---|
| Successful stage advance (non-won) | `commercial_proposals` | `stage_change_time` (SS-PROP only) |
| Won + commercial (SS-PROP) | `commercial_proposals` | `status='won'`, `start_date`, `crew_assignment` |
| Won + residential (SS-LEAD) | — | No SQLite write; activity note only |

All SQLite writes skipped when `dry_run=True`.

---

## Error Handling

Each phase fails independently:

| Phase | On failure |
|---|---|
| `_pick_deal()` Pipedrive GET fails | Return `GeneratorResult(success=False, message="pipedrive fetch failed: {e}")` |
| No open deals | Return `GeneratorResult(success=False, message="no open deals")` |
| `PUT /v1/deals` fails | Log warning, return `GeneratorResult(success=False)` — do NOT update SQLite or log activity |
| `commercial_proposals` UPDATE fails | Log warning, do not crash — Pipedrive already updated, SQLite is best-effort |
| `POST /v1/activities` fails | Log warning, do not crash — activity note is informational only |

Unexpected exceptions propagate naturally to the engine's `dispatch()` error handler.

---

## Auth and Imports

```python
from auth import get_client
from database.mappings import get_canonical_id
from simulation.config import DAILY_VOLUMES, CREW_ASSIGNMENT_WEIGHTS, SERVICE_TYPE_WEIGHTS, COMMERCIAL_SERVICE_WEIGHTS
from config.business import SERVICE_TYPES  # base prices for contract_value calculation
```

Auth: `client = get_client("pipedrive")` — never import `credentials.py` directly.

Never write to `poll_state`. Never create Asana tasks.

---

## Testing Surface

### Pure unit tests (no mocks)

**`calculate_advance_probability(deal)`** — 4 cases:
```
days_in_stage=2  → 0.150
days_in_stage=5  → 0.225
days_in_stage=10 → 0.300
days_in_stage=20 → 0.075
```

**`calculate_loss_probability(deal)`** — 4 cases:
```
days_in_stage=2  → 0.015
days_in_stage=5  → 0.030
days_in_stage=10 → 0.045
days_in_stage=20 → 0.075
```

### `_pick_deal()` — mock Pipedrive GET

- Returns `None` when API returns empty list → `execute()` returns `GeneratorResult(success=False)`
- Weighted selection: seed `random`, assert weighted-older deals are picked more often over N trials

### `_advance_deal()` — seed random, mock Pipedrive PUT

- `random.random()` → 0.01 → advance fires → PUT called with correct next `stage_id`
- `random.random()` → 0.99 → no advance, no loss → PUT not called
- `random.random()` → triggers loss roll → PUT called with `status=lost`
- Deal in Negotiation + advance fires → `_complete_won_deal()` called once

### `_complete_won_deal()` — mock Pipedrive PUT + POST, real SQLite in-memory

- `SS-PROP` canonical ID → `commercial_proposals` updated with `status=won`, `start_date`, `crew_assignment`
- `SS-LEAD` canonical ID → no SQLite write, activity POST still fires
- `dry_run=True` → PUT and POST not called, SQLite not written

### Service frequency branching — 3 cases × 20 iterations, seeded random

| Case | Input | Expected pool |
|---|---|---|
| Residential recurring | Client Type=residential, EMV > 0 | `weekly_recurring`, `biweekly_recurring`, `monthly_recurring` |
| Residential one-time | Client Type=residential, EMV = null/0 | `one_time_standard`, `one_time_deep_clean`, `one_time_move_in_out` |
| Commercial | Client Type=commercial | `nightly_clean`, `weekend_deep_clean`, `one_time_project` |

Assert no crossover across all 20 draws per case.

### `execute()` — integration-level

- `dry_run=True` + open deals exist → GET called, no PUT/POST, returns `GeneratorResult(success=True)`
- `_ensure_schema()` adds missing columns without crashing on a fresh in-memory DB

---

## Constraints

- Never write to `poll_state` — automation runner owns those watermarks
- Never create Asana tasks — runner handles downstream creation when it detects won deals
- Never create Pipedrive persons or deals — runner creates those from HubSpot SQLs
- Auth exclusively via `auth.get_client("pipedrive")`
- `--dry-run` skips all API writes, SQLite writes, and Slack posts
