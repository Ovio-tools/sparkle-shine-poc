# Skill: Project Conventions

**When to read:** At the start of every Claude Code session working on this project. Especially before the first line of code.

This doc consolidates the rules, patterns, and conventions that apply across the entire Sparkle & Shine codebase. If you follow these, your code will fit in without friction.

---

## Simulation Data Integrity Rule (L5)

In the simulation engine, there is no regeneration step. Data is created once, live, in real time. If a generator produces incorrect data:

- **Fix the generator code.** Let future events produce correct data going forward.
- **Do NOT write one-off SQL patches** to fix simulation-generated records. This creates invisible divergence between what the code produces and what the database contains.
- **The only acceptable SQL patches** are for historical data created by the Phase 2 seeding scripts, which cannot be regenerated without resetting all 8 tools.

If a batch of bad simulation data needs correction (e.g., 50 invoices with wrong amounts), the fix is: (1) fix the generator, (2) write a migration script that updates the affected records in SQLite AND the corresponding tool records via API, (3) commit the migration script so the fix is documented and reproducible.

---

## Automation Boundary Rules (L21)

The simulation engine and the automation runner (`automations/runner.py`) share the same SaaS tools and SQLite database. These rules prevent them from creating duplicate records or desynchronizing state.

**Rule 1: Never write to the `poll_state` table.**
The automation runner uses `poll_state` timestamps to track what it has already processed. If the simulation writes to `poll_state`, the runner's watermarks desync -- it will either skip events or reprocess old ones. The simulation injects events at the source tool's API and lets the runner discover them through normal polling.

**Rule 2: Never create records the automation also creates.**
The automation runner handles these creations:
- Pipedrive deals (from HubSpot SQLs -- detected via `cross_tool_mapping` absence)
- Asana onboarding tasks (from Pipedrive won deals -- detected via `poll_state`)
- QuickBooks invoices (from Jobber completed jobs -- detected via `poll_state`)

The simulation must NOT create any of the above. It creates the upstream trigger (HubSpot contact, Pipedrive won status, Jobber completed status) and lets the runner handle the downstream creation.

**Rule 3: Register mappings only for the tool you wrote to.**
When the simulation creates a HubSpot SQL contact, it registers `link(canonical_id, "hubspot", hubspot_id)` only. It does NOT register a Pipedrive mapping. The absence of the Pipedrive mapping is how the runner detects new SQLs. If a Pipedrive mapping is registered prematurely, the runner skips the contact forever.

---

## Import Paths

**CRITICAL:** Confirm these against `SIMULATION_AUDIT.md` before writing any imports. The paths below reflect what CLAUDE.md documents, but the actual repo may differ.

```python
# Database access
from database.schema import ...          # NOT from db.schema
from database.mappings import generate_id, link, lookup, reverse_lookup, find_unmapped

# Auth (CONFIRMED: use get_client exclusively)
from auth import get_client
# get_client("hubspot") returns a configured requests.Session
# get_client("jobber") returns a session with OAuth auto-refresh
# NEVER do: from credentials import get_credential  (internal to auth layer)

# Config
from config.business import COMPANY, EMPLOYEES, CREWS, SERVICES, NEIGHBORHOODS
from config.narrative import TIMELINE
import json
with open("config/tool_ids.json") as f:
    TOOL_IDS = json.load(f)

# Rate limiting
from seeding.utils.throttler import HUBSPOT, PIPEDRIVE, JOBBER, QUICKBOOKS, ASANA, MAILCHIMP

# Logging
from intelligence.logging_config import setup_logging
logger = setup_logging(__name__)

# Simulation
from simulation.config import DAILY_VOLUMES, SEASONAL_WEIGHTS, ...
from simulation.variation import get_daily_multiplier, get_adjusted_volume, should_event_happen
from simulation.error_reporter import report_error
```

---

## Naming Conventions

### Files

| Type | Pattern | Example |
|------|---------|---------|
| Generator | `simulation/generators/{noun}.py` | `contacts.py`, `churn.py` |
| Syncer | `intelligence/syncers/sync_{tool}.py` | `sync_jobber.py` |
| Pusher | `seeding/pushers/push_{tool}.py` | `push_hubspot.py` |
| Test | `tests/test_{scope}.py` | `test_simulation.py` |
| Config | `{module}/config.py` | `intelligence/config.py` |

### Classes

| Type | Pattern | Example |
|------|---------|---------|
| Generator | `{Noun}Generator` | `ContactGenerator`, `ChurnGenerator` |
| Syncer | `{Tool}Syncer` | `JobberSyncer` |
| Error | `{Noun}Error` | `TokenExpiredError`, `ToolUnavailableError` |

### Variables

| Type | Pattern | Example |
|------|---------|---------|
| Canonical ID | `canonical_id` | `"SS-CLIENT-0047"` |
| Tool-specific ID | `{tool}_id` | `hubspot_id`, `pipedrive_id`, `jobber_id` |
| Database connection | `db` | `db = sqlite3.connect(self.db_path)` |
| API session | `session` | `session = requests.Session()` |

---

## The --dry-run Convention

Every runner, pusher, generator, and automation supports a `--dry-run` flag. When enabled:

- Log what WOULD happen (at INFO level)
- Do NOT make any API calls
- Do NOT write to SQLite
- Do NOT post to Slack
- Return results as if the operation succeeded (for testing downstream logic)

Pattern:

```python
def execute(self, dry_run: bool = False):
    data = self.prepare_data()

    if dry_run:
        logger.info(f"[DRY RUN] Would create {data['type']} for {data['name']}")
        return {"id": "DRY-RUN-ID", "status": "dry_run"}

    result = self.call_api(data)
    return result
```

CLI flag:

```python
parser.add_argument("--dry-run", action="store_true",
                    help="Log actions without making API calls")
```

---

## Error Handling

### Rule 1: One Tool Failing Never Cascades

```python
# BAD: if HubSpot fails, Pipedrive, Jobber, QBO all get skipped
for tool in [hubspot, pipedrive, jobber, quickbooks]:
    tool.sync()  # one exception kills the loop

# GOOD: each tool is independent
for tool in [hubspot, pipedrive, jobber, quickbooks]:
    try:
        tool.sync()
    except Exception as e:
        logger.error(f"{tool.name} failed: {e}")
        errors.append(tool.name)
        continue
```

### Rule 2: Log to File, Report to Slack

Technical details (stack traces, HTTP response bodies) go to the log file. Human-readable summaries go to Slack `#automation-failure`.

```python
try:
    result = call_api(...)
except Exception as e:
    # Full details to log file
    logger.exception(f"QuickBooks invoice creation failed for {canonical_id}")

    # Plain language to Slack
    from simulation.error_reporter import report_error
    report_error(e, tool_name="QuickBooks",
                 context=f"creating invoice for {client_name}")
```

### Rule 3: Retry Transient Errors, Fail Fast on Auth Errors

- 429 (rate limited): wait and retry (respect Retry-After header)
- 500-504 (server error): retry with exponential backoff
- ConnectionError, Timeout: retry once
- 401 (unauthorized): do NOT retry. Report as critical. Token needs refresh.
- 400, 403, 404: do NOT retry. These are logic errors.

---

## SQLite Patterns

### Always Use Parameterized Queries

```python
# BAD: SQL injection risk, breaks on apostrophes in names
db.execute(f"INSERT INTO clients (name) VALUES ('{name}')")

# GOOD
db.execute("INSERT INTO clients (name) VALUES (?)", (name,))
```

### Always Commit After Writes

```python
db.execute("INSERT INTO ...", (...))
db.commit()  # don't forget this
```

### Wrap Multi-Step Operations in Try/Rollback

```python
try:
    db.execute("INSERT INTO clients ...", (...))
    db.execute("INSERT INTO cross_tool_mapping ...", (...))
    db.commit()
except Exception:
    db.rollback()
    raise
```

### Close Connections

```python
db = sqlite3.connect(self.db_path)
try:
    # do work
finally:
    db.close()
```

### Date/Time Storage

Store all timestamps as ISO 8601 strings in UTC:

```python
from datetime import datetime
now = datetime.utcnow().isoformat()  # "2026-03-27T14:30:00.000000"
```

Store dates as `YYYY-MM-DD`:

```python
from datetime import date
today = date.today().isoformat()  # "2026-03-27"
```

### Avoid SQLite-Specific Functions (Railway Prep)

These will break when migrating to PostgreSQL:

```python
# AVOID (SQLite-specific)
db.execute("SELECT datetime('now')")
db.execute("INSERT OR REPLACE INTO ...")

# USE INSTEAD
from datetime import datetime
now = datetime.utcnow().isoformat()
db.execute("INSERT INTO ... VALUES (...)", (...))
# Handle upserts with: SELECT first, then INSERT or UPDATE
```

---

## Testing Patterns

### File Location

```
tests/test_phase1.py     # Phase 1 integration checks
tests/test_phase2.py     # Phase 2 volume + mapping + pattern checks
tests/test_phase4.py     # Phase 4 intelligence layer tests
tests/test_simulation.py # Simulation engine tests (new)
```

### Integration Test Gate

Tests that call live APIs are gated behind an environment variable:

```python
@unittest.skipUnless(os.getenv("RUN_INTEGRATION"), "Skipping integration tests")
def test_hubspot_contact_creation_live(self):
    ...
```

Run unit tests only (fast, no API calls):
```bash
python tests/test_simulation.py -v -k "not live and not integration"
```

Run full suite including integration:
```bash
RUN_INTEGRATION=1 python tests/test_simulation.py -v
```

### What Tests Verify

1. **Correctness:** Does the code produce the right output?
2. **Narrative consistency:** Do the numbers match the business story? (Less important for simulation tests since we're generating forward, but still relevant for the intelligence layer.)
3. **Cross-tool consistency:** Does the SQLite record match the tool record?
4. **Error handling:** Does the code handle failures gracefully?

### Test Data Cleanup

Integration tests that create real records in SaaS tools should clean up after themselves when possible:

```python
def test_create_and_delete_contact(self):
    # Create
    result = gen.execute_one()
    hubspot_id = lookup(result.canonical_id, "hubspot")

    # Verify
    assert hubspot_id is not None

    # Cleanup
    session.delete(f"{BASE_URL}/crm/v3/objects/contacts/{hubspot_id}")
```

Not all tools support easy deletion (Jobber, QuickBooks). For those, use distinctive test names (prefix "TEST_" or "ZZZ_TEST_") so test records are identifiable.

---

## Config and Magic Numbers

All thresholds, targets, probabilities, and volume ranges live in config files. Never hardcode them.

| Config File | Contains |
|------------|----------|
| `config/business.py` | Company profile, employees, crews, services, neighborhoods, rate card |
| `config/narrative.py` | 12-month timeline as a data structure |
| `config/tool_ids.json` | Pipeline stage IDs, custom field keys, project GIDs, audience IDs |
| `intelligence/config.py` | Revenue targets, alert thresholds, crew capacity, model config, system prompts |
| `simulation/config.py` | Daily volumes, seasonal weights, day-of-week weights, churn rates, payment distributions |

When adding a new config value, put it in the most specific file. Simulation-related values go in `simulation/config.py`. Intelligence-related values go in `intelligence/config.py`. Shared business facts go in `config/business.py`.

---

## Checkpoint Pattern

Long-running operations that can be interrupted (pushers, the simulation engine) save checkpoints so they can resume:

```python
import json
from pathlib import Path

CHECKPOINT_FILE = Path("simulation/checkpoint.json")

def save_checkpoint(state: dict):
    CHECKPOINT_FILE.write_text(json.dumps(state, indent=2))

def load_checkpoint() -> dict | None:
    if CHECKPOINT_FILE.exists():
        return json.loads(CHECKPOINT_FILE.read_text())
    return None

def clear_checkpoint():
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()
```

The simulation engine checkpoints after every 10 events and on shutdown. Format:

```json
{
    "date": "2026-03-27",
    "counters": {"contacts": 4, "deals": 1, "operations": 12, ...},
    "last_event_time": "2026-03-27T14:23:00"
}
```

If the engine restarts mid-day, it loads the checkpoint, restores counters, and picks up where it left off without double-generating events.

---

## Traceability

Every record created by the simulation should be traceable back to its canonical ID:

- **HubSpot:** custom property or contact note containing `SS-ID: SS-CLIENT-0312`
- **Pipedrive:** deal or person note containing the SS-ID
- **Jobber:** client note containing the SS-ID
- **QuickBooks:** invoice PrivateNote or customer Notes containing the SS-ID
- **Asana:** task description containing the SS-ID
- **Mailchimp:** tag or merge field with the SS-ID

This convention is already established across the seeded data. The simulation engine must continue it.

---

## Process Management

### Running Locally

```bash
# Terminal 1: Simulation engine (continuous)
python -m simulation.engine

# Terminal 2: Automation runner (already on cron, or run manually)
python -m automations.runner --poll

# Terminal 3: Intelligence runner (manual or cron at 6 AM)
python -m intelligence.runner --skip-sync --date 2026-03-27
```

### Graceful Shutdown

The simulation engine handles SIGTERM and SIGINT:

```python
signal.signal(signal.SIGTERM, self.handle_shutdown)
signal.signal(signal.SIGINT, self.handle_shutdown)
```

On shutdown: save checkpoint, log daily summary, exit cleanly. Never kill with `kill -9` unless the process is truly hung.

---

## Git Commit Convention

Commit after each completed step. Use descriptive messages:

```
Add simulation engine framework (Step 1)
Add contact generator with Austin name/address data (Step 2)
Add deal progression and won-deal completion (Step 3)
Wire error reporter to Slack #automation-failure (Step 8)
```

Do not commit `.env`, token JSON files, or `sparkle_shine.db`. These are already in `.gitignore`.

---

## Config Value Documentation Rule (L1)

Every numeric config value (rates, probabilities, thresholds, prices, volumes) MUST have an inline comment with one of:

```python
# Source: HomeAdvisor 2024 report -- 3-10 daily inquiries for metro home service businesses
"base_min": 3,

# ESTIMATED -- reasoning: 35% of cleaning inquiries are serious enough for sales conversation.
# Higher than SaaS (5-15%) because cleaning is a considered but not complex purchase.
"sql_fraction": 0.35,
```

Never write a bare number without explaining where it came from. The original project got burned by unbenchmarked commercial pricing (5-10x too low). Every future config change should be traceable to either data or documented reasoning.

After changing any config values, run `config_math_trace()` to verify the business trajectory is still realistic (L2).
