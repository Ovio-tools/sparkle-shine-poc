# Skill: Generator Template

**When to read:** Before creating a new simulation generator. Copy the skeleton, fill in the tool-specific logic.

Every generator in the simulation engine follows the same pattern: register with the engine, implement `execute_one()`, return a `GeneratorResult`, handle errors gracefully.

---

## Skeleton (Copy This)

```python
"""
simulation/generators/{name}.py

{One-line description of what this generator does.}
"""

import sqlite3
from datetime import datetime, date
from dataclasses import dataclass, field

# Import paths -- confirm these match the codebase (see SIMULATION_AUDIT.md)
from database.mappings import generate_id, link, lookup, reverse_lookup
from simulation.config import DAILY_VOLUMES
from simulation.variation import should_event_happen, get_adjusted_volume

# Auth -- CONFIRMED: use auth.get_client() exclusively
# Never import credentials.py directly in simulation code.
from auth import get_client

# Throttler -- use the pre-configured instance for this tool
from seeding.utils.throttler import HUBSPOT as throttler
# Available: JOBBER, QUICKBOOKS, HUBSPOT, MAILCHIMP, PIPEDRIVE, ASANA

# Logging
from intelligence.logging_config import setup_logging

logger = setup_logging("simulation.{name}")


@dataclass
class GeneratorResult:
    """Returned by every execute_one() call."""
    summary: str                # one-line log message
    tool: str                   # primary tool affected (e.g., "hubspot")
    canonical_id: str           # SS-TYPE-NNNN of the created/modified record
    details: dict = field(default_factory=dict)  # full data for debugging
    error: str | None = None    # None if successful


class {ClassName}Generator:
    """
    {What this generator does.}
    
    Registered with the simulation engine as the "{name}" generator.
    Engine calls execute_one() on each tick when this generator
    hasn't hit its daily target yet.
    """

    name = "{name}"

    def __init__(self, db_path: str = "sparkle_shine.db"):
        self.db_path = db_path
        self.logger = logger

    async def execute_one(self) -> GeneratorResult:
        """Execute one atomic unit of work.

        This method must:
        1. Decide WHAT to do (pick a record to act on, generate data, etc.)
        2. DO it (API call to the tool + SQLite write + mapping)
        3. Return a GeneratorResult

        If there's nothing to do (e.g., no eligible records), return
        a result with summary="No eligible records" and move on.

        Exceptions are caught by the engine and sent to the error reporter.
        Only raise if the error is unrecoverable. For transient errors
        (rate limits, timeouts), retry internally or let the engine retry.
        """
        db = sqlite3.connect(self.db_path)

        try:
            # ── 1. Decide what to do ──────────────────────────
            # Example: pick an eligible record from SQLite
            eligible = self._get_eligible_records(db)
            if not eligible:
                return GeneratorResult(
                    summary="No eligible records",
                    tool="{tool}",
                    canonical_id="",
                )

            record = self._pick_one(eligible)

            # ── 2. Do it ──────────────────────────────────────
            # Generate any needed data
            data = self._generate_data(record)

            # Call the tool API
            tool_id = self._call_tool_api(data)

            # Write to SQLite
            self._write_to_sqlite(db, record, data, tool_id)

            # Register cross-tool mapping (if creating a new record)
            canonical_id = record.get("canonical_id") or generate_id("{TYPE}")
            link(canonical_id, "{tool}", tool_id)

            db.commit()

            # ── 3. Return result ──────────────────────────────
            return GeneratorResult(
                summary=f"Created {data['description']} for {record['name']}",
                tool="{tool}",
                canonical_id=canonical_id,
                details=data,
            )

        except Exception as e:
            db.rollback()
            self.logger.error(f"Failed: {e}")
            raise  # let the engine's error handler deal with it

        finally:
            db.close()

    def _get_eligible_records(self, db: sqlite3.Connection) -> list[dict]:
        """Query SQLite for records this generator can act on.

        Examples:
        - Contacts generator: returns [] (it generates from scratch)
        - Deal generator: returns open deals that haven't moved today
        - Payment generator: returns unpaid invoices past their due window
        - Churn generator: returns active clients not yet checked today
        """
        cursor = db.execute("""
            SELECT canonical_id, column1, column2
            FROM some_table
            WHERE status = 'active'
        """)
        return [dict(zip([d[0] for d in cursor.description], row))
                for row in cursor.fetchall()]

    def _pick_one(self, eligible: list[dict]) -> dict:
        """Pick one record from the eligible list.

        Options:
        - Random: random.choice(eligible)
        - Weighted by age: older records more likely
        - Weighted by value: higher-value records more likely
        - Sequential: eligible[0] (FIFO)
        """
        import random
        return random.choice(eligible)

    def _generate_data(self, record: dict) -> dict:
        """Generate any data needed for the action.

        Examples:
        - Contact generator: full profile (name, email, address, etc.)
        - Payment generator: payment amount and date
        - Churn generator: churn reason
        """
        return {}

    def _call_tool_api(self, data: dict) -> str:
        """Make the API call to the SaaS tool.

        See docs/skills/tool-api-patterns.md for the correct
        endpoint and payload format per tool.

        Returns the tool-specific ID of the created/updated record.
        """
        # Get an authenticated session via the unified auth interface
        session = get_client("{tool_name}")

        # Throttle
        throttler.wait()

        # Call
        resp = session.post(url, json=payload, timeout=30)
        if resp.status_code not in (200, 201):
            raise RuntimeError(f"API returned {resp.status_code}: {resp.text[:300]}")

        return resp.json()["id"]

    def _write_to_sqlite(self, db, record, data, tool_id):
        """Write the result to SQLite.

        See docs/skills/canonical-record.md for table schemas
        and the correct insert/update patterns.
        """
        db.execute("""
            INSERT INTO some_table (canonical_id, ...)
            VALUES (?, ...)
        """, (...))
```

---

## Registration With the Engine

In `simulation/engine.py`, generators are registered at startup:

```python
from simulation.generators.contacts import ContactGenerator
from simulation.generators.deals import DealProgressionGenerator
from simulation.generators.operations import OperationsGenerator
from simulation.generators.invoicing import InvoicingGenerator
from simulation.generators.payments import PaymentGenerator
from simulation.generators.tasks import TaskCompletionGenerator
from simulation.generators.churn import ChurnGenerator

engine = SimulationEngine()

engine.register_generator("contacts", ContactGenerator(db_path), weight=1.0)
engine.register_generator("deals", DealProgressionGenerator(db_path), weight=0.8)
engine.register_generator("operations", OperationsGenerator(db_path), weight=1.2)
engine.register_generator("invoicing", InvoicingGenerator(db_path), weight=0.6)
engine.register_generator("payments", PaymentGenerator(db_path), weight=0.5)
engine.register_generator("tasks", TaskCompletionGenerator(db_path), weight=0.7)
engine.register_generator("churn", ChurnGenerator(db_path), weight=0.2)
```

The weight determines how often the engine picks a given generator relative to others. Higher weight = more events per day.

---

## Generator Types

### Type 1: Creates Records From Scratch

Examples: `contacts.py`

These generators don't query for eligible records. They generate entirely new data and push it to a tool.

```python
async def execute_one(self) -> GeneratorResult:
    profile = self.generate_contact_profile()  # random data
    stage = self.assign_lifecycle_stage(profile)
    hubspot_id = self.create_in_hubspot(profile, stage)
    canonical_id = self.create_canonical_record(profile, hubspot_id)
    return GeneratorResult(
        summary=f"Created contact: {profile['first_name']} {profile['last_name']} ({stage})",
        tool="hubspot",
        canonical_id=canonical_id,
    )
```

### Type 2: Progresses Existing Records

Examples: `deals.py`, `tasks.py`

These generators query for eligible records and modify them.

```python
async def execute_one(self) -> GeneratorResult:
    deals = self.get_open_deals()
    if not deals:
        return GeneratorResult(summary="No open deals", tool="pipedrive", canonical_id="")

    deal = self.pick_one_weighted(deals)

    if self.should_advance(deal):
        new_stage = self.next_stage(deal["current_stage"])
        self.advance_deal(deal, new_stage)
        return GeneratorResult(
            summary=f"Advanced {deal['title']} to {new_stage}",
            tool="pipedrive",
            canonical_id=deal["canonical_id"],
        )
    else:
        return GeneratorResult(
            summary=f"Checked {deal['title']}, no movement",
            tool="pipedrive",
            canonical_id=deal["canonical_id"],
        )
```

### Type 3: Reacts to Prior Events

Examples: `invoicing.py`, `payments.py`

These generators look for records created by other generators (or automations) that need follow-up.

```python
async def execute_one(self) -> GeneratorResult:
    # Find completed jobs that don't have invoices yet
    uninvoiced = self.get_completed_jobs_without_invoices()
    if not uninvoiced:
        return GeneratorResult(summary="No jobs to invoice", tool="quickbooks", canonical_id="")

    job = uninvoiced[0]  # FIFO -- oldest first
    invoice_id = self.create_invoice(job)
    return GeneratorResult(
        summary=f"Invoiced ${job['amount']:.0f} for {job['client_name']}",
        tool="quickbooks",
        canonical_id=invoice_id,
    )
```

### Type 4: Multi-Tool Cascade

Examples: `churn.py`

These generators touch 5+ tools in a single execution. Wrap each tool call in try/except so one failure doesn't block the others.

```python
async def execute_one(self) -> GeneratorResult:
    client = self.pick_churn_candidate()
    if not self.should_churn(client):
        return GeneratorResult(summary=f"Checked {client['name']}, not churning", ...)

    reason = self.pick_churn_reason(client)
    errors = []

    # Each tool call is independent -- don't let one block the rest
    try:
        self.cancel_jobber(client)
    except Exception as e:
        errors.append(f"Jobber: {e}")

    try:
        self.update_hubspot(client, reason)
    except Exception as e:
        errors.append(f"HubSpot: {e}")

    try:
        self.update_pipedrive(client, reason)
    except Exception as e:
        errors.append(f"Pipedrive: {e}")

    try:
        self.unsubscribe_mailchimp(client)
    except Exception as e:
        errors.append(f"Mailchimp: {e}")

    try:
        self.create_retention_task(client, reason)
    except Exception as e:
        errors.append(f"Asana: {e}")

    self.update_sqlite(client, reason)

    summary = f"Churned: {client['name']} ({reason})"
    if errors:
        summary += f" [{len(errors)} tool errors]"

    return GeneratorResult(
        summary=summary,
        tool="multi",
        canonical_id=client["canonical_id"],
        error="; ".join(errors) if errors else None,
    )
```

---

## Testing a Generator

Each generator should have:

1. **A unit test** that calls `_generate_data()` or `generate_contact_profile()` and verifies the output format without making API calls.

2. **A dry-run test** that calls `execute_one()` with API calls mocked and verifies SQLite writes.

3. **An integration test** (gated behind `RUN_INTEGRATION`) that creates one real record in the tool and verifies it.

```python
# tests/test_simulation.py

def test_contact_profile_generation():
    gen = ContactGenerator("sparkle_shine.db")
    for _ in range(20):
        p = gen.generate_contact_profile()
        assert p["zip"] in VALID_AUSTIN_ZIPS
        assert "@" in p["email"]
        assert p["client_type"] in ("residential", "commercial")

@unittest.skipUnless(os.getenv("RUN_INTEGRATION"), "Skipping")
def test_contact_creation_live():
    gen = ContactGenerator("sparkle_shine.db")
    result = asyncio.run(gen.execute_one())
    assert result.canonical_id.startswith("SS-")
    assert result.error is None
    # Verify the mapping was created
    from database.mappings import lookup
    assert lookup(result.canonical_id, "hubspot") is not None
```
