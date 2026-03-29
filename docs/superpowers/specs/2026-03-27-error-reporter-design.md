# Design Spec: simulation/error_reporter.py

**Date:** 2026-03-27
**Module:** `simulation/error_reporter.py`
**Scope:** Translate all simulation/automation/intelligence errors into plain-language Slack messages posted to `#automation-failure`. No stack traces. No jargon.

---

## Context

This module is the single integration point for error reporting across the simulation engine, all generators, the automation runner, and future modules (e.g., the reconciliation CLI at Step 7). The project-conventions rule is: "Log to file, Report to Slack." Callers do `logger.exception()` first for the technical record, then call `report_error()` for the human-readable Slack message.

The module is the first file in the `simulation/` package. An empty `simulation/__init__.py` is created alongside it.

---

## Files Created

```
simulation/__init__.py       # empty package marker
simulation/error_reporter.py # this module
```

---

## Public API

```python
# Configurable escalation thresholds (documented here for Step 10 tests)
ESCALATION_THRESHOLD = 3        # warnings from same tool within window → critical
ESCALATION_WINDOW_MINUTES = 30  # rolling window in minutes
                                # 30 min covers 2-3 automation poll cycles and accounts for
                                # off-peak event spacing where events can be 15-30 min apart.
                                # A 10-min window would miss repeated failures during slow periods.

def setup_channel(dry_run: bool = False) -> str | None:
    """Create #automation-failure if it doesn't exist, set its topic, cache and return channel ID.

    Idempotent: subsequent calls return the cached ID immediately.
    Returns None if Slack is unreachable — callers must handle None gracefully.
    """

def report_error(
    exc: Exception | str,
    tool_name: str,
    context: str,
    severity: str | None = None,   # "info" | "warning" | "critical" — overrides auto-detection
    dry_run: bool = False,
) -> bool:
    """Translate exc to plain language and post to #automation-failure.

    exc may be a caught Exception or a plain string (for findings that aren't exceptions,
    e.g. "3 completed jobs from yesterday don't have invoices.").

    severity override bypasses escalation logic entirely.

    Escalation: warning-level errors from the same tool_name are tracked in a rolling
    ESCALATION_WINDOW_MINUTES window. When count >= ESCALATION_THRESHOLD the message
    is auto-promoted to critical with header "Automation Issue — Repeated Failures".

    Returns True if posted (or dry_run=True). Never raises.
    """

def report_reconciliation_issue(
    finding: dict,
    dry_run: bool = False,
) -> bool:
    """Post a reconciliation finding to #automation-failure.

    Uses :mag: *Data Mismatch Detected* header (not :warning: *Automation Issue*).
    Called directly by the Step 7 reconciliation CLI.

    Expected finding dict keys:
        category  str  "reconciliation_mismatch" | "reconciliation_missing"
                       | "reconciliation_automation_gap"
        tool      str  tool name (e.g. "quickbooks")
        entity    str  canonical ID or human description (e.g. "SS-CLIENT-0047")
        count     int  required for "reconciliation_automation_gap" category
        details   str  optional — appended as a 4th Block Kit section if present

    Returns True if posted (or dry_run=True). Never raises.
    """
```

---

## Module-Level State

```python
_channel_id: str | None = None
# Cached channel ID for #automation-failure.
# None until setup_channel() succeeds.

_warning_log: dict[str, list[float]] = {}
# Sliding-window escalation tracker.
# Key: tool_name. Value: list of unix timestamps for warning-level errors from that tool.
# On each report_error() call, only _warning_log[tool_name] is pruned (entries older than
# ESCALATION_WINDOW_MINUTES are removed). Then len(_warning_log[tool_name]) is checked
# against ESCALATION_THRESHOLD.
```

---

## Translation Map

### Error Classification

`_classify(exc)` returns a category string:

| Input | Category |
|---|---|
| `TokenExpiredError`, HTTP 401 in exception message | `"token_expired"` |
| HTTP 403 | `"permission_error"` |
| HTTP 429, `RateLimitError` | `"rate_limited"` |
| HTTP 500–504, `ToolUnavailableError` | `"server_error"` |
| `requests.ConnectionError` | `"connection_error"` |
| `requests.Timeout` | `"timeout"` |
| HTTP 400, `ToolAPIError` (non-404) | `"client_error"` |
| HTTP 404 | `"not_found"` |
| Plain `str` passed as `exc` | `"manual"` — string used directly as `what_happened` |
| Anything else | `"unknown"` |

### Category Defaults (`_CATEGORY_DEFAULTS`)

`{tool}` is interpolated with `tool_name.title()` (e.g. `"quickbooks"` → `"Quickbooks"`).

| Category | what_happened | what_to_do | severity |
|---|---|---|---|
| `token_expired` | "The connection to {tool} has expired." | "Run: `python -m demo.hardening.token_preflight`" | critical |
| `permission_error` | "{tool} rejected the request — it may have lost a required permission." | "Check that the {tool} token still has all required scopes." | warning |
| `rate_limited` | "{tool} asked us to slow down." | "The engine will retry automatically. No action needed." | warning |
| `server_error` | "{tool} returned a server error." | "The engine will retry. If this persists, check {tool}'s status page." | warning |
| `connection_error` | "Could not reach {tool}." | "Check network connectivity. The engine will retry." | warning |
| `timeout` | "The request to {tool} timed out." | "The engine will retry. If this persists, check {tool}'s status page." | warning |
| `client_error` | "A data error occurred sending a record to {tool}." | "Check the log file for the rejected record's details." | info |
| `not_found` | "A record expected in {tool} was not found." | "Check the log file for the missing record's ID." | **warning** |
| `manual` | *(the string passed as `exc`)* | "Review the log file for details." | info |
| `unknown` | "An unexpected error occurred with {tool}." | "Check the log file for the full stack trace." | warning |

**Why `not_found` is warning:** A missing record means every subsequent automation that references it will also fail. The reconciler (Step 7) will eventually catch it, but the immediate signal should be amber.

### Tool-Specific Overrides (`_TOOL_OVERRIDES`)

Only entries that differ from the category defaults.

| Tool | Category | Override |
|---|---|---|
| `quickbooks` | `token_expired` | `what_to_do`: "Refresh the token: `python -m auth.quickbooks_auth`" |
| `jobber` | `token_expired` | `what_to_do`: "Refresh the token: `python -m auth.jobber_auth`" |
| `google` | `token_expired` | `what_to_do`: "Re-authenticate: `python -m auth.google_auth`" |
| `asana` | `permission_error` | Append to `what_to_do`: "Asana occasionally returns 403 for tasks in restricted projects — check if this is a one-off before escalating." |

**Note:** `demo/hardening/token_preflight.py` exists in the repo but accepts no `--tool` flag. The category default (`python -m demo.hardening.token_preflight`) runs all token checks and is valid for the generic case. Tool-specific overrides use each tool's own auth module directly.

### Reconciliation Categories (`_RECONCILIATION_DEFAULTS`)

Used only by `report_reconciliation_issue()`. `{tool}`, `{entity}`, `{count}` interpolated from the `finding` dict.

| Category | what_happened | what_to_do | severity |
|---|---|---|---|
| `reconciliation_mismatch` | "{tool} record for {entity} doesn't match the canonical database." | "Review the mismatch details below. Auto-repaired mismatches need no action." | info |
| `reconciliation_missing` | "Expected record in {tool} for {entity} was not found." | "The record may need to be recreated. Check the log for the canonical ID." | warning |
| `reconciliation_automation_gap` | "{count} completed jobs have no invoices after 24 hours." | "The Jobber-to-QuickBooks automation may have missed them. Check poll_state and QuickBooks auth." | critical |

**Why `automation_gap` is critical:** Missing invoices mean completed work is not being billed — direct revenue impact, highest-priority automation failure.

---

## Data Flow

### `report_error()`

```
report_error(exc, tool_name, context, severity=None, dry_run=False)
      │
      ├─ 1. setup_channel() lazy call → channel_id (or None if Slack unreachable)
      │         if channel_id is None: logger.warning(…); return False
      │
      ├─ 2. _classify(exc) → category
      │
      ├─ 3. Look up _CATEGORY_DEFAULTS[category]
      │         → {what_happened, what_to_do, base_severity}
      │
      ├─ 4. Apply _TOOL_OVERRIDES[tool_name][category] if present
      │         (merge / append override fields)
      │
      ├─ 5. Interpolate {tool} with tool_name.title()
      │         NOTE: the `context` parameter passed by the caller IS the "What was affected"
      │         content. _build_error_blocks() slots `context` directly into the
      │         "What was affected" section — no transformation.
      │
      ├─ 6. Determine final severity
      │         if severity param passed → use it (escalation bypassed)
      │         else:
      │             final_severity = base_severity
      │             if base_severity == "warning":
      │                 append time.time() to _warning_log[tool_name]
      │                 prune _warning_log[tool_name] entries older than ESCALATION_WINDOW_MINUTES
      │                 if len(_warning_log[tool_name]) >= ESCALATION_THRESHOLD:
      │                     final_severity = "critical"
      │                     header_text = "Automation Issue — Repeated Failures"
      │
      ├─ 7. _build_error_blocks(...) → list[dict]
      │         blocks = [header, divider, what_happened section, what_was_affected section,
      │                   what_to_do section, divider, context footer]
      │
      ├─ 8. dry_run=True → logger.info("[DRY RUN] would post to #automation-failure"); return True
      │
      └─ 9. slack_client.chat_postMessage(
                 channel=channel_id,
                 text=fallback_text,          # plain-text fallback for notifications
                 blocks=blocks,               # Block Kit content (header, sections, dividers)
                 attachments=[{"color": color_hex, "blocks": []}]  # color sidebar only
             )
               ok=True  → return True
               ok=False → logger.error(…); return False
               Exception → logger.error(…); return False  ← never raises
```

### `report_reconciliation_issue()`

```
report_reconciliation_issue(finding, dry_run=False)
      │
      ├─ 1. setup_channel() lazy call → channel_id
      │
      ├─ 2. Extract: category, tool, entity, count (if present), details (if present)
      │
      ├─ 3. Look up _RECONCILIATION_DEFAULTS[category]
      │         Interpolate {tool}, {entity}, {count}
      │
      ├─ 4. _build_reconciliation_blocks(...) → list[dict]
      │
      ├─ 5. dry_run=True → logger.info("[DRY RUN] …"); return True
      │
      └─ 6. slack_client.chat_postMessage(…)
```

### `setup_channel()`

```
setup_channel(dry_run=False)
      │
      ├─ if _channel_id is not None → return _channel_id  (cached, no API call)
      │
      ├─ conversations_list(types="public_channel", limit=200)
      │         # Most workspaces have fewer than 200 channels. Exhaustive pagination on a
      │         # large workspace would slow engine startup for no benefit. If not found in
      │         # first 200 results, skip further pages and go straight to conversations_create.
      │         found "automation-failure" → cache id, set topic, return id
      │
      ├─ not found → conversations_create(name="automation-failure")
      │         cache new id
      │         conversations_setTopic(topic="Simulation and automation errors —
      │             plain language only, no stack traces")
      │         return id
      │
      └─ any Exception → logger.warning("Could not set up #automation-failure: …")
                         return None
```

---

## Block Kit Payloads

### `report_error()` — warning example (QuickBooks token expired)

```
header:   ":warning: Automation Issue"
divider
section:  "*What happened:* The connection to Quickbooks has expired."
section:  "*What was affected:* Invoice for Sarah Chen was skipped."
section:  "*What to do:* Refresh the token: python -m auth.quickbooks_auth"
divider
context:  "_Tool: quickbooks | 2026-03-27 14:23 UTC_"
```

All message content (header, divider, sections, context) is sent in the top-level `blocks` field. A single `attachments` entry with only `color` is added to get the severity sidebar — no content goes inside `attachments`.

```python
slack_client.chat_postMessage(
    channel=channel_id,
    text="Automation Issue — <what_happened>",  # plain-text fallback
    blocks=[...],                                # all message content here
    attachments=[{"color": "#FFC107", "blocks": []}],  # color only
)
```

Severity → color mapping:

| Severity | Header emoji | Attachment color |
|---|---|---|
| `info` | *(none)* | `#2196F3` (blue) |
| `warning` | `:warning:` | `#FFC107` (amber) |
| `critical` | `:rotating_light:` | `#D32F2F` (red) |

Auto-escalated critical header: `":rotating_light: Automation Issue — Repeated Failures"`

### `report_reconciliation_issue()` — missing record example

```
header:   ":mag: Data Mismatch Detected"
divider
section:  "*What happened:* Expected record in Quickbooks for SS-CLIENT-0047 was not found."
section:  "*What was affected:* SS-CLIENT-0047"
section:  "*What to do:* The record may need to be recreated. Check the log for the canonical ID."
[section: finding["details"] — only if present]
divider
context:  "_Tool: quickbooks | Category: reconciliation_missing | 2026-03-27 14:23 UTC_"
```

---

## Integration Points

### Simulation engine (Step 1 — main exception handler)

```python
# In simulation/engine.py main event loop
from simulation.error_reporter import report_error

try:
    generator.run_event(event)
except Exception as e:
    logger.exception("Event failed: %s", event)
    report_error(e, tool_name=event.tool, context=f"{event.description}")
    # context = "What was affected" in the Slack message.
    # Example: report_error(e, tool_name="quickbooks", context="Invoice for Sarah Chen was skipped")
    continue  # one tool failing never cascades
```

Engine startup (after loading checkpoint, before entering event loop):

```python
from simulation.error_reporter import setup_channel

channel_id = setup_channel()
if channel_id is None:
    logger.warning("Slack #automation-failure unavailable — error reporting disabled for this session")
# engine continues regardless
```

### Generators (call through the engine)

Generators do not call `report_error()` directly. They raise exceptions; the engine's main handler calls `report_error()`. This keeps Slack logic in one place.

### Reconciliation CLI (Step 7)

```python
from simulation.error_reporter import report_reconciliation_issue

report_reconciliation_issue({
    "category": "reconciliation_missing",
    "tool": "quickbooks",
    "entity": "SS-CLIENT-0047",
    "details": "Job SS-JOB-8201 marked completed 2026-03-26, no invoice found.",
})
```

---

## Dry-Run Behaviour

Both `report_error()` and `report_reconciliation_issue()` honour `dry_run=True`:
- Log what would be posted at INFO level (include the full resolved message text)
- Do NOT call the Slack API
- Return `True`

`setup_channel()` with `dry_run=True`:
- Log what would be created/set
- Do NOT call the Slack API
- Return `"DRY-RUN-CHANNEL-ID"` (so callers can proceed without branching)

---

## Testing Notes (Step 10)

Tests to verify:

1. **Classification:** Each exception type and status code string maps to the correct category.
2. **Translation:** `_resolve_translation("quickbooks", "token_expired")` returns QuickBooks-specific `what_to_do`.
3. **Interpolation:** `{tool}` placeholder is replaced with `tool_name.title()` in all fields.
4. **Severity override:** Passing `severity="critical"` skips escalation logic and posts critical regardless of `_warning_log`.
5. **Escalation:** After `ESCALATION_THRESHOLD` warning calls for the same tool within `ESCALATION_WINDOW_MINUTES`, severity auto-promotes to critical.
6. **Escalation window expiry:** Warnings older than `ESCALATION_WINDOW_MINUTES` are pruned and do not count toward the threshold.
7. **`setup_channel()` idempotency:** Second call returns cached ID without hitting the Slack API.
8. **`setup_channel()` creates channel:** When `#automation-failure` does not exist, it is created and topic is set.
9. **Reconciliation categories:** Each of the three reconciliation categories produces the correct `what_happened` / `what_to_do` / severity.
10. **`automation_gap` is critical:** `report_reconciliation_issue({"category": "reconciliation_automation_gap", ...})` posts with critical severity and red sidebar.
11. **`str` exc path:** Passing a plain string as `exc` bypasses classification and uses the string as `what_happened`.
12. **Dry-run:** `dry_run=True` logs but does not call Slack API. Returns `True`.
13. **Never raises:** An exception inside `chat_postMessage` is caught, logged, and `False` is returned.
14. **`setup_channel()` failure is non-fatal:** If Slack is unreachable, `setup_channel()` returns `None` and `report_error()` returns `False` without raising.
15. **Cold-start lazy initialization:** With `_channel_id = None` and `setup_channel()` never called explicitly, calling `report_error()` triggers `setup_channel()` automatically, sets up the channel, and posts the message successfully. Verify `_channel_id` is populated after the call.
