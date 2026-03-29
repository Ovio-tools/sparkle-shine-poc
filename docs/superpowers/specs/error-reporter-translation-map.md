# Design — Section 2: Translation Map
## `simulation/error_reporter.py`

---

## Error Classification

`_classify(exc)` maps an exception or HTTP status code to a category string:

| Input | Category |
|---|---|
| `TokenExpiredError`, HTTP 401 | `"token_expired"` |
| HTTP 403 | `"permission_error"` |
| HTTP 429, `RateLimitError` | `"rate_limited"` |
| HTTP 500–504, `ToolUnavailableError` | `"server_error"` |
| `requests.ConnectionError` | `"connection_error"` |
| `requests.Timeout` | `"timeout"` |
| HTTP 400, `ToolAPIError` (non-404) | `"client_error"` |
| HTTP 404 | `"not_found"` |
| Plain `str` passed as `exc` | `"manual"` — bypass translation, use the string as `what_happened` |
| Anything else | `"unknown"` |

---

## Category Defaults

One entry per category with `what_happened`, `what_to_do`, and `severity`. Tool name is interpolated via `{tool}` placeholder.

| Category | what_happened | what_to_do | severity |
|---|---|---|---|
| `token_expired` | "The connection to {tool} has expired." | "Run: `python -m demo.hardening.token_preflight`" | critical |
| `permission_error` | "{tool} rejected the request — it may have lost a required permission." | "Check that the {tool} token still has all required scopes." | warning |
| `rate_limited` | "{tool} asked us to slow down." | "The engine will retry automatically. No action needed." | warning |
| `server_error` | "{tool} returned a server error." | "The engine will retry. If this persists, check {tool}'s status page." | warning |
| `connection_error` | "Could not reach {tool}." | "Check network connectivity. The engine will retry." | warning |
| `timeout` | "The request to {tool} timed out." | "The engine will retry. If this persists, check {tool}'s status page." | warning |
| `client_error` | "A data error occurred sending a record to {tool}." | "Check the log file for the rejected record's details." | info |
| `not_found` | "A record expected in {tool} was not found." | "Check the log file for the missing record's ID." | warning |
| `manual` | *(the string passed in as `exc`)* | "Review the log file for details." | info |
| `unknown` | "An unexpected error occurred with {tool}." | "Check the log file for the full stack trace." | warning |

---

## Tool-Specific Overrides

Only entries that differ from the category defaults above.

| Tool | Category | Override |
|---|---|---|
| `quickbooks` | `token_expired` | `what_to_do`: "Refresh the token: `python -m auth.quickbooks_auth`" |
| `jobber` | `token_expired` | `what_to_do`: "Refresh the token: `python -m auth.jobber_auth`" |
| `google` | `token_expired` | `what_to_do`: "Re-authenticate: `python -m auth.google_auth`" |
| `asana` | `permission_error` | Append to `what_to_do`: "Asana occasionally returns 403 for tasks in restricted projects — check if this is a one-off before escalating." |

> **Note:** `demo/hardening/token_preflight.py` exists but has no `--tool` flag. The category default (`python -m demo.hardening.token_preflight`) runs all token checks and remains valid for the generic case. Tool-specific overrides use each tool's own auth module directly.

---

## Escalation Logic

Repeated warning-level errors from the same tool within a rolling window auto-escalate to critical.

| Constant | Default | Meaning |
|---|---|---|
| `ESCALATION_THRESHOLD` | `3` | Number of warnings from the same tool that triggers escalation |
| `ESCALATION_WINDOW_MINUTES` | `30` | Rolling window in minutes |

**Behaviour:** Each call to `report_error()` that resolves to `warning` appends `(tool_name, timestamp)` to the module-level `_warning_log`. Before posting, entries older than `ESCALATION_WINDOW_MINUTES` are pruned. If the count of remaining entries for the given `tool_name` reaches `ESCALATION_THRESHOLD`, the severity is promoted to `critical` and the Slack message header changes to `:rotating_light: *Automation Issue — Repeated Failures*`.

A manually passed `severity` override bypasses escalation.

---

---

## Reconciliation Categories

Used exclusively by `report_reconciliation_issue()`. These categories use the `:mag: *Data Mismatch Detected*` header instead of the `:warning: *Automation Issue*` header used by `report_error()`.

`{tool}`, `{entity}`, and `{count}` are interpolated from the `finding` dict passed by the reconciler.

| Category | what_happened | what_to_do | severity |
|---|---|---|---|
| `reconciliation_mismatch` | "{tool} record for {entity} doesn't match the canonical database." | "Review the mismatch details below. Auto-repaired mismatches need no action." | info |
| `reconciliation_missing` | "Expected record in {tool} for {entity} was not found." | "The record may need to be recreated. Check the log for the canonical ID." | warning |
| `reconciliation_automation_gap` | "{count} completed jobs have no invoices after 24 hours." | "The Jobber-to-QuickBooks automation may have missed them. Check poll_state and QuickBooks auth." | critical |

> **Why `automation_gap` is critical:** Missing invoices mean completed work is not being billed. This is the highest-priority automation failure — it has direct revenue impact and requires immediate investigation.

---

## Severity → Slack Formatting

| Severity | Header emoji | Attachment color |
|---|---|---|
| `info` | *(none)* | `#2196F3` (blue) |
| `warning` | `:warning:` | `#FFC107` (amber) |
| `critical` | `:rotating_light:` | `#D32F2F` (red) |
