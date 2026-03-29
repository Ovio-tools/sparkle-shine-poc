# Weekly Report Feature — Design Spec
**Date:** 2026-03-28
**Status:** Approved

---

## Overview

Build a weekly business intelligence report for Sparkle & Shine using Claude Opus 4.6. The report surfaces patterns and trends invisible at the daily level, with clickable citations linking into each SaaS tool's UI. Delivered to `#weekly-briefing` on Sundays/Mondays via the existing intelligence runner.

---

## Scope

**New files:**
- `simulation/deep_links.py` — URL building and Slack mrkdwn citation formatting
- `intelligence/weekly_report.py` — Opus-based weekly report with 4 systems

**Edited files:**
- `intelligence/config.py` — add `weekly_model`, `max_tokens_weekly`, `temperature_weekly` to `MODEL_CONFIG`
- `intelligence/runner.py` — route `--report-type weekly` to `generate_weekly_report()` instead of `generate_briefing()`; call `ensure_channel()` before posting
- `intelligence/slack_publisher.py` — add `ensure_channel(channel_name)` function

**Out of scope:** changes to `context_builder.py`, `briefing_generator.py`, syncers, or metrics modules. The weekly context document produced by `build_weekly_context()` is used as-is.

---

## Architecture

The weekly path follows the same pipeline as daily: **Sync → Metrics → Context → Generate → Publish**. The only stage that changes is Generate, which now calls `generate_weekly_report()` instead of `generate_briefing()`.

```
intelligence/runner.py
  └── build_weekly_context()                  # existing, unchanged
  └── generate_weekly_report(context)         # NEW — weekly_report.py
        ├── _build_citation_index()           # built from context.metrics
        ├── _load_insight_history()           # weekly_reports/insight_history.json
        ├── [Opus 4.6 API call]
        ├── _extract_and_update_insights()    # (1) parse [insight_id:] markers,
        │                                     #     update insight_history.json,
        │                                     #     strip markers from text
        ├── _strip_low_confidence()           # (2) remove LOW-confidence sentences
        ├── _inject_citations()               # (3) replace [R01] with Slack mrkdwn
        └── _score_report()                  # (4) Sonnet quality scoring on final text
  └── ensure_channel(weekly_channel)          # NEW — slack_publisher.py
  └── post_briefing(briefing)                 # existing, unchanged
```

`generate_weekly_report()` returns a `Briefing` object — the same type as `generate_briefing()`. No other runner changes are needed.

---

## `simulation/deep_links.py`

### Purpose
Build clickable UI URLs for each SaaS tool and format them as Slack mrkdwn citations.

### Lazy cache pattern
Module-level globals `_pipedrive_subdomain`, `_hubspot_portal_id`, `_cache_loaded`. Populated on first call to `get_deep_link()` via one API call each:
- Pipedrive: `GET /users/me` → `data.company_domain`
- HubSpot: `GET /integrations/v1/me` → `portalId`

If either call fails, log a warning and leave the variable `None`. Deep links for that tool degrade gracefully to `"#"`.

### QuickBooks sandbox detection
Mirror `get_base_url()` in `auth/quickbooks_auth.py`:
```python
def _qbo_ui_base() -> str:
    api_base = os.getenv("QBO_BASE_URL", "https://sandbox-quickbooks.api.intuit.com/v3/company")
    return "https://app.sandbox.qbo.intuit.com/app" if "sandbox" in api_base else "https://app.qbo.intuit.com/app"
```
No import from auth needed. The signal (`QBO_BASE_URL`) is the same.

### URL templates by tool

| Tool | Record Type | URL Pattern |
|------|------------|-------------|
| HubSpot | contact | `app.hubspot.com/contacts/{portal_id}/contact/{id}` |
| HubSpot | deal | `app.hubspot.com/contacts/{portal_id}/deal/{id}` |
| Pipedrive | deal | `{subdomain}.pipedrive.com/deal/{id}` |
| Pipedrive | person | `{subdomain}.pipedrive.com/person/{id}` |
| Jobber | client | `app.getjobber.com/client/{id}` |
| Jobber | job | `app.getjobber.com/work_requests/{id}` |
| QuickBooks | invoice | `{qbo_ui_base}/invoice?txnId={id}` |
| QuickBooks | report_pl | `{qbo_ui_base}/reportv2?token=PROFIT_AND_LOSS` |
| Asana | task | `app.asana.com/0/{project_gid}/{id}` (see note below) |
| Mailchimp | campaign | `us{dc}.admin.mailchimp.com/campaigns/show?id={id}` (dc from `MAILCHIMP_DATA_CENTER`) |

**Asana `project_gid` source:** Load from `config/tool_ids.json` at module init:
```python
TOOL_IDS["asana"]["projects"] = {
    "Sales Pipeline Tasks": "1213719393240330",
    "Marketing Calendar":   "1213719401725621",
    "Admin & Operations":   "1213719394454339",
    "Client Success":       "1213719346640011",
}
```
Determine the task's project from the `tasks.project` column in SQLite (populated during seeding). Look up the matching GID in the map above. If the project name isn't in the map or the task's project is unknown, fall back to:
```
https://app.asana.com/0/search?q={task_gid}
```
Less precise (Asana search by GID), but still navigable. Log a debug message when the fallback is used.

### Public API
```python
get_deep_link(tool: str, record_type: str, record_id: str) -> str
get_report_link(tool: str) -> str
format_citation(text: str, tool: str, record_type: str, record_id: str) -> str
# Returns: "(<URL|text>)" for Slack mrkdwn, or plain text if URL unavailable
```

---

## `intelligence/weekly_report.py`

### System 1 — Insight History

**File:** `weekly_reports/insight_history.json` (auto-created if missing, with `{"last_updated": null, "insights": []}`)

**On each run:**
1. Load file. Extract last 4 weeks of `active` and `graduated` entries.
2. Build `PREVIOUSLY REPORTED` block and inject into system prompt as `{insight_history_block}`.

**After generation — handled by `_extract_and_update_insights()` (post-processing step 1):**
1. Parse Opus output for `[insight_id: <id>]` markers.
2. Increment `times_reported` and update `last_reported` for each matched insight.
3. Add new insight IDs not previously seen (status `"active"`, `times_reported: 1`).
4. Graduate any insight where `times_reported >= 3` and `status == "active"` → set `status = "graduated"`.
5. Write updated file back.
6. Strip all `[insight_id: ...]` markers from the text and return the cleaned text.

This runs before `_strip_low_confidence()` and `_inject_citations()` so markers never appear in the final Slack output and the quality score reflects the actual posted text.

**Insight ID matching:** The system prompt instructs Opus to tag each insight it reports using `[insight_id: <id>]` markers at the end of the relevant sentence (e.g., `Crew A's rating dipped to 4.3 this week. [insight_id: crew_a_speed_quality]`). `_extract_and_update_insights()` extracts these markers, updates the history file, and returns the text with all markers stripped.

### System 2 — Confidence Levels

Context document tags each metric bucket as `HIGH`, `MEDIUM`, or `LOW` using the rubric from `docs/skills/weekly-report.md`:

| Signal | Confidence |
|--------|-----------|
| 50+ records over 4+ weeks | HIGH |
| 20–50 records over 2–3 weeks | HIGH (consistent) or MEDIUM (volatile) |
| 5–20 records or 1 week | MEDIUM |
| Fewer than 5 records | LOW (exclude) |
| Cross-tool pattern (visible in 2+ tools) | Boost one level |
| Single-tool observation | No boost |
| Observation contradicted by another metric | Drop one level |

**Post-generation validation (string matching, not LLM):**
```python
def _strip_low_confidence(text: str, citation_index: list[dict]) -> tuple[str, int]:
    # Pass 1: strip sentences containing literal "[LOW]" tags
    # Pass 2: strip sentences containing ref_ids where metric was tagged LOW
    # Returns: (cleaned_text, removed_count)
```
```python
removed = _strip_low_confidence(report_text, citation_index)
if removed > 0:
    logger.warning("Removed %d LOW-confidence claims from weekly report", removed)
```

### System 3 — Citation Index

Built from `context.metrics` before the Opus call. Each entry:
```json
{
    "ref_id": "R01",
    "claim": "Weekly revenue: $38,450",
    "tool": "quickbooks",
    "record_type": "report_pl",
    "record_id": null,
    "url": "https://app.sandbox.qbo.intuit.com/app/reportv2?token=PROFIT_AND_LOSS",
    "confidence": "HIGH"
}
```

Passed to Opus as a structured block in the user message. Opus is instructed to embed `[R01]` inline. Post-processing replaces `[R01]` with `(<url|claim>)`.

If `url` is `"#"` (deep link degraded), `format_citation()` returns plain text and no link is inserted.

### System 4 — Quality Scoring

**Model:** `claude-sonnet-4-6` (cheap call, not Opus)
**Tokens:** 200 max output
**Temperature:** 0.0

Rubric loaded verbatim from `docs/skills/weekly-report.md` on first call (lazy-load, module-level `_rubric_text` cache):
```python
_rubric_text: str | None = None

def _load_rubric() -> str:
    global _rubric_text
    if _rubric_text is None:
        doc = Path("docs/skills/weekly-report.md").read_text()
        _rubric_text = _extract_section(doc, "## Quality Scoring Rubric")
    return _rubric_text
```

`_extract_section()` finds the heading and extracts until the next `---` divider.

Score logged at INFO. If score < 60, log WARNING. Report posts to Slack regardless (POC — no blocking). Quality score and rubric breakdown included in the archived `.md` file header.

### System prompt

Uses the template from `docs/skills/weekly-report.md` verbatim, with runtime injections:
- `{insight_history_block}` — last 4 weeks of insight history
- `{month_name}` — current month name (e.g., `"March"`)
- `{seasonal_weight}` — from `simulation/config.py SEASONAL_WEIGHTS[month]`
- `{seasonal_note}` — plain-English interpretation (e.g., `"A 5% dip from February is expected."`)

### Function signature
```python
def generate_weekly_report(context: BriefingContext, dry_run: bool = False) -> Briefing:
```

Returns a `Briefing` with `report_type="weekly"` and `model_used="claude-opus-4-6"`.

In dry-run mode: loads insight history, builds citation index, but skips the Opus API call and returns a placeholder `Briefing`.

---

## `intelligence/config.py` changes

Add three keys to `MODEL_CONFIG`:
```python
"weekly_model":        "claude-opus-4-6",
"max_tokens_weekly":   3000,
"temperature_weekly":  0.4,
```

---

## `intelligence/runner.py` changes

**Stage 4 (Generate) — one branch added:**
```python
if args.report_type == "weekly":
    from intelligence.weekly_report import generate_weekly_report
    briefing = generate_weekly_report(context, dry_run=args.dry_run)
else:
    briefing = generate_briefing(context, dry_run=args.dry_run)
```

**Before Stage 5 (Publish), weekly path only:**
```python
if args.report_type == "weekly" and not args.dry_run:
    from intelligence.slack_publisher import ensure_channel
    ensure_channel(SLACK_CONFIG["weekly_channel"])
```

---

## `intelligence/slack_publisher.py` changes

New `ensure_channel(channel_name: str) -> str` function:

1. Try `resolve_channel_id()` first (fast path — channel already exists and bot is a member).
2. On `ValueError`: call `conversations.create(name=name, is_private=False)`.
3. On `name_taken` error from create: call `conversations.join(channel=name)` (channel exists but bot is not a member).
4. Cache result in `_channel_id_cache`. Return channel ID.

The `#daily-briefing` path is unchanged and does not call `ensure_channel`.

---

## Error handling

- Deep link failures (network, bad token): log warning, degrade to plain text. Report still generates.
- Insight history file missing: auto-create with empty structure. Not an error.
- Rubric file missing (`docs/skills/weekly-report.md`): log error, skip quality scoring for this run. Report still generates.
- Opus API failure: follows existing `generate_briefing` retry pattern (2 retries, exponential backoff).
- `ensure_channel` failure: log error, proceed to `post_briefing` which will fail gracefully (returns `False`, runner logs error).

---

## Testing

New tests in `tests/test_phase4.py`:
- `test_deep_links_format_citation` — verifies `(<url|text>)` format
- `test_deep_links_qbo_sandbox_detection` — verifies sandbox vs production URL selection based on `QBO_BASE_URL`
- `test_deep_links_fallback_on_error` — verifies graceful degradation to plain text when cache load fails
- `test_weekly_report_no_low_confidence` — asserts no LOW-confidence claims in final output after `_strip_low_confidence()`
- `test_weekly_report_quality_score` — asserts score ≥ 60 on a well-formed report
- `test_weekly_report_insight_history_graduation` — asserts insight graduates to `"graduated"` after 3 reports
- `test_weekly_report_dry_run` — asserts no API calls made, returns placeholder `Briefing`
- `test_ensure_channel_creates_if_missing` — mocked Slack client, asserts `conversations.create` called when channel not found
- `test_ensure_channel_joins_if_name_taken` — asserts `conversations.join` called on `name_taken` error

Integration tests (gated behind `RUN_INTEGRATION`):
- `test_weekly_report_live` — runs full weekly report against live DB, asserts `Briefing` returned with `report_type="weekly"`

---

## Invocation

```bash
# Run weekly report (skipping sync)
python -m intelligence.runner --skip-sync --date 2026-03-23 --report-type weekly

# Dry run (inspect without API calls)
python -m intelligence.runner --skip-sync --date 2026-03-23 --report-type weekly --dry-run
```
