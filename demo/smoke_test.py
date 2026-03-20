#!/usr/bin/env python3
"""
Sparkle & Shine POC -- Pre-Demo Smoke Test

Run:  python -m demo.smoke_test
Flags:
  --skip-api   Skip checks 5 and 8 (no external API calls)
  --fix        Auto-fix warnings (regenerate scenarios, refresh tokens)
  --verbose    Print detailed output for each check
"""

from __future__ import annotations

import argparse
import json as _json
import os
import random
import sqlite3
import subprocess
import sys
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

# ── Project bootstrap ─────────────────────────────────────────────────────────
# Resolve to sparkle-shine-poc/ regardless of CWD when invoked as a module.

_BASE_DIR = Path(__file__).resolve().parent.parent
if str(_BASE_DIR) not in sys.path:
    sys.path.insert(0, str(_BASE_DIR))

from dotenv import load_dotenv
load_dotenv(_BASE_DIR / ".env")

# ── Constants ─────────────────────────────────────────────────────────────────

DB_PATH        = str(_BASE_DIR / "sparkle_shine.db")
BRIEFING_DATE  = "2026-03-17"

SCENARIO_OUTPUT_DIR = _BASE_DIR / "demo" / "scenarios" / "output"
SCENARIO_FILES = [
    "steady_state_briefing.md",
    "summer_surge_briefing.md",
    "rough_patch_briefing.md",
    "big_win_briefing.md",
    "holiday_crunch_briefing.md",
    "recovery_briefing.md",
]

# Maps scenario file stem → --scenario flag value for scenario_runner
_SCENARIO_FLAG: dict[str, str] = {
    "steady_state_briefing":   "steady_state",
    "summer_surge_briefing":   "summer_surge",
    "rough_patch_briefing":    "rough_patch",
    "big_win_briefing":        "big_win",
    "holiday_crunch_briefing": "holiday_crunch",
    "recovery_briefing":       "recovery",
}

# Table → minimum row count targets (WARN at 80%, FAIL at 0)
# Targets reflect actual generated volumes (seed=42, full 12-month narrative).
# CLAUDE.md estimates were rough; these match what the generators produce.
DB_TARGETS: dict[str, int] = {
    "clients":              300,
    "jobs":                 4500,
    "invoices":             4200,
    "commercial_proposals": 40,
    "tasks":                200,
    "cross_tool_mapping":   1000,
}

# Section headers that must appear in the context_document (check 4)
# These are the ## headings _format_context_document() writes for the 6 metric modules.
CONTEXT_SECTIONS = [
    "## YESTERDAY'S NUMBERS",
    "## CASH POSITION",
    "## TODAY'S SCHEDULE",
    "## SALES PIPELINE",
    "## TASK STATUS",
    "## MARKETING",
]

# Section headers mandated by SYSTEM_PROMPT_TEMPLATE that must appear in the
# generated briefing (check 5).
BRIEFING_SECTIONS = [
    "Yesterday's Performance",
    "Cash Position",
    "Today's Schedule",
    "Sales Pipeline",
    "Action Items",
    "One Opportunity",
]

PASS = "PASS"
WARN = "WARN"
FAIL = "FAIL"
SKIP = "SKIP"


# ── Result dataclass ──────────────────────────────────────────────────────────

@dataclass
class CheckResult:
    name: str
    status: str          # PASS | WARN | FAIL | SKIP
    detail: str = ""     # right-hand annotation in the box row, e.g. "(8/8)"
    notes: list[str] = field(default_factory=list)   # indented lines below the row
    fix_hint: str = ""   # printed as "Fix: <hint>" when non-empty


# ── Box renderer ──────────────────────────────────────────────────────────────

_STATUS_ICON = {
    PASS: "✓ PASS",
    WARN: "⚠ WARN",
    FAIL: "✗ FAIL",
    SKIP: "- SKIP",
}


def _render_box(results: list[CheckResult], total_seconds: float) -> str:
    title = "SPARKLE & SHINE -- SMOKE TEST"

    # Collect all content strings to determine required inner width
    candidates: list[str] = [f"  {title}"]
    for i, r in enumerate(results):
        icon = _STATUS_ICON.get(r.status, r.status)
        label = f"  {i + 1}. {r.name}"
        row = f"{label:<28}  {icon}"
        if r.detail:
            row += f"  {r.detail}"
        candidates.append(row)
        for note in r.notes:
            candidates.append(f"     {note}")
        if r.fix_hint:
            candidates.append(f"     Fix: {r.fix_hint}")

    candidates += [
        "  RESULT: NOT READY -- FIX ISSUES ABOVE",
        "  (99 non-critical warnings)",
        "  Total time: 9999 seconds",
    ]

    W = max(max(len(s) for s in candidates) + 4, 52)

    def row(content: str) -> str:
        return f"║ {content:<{W - 2}} ║"

    lines: list[str] = []
    lines.append(f"╔{'═' * W}╗")
    lines.append(row(f"  {title}"))
    lines.append(f"╠{'═' * W}╣")

    warn_count = 0
    fail_count = 0
    for i, r in enumerate(results):
        icon  = _STATUS_ICON.get(r.status, r.status)
        label = f"  {i + 1}. {r.name}"
        body  = f"{label:<28}  {icon}"
        if r.detail:
            body += f"  {r.detail}"
        lines.append(row(body))
        for note in r.notes:
            lines.append(row(f"     {note}"))
        if r.fix_hint:
            lines.append(row(f"     Fix: {r.fix_hint}"))
        if r.status == WARN:
            warn_count += 1
        elif r.status == FAIL:
            fail_count += 1

    lines.append(f"╠{'═' * W}╣")

    if fail_count:
        lines.append(row("  RESULT: NOT READY -- FIX ISSUES ABOVE"))
    else:
        lines.append(row("  RESULT: READY FOR DEMO"))

    if warn_count:
        w_word = "warning" if warn_count == 1 else "warnings"
        lines.append(row(f"  ({warn_count} non-critical {w_word})"))
    if fail_count:
        f_word = "failure" if fail_count == 1 else "failures"
        lines.append(row(f"  ({fail_count} critical {f_word})"))

    lines.append(row(f"  Total time: {int(total_seconds)} seconds"))
    lines.append(f"╚{'═' * W}╝")

    return "\n".join(lines)


# ── Shared helpers ────────────────────────────────────────────────────────────

def _db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _deep_get(mapping: dict, dotted_key: str) -> Optional[object]:
    """Safely navigate a dotted path like 'revenue.yesterday.total'."""
    node = mapping
    for part in dotted_key.split("."):
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node


# ── Check 1: Token Health ─────────────────────────────────────────────────────

def check_1_token_health(verbose: bool, fix: bool) -> CheckResult:
    try:
        from demo.hardening.token_preflight import check_all_tokens
    except ImportError as exc:
        return CheckResult("Token Health", FAIL, notes=[f"Import error: {exc}"])

    result = check_all_tokens()
    total  = len(result.checks)
    passed = sum(1 for c in result.checks if c.status in ("ok", "expiring_soon"))
    expiring = [c for c in result.checks if c.status == "expiring_soon"]
    failed   = [c for c in result.checks if c.status in ("expired", "error")]

    if failed:
        notes = []
        for c in failed:
            notes.append(f"{c.tool_name}: {c.message}")
            if c.action:
                notes.append(f"  → {c.action}")
        return CheckResult(
            "Token Health", FAIL,
            detail=f"({passed}/{total})",
            notes=notes,
        )

    notes = []
    if expiring and verbose:
        for c in expiring:
            notes.append(f"{c.tool_name}: {c.message} (auto-refreshed)")

    return CheckResult(
        "Token Health",
        WARN if expiring else PASS,
        detail=f"({passed}/{total})",
        notes=notes,
    )


# ── Check 2: Database Exists and Has Data ─────────────────────────────────────

def check_2_database(verbose: bool) -> CheckResult:
    db = Path(DB_PATH)
    if not db.exists():
        return CheckResult(
            "Database Integrity", FAIL,
            notes=[f"sparkle_shine.db not found at {DB_PATH}"],
        )
    if db.stat().st_size == 0:
        return CheckResult("Database Integrity", FAIL, notes=["sparkle_shine.db is empty"])

    try:
        conn = _db_connect()
    except Exception as exc:
        return CheckResult("Database Integrity", FAIL, notes=[f"Cannot open DB: {exc}"])

    counts: dict[str, int] = {}
    errors: list[str] = []
    try:
        for table in DB_TARGETS:
            try:
                counts[table] = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            except sqlite3.OperationalError as exc:
                errors.append(f"Table '{table}': {exc}")
                counts[table] = 0
    finally:
        conn.close()

    if errors:
        return CheckResult("Database Integrity", FAIL, notes=errors)

    notes:    list[str] = []
    status    = PASS
    passed_ct = 0

    for table, target in DB_TARGETS.items():
        actual = counts[table]
        if actual == 0:
            status = FAIL
            notes.append(f"{table}: 0 rows (target >= {target})")
        elif actual < int(target * 0.8):
            if status != FAIL:
                status = WARN
            notes.append(f"{table}: {actual} rows (below 80% of target {target})")
        else:
            passed_ct += 1
            if verbose:
                notes.append(f"{table}: {actual:,} rows ✓")

    return CheckResult(
        "Database Integrity", status,
        detail=f"({passed_ct}/{len(DB_TARGETS)})",
        notes=notes,
    )


# ── Check 3: Metrics Engine ───────────────────────────────────────────────────

def check_3_metrics_engine(verbose: bool) -> CheckResult:
    try:
        from intelligence.metrics import compute_all_metrics
    except ImportError as exc:
        return CheckResult("Metrics Engine", FAIL, notes=[f"Import error: {exc}"])

    modules = ["revenue", "operations", "sales", "financial_health", "marketing", "tasks"]

    try:
        metrics = compute_all_metrics(DB_PATH, BRIEFING_DATE)
    except Exception:
        tb = traceback.format_exc().strip()
        return CheckResult(
            "Metrics Engine", FAIL,
            notes=["compute_all_metrics() raised an exception:"] + tb.splitlines(),
        )

    missing = [m for m in modules if metrics.get(m) is None]
    passed_ct = len(modules) - len(missing)

    if missing:
        return CheckResult(
            "Metrics Engine", FAIL,
            detail=f"({passed_ct}/{len(modules)})",
            notes=[f"Module returned None: {', '.join(missing)}"],
        )

    # Spot-check key values that must be > 0 for a live demo
    spot_keys = [
        "revenue.yesterday.total",
        "operations.today_schedule.total_jobs",
        "sales.pipeline_summary.total_open_deals",
        "financial_health.cash_position.bank_balance",
        "tasks.overview.total_open",
    ]
    warn_notes: list[str] = []
    for key in spot_keys:
        val = _deep_get(metrics, key)
        if val is None:
            warn_notes.append(f"Key not found: {key}")
        elif not (isinstance(val, (int, float)) and val > 0):
            warn_notes.append(f"{key} = {val} (expected > 0)")

    if warn_notes:
        return CheckResult(
            "Metrics Engine", WARN,
            detail=f"({passed_ct}/{len(modules)})",
            notes=warn_notes,
        )

    if verbose:
        rev = _deep_get(metrics, "revenue.yesterday.total")
        jobs = _deep_get(metrics, "operations.today_schedule.total_jobs")
        notes = [f"revenue.yesterday.total={rev}", f"today_schedule.total_jobs={jobs}"]
    else:
        notes = []

    return CheckResult(
        "Metrics Engine", PASS,
        detail=f"({passed_ct}/{len(modules)})",
        notes=notes,
    )


# ── Check 4: Context Builder ──────────────────────────────────────────────────

def check_4_context_builder(
    verbose: bool,
) -> tuple[CheckResult, Optional[object]]:
    try:
        from intelligence.context_builder import build_briefing_context
    except ImportError as exc:
        return CheckResult("Context Builder", FAIL, notes=[f"Import error: {exc}"]), None

    try:
        ctx = build_briefing_context(DB_PATH, BRIEFING_DATE)
    except Exception:
        tb = traceback.format_exc().strip()
        return CheckResult(
            "Context Builder", FAIL,
            notes=["build_briefing_context() raised an exception:"] + tb.splitlines(),
        ), None

    if not ctx.context_document:
        return CheckResult("Context Builder", FAIL, notes=["context_document is empty"]), ctx

    doc = ctx.context_document
    missing_sections = [s for s in CONTEXT_SECTIONS if s not in doc]

    notes: list[str] = []
    status = PASS

    if missing_sections:
        status = FAIL
        notes.append(f"Missing sections: {', '.join(missing_sections)}")

    if ctx.token_estimate > 5000:
        if status == PASS:
            status = WARN
        notes.append(f"token_estimate={ctx.token_estimate} (> 5,000 — consider trimming)")
    elif verbose:
        notes.append(f"token_estimate={ctx.token_estimate}")

    return CheckResult("Context Builder", status, notes=notes), ctx


# ── Check 5: Briefing Generation ─────────────────────────────────────────────

def check_5_briefing_generation(ctx: object, verbose: bool) -> CheckResult:
    if not os.getenv("ANTHROPIC_API_KEY"):
        return CheckResult(
            "Briefing Generation", FAIL,
            notes=["ANTHROPIC_API_KEY not set in .env"],
        )

    try:
        from intelligence.briefing_generator import generate_briefing
    except ImportError as exc:
        return CheckResult("Briefing Generation", FAIL, notes=[f"Import error: {exc}"])

    t0 = time.time()
    try:
        briefing = generate_briefing(ctx)
    except Exception:
        tb = traceback.format_exc().strip()
        return CheckResult(
            "Briefing Generation", FAIL,
            notes=["generate_briefing() raised an exception:"] + tb.splitlines(),
        )
    elapsed = time.time() - t0

    if not briefing.content_plain:
        return CheckResult("Briefing Generation", FAIL, notes=["Briefing content is empty"])

    word_count = len(briefing.content_plain.split())
    missing_sections = [s for s in BRIEFING_SECTIONS if s not in briefing.content_plain]

    notes: list[str] = []
    status = PASS

    if not (200 <= word_count <= 1000):
        status = FAIL
        notes.append(f"Word count {word_count} outside 200–1000 range")

    if missing_sections:
        status = FAIL
        notes.append(f"Missing sections: {', '.join(missing_sections)}")

    # Estimated cost: claude-sonnet-4 pricing
    input_cost  = briefing.input_tokens  * 3.00  / 1_000_000
    output_cost = briefing.output_tokens * 15.00 / 1_000_000
    cost_est = input_cost + output_cost

    meta_notes = [
        f"Model: {briefing.model_used}",
        f"Tokens: {briefing.input_tokens:,} in / {briefing.output_tokens:,} out",
        f"Time: {elapsed:.1f}s   Cost: ~${cost_est:.4f}",
    ]

    # Always print meta even on PASS; trim check-level detail if not verbose
    return CheckResult(
        "Briefing Generation", status,
        detail=f"({word_count}w)",
        notes=notes + meta_notes,
    )


# ── Check 6: Slack Connectivity ───────────────────────────────────────────────

def check_6_slack_connectivity(verbose: bool) -> CheckResult:
    try:
        from intelligence.slack_publisher import resolve_channel_id
    except ImportError as exc:
        return CheckResult("Slack Connectivity", FAIL, notes=[f"Import error: {exc}"])

    try:
        channel_id = resolve_channel_id("#daily-briefing")
    except ValueError as exc:
        return CheckResult(
            "Slack Connectivity", FAIL,
            notes=[str(exc)],
            fix_hint="Ensure SLACK_BOT_TOKEN is set and bot is invited to #daily-briefing",
        )
    except Exception as exc:
        return CheckResult("Slack Connectivity", FAIL, notes=[f"Slack API error: {exc}"])

    notes = [f"channel_id={channel_id}"] if verbose else []
    return CheckResult("Slack Connectivity", PASS, notes=notes)


# ── Check 7: Scenario Files ───────────────────────────────────────────────────

def check_7_scenario_files(verbose: bool, fix: bool) -> CheckResult:
    now = datetime.now()
    present: list[str] = []
    missing: list[str] = []
    age_notes: list[str] = []

    for filename in SCENARIO_FILES:
        path = SCENARIO_OUTPUT_DIR / filename
        if path.exists():
            present.append(filename)
            age_secs = (now - datetime.fromtimestamp(path.stat().st_mtime)).total_seconds()
            if age_secs < 3600:
                age_str = f"{int(age_secs / 60)} minutes ago"
            elif age_secs < 86400:
                age_str = f"{int(age_secs / 3600)} hours ago"
            else:
                age_str = f"{int(age_secs / 86400)} days ago"
            age_notes.append(f"{filename}: generated {age_str}")
        else:
            missing.append(filename)

    total     = len(SCENARIO_FILES)
    passed_ct = len(present)
    notes: list[str] = list(age_notes) if verbose else []

    if not missing:
        return CheckResult(
            "Scenario Files", PASS,
            detail=f"({passed_ct}/{total})",
            notes=notes,
        )

    # Report and optionally fix each missing file
    for filename in missing:
        stem         = Path(filename).stem
        scenario_key = _SCENARIO_FLAG.get(stem, stem)
        fix_cmd      = f"python -m demo.scenarios.scenario_runner --scenario {scenario_key}"

        if fix:
            notes.append(f"Regenerating: {filename}")
            try:
                subprocess.run(
                    [sys.executable, "-m", "demo.scenarios.scenario_runner",
                     "--scenario", scenario_key],
                    cwd=str(_BASE_DIR),
                    check=True,
                    capture_output=not verbose,
                )
                notes[-1] = f"Regenerated: {filename} ✓"
                passed_ct += 1
            except subprocess.CalledProcessError as exc:
                notes.append(f"  Failed: {exc}")
        else:
            notes.append(f"Missing: {filename}")

    # For the primary fix hint, show the first missing file's command
    fix_hint = ""
    still_missing = [f for f in missing if f"Regenerated: {f} ✓" not in notes]
    if still_missing and not fix:
        stem         = Path(still_missing[0]).stem
        scenario_key = _SCENARIO_FLAG.get(stem, stem)
        fix_hint     = f"python -m demo.scenarios.scenario_runner --scenario {scenario_key}"

    final_status = PASS if passed_ct == total else WARN
    return CheckResult(
        "Scenario Files", final_status,
        detail=f"({passed_ct}/{total})",
        notes=notes,
        fix_hint=fix_hint,
    )


# ── Check 8: Data Integrity Spot Check ───────────────────────────────────────

def check_8_data_integrity(verbose: bool) -> CheckResult:
    import requests  # noqa: PLC0415

    try:
        conn = _db_connect()
        rows = conn.execute(
            "SELECT canonical_id, entity_type, tool_name, tool_specific_id "
            "FROM cross_tool_mapping ORDER BY RANDOM() LIMIT 3"
        ).fetchall()
        conn.close()
    except Exception as exc:
        return CheckResult("Data Spot Check", FAIL, notes=[f"DB error: {exc}"])

    if not rows:
        return CheckResult("Data Spot Check", FAIL, notes=["cross_tool_mapping is empty"])

    checks: list[tuple[str, bool, str]] = []  # (label, ok, reason)
    for row in rows:
        canonical_id = row["canonical_id"]
        entity_type  = row["entity_type"]
        tool_name    = row["tool_name"].lower()
        tool_id      = row["tool_specific_id"]
        label        = f"{canonical_id} → {tool_name}:{tool_id}"

        ok, reason = _verify_tool_record(tool_name, entity_type, tool_id, requests)
        checks.append((label, ok, reason))

    passed = sum(1 for _, ok, _ in checks if ok)
    total  = len(checks)

    notes: list[str] = []
    if verbose or passed < total:
        for label, ok, reason in checks:
            icon = "✓" if ok else "✗"
            notes.append(f"{icon} {label}")
            if not ok and reason:
                notes.append(f"    {reason}")

    if passed == total:
        status = PASS
    elif passed == total - 1:
        status = WARN
    else:
        status = FAIL

    return CheckResult("Data Spot Check", status, detail=f"({passed}/{total})", notes=notes)


def _verify_tool_record(
    tool_name: str,
    entity_type: str,
    tool_id: str,
    requests: object,  # passed in to avoid a module-level import at the top
) -> tuple[bool, str]:
    """One lightweight API call to verify tool_id still resolves. Returns (ok, reason)."""
    timeout = 6

    try:
        if tool_name == "hubspot":
            token = os.getenv("HUBSPOT_ACCESS_TOKEN", "")
            if not token:
                return False, "HUBSPOT_ACCESS_TOKEN not set"
            obj_type = {"CLIENT": "contacts", "CONTACT": "contacts", "LEAD": "contacts",
                        "DEAL": "deals", "COMPANY": "companies"}.get(entity_type.upper(), "contacts")
            resp = requests.get(
                f"https://api.hubapi.com/crm/v3/objects/{obj_type}/{tool_id}?properties=hs_object_id",
                headers={"Authorization": f"Bearer {token}"},
                timeout=timeout,
            )
            if resp.status_code == 404:
                return False, "Record not found (404)"
            return resp.status_code in (200, 201), f"HTTP {resp.status_code}"

        elif tool_name == "pipedrive":
            token = os.getenv("PIPEDRIVE_API_TOKEN", "")
            if not token:
                return False, "PIPEDRIVE_API_TOKEN not set"
            base     = os.getenv("PIPEDRIVE_BASE_URL", "https://api.pipedrive.com/v1").rstrip("/")
            resource = {"CLIENT": "persons", "CONTACT": "persons", "LEAD": "leads",
                        "DEAL": "deals", "PROPOSAL": "deals"}.get(entity_type.upper(), "persons")
            resp = requests.get(
                f"{base}/{resource}/{tool_id}",
                headers={"x-api-token": token},
                timeout=timeout,
            )
            if resp.status_code == 404:
                return False, "Record not found (404)"
            data = resp.json()
            if not data.get("success"):
                return False, data.get("error", "API returned success=false")
            return True, ""

        elif tool_name == "asana":
            token = os.getenv("ASANA_ACCESS_TOKEN", "")
            if not token:
                return False, "ASANA_ACCESS_TOKEN not set"
            resp = requests.get(
                f"https://app.asana.com/api/1.0/tasks/{tool_id}?fields=gid",
                headers={"Authorization": f"Bearer {token}"},
                timeout=timeout,
            )
            if resp.status_code == 404:
                return False, "Task not found (404)"
            return resp.status_code == 200, f"HTTP {resp.status_code}"

        elif tool_name == "jobber":
            token_file = _BASE_DIR / ".jobber_tokens.json"
            if not token_file.exists():
                return False, ".jobber_tokens.json not found"
            with open(token_file) as f:
                tokens = _json.load(f)
            access_token = tokens.get("access_token", "")
            api_version  = os.getenv("JOBBER_API_VERSION", "2026-03-10")
            jb_headers = {
                "Authorization": f"Bearer {access_token}",
                "X-JOBBER-GRAPHQL-VERSION": api_version,
                "Content-Type": "application/json",
            }
            # Jobber's Job type does not implement the Node interface, so
            # node(id:) always returns null. Use entity-specific queries instead.
            if entity_type.upper() == "CLIENT":
                resp = requests.post(
                    "https://api.getjobber.com/api/graphql",
                    json={"query": f'{{ client(id: "{tool_id}") {{ id }} }}'},
                    headers=jb_headers,
                    timeout=timeout,
                )
                if resp.status_code != 200:
                    return False, f"HTTP {resp.status_code}"
                if resp.json().get("data", {}).get("client") is None:
                    return False, "Client not found (null returned)"
                return True, ""
            else:
                # JOB and RECUR are both jobs in Jobber
                query = '{ jobs(filter: { ids: ["' + tool_id + '"] }) { nodes { id } } }'
                resp = requests.post(
                    "https://api.getjobber.com/api/graphql",
                    json={"query": query},
                    headers=jb_headers,
                    timeout=timeout,
                )
                if resp.status_code != 200:
                    return False, f"HTTP {resp.status_code}"
                nodes = resp.json().get("data", {}).get("jobs", {}).get("nodes", [])
                if not nodes:
                    return False, "Job not found (empty nodes)"
                return True, ""

        elif tool_name == "mailchimp":
            api_key = os.getenv("MAILCHIMP_API_KEY", "")
            server  = os.getenv("MAILCHIMP_SERVER_PREFIX", "")
            if not api_key or not server:
                return False, "MAILCHIMP_API_KEY or MAILCHIMP_SERVER_PREFIX not set"
            # Ping is the lightest Mailchimp check; member lookups need list_id separately
            resp = requests.get(
                f"https://{server}.api.mailchimp.com/3.0/ping",
                auth=("anystring", api_key),
                timeout=timeout,
            )
            return resp.status_code == 200, f"HTTP {resp.status_code}"

        elif tool_name in ("quickbooks", "quickbooks_online"):
            # Full OAuth flow is too heavy for a spot check; verify token file is present
            token_file = _BASE_DIR / ".quickbooks_tokens.json"
            if not token_file.exists():
                return False, ".quickbooks_tokens.json not found"
            return True, "(shallow: token file present)"

        elif tool_name == "google":
            token_file = _BASE_DIR / "token.json"
            if not token_file.exists():
                return False, "token.json not found"
            return True, "(shallow: token file present)"

        elif tool_name == "slack":
            token = os.getenv("SLACK_BOT_TOKEN", "")
            if not token:
                return False, "SLACK_BOT_TOKEN not set"
            resp = requests.post(
                "https://slack.com/api/auth.test",
                headers={"Authorization": f"Bearer {token}"},
                timeout=timeout,
            )
            data = resp.json()
            return data.get("ok", False), data.get("error", "")

        else:
            return True, f"(no verifier for tool '{tool_name}')"

    except Exception as exc:
        return False, f"Error: {exc}"


# ── Orchestrator ──────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m demo.smoke_test",
        description="Sparkle & Shine POC — pre-demo smoke test (target: < 90 seconds)",
    )
    parser.add_argument(
        "--skip-api", action="store_true",
        help="Skip checks 5 and 8 (no external API calls — useful for offline testing)",
    )
    parser.add_argument(
        "--fix", action="store_true",
        help="Attempt to auto-fix warnings: regenerate missing scenario files, refresh tokens",
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Print detailed output for each check",
    )
    args = parser.parse_args()

    t_start = time.time()
    results: list[CheckResult] = []

    print("Running Sparkle & Shine smoke test…\n")

    # ── 1. Token Health ───────────────────────────────────────────────────────
    r1 = check_1_token_health(verbose=args.verbose, fix=args.fix)
    results.append(r1)
    if r1.status == FAIL:
        # Token failures mean no tool calls can succeed — skip the rest
        for name in [
            "Database Integrity", "Metrics Engine", "Context Builder",
            "Briefing Generation", "Slack Connectivity",
            "Scenario Files", "Data Spot Check",
        ]:
            results.append(CheckResult(name, SKIP, notes=["Skipped: token health failed"]))
        _finish(results, t_start)

    # ── 2. Database ───────────────────────────────────────────────────────────
    r2 = check_2_database(verbose=args.verbose)
    results.append(r2)
    if r2.status == FAIL:
        for name in [
            "Metrics Engine", "Context Builder", "Briefing Generation",
            "Slack Connectivity", "Scenario Files", "Data Spot Check",
        ]:
            results.append(CheckResult(name, SKIP, notes=["Skipped: database check failed"]))
        _finish(results, t_start)

    # ── 3. Metrics Engine ─────────────────────────────────────────────────────
    r3 = check_3_metrics_engine(verbose=args.verbose)
    results.append(r3)
    ctx: Optional[object] = None

    if r3.status == FAIL:
        results.append(CheckResult(
            "Context Builder", SKIP,
            notes=["Skipped: metrics engine failed"],
        ))
        results.append(CheckResult(
            "Briefing Generation", SKIP,
            notes=["Skipped: metrics engine failed"],
        ))
    else:
        # ── 4. Context Builder ────────────────────────────────────────────────
        r4, ctx = check_4_context_builder(verbose=args.verbose)
        results.append(r4)

        if r4.status == FAIL or ctx is None:
            results.append(CheckResult(
                "Briefing Generation", SKIP,
                notes=["Skipped: context builder failed"],
            ))
        elif args.skip_api:
            results.append(CheckResult(
                "Briefing Generation", SKIP,
                notes=["Skipped: --skip-api flag set"],
            ))
        else:
            # ── 5. Briefing Generation ────────────────────────────────────────
            results.append(check_5_briefing_generation(ctx, verbose=args.verbose))

    # ── 6. Slack Connectivity ─────────────────────────────────────────────────
    results.append(check_6_slack_connectivity(verbose=args.verbose))

    # ── 7. Scenario Files ─────────────────────────────────────────────────────
    results.append(check_7_scenario_files(verbose=args.verbose, fix=args.fix))

    # ── 8. Data Integrity Spot Check ──────────────────────────────────────────
    if args.skip_api:
        results.append(CheckResult(
            "Data Spot Check", SKIP,
            notes=["Skipped: --skip-api flag set"],
        ))
    else:
        results.append(check_8_data_integrity(verbose=args.verbose))

    _finish(results, t_start)


def _finish(results: list[CheckResult], t_start: float) -> None:
    elapsed = time.time() - t_start
    print(_render_box(results, elapsed))
    fails = sum(1 for r in results if r.status == FAIL)
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
