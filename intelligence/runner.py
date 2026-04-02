"""
intelligence/runner.py

Master orchestrator for the Sparkle & Shine intelligence pipeline.

Supports two report types:
    daily  (default) -- tactical ops report: yesterday + today + top 3 actions
    weekly           -- strategic review covering the past 7 days with TL;DR

Stages:
    0. Preflight -- verify all tokens are valid before syncing (optional)
    1. Sync    -- pull fresh data from all 8 tools into SQLite
    2. Metrics -- compute all 6 metric modules
    3. Context -- assemble the context document for Claude
    4. Generate -- call the Anthropic API to produce the report
    5. Publish -- post to Slack; route critical alerts to the right channels
    6. Archive -- save report and context to briefings/

Usage:
    python -m intelligence.runner
    python -m intelligence.runner --date 2026-03-17
    python -m intelligence.runner --skip-sync --date 2026-03-17
    python -m intelligence.runner --skip-sync --date 2026-03-17 --dry-run
    python -m intelligence.runner --skip-sync --date 2026-03-17 --report-type weekly
    python -m intelligence.runner --sync-only --verbose
    python -m intelligence.runner --preflight

# ── Cron scheduling ──────────────────────────────────────────────────────────
# Daily tactical report at 7:00 AM (Mon–Sun):
# 0 7 * * * cd /path/to/sparkle-shine-poc && /path/to/python -m intelligence.runner --skip-sync >> logs/cron.log 2>&1
#
# Weekly strategic report at 7:30 AM every Monday:
# 30 7 * * 1 cd /path/to/sparkle-shine-poc && /path/to/python -m intelligence.runner --skip-sync --report-type weekly >> logs/cron.log 2>&1
#
# For the POC demo, run manually:
# python -m intelligence.runner --skip-sync --date 2026-03-17
# python -m intelligence.runner --skip-sync --date 2026-03-17 --report-type weekly
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from dataclasses import dataclass, field
from datetime import date, timedelta

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))

from intelligence.logging_config import setup_logging

logger = setup_logging(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "sparkle_shine.db")
BRIEFINGS_DIR = os.path.join(PROJECT_ROOT, "briefings")

# Syncer definitions: (module_attr, display_name)
_SYNCERS: list[tuple[str, str]] = [
    ("GoogleSyncer",      "google"),
    ("JobberSyncer",      "jobber"),
    ("QuickBooksSyncer",  "quickbooks"),
    ("HubSpotSyncer",     "hubspot"),
    ("PipedriveSyncer",   "pipedrive"),
    ("MailchimpSyncer",   "mailchimp"),
    ("AsanaSyncer",       "asana"),
    ("SlackSyncer",       "slack"),
]

# Alert source -> Slack channel routing
_ALERT_CHANNEL_MAP: dict[str, str] = {
    "revenue":          "#operations",
    "financial_health": "#operations",
    "operations":       "#operations",
    "tasks":            "#operations",
    "sales":            "#sales",
    "marketing":        "#sales",
}

# Cost estimate: claude-sonnet-4 pricing per million tokens (approximate)
_COST_PER_M_INPUT  = 3.00
_COST_PER_M_OUTPUT = 15.00


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def _resolve_date(date_arg: str) -> str:
    today = date.today()
    if date_arg == "today":
        return str(today)
    if date_arg == "yesterday":
        return str(today - timedelta(days=1))
    return date_arg


def _fmt_date_long(iso_date: str) -> str:
    """Format ISO date as 'Tuesday, March 17, 2026'."""
    d = date.fromisoformat(iso_date)
    return d.strftime("%A, %B ") + str(d.day) + d.strftime(", %Y")


# ---------------------------------------------------------------------------
# Pipeline result accumulator
# ---------------------------------------------------------------------------

@dataclass
class PipelineResult:
    briefing_date: str = ""
    sync_total: int = len(_SYNCERS)
    sync_succeeded: int = 0
    sync_skipped: int = 0
    sync_errors: list[str] = field(default_factory=list)      # ["mailchimp: 401 Unauthorized"]
    metrics_total: int = 6
    metrics_computed: int = 0
    alert_warnings: int = 0
    alert_criticals: int = 0
    briefing_words: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    published_channel: str = ""
    published_at: str = ""
    duration_seconds: float = 0.0
    archived: bool = False


# ---------------------------------------------------------------------------
# Stage 0: Token preflight helpers
# ---------------------------------------------------------------------------

_SYNCER_TIMEOUT_SECONDS = 600  # 10 min; Asana full sync needs ~520s at 2.2 req/s
_OAUTH_TOOLS = {"jobber", "quickbooks", "google"}
_PERMANENT_TOKEN_TOOLS = {"hubspot", "pipedrive", "asana", "mailchimp", "slack"}


def _run_token_preflight() -> set[str]:
    """
    Run full token preflight. Returns a set of tool names to skip during sync.

    - OAuth tools that are "expired" or "error": add to skip_set.
    - If ALL permanent-token tools fail: abort (likely network issue).
    - Permanent-token tools that fail are also added to skip_set.
    """
    from demo.hardening.token_preflight import check_all_tokens, print_preflight_table

    logger.info("--- Stage 0: TOKEN PREFLIGHT ---")
    preflight_result = check_all_tokens()
    print_preflight_table(preflight_result)

    skip_set: set[str] = set()

    failed_permanent: set[str] = set()
    for check in preflight_result.checks:
        tool_lower = check.tool_name.lower()
        if check.status in ("expired", "error"):
            if tool_lower in _OAUTH_TOOLS:
                logger.critical(
                    "OAuth token for %s is %s: %s -- skipping syncer",
                    check.tool_name, check.status, check.message,
                )
            skip_set.add(tool_lower)
            if tool_lower in _PERMANENT_TOKEN_TOOLS:
                failed_permanent.add(tool_lower)

    if failed_permanent == _PERMANENT_TOKEN_TOOLS:
        logger.critical(
            "All permanent-token tools failed preflight (%s). "
            "This indicates a network issue. Aborting pipeline.",
            ", ".join(sorted(failed_permanent)),
        )
        sys.exit(1)

    if skip_set:
        logger.warning("Skipping syncers due to preflight failures: %s", sorted(skip_set))
    else:
        logger.info("Preflight passed. All tokens are valid.")

    return skip_set


# ---------------------------------------------------------------------------
# Stage 1: Sync
# ---------------------------------------------------------------------------

def _run_syncer_with_timeout(syncer_cls, db_path: str) -> object:
    """Run a single syncer in a worker thread with a 120s timeout."""
    def _do():
        with syncer_cls(db_path) as syncer:
            return syncer.sync()

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(_do)
        return future.result(timeout=_SYNCER_TIMEOUT_SECONDS)


def _run_syncers(db_path: str, skip_set: set[str] | None = None) -> tuple[int, list[str]]:
    """
    Run all 8 syncers in dependency order.
    Returns (succeeded_count, error_list).
    Each error entry is "{tool_name}: {error_message}".

    skip_set: tool names (lowercase) to skip, e.g. from preflight failures.
    """
    from intelligence.syncers.sync_google     import GoogleSyncer
    from intelligence.syncers.sync_jobber     import JobberSyncer
    from intelligence.syncers.sync_quickbooks import QuickBooksSyncer
    from intelligence.syncers.sync_hubspot    import HubSpotSyncer
    from intelligence.syncers.sync_pipedrive  import PipedriveSyncer
    from intelligence.syncers.sync_mailchimp  import MailchimpSyncer
    from intelligence.syncers.sync_asana      import AsanaSyncer
    from intelligence.syncers.sync_slack      import SlackSyncer

    syncer_classes = [
        GoogleSyncer,
        JobberSyncer,
        QuickBooksSyncer,
        HubSpotSyncer,
        PipedriveSyncer,
        MailchimpSyncer,
        AsanaSyncer,
        SlackSyncer,
    ]

    _skip = skip_set or set()
    succeeded = 0
    errors: list[str] = []

    for syncer_cls in syncer_classes:
        tool_name = getattr(syncer_cls, "tool_name", syncer_cls.__name__)
        if tool_name.lower() in _skip:
            logger.info("Skipping %s syncer (preflight failure)", tool_name)
            errors.append(f"{tool_name}: skipped (preflight failure)")
            continue

        try:
            logger.info("Syncing %s ...", tool_name)
            sync_result = _run_syncer_with_timeout(syncer_cls, db_path)
            logger.info(
                "Synced %s: %d records in %.1fs",
                tool_name,
                sync_result.records_synced,
                sync_result.duration_seconds,
            )
            if sync_result.errors:
                for err in sync_result.errors:
                    logger.warning("Non-fatal error in %s syncer: %s", tool_name, err)
            succeeded += 1
        except FuturesTimeoutError:
            logger.error(
                "Syncer %s timed out after %ds -- skipping",
                tool_name, _SYNCER_TIMEOUT_SECONDS,
            )
            errors.append(f"{tool_name}: timed out after {_SYNCER_TIMEOUT_SECONDS} seconds")
        except Exception as exc:
            error_msg = str(exc)
            logger.error("Syncer failed for %s: %s", tool_name, error_msg)
            errors.append(f"{tool_name}: {error_msg}")

    total = len(syncer_classes)
    error_summary = ", ".join(errors) if errors else "none"
    logger.info(
        "Sync complete: %d/%d tools synced, %d error(s) (%s)",
        succeeded,
        total,
        len(errors),
        error_summary,
    )
    if errors:
        print(
            f"Sync complete: {succeeded}/{total} tools synced, "
            f"{len(errors)} error ({''.join(errors[:1])})"
        )
    else:
        print(f"Sync complete: {succeeded}/{total} tools synced, 0 errors")

    return succeeded, errors


# ---------------------------------------------------------------------------
# Stage 5 helper: classify and route alerts
# ---------------------------------------------------------------------------

def _is_critical(alert_text: str) -> bool:
    """Heuristic: alert is critical if its text contains 'critical' (case-insensitive)."""
    return "critical" in alert_text.lower()


def _collect_alerts(metrics: dict) -> list[dict]:
    """Collect all alerts from every metrics module."""
    all_alerts: list[dict] = []
    for module_name, module_result in metrics.items():
        if isinstance(module_result, dict) and "alerts" in module_result:
            for alert_text in module_result["alerts"]:
                all_alerts.append({"source": module_name, "text": alert_text})
    return all_alerts


def _count_alert_levels(all_alerts: list[dict]) -> tuple[int, int]:
    """Return (warning_count, critical_count)."""
    criticals = sum(1 for a in all_alerts if _is_critical(a["text"]))
    warnings  = len(all_alerts) - criticals
    return warnings, criticals


def _post_critical_alerts(all_alerts: list[dict]) -> None:
    """Post critical-level alerts to their appropriate Slack channels."""
    from intelligence.slack_publisher import post_alert

    critical_alerts = [a for a in all_alerts if _is_critical(a["text"])]
    for alert in critical_alerts:
        channel = _ALERT_CHANNEL_MAP.get(alert["source"], "#operations")
        logger.info(
            "Posting critical alert [%s] to %s: %s",
            alert["source"],
            channel,
            alert["text"][:80],
        )
        ok = post_alert(alert["text"], channel=channel, urgency="critical")
        if not ok:
            logger.error(
                "Failed to post critical alert to %s: %s",
                channel,
                alert["text"][:80],
            )


# ---------------------------------------------------------------------------
# Stage 6: Archive
# ---------------------------------------------------------------------------

def _archive(
    briefing_date: str,
    briefing_content: str,
    context_document: str,
    report_type: str = "daily",
) -> None:
    """Save report and context documents to briefings/."""
    os.makedirs(BRIEFINGS_DIR, exist_ok=True)

    prefix = "weekly_report" if report_type == "weekly" else "daily_report"
    briefing_path = os.path.join(BRIEFINGS_DIR, f"{prefix}_{briefing_date}.md")
    context_path  = os.path.join(BRIEFINGS_DIR, f"context_{prefix}_{briefing_date}.md")

    with open(briefing_path, "w", encoding="utf-8") as f:
        f.write(briefing_content)

    with open(context_path, "w", encoding="utf-8") as f:
        f.write(context_document)

    logger.info("Report archived to %s", briefing_path)
    logger.info("Context archived to %s", context_path)


# ---------------------------------------------------------------------------
# Summary printer
# ---------------------------------------------------------------------------

def _print_summary(result: PipelineResult) -> None:
    minutes   = int(result.duration_seconds) // 60
    seconds   = int(result.duration_seconds) % 60
    duration  = f"{minutes}m {seconds:02d}s"

    total_tokens = result.input_tokens + result.output_tokens
    cost_usd = (
        result.input_tokens  / 1_000_000 * _COST_PER_M_INPUT
        + result.output_tokens / 1_000_000 * _COST_PER_M_OUTPUT
    )

    sync_line = (
        f"{result.sync_succeeded}/{result.sync_total} tools "
        f"(skipped: {result.sync_skipped}, "
        f"errors: {len(result.sync_errors)})"
    )

    alert_line = (
        f"{result.alert_warnings} warning{'s' if result.alert_warnings != 1 else ''}, "
        f"{result.alert_criticals} critical"
    )

    token_line = (
        f"{result.briefing_words} words, "
        f"{result.input_tokens}+{result.output_tokens} tokens"
    )

    published_line = (
        f"{result.published_channel} at {result.published_at}"
        if result.published_channel
        else "not published"
    )

    print("")
    print("==========================================")
    print("  Intelligence Pipeline Complete")
    print("==========================================")
    print(f"  Date:        {_fmt_date_long(result.briefing_date)}")
    print(f"  Sync:        {sync_line}")
    print(f"  Metrics:     {result.metrics_computed}/{result.metrics_total} modules computed")
    print(f"  Alerts:      {alert_line}")
    print(f"  Briefing:    {token_line}")
    print(f"  Published:   {published_line}")
    print(f"  Cost:        ~${cost_usd:.3f}")
    print(f"  Duration:    {duration} total")
    print("==========================================")


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

def _run_health_check() -> None:
    """Run intelligence pipeline health checks and exit.

    Answers: 'Can the intelligence pipeline produce a briefing right now?'
    Called by --health before any pipeline stage runs.
    Exits 0 if all checks PASS or WARN, exits 1 if any FAIL.
    """
    import importlib

    from database.health import (
        HealthCheck,
        check_connection,
        check_table_inventory,
        check_sequences,
        check_oauth_tokens,
        render_table,
    )

    checks: list[HealthCheck] = []

    _TABLES = [
        "daily_metrics_snapshot", "document_index",
        "jobs", "clients", "invoices",
    ]

    # 1. DB connection
    conn_check, conn = check_connection()
    checks.append(conn_check)

    if conn is None:
        for name in ("Table inventory", "Sequence health", "OAuth tokens"):
            checks.append(HealthCheck(name, "SKIP", "DB unreachable"))
    else:
        try:
            # 2. Table inventory
            checks.extend(check_table_inventory(conn, _TABLES))
            # 3. Sequence health (document_index has SERIAL PK; others use TEXT)
            checks.extend(check_sequences(conn, _TABLES))
            # 4. OAuth tokens (jobber, quickbooks, google)
            checks.extend(check_oauth_tokens(conn))
        finally:
            conn.close()

    # 5. ANTHROPIC_API_KEY
    if os.environ.get("ANTHROPIC_API_KEY"):
        checks.append(HealthCheck("ANTHROPIC_API_KEY", "PASS", "set"))
    else:
        checks.append(HealthCheck(
            "ANTHROPIC_API_KEY", "FAIL",
            "not set — add to .env or Railway environment",
        ))

    # 6. briefings/ directory
    if os.path.isdir(BRIEFINGS_DIR):
        checks.append(HealthCheck("briefings/ directory", "PASS", ""))
    else:
        checks.append(HealthCheck(
            "briefings/ directory", "WARN",
            f"{BRIEFINGS_DIR} does not exist (will be created on first run)",
        ))

    # 7. Syncer imports
    _SYNCER_IMPORTS = [
        ("intelligence.syncers.sync_google",     "GoogleSyncer"),
        ("intelligence.syncers.sync_jobber",      "JobberSyncer"),
        ("intelligence.syncers.sync_quickbooks",  "QuickBooksSyncer"),
        ("intelligence.syncers.sync_hubspot",     "HubSpotSyncer"),
        ("intelligence.syncers.sync_pipedrive",   "PipedriveSyncer"),
        ("intelligence.syncers.sync_mailchimp",   "MailchimpSyncer"),
        ("intelligence.syncers.sync_asana",       "AsanaSyncer"),
        ("intelligence.syncers.sync_slack",       "SlackSyncer"),
    ]
    for module_path, class_name in _SYNCER_IMPORTS:
        try:
            mod = importlib.import_module(module_path)
            getattr(mod, class_name)
            checks.append(HealthCheck(f"Import: {class_name}", "PASS", ""))
        except (ImportError, AttributeError) as exc:
            checks.append(HealthCheck(f"Import: {class_name}", "WARN", str(exc)))

    render_table("Intelligence Runner — Health Check", checks)
    sys.exit(1 if any(c.status == "FAIL" for c in checks) else 0)


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Sparkle & Shine intelligence pipeline orchestrator.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python -m intelligence.runner\n"
            "  python -m intelligence.runner --date 2026-03-17 --skip-sync\n"
            "  python -m intelligence.runner --date yesterday --dry-run\n"
            "  python -m intelligence.runner --sync-only --verbose\n"
        ),
    )
    parser.add_argument(
        "--date",
        default="today",
        metavar="DATE",
        help="Briefing date as YYYY-MM-DD, 'today', or 'yesterday' (default: today).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run everything except the Claude API call and Slack post. "
             "Prints the context document to stdout instead.",
    )
    parser.add_argument(
        "--skip-sync",
        action="store_true",
        help="Skip the sync stage. Use existing SQLite data. "
             "Useful for demos and testing against seeded data.",
    )
    parser.add_argument(
        "--sync-only",
        action="store_true",
        help="Run only the syncers, then stop. For debugging sync issues.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Set logging to DEBUG level.",
    )
    parser.add_argument(
        "--channel",
        default=None,
        metavar="CHANNEL",
        help="Override the Slack channel for this run (e.g. '#test-channel').",
    )
    parser.add_argument(
        "--no-archive",
        action="store_true",
        help="Skip saving the briefing to the briefings/ directory.",
    )
    parser.add_argument(
        "--preflight",
        action="store_true",
        help="Run only the token preflight check and exit. "
             "Useful before demos to verify all 8 tool tokens are valid.",
    )
    parser.add_argument(
        "--report-type",
        default="daily",
        choices=["daily", "weekly"],
        dest="report_type",
        help=(
            "Report type to generate. 'daily' (default) produces a short tactical ops "
            "report for the current day. 'weekly' produces a strategic review of the "
            "past 7 days with a TL;DR, posted to #weekly-briefing."
        ),
    )
    parser.add_argument(
        "--health",
        action="store_true",
        help="Run service health checks and exit. Does not run the pipeline.",
    )
    return parser


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def main() -> None:
    parser = _build_parser()
    args   = parser.parse_args()

    if args.health:
        _run_health_check()  # exits internally

    # --- Configure logging level ---
    if args.verbose:
        logging.getLogger("intelligence").setLevel(logging.DEBUG)
        logging.getLogger("syncer").setLevel(logging.DEBUG)

    # -----------------------------------------------------------------------
    # --preflight mode: run token check only, then exit
    # -----------------------------------------------------------------------
    if args.preflight:
        from demo.hardening.token_preflight import check_all_tokens, print_preflight_table
        print("Sparkle & Shine -- Token Preflight Check")
        print("=" * 45)
        print()
        result = check_all_tokens()
        print_preflight_table(result)
        sys.exit(0 if result.all_passed else 1)

    pipeline_start = time.monotonic()
    briefing_date  = _resolve_date(args.date)
    report_type    = args.report_type

    result = PipelineResult(briefing_date=briefing_date)

    logger.info(
        "Intelligence pipeline starting | date=%s | report_type=%s | "
        "dry_run=%s | skip_sync=%s | sync_only=%s",
        briefing_date,
        report_type,
        args.dry_run,
        args.skip_sync,
        args.sync_only,
    )

    # -----------------------------------------------------------------------
    # Stage 0: TOKEN PREFLIGHT  (only when sync will run)
    # -----------------------------------------------------------------------
    run_sync = not args.skip_sync and not args.dry_run
    skip_set: set[str] = set()

    if run_sync:
        skip_set = _run_token_preflight()

    # -----------------------------------------------------------------------
    # Stage 1: SYNC
    # -----------------------------------------------------------------------
    if run_sync:
        logger.info("--- Stage 1: SYNC ---")
        succeeded, errors = _run_syncers(DB_PATH, skip_set=skip_set)
        result.sync_succeeded = succeeded
        result.sync_errors    = errors
    else:
        reason = "dry-run" if args.dry_run else "--skip-sync"
        logger.info("Stage 1: SYNC skipped (%s)", reason)
        result.sync_skipped = len(_SYNCERS)

    if args.sync_only:
        result.duration_seconds = time.monotonic() - pipeline_start
        _print_summary(result)
        return

    # -----------------------------------------------------------------------
    # Stage 2: COMPUTE METRICS
    # -----------------------------------------------------------------------
    logger.info("--- Stage 2: COMPUTE METRICS ---")
    from intelligence.metrics import compute_all_metrics

    metrics_start = time.monotonic()
    metrics = compute_all_metrics(DB_PATH, briefing_date)
    metrics_time = time.monotonic() - metrics_start

    # Count successfully computed modules (all except 'computed_at')
    computed_modules = sum(
        1 for k, v in metrics.items()
        if k != "computed_at" and isinstance(v, dict)
    )
    result.metrics_computed = computed_modules

    logger.info("Metrics computed in %.1fs", metrics_time)

    # -----------------------------------------------------------------------
    # Stage 3: BUILD CONTEXT
    # -----------------------------------------------------------------------
    logger.info("--- Stage 3: BUILD CONTEXT (%s) ---", report_type)
    from intelligence.context_builder import build_briefing_context, build_weekly_context

    if report_type == "weekly":
        context = build_weekly_context(DB_PATH, briefing_date)
        logger.info("Weekly context built: ~%d estimated tokens", context.token_estimate)
    else:
        context = build_briefing_context(DB_PATH, briefing_date, briefings_dir=BRIEFINGS_DIR)
        logger.info(
            "Daily context built: ~%d estimated tokens (%d recent briefing(s) injected)",
            context.token_estimate,
            context.recent_briefings_loaded,
        )

    # Collect alerts for later use in stages 5 and summary
    all_alerts = _collect_alerts(metrics)
    result.alert_warnings, result.alert_criticals = _count_alert_levels(all_alerts)

    if args.dry_run:
        print(context.context_document)
        result.duration_seconds = time.monotonic() - pipeline_start
        logger.info("Dry run complete. Exiting before API call.")
        _print_summary(result)
        return

    # -----------------------------------------------------------------------
    # Stage 4: GENERATE REPORT
    # -----------------------------------------------------------------------
    logger.info("--- Stage 4: GENERATE REPORT (%s) ---", report_type)
    from intelligence.briefing_generator import generate_briefing

    if args.report_type == "weekly":
        from intelligence.weekly_report import generate_weekly_report
        briefing = generate_weekly_report(context, dry_run=args.dry_run)
    else:
        briefing = generate_briefing(context, dry_run=args.dry_run)

    word_count = len(briefing.content_plain.split())
    result.briefing_words  = word_count
    result.input_tokens    = briefing.input_tokens
    result.output_tokens   = briefing.output_tokens

    logger.info(
        "%s report generated: %d words, %d+%d tokens, %.1fs",
        report_type,
        word_count,
        briefing.input_tokens,
        briefing.output_tokens,
        briefing.generation_time_seconds,
    )

    # -----------------------------------------------------------------------
    # Stage 5: PUBLISH TO SLACK
    # -----------------------------------------------------------------------
    logger.info("--- Stage 5: PUBLISH TO SLACK ---")
    from intelligence.slack_publisher import post_briefing
    from intelligence.config import SLACK_CONFIG

    if args.channel:
        publish_channel = args.channel
    elif report_type == "weekly":
        publish_channel = SLACK_CONFIG["weekly_channel"]
    else:
        publish_channel = SLACK_CONFIG["briefing_channel"]

    if args.report_type == "weekly" and not args.dry_run:
        from intelligence.slack_publisher import ensure_channel
        ensure_channel(SLACK_CONFIG["weekly_channel"])

    ok = post_briefing(briefing, channel=publish_channel)
    if ok:
        result.published_channel = publish_channel
        result.published_at      = time.strftime("%I:%M %p")
        logger.info("%s report published to %s", report_type, publish_channel)
    else:
        logger.error(
            "Failed to publish %s report to %s; report will still be archived.",
            report_type,
            publish_channel,
        )

    # Post critical alerts to their respective channels
    if all_alerts:
        _post_critical_alerts(all_alerts)

    # -----------------------------------------------------------------------
    # Stage 6: ARCHIVE
    # -----------------------------------------------------------------------
    if not args.no_archive:
        logger.info("--- Stage 6: ARCHIVE ---")
        _archive(
            briefing_date=briefing_date,
            briefing_content=briefing.content_plain,
            context_document=context.context_document,
            report_type=report_type,
        )
        result.archived = True
    else:
        logger.info("Stage 6: ARCHIVE skipped (--no-archive)")

    # -----------------------------------------------------------------------
    # Final summary
    # -----------------------------------------------------------------------
    result.duration_seconds = time.monotonic() - pipeline_start
    _print_summary(result)


if __name__ == "__main__":
    main()
