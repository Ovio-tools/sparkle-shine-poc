"""
Slack syncer -- lightweight message-count tracker.

Slack is primarily an output channel; this syncer only counts messages in
#operations and #sales over the last 24 hours and writes the totals
to daily_metrics_snapshot.raw_json. Message content is never stored.
"""
import json
import time
from datetime import date, datetime, timezone
from typing import Optional

from auth import get_client
from intelligence.syncers.base_syncer import BaseSyncer, SyncResult

# Channel IDs from tool_ids.json
_CHANNELS = {
    "operations":    "C0AM76H9K34",
    "sales": "C0ALRNT2Z8F",
}


class SlackSyncer(BaseSyncer):
    tool_name = "slack"

    def sync(self, since: Optional[datetime] = None) -> SyncResult:
        is_incremental = since is not None
        start = time.monotonic()
        errors: list[str] = []
        total = 0

        self.logger.info("Starting Slack message-count sync")

        try:
            client = get_client("slack")
        except Exception as exc:
            errors.append(f"Auth failed: {exc}")
            self.update_sync_state(0, error=str(exc))
            return SyncResult(
                tool_name=self.tool_name,
                records_synced=0,
                errors=errors,
                duration_seconds=time.monotonic() - start,
                is_incremental=is_incremental,
            )

        # Default window: last 24 hours
        if since is not None:
            oldest_unix = since.replace(tzinfo=timezone.utc).timestamp()
        else:
            oldest_unix = datetime.now(timezone.utc).timestamp() - 86400

        counts: dict[str, int] = {}
        for channel_key, channel_id in _CHANNELS.items():
            try:
                count = self._count_messages(client, channel_id, oldest_unix)
                counts[f"slack_{channel_key.replace('-', '_')}_messages"] = count
                total += count
                self.logger.debug("Channel #%s: %d messages", channel_key, count)
            except Exception as exc:
                errors.append(f"channel {channel_key}: {exc}")

        if counts:
            today = date.today().isoformat()
            self._merge_snapshot(today, counts)

        self.update_sync_state(total, error=errors[0] if errors else None)
        duration = time.monotonic() - start
        self.logger.info("Slack sync complete: %d messages counted in %.1fs", total, duration)
        return SyncResult(
            tool_name=self.tool_name,
            records_synced=total,
            errors=errors,
            duration_seconds=duration,
            is_incremental=is_incremental,
        )

    def _count_messages(self, client, channel_id: str, oldest_unix: float) -> int:
        """Return the number of non-bot messages since oldest_unix."""
        count = 0
        cursor = None

        while True:
            kwargs: dict = {
                "channel": channel_id,
                "oldest": str(oldest_unix),
                "limit": 200,
            }
            if cursor:
                kwargs["cursor"] = cursor

            resp = client.conversations_history(**kwargs)
            messages = resp.get("messages") or []

            # Count only non-bot messages
            count += sum(
                1 for m in messages
                if m.get("type") == "message" and not m.get("bot_id")
            )

            meta = resp.get("response_metadata") or {}
            cursor = meta.get("next_cursor")
            if not cursor:
                break

            # Respect Slack rate limit (1 msg/sec for chat.write; conversations.history is more generous)
            time.sleep(0.2)

        return count

    def _merge_snapshot(self, snapshot_date: str, new_data: dict) -> None:
        row = self.db.execute(
            "SELECT raw_json FROM daily_metrics_snapshot WHERE snapshot_date = ?",
            (snapshot_date,),
        ).fetchone()
        existing = json.loads(row["raw_json"]) if (row and row["raw_json"]) else {}
        existing.update(new_data)
        with self.db:
            self.db.execute(
                """
                INSERT INTO daily_metrics_snapshot (snapshot_date, raw_json)
                VALUES (?, ?)
                ON CONFLICT(snapshot_date) DO UPDATE SET raw_json = excluded.raw_json
                """,
                (snapshot_date, json.dumps(existing)),
            )


# ------------------------------------------------------------------ #
# CLI entry point
# ------------------------------------------------------------------ #

if __name__ == "__main__":
    import argparse
    import os
    import sys

    parser = argparse.ArgumentParser(description="Count Slack messages in #operations and #sales")
    parser.add_argument("--dry-run", action="store_true", help="Auth check + live count preview; no DB writes")
    parser.add_argument("--since", metavar="YYYY-MM-DD", help="Count messages since this date (default: last 24h)")
    parser.add_argument("--db", default="sparkle_shine.db", help="Path to SQLite database")
    args = parser.parse_args()

    db_path = os.path.abspath(args.db)
    since = datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=timezone.utc) if args.since else None

    syncer = SlackSyncer(db_path)
    last_sync = syncer.get_last_sync_time()

    print(f"\n[slack] DB:        {db_path}")
    print(f"[slack] Last sync: {last_sync or 'never'}")
    print(f"[slack] Mode:      {'DRY RUN' if args.dry_run else 'LIVE'}")

    if args.dry_run:
        print("\n[slack] --- Auth check ---")
        try:
            client = get_client("slack")
            auth = client.auth_test()
            print(f"[slack] Auth OK — bot: {auth.get('bot_id')}  team: {auth.get('team')}")
        except Exception as exc:
            print(f"[slack] Auth FAILED: {exc}")
            syncer.close()
            sys.exit(1)

        oldest = since.timestamp() if since else (datetime.now(timezone.utc).timestamp() - 86400)
        window_label = since.date().isoformat() if since else "last 24h"

        print(f"\n[slack] --- Message count preview (since {window_label}, no DB writes) ---")
        for channel_key, channel_id in _CHANNELS.items():
            try:
                count = syncer._count_messages(client, channel_id, oldest)
                print(f"  #{channel_key:<20} {count:>5} messages")
            except Exception as exc:
                print(f"  #{channel_key:<20} ERROR: {exc}")

        print(f"\n[slack] Would write message counts to daily_metrics_snapshot.raw_json.")
        print(f"[slack] Run without --dry-run to apply changes.")

        syncer.close()
        sys.exit(0)

    result = syncer.sync(since=since)
    syncer.close()

    print(f"\n[slack] Counted {result.records_synced} total messages in {result.duration_seconds:.1f}s")
    if result.errors:
        print(f"[slack] {len(result.errors)} error(s):")
        for err in result.errors[:10]:
            print(f"  - {err}")
    sys.exit(1 if result.errors else 0)
