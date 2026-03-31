"""
Mailchimp syncer -- pulls audience stats and campaign reports into SQLite.

Audience totals go into daily_metrics_snapshot.raw_json.
Campaign metrics are upserted into marketing_campaigns.
"""
import json
import time
from datetime import datetime
from typing import Optional

from auth import get_client
from database.mappings import get_canonical_id, register_mapping, generate_id
from intelligence.syncers.base_syncer import BaseSyncer, SyncResult

# Mailchimp audience ID from tool_ids.json
_AUDIENCE_ID = "92f05d2d65"


class MailchimpSyncer(BaseSyncer):
    tool_name = "mailchimp"

    def sync(self, since: Optional[datetime] = None) -> SyncResult:
        is_incremental = since is not None
        start = time.monotonic()
        errors: list[str] = []
        total = 0

        self.logger.info(
            "Starting %s Mailchimp sync (since=%s)",
            "incremental" if is_incremental else "full",
            since,
        )

        try:
            client = get_client("mailchimp")
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

        since_str = since.strftime("%Y-%m-%dT%H:%M:%S+00:00") if since else None

        self._sync_audience_stats(client, errors)
        total += self._sync_campaign_reports(client, since_str, errors)

        self.update_sync_state(total, error=errors[0] if errors else None)
        duration = time.monotonic() - start
        self.logger.info("Mailchimp sync complete: %d records in %.1fs", total, duration)
        return SyncResult(
            tool_name=self.tool_name,
            records_synced=total,
            errors=errors,
            duration_seconds=duration,
            is_incremental=is_incremental,
        )

    # ------------------------------------------------------------------ #
    # Audience stats
    # ------------------------------------------------------------------ #

    def _sync_audience_stats(self, client, errors: list) -> None:
        try:
            info = client.lists.get_list(_AUDIENCE_ID)
            stats = info.get("stats") or {}
            member_count = stats.get("member_count", 0)
            unsubscribe_count = stats.get("unsubscribe_count", 0)
            campaign_count = stats.get("campaign_count", 0)

            from datetime import date
            today = date.today().isoformat()
            self._merge_snapshot(today, {
                "mailchimp_member_count": member_count,
                "mailchimp_unsubscribe_count": unsubscribe_count,
                "mailchimp_campaign_count": campaign_count,
            })
            self.logger.debug(
                "Audience stats: %d members, %d unsubscribes, %d campaigns",
                member_count, unsubscribe_count, campaign_count,
            )
        except Exception as exc:
            errors.append(f"audience stats error: {exc}")

    # ------------------------------------------------------------------ #
    # Campaign reports
    # ------------------------------------------------------------------ #

    def _sync_campaign_reports(self, client, since_str: Optional[str], errors: list) -> int:
        count = 0
        offset = 0
        page_size = 50

        while True:
            try:
                kwargs = {"count": page_size, "offset": offset}
                if since_str:
                    kwargs["since_send_time"] = since_str

                resp = client.reports.get_all_campaign_reports(**kwargs)
                reports = resp.get("reports") or []
            except Exception as exc:
                errors.append(f"campaign reports fetch error: {exc}")
                break

            if not reports:
                break

            for report in reports:
                try:
                    self._upsert_campaign(report)
                    count += 1
                except Exception as exc:
                    errors.append(f"campaign {report.get('id')}: {exc}")

            if len(reports) < page_size:
                break
            offset += page_size

        self.logger.debug("Synced %d campaign reports from Mailchimp", count)
        return count

    def _upsert_campaign(self, report: dict) -> None:
        mc_id = report.get("id") or ""
        canonical_id = get_canonical_id("mailchimp", mc_id, self.db_path)

        name = report.get("campaign_title") or report.get("subject_line") or "Untitled"
        send_date = (report.get("send_time") or "")[:10] or None
        emails_sent = report.get("emails_sent") or 0
        open_rate = float((report.get("opens") or {}).get("open_rate") or 0)
        click_rate = float((report.get("clicks") or {}).get("click_rate") or 0)
        unsubscribed = int((report.get("unsubscribes") or {}).get("unsubscribes") or 0)

        if canonical_id is None:
            canonical_id = generate_id("CAMP", self.db_path)
            with self.db:
                self.db.execute(
                    """
                    INSERT INTO marketing_campaigns
                        (id, name, platform, send_date, recipient_count,
                         open_rate, click_rate)
                    VALUES (%s, %s, 'mailchimp', %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (canonical_id, name, send_date, emails_sent, open_rate, click_rate),
                )
            register_mapping(canonical_id, "mailchimp", mc_id, db_path=self.db_path)
        else:
            with self.db:
                self.db.execute(
                    """
                    UPDATE marketing_campaigns
                    SET open_rate      = %s,
                        click_rate     = %s,
                        recipient_count = %s
                    WHERE id = %s
                    """,
                    (open_rate, click_rate, emails_sent, canonical_id),
                )

    # ------------------------------------------------------------------ #
    # Snapshot helper (shared with QuickBooks pattern)
    # ------------------------------------------------------------------ #

    def _merge_snapshot(self, snapshot_date: str, new_data: dict) -> None:
        row = self.db.execute(
            "SELECT raw_json FROM daily_metrics_snapshot WHERE snapshot_date = %s",
            (snapshot_date,),
        ).fetchone()
        existing = json.loads(row["raw_json"]) if (row and row["raw_json"]) else {}
        existing.update(new_data)
        with self.db:
            self.db.execute(
                """
                INSERT INTO daily_metrics_snapshot (snapshot_date, raw_json)
                VALUES (%s, %s)
                ON CONFLICT(snapshot_date) DO UPDATE SET raw_json = EXCLUDED.raw_json
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

    parser = argparse.ArgumentParser(description="Sync Mailchimp audience stats and campaign reports into SQLite")
    parser.add_argument("--dry-run", action="store_true", help="Auth check + sample fetch; no DB writes")
    parser.add_argument("--since", metavar="YYYY-MM-DD", help="Incremental sync from this date")
    parser.add_argument("--db", default="sparkle_shine.db", help="Path to SQLite database")
    args = parser.parse_args()

    db_path = os.path.abspath(args.db)
    since = datetime.strptime(args.since, "%Y-%m-%d") if args.since else None

    syncer = MailchimpSyncer(db_path)
    last_sync = syncer.get_last_sync_time()

    print(f"\n[mailchimp] DB:        {db_path}")
    print(f"[mailchimp] Last sync: {last_sync or 'never'}")
    print(f"[mailchimp] Mode:      {'DRY RUN' if args.dry_run else 'LIVE'}")
    if since:
        print(f"[mailchimp] Since:     {since.date()}")

    if args.dry_run:
        print("\n[mailchimp] --- Auth check ---")
        try:
            client = get_client("mailchimp")
            print("[mailchimp] Auth OK")
        except Exception as exc:
            print(f"[mailchimp] Auth FAILED: {exc}")
            syncer.close()
            sys.exit(1)

        print(f"\n[mailchimp] --- Audience stats preview (audience: {_AUDIENCE_ID}) ---")
        try:
            info = client.lists.get_list(_AUDIENCE_ID)
            stats = info.get("stats") or {}
            print(f"  Name:          {info.get('name')}")
            print(f"  Members:       {stats.get('member_count', 0):,}")
            print(f"  Unsubscribes:  {stats.get('unsubscribe_count', 0):,}")
            print(f"  Campaigns:     {stats.get('campaign_count', 0)}")

            print("\n[mailchimp] --- Recent campaign reports (first 3) ---")
            reports = (client.reports.get_all_campaign_reports(count=3).get("reports") or [])
            for r in reports:
                opens = r.get("opens") or {}
                clicks = r.get("clicks") or {}
                print(
                    f"  [{r.get('id')}] {r.get('campaign_title', r.get('subject_line', '?'))[:50]} — "
                    f"sent {r.get('emails_sent', 0)}, "
                    f"open {opens.get('open_rate', 0):.1%}, "
                    f"click {clicks.get('click_rate', 0):.1%}"
                )
            if not reports:
                print("  (no campaign reports returned)")

            print(f"\n[mailchimp] Would write audience stats to snapshot and upsert campaign records.")
            print(f"[mailchimp] Run without --dry-run to apply changes.")
        except Exception as exc:
            print(f"[mailchimp] Sample fetch failed: {exc}")

        syncer.close()
        sys.exit(0)

    result = syncer.sync(since=since)
    syncer.close()

    print(f"\n[mailchimp] Synced {result.records_synced} records in {result.duration_seconds:.1f}s")
    if result.errors:
        print(f"[mailchimp] {len(result.errors)} error(s):")
        for err in result.errors[:10]:
            print(f"  - {err}")
    sys.exit(1 if result.errors else 0)
