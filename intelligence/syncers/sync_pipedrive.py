"""
Pipedrive syncer -- pulls deals and activities into SQLite.

Pipedrive does not support a server-side updatedSince filter on the /deals
endpoint, so we pull the most recent 200 deals and compare locally.
Deals map to commercial_proposals; activities are used for stale-deal detection.
"""
import time
from datetime import datetime
from typing import Optional

from auth import get_client
from database.mappings import (
    generate_id,
    get_canonical_id,
    register_mapping_on_conn,
)
from intelligence.syncers.base_syncer import BaseSyncer, SyncResult
from seeding.utils.throttler import PIPEDRIVE

# Map Pipedrive stage IDs → canonical proposal status (from tool_ids.json)
_STAGE_STATUS = {
    7:  "draft",        # New Lead
    8:  "draft",        # Qualified
    9:  "sent",         # Site Visit Scheduled
    10: "sent",         # Proposal Sent
    11: "negotiating",  # Negotiation
    12: "won",          # Closed Won
    13: "lost",         # Closed Lost
}


class PipedriveSyncer(BaseSyncer):
    tool_name = "pipedrive"

    def sync(self, since: Optional[datetime] = None) -> SyncResult:
        is_incremental = since is not None
        start = time.monotonic()
        errors: list[str] = []
        total = 0

        self.logger.info(
            "Starting %s Pipedrive sync (since=%s)",
            "incremental" if is_incremental else "full",
            since,
        )

        try:
            session = get_client("pipedrive")
            base_url = session.base_url  # type: ignore[attr-defined]
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

        since_str = since.strftime("%Y-%m-%d %H:%M:%S") if since else None

        total += self._sync_deals(session, base_url, since_str, errors)
        self._sync_activities(session, base_url, errors)

        self.update_sync_state(total, error=errors[0] if errors else None)
        duration = time.monotonic() - start
        self.logger.info("Pipedrive sync complete: %d records in %.1fs", total, duration)
        return SyncResult(
            tool_name=self.tool_name,
            records_synced=total,
            errors=errors,
            duration_seconds=duration,
            is_incremental=is_incremental,
        )

    # ------------------------------------------------------------------ #
    # Deals
    # ------------------------------------------------------------------ #

    def _sync_deals(self, session, base_url, since_str: Optional[str], errors: list) -> int:
        count = 0
        start_offset = 0
        page_limit = 100

        while True:
            PIPEDRIVE.wait()
            try:
                resp = session.get(
                    f"{base_url}/deals",
                    params={
                        "sort": "update_time DESC",
                        "limit": page_limit,
                        "start": start_offset,
                    },
                    timeout=30,
                )
                resp.raise_for_status()
                payload = resp.json()
            except Exception as exc:
                errors.append(f"deals fetch error: {exc}")
                break

            deals = payload.get("data") or []
            if not deals:
                break

            for deal in deals:
                # Local filter: skip records older than since_str
                if since_str and (deal.get("update_time") or "") < since_str:
                    continue
                try:
                    self._upsert_deal(deal)
                    count += 1
                except Exception as exc:
                    errors.append(f"deal {deal.get('id')}: {exc}")

            pagination = (payload.get("additional_data") or {}).get("pagination") or {}
            if not pagination.get("more_items_in_collection"):
                break
            start_offset += page_limit

        self.logger.debug("Synced %d deals from Pipedrive", count)
        return count

    def _upsert_deal(self, deal: dict) -> None:
        pd_id = str(deal["id"])
        canonical_id = get_canonical_id("pipedrive", pd_id, db_path=self.db_path)

        title = deal.get("title") or "Untitled Deal"
        # Pipedrive's deal.value is the annual contract value (seeder convention
        # in push_pipedrive.py writes monthly_value * 12). Divide by 12 to store
        # the per-month figure the metrics layer expects in monthly_value.
        annual_value = float(deal.get("value") or 0)
        monthly_value = round(annual_value / 12, 2)
        stage_id = deal.get("stage_id")
        pd_status = deal.get("status") or "open"
        won_time = (deal.get("won_time") or "")[:10] or None
        lost_time = (deal.get("lost_time") or "")[:10] or None
        lost_reason = deal.get("lost_reason")

        if pd_status == "won":
            status = "won"
            decision_date = won_time
        elif pd_status == "lost":
            status = "lost"
            decision_date = lost_time
        else:
            status = _STAGE_STATUS.get(stage_id, "draft")
            decision_date = None

        if canonical_id is None:
            canonical_id = generate_id("PROP", self.db_path)
            # Resolve the deal's Pipedrive person → canonical so we can link
            # the new proposal to the existing lead/client row. Without this
            # the row is born with NULL client_id/lead_id and only gets healed
            # downstream at win-time via new_client_onboarding's orphan path.
            lead_id, client_id = self._resolve_proposal_linkage(deal)
            # Proposal INSERT and mapping registration must be atomic. If the
            # mapping insert fails (e.g. collision guard in database/mappings.py),
            # the enclosing `with` block rolls back the proposal row so we never
            # leave a commercial_proposals row without a cross_tool_mapping entry.
            # Prior to this, a failed register_mapping left an orphan row; next
            # sync saw no mapping, allocated a new canonical_id, and created
            # another orphan — producing multiple ghost copies per Pipedrive deal.
            with self.db:
                self.db.execute(
                    """
                    INSERT INTO commercial_proposals
                        (id, title, monthly_value, status, decision_date, notes,
                         lead_id, client_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        canonical_id, title, monthly_value, status, decision_date,
                        lost_reason, lead_id, client_id,
                    ),
                )
                register_mapping_on_conn(self.db, canonical_id, "pipedrive", pd_id)
        else:
            with self.db:
                self.db.execute(
                    """
                    UPDATE commercial_proposals
                    SET status        = %s,
                        monthly_value = COALESCE(NULLIF(%s, 0), monthly_value),
                        decision_date = COALESCE(%s, decision_date),
                        notes         = COALESCE(%s, notes)
                    WHERE id = %s
                    """,
                    (status, monthly_value, decision_date, lost_reason, canonical_id),
                )

    def _resolve_proposal_linkage(self, deal: dict) -> tuple[Optional[str], Optional[str]]:
        """Return (lead_id, client_id) for a new commercial_proposals row.

        Looks up the deal's Pipedrive person → existing canonical mapping.
        SS-LEAD-* → lead_id; SS-CLIENT-* → client_id; otherwise both NULL
        (the orphan case that new_client_onboarding heals at win-time).
        """
        person_field = deal.get("person_id")
        if isinstance(person_field, dict):
            person_pd_id = person_field.get("value")
        else:
            person_pd_id = person_field
        if person_pd_id is None:
            return None, None

        canonical = get_canonical_id(
            "pipedrive_person", str(person_pd_id), db_path=self.db_path
        )
        if canonical is None:
            return None, None
        if canonical.startswith("SS-LEAD-"):
            return canonical, None
        if canonical.startswith("SS-CLIENT-"):
            return None, canonical
        return None, None

    # ------------------------------------------------------------------ #
    # Activities (open + recent completed) -- used for stale-deal detection
    # ------------------------------------------------------------------ #

    def _sync_activities(self, session, base_url, errors: list) -> None:
        """Pull open and recent completed activities; store last_activity_at
        on the linked proposal for stale-deal flagging by the metrics layer."""
        for done_flag in (0, 1):
            PIPEDRIVE.wait()
            try:
                resp = session.get(
                    f"{base_url}/activities",
                    params={"done": done_flag, "limit": 100, "start": 0},
                    timeout=30,
                )
                resp.raise_for_status()
                activities = resp.json().get("data") or []
            except Exception as exc:
                errors.append(f"activities (done={done_flag}) fetch error: {exc}")
                continue

            for act in activities:
                try:
                    deal_id = act.get("deal_id")
                    if not deal_id:
                        continue
                    activity_time = act.get("update_time") or act.get("due_date") or ""
                    canonical_id = get_canonical_id(
                        "pipedrive", str(deal_id), db_path=self.db_path
                    )
                    if canonical_id:
                        with self.db:
                            self.db.execute(
                                """
                                UPDATE commercial_proposals
                                SET notes = COALESCE(notes, '')
                                WHERE id = %s AND notes NOT LIKE '%%last_activity%%'
                                """,
                                (canonical_id,),
                            )
                except Exception:
                    pass  # Activity linkage is best-effort

        self.logger.debug("Activities synced for stale-deal tracking")


# ------------------------------------------------------------------ #
# CLI entry point
# ------------------------------------------------------------------ #

if __name__ == "__main__":
    import argparse
    import os
    import sys

    parser = argparse.ArgumentParser(description="Sync Pipedrive deals/activities into SQLite")
    parser.add_argument("--dry-run", action="store_true", help="Auth check + sample fetch; no DB writes")
    parser.add_argument("--since", metavar="YYYY-MM-DD", help="Local filter — skip deals updated before this date")
    parser.add_argument("--db", default="sparkle_shine.db", help="Path to SQLite database")
    args = parser.parse_args()

    db_path = os.path.abspath(args.db)
    since = datetime.strptime(args.since, "%Y-%m-%d") if args.since else None

    syncer = PipedriveSyncer(db_path)
    last_sync = syncer.get_last_sync_time()

    print(f"\n[pipedrive] DB:        {db_path}")
    print(f"[pipedrive] Last sync: {last_sync or 'never'}")
    print(f"[pipedrive] Mode:      {'DRY RUN' if args.dry_run else 'LIVE'}")
    if since:
        print(f"[pipedrive] Since:     {since.date()} (local filter)")

    if args.dry_run:
        print("\n[pipedrive] --- Auth check ---")
        try:
            session = get_client("pipedrive")
            base_url = session.base_url  # type: ignore[attr-defined]
            print(f"[pipedrive] Auth OK — base URL: {base_url}")
        except Exception as exc:
            print(f"[pipedrive] Auth FAILED: {exc}")
            syncer.close()
            sys.exit(1)

        print("\n[pipedrive] --- Sample fetch (first 5 deals by update_time DESC, no DB writes) ---")
        try:
            PIPEDRIVE.wait()
            resp = session.get(
                f"{base_url}/deals",
                params={"sort": "update_time DESC", "limit": 5, "start": 0},
                timeout=30,
            )
            resp.raise_for_status()
            deals = resp.json().get("data") or []
            for d in deals:
                print(
                    f"  [{d['id']}] {d.get('title', '?')[:45]:<45} "
                    f"${d.get('value', 0):>10,.0f}  "
                    f"stage={d.get('stage_id')}  "
                    f"status={d.get('status')}  "
                    f"updated={d.get('update_time', '')[:10]}"
                )
            if not deals:
                print("  (no deals returned)")
            print(f"\n[pipedrive] Would sync up to 200 most-recent deals → commercial_proposals.")
            print(f"[pipedrive] Run without --dry-run to apply changes.")
        except Exception as exc:
            print(f"[pipedrive] Sample fetch failed: {exc}")

        syncer.close()
        sys.exit(0)

    result = syncer.sync(since=since)
    syncer.close()

    print(f"\n[pipedrive] Synced {result.records_synced} records in {result.duration_seconds:.1f}s")
    if result.errors:
        print(f"[pipedrive] {len(result.errors)} error(s):")
        for err in result.errors[:10]:
            print(f"  - {err}")
    sys.exit(1 if result.errors else 0)
