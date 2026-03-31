"""
Google Workspace syncer -- Drive documents, Calendar events, and Gmail metadata.

Three separate Google API clients are used (Drive, Calendar, Gmail).
Drive text content is stored in documents + document_index.
Calendar events go into calendar_events.
Gmail metadata (no bodies) goes into gmail_metadata.
"""
import json
import time
from datetime import date, datetime, timezone, timedelta
from typing import Optional

from auth import get_client
from database.mappings import get_canonical_id, register_mapping, generate_id
from intelligence.syncers.base_syncer import BaseSyncer, SyncResult

# Google Drive folder / file IDs for documents (from tool_ids.json)
_DRIVE_DOC_IDS: dict[str, str] = {
    "employee_handbook":          "1nMBvM4szdxP47AalGcvEpl-_3biJUy38EpCt2XXA_j0",
    "service_quality_checklist":  "15frlFWs7kG6cAf3xhq7UHZ9hUZ4C--R4aP99eR1HaSs",
    "client_onboarding_guide":    "1yAYHjYT3Za4Tg8QQJ88ZLFj9XofzQgAgGfnk-VvRbXE",
    "safety_chemical_manual":     "1LerAOxr2R8B7MuIn3Pg4h0W_ArRqwicigsR7aVmFYQ4",
    "sales_proposal_templates":   "1cG5y6DHVtPa1qYGrStRLvwwJ6h5n-pm3XLA_I-nc-1o",
    "marketing_playbook":         "1XmyKEk2aZ-QOCtyMxvNGqyXxgzkEFNQCgB8vzlSav2s",
    "vendor_supplier_directory":  "1_IfpzM8DdoWxtFWiSDyD97hs7WZc2bWdRjrB2gU8A08",
    "fy2026_growth_plan":         "1oLbD57aqQlIpZcqialm7H10FBMEpVj-v-_cdsXoTzMY",
}

_CREATE_GMAIL_METADATA = """
CREATE TABLE IF NOT EXISTS gmail_metadata (
    message_id      TEXT PRIMARY KEY,
    from_address    TEXT,
    to_address      TEXT,
    subject         TEXT,
    message_date    TEXT,
    synced_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


class GoogleSyncer(BaseSyncer):
    tool_name = "google"

    def __init__(self, db_path: str):
        super().__init__(db_path)
        with self.db:
            self.db.execute(_CREATE_GMAIL_METADATA)

    def sync(self, since: Optional[datetime] = None) -> SyncResult:
        is_incremental = since is not None
        start = time.monotonic()
        errors: list[str] = []
        total = 0

        self.logger.info(
            "Starting %s Google sync (since=%s)",
            "incremental" if is_incremental else "full",
            since,
        )

        try:
            drive_svc = get_client("google_drive")
            cal_svc = get_client("google_calendar")
            gmail_svc = get_client("google_gmail")
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

        total += self._sync_drive_docs(drive_svc, since, errors)
        total += self._sync_calendar(cal_svc, errors)
        total += self._sync_gmail_metadata(gmail_svc, errors)

        self.update_sync_state(total, error=errors[0] if errors else None)
        duration = time.monotonic() - start
        self.logger.info("Google sync complete: %d records in %.1fs", total, duration)
        return SyncResult(
            tool_name=self.tool_name,
            records_synced=total,
            errors=errors,
            duration_seconds=duration,
            is_incremental=is_incremental,
        )

    # ------------------------------------------------------------------ #
    # Drive documents
    # ------------------------------------------------------------------ #

    def _sync_drive_docs(self, drive_svc, since: Optional[datetime], errors: list) -> int:
        count = 0
        since_str = since.isoformat() + "Z" if since else None

        for doc_key, file_id in _DRIVE_DOC_IDS.items():
            try:
                # Check modified time before exporting
                meta = drive_svc.files().get(
                    fileId=file_id,
                    fields="id,name,modifiedTime,mimeType",
                ).execute()

                modified_time = meta.get("modifiedTime", "")
                if since_str and modified_time < since_str:
                    continue  # Not modified since last sync

                title = meta.get("name") or doc_key
                mime = meta.get("mimeType") or ""

                # Export as plain text (works for Google Docs and Sheets)
                export_mime = "text/plain"
                content_bytes = drive_svc.files().export(
                    fileId=file_id,
                    mimeType=export_mime,
                ).execute()

                content_text = (
                    content_bytes.decode("utf-8", errors="replace")
                    if isinstance(content_bytes, bytes)
                    else str(content_bytes)
                )

                # Determine doc_type
                if "spreadsheet" in mime:
                    doc_type = "spreadsheet"
                    platform = "google_sheets"
                else:
                    doc_type = "sop"
                    platform = "google_docs"

                self._upsert_document(
                    file_id=file_id,
                    title=title,
                    doc_type=doc_type,
                    platform=platform,
                    content_text=content_text,
                    modified_time=modified_time[:10],
                )
                count += 1
                self.logger.debug("Indexed document: %s", title)

            except Exception as exc:
                errors.append(f"drive doc {doc_key}: {exc}")

        return count

    def _upsert_document(
        self,
        file_id: str,
        title: str,
        doc_type: str,
        platform: str,
        content_text: str,
        modified_time: str,
    ) -> None:
        # Find existing document by google_file_id
        row = self.db.execute(
            "SELECT id FROM documents WHERE google_file_id = %s", (file_id,)
        ).fetchone()

        if row:
            canonical_id = row["id"]
            with self.db:
                self.db.execute(
                    """
                    UPDATE documents
                    SET content_text   = %s,
                        last_indexed_at = %s
                    WHERE id = %s
                    """,
                    (content_text, modified_time, canonical_id),
                )
        else:
            canonical_id = generate_id("DOC", self.db_path)
            with self.db:
                self.db.execute(
                    """
                    INSERT INTO documents
                        (id, title, doc_type, platform, google_file_id,
                         content_text, last_indexed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        canonical_id, title, doc_type, platform,
                        file_id, content_text, modified_time,
                    ),
                )
            register_mapping(canonical_id, "google", file_id, db_path=self.db_path)

        # Refresh document_index: delete old chunks, insert fresh
        with self.db:
            self.db.execute(
                "DELETE FROM document_index WHERE doc_id = %s", (canonical_id,)
            )
            self.db.execute(
                """
                INSERT INTO document_index (doc_id, chunk_text, source_title, indexed_at)
                VALUES (%s, %s, %s, CURRENT_DATE)
                """,
                (canonical_id, content_text, title),
            )

    # ------------------------------------------------------------------ #
    # Calendar events (yesterday + next 7 days)
    # ------------------------------------------------------------------ #

    def _sync_calendar(self, cal_svc, errors: list) -> int:
        count = 0
        try:
            yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
            next_week = (datetime.now(timezone.utc) + timedelta(days=7)).isoformat()

            resp = cal_svc.events().list(
                calendarId="primary",
                timeMin=yesterday,
                timeMax=next_week,
                singleEvents=True,
                orderBy="startTime",
                maxResults=250,
            ).execute()

            events = resp.get("items") or []
            for event in events:
                try:
                    self._upsert_calendar_event(event)
                    count += 1
                except Exception as exc:
                    errors.append(f"calendar event {event.get('id')}: {exc}")

        except Exception as exc:
            errors.append(f"calendar fetch error: {exc}")

        self.logger.debug("Synced %d calendar events", count)
        return count

    def _upsert_calendar_event(self, event: dict) -> None:
        google_event_id = event.get("id") or ""
        canonical_id = get_canonical_id("google", google_event_id, self.db_path)

        title = event.get("summary") or "Untitled Event"
        start = event.get("start") or {}
        end = event.get("end") or {}

        start_dt = start.get("dateTime") or start.get("date") or ""
        end_dt = end.get("dateTime") or end.get("date") or ""

        attendees = json.dumps([
            a.get("email") for a in (event.get("attendees") or [])
            if a.get("email")
        ])
        notes = event.get("description") or None

        if canonical_id is None:
            canonical_id = generate_id("CAL", self.db_path)
            with self.db:
                self.db.execute(
                    """
                    INSERT INTO calendar_events
                        (id, title, start_datetime, end_datetime, attendees, notes)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (canonical_id, title, start_dt, end_dt, attendees, notes),
                )
            register_mapping(canonical_id, "google", google_event_id, db_path=self.db_path)
        else:
            with self.db:
                self.db.execute(
                    """
                    UPDATE calendar_events
                    SET title          = %s,
                        start_datetime = %s,
                        end_datetime   = %s,
                        attendees      = %s,
                        notes          = COALESCE(%s, notes)
                    WHERE id = %s
                    """,
                    (title, start_dt, end_dt, attendees, notes, canonical_id),
                )

    # ------------------------------------------------------------------ #
    # Gmail metadata (last 24 hours) -- no message bodies stored
    # ------------------------------------------------------------------ #

    def _sync_gmail_metadata(self, gmail_svc, errors: list) -> int:
        count = 0
        try:
            yesterday_epoch = int(
                (datetime.now(timezone.utc) - timedelta(days=1)).timestamp()
            )
            list_resp = gmail_svc.users().messages().list(
                userId="me",
                q=f"after:{yesterday_epoch}",
                maxResults=100,
            ).execute()

            messages = list_resp.get("messages") or []
            for msg_ref in messages:
                try:
                    msg_id = msg_ref["id"]
                    # Skip if already in our metadata table
                    existing = self.db.execute(
                        "SELECT message_id FROM gmail_metadata WHERE message_id = %s",
                        (msg_id,),
                    ).fetchone()
                    if existing:
                        continue

                    detail = gmail_svc.users().messages().get(
                        userId="me",
                        id=msg_id,
                        format="metadata",
                        metadataHeaders=["From", "To", "Subject", "Date"],
                    ).execute()

                    headers = {
                        h["name"]: h["value"]
                        for h in (detail.get("payload", {}).get("headers") or [])
                    }

                    with self.db:
                        self.db.execute(
                            """
                            INSERT INTO gmail_metadata
                                (message_id, from_address, to_address, subject, message_date)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT DO NOTHING
                            """,
                            (
                                msg_id,
                                headers.get("From"),
                                headers.get("To"),
                                headers.get("Subject"),
                                headers.get("Date"),
                            ),
                        )
                    count += 1
                except Exception as exc:
                    errors.append(f"gmail message {msg_ref.get('id')}: {exc}")

        except Exception as exc:
            errors.append(f"gmail list error: {exc}")

        self.logger.debug("Synced %d Gmail metadata records", count)
        return count


# ------------------------------------------------------------------ #
# CLI entry point
# ------------------------------------------------------------------ #

if __name__ == "__main__":
    import argparse
    import os
    import sys

    parser = argparse.ArgumentParser(
        description="Sync Google Drive docs, Calendar events, and Gmail metadata into SQLite"
    )
    parser.add_argument("--dry-run", action="store_true", help="Auth check + metadata preview; no DB writes")
    parser.add_argument("--since", metavar="YYYY-MM-DD", help="Only re-index Drive docs modified after this date")
    parser.add_argument("--db", default="sparkle_shine.db", help="Path to SQLite database")
    args = parser.parse_args()

    db_path = os.path.abspath(args.db)
    since = datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=timezone.utc) if args.since else None

    syncer = GoogleSyncer(db_path)
    last_sync = syncer.get_last_sync_time()

    print(f"\n[google] DB:        {db_path}")
    print(f"[google] Last sync: {last_sync or 'never'}")
    print(f"[google] Mode:      {'DRY RUN' if args.dry_run else 'LIVE'}")
    if since:
        print(f"[google] Since:     {since.date()}")

    if args.dry_run:
        print("\n[google] --- Auth check (Drive, Calendar, Gmail) ---")
        try:
            drive_svc = get_client("google_drive")
            cal_svc = get_client("google_calendar")
            gmail_svc = get_client("google_gmail")
            print("[google] Auth OK — all three Google services authenticated")
        except Exception as exc:
            print(f"[google] Auth FAILED: {exc}")
            syncer.close()
            sys.exit(1)

        print(f"\n[google] --- Drive document metadata ({len(_DRIVE_DOC_IDS)} tracked files) ---")
        since_str = since.isoformat() + "Z" if since else None
        try:
            for doc_key, file_id in list(_DRIVE_DOC_IDS.items()):
                meta = drive_svc.files().get(
                    fileId=file_id, fields="id,name,modifiedTime,mimeType"
                ).execute()
                modified = meta.get("modifiedTime", "")[:10]
                would_sync = not since_str or meta.get("modifiedTime", "") >= since_str
                flag = "→ would re-index" if would_sync else "  (unchanged)"
                print(f"  {flag}  {meta.get('name', doc_key)[:50]} — modified {modified}")
        except Exception as exc:
            print(f"  Drive fetch failed: {exc}")

        print("\n[google] --- Calendar events (yesterday → +7 days) ---")
        try:
            yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
            next_week = (datetime.now(timezone.utc) + timedelta(days=7)).isoformat()
            resp = cal_svc.events().list(
                calendarId="primary",
                timeMin=yesterday,
                timeMax=next_week,
                singleEvents=True,
                orderBy="startTime",
                maxResults=5,
            ).execute()
            events = resp.get("items") or []
            for ev in events:
                start = (ev.get("start") or {})
                start_str = start.get("dateTime") or start.get("date") or "?"
                print(f"  {start_str[:16]}  {ev.get('summary', 'Untitled')[:55]}")
            if not events:
                print("  (no upcoming events)")
        except Exception as exc:
            print(f"  Calendar fetch failed: {exc}")

        print("\n[google] --- Gmail (last 24h message count) ---")
        try:
            yesterday_epoch = int((datetime.now(timezone.utc) - timedelta(days=1)).timestamp())
            list_resp = gmail_svc.users().messages().list(
                userId="me", q=f"after:{yesterday_epoch}", maxResults=1
            ).execute()
            total_estimate = list_resp.get("resultSizeEstimate", 0)
            print(f"  ~{total_estimate} messages in the last 24h (metadata only; bodies never stored)")
        except Exception as exc:
            print(f"  Gmail fetch failed: {exc}")

        print(f"\n[google] Would index docs, upsert calendar events, and store Gmail metadata.")
        print(f"[google] Run without --dry-run to apply changes.")

        syncer.close()
        sys.exit(0)

    result = syncer.sync(since=since)
    syncer.close()

    print(f"\n[google] Synced {result.records_synced} records in {result.duration_seconds:.1f}s")
    if result.errors:
        print(f"[google] {len(result.errors)} error(s):")
        for err in result.errors[:10]:
            print(f"  - {err}")
    sys.exit(1 if result.errors else 0)
