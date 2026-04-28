"""
Jobber syncer -- pulls clients, jobs, and recurring agreements into SQLite.

Uses the Jobber GraphQL API with cursor-based pagination.
Handles cross_tool_mapping for all three entity types.
"""
import re
import time
from datetime import datetime
from typing import Optional

from auth import get_client
from config.service_catalog import canonical_service_id
from database.mappings import get_canonical_id, register_mapping, generate_id
from intelligence.syncers.base_syncer import BaseSyncer, SyncResult
from seeding.utils.throttler import JOBBER

_GRAPHQL_URL = "https://api.getjobber.com/api/graphql"

_CLIENTS_QUERY = """
query ListClients($cursor: String) {
  clients(first: 100, after: $cursor) {
    nodes {
      id
      firstName
      lastName
      emails { address primary }
      phones { number primary }
      createdAt
      updatedAt
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

_JOBS_QUERY = """
query ListJobs($cursor: String) {
  jobs(first: 100, after: $cursor) {
    nodes {
      id
      title
      instructions
      startAt
      endAt
      jobStatus
      jobType
      client { id }
      visitSchedule {
        recurrenceSchedule {
          calendarRule
        }
      }
      visits(first: 1) {
        nodes {
          duration
        }
      }
      updatedAt
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

_RECURRING_QUERY = """
query ListRecurring($cursor: String) {
  quotes(first: 100, after: $cursor) {
    nodes {
      id
      title
      client { id }
      amounts { subtotal }
      jobs(first: 5) {
        nodes {
          jobType
        }
      }
      updatedAt
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

_JOBBER_STATUS_MAP = {
    "ACTIVE": "scheduled",
    "COMPLETED": "completed",
    "REQUIRES_INVOICING": "completed",
    "CANCELLED": "cancelled",
    "LATE": "scheduled",
    "UNSCHEDULED": "scheduled",
}

def _clean_text(value: object) -> Optional[str]:
    text = (value or "").strip()
    return text or None


def _slug_title(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def _service_type_from_title(title: Optional[str]) -> Optional[str]:
    return canonical_service_id(_clean_text(title))


def _duration_minutes(node: dict) -> Optional[int]:
    visit_nodes = (node.get("visits") or {}).get("nodes") or []
    first_visit = visit_nodes[0] if visit_nodes else {}
    raw_duration = first_visit.get("duration") or 0
    return round(raw_duration / 60) if raw_duration else None


def _is_recurring_job(node: dict) -> Optional[bool]:
    recurrence = (node.get("visitSchedule") or {}).get("recurrenceSchedule")
    if recurrence is not None:
        return True

    title = _clean_text(node.get("title"))
    if title:
        lowered = _slug_title(title)
        if lowered.startswith("recurring ") or lowered.endswith(" recurring"):
            return True
    return None


def _choose_service_type(existing_value: Optional[str], node: dict) -> str:
    candidate = _service_type_from_title(node.get("title"))
    current = _clean_text(existing_value)
    if candidate:
        if not current or current == "residential-clean":
            return candidate
        return current
    return current or "residential-clean"


def _quote_is_recurring(node: dict) -> bool:
    jobs = (node.get("jobs") or {}).get("nodes") or []
    return any(job.get("jobType") == "RECURRING" for job in jobs)


class JobberSyncer(BaseSyncer):
    tool_name = "jobber"

    def sync(self, since: Optional[datetime] = None) -> SyncResult:
        is_incremental = since is not None
        start = time.monotonic()
        errors: list[str] = []
        total = 0

        self.logger.info(
            "Starting %s Jobber sync (since=%s)",
            "incremental" if is_incremental else "full",
            since,
        )

        try:
            session = get_client("jobber")
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

        since_iso = since.isoformat() if since else None

        total += self._sync_clients(session, since_iso, errors)
        total += self._sync_jobs(session, since_iso, errors)
        total += self._sync_recurring(session, since_iso, errors)

        self.update_sync_state(total, error=errors[0] if errors else None)
        duration = time.monotonic() - start
        self.logger.info("Jobber sync complete: %d records in %.1fs", total, duration)
        return SyncResult(
            tool_name=self.tool_name,
            records_synced=total,
            errors=errors,
            duration_seconds=duration,
            is_incremental=is_incremental,
        )

    # ------------------------------------------------------------------ #
    # GraphQL helper
    # ------------------------------------------------------------------ #

    _GQL_MAX_RETRIES = 4
    _GQL_BASE_BACKOFF = 5  # seconds; Jobber docs recommend waiting before retry

    def _gql(self, session, query: str, variables: dict) -> dict:
        for attempt in range(self._GQL_MAX_RETRIES):
            JOBBER.wait()
            resp = session.post(
                _GRAPHQL_URL,
                json={"query": query, "variables": variables},
                timeout=30,
            )
            resp.raise_for_status()
            payload = resp.json()

            if "errors" not in payload:
                return payload["data"]

            # Retry on THROTTLED; fail on anything else
            throttled = any(
                (e.get("extensions") or {}).get("code") == "THROTTLED"
                for e in payload["errors"]
            )
            if not throttled:
                raise RuntimeError(payload["errors"])

            backoff = self._GQL_BASE_BACKOFF * (2 ** attempt)
            self.logger.warning(
                "Jobber THROTTLED (attempt %d/%d), backing off %ds",
                attempt + 1, self._GQL_MAX_RETRIES, backoff,
            )
            time.sleep(backoff)

        raise RuntimeError(payload["errors"])

    # ------------------------------------------------------------------ #
    # Clients
    # ------------------------------------------------------------------ #

    def _sync_clients(self, session, since_iso: Optional[str], errors: list) -> int:
        count = 0
        cursor = None
        while True:
            try:
                data = self._gql(session, _CLIENTS_QUERY, {"cursor": cursor})["clients"]
            except Exception as exc:
                errors.append(f"clients page error: {exc}")
                break

            for node in data["nodes"]:
                if since_iso and (node.get("updatedAt") or "") < since_iso:
                    continue
                try:
                    self._upsert_client(node)
                    count += 1
                except Exception as exc:
                    errors.append(f"client {node['id']}: {exc}")

            page = data["pageInfo"]
            if not page["hasNextPage"]:
                break
            cursor = page["endCursor"]

        self.logger.debug("Synced %d clients from Jobber", count)
        return count

    def _upsert_client(self, node: dict) -> None:
        jobber_id = node["id"]
        canonical_id = get_canonical_id(
            "jobber", jobber_id, entity_type="CLIENT", db_path=self.db_path
        )

        emails = node.get("emails") or []
        primary_email = next(
            (e["address"] for e in emails if e.get("primary")), None
        ) or next((e["address"] for e in emails), None)

        phones = node.get("phones") or []
        primary_phone = next(
            (p["number"] for p in phones if p.get("primary")), None
        ) or next((p["number"] for p in phones), None)

        first_name = node.get("firstName") or ""
        last_name = node.get("lastName") or ""

        if canonical_id is None:
            # Check if a client with this email already exists in SQLite
            if primary_email:
                row = self.db.execute(
                    "SELECT id FROM clients WHERE email = %s", (primary_email,)
                ).fetchone()
                if row:
                    canonical_id = row["id"]

            if canonical_id is None:
                canonical_id = generate_id("CLIENT", self.db_path)
                with self.db:
                    self.db.execute(
                        """
                        INSERT INTO clients
                            (id, client_type, first_name, last_name, email, phone, created_at)
                        VALUES (%s, 'residential', %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                        """,
                        (
                            canonical_id,
                            first_name,
                            last_name,
                            primary_email or "",
                            primary_phone,
                            node.get("createdAt"),
                        ),
                    )

            register_mapping(canonical_id, "jobber", jobber_id, db_path=self.db_path)

        # Keep names and phone current
        with self.db:
            self.db.execute(
                """
                UPDATE clients
                SET first_name = %s,
                    last_name  = %s,
                    phone      = COALESCE(%s, phone)
                WHERE id = %s
                """,
                (first_name, last_name, primary_phone, canonical_id),
            )

    # ------------------------------------------------------------------ #
    # Jobs
    # ------------------------------------------------------------------ #

    def _sync_jobs(self, session, since_iso: Optional[str], errors: list) -> int:
        count = 0
        cursor = None
        while True:
            try:
                data = self._gql(session, _JOBS_QUERY, {"cursor": cursor})["jobs"]
            except Exception as exc:
                errors.append(f"jobs page error: {exc}")
                break

            for node in data["nodes"]:
                if since_iso and (node.get("updatedAt") or "") < since_iso:
                    continue
                try:
                    self._upsert_job(node)
                    count += 1
                except Exception as exc:
                    errors.append(f"job {node['id']}: {exc}")

            page = data["pageInfo"]
            if not page["hasNextPage"]:
                break
            cursor = page["endCursor"]

        self.logger.debug("Synced %d jobs from Jobber", count)
        return count

    def _upsert_job(self, node: dict) -> None:
        jobber_id = node["id"]
        canonical_id = get_canonical_id(
            "jobber", jobber_id, entity_type="JOB", db_path=self.db_path
        )

        client_jobber_id = (node.get("client") or {}).get("id")
        # entity_type filter is critical: a Jobber client tool_specific_id can
        # appear on multiple cross_tool_mapping rows (e.g. CLIENT and PROP).
        # Without this filter we may pick up a SS-PROP-* row, then violate the
        # jobs_client_id_fkey FK to clients(id) on insert.
        client_canonical = (
            get_canonical_id(
                "jobber", client_jobber_id, entity_type="CLIENT", db_path=self.db_path
            )
            if client_jobber_id else None
        )
        if client_canonical is None:
            return  # Skip jobs whose client we haven't synced yet

        status = _JOBBER_STATUS_MAP.get(node.get("jobStatus", "ACTIVE"), "scheduled")
        start_at = node.get("startAt") or ""
        scheduled_date = start_at[:10] or None
        scheduled_time = start_at[11:16] or None
        end_at = node.get("endAt")
        title = _clean_text(node.get("title"))
        instructions = _clean_text(node.get("instructions"))
        job_type = _clean_text(node.get("jobType"))
        duration_minutes = _duration_minutes(node)
        is_recurring = _is_recurring_job(node)
        updated_at = _clean_text(node.get("updatedAt"))

        if canonical_id is None:
            service_type_id = _choose_service_type(None, node)
            canonical_id = generate_id("JOB", self.db_path)
            with self.db:
                self.db.execute(
                    """
                    INSERT INTO jobs
                        (id, client_id, service_type_id, job_title_raw,
                         jobber_job_type, scheduled_date, scheduled_time,
                         duration_minutes_actual, status, notes,
                         is_recurring_job, jobber_updated_at, completed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        canonical_id,
                        client_canonical,
                        service_type_id,
                        title,
                        job_type,
                        scheduled_date,
                        scheduled_time,
                        duration_minutes,
                        status,
                        instructions,
                        is_recurring,
                        updated_at,
                        end_at if status == "completed" else None,
                    ),
                )
            register_mapping(canonical_id, "jobber", jobber_id, db_path=self.db_path)
        else:
            row = self.db.execute(
                """
                SELECT service_type_id, job_title_raw, jobber_job_type,
                       scheduled_date, scheduled_time, duration_minutes_actual,
                       status, notes, is_recurring_job, jobber_updated_at, completed_at
                FROM jobs
                WHERE id = %s
                """,
                (canonical_id,),
            ).fetchone()
            if row is None:
                return

            merged_status = "completed" if row["completed_at"] is not None and status == "scheduled" else status
            merged_completed_at = end_at if status == "completed" else row["completed_at"]
            merged_service_type = _choose_service_type(row["service_type_id"], node)
            merged_title = title or row["job_title_raw"]
            merged_job_type = job_type or row["jobber_job_type"]
            merged_scheduled_date = scheduled_date or row["scheduled_date"]
            merged_scheduled_time = scheduled_time or row["scheduled_time"]
            merged_duration = duration_minutes or row["duration_minutes_actual"]
            merged_notes = instructions or row["notes"]
            merged_is_recurring = is_recurring if is_recurring is not None else row["is_recurring_job"]
            merged_updated_at = updated_at or row["jobber_updated_at"]

            with self.db:
                self.db.execute(
                    """
                    UPDATE jobs
                    SET service_type_id         = %s,
                        job_title_raw           = %s,
                        jobber_job_type         = %s,
                        scheduled_date          = %s,
                        scheduled_time          = %s,
                        duration_minutes_actual = %s,
                        status                  = %s,
                        notes                   = %s,
                        is_recurring_job        = %s,
                        jobber_updated_at       = %s,
                        completed_at            = %s
                    WHERE id = %s
                    """,
                    (
                        merged_service_type,
                        merged_title,
                        merged_job_type,
                        merged_scheduled_date,
                        merged_scheduled_time,
                        merged_duration,
                        merged_status,
                        merged_notes,
                        merged_is_recurring,
                        merged_updated_at,
                        merged_completed_at,
                        canonical_id,
                    ),
                )

    # ------------------------------------------------------------------ #
    # Recurring agreements
    # ------------------------------------------------------------------ #

    def _sync_recurring(self, session, since_iso: Optional[str], errors: list) -> int:
        count = 0
        cursor = None
        while True:
            try:
                data = self._gql(session, _RECURRING_QUERY, {"cursor": cursor})["quotes"]
            except Exception as exc:
                errors.append(f"recurring page error: {exc}")
                break

            for node in data["nodes"]:
                if since_iso and (node.get("updatedAt") or "") < since_iso:
                    continue
                if not _quote_is_recurring(node):
                    continue
                try:
                    self._upsert_recurring(node)
                    count += 1
                except Exception as exc:
                    errors.append(f"recurring {node['id']}: {exc}")

            page = data["pageInfo"]
            if not page["hasNextPage"]:
                break
            cursor = page["endCursor"]

        self.logger.debug("Synced %d recurring agreements from Jobber", count)
        return count

    def _upsert_recurring(self, node: dict) -> None:
        jobber_id = node["id"]
        canonical_id = get_canonical_id(
            "jobber", jobber_id, entity_type="RECUR", db_path=self.db_path
        )

        client_jobber_id = (node.get("client") or {}).get("id")
        client_canonical = (
            get_canonical_id(
                "jobber", client_jobber_id, entity_type="CLIENT", db_path=self.db_path
            )
            if client_jobber_id else None
        )
        if client_canonical is None:
            return

        price = float((node.get("amounts") or {}).get("subtotal") or 0.0)

        if canonical_id is None:
            canonical_id = generate_id("RECUR", self.db_path)
            with self.db:
                self.db.execute(
                    """
                    INSERT INTO recurring_agreements
                        (id, client_id, service_type_id, frequency,
                         price_per_visit, start_date, status)
                    VALUES (%s, %s, 'residential-clean', 'biweekly', %s, CURRENT_DATE, 'active')
                    ON CONFLICT DO NOTHING
                    """,
                    (canonical_id, client_canonical, price),
                )
            register_mapping(canonical_id, "jobber", jobber_id, db_path=self.db_path)
        else:
            with self.db:
                self.db.execute(
                    "UPDATE recurring_agreements SET price_per_visit = %s WHERE id = %s",
                    (price, canonical_id),
                )


# ------------------------------------------------------------------ #
# CLI entry point
# ------------------------------------------------------------------ #

if __name__ == "__main__":
    import argparse
    import os
    import sys

    parser = argparse.ArgumentParser(description="Sync Jobber clients/jobs/recurring into SQLite")
    parser.add_argument("--dry-run", action="store_true", help="Auth check + sample fetch; no DB writes")
    parser.add_argument("--since", metavar="YYYY-MM-DD", help="Incremental sync from this date")
    parser.add_argument("--db", default="sparkle_shine.db", help="Path to SQLite database")
    args = parser.parse_args()

    db_path = os.path.abspath(args.db)
    since = datetime.strptime(args.since, "%Y-%m-%d") if args.since else None

    syncer = JobberSyncer(db_path)
    last_sync = syncer.get_last_sync_time()

    print(f"\n[jobber] DB:        {db_path}")
    print(f"[jobber] Last sync: {last_sync or 'never'}")
    print(f"[jobber] Mode:      {'DRY RUN' if args.dry_run else 'LIVE'}")
    if since:
        print(f"[jobber] Since:     {since.date()}")

    if args.dry_run:
        print("\n[jobber] --- Auth check ---")
        try:
            session = get_client("jobber")
            print("[jobber] Auth OK")
        except Exception as exc:
            print(f"[jobber] Auth FAILED: {exc}")
            syncer.close()
            sys.exit(1)

        print("\n[jobber] --- Sample fetch (first 3 clients, no DB writes) ---")
        try:
            JOBBER.wait()
            resp = session.post(
                _GRAPHQL_URL,
                json={
                    "query": """
                        query { clients(first: 3) {
                            nodes { id firstName lastName emails { address primary } updatedAt }
                        } }
                    """
                },
                timeout=30,
            )
            resp.raise_for_status()
            nodes = resp.json().get("data", {}).get("clients", {}).get("nodes", [])
            for node in nodes:
                email = next(
                    (e["address"] for e in (node.get("emails") or []) if e.get("primary")),
                    "—",
                )
                print(f"  [{node['id']}] {node.get('firstName')} {node.get('lastName')} <{email}> (updated {(node.get('updatedAt') or '')[:10]})")
            if not nodes:
                print("  (no clients returned)")
            print(f"\n[jobber] Would sync clients, jobs, and recurring agreements.")
            print(f"[jobber] Run without --dry-run to apply changes.")
        except Exception as exc:
            print(f"[jobber] Sample fetch failed: {exc}")

        syncer.close()
        sys.exit(0)

    result = syncer.sync(since=since)
    syncer.close()

    print(f"\n[jobber] Synced {result.records_synced} records in {result.duration_seconds:.1f}s")
    if result.errors:
        print(f"[jobber] {len(result.errors)} error(s):")
        for err in result.errors[:10]:
            print(f"  - {err}")
    sys.exit(1 if result.errors else 0)
