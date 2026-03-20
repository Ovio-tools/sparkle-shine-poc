"""
HubSpot syncer -- pulls contacts and deals into SQLite.

Contacts are classified as clients or leads based on the client_type property.
Deals are upserted into commercial_proposals.
"""
import time
from datetime import datetime
from typing import Optional

from auth import get_client
from database.mappings import get_canonical_id, register_mapping, generate_id
from intelligence.syncers.base_syncer import BaseSyncer, SyncResult
from seeding.utils.throttler import HUBSPOT

_CONTACT_PROPERTIES = [
    "email",
    "firstname",
    "lastname",
    "lifecyclestage",
    "client_type",
    "service_frequency",
    "lead_source_detail",
    "neighborhood",
    "lifetime_value",
    "last_service_date",
    "hs_object_id",
    "lastmodifieddate",
]

_DEAL_PROPERTIES = [
    "dealname",
    "amount",
    "dealstage",
    "closedate",
    "pipeline",
    "proposal_id",
    "service_start_date",
    "monthly_contract_value",
    "hs_object_id",
    "hs_lastmodifieddate",
]

# HubSpot deal stage → canonical proposal status
_STAGE_STATUS_MAP = {
    "appointmentscheduled":  "draft",
    "qualifiedtobuy":        "draft",
    "presentationscheduled": "sent",
    "decisionmakerboughtin": "negotiating",
    "contractsent":          "sent",
    "closedwon":             "won",
    "closedlost":            "lost",
}


class HubSpotSyncer(BaseSyncer):
    tool_name = "hubspot"

    def sync(self, since: Optional[datetime] = None) -> SyncResult:
        is_incremental = since is not None
        start = time.monotonic()
        errors: list[str] = []
        total = 0

        self.logger.info(
            "Starting %s HubSpot sync (since=%s)",
            "incremental" if is_incremental else "full",
            since,
        )

        try:
            client = get_client("hubspot")
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

        # HubSpot filter timestamp is in milliseconds
        since_ms = int(since.timestamp() * 1000) if since else 0

        total += self._sync_contacts(client, since_ms, errors)
        total += self._sync_deals(client, since_ms, errors)

        self.update_sync_state(total, error=errors[0] if errors else None)
        duration = time.monotonic() - start
        self.logger.info("HubSpot sync complete: %d records in %.1fs", total, duration)
        return SyncResult(
            tool_name=self.tool_name,
            records_synced=total,
            errors=errors,
            duration_seconds=duration,
            is_incremental=is_incremental,
        )

    # ------------------------------------------------------------------ #
    # Contacts
    # ------------------------------------------------------------------ #

    def _sync_contacts(self, client, since_ms: int, errors: list) -> int:
        from hubspot.crm.contacts import PublicObjectSearchRequest

        count = 0
        after = None

        while True:
            HUBSPOT.wait()
            try:
                filters = []
                if since_ms > 0:
                    filters.append({
                        "propertyName": "lastmodifieddate",
                        "operator": "GTE",
                        "value": str(since_ms),
                    })

                search_req = PublicObjectSearchRequest(
                    filter_groups=[{"filters": filters}] if filters else [],
                    properties=_CONTACT_PROPERTIES,
                    limit=100,
                    after=after,
                )
                resp = client.crm.contacts.search_api.do_search(
                    public_object_search_request=search_req
                )
            except Exception as exc:
                errors.append(f"contacts search error: {exc}")
                break

            for contact in (resp.results or []):
                try:
                    self._upsert_contact(contact)
                    count += 1
                except Exception as exc:
                    errors.append(f"contact {contact.id}: {exc}")

            paging = resp.paging
            if paging and paging.next and paging.next.after:
                after = paging.next.after
            else:
                break

        self.logger.debug("Synced %d contacts from HubSpot", count)
        return count

    def _upsert_contact(self, contact) -> None:
        hs_id = str(contact.id)
        props = contact.properties or {}

        email = props.get("email") or ""
        first_name = props.get("firstname") or ""
        last_name = props.get("lastname") or ""
        client_type_prop = (props.get("client_type") or "").lower()
        lifecycle = props.get("lifecyclestage") or ""
        source = props.get("lead_source_detail") or ""
        neighborhood = props.get("neighborhood") or ""
        lifetime_value = float(props.get("lifetime_value") or 0)
        last_service = props.get("last_service_date")

        canonical_id = get_canonical_id("hubspot", hs_id, self.db_path)

        # Determine whether this is a client or a lead
        is_client = client_type_prop in ("residential", "commercial") or lifecycle == "customer"

        if is_client:
            if canonical_id is None:
                # Try to match by email
                row = self.db.execute(
                    "SELECT id FROM clients WHERE email = ?", (email,)
                ).fetchone() if email else None

                if row:
                    canonical_id = row["id"]
                else:
                    canonical_id = generate_id("CLIENT", self.db_path)
                    with self.db:
                        self.db.execute(
                            """
                            INSERT OR IGNORE INTO clients
                                (id, client_type, first_name, last_name, email,
                                 neighborhood, acquisition_source, lifetime_value)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                canonical_id,
                                client_type_prop or "residential",
                                first_name, last_name, email,
                                neighborhood, source, lifetime_value,
                            ),
                        )
                register_mapping(canonical_id, "hubspot", hs_id, db_path=self.db_path)

            with self.db:
                self.db.execute(
                    """
                    UPDATE clients
                    SET lifetime_value    = MAX(lifetime_value, ?),
                        last_service_date = COALESCE(?, last_service_date),
                        neighborhood      = COALESCE(NULLIF(?, ''), neighborhood),
                        acquisition_source = COALESCE(NULLIF(?, ''), acquisition_source)
                    WHERE id = ?
                    """,
                    (lifetime_value, last_service, neighborhood, source, canonical_id),
                )
        else:
            # Treat as a lead
            if canonical_id is None:
                row = self.db.execute(
                    "SELECT id FROM leads WHERE email = ?", (email,)
                ).fetchone() if email else None

                if row:
                    canonical_id = row["id"]
                else:
                    canonical_id = generate_id("LEAD", self.db_path)
                    lead_type = client_type_prop if client_type_prop in ("residential", "commercial") else "residential"
                    with self.db:
                        self.db.execute(
                            """
                            INSERT OR IGNORE INTO leads
                                (id, first_name, last_name, email, lead_type, source, status)
                            VALUES (?, ?, ?, ?, ?, ?, 'new')
                            """,
                            (canonical_id, first_name, last_name, email, lead_type, source),
                        )
                register_mapping(canonical_id, "hubspot", hs_id, db_path=self.db_path)

    # ------------------------------------------------------------------ #
    # Deals
    # ------------------------------------------------------------------ #

    def _sync_deals(self, client, since_ms: int, errors: list) -> int:
        from hubspot.crm.deals import PublicObjectSearchRequest

        count = 0
        after = None

        while True:
            HUBSPOT.wait()
            try:
                filters = []
                if since_ms > 0:
                    filters.append({
                        "propertyName": "hs_lastmodifieddate",
                        "operator": "GTE",
                        "value": str(since_ms),
                    })

                search_req = PublicObjectSearchRequest(
                    filter_groups=[{"filters": filters}] if filters else [],
                    properties=_DEAL_PROPERTIES,
                    limit=100,
                    after=after,
                )
                resp = client.crm.deals.search_api.do_search(
                    public_object_search_request=search_req
                )
            except Exception as exc:
                errors.append(f"deals search error: {exc}")
                break

            for deal in (resp.results or []):
                try:
                    self._upsert_deal(deal)
                    count += 1
                except Exception as exc:
                    errors.append(f"deal {deal.id}: {exc}")

            paging = resp.paging
            if paging and paging.next and paging.next.after:
                after = paging.next.after
            else:
                break

        self.logger.debug("Synced %d deals from HubSpot", count)
        return count

    def _upsert_deal(self, deal) -> None:
        hs_id = str(deal.id)
        props = deal.properties or {}

        title = props.get("dealname") or "Untitled Deal"
        amount = float(props.get("amount") or 0)
        monthly_value = float(props.get("monthly_contract_value") or amount)
        stage = (props.get("dealstage") or "").lower()
        close_date = props.get("closedate", "")[:10] if props.get("closedate") else None
        status = _STAGE_STATUS_MAP.get(stage, "draft")

        canonical_id = get_canonical_id("hubspot", hs_id, self.db_path)

        if canonical_id is None:
            canonical_id = generate_id("PROP", self.db_path)
            with self.db:
                self.db.execute(
                    """
                    INSERT OR IGNORE INTO commercial_proposals
                        (id, title, monthly_value, status, decision_date)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (canonical_id, title, monthly_value, status, close_date),
                )
            register_mapping(canonical_id, "hubspot", hs_id, db_path=self.db_path)
        else:
            with self.db:
                self.db.execute(
                    """
                    UPDATE commercial_proposals
                    SET status        = ?,
                        monthly_value = COALESCE(NULLIF(?, 0), monthly_value),
                        decision_date = COALESCE(?, decision_date)
                    WHERE id = ?
                    """,
                    (status, monthly_value, close_date, canonical_id),
                )


# ------------------------------------------------------------------ #
# CLI entry point
# ------------------------------------------------------------------ #

if __name__ == "__main__":
    import argparse
    import os
    import sys

    parser = argparse.ArgumentParser(description="Sync HubSpot contacts/deals into SQLite")
    parser.add_argument("--dry-run", action="store_true", help="Auth check + sample fetch; no DB writes")
    parser.add_argument("--since", metavar="YYYY-MM-DD", help="Incremental sync from this date")
    parser.add_argument("--db", default="sparkle_shine.db", help="Path to SQLite database")
    args = parser.parse_args()

    db_path = os.path.abspath(args.db)
    since = datetime.strptime(args.since, "%Y-%m-%d") if args.since else None

    syncer = HubSpotSyncer(db_path)
    last_sync = syncer.get_last_sync_time()

    print(f"\n[hubspot] DB:        {db_path}")
    print(f"[hubspot] Last sync: {last_sync or 'never'}")
    print(f"[hubspot] Mode:      {'DRY RUN' if args.dry_run else 'LIVE'}")
    if since:
        print(f"[hubspot] Since:     {since.date()}")

    if args.dry_run:
        print("\n[hubspot] --- Auth check ---")
        try:
            client = get_client("hubspot")
            print("[hubspot] Auth OK")
        except Exception as exc:
            print(f"[hubspot] Auth FAILED: {exc}")
            syncer.close()
            sys.exit(1)

        print("\n[hubspot] --- Sample fetch (first 3 contacts, no DB writes) ---")
        try:
            from hubspot.crm.contacts import PublicObjectSearchRequest

            HUBSPOT.wait()
            req = PublicObjectSearchRequest(
                filter_groups=[],
                properties=["email", "firstname", "lastname", "lifecyclestage", "client_type"],
                limit=3,
            )
            resp = client.crm.contacts.search_api.do_search(
                public_object_search_request=req
            )
            for c in (resp.results or []):
                p = c.properties or {}
                print(
                    f"  [{c.id}] {p.get('firstname')} {p.get('lastname')} "
                    f"<{p.get('email')}> [{p.get('lifecyclestage')} / {p.get('client_type')}]"
                )
            if not resp.results:
                print("  (no contacts returned)")

            print("\n[hubspot] --- Sample fetch (first 3 deals) ---")
            HUBSPOT.wait()
            from hubspot.crm.deals import PublicObjectSearchRequest as DealSearchRequest
            dreq = DealSearchRequest(
                filter_groups=[],
                properties=["dealname", "amount", "dealstage"],
                limit=3,
            )
            dresp = client.crm.deals.search_api.do_search(
                public_object_search_request=dreq
            )
            for d in (dresp.results or []):
                p = d.properties or {}
                print(f"  [{d.id}] {p.get('dealname')} — ${p.get('amount', 0)} [{p.get('dealstage')}]")
            if not dresp.results:
                print("  (no deals returned)")

            print(f"\n[hubspot] Would sync contacts → clients/leads and deals → proposals.")
            print(f"[hubspot] Run without --dry-run to apply changes.")
        except Exception as exc:
            print(f"[hubspot] Sample fetch failed: {exc}")

        syncer.close()
        sys.exit(0)

    result = syncer.sync(since=since)
    syncer.close()

    print(f"\n[hubspot] Synced {result.records_synced} records in {result.duration_seconds:.1f}s")
    if result.errors:
        print(f"[hubspot] {len(result.errors)} error(s):")
        for err in result.errors[:10]:
            print(f"  - {err}")
    sys.exit(1 if result.errors else 0)
