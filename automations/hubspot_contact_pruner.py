"""
automations/hubspot_contact_pruner.py

Automation — HubSpot Contact Pruner (scheduled, daily)

HubSpot's free tier hard-caps the account at 1,000 total contacts. Once full,
no new contacts can be created. This job keeps the account under the cap by
archiving disposable contacts from HubSpot — WITHOUT losing the canonical
record (which lives in Postgres: clients / leads + cross_tool_mapping) and
WITHOUT touching active customers.

Each run:
  1. Read the live HubSpot contact count. If at/below PRUNE_THRESHOLD, do nothing.
  2. Compute how many to delete: min(MAX_DELETIONS_PER_RUN, live_total - PRUNE_TARGET).
  3. Select the OLDEST contacts (by created_at) drawn ONLY from the safe pool:
       - churned clients (status='churned') not on an active recurring agreement
       - dead leads (status='lost', or stale status='new' with no activity)
     Active clients and contacted/qualified leads are never eligible.
  4. For each: archive the HubSpot contact (404 → treated as already-gone), then
     delete its 'hubspot' row from cross_tool_mapping (so the reconciler doesn't
     flag a dangling "mapped but deleted" contact). The clients/leads row stays.
  5. If the safe pool is too small to reach the target, prune what is safe and
     post a Slack alert — never delete active customers to hit the number.

This automation rides the existing automation-runner service via its
--scheduled mode, gated to once / 24h by a sentinel file (see runner.py).
"""
import os
import sys
from typing import Optional

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from automations.base import BaseAutomation
from database.mappings import delete_mapping_on_conn
from seeding.utils.throttler import HUBSPOT

# ─────────────────────────────────────────────────────────────────────────────
# Constants (operational policy local to this automation)
# ─────────────────────────────────────────────────────────────────────────────

HUBSPOT_CONTACT_CAP   = 1000   # free-tier hard cap (documentation only)
PRUNE_THRESHOLD       = 900    # only prune when the live count exceeds this
PRUNE_TARGET          = 850    # prune back down toward this (headroom so it
                               # doesn't re-trigger every day at 901)
MAX_DELETIONS_PER_RUN = 10

_SLACK_CHANNEL = "operations"


# ─────────────────────────────────────────────────────────────────────────────
# Safe-candidate SQL
# ─────────────────────────────────────────────────────────────────────────────
#
# The safe pool is the UNION of churned clients and dead leads, each JOINed to
# its 'hubspot' mapping so we only ever consider contacts that actually exist in
# HubSpot. created_at is zero-padded ISO-8601 TEXT, so lexical ORDER BY is
# chronological. Active clients are excluded by status='churned'; the
# recurring_agreements NOT EXISTS guard is belt-and-suspenders; 'contacted' /
# 'qualified' leads are excluded by construction.

_SAFE_POOL_FROM = """
FROM (
    SELECT c.id AS canonical_id, 'CLIENT' AS entity_type,
           c.created_at AS created_at, m.tool_specific_id
    FROM clients c
    JOIN cross_tool_mapping m
      ON m.canonical_id = c.id AND m.tool_name = 'hubspot' AND m.entity_type = 'CLIENT'
    WHERE c.status = 'churned'
      AND NOT EXISTS (
          SELECT 1 FROM recurring_agreements ra
          WHERE ra.client_id = c.id AND ra.status = 'active'
      )
    UNION ALL
    SELECT l.id AS canonical_id, 'LEAD' AS entity_type,
           l.created_at AS created_at, m.tool_specific_id
    FROM leads l
    JOIN cross_tool_mapping m
      ON m.canonical_id = l.id AND m.tool_name = 'hubspot' AND m.entity_type = 'LEAD'
    WHERE l.status = 'lost'
       OR (l.status = 'new' AND l.last_activity_at IS NULL)
) AS safe_pool
"""

_SELECT_SAFE_CANDIDATES = (
    "SELECT canonical_id, entity_type, tool_specific_id AS hubspot_id, created_at "
    + _SAFE_POOL_FROM
    + "ORDER BY created_at ASC LIMIT %s"
)

_COUNT_SAFE_POOL = "SELECT COUNT(*) AS cnt " + _SAFE_POOL_FROM


# ─────────────────────────────────────────────────────────────────────────────
# Main class
# ─────────────────────────────────────────────────────────────────────────────

class HubSpotContactPruner(BaseAutomation):
    """Scheduled daily automation: archive oldest safe-pool contacts from
    HubSpot to keep the free-tier account under its 1,000-contact cap."""

    def run(self) -> dict:
        run_id         = self.generate_run_id()
        trigger_source = "scheduled:hubspot_contact_pruner"
        results        = {"processed": 0, "succeeded": 0, "failed": 0}

        # ── Step 1: live count ────────────────────────────────────────────────
        try:
            live_total = self._get_live_contact_count()
        except Exception as exc:
            self.log_action(
                run_id, "check_contact_count", None, "failed",
                error_message=str(exc), trigger_source=trigger_source,
            )
            results["failed"] += 1
            return results

        # ── Step 2: threshold gate ────────────────────────────────────────────
        if live_total <= PRUNE_THRESHOLD:
            # status is constrained to success/failed/skipped; "skipped" == nothing to prune.
            self.log_action(
                run_id, "check_contact_count",
                f"hubspot:contacts:{live_total}", "skipped",
                trigger_source=trigger_source,
                trigger_detail={"live_total": live_total, "threshold": PRUNE_THRESHOLD},
            )
            return results

        to_delete = min(MAX_DELETIONS_PER_RUN, live_total - PRUNE_TARGET)
        self.log_action(
            run_id, "check_contact_count",
            f"hubspot:contacts:{live_total}", "success",
            trigger_source=trigger_source,
            trigger_detail={
                "live_total": live_total, "threshold": PRUNE_THRESHOLD,
                "target": PRUNE_TARGET, "to_delete": to_delete,
            },
        )

        # ── Step 3: select safe candidates ────────────────────────────────────
        pool_size  = self._count_safe_pool()
        deletable  = min(to_delete, pool_size)
        candidates = self._select_safe_candidates(deletable) if deletable > 0 else []
        self.log_action(
            run_id, "select_safe_candidates",
            f"safe_pool:{pool_size}", "success",
            trigger_source=trigger_source,
            trigger_detail={"pool_size": pool_size, "selected": len(candidates)},
        )

        # ── Step 4: archive + unmap each candidate ────────────────────────────
        deleted = 0
        for cand in candidates:
            results["processed"] += 1
            hubspot_id   = cand["hubspot_id"]
            canonical_id = cand["canonical_id"]
            try:
                outcome = self._archive_hubspot_contact(hubspot_id)
                self._delete_mapping(canonical_id)
                # 404 (already gone) is still a success; the nuance lives in trigger_detail.
                self.log_action(
                    run_id, "archive_hubspot_contact",
                    f"hubspot:contact:{hubspot_id}", "success",
                    trigger_source=trigger_source,
                    trigger_detail={"canonical_id": canonical_id, "outcome": outcome},
                )
                self.log_action(
                    run_id, "delete_cross_tool_mapping",
                    f"{canonical_id}:hubspot", "success",
                    trigger_source=trigger_source,
                )
                deleted += 1
                results["succeeded"] += 1
            except Exception as exc:
                # Non-404 archive failure: leave the mapping intact so the next
                # daily run retries; never orphan a mapping on a failed archive.
                self.log_action(
                    run_id, "archive_hubspot_contact",
                    f"hubspot:contact:{hubspot_id}", "failed",
                    error_message=str(exc), trigger_source=trigger_source,
                    trigger_detail={"canonical_id": canonical_id},
                )
                results["failed"] += 1

        # ── Step 5: safe-pool exhaustion alert ────────────────────────────────
        if pool_size < to_delete:
            shortfall = to_delete - deleted
            self._alert_safe_pool_exhausted(live_total, to_delete, pool_size, deleted)
            # "skipped" == we deliberately did NOT delete the remaining shortfall
            # (no safe candidates left); details in trigger_detail + the Slack alert.
            self.log_action(
                run_id, "safe_pool_exhausted",
                f"hubspot:contacts:{live_total}", "skipped",
                trigger_source=trigger_source,
                trigger_detail={
                    "live_total": live_total, "to_delete": to_delete,
                    "pool_size": pool_size, "deleted": deleted, "shortfall": shortfall,
                },
            )

        return results

    # ── Step 1 ────────────────────────────────────────────────────────────────

    def _get_live_contact_count(self) -> int:
        """Return the live count of non-archived HubSpot contacts.

        Uses the search API with limit=1; the server returns the full ``total``
        without paging. Search returns non-archived contacts only — exactly the
        set that counts against the free-tier cap.
        """
        from hubspot.crm.contacts import PublicObjectSearchRequest

        if self.dry_run:
            # Read-only call is safe in dry-run; still hit the live API so the
            # operator sees the real count.
            pass

        HUBSPOT.wait()
        client = self.clients("hubspot")
        resp = client.crm.contacts.search_api.do_search(
            public_object_search_request=PublicObjectSearchRequest(limit=1),
            _request_timeout=30,
        )
        return int(resp.total)

    # ── Step 3 ────────────────────────────────────────────────────────────────

    def _count_safe_pool(self) -> int:
        row = self.db.execute(_COUNT_SAFE_POOL).fetchone()
        return int(row["cnt"]) if row else 0

    def _select_safe_candidates(self, limit: int) -> list:
        if limit <= 0:
            return []
        return list(self.db.execute(_SELECT_SAFE_CANDIDATES, (limit,)).fetchall())

    # ── Step 4 ────────────────────────────────────────────────────────────────

    def _archive_hubspot_contact(self, hubspot_id: str) -> str:
        """Archive a HubSpot contact (removes it from the active count).

        Returns "archived" on success, or "not_found" if HubSpot reports the
        contact is already gone (404) — treated as success so we still clean up
        the dangling mapping. Re-raises any other API error.
        """
        if self.dry_run:
            print(f"[DRY RUN] Would archive HubSpot contact {hubspot_id}")
            return "archived"

        HUBSPOT.wait()
        client = self.clients("hubspot")
        try:
            client.crm.contacts.basic_api.archive(hubspot_id)
            return "archived"
        except Exception as exc:
            if getattr(exc, "status", None) == 404:
                return "not_found"
            raise

    def _delete_mapping(self, canonical_id: str) -> None:
        """Remove the 'hubspot' cross_tool_mapping row (canonical record stays)."""
        if self.dry_run:
            print(f"[DRY RUN] Would delete hubspot mapping for {canonical_id}")
            return
        with self.db:
            delete_mapping_on_conn(self.db, canonical_id, "hubspot")

    # ── Step 5 ────────────────────────────────────────────────────────────────

    def _alert_safe_pool_exhausted(
        self, live_total: int, to_delete: int, pool_size: int, deleted: int
    ) -> None:
        shortfall = to_delete - deleted
        text = (
            f":warning: *HubSpot near contact cap*: {live_total}/{HUBSPOT_CONTACT_CAP}. "
            f"Target {PRUNE_TARGET}; needed to remove {to_delete}, but the safe pool "
            f"(churned clients + dead leads) only had {pool_size}. "
            f"Archived {deleted}; {shortfall} short.\n"
            f"Manual review needed — *not* deleting active customers. "
            f"Consider a HubSpot upgrade or reducing what is synced to HubSpot."
        )
        self.send_slack(_SLACK_CHANNEL, text)
