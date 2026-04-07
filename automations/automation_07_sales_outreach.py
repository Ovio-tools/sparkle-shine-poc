"""
automations/automation_07_sales_outreach.py

Automation 7 — Inbound Lead Reply

Polls HubSpot for new contacts (lifecyclestage = 'lead' or 'subscriber'),
synthesises a personalised inbound reply email draft, then creates a Gmail
draft and notifies the #sales Slack channel.

Steps:
  1. Poll HubSpot for new contacts since last watermark
  2. Skip contacts already mapped with entity_type='DRAFT' (duplicate guard)
  3. For each new contact:
     a. Run email synthesis agent
     b. Create Gmail draft
     c. Post Slack success/error notification to #sales
     d. Write outreach_drafts record to PostgreSQL
     e. Register cross_tool_mapping entries (gmail + hubspot)
  4. Update poll_state watermark to newest createdate seen
"""
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from hubspot.crm.contacts import PublicObjectSearchRequest

from automations.agents.email_synthesis_agent import synthesize_email
from automations.agents.similar_jobs_agent import find_similar_jobs
from automations.base import BaseAutomation
from automations.helpers.gmail_draft import create_gmail_draft
from automations.helpers.slack_sales_notify import notify_sales_channel
from automations.state import get_last_poll, update_last_poll

logger = logging.getLogger("automation_07")

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

_POLL_TOOL   = "hubspot"
_POLL_ENTITY = "OUTREACH_CONTACT"

# Hard ceiling on how far back the watermark may look. Contacts older than
# this are never eligible for outreach — even on the first run or if the
# watermark is somehow cleared/reset. Keeps the automation focused on
# net-new contacts and prevents a stale watermark from triggering mass outreach.
_MAX_LOOKBACK_HOURS = 1

# Throttle between contacts when the batch is large
_BATCH_DELAY_THRESHOLD = 10     # contacts
_BATCH_DELAY_SECONDS   = 5      # seconds

# Warn when per-lead cost estimate exceeds this amount
_COST_WARN_THRESHOLD = 0.50     # USD

# ── Token/cost estimates (flat, per-lead) ─────────────────────────────────────
# Agent 3 (email synthesis): claude-sonnet-4-6.
# Pricing (per million tokens): Sonnet input $3, output $15.
_A3_IN, _A3_OUT     = 500, 300   # Sonnet — short prompt, short output
_SONNET_IN_RATE     = 3.0 / 1_000_000
_SONNET_OUT_RATE    = 15.0 / 1_000_000


def _estimate_tokens_and_cost() -> Tuple[int, float]:
    """Return (total_tokens, estimated_cost_usd) using flat per-agent estimates."""
    cost   = _A3_IN * _SONNET_IN_RATE + _A3_OUT * _SONNET_OUT_RATE
    tokens = _A3_IN + _A3_OUT
    return tokens, round(cost, 4)


# ─────────────────────────────────────────────────────────────────────────────
# Main class
# ─────────────────────────────────────────────────────────────────────────────

class SalesOutreachAutomation(BaseAutomation):
    """
    Automation 7: poll HubSpot for new leads/subscribers, run the three-agent
    outreach chain, create a Gmail draft, and notify #sales.

    The base class provides self.clients (callable → SDK client),
    self.db (psycopg2 connection, RealDictCursor), and self.dry_run.
    """

    def run(self) -> None:
        run_id         = self.generate_run_id()
        trigger_source = "poll:hubspot_new_contacts"

        # ── Step 1: Read watermark ────────────────────────────────────────────
        cutoff_ms = self._read_watermark()

        # ── Step 2: Poll HubSpot ─────────────────────────────────────────────
        try:
            contacts, newest_ms = self._fetch_new_contacts(cutoff_ms)
            logger.info(
                "HubSpot poll found %d new lead/subscriber contact(s) since watermark.",
                len(contacts),
            )
            self.log_action(
                run_id, "fetch_hubspot_contacts",
                f"hubspot:contacts:{len(contacts)}",
                "success",
                trigger_source=trigger_source,
                trigger_detail={"count": len(contacts), "cutoff_ms": cutoff_ms},
            )
        except Exception as exc:
            logger.error("HubSpot poll failed: %s", exc)
            self.log_action(
                run_id, "fetch_hubspot_contacts", None, "failed",
                error_message=str(exc), trigger_source=trigger_source,
            )
            return

        if not contacts:
            logger.info("No new contacts to process.")
            # Still advance the watermark if we saw contacts but filtered them all
            if newest_ms > cutoff_ms and not self.dry_run:
                self._write_watermark(newest_ms)
            return

        # ── Step 3: Process each contact ─────────────────────────────────────
        processed = 0
        last_written_ms = cutoff_ms
        for i, contact in enumerate(contacts):
            if len(contacts) >= _BATCH_DELAY_THRESHOLD and i > 0:
                time.sleep(_BATCH_DELAY_SECONDS)

            try:
                self._process_contact(contact, run_id, trigger_source)
                processed += 1
            except Exception as exc:
                logger.error(
                    "Unhandled error processing contact %s (%s): %s",
                    contact.get("hubspot_id"),
                    contact.get("email"),
                    exc,
                )
                self.log_action(
                    run_id, "process_contact",
                    contact.get("hubspot_id"),
                    "failed",
                    error_message=str(exc),
                    trigger_source=trigger_source,
                )

            # Advance watermark after each contact so a mid-batch failure
            # doesn't cause the entire batch to be reprocessed next run.
            contact_ms = contact.get("createdate_ms", 0)
            if not self.dry_run and contact_ms > last_written_ms:
                self._write_watermark(contact_ms)
                last_written_ms = contact_ms

        logger.info(
            "Automation 07 complete: %d/%d contact(s) processed.",
            processed, len(contacts),
        )

        # ── Step 4: Ensure watermark is at newest_ms (catches any gaps) ──────
        if not self.dry_run and newest_ms > last_written_ms:
            self._write_watermark(newest_ms)

    # ── Watermark helpers ─────────────────────────────────────────────────────

    def _read_watermark(self) -> int:
        """Return the last-poll cutoff as a ms-since-epoch integer.

        The returned value is always at least as recent as _MAX_LOOKBACK_HOURS
        ago. This means:
        - Normal 5-min poll runs use their stored watermark (a few minutes old),
          which is more recent than the 24h floor, so nothing changes.
        - First run (no watermark) starts from 24h ago, not 30 days.
        - If the watermark is ever stale or reset, the 24h floor kicks in and
          prevents the automation from bulk-processing old contacts.
        """
        floor_ms = int(
            (datetime.now(timezone.utc) - timedelta(hours=_MAX_LOOKBACK_HOURS))
            .timestamp() * 1000
        )
        state = get_last_poll(self.db, _POLL_TOOL, _POLL_ENTITY)
        if state and state.get("last_processed_timestamp"):
            ts_str = state["last_processed_timestamp"]
            try:
                dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                stored_ms = int(dt.timestamp() * 1000)
                # Take whichever is more recent: the stored watermark or the 24h floor.
                # max() here means "don't go further back than 24h no matter what".
                return max(stored_ms, floor_ms)
            except (ValueError, AttributeError):
                pass
        # First run or unreadable watermark: use the 24h floor
        return floor_ms

    def _write_watermark(self, newest_ms: int) -> None:
        ts = datetime.fromtimestamp(newest_ms / 1000, tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        update_last_poll(
            self.db, _POLL_TOOL, _POLL_ENTITY,
            last_id=None,
            last_timestamp=ts,
        )
        logger.info("Watermark advanced to %s.", ts)

    # ── HubSpot polling ───────────────────────────────────────────────────────

    def _fetch_new_contacts(self, cutoff_ms: int) -> Tuple[List[Dict], int]:
        """
        Query HubSpot for contacts with lifecyclestage='lead' OR 'subscriber'
        created after cutoff_ms (which is never older than 24h — see _read_watermark).
        Applies _should_skip_contact to exclude existing clients, pipeline contacts,
        previously attempted contacts, and contacts with no email.
        Returns (contacts_to_process, newest_createdate_ms_seen).
        """
        if self.dry_run:
            logger.info(
                "[DRY RUN] Would POST /crm/v3/objects/contacts/search "
                "(lifecyclestage in [lead, subscriber], createdate > %d).", cutoff_ms,
            )
            return _DRY_RUN_CONTACTS, cutoff_ms

        search_request = PublicObjectSearchRequest(
            filter_groups=[
                {
                    "filters": [
                        {"propertyName": "createdate",     "operator": "GT", "value": str(cutoff_ms)},
                        {"propertyName": "lifecyclestage", "operator": "EQ", "value": "lead"},
                    ]
                },
                {
                    "filters": [
                        {"propertyName": "createdate",     "operator": "GT", "value": str(cutoff_ms)},
                        {"propertyName": "lifecyclestage", "operator": "EQ", "value": "subscriber"},
                    ]
                },
            ],
            properties=[
                "email", "firstname", "lastname", "company",
                "address", "city", "zip", "contact_type", "client_type",
                "lead_source_detail", "hs_analytics_source",
                "hs_analytics_source_data_1", "service_interest",
                "lifecyclestage", "createdate",
            ],
            sorts=[{"propertyName": "createdate", "direction": "ASCENDING"}],
            limit=100,
        )

        hs_client = self.clients("hubspot")
        response  = hs_client.crm.contacts.search_api.do_search(
            search_request, _request_timeout=30
        )
        raw_results = response.results or []

        newest_ms = cutoff_ms
        contacts  = []
        seen_ids: set = set()

        for item in raw_results:
            props      = item.properties or {}
            hubspot_id = str(item.id)

            # Track the newest createdate we saw (for watermark)
            item_ms = 0
            try:
                item_ms = int(props.get("createdate") or 0)
                if item_ms > newest_ms:
                    newest_ms = item_ms
            except (TypeError, ValueError):
                pass

            email = props.get("email") or ""

            # Deduplicate: HubSpot OR filterGroups can return the same contact twice
            if hubspot_id in seen_ids:
                logger.debug("Deduplicating contact %s — seen twice in API results.", hubspot_id)
                continue
            seen_ids.add(hubspot_id)

            # Safety: skip contacts that should not receive outreach
            should_skip, skip_reason = self._should_skip_contact(hubspot_id, email)
            if should_skip:
                logger.debug(
                    "Skipping contact %s (%s) — %s.", hubspot_id, email, skip_reason
                )
                continue

            contacts.append({
                "hubspot_id":               hubspot_id,
                "firstname":                props.get("firstname")               or "",
                "lastname":                 props.get("lastname")                or "",
                "email":                    email,
                "company":                  props.get("company")                 or "",
                "address":                  props.get("address")                 or "",
                "city":                     props.get("city")                    or "",
                "zip":                      props.get("zip")                     or "",
                "contact_type": (
                    props.get("contact_type") or props.get("client_type")        or ""
                ),
                "lead_source": (
                    props.get("lead_source_detail")
                    or props.get("hs_analytics_source")                          or ""
                ),
                "hs_analytics_source":      props.get("hs_analytics_source")     or "",
                "hs_analytics_source_data_1": (
                    props.get("hs_analytics_source_data_1")                      or ""
                ),
                "service_interest":         props.get("service_interest")        or "",
                "lifecyclestage":           props.get("lifecyclestage")          or "",
                # Stored so we can advance the watermark per-contact (not just at the end)
                "createdate_ms":            item_ms,
            })

        return contacts, newest_ms

    def _should_skip_contact(self, hubspot_id: str, email: str) -> Tuple[bool, str]:
        """
        Return (True, reason) if this contact must NOT receive outreach.

        Checks in priority order:
        1. No email address — nothing to send to
        2. DRAFT mapping exists — already processed successfully
        3. outreach_drafts record exists — already attempted (even if failed),
           prevents retry-spam when agents hit rate limits
        4. entity_type=CLIENT in cross_tool_mapping — this person is an
           existing customer; sending a prospect email is a trust violation
        5. Active record in clients table by email — belt-and-suspenders
           guard against stale or incomplete cross_tool_mapping entries
        6. Has a Pipedrive mapping — actively being worked by a salesperson;
           a generic outreach email would step on that relationship
        """
        # 1. No email
        if not email:
            return True, "no email address"

        # 2. Already processed (DRAFT mapping present)
        row = self.db.execute(
            """
            SELECT 1 FROM cross_tool_mapping
            WHERE entity_type = 'DRAFT'
              AND tool_name = 'hubspot'
              AND tool_specific_id = %s
            LIMIT 1
            """,
            (hubspot_id,),
        ).fetchone()
        if row:
            return True, "DRAFT mapping already exists"

        # 3. Any previous outreach attempt (including failed ones)
        row = self.db.execute(
            "SELECT 1 FROM outreach_drafts WHERE hubspot_contact_id = %s LIMIT 1",
            (hubspot_id,),
        ).fetchone()
        if row:
            return True, "outreach already attempted"

        # 4. Existing client by HubSpot ID (entity_type = CLIENT)
        row = self.db.execute(
            """
            SELECT 1 FROM cross_tool_mapping
            WHERE entity_type = 'CLIENT'
              AND tool_name = 'hubspot'
              AND tool_specific_id = %s
            LIMIT 1
            """,
            (hubspot_id,),
        ).fetchone()
        if row:
            return True, "existing client (CLIENT mapping in cross_tool_mapping)"

        # 5. Active client by email in clients table (stale-mapping safety net)
        row = self.db.execute(
            """
            SELECT 1 FROM clients
            WHERE email = %s
              AND (status IS NULL OR status != 'churned')
            LIMIT 1
            """,
            (email,),
        ).fetchone()
        if row:
            return True, "active client record in clients table"

        # 6. Already in Pipedrive pipeline (salesperson is working this contact)
        mapping_row = self.db.execute(
            """
            SELECT canonical_id FROM cross_tool_mapping
            WHERE tool_name = 'hubspot'
              AND tool_specific_id = %s
            LIMIT 1
            """,
            (hubspot_id,),
        ).fetchone()
        if mapping_row:
            canonical_id = mapping_row["canonical_id"]
            row = self.db.execute(
                """
                SELECT 1 FROM cross_tool_mapping
                WHERE canonical_id = %s
                  AND tool_name IN ('pipedrive', 'pipedrive_deal', 'pipedrive_person')
                LIMIT 1
                """,
                (canonical_id,),
            ).fetchone()
            if row:
                return True, "contact is in the active Pipedrive sales pipeline"

        return False, ""

    # ── Per-contact orchestration ─────────────────────────────────────────────

    def _process_contact(
        self, contact: dict, run_id: str, trigger_source: str
    ) -> None:
        """
        Run the full agent chain for one contact. Each step is wrapped in its
        own try/except so a failure is reported but does not skip later steps
        (Slack notification and DB write happen even on partial failure).
        """
        name       = (
            f"{contact.get('firstname', '')} {contact.get('lastname', '')}".strip()
            or contact.get("email", "unknown")
        )
        hubspot_id = contact["hubspot_id"]
        logger.info("Processing contact: %s (hubspot_id=%s)", name, hubspot_id)

        # Re-check skip condition here in case a concurrent runner instance
        # processed this contact after _fetch_new_contacts built the list.
        if not self.dry_run:
            should_skip, skip_reason = self._should_skip_contact(
                hubspot_id, contact.get("email", "")
            )
            if should_skip:
                logger.info(
                    "Skipping contact %s (%s) — %s (detected at process time).",
                    hubspot_id, name, skip_reason,
                )
                return

        error_message = None  # type: Optional[str]
        email_result = None  # type: Optional[Dict]
        gmail_result = None  # type: Optional[Dict]
        jobs_output: Dict = {}

        # ── a. Email synthesis agent ──────────────────────────────────────────
        if self.dry_run:
            logger.info("[DRY RUN] Would run email synthesis agent for %s.", name)
            email_result = _DRY_RUN_EMAIL
        else:
            try:
                email_result = synthesize_email(contact)
                if email_result is None:
                    error_message = "Email synthesis agent returned None."
                    logger.warning("%s — contact: %s", error_message, name)
                else:
                    self.log_action(
                        run_id, "run_email_synthesis_agent",
                        f"hubspot:{hubspot_id}", "success",
                        trigger_source=trigger_source,
                        trigger_detail={
                            "template_set": email_result.get("template_set_used"),
                            "word_count":   email_result.get("word_count"),
                        },
                    )
            except Exception as exc:
                error_message = f"Email synthesis raised: {exc}"
                logger.error("Email synthesis failed for %s: %s", name, exc)

        # ── b. Similar jobs matching ──────────────────────────────────────────
        if self.dry_run:
            logger.info("[DRY RUN] Would run similar jobs agent for %s.", name)
        else:
            try:
                jobs_output = find_similar_jobs(contact)
                self.log_action(
                    run_id, "run_similar_jobs_agent",
                    f"hubspot:{hubspot_id}", "success",
                    trigger_source=trigger_source,
                    trigger_detail={
                        "match_confidence": jobs_output.get("match_confidence"),
                        "match_count": len(jobs_output.get("matches") or []),
                    },
                )
            except Exception as exc:
                logger.error("Similar jobs agent failed for %s: %s", name, exc)

        # ── c. Create Gmail draft ─────────────────────────────────────────────
        if error_message is None and email_result is not None:
            if self.dry_run:
                logger.info(
                    "[DRY RUN] Would create Gmail draft to %s (subject: %r).",
                    contact["email"], email_result.get("subject"),
                )
                gmail_result = {
                    "draft_id":   "DRY-DRAFT-ID",
                    "gmail_link": "https://mail.google.com/#drafts",
                }
            else:
                try:
                    gmail_result = create_gmail_draft(
                        contact["email"],
                        email_result["subject"],
                        email_result["body"],
                    )
                    if gmail_result is None:
                        error_message = "Gmail draft creation failed."
                        logger.warning("%s — contact: %s", error_message, name)
                        self.log_action(
                            run_id, "create_gmail_draft", None, "failed",
                            error_message=error_message,
                            trigger_source=trigger_source,
                        )
                    else:
                        self.log_action(
                            run_id, "create_gmail_draft",
                            f"gmail:{gmail_result['draft_id']}",
                            "success", trigger_source=trigger_source,
                            trigger_detail={
                                "draft_id": gmail_result["draft_id"],
                                "to":       contact["email"],
                            },
                        )
                except Exception as exc:
                    error_message = f"Gmail draft raised: {exc}"
                    logger.error("Gmail draft failed for %s: %s", name, exc)
                    self.log_action(
                        run_id, "create_gmail_draft", None, "failed",
                        error_message=str(exc),
                        trigger_source=trigger_source,
                    )

        # ── d. Slack notification ─────────────────────────────────────────────
        template_info = email_result or {}
        slack_ts: Optional[str] = None
        try:
            if self.dry_run:
                mode = "success" if gmail_result else "error"
                logger.info(
                    "[DRY RUN] Would post %s notification to #sales for %s.", mode, name
                )
            else:
                slack_ts = notify_sales_channel(
                    contact=contact,
                    research_output={},
                    jobs_output=jobs_output,
                    gmail_result=gmail_result,
                    template_info=template_info,
                    error_message=error_message,
                )
        except Exception as exc:
            logger.error("Slack notification failed for %s: %s", name, exc)

        # ── e. Write outreach_drafts DB record ────────────────────────────────
        total_tokens, estimated_cost = _estimate_tokens_and_cost()

        if estimated_cost > _COST_WARN_THRESHOLD:
            logger.warning(
                "Per-lead cost estimate $%.4f exceeds $%.2f threshold for contact %s.",
                estimated_cost, _COST_WARN_THRESHOLD, name,
            )
        else:
            logger.info(
                "Per-lead cost estimate: $%.4f (%d tokens) for contact %s.",
                estimated_cost, total_tokens, name,
            )

        canonical_id = self._next_draft_canonical_id()

        try:
            if self.dry_run:
                logger.info(
                    "[DRY RUN] Would write outreach_drafts record %s for contact %s.",
                    canonical_id, name,
                )
            else:
                self._write_outreach_draft(
                    canonical_id=canonical_id,
                    contact=contact,
                    email_result=email_result,
                    gmail_result=gmail_result,
                    slack_ts=slack_ts,
                    total_tokens=total_tokens,
                    estimated_cost=estimated_cost,
                    error_message=error_message,
                )
                self.log_action(
                    run_id, "write_outreach_draft",
                    f"outreach_drafts:{canonical_id}",
                    "success", trigger_source=trigger_source,
                    trigger_detail={"canonical_id": canonical_id},
                )
        except Exception as exc:
            logger.error("DB write failed for %s: %s", name, exc)
            self.log_action(
                run_id, "write_outreach_draft", None, "failed",
                error_message=str(exc), trigger_source=trigger_source,
            )

        # ── f. Register cross_tool_mapping ────────────────────────────────────
        if gmail_result and not self.dry_run:
            try:
                self._register_draft_mapping(
                    canonical_id  = canonical_id,
                    gmail_draft_id= gmail_result["draft_id"],
                    hubspot_id    = hubspot_id,
                )
                self.log_action(
                    run_id, "register_draft_mapping",
                    f"cross_tool_mapping:{canonical_id}",
                    "success", trigger_source=trigger_source,
                    trigger_detail={
                        "canonical_id":  canonical_id,
                        "gmail_draft_id": gmail_result["draft_id"],
                        "hubspot_id":    hubspot_id,
                    },
                )
            except Exception as exc:
                logger.error(
                    "Draft mapping registration failed for %s: %s", name, exc
                )
                self.log_action(
                    run_id, "register_draft_mapping", None, "failed",
                    error_message=str(exc), trigger_source=trigger_source,
                )
        elif self.dry_run and gmail_result:
            logger.info(
                "[DRY RUN] Would register cross_tool_mapping: %s → gmail:%s, hubspot:%s.",
                canonical_id, gmail_result["draft_id"], hubspot_id,
            )

    # ── DB helpers ────────────────────────────────────────────────────────────

    def _next_draft_canonical_id(self) -> str:
        """
        Return the next SS-DRAFT-NNNN canonical ID by querying the current
        maximum from both outreach_drafts and cross_tool_mapping(entity_type='DRAFT').

        Because contacts are processed sequentially within a single automation
        run, there is no concurrent allocation risk.  ON CONFLICT guards in the
        caller's write statements handle any edge-case collisions from parallel
        runner instances.
        """
        if self.dry_run:
            return "SS-DRAFT-DRY"

        max_n = 0

        row = self.db.execute(
            """
            SELECT canonical_id FROM cross_tool_mapping
            WHERE entity_type = 'DRAFT'
            ORDER BY canonical_id DESC LIMIT 1
            """
        ).fetchone()
        if row:
            try:
                max_n = max(max_n, int(row["canonical_id"].split("-")[-1]))
            except (ValueError, IndexError):
                pass

        row2 = self.db.execute(
            "SELECT id FROM outreach_drafts ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if row2:
            try:
                max_n = max(max_n, int(row2["id"].split("-")[-1]))
            except (ValueError, IndexError):
                pass

        return f"SS-DRAFT-{max_n + 1:04d}"

    def _write_outreach_draft(
        self,
        canonical_id: str,
        contact: dict,
        email_result,  # type: Optional[Dict]
        gmail_result,  # type: Optional[Dict]
        slack_ts,      # type: Optional[str]
        total_tokens,  # type: int
        estimated_cost,  # type: float
        error_message,  # type: Optional[str]
    ) -> None:
        """Insert one row into outreach_drafts (ON CONFLICT DO NOTHING for safety)."""
        name            = (
            f"{contact.get('firstname', '')} {contact.get('lastname', '')}".strip()
        )
        template_set    = (email_result or {}).get("template_set_used", "")
        template_variant = (email_result or {}).get("variant_used", "")
        status          = "completed" if (gmail_result and email_result) else "failed"

        with self.db:
            self.db.execute(
                """
                INSERT INTO outreach_drafts (
                    id, hubspot_contact_id, contact_name, contact_email,
                    contact_type, template_set, template_variant,
                    lead_source, gmail_draft_id, gmail_link,
                    slack_message_ts,
                    agent_3_output,
                    total_tokens_used, estimated_cost_usd,
                    status, error_message
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s,
                    %s,
                    %s, %s,
                    %s, %s
                )
                ON CONFLICT (id) DO NOTHING
                """,
                (
                    canonical_id,
                    contact["hubspot_id"],
                    name,
                    contact.get("email", ""),
                    contact.get("contact_type", ""),
                    template_set,
                    template_variant,
                    contact.get("lead_source", ""),
                    (gmail_result or {}).get("draft_id"),
                    (gmail_result or {}).get("gmail_link"),
                    slack_ts,
                    json.dumps(email_result) if email_result else None,
                    total_tokens,
                    estimated_cost,
                    status,
                    error_message,
                ),
            )

    def _register_draft_mapping(
        self,
        canonical_id:   str,
        gmail_draft_id: str,
        hubspot_id:     str,
    ) -> None:
        """
        Register two cross_tool_mapping entries for the draft canonical ID:
          • (canonical_id, 'DRAFT', 'gmail',   gmail_draft_id) — deep link
          • (canonical_id, 'DRAFT', 'hubspot', hubspot_id)     — duplicate guard
        Both are inserted atomically; ON CONFLICT DO NOTHING makes this
        safe to retry.
        """
        try:
            for tool_name, tool_id in [("gmail", gmail_draft_id), ("hubspot", hubspot_id)]:
                self.db.execute(
                    """
                    INSERT INTO cross_tool_mapping
                        (canonical_id, entity_type, tool_name, tool_specific_id, synced_at)
                    VALUES (%s, 'DRAFT', %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (canonical_id, tool_name) DO NOTHING
                    """,
                    (canonical_id, tool_name, tool_id),
                )
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise


# ─────────────────────────────────────────────────────────────────────────────
# Dry-run sample data
# ─────────────────────────────────────────────────────────────────────────────

_DRY_RUN_CONTACTS = [
    {
        "hubspot_id":               "dry-hs-lead-001",
        "firstname":                "Priya",
        "lastname":                 "Nair",
        "email":                    "priya.nair@example.com",
        "company":                  "",
        "address":                  "4821 Bull Creek Rd",
        "city":                     "Austin",
        "contact_type":             "residential",
        "lead_source":              "REFERRAL",
        "hs_analytics_source":      "REFERRAL",
        "hs_analytics_source_data_1": "John Smith",
        "service_interest":         "biweekly_recurring",
        "lifecyclestage":           "lead",
    },
    {
        "hubspot_id":               "dry-hs-lead-002",
        "firstname":                "Robert",
        "lastname":                 "Chen",
        "email":                    "rchen@austinmedclinic.com",
        "company":                  "Austin Medical Clinic",
        "address":                  "2200 Barton Springs Rd",
        "city":                     "Austin",
        "contact_type":             "commercial",
        "lead_source":              "ORGANIC_SEARCH",
        "hs_analytics_source":      "ORGANIC_SEARCH",
        "hs_analytics_source_data_1": "",
        "service_interest":         "commercial_nightly",
        "lifecyclestage":           "subscriber",
    },
]

_DRY_RUN_EMAIL = {
    "subject":           "Got your inquiry",
    "body":              "[DRY RUN] Email body placeholder.",
    "template_set_used": "residential",
    "variant_used":      "inbound",
    "word_count":        50,
}


# ─────────────────────────────────────────────────────────────────────────────
# CLI entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    import os as _os
    try:
        from dotenv import load_dotenv as _load_dotenv
        _load_dotenv(_os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))), ".env"))
    except ImportError:
        pass

    parser = argparse.ArgumentParser(
        description="Automation 07 — Sales Research & Outreach Agent Chain"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Log actions without making API calls or writing to DB (default).",
    )
    parser.add_argument(
        "--live", action="store_true",
        help="Force live mode (overrides dry-run default).",
    )
    args = parser.parse_args()

    live    = args.live
    dry_run = not live

    print("=" * 65)
    print(f"  SalesOutreachAutomation — {'LIVE RUN' if live else 'dry-run'}")
    print("=" * 65)

    from auth import get_client
    from database.schema import get_connection
    from automations.migrate import run_migration
    from simulation.error_reporter import setup_channel, report_error
    import sys as _sys

    # Ensure automation tables (including outreach_drafts) exist
    run_migration()

    db = get_connection()

    # Ensure #automation-failure channel exists for service-level error reporting
    setup_channel(dry_run=dry_run)

    automation = SalesOutreachAutomation(
        clients=get_client,
        db=db,
        dry_run=dry_run,
    )
    try:
        automation.run()
    except Exception as _exc:
        report_error(
            _exc,
            tool_name="sales-outreach",
            context="SalesOutreachAutomation main loop — unhandled exception",
        )
        db.close()
        _sys.exit(1)

    print()
    print("─" * 65)
    print("automation_log entries for this run:")
    print("─" * 65)
    rows = db.execute(
        """
        SELECT action_name, action_target, status, error_message
        FROM automation_log
        WHERE automation_name = 'SalesOutreachAutomation'
        ORDER BY id DESC
        LIMIT 20
        """
    ).fetchall()
    for row in reversed(rows):
        r      = dict(row)
        marker = "OK " if r["status"] == "success" else "ERR"
        print(f"  [{marker}] {r['action_name']:<50} → {r['action_target'] or 'n/a'}")
        if r["error_message"]:
            print(f"         note: {r['error_message']}")

    print()
    print("Dry-run complete." if dry_run else "Live run complete.")
    db.close()
