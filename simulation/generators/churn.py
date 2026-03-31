"""
simulation/generators/churn.py

Simulates client cancellations cascading across Jobber, HubSpot, Pipedrive,
Mailchimp, Asana, Slack, and SQLite.

Type 4: multi-tool cascade. Each tool call is wrapped in its own try/except
so a single tool failure never blocks the remaining cascade steps.

Daily churn probabilities:
  Residential: 2.5%/month  = ~0.0833%/day
  Commercial:  1.5%/month  = ~0.0500%/day

Modifiers applied to the base daily rate before rolling:
  Referral acquisition source : ×0.5  (stickier)
  Recent complaint (rating≤2 in 30 days): ×3.0
  Daily multiplier from variation engine  : applied via should_event_happen()

Churn reasons (base weights, sum = 100):
  Moving out of area      25%
  Switching to competitor 20%
  Budget cuts             15%
  Dissatisfied with service 15%
  No longer needs service 15%
  Seasonal -- will return 10%

Clients with a recent complaint are weighted 3× toward
"Dissatisfied with service" and "Switching to competitor".
"""
from __future__ import annotations

import hashlib
import os
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional

from auth import get_client
from database.connection import get_connection, get_column_names
from database.mappings import get_tool_id
from intelligence.logging_config import setup_logging
from seeding.utils.throttler import (
    ASANA as asana_throttler,
    HUBSPOT as hubspot_throttler,
    JOBBER as jobber_throttler,
    MAILCHIMP as mailchimp_throttler,
    PIPEDRIVE as pipedrive_throttler,
    SLACK as slack_throttler,
)
from simulation.config import DAILY_VOLUMES
from simulation.variation import should_event_happen

logger = setup_logging("simulation.churn")


# ---------------------------------------------------------------------------
# Result type  (matches every other generator in this package)
# ---------------------------------------------------------------------------

@dataclass
class GeneratorResult:
    success: bool
    message: str = ""


# ---------------------------------------------------------------------------
# Probability constants
# ---------------------------------------------------------------------------

_CHURN_CFG = DAILY_VOLUMES["churn"]
_MONTHLY_RESIDENTIAL_RATE: float = _CHURN_CFG["monthly_residential_churn_rate"]  # 0.025
_MONTHLY_COMMERCIAL_RATE: float  = _CHURN_CFG["monthly_commercial_churn_rate"]   # 0.015
_DAILY_RESIDENTIAL_RATE: float   = _MONTHLY_RESIDENTIAL_RATE / 30   # ~0.000833
_DAILY_COMMERCIAL_RATE: float    = _MONTHLY_COMMERCIAL_RATE / 30    # ~0.000500

_REFERRAL_MODIFIER: float  = 0.5
_COMPLAINT_MODIFIER: float = 3.0
_COMPLAINT_LOOKBACK_DAYS   = 30
_COMPLAINT_MAX_RATING      = 2

_CHURN_REASONS = [
    "Moving out of area",
    "Switching to competitor",
    "Budget cuts",
    "Dissatisfied with service",
    "No longer needs service",
    "Seasonal -- will return",
]
# Base weights must sum to 100 for readability; random.choices normalises them
_BASE_WEIGHTS      = [25, 20, 15, 15, 15, 10]
# 3× boost on "Switching to competitor" (index 1) and "Dissatisfied" (index 3)
_COMPLAINT_WEIGHTS = [25, 60, 15, 45, 15, 10]


# ---------------------------------------------------------------------------
# Tool endpoints and IDs
# ---------------------------------------------------------------------------

_JOBBER_GQL_URL      = "https://api.getjobber.com/api/graphql"
_JOBBER_VERSION_HDR  = {"X-JOBBER-GRAPHQL-VERSION": "2026-03-10"}

_HUBSPOT_BASE   = "https://api.hubapi.com"
_PIPEDRIVE_BASE = "https://api.pipedrive.com/v1"
_ASANA_BASE     = "https://app.asana.com/api/1.0"
_SLACK_BASE     = "https://slack.com/api"

# From config/tool_ids.json
_MAILCHIMP_AUDIENCE_ID      = "92f05d2d65"
_ASANA_CLIENT_SUCCESS_GID   = "1213719346640011"
_ASANA_WORKSPACE_GID        = "1213704231015587"
_ASANA_CHURNED_SECTION_GID  = "1213719502048516"   # "Churned" section inside Client Success
_SLACK_OPERATIONS_CHANNEL   = "C0AM76H9K34"


# ---------------------------------------------------------------------------
# Jobber GraphQL
# ---------------------------------------------------------------------------

_CLIENT_JOBS_QUERY = """
query ClientFutureJobs($clientId: EncodedId!) {
  client(id: $clientId) {
    id
    jobs(first: 50) {
      nodes {
        id
        title
        jobStatus
        startAt
      }
    }
  }
}
"""

_JOB_DELETE = """
mutation JobDelete($jobId: EncodedId!) {
  jobDelete(id: $jobId) {
    deletedJobId
    userErrors { message path }
  }
}
"""


# ---------------------------------------------------------------------------
# Main generator class
# ---------------------------------------------------------------------------

class ChurnGenerator:
    """
    Simulates client cancellations across all 7 tools.

    Each call to execute() selects one active client at random (optionally
    filtered by client_type), tests whether they churn today (using per-type
    base rate + modifiers + variation-engine daily multiplier), and if so
    executes the full 7-step cascade.

    Registered with the simulation engine as the "churn" generator.
    The engine calls execute(dry_run=..., client_type=...) on each tick.
    When dry_run=True all API calls and SQLite writes are skipped.
    """

    name = "churn"

    def __init__(self, db_path: str = "sparkle_shine.db"):
        self.db_path = db_path
        self.logger = logger
        with get_connection() as conn:
            self._ensure_schema(conn)

    def _ensure_schema(self, conn) -> None:
        """Add churn_date and churn_reason columns to clients if absent."""
        existing = set(get_column_names(conn, "clients"))
        for col_name, col_type in [
            ("churn_date",   "TEXT"),
            ("churn_reason", "TEXT"),
        ]:
            if col_name not in existing:
                conn.execute(
                    f"ALTER TABLE clients ADD COLUMN {col_name} {col_type}"
                )

    def execute(self, dry_run: bool = False, client_type: Optional[str] = None) -> GeneratorResult:
        """Execute one churn check (and cascade if the client churns).

        Args:
            dry_run:     When True, log what would happen but skip all API calls
                         and SQLite writes.
            client_type: If provided ("residential" or "commercial"), only
                         eligible clients of that type are considered.
                         The engine passes this to match plan_day() buckets.
        """
        db = get_connection()
        today = date.today()

        try:
            # ── 1. Pick a candidate ───────────────────────────────────────
            clients = self._get_eligible_clients(db, client_type=client_type)
            if not clients:
                return GeneratorResult(
                    success=True,
                    message="No active clients to check for churn",
                )

            client = random.choice(clients)
            client_name = self._display_name(client)

            # ── 2. Decide whether to churn ────────────────────────────────
            has_complaint = self._has_recent_complaint(db, client["id"])
            base_rate = (
                _DAILY_RESIDENTIAL_RATE
                if client["client_type"] == "residential"
                else _DAILY_COMMERCIAL_RATE
            )

            modifier = 1.0
            acquisition = (client["acquisition_source"] or "").lower()
            if "referral" in acquisition:
                modifier *= _REFERRAL_MODIFIER
            if has_complaint:
                modifier *= _COMPLAINT_MODIFIER

            if not should_event_happen(base_rate * modifier, today):
                return GeneratorResult(
                    success=True,
                    message=f"Checked {client_name}, not churning today",
                )

            # ── 3. Execute the cascade ────────────────────────────────────
            reason = self._pick_churn_reason(has_complaint)
            errors: list[str] = []

            if dry_run:
                self.logger.info(
                    f"[dry-run] Would churn {client_name} ({client['id']}): {reason}"
                )
                return GeneratorResult(
                    success=True,
                    message=f"[dry-run] Would churn: {client_name} ({reason})",
                )

            # Step 1 – Jobber: cancel recurring schedule + future jobs
            try:
                cancelled = self._cancel_jobber(client)
                self.logger.info(
                    f"[{client['id']}] Jobber: cancelled {cancelled} future job(s)"
                )
            except Exception as exc:
                errors.append(f"Jobber: {exc}")
                self.logger.warning(f"[{client['id']}] Jobber cancel failed: {exc}")

            # Step 2 – HubSpot: lifecycle → other, record churn metadata
            try:
                self._update_hubspot(client, reason, today)
                self.logger.info(f"[{client['id']}] HubSpot: lifecycle set to 'other'")
            except Exception as exc:
                errors.append(f"HubSpot: {exc}")
                self.logger.warning(f"[{client['id']}] HubSpot update failed: {exc}")

            # Step 3 – Pipedrive: activity note + mark person inactive
            try:
                self._update_pipedrive(client, reason)
                self.logger.info(f"[{client['id']}] Pipedrive: person marked inactive")
            except Exception as exc:
                errors.append(f"Pipedrive: {exc}")
                self.logger.warning(f"[{client['id']}] Pipedrive update failed: {exc}")

            # Step 4 – Mailchimp: unsubscribe + tag "churned"
            try:
                self._unsubscribe_mailchimp(client)
                self.logger.info(f"[{client['id']}] Mailchimp: unsubscribed + tagged")
            except Exception as exc:
                errors.append(f"Mailchimp: {exc}")
                self.logger.warning(f"[{client['id']}] Mailchimp failed: {exc}")

            # Step 5 – Asana: retention follow-up task
            try:
                self._create_retention_task(client, reason, today)
                self.logger.info(
                    f"[{client['id']}] Asana: retention task created"
                )
            except Exception as exc:
                errors.append(f"Asana: {exc}")
                self.logger.warning(f"[{client['id']}] Asana task failed: {exc}")

            # Step 6 – Slack: churn notification to #operations
            try:
                self._post_slack_notification(client, reason, today)
                self.logger.info(
                    f"[{client['id']}] Slack: notification posted to #operations"
                )
            except Exception as exc:
                errors.append(f"Slack: {exc}")
                self.logger.warning(f"[{client['id']}] Slack notification failed: {exc}")

            # Step 7 – SQLite: update clients + recurring_agreements (always last)
            self._update_sqlite(db, client, reason, today)
            db.commit()

            summary = f"Churned: {client_name} ({reason})"
            if errors:
                summary += f" [{len(errors)} tool error(s): {'; '.join(errors)}]"

            self.logger.info(summary)
            return GeneratorResult(success=True, message=summary)

        except Exception as exc:
            db.rollback()
            self.logger.error(f"Churn cascade fatal error: {exc}")
            raise

        finally:
            db.close()

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _display_name(self, client: dict) -> str:
        if client["client_type"] == "residential":
            return f"{client['first_name']} {client['last_name']}"
        return client["company_name"] or f"{client['first_name']} {client['last_name']}"

    # ── Eligibility ─────────────────────────────────────────────────────────

    def _get_eligible_clients(
        self,
        db,
        client_type: Optional[str] = None,
    ) -> list[dict]:
        if client_type:
            cursor = db.execute(
                """
                SELECT id, client_type, first_name, last_name, company_name,
                       email, acquisition_source, lifetime_value, notes
                FROM clients
                WHERE status = 'active' AND client_type = %s
                """,
                (client_type,),
            )
        else:
            cursor = db.execute("""
                SELECT id, client_type, first_name, last_name, company_name,
                       email, acquisition_source, lifetime_value, notes
                FROM clients
                WHERE status = 'active'
            """)
        return [dict(row) for row in cursor.fetchall()]

    def _has_recent_complaint(self, db, client_id: str) -> bool:
        """True if the client had a review rating ≤ 2 in the last 30 days."""
        cutoff = (date.today() - timedelta(days=_COMPLAINT_LOOKBACK_DAYS)).isoformat()
        cursor = db.execute(
            """
            SELECT 1 FROM reviews
            WHERE client_id = %s
              AND rating <= %s
              AND review_date >= %s
            LIMIT 1
            """,
            (client_id, _COMPLAINT_MAX_RATING, cutoff),
        )
        return cursor.fetchone() is not None

    # ── Churn reason ─────────────────────────────────────────────────────────

    def _pick_churn_reason(self, has_complaint: bool) -> str:
        weights = _COMPLAINT_WEIGHTS if has_complaint else _BASE_WEIGHTS
        return random.choices(_CHURN_REASONS, weights=weights, k=1)[0]

    # ── Cascade: Step 1 – Jobber ─────────────────────────────────────────────

    def _cancel_jobber(self, client: dict) -> int:
        """Cancel future jobs in Jobber and mark recurring agreements cancelled.

        Returns the count of Jobber jobs that were deleted.
        """
        jobber_client_id = get_tool_id(client["id"], "jobber", self.db_path)
        if not jobber_client_id:
            raise ValueError(f"No Jobber mapping for {client['id']}")

        session = get_client("jobber")
        today_iso = datetime.utcnow().isoformat()

        # Query all jobs for this client from Jobber
        jobber_throttler.wait()
        resp = session.post(
            _JOBBER_GQL_URL,
            json={
                "query": _CLIENT_JOBS_QUERY,
                "variables": {"clientId": jobber_client_id},
            },
            headers=_JOBBER_VERSION_HDR,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("errors"):
            raise RuntimeError(f"Jobber GQL errors: {data['errors']}")

        nodes = (
            data.get("data", {})
            .get("client", {})
            .get("jobs", {})
            .get("nodes", [])
        )

        cancelled_count = 0
        for job in nodes:
            # Only delete future jobs that are still active / awaiting
            start_at = job.get("startAt") or ""
            if start_at > today_iso and job.get("jobStatus") in (
                "ACTIVE", "AWAITING_PAYMENT", "SCHEDULED", "TODAY"
            ):
                jobber_throttler.wait()
                del_resp = session.post(
                    _JOBBER_GQL_URL,
                    json={
                        "query": _JOB_DELETE,
                        "variables": {"jobId": job["id"]},
                    },
                    headers=_JOBBER_VERSION_HDR,
                    timeout=30,
                )
                del_resp.raise_for_status()
                del_data = del_resp.json()
                user_errors = (
                    del_data.get("data", {})
                    .get("jobDelete", {})
                    .get("userErrors", [])
                )
                if user_errors:
                    self.logger.warning(
                        f"Jobber jobDelete userErrors for job {job['id']}: {user_errors}"
                    )
                else:
                    cancelled_count += 1

        return cancelled_count

    # ── Cascade: Step 2 – HubSpot ────────────────────────────────────────────

    def _update_hubspot(self, client: dict, reason: str, today: date) -> None:
        """Set lifecyclestage → 'other' and record churn date + reason."""
        hubspot_id = get_tool_id(client["id"], "hubspot", self.db_path)
        if not hubspot_id:
            raise ValueError(f"No HubSpot mapping for {client['id']}")

        session = get_client("hubspot")
        hubspot_throttler.wait()
        resp = session.patch(
            f"{_HUBSPOT_BASE}/crm/v3/objects/contacts/{hubspot_id}",
            json={
                "properties": {
                    "lifecyclestage": "other",
                    "hs_lead_status": "CHURNED",
                    "churn_date": today.isoformat(),
                    "churn_reason": reason,
                }
            },
            timeout=30,
        )
        if resp.status_code not in (200, 204):
            raise RuntimeError(
                f"HubSpot PATCH {resp.status_code}: {resp.text[:200]}"
            )

    # ── Cascade: Step 3 – Pipedrive ──────────────────────────────────────────

    def _update_pipedrive(self, client: dict, reason: str) -> None:
        """Add a churn activity note and mark the person inactive."""
        pipedrive_id = get_tool_id(client["id"], "pipedrive", self.db_path)
        if not pipedrive_id:
            raise ValueError(f"No Pipedrive mapping for {client['id']}")

        session = get_client("pipedrive")
        name = self._display_name(client)

        # Activity note
        pipedrive_throttler.wait()
        note_resp = session.post(
            f"{_PIPEDRIVE_BASE}/activities",
            json={
                "subject": f"Client churned: {name}",
                "person_id": int(pipedrive_id),
                "type": "note",
                "done": 1,
                "note": (
                    f"Client cancelled service.\n"
                    f"Reason: {reason}\n"
                    f"SS-ID: {client['id']}"
                ),
            },
            timeout=30,
        )
        activity_data = note_resp.json()
        if not activity_data.get("success"):
            raise RuntimeError(
                f"Pipedrive activity failed: {note_resp.text[:200]}"
            )

        # Mark person inactive
        pipedrive_throttler.wait()
        person_resp = session.put(
            f"{_PIPEDRIVE_BASE}/persons/{pipedrive_id}",
            json={"active_flag": False},
            timeout=30,
        )
        person_data = person_resp.json()
        if not person_data.get("success"):
            raise RuntimeError(
                f"Pipedrive person update failed: {person_resp.text[:200]}"
            )

    # ── Cascade: Step 4 – Mailchimp ──────────────────────────────────────────

    def _unsubscribe_mailchimp(self, client: dict) -> None:
        """Unsubscribe the member and add the 'churned' tag."""
        email = client["email"]
        subscriber_hash = hashlib.md5(email.lower().encode()).hexdigest()
        server = os.getenv("MAILCHIMP_SERVER_PREFIX", "us6")
        base = f"https://{server}.api.mailchimp.com/3.0"
        member_url = f"{base}/lists/{_MAILCHIMP_AUDIENCE_ID}/members/{subscriber_hash}"

        session = get_client("mailchimp")

        # Unsubscribe
        mailchimp_throttler.wait()
        unsub_resp = session.patch(
            member_url,
            json={"status": "unsubscribed"},
            timeout=30,
        )
        if unsub_resp.status_code not in (200, 204):
            raise RuntimeError(
                f"Mailchimp unsubscribe {unsub_resp.status_code}: {unsub_resp.text[:200]}"
            )

        # Add "churned" tag
        mailchimp_throttler.wait()
        tag_resp = session.post(
            f"{member_url}/tags",
            json={"tags": [{"name": "churned", "status": "active"}]},
            timeout=30,
        )
        if tag_resp.status_code not in (200, 204):
            raise RuntimeError(
                f"Mailchimp tag {tag_resp.status_code}: {tag_resp.text[:200]}"
            )

    # ── Cascade: Step 5 – Asana ──────────────────────────────────────────────

    def _create_retention_task(
        self, client: dict, reason: str, today: date
    ) -> None:
        """Create a retention follow-up task in the Client Success project."""
        name = self._display_name(client)
        due_on = (today + timedelta(days=3)).isoformat()
        ltv = client.get("lifetime_value") or 0.0

        session = get_client("asana")
        asana_throttler.wait()
        resp = session.post(
            f"{_ASANA_BASE}/tasks",
            json={
                "data": {
                    "workspace": _ASANA_WORKSPACE_GID,
                    "projects": [_ASANA_CLIENT_SUCCESS_GID],
                    "memberships": [
                        {
                            "project": _ASANA_CLIENT_SUCCESS_GID,
                            "section": _ASANA_CHURNED_SECTION_GID,
                        }
                    ],
                    "name": f"Retention follow-up: {name} cancelled",
                    "notes": (
                        f"Reason: {reason}\n"
                        f"LTV: ${ltv:,.0f}\n"
                        f"SS-ID: {client['id']}"
                    ),
                    "due_on": due_on,
                }
            },
            timeout=30,
        )
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                f"Asana task {resp.status_code}: {resp.text[:200]}"
            )

    # ── Cascade: Step 6 – Slack ──────────────────────────────────────────────

    def _post_slack_notification(
        self, client: dict, reason: str, today: date
    ) -> None:
        """Post a churn notification to #operations."""
        name = self._display_name(client)
        ltv = client.get("lifetime_value") or 0.0
        client_type = client["client_type"].capitalize()

        session = get_client("slack")
        slack_throttler.wait()
        resp = session.post(
            f"{_SLACK_BASE}/chat.postMessage",
            json={
                "channel": _SLACK_OPERATIONS_CHANNEL,
                "text": f":rotating_light: Client cancellation: {name}",
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": ":rotating_light: Client Cancellation",
                        },
                    },
                    {
                        "type": "section",
                        "fields": [
                            {"type": "mrkdwn", "text": f"*Client:* {name}"},
                            {"type": "mrkdwn", "text": f"*Type:* {client_type}"},
                            {"type": "mrkdwn", "text": f"*Reason:* {reason}"},
                            {"type": "mrkdwn", "text": f"*LTV:* ${ltv:,.0f}"},
                            {"type": "mrkdwn", "text": f"*Date:* {today.isoformat()}"},
                            {"type": "mrkdwn", "text": f"*SS-ID:* `{client['id']}`"},
                        ],
                    },
                ],
            },
            timeout=30,
        )
        data = resp.json()
        if not data.get("ok"):
            raise RuntimeError(
                f"Slack post failed: {data.get('error', resp.text[:200])}"
            )

    # ── Cascade: Step 7 – SQLite ─────────────────────────────────────────────

    def _update_sqlite(
        self,
        db,
        client: dict,
        reason: str,
        today: date,
    ) -> None:
        """Mark client as churned and cancel their recurring agreements.

        Writes to the structured churn_date and churn_reason columns (added by
        _ensure_schema) so the intelligence layer can run date-range queries.
        Also prepends a human-readable note for quick visual inspection.
        """
        existing_notes = client.get("notes") or ""
        churn_note = f"[CHURNED {today.isoformat()}] Reason: {reason}"
        updated_notes = f"{churn_note}\n{existing_notes}".strip()

        db.execute(
            """
            UPDATE clients
            SET status      = 'churned',
                churn_date  = %s,
                churn_reason = %s,
                notes        = %s
            WHERE id = %s
            """,
            (today.isoformat(), reason, updated_notes, client["id"]),
        )

        # Cancel all active recurring agreements for this client
        db.execute(
            """
            UPDATE recurring_agreements
            SET status   = 'cancelled',
                end_date = %s
            WHERE client_id = %s
              AND status     = 'active'
            """,
            (today.isoformat(), client["id"]),
        )
