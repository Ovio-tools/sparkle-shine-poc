"""
automations/negative_review.py

Automation 4 — Negative Review Response
Trigger: a review with rating <= 2 appears in the Google Sheets review tracker
         (from poll_sheets_negative_reviews).

Actions (each in its own try/except so failures are isolated):
  1. Post an immediate Slack alert to #operations
  2. Create an urgent same-day Asana response task in Client Success
  3. Flag the HubSpot contact as at-risk and create a note
"""
import json
import os
import sys
from datetime import date

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from automations.base import BaseAutomation
from automations.utils.assignees import get_assignee_email
from automations.utils.asana_tasks import create_tasks

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

_OFFICE_MANAGER_EMAIL = get_assignee_email("office_manager")

# HubSpot note-to-contact association type (HUBSPOT_DEFINED category, id 202)
_HS_NOTE_TO_CONTACT_TYPE_ID = 202


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _load_tool_ids() -> dict:
    path = os.path.join(_PROJECT_ROOT, "config", "tool_ids.json")
    with open(path) as f:
        return json.load(f)


def _truncate(text: str, max_chars: int) -> str:
    """Return text truncated to max_chars characters (no ellipsis if already short enough)."""
    if len(text) <= max_chars:
        return text
    return text[:max_chars]


# ─────────────────────────────────────────────────────────────────────────────
# Main class
# ─────────────────────────────────────────────────────────────────────────────

class NegativeReviewResponse(BaseAutomation):
    """
    Orchestrates the three-step negative-review response flow.

    Expects `self.clients` to be callable: clients("tool_name") → client/session.
    trigger_event must come from poll_sheets_negative_reviews() and contain:
        row_index, date, client_name, client_email, rating,
        review_text, crew, service_type.
    """

    # ── Entry point ───────────────────────────────────────────────────────────

    def run(self, trigger_event: dict) -> None:
        run_id = self.generate_run_id()
        trigger_source = (
            f"google_sheets:review:row_{trigger_event.get('row_index', 'unknown')}"
        )

        client_name   = trigger_event.get("client_name") or "Unknown Client"
        client_email  = (trigger_event.get("client_email") or "").strip().lower()
        rating        = trigger_event.get("rating", 0)
        review_text   = trigger_event.get("review_text") or ""
        service_type  = trigger_event.get("service_type") or ""
        review_date   = trigger_event.get("date") or date.today().isoformat()
        crew          = trigger_event.get("crew") or "unassigned"

        tool_ids = _load_tool_ids()

        # ── Action 1: Slack alert ─────────────────────────────────────────────
        try:
            self._action_slack_alert(
                client_name, rating, service_type, review_date, crew, review_text
            )
            self.log_action(
                run_id, "post_slack_alert",
                "slack:channel:operations",
                "success",
                trigger_source=trigger_source,
                trigger_detail={
                    "client_name": client_name,
                    "rating": rating,
                    "row_index": trigger_event.get("row_index"),
                },
            )
        except Exception as exc:
            self.log_action(
                run_id, "post_slack_alert", None, "failed",
                error_message=str(exc), trigger_source=trigger_source,
            )

        # ── Action 2: Asana response task ─────────────────────────────────────
        try:
            task_gid = self._action_asana_task(
                client_name, client_email, rating, review_text,
                service_type, review_date, crew, tool_ids,
            )
            self.log_action(
                run_id, "create_asana_task",
                f"asana:task:{task_gid}",
                "success",
                trigger_source=trigger_source,
            )
        except Exception as exc:
            self.log_action(
                run_id, "create_asana_task", None, "failed",
                error_message=str(exc), trigger_source=trigger_source,
            )

        # ── Action 3: HubSpot at-risk flag ────────────────────────────────────
        try:
            result = self._action_hubspot_flag(
                client_email, client_name, rating, review_text,
                service_type, review_date, crew,
            )
            self.log_action(
                run_id, "flag_hubspot_contact",
                f"hubspot:contact:{result}",
                "success" if result != "skipped" else "skipped",
                error_message=(
                    f"HubSpot contact not found for {client_email}"
                    if result == "skipped" else None
                ),
                trigger_source=trigger_source,
            )
        except Exception as exc:
            self.log_action(
                run_id, "flag_hubspot_contact", None, "failed",
                error_message=str(exc), trigger_source=trigger_source,
            )

    # ── Action 1: Slack alert ─────────────────────────────────────────────────

    def _action_slack_alert(
        self,
        client_name: str,
        rating: int,
        service_type: str,
        review_date: str,
        crew: str,
        review_text: str,
    ) -> None:
        truncated = _truncate(review_text, 150)
        if len(review_text) > 150:
            review_display = f'"{truncated}..."'
        else:
            review_display = f'"{truncated}"'

        text = (
            f":rotating_light: *NEGATIVE REVIEW ALERT*\n\n"
            f"*Client:* {client_name}\n"
            f"*Rating:* {rating}/5 stars\n"
            f"*Service:* {service_type} on {review_date}\n"
            f"*Crew:* {crew}\n"
            f"*Review:* {review_display}\n\n"
            f"Asana task created for follow-up."
        )

        if self.dry_run:
            print(f"[DRY RUN] Would post to #operations:\n{text}")
            return

        self.send_slack("operations", text)

    # ── Action 2: Asana response task ─────────────────────────────────────────

    def _action_asana_task(
        self,
        client_name: str,
        client_email: str,
        rating: int,
        review_text: str,
        service_type: str,
        review_date: str,
        crew: str,
        tool_ids: dict,
    ) -> str:
        title = f"URGENT -- Respond to negative review: {client_name} ({rating} stars)"
        description = (
            f"Full review:\n"
            f"{review_text}\n\n"
            f"Service: {service_type}\n"
            f"Date: {review_date}\n"
            f"Crew: {crew}\n"
            f"Client email: {client_email}\n\n"
            f"Suggested response steps:\n"
            f"1. Call the client within 4 hours\n"
            f"2. Acknowledge the issue and apologize\n"
            f"3. Offer to re-clean at no charge if appropriate\n"
            f"4. Follow up in writing within 24 hours\n"
            f"5. Debrief with the crew lead"
        )

        task_def = {
            "title": title,
            "description": description,
            "assignee_email": _OFFICE_MANAGER_EMAIL,
            "due_date": date.today().isoformat(),
        }

        if self.dry_run:
            print(
                f"[DRY RUN] Would create Asana task in 'Client Success → At Risk':\n"
                f"  Title: {title}\n"
                f"  Assignee: {_OFFICE_MANAGER_EMAIL}\n"
                f"  Due: {task_def['due_date']}"
            )
            return "dry-run-asana-gid"

        asana_client = self.clients("asana")
        gids = create_tasks(
            client=asana_client,
            project_name="Client Success",
            section_name="At Risk",
            tasks=[task_def],
            tool_ids=tool_ids,
            deduplicate_by_title=False,
        )
        return gids[0] if gids else "unknown"

    # ── Action 3: HubSpot at-risk flag ───────────────────────────────────────

    def _action_hubspot_flag(
        self,
        client_email: str,
        client_name: str,
        rating: int,
        review_text: str,
        service_type: str,
        review_date: str,
        crew: str,
    ) -> str:
        """
        Search HubSpot for the contact by email, flag as at-risk, and create a note.
        Returns the contact ID on success, "skipped" if not found.
        """
        note_body = (
            f"Negative review ({rating}/5) received on {review_date}. "
            f"Service: {service_type}. Crew: {crew}. "
            f"Review: {_truncate(review_text, 100)}."
        )

        if self.dry_run:
            print(
                f"[DRY RUN] Would search HubSpot for contact: {client_email}"
            )
            print(
                f"[DRY RUN] Would PATCH HubSpot contact: at_risk=true"
            )
            print(
                f"[DRY RUN] Would create HubSpot note: {note_body}"
            )
            return "dry-run-hs-contact-id"

        hs_client = self.clients("hubspot")

        # Search for contact by email
        from hubspot.crm.contacts import PublicObjectSearchRequest
        from hubspot.crm.contacts import SimplePublicObjectInput
        from hubspot.crm.objects.notes import SimplePublicObjectInputForCreate

        search_request = PublicObjectSearchRequest(
            filter_groups=[
                {
                    "filters": [
                        {
                            "propertyName": "email",
                            "operator": "EQ",
                            "value": client_email,
                        }
                    ]
                }
            ],
            properties=["email"],
            limit=1,
        )
        search_result = hs_client.crm.contacts.search_api.do_search(search_request, _request_timeout=30)

        if not search_result.results:
            print(f"[WARN] HubSpot contact not found for {client_email}")
            try:
                from simulation.error_reporter import report_error
                report_error(
                    f"HubSpot contact not found for {client_email}",
                    tool_name="hubspot",
                    context="Negative review response — could not find contact to flag",
                    dry_run=self.dry_run,
                )
            except Exception:
                pass
            return "skipped"

        contact_id = search_result.results[0].id

        # PATCH: set at_risk = "true"
        hs_client.crm.contacts.basic_api.update(
            contact_id,
            SimplePublicObjectInput(
                properties={"at_risk": "true"}
            ),
            _request_timeout=30,
        )

        # Create a note associated with the contact
        note_input = SimplePublicObjectInputForCreate(
            properties={
                "hs_note_body": note_body,
                "hs_timestamp": (
                    __import__("datetime")
                    .datetime.utcnow()
                    .strftime("%Y-%m-%dT%H:%M:%SZ")
                ),
            },
            associations=[
                {
                    "to": {"id": contact_id},
                    "types": [
                        {
                            "associationCategory": "HUBSPOT_DEFINED",
                            "associationTypeId": _HS_NOTE_TO_CONTACT_TYPE_ID,
                        }
                    ],
                }
            ],
        )
        hs_client.crm.objects.notes.basic_api.create(note_input, _request_timeout=30)

        return contact_id


# ─────────────────────────────────────────────────────────────────────────────
# Dry-run sanity test
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    sys.path.insert(0, _PROJECT_ROOT)

    from auth import get_client
    from database.schema import get_connection

    print("=" * 65)
    print("  NegativeReviewResponse — dry-run sanity test")
    print("=" * 65)

    db = get_connection(os.path.join(_PROJECT_ROOT, "sparkle_shine.db"))

    fake_event = {
        "row_index": 7,
        "date": date.today().isoformat(),
        "client_name": "Marcus Whitfield",
        "client_email": "marcus.whitfield.dryrun@example.com",
        "rating": 1,
        "review_text": (
            "The crew arrived 45 minutes late and left without cleaning two "
            "bathrooms. I had to call three times before anyone picked up. "
            "Extremely disappointed with the service. Would not recommend."
        ),
        "crew": "Claudia Ramirez, Leticia Morales",
        "service_type": "Standard Residential Clean",
    }

    automation = NegativeReviewResponse(
        clients=get_client,
        db=db,
        dry_run=True,
    )

    print(
        f"\nTrigger event: row={fake_event['row_index']}, "
        f"client={fake_event['client_name']}, "
        f"rating={fake_event['rating']}/5"
    )
    print()

    automation.run(fake_event)

    print()
    print("─" * 65)
    print("automation_log entries for this run:")
    print("─" * 65)
    rows = db.execute(
        """
        SELECT action_name, action_target, status, error_message, created_at
        FROM automation_log
        WHERE automation_name = 'NegativeReviewResponse'
        ORDER BY id DESC
        LIMIT 6
        """
    ).fetchall()
    for row in reversed(rows):
        r = dict(row)
        marker = (
            "OK " if r["status"] == "success"
            else ("---" if r["status"] == "skipped" else "ERR")
        )
        print(
            f"  [{marker}] {r['action_name']:<38} → {r['action_target'] or 'n/a'}"
        )
        if r["error_message"]:
            print(f"         note: {r['error_message']}")

    print()
    print("Dry-run complete. No external API calls were made.")
    db.close()
