"""
automations/base.py

BaseAutomation — shared scaffold for every automation in this project.
"""
import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

from automations.utils.id_resolver import MappingNotFoundError, resolve, reverse_resolve
from automations.utils.slack_notify import post_slack_message


class BaseAutomation:
    """
    Shared base for all Sparkle & Shine automations.

    Parameters
    ----------
    clients  : callable or dict-like with a get_client(tool_name) interface,
               or any object that supports clients("tool_name") / clients.get_client(...)
    db       : open sqlite3.Connection (row_factory already set)
    dry_run  : if True, no write operations are performed; actions are printed
    """

    def __init__(self, clients: Any, db, dry_run: bool = False):
        self.clients = clients
        self.db = db
        self.dry_run = dry_run

    # ------------------------------------------------------------------ #
    # Run ID
    # ------------------------------------------------------------------ #

    def generate_run_id(self) -> str:
        """Return a UUID4 string to group all actions in one trigger response."""
        return str(uuid.uuid4())

    # ------------------------------------------------------------------ #
    # Audit logging
    # ------------------------------------------------------------------ #

    def log_action(
        self,
        run_id: str,
        action_name: str,
        action_target: Optional[str],
        status: str,
        error_message: Optional[str] = None,
        trigger_source: Optional[str] = None,
        trigger_detail: Optional[Any] = None,
    ) -> None:
        """Write one row to automation_log."""
        automation_name = self.__class__.__name__
        detail_str = (
            json.dumps(trigger_detail) if trigger_detail is not None else None
        )
        with self.db:
            self.db.execute(
                """
                INSERT INTO automation_log
                    (run_id, automation_name, trigger_source, trigger_detail,
                     action_name, action_target, status, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    automation_name,
                    trigger_source,
                    detail_str,
                    action_name,
                    action_target,
                    status,
                    error_message,
                ),
            )

    # ------------------------------------------------------------------ #
    # Cross-tool ID resolution
    # ------------------------------------------------------------------ #

    def resolve_id(self, canonical_id: str, target_tool: str) -> str:
        """
        Return the tool-specific ID for a canonical SS-ID.
        Raises MappingNotFoundError if no mapping exists.
        """
        return resolve(self.db, canonical_id, target_tool)

    def reverse_resolve_id(self, tool_specific_id: str, source_tool: str) -> str:
        """Return the canonical SS-ID for a tool-specific ID."""
        return reverse_resolve(self.db, tool_specific_id, source_tool)

    # ------------------------------------------------------------------ #
    # Slack
    # ------------------------------------------------------------------ #

    def send_slack(
        self,
        channel: str,
        text: str,
        blocks: Optional[list] = None,
    ) -> None:
        """
        Post a Slack message.
        In dry_run mode, prints the message instead of sending it.
        """
        if self.dry_run:
            print(f"[DRY RUN] Would post to #{channel}: {text}")
            return

        slack_client = self.clients("slack")
        post_slack_message(slack_client, channel, text, blocks=blocks)

    # ------------------------------------------------------------------ #
    # Delayed / scheduled actions
    # ------------------------------------------------------------------ #

    def schedule_delayed_action(
        self,
        action_name: str,
        trigger_context_dict: dict,
        delay_hours: float,
    ) -> None:
        """
        Insert a row into pending_actions to be executed after delay_hours.

        execute_after is computed as UTC now + delay_hours.
        """
        execute_after = (
            datetime.now(timezone.utc) + timedelta(hours=delay_hours)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")

        automation_name = self.__class__.__name__
        context_str = json.dumps(trigger_context_dict)

        with self.db:
            self.db.execute(
                """
                INSERT INTO pending_actions
                    (automation_name, action_name, trigger_context, execute_after)
                VALUES (?, ?, ?, ?)
                """,
                (automation_name, action_name, context_str, execute_after),
            )
