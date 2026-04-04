"""
automations/helpers/gmail_draft.py

Helper for creating Gmail drafts via the Gmail API v1.
Used by Automation #7: Sales Research & Outreach Agent Chain.
"""
from __future__ import annotations

import base64
import logging
from email.mime.text import MIMEText

from auth import get_client

logger = logging.getLogger("automation_07")

_FROM_ADDRESS = "maria@sparkleshineaustin.com"


def create_gmail_draft(to_email: str, subject: str, body_text: str) -> dict | None:
    """
    Create a Gmail draft and return the draft ID + deep link.

    Uses auth.get_client("google") with service_name="gmail", version="v1".

    Args:
        to_email: Recipient email address
        subject: Email subject line
        body_text: Plain text email body

    Returns:
        dict: {"draft_id": str, "gmail_link": str}
        On failure: None
    """
    try:
        service = get_client("google", service_name="gmail", version="v1")

        message = MIMEText(body_text)
        message["to"] = to_email
        message["subject"] = subject
        message["from"] = _FROM_ADDRESS

        raw = base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")

        draft = service.users().drafts().create(
            userId="me",
            body={"message": {"raw": raw}}
        ).execute()

        draft_id = draft["id"]
        gmail_link = f"https://mail.google.com/mail/u/0/#drafts?compose={draft_id}"

        logger.info("Created Gmail draft %s for %s", draft_id, to_email)
        return {"draft_id": draft_id, "gmail_link": gmail_link}

    except Exception:
        logger.exception("Failed to create Gmail draft for %s (subject: %r)", to_email, subject)
        return None
