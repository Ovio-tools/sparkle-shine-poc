"""
Unified client factory for all Sparkle & Shine tools.

Usage:
    from auth import get_client
    session = get_client("pipedrive")
    client  = get_client("hubspot")
    service = get_client("google_drive")
"""
from typing import Any

_TOOL_NAMES = [
    "pipedrive",
    "jobber",
    "quickbooks",
    "asana",
    "hubspot",
    "mailchimp",
    "slack",
    "google_drive",
    "google_docs",
    "google_sheets",
    "google_calendar",
    "google_gmail",
]


def get_client(tool_name: str) -> Any:
    """
    Return the authenticated client/session/service for the given tool.

    Supported tool names:
        pipedrive, jobber, quickbooks, asana, hubspot, mailchimp, slack,
        google_drive, google_docs, google_sheets, google_calendar, google_gmail
    """
    name = tool_name.lower().strip()

    if name == "pipedrive":
        from auth.simple_clients import get_pipedrive_session
        return get_pipedrive_session()

    if name == "jobber":
        from auth.jobber_auth import get_jobber_session
        return get_jobber_session()

    if name == "quickbooks":
        from auth.quickbooks_auth import get_quickbooks_headers
        return get_quickbooks_headers()

    if name == "asana":
        from auth.simple_clients import get_asana_client
        return get_asana_client()

    if name == "hubspot":
        from auth.simple_clients import get_hubspot_client
        return get_hubspot_client()

    if name == "mailchimp":
        from auth.simple_clients import get_mailchimp_client
        return get_mailchimp_client()

    if name == "slack":
        from auth.simple_clients import get_slack_client
        return get_slack_client()

    if name == "google_drive":
        from auth.google_auth import get_drive_service
        return get_drive_service()

    if name == "google_docs":
        from auth.google_auth import get_docs_service
        return get_docs_service()

    if name == "google_sheets":
        from auth.google_auth import get_sheets_service
        return get_sheets_service()

    if name == "google_calendar":
        from auth.google_auth import get_calendar_service
        return get_calendar_service()

    if name == "google_gmail":
        from auth.google_auth import get_gmail_service
        return get_gmail_service()

    raise ValueError(
        f"Unknown tool '{tool_name}'. Valid names: {', '.join(_TOOL_NAMES)}"
    )
