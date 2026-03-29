"""
simulation/deep_links.py

Build clickable UI deep links for each SaaS tool and format them as
Slack mrkdwn citations. Used by the weekly report generator.

Lazy-loads Pipedrive subdomain and HubSpot portal ID on first call to
get_deep_link() — one API call each per process lifetime. Falls back
gracefully if the call fails: deep links degrade to plain text, not an
error.

For Asana tasks, record_type is the project name (e.g. "Client Success").
Project GIDs are loaded from config/tool_ids.json.
"""
from __future__ import annotations

import json
import os
import requests
from pathlib import Path

from auth import get_client
from intelligence.logging_config import setup_logging

logger = setup_logging(__name__)

# ── Module-level lazy cache ──────────────────────────────────────────────────
_pipedrive_subdomain: str | None = None
_hubspot_portal_id: str | None = None
_cache_loaded: bool = False

# ── Asana project GIDs (from config/tool_ids.json, loaded on first use) ─────
_asana_project_gids: dict[str, str] | None = None


def _get_asana_project_gids() -> dict[str, str]:
    global _asana_project_gids
    if _asana_project_gids is None:
        tool_ids_path = Path(__file__).parent.parent / "config" / "tool_ids.json"
        with open(tool_ids_path) as f:
            tool_ids = json.load(f)
        _asana_project_gids = tool_ids.get("asana", {}).get("projects", {})
    return _asana_project_gids


# ── Account info loader ──────────────────────────────────────────────────────

def _load_account_info() -> None:
    """Populate _pipedrive_subdomain and _hubspot_portal_id.

    Sets _cache_loaded=True regardless of success so failures don't
    trigger repeated API calls within the same process.
    """
    global _pipedrive_subdomain, _hubspot_portal_id, _cache_loaded
    if _cache_loaded:
        return

    try:
        session = get_client("pipedrive")
        resp = session.get("https://api.pipedrive.com/v1/users/me")
        resp.raise_for_status()
        _pipedrive_subdomain = resp.json().get("data", {}).get("company_domain")
    except Exception as exc:
        logger.warning("Could not load Pipedrive subdomain for deep links: %s", exc)

    try:
        token = os.getenv("HUBSPOT_ACCESS_TOKEN", "")
        if not token:
            raise ValueError("HUBSPOT_ACCESS_TOKEN not set")
        resp = requests.get(
            "https://api.hubapi.com/integrations/v1/me",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        )
        resp.raise_for_status()
        portal_id = resp.json().get("portalId")
        _hubspot_portal_id = str(portal_id) if portal_id else None
    except Exception as exc:
        logger.warning("Could not load HubSpot portal ID for deep links: %s", exc)

    _cache_loaded = True


# ── QBO environment detection ────────────────────────────────────────────────

def _qbo_ui_base() -> str:
    """Return the QBO UI base URL.

    Mirrors the sandbox-detection logic in auth/quickbooks_auth.py get_base_url():
    check QBO_BASE_URL env var; if it contains 'sandbox', use the sandbox UI host.
    """
    api_base = os.getenv(
        "QBO_BASE_URL",
        "https://sandbox-quickbooks.api.intuit.com/v3/company",
    )
    if "sandbox" in api_base:
        return "https://app.sandbox.qbo.intuit.com/app"
    return "https://app.qbo.intuit.com/app"


# ── URL builders ─────────────────────────────────────────────────────────────

def get_deep_link(tool: str, record_type: str, record_id: str) -> str:
    """Return a clickable UI URL for the given tool record.

    For Asana tasks, pass the project name as record_type
    (e.g. "Client Success", "Admin & Operations"). The project GID
    is looked up in config/tool_ids.json. If the project isn't found,
    falls back to app.asana.com/0/search?q={record_id}.

    Returns "#" if the URL cannot be built (missing credentials,
    unknown tool/record_type, or API failure on cache load).
    """
    _load_account_info()

    try:
        if tool == "hubspot":
            pid = _hubspot_portal_id or ""
            if not pid:
                return "#"
            if record_type == "contact":
                return f"https://app.hubspot.com/contacts/{pid}/contact/{record_id}"
            if record_type == "deal":
                return f"https://app.hubspot.com/contacts/{pid}/deal/{record_id}"

        elif tool == "pipedrive":
            sub = _pipedrive_subdomain or ""
            if not sub:
                return "#"
            if record_type == "deal":
                return f"https://{sub}.pipedrive.com/deal/{record_id}"
            if record_type == "person":
                return f"https://{sub}.pipedrive.com/person/{record_id}"

        elif tool == "jobber":
            if record_type == "client":
                return f"https://app.getjobber.com/client/{record_id}"
            if record_type == "job":
                return f"https://app.getjobber.com/work_requests/{record_id}"

        elif tool == "quickbooks":
            base = _qbo_ui_base()
            if record_type == "invoice":
                return f"{base}/invoice?txnId={record_id}"
            if record_type == "report_pl":
                return f"{base}/reportv2?token=PROFIT_AND_LOSS"
            if record_type == "report_ar":
                return f"{base}/reportv2?token=AGING_DETAIL"

        elif tool == "asana":
            gids = _get_asana_project_gids()
            project_gid = gids.get(record_type)
            if project_gid:
                return f"https://app.asana.com/0/{project_gid}/{record_id}"
            logger.debug(
                "Asana project '%s' not in tool_ids.json — using search fallback",
                record_type,
            )
            return f"https://app.asana.com/0/search?q={record_id}"

        elif tool == "mailchimp":
            # MAILCHIMP_SERVER_PREFIX stores the full prefix (e.g. "us4"), not just
            # the numeric part. Use it directly — do NOT prepend "us" again.
            dc = os.getenv("MAILCHIMP_SERVER_PREFIX") or os.getenv(
                "MAILCHIMP_DATA_CENTER", "us1"
            )
            if record_type == "campaign":
                return f"https://{dc}.admin.mailchimp.com/campaigns/show?id={record_id}"

    except Exception as exc:
        logger.warning(
            "Deep link build failed for %s/%s/%s: %s", tool, record_type, record_id, exc
        )

    return "#"


def get_report_link(tool: str) -> str:
    """Return a URL to the tool's aggregate report or dashboard.

    Used for revenue and cash-flow citations that refer to a whole-week
    view rather than a specific transaction.
    """
    if tool == "quickbooks":
        return f"{_qbo_ui_base()}/reportv2?token=PROFIT_AND_LOSS"
    return "#"


def format_citation(text: str, tool: str, record_type: str, record_id: str) -> str:
    """Format a Slack mrkdwn citation.

    Returns "(<URL|text>)" if a URL is available, or plain text if not.
    The plain-text fallback means the report generates even when deep
    links are unavailable.
    """
    url = get_deep_link(tool, record_type, record_id)
    if url == "#":
        return text
    return f"(<{url}|{text}>)"
