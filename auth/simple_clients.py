"""
Clients for the five permanent-token tools:
  Pipedrive, Asana, HubSpot, Mailchimp, Slack
"""
import os
import sys

import requests
import asana
import hubspot
import mailchimp_marketing
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from credentials import get_credential


# ------------------------------------------------------------------ #
# Pipedrive
# ------------------------------------------------------------------ #

def get_pipedrive_session() -> requests.Session:
    """Return a requests.Session pre-loaded with Pipedrive auth header."""
    token = get_credential("PIPEDRIVE_API_TOKEN")
    base_url = os.getenv("PIPEDRIVE_BASE_URL", "https://api.pipedrive.com/v1")

    session = requests.Session()
    session.headers.update({"x-api-token": token})

    # Normalise: strip trailing slash, append /v1 only if the URL doesn't
    # already contain a version segment (e.g. sandbox URLs end with /api).
    normalised = base_url.rstrip("/")
    if not any(seg in normalised for seg in ("/v1", "/v2")):
        normalised = f"{normalised}/v1"

    session.base_url = normalised  # type: ignore[attr-defined]

    try:
        resp = session.get(f"{normalised}/users/me", timeout=10)
        resp.raise_for_status()
    except Exception as exc:
        raise RuntimeError(f"Pipedrive auth failed: {exc}") from exc

    return session


# ------------------------------------------------------------------ #
# Asana
# ------------------------------------------------------------------ #

def get_asana_client() -> asana.ApiClient:
    """Return a configured Asana ApiClient."""
    token = get_credential("ASANA_ACCESS_TOKEN")

    config = asana.Configuration()
    config.access_token = token
    client = asana.ApiClient(config)

    try:
        users_api = asana.UsersApi(client)
        users_api.get_user("me", opts={})
    except Exception as exc:
        raise RuntimeError(f"Asana auth failed: {exc}") from exc

    return client


# ------------------------------------------------------------------ #
# HubSpot
# ------------------------------------------------------------------ #

def get_hubspot_client() -> hubspot.HubSpot:
    """Return a configured HubSpot client."""
    token = get_credential("HUBSPOT_ACCESS_TOKEN")

    client = hubspot.HubSpot(access_token=token)

    try:
        client.crm.owners.owners_api.get_page(limit=1)
    except Exception as exc:
        raise RuntimeError(f"HubSpot auth failed: {exc}") from exc

    return client


# ------------------------------------------------------------------ #
# Mailchimp
# ------------------------------------------------------------------ #

def get_mailchimp_client() -> mailchimp_marketing.Client:
    """Return a configured Mailchimp Marketing client."""
    api_key = get_credential("MAILCHIMP_API_KEY")
    server = get_credential("MAILCHIMP_SERVER_PREFIX")

    client = mailchimp_marketing.Client()
    client.set_config({"api_key": api_key, "server": server})

    try:
        client.ping.get()
    except Exception as exc:
        raise RuntimeError(f"Mailchimp auth failed: {exc}") from exc

    return client


# ------------------------------------------------------------------ #
# Slack
# ------------------------------------------------------------------ #

def get_slack_client() -> WebClient:
    """Return a configured Slack WebClient."""
    token = get_credential("SLACK_BOT_TOKEN")

    client = WebClient(token=token)

    try:
        client.auth_test()
    except SlackApiError as exc:
        raise RuntimeError(f"Slack auth failed: {exc.response['error']}") from exc
    except Exception as exc:
        raise RuntimeError(f"Slack auth failed: {exc}") from exc

    return client
