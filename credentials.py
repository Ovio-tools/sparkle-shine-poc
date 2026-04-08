from __future__ import annotations

import json
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

_PROJECT_ROOT = Path(__file__).resolve().parent

REQUIRED_KEYS = [
    "ANTHROPIC_API_KEY",
    "PIPEDRIVE_API_TOKEN",
    "JOBBER_ACCESS_TOKEN",
    "QBO_ACCESS_TOKEN",
    "QBO_COMPANY_ID",
    "ASANA_ACCESS_TOKEN",
    "ASANA_WORKSPACE_GID",
    "HUBSPOT_ACCESS_TOKEN",
    "MAILCHIMP_API_KEY",
    "MAILCHIMP_SERVER_PREFIX",
    "SLACK_BOT_TOKEN",
]

_GOOGLE_ENV_KEYS = [
    "GOOGLE_CLIENT_ID",
    "GOOGLE_CLIENT_SECRET",
    "GOOGLE_REFRESH_TOKEN",
]


def _resolve_google_file(env_key: str, default_name: str) -> Path | None:
    raw = os.getenv(env_key, default_name)
    if not raw:
        return None

    candidate = Path(raw)
    if candidate.is_absolute():
        return candidate if candidate.exists() else None

    project_path = _PROJECT_ROOT / raw
    if project_path.exists():
        return project_path

    parent_path = _PROJECT_ROOT.parent / raw
    if parent_path.exists():
        return parent_path

    return None


def _load_json(path: Path | None) -> dict:
    if path is None or not path.exists():
        return {}
    try:
        with path.open() as f:
            return json.load(f)
    except Exception:
        return {}


def google_auth_mode() -> str | None:
    """Return the configured non-interactive Google auth mode, if any."""
    if all(os.getenv(key) for key in _GOOGLE_ENV_KEYS):
        return "env"

    credentials_path = _resolve_google_file("GOOGLE_CREDENTIALS_FILE", "credentials.json")
    token_path = _resolve_google_file("GOOGLE_TOKEN_FILE", "token.json")
    token_data = _load_json(token_path)
    if credentials_path and token_data.get("refresh_token"):
        return "files"

    return None


def google_noninteractive_credentials_available() -> bool:
    return google_auth_mode() is not None


def missing_required_credentials() -> list[str]:
    missing = [key for key in REQUIRED_KEYS if not os.getenv(key)]
    if not google_noninteractive_credentials_available():
        missing.append(
            "GOOGLE_AUTH (set GOOGLE_CLIENT_ID/GOOGLE_CLIENT_SECRET/GOOGLE_REFRESH_TOKEN "
            "or provide GOOGLE_CREDENTIALS_FILE/GOOGLE_TOKEN_FILE with a refresh token)"
        )
    return missing


def get_credential(key: str) -> str:
    value = os.getenv(key)
    if not value:
        raise EnvironmentError(
            f"Missing required credential: '{key}'. "
            f"Please set it in your .env file (see .env.example)."
        )
    return value


def verify_all() -> bool:
    missing = set(missing_required_credentials())
    all_present = not missing

    for key in REQUIRED_KEYS:
        if os.getenv(key):
            print(f"  OK      {key}")
        else:
            print(f"  MISSING {key}")

    mode = google_auth_mode()
    if mode == "env":
        print("  OK      GOOGLE_AUTH (via env vars)")
    elif mode == "files":
        print("  OK      GOOGLE_AUTH (via credentials.json + token.json)")
    else:
        print("  MISSING GOOGLE_AUTH")

    return all_present


if __name__ == "__main__":
    print("Verifying credentials...\n")
    result = verify_all()
    print()
    if result:
        print("All credentials present.")
    else:
        print("One or more credentials are missing.")
