"""
scripts/reauth_google.py

Re-authorize Google OAuth with the full set of required scopes, then
optionally push the new token to the Railway PostgreSQL database.

Usage:
    python scripts/reauth_google.py              # re-auth locally (browser flow)
    python scripts/reauth_google.py --push-db    # read existing token.json and push to DB
                                                 # (use with `railway run` to target Railway)

The two-step workflow for fixing Railway:
    1. python3 scripts/reauth_google.py                          # local: browser consent
    2. railway run python3 scripts/reauth_google.py --push-db    # push token to Railway DB
"""
from __future__ import annotations

import argparse
import json
import os
import sys

# Ensure project root is on the path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from auth.google_auth import _SCOPES, _token_file
from auth import token_store


def main():
    parser = argparse.ArgumentParser(
        description="Re-authorize Google OAuth with updated scopes"
    )
    parser.add_argument(
        "--push-db",
        action="store_true",
        help="Skip browser flow — read the existing token.json and push it to the DB. "
             "Use with `railway run` to target Railway Postgres.",
    )
    args = parser.parse_args()

    token_path = _token_file()

    if args.push_db:
        # --push-db mode: read existing token.json and save to DB
        if not os.path.exists(token_path):
            print(f"ERROR: {token_path} does not exist. Run without --push-db first.")
            sys.exit(1)

        with open(token_path) as f:
            token_json = json.load(f)

        granted_scopes = set(token_json.get("scopes", []))
        missing = [s for s in _SCOPES if s not in granted_scopes]
        if missing:
            print(f"WARNING: token.json is missing scopes: {missing}")
            print("Run without --push-db first to re-authorize with full scopes.")
            sys.exit(1)

        print(f"Reading token from {token_path}")
        print(f"Scopes: {len(granted_scopes)} (all required scopes present)")

        token_store.save_tokens("google", token_json)
        db_url = os.getenv("DATABASE_URL", "(not set)")
        print(f"Saved token to DB (DATABASE_URL: {db_url[:40]}...)")
        print("\nDone. Gmail drafts should work on the next sales-outreach run.")
        return

    # Normal mode: delete stale token, run browser consent flow
    if os.path.exists(token_path):
        os.remove(token_path)
        print(f"Deleted stale {token_path}")

    from auth.google_auth import _credentials_file
    try:
        creds_file = _credentials_file()
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}")
        sys.exit(1)

    print(f"Using credentials file: {creds_file}")
    print(f"Required scopes ({len(_SCOPES)}):")
    for s in _SCOPES:
        print(f"  - {s}")

    from google_auth_oauthlib.flow import InstalledAppFlow

    print("\nLaunching browser consent flow...")
    flow = InstalledAppFlow.from_client_secrets_file(creds_file, _SCOPES)
    creds = flow.run_local_server(port=8025, open_browser=True)

    if not creds or not creds.valid:
        print("ERROR: Consent flow did not return valid credentials.")
        sys.exit(1)

    token_json = json.loads(creds.to_json())
    granted_scopes = set(token_json.get("scopes", []))
    print(f"\nGranted scopes ({len(granted_scopes)}):")
    for s in sorted(granted_scopes):
        print(f"  - {s}")

    missing = [s for s in _SCOPES if s not in granted_scopes]
    if missing:
        print(f"\nWARNING: Still missing scopes: {missing}")
        print("You may need to remove the app from your Google account's ")
        print("'Third-party apps with account access' and re-run this script.")

    # Save to token.json
    with open(token_path, "w") as f:
        json.dump(token_json, f, indent=2)
    print(f"\nSaved token to {token_path}")

    # Save to local DB
    token_store.save_tokens("google", token_json)
    db_url = os.getenv("DATABASE_URL", "(not set)")
    print(f"Saved token to local DB (DATABASE_URL: {db_url[:40]}...)")

    print(
        "\nNext step: push to Railway DB:\n"
        "  railway run python3 scripts/reauth_google.py --push-db"
    )


if __name__ == "__main__":
    main()
