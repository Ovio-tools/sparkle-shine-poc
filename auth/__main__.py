"""
CLI auth verifier.

Usage:
    python -m auth --verify
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from auth import get_client, _TOOL_NAMES

_GOOGLE_TOOLS = {"google_drive", "google_docs", "google_sheets", "google_calendar", "google_gmail"}


def verify_all():
    print("\nVerifying auth for all tools...\n")
    print(f"  {'Tool':<20} {'Status'}")
    print(f"  {'-'*20} {'-'*30}")

    results = {}
    for tool in _TOOL_NAMES:
        # Skip individual Google services after the first one passes —
        # they all share the same credential object.
        if tool in _GOOGLE_TOOLS and tool != "google_drive":
            if results.get("google_drive") == "OK":
                results[tool] = "OK (shared creds)"
                print(f"  {tool:<20} OK  (shared Google credentials)")
                continue

        try:
            get_client(tool)
            results[tool] = "OK"
            print(f"  {tool:<20} OK")
        except Exception as exc:
            results[tool] = f"FAILED: {exc}"
            print(f"  {tool:<20} FAILED — {exc}")

    passed = sum(1 for v in results.values() if v.startswith("OK"))
    total = len(results)
    print(f"\n  {passed}/{total} tools authenticated successfully.\n")
    return passed == total


if __name__ == "__main__":
    if "--verify" not in sys.argv:
        print("Usage: python -m auth --verify")
        sys.exit(0)

    ok = verify_all()
    sys.exit(0 if ok else 1)
