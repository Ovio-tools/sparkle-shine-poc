"""
Back-fill HubSpot deal mappings in cross_tool_mapping.

The HubSpot pusher creates one deal per commercial client but never registers
those deals in cross_tool_mapping (it only registers the contact mapping).
This script queries all HubSpot deals, resolves the canonical client ID via
the associated contact, then registers canonical_id=proposal_id → deal_id
for every proposal belonging to that client.

Run:  python -m demo.fixes.fix_hubspot_deal_mappings
"""

from __future__ import annotations

import os
import sys

_PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import requests

from credentials import get_credential
from database.schema import get_connection
from database.mappings import get_canonical_id
from seeding.utils.throttler import HUBSPOT

_DB_PATH = os.path.join(_PROJECT_ROOT, "sparkle_shine.db")
_BASE_URL = "https://api.hubapi.com"


def _hs_get(path: str, token: str, params: dict | None = None) -> dict:
    HUBSPOT.wait()
    resp = requests.get(
        f"{_BASE_URL}{path}",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        params=params or {},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def register_deal_mappings() -> None:
    """
    Fetch all HubSpot deals → resolve to canonical client → register
    canonical_id=proposal_id → HubSpot deal_id for every proposal of that client.
    """
    token = get_credential("HUBSPOT_ACCESS_TOKEN")
    conn = get_connection(_DB_PATH)

    # ------------------------------------------------------------------
    # Fetch all HubSpot deals (paginated)
    # ------------------------------------------------------------------
    deals: list[dict] = []
    after = None
    while True:
        params: dict = {"limit": 100, "properties": "dealname,amount,dealstage"}
        if after:
            params["after"] = after
        data = _hs_get("/crm/v3/objects/deals", token, params)
        deals.extend(data.get("results", []))
        after = (data.get("paging") or {}).get("next", {}).get("after")
        if not after:
            break

    print(f"[HubSpot] Found {len(deals)} deals")

    registered = 0
    skipped = 0

    for deal in deals:
        deal_id = deal["id"]
        deal_name = (deal.get("properties") or {}).get("dealname", "")

        # ------------------------------------------------------------------
        # Get associated contacts for this deal
        # ------------------------------------------------------------------
        try:
            assoc_data = _hs_get(
                f"/crm/v3/objects/deals/{deal_id}/associations/contacts",
                token,
            )
            contact_ids = [r["id"] for r in assoc_data.get("results", [])]
        except Exception as exc:
            print(f"  [WARN] Cannot get associations for deal {deal_id} ({deal_name!r}): {exc}")
            skipped += 1
            continue

        if not contact_ids:
            print(f"  [SKIP] Deal {deal_id} ({deal_name!r}): no associated contacts")
            skipped += 1
            continue

        hs_contact_id = contact_ids[0]

        # ------------------------------------------------------------------
        # Resolve canonical client ID
        # ------------------------------------------------------------------
        canonical_client_id = get_canonical_id("hubspot", hs_contact_id, _DB_PATH)
        if not canonical_client_id:
            print(
                f"  [SKIP] Deal {deal_id}: HubSpot contact {hs_contact_id} "
                f"not in cross_tool_mapping"
            )
            skipped += 1
            continue

        # ------------------------------------------------------------------
        # Find all commercial proposals for this client
        # ------------------------------------------------------------------
        prop_rows = conn.execute(
            "SELECT id FROM commercial_proposals WHERE client_id = ? ORDER BY id",
            (canonical_client_id,),
        ).fetchall()

        if not prop_rows:
            print(
                f"  [SKIP] Deal {deal_id} ({deal_name!r}): "
                f"no proposals for client {canonical_client_id}"
            )
            skipped += 1
            continue

        # ------------------------------------------------------------------
        # Register each proposal → HubSpot deal mapping
        # ------------------------------------------------------------------
        for prop_row in prop_rows:
            prop_id = prop_row["id"]
            existing = conn.execute(
                "SELECT 1 FROM cross_tool_mapping WHERE canonical_id = ? AND tool_name = 'hubspot'",
                (prop_id,),
            ).fetchone()
            if existing:
                continue  # already mapped

            conn.execute(
                """
                INSERT INTO cross_tool_mapping
                    (canonical_id, entity_type, tool_name, tool_specific_id, synced_at)
                VALUES (?, 'PROP', 'hubspot', ?, datetime('now'))
                """,
                (prop_id, deal_id),
            )
            registered += 1
            print(f"  [OK] {prop_id} → HubSpot deal {deal_id} ({deal_name!r})")

    conn.commit()
    conn.close()
    print(f"\n[Done] Registered {registered} mappings, skipped {skipped}")


if __name__ == "__main__":
    register_deal_mappings()
