"""
scripts/backfill_pipedrive_orgs.py

One-off backfill: for every HubSpot contact that has a company set but whose
Pipedrive person has no organization, update the Pipedrive person with
org_name from HubSpot.

Usage:
    python scripts/backfill_pipedrive_orgs.py           # dry run (default)
    python scripts/backfill_pipedrive_orgs.py --live    # live run

Scope:
  - All canonical IDs that have both a 'hubspot' AND a 'pipedrive' (or
    'pipedrive_person') mapping in cross_tool_mapping.
  - SS-PROP entries (deals/proposals) are excluded — only persons are updated.
"""
import os
import sys
import time

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

from auth import get_client          # noqa: E402 — loads .env via credentials.py
import sqlite3                       # noqa: E402


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _pd_base(session) -> str:
    base = session.base_url.rstrip("/")
    if not any(s in base for s in ("/v1", "/v2")):
        base += "/v1"
    return base


def _fetch_hs_contact_data(hs_client, hs_ids: list[str]) -> dict[str, dict]:
    """
    Batch-fetch 'company' and 'email' for a list of HubSpot contact IDs.
    Returns {hs_id: {"company": str, "email": str}}.
    Uses batches of 100 (HubSpot limit).
    """
    from hubspot.crm.contacts.models import BatchReadInputSimplePublicObjectId

    result: dict[str, dict] = {}
    batch_size = 100

    for i in range(0, len(hs_ids), batch_size):
        chunk = hs_ids[i : i + batch_size]
        payload = BatchReadInputSimplePublicObjectId(
            properties=["company", "email"],
            inputs=[{"id": h} for h in chunk],
        )
        resp = hs_client.crm.contacts.batch_api.read(payload)
        for contact in (resp.results or []):
            props = contact.properties or {}
            result[str(contact.id)] = {
                "company": (props.get("company") or "").strip(),
                "email":   (props.get("email")   or "").strip().lower(),
            }
        time.sleep(0.15)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main(live: bool) -> None:
    dry_run = not live
    label = "LIVE" if live else "DRY RUN"
    print("=" * 65)
    print(f"  Backfill Pipedrive Orgs from HubSpot — {label}")
    print("=" * 65)

    db_path = os.path.join(_ROOT, "sparkle_shine.db")
    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row

    # ── Step 1: Collect all contacts with both HubSpot and Pipedrive mappings ─
    rows = db.execute(
        """
        SELECT h.canonical_id,
               h.tool_specific_id  AS hs_id,
               COALESCE(pp.tool_specific_id, p.tool_specific_id) AS pd_person_id
        FROM cross_tool_mapping h
        LEFT JOIN cross_tool_mapping p
               ON p.canonical_id = h.canonical_id AND p.tool_name = 'pipedrive'
        LEFT JOIN cross_tool_mapping pp
               ON pp.canonical_id = h.canonical_id AND pp.tool_name = 'pipedrive_person'
        WHERE h.tool_name = 'hubspot'
          AND h.entity_type IN ('CLIENT', 'LEAD')
          AND (p.canonical_id IS NOT NULL OR pp.canonical_id IS NOT NULL)
        ORDER BY h.canonical_id
        """
    ).fetchall()

    contacts = [
        {
            "canonical_id": r["canonical_id"],
            "hs_id":        r["hs_id"],
            "pd_person_id": r["pd_person_id"],
        }
        for r in rows
        if r["pd_person_id"]
    ]
    print(f"\nContacts with HubSpot + Pipedrive mappings: {len(contacts)}")

    # ── Step 2: Check each Pipedrive person for existing org ──────────────────
    pd_client = get_client("pipedrive")
    base      = _pd_base(pd_client)

    needs_update: list[dict] = []  # contacts that need org set
    already_set: int         = 0
    pd_missing: int          = 0

    print("Checking Pipedrive persons for existing organizations...")
    for c in contacts:
        resp = pd_client.get(f"{base}/persons/{c['pd_person_id']}", timeout=15)
        if resp.status_code != 200:
            pd_missing += 1
            continue
        person = resp.json().get("data") or {}
        pd_email = ""
        for e in (person.get("email") or []):
            if e.get("primary"):
                pd_email = (e.get("value") or "").lower()
                break
        if person.get("org_id"):
            already_set += 1
        else:
            needs_update.append({
                **c,
                "pd_name":  person.get("name", ""),
                "pd_email": pd_email,
            })
        time.sleep(0.05)

    print(f"  Already have an org:  {already_set}")
    print(f"  No org (candidates):  {len(needs_update)}")
    if pd_missing:
        print(f"  Person not found:     {pd_missing}")

    if not needs_update:
        print("\nNothing to update.")
        db.close()
        return

    # ── Step 3: Batch-fetch HubSpot 'company' for candidates ──────────────────
    hs_ids = [c["hs_id"] for c in needs_update]
    print(f"\nFetching 'company' from HubSpot for {len(hs_ids)} contacts...")
    hs_client      = get_client("hubspot")
    hs_company_map = _fetch_hs_contact_data(hs_client, hs_ids)

    email_mismatch: int = 0
    to_patch = []
    no_company = 0
    for c in needs_update:
        data    = hs_company_map.get(c["hs_id"], {})
        company = data.get("company", "").strip()
        hs_email = data.get("email", "").strip().lower()

        if not company:
            no_company += 1
            continue

        # Verify emails match to guard against cross-contaminated mappings
        if c["pd_email"] and hs_email and c["pd_email"] != hs_email:
            print(
                f"  SKIP  {c['canonical_id']} persons/{c['pd_person_id']} "
                f"({c['pd_name']}) — email mismatch: "
                f"Pipedrive={c['pd_email']} vs HubSpot={hs_email}"
            )
            email_mismatch += 1
            continue

        to_patch.append({**c, "company": company})
    print(f"  HubSpot has company:  {len(to_patch) + email_mismatch}")
    print(f"  HubSpot no company:   {no_company}")
    if email_mismatch:
        print(f"  Skipped (email mismatch): {email_mismatch}")

    if not to_patch:
        print("\nNo contacts with a company to patch.")
        db.close()
        return

    # ── Step 4: Update Pipedrive persons ──────────────────────────────────────
    print(f"\n{'[DRY RUN] ' if dry_run else ''}Patching {len(to_patch)} Pipedrive person(s)...\n")
    updated = 0
    failed  = 0

    # Cache org_id lookups to avoid creating duplicate orgs within this run
    _org_id_cache: dict[str, int] = {}

    def _get_or_create_org(company: str) -> int:
        """Return Pipedrive org_id for company, creating it if necessary."""
        if company in _org_id_cache:
            return _org_id_cache[company]
        # Search first
        sr = pd_client.get(
            f"{base}/organizations/search",
            params={"term": company, "limit": 10, "exact_match": True},
            timeout=15,
        )
        for item in (sr.json().get("data", {}).get("items") or []):
            org = item.get("item") or {}
            if org.get("name", "").lower() == company.lower():
                _org_id_cache[company] = int(org["id"])
                return _org_id_cache[company]
        # Not found — create
        cr = pd_client.post(
            f"{base}/organizations",
            json={"name": company},
            timeout=15,
        )
        cr.raise_for_status()
        org_id = int(cr.json()["data"]["id"])
        _org_id_cache[company] = org_id
        return org_id

    for c in to_patch:
        name    = c["pd_name"] or c["canonical_id"]
        company = c["company"]
        pid     = c["pd_person_id"]

        if dry_run:
            print(
                f"  [DRY RUN] Would find/create org '{company}' then "
                f"PATCH persons/{pid} ({name}) → org_id"
            )
            updated += 1
            continue

        try:
            org_id = _get_or_create_org(company)
            time.sleep(0.1)
        except Exception as exc:
            print(f"  FAIL org lookup/create for '{company}': {exc}")
            failed += 1
            continue

        resp = pd_client.put(
            f"{base}/persons/{pid}",
            json={"org_id": org_id},
            timeout=15,
        )
        if resp.ok:
            print(f"  OK   persons/{pid:<4}  ({name}) → org_id={org_id} ('{company}')")
            updated += 1
        else:
            print(
                f"  FAIL persons/{pid:<4}  ({name}) → "
                f"HTTP {resp.status_code}: {resp.text[:120]}"
            )
            failed += 1
        time.sleep(0.1)

    print()
    print("─" * 65)
    print(f"Updated:  {updated}")
    if failed:
        print(f"Failed:   {failed}")
    print(f"Skipped (already had org):    {already_set}")
    print(f"Skipped (no HubSpot company): {no_company + pd_missing}")
    if email_mismatch:
        print(f"Skipped (email mismatch):     {email_mismatch}")
    db.close()


if __name__ == "__main__":
    live = "--live" in sys.argv
    main(live=live)
