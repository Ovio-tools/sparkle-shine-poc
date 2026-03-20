"""
Push Sparkle & Shine commercial proposals to Pipedrive as deals with full stage histories.

Full run:  python seeding/pushers/push_pipedrive.py
Dry run:   python seeding/pushers/push_pipedrive.py --dry-run

Push order:
  Phase 1 — Persons        (contacts for each commercial lead / client)
  Phase 2 — Organizations  (companies linked to persons)
  Phase 3 — Deals          (48 commercial proposals, created at New Lead stage)
  Phase 4 — Stage History  (sequential PUT to simulate deal progression)
  Phase 5 — Activities     (one per stage traversed, with LLM-generated notes)
  Phase 6 — Won/Lost       (PUT status=won / status=lost + lost_reason)
  Phase 7 — Verification   (GET /v1/deals?pipeline_id=2&status=all_not_deleted)

Auth: auth.get_client("pipedrive") validates credentials on startup.
      PIPEDRIVE_API_TOKEN is appended as ?api_token= on every request.

Custom deal fields (pre-created in Pipedrive, keys from tool_ids.json):
  Client Type, Estimated Monthly Value, Lead Source

Custom person fields:
  HubSpot Contact ID, Jobber Client ID, Acquisition Source, Neighborhood
"""

from __future__ import annotations

import json
import os
import sys
from datetime import date, timedelta
from typing import Optional

import requests

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from auth import get_client                                                     # noqa: E402
from credentials import get_credential                                          # noqa: E402
from database.schema import get_connection                                      # noqa: E402
from database.mappings import register_mapping, get_tool_id, find_unmapped     # noqa: E402
from seeding.utils.throttler import PIPEDRIVE                                   # noqa: E402
from seeding.utils.text_generator import generate_pipedrive_activity_note      # noqa: E402

_DB_PATH  = os.path.join(_PROJECT_ROOT, "sparkle_shine.db")
_BASE_URL = "https://api.pipedrive.com/v1"

# ---------------------------------------------------------------------------
# Tool IDs (loaded once at import time)
# ---------------------------------------------------------------------------

with open(os.path.join(_PROJECT_ROOT, "config", "tool_ids.json")) as _f:
    _TOOL_IDS = json.load(_f)

_PD           = _TOOL_IDS["pipedrive"]
_PIPELINE_ID  = _PD["pipelines"]["Cleaning Services Sales"]   # 2
_STAGES       = _PD["stages"]                                  # {"New Lead": 7, ...}
_DEAL_FIELDS  = _PD["deal_fields"]
_PERSON_FIELDS = _PD["person_fields"]

# Stage ID constants
_S_NEW_LEAD   = _STAGES["New Lead"]              # 7
_S_QUALIFIED  = _STAGES["Qualified"]             # 8
_S_SITE_VISIT = _STAGES["Site Visit Scheduled"]  # 9
_S_PROP_SENT  = _STAGES["Proposal Sent"]         # 10
_S_NEGOT      = _STAGES["Negotiation"]           # 11
_S_WON        = _STAGES["Closed Won"]            # 12

# stage_id → (pipedrive_activity_type, stage_label, default_outcome)
_STAGE_META: dict[int, tuple[str, str, str]] = {
    _S_NEW_LEAD:   ("call",    "New Lead",             "initial contact made"),
    _S_QUALIFIED:  ("email",   "Qualified",            "qualified — scheduling site visit"),
    _S_SITE_VISIT: ("meeting", "Site Visit Scheduled", "site visit completed, scope confirmed"),
    _S_PROP_SENT:  ("email",   "Proposal Sent",        "proposal sent, awaiting decision"),
    _S_NEGOT:      ("call",    "Negotiation",          "negotiation in progress"),
    _S_WON:        ("meeting", "Closed Won",           "deal closed"),
}

# Pipedrive activity type → text_generator note type label
_ACT_TO_NOTE_TYPE = {
    "call":    "call_recap",
    "email":   "email_summary",
    "meeting": "site_visit_notes",
}

# Populated by push_organizations(); consumed by push_deals()
_company_to_org_id: dict[str, int] = {}

# Set in main()
_api_token: str = ""
_session: Optional[requests.Session] = None


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"Content-Type": "application/json"})
    return s


def _url(path: str) -> str:
    return f"{_BASE_URL}{path}?api_token={_api_token}"


def _get(path: str, params: Optional[dict] = None) -> dict:
    PIPEDRIVE.wait()
    PIPEDRIVE.track_call(path)
    resp = _session.get(_url(path), params=params or {}, timeout=30)
    if not resp.ok:
        raise RuntimeError(f"Pipedrive GET {path} → {resp.status_code}: {resp.text[:300]}")
    return resp.json()


def _post(path: str, payload: dict) -> dict:
    PIPEDRIVE.wait()
    PIPEDRIVE.track_call(path)
    resp = _session.post(_url(path), json=payload, timeout=30)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Pipedrive POST {path} → {resp.status_code}: {resp.text[:300]}")
    body = resp.json()
    if not body.get("success"):
        raise RuntimeError(f"Pipedrive POST {path} success=false: {resp.text[:300]}")
    return body.get("data") or {}


def _put(path: str, payload: dict) -> dict:
    PIPEDRIVE.wait()
    PIPEDRIVE.track_call(path)
    resp = _session.put(_url(path), json=payload, timeout=30)
    if not resp.ok:
        raise RuntimeError(f"Pipedrive PUT {path} → {resp.status_code}: {resp.text[:300]}")
    body = resp.json()
    return body.get("data") or {}


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def _fetch_proposal_contacts(conn) -> list[dict]:
    """Return all unique clients and leads referenced by commercial proposals."""
    rows = conn.execute("""
        SELECT DISTINCT
            'client'                AS entity_kind,
            c.id                    AS entity_id,
            TRIM(COALESCE(c.first_name,'') || ' ' || COALESCE(c.last_name,'')) AS full_name,
            c.email,
            c.phone,
            c.company_name,
            c.acquisition_source    AS acq_source,
            c.neighborhood,
            ctm_hs.tool_specific_id AS hubspot_contact_id,
            ctm_j.tool_specific_id  AS jobber_client_id
        FROM commercial_proposals cp
        JOIN clients c ON c.id = cp.client_id
        LEFT JOIN cross_tool_mapping ctm_hs
            ON ctm_hs.canonical_id = c.id AND ctm_hs.tool_name = 'hubspot'
        LEFT JOIN cross_tool_mapping ctm_j
            ON ctm_j.canonical_id = c.id AND ctm_j.tool_name = 'jobber'
        WHERE cp.client_id IS NOT NULL

        UNION

        SELECT DISTINCT
            'lead'                  AS entity_kind,
            l.id                    AS entity_id,
            TRIM(COALESCE(l.first_name,'') || ' ' || COALESCE(l.last_name,'')) AS full_name,
            l.email,
            l.phone,
            l.company_name,
            l.source                AS acq_source,
            NULL                    AS neighborhood,
            ctm_hs.tool_specific_id AS hubspot_contact_id,
            NULL                    AS jobber_client_id
        FROM commercial_proposals cp
        JOIN leads l ON l.id = cp.lead_id
        LEFT JOIN cross_tool_mapping ctm_hs
            ON ctm_hs.canonical_id = l.id AND ctm_hs.tool_name = 'hubspot'
        WHERE cp.lead_id IS NOT NULL

        ORDER BY entity_kind, entity_id
    """).fetchall()
    return [dict(r) for r in rows]


def _fetch_proposals(conn) -> list[dict]:
    """Return all 48 commercial proposals with joined contact fields.

    is_historical: True for the first 33 proposals (10 won + 23 lost), which were
    generated before the 15 active pipeline proposals.  Used to distinguish the 5
    'draft' proposals that lost at Qualified from the 4 active Qualified proposals
    that happen to also have status='draft' and note_count=2.
    """
    rows = conn.execute("""
        SELECT
            cp.id,
            cp.lead_id,
            cp.client_id,
            cp.service_scope,
            cp.monthly_value,
            cp.status,
            cp.sent_date,
            cp.decision_date,
            cp.notes,
            COALESCE(c.company_name, l.company_name, '') AS company_name,
            COALESCE(c.acquisition_source, l.source, '') AS lead_source,
            COALESCE(cp.client_id, cp.lead_id)           AS contact_canonical_id,
            CASE WHEN CAST(SUBSTR(cp.id, 9) AS INTEGER) <= 33 THEN 1 ELSE 0 END
                                                          AS is_historical
        FROM commercial_proposals cp
        LEFT JOIN clients c ON c.id = cp.client_id
        LEFT JOIN leads   l ON l.id = cp.lead_id
        ORDER BY cp.id
    """).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Stage-sequence logic
# ---------------------------------------------------------------------------

def _stage_patches(proposal: dict) -> tuple[list[int], Optional[str], Optional[str]]:
    """
    Return (patch_stages, final_status, lost_reason).

    patch_stages  — stage IDs to PUT to after initial deal creation at New Lead (7).
    final_status  — "won", "lost", or None (open deal).
    lost_reason   — plain-text reason string when final_status == "lost".

    Stage-sequence rules (derived from gen_marketing.py note-count patterns):
      Won (10):
        note_count <= 5  → no negotiation: [Qualified, Site Visit, Proposal Sent, Closed Won]
        note_count >= 6  → with negotiation: [... Negotiation, Closed Won]
      Lost-status (18):
        note_count == 5  → lost at Proposal Sent (chose competitor)
        note_count >= 6  → lost at Negotiation (no budget / post-contract; simplified)
      Draft + is_historical (5):
        → lost at Qualified (price too high); SS-PROP-0011 through SS-PROP-0015
      Draft/open (11) + Sent (4):
        → open deals at their current stage inferred from note_count / status
    """
    status        = proposal["status"]
    is_historical = bool(proposal.get("is_historical"))
    notes         = proposal.get("notes") or ""
    note_count    = len([p for p in notes.split("\n---\n") if p.strip()])

    if status == "won":
        if note_count >= 6:
            return ([_S_QUALIFIED, _S_SITE_VISIT, _S_PROP_SENT, _S_NEGOT, _S_WON], "won", None)
        return ([_S_QUALIFIED, _S_SITE_VISIT, _S_PROP_SENT, _S_WON], "won", None)

    if status == "lost":
        if note_count <= 5:
            return ([_S_QUALIFIED, _S_SITE_VISIT, _S_PROP_SENT], "lost", "chose_competitor")
        return ([_S_QUALIFIED, _S_SITE_VISIT, _S_PROP_SENT, _S_NEGOT], "lost", "no_budget")

    if status == "draft":
        if is_historical:
            # Historical draft = one of the 5 proposals that lost at Qualified
            # (SS-PROP-0011–0015; generated in the 2025 lost batch, not the 2026 active pipeline)
            return ([_S_QUALIFIED], "lost", "price_too_high")
        # Active open deals — infer current stage from note count
        if note_count >= 3:
            return ([_S_QUALIFIED, _S_SITE_VISIT], None, None)
        if note_count == 2:
            return ([_S_QUALIFIED], None, None)
        return ([], None, None)  # 1 note — stays at New Lead

    if status == "sent":
        return ([_S_QUALIFIED, _S_SITE_VISIT, _S_PROP_SENT], None, None)

    return ([], None, None)


def _compute_due_date(proposal: dict, stage_index: int, total_stages: int) -> str:
    """Space activity due-dates across the deal's timeline."""
    if proposal.get("decision_date"):
        anchor = date.fromisoformat(proposal["decision_date"])
        days_back = (total_stages - stage_index) * 12
        return (anchor - timedelta(days=days_back)).isoformat()
    if proposal.get("sent_date"):
        anchor = date.fromisoformat(proposal["sent_date"])
        return (anchor + timedelta(days=stage_index * 10)).isoformat()
    # Open deal: space backward from today
    today = date.today()
    days_back = (total_stages - stage_index) * 10
    return (today - timedelta(days=days_back)).isoformat()


# ---------------------------------------------------------------------------
# Phase 1: Persons
# ---------------------------------------------------------------------------

def push_persons(dry_run: bool = False) -> int:
    conn = get_connection(_DB_PATH)
    contacts = _fetch_proposal_contacts(conn)
    conn.close()

    print(f"\n[Phase 1] Persons — {len(contacts)} unique contacts")
    created = 0

    for contact in contacts:
        entity_id = contact["entity_id"]

        if not dry_run and get_tool_id(entity_id, "pipedrive", db_path=_DB_PATH):
            continue  # already pushed

        name = contact["full_name"].strip() or contact.get("company_name") or entity_id

        if dry_run:
            print(f"  [dry-run] Would create person: {name} ({entity_id})")
            created += 1
            continue

        payload: dict = {"name": name}
        if contact.get("email"):
            payload["email"] = [{"value": contact["email"], "primary": True}]
        if contact.get("phone"):
            payload["phone"] = [{"value": contact["phone"], "primary": True}]
        if contact.get("hubspot_contact_id"):
            payload[_PERSON_FIELDS["HubSpot Contact ID"]] = contact["hubspot_contact_id"]
        if contact.get("jobber_client_id"):
            payload[_PERSON_FIELDS["Jobber Client ID"]] = contact["jobber_client_id"]
        if contact.get("acq_source"):
            payload[_PERSON_FIELDS["Acquisition Source"]] = contact["acq_source"]
        if contact.get("neighborhood"):
            payload[_PERSON_FIELDS["Neighborhood"]] = contact["neighborhood"]

        try:
            data = _post("/persons", payload)
            person_id = str(data.get("id", ""))
            if person_id:
                register_mapping(entity_id, "pipedrive", person_id, db_path=_DB_PATH)
                created += 1
                print(f"  Created person: {name} → PD {person_id}")
        except Exception as exc:
            print(f"  [WARN] Person create failed for {entity_id} ({name}): {exc}")

    print(f"[Phase 1] Done — {created} persons created")
    return created


# ---------------------------------------------------------------------------
# Phase 2: Organizations
# ---------------------------------------------------------------------------

def push_organizations(dry_run: bool = False) -> int:
    global _company_to_org_id

    conn = get_connection(_DB_PATH)
    contacts = _fetch_proposal_contacts(conn)
    conn.close()

    # Unique company names → list of entity IDs that belong to each company
    orgs: dict[str, list[str]] = {}
    entity_to_company: dict[str, str] = {}
    for c in contacts:
        company = (c.get("company_name") or "").strip()
        if company:
            orgs.setdefault(company, []).append(c["entity_id"])
            entity_to_company[c["entity_id"]] = company

    print(f"\n[Phase 2] Organizations — {len(orgs)} unique companies")
    created = 0

    for company_name in orgs:
        if dry_run:
            print(f"  [dry-run] Would create org: {company_name}")
            created += 1
            continue

        try:
            data = _post("/organizations", {"name": company_name})
            org_id = data.get("id")
            if org_id:
                _company_to_org_id[company_name] = int(org_id)
                created += 1
                print(f"  Created org: {company_name} → PD {org_id}")
        except Exception as exc:
            print(f"  [WARN] Org create failed for '{company_name}': {exc}")

    # Link persons to their organizations via PUT /persons/{id}
    if not dry_run and _company_to_org_id:
        print(f"  Linking persons to organizations...")
        linked = 0
        for entity_id, company in entity_to_company.items():
            org_id = _company_to_org_id.get(company)
            person_pd_id = get_tool_id(entity_id, "pipedrive", db_path=_DB_PATH)
            if org_id and person_pd_id:
                try:
                    _put(f"/persons/{person_pd_id}", {"org_id": org_id})
                    linked += 1
                except Exception as exc:
                    print(f"  [WARN] Link person {person_pd_id} → org {org_id}: {exc}")
        print(f"  Linked {linked} persons to organizations")

    print(f"[Phase 2] Done — {created} organizations created")
    return created


# ---------------------------------------------------------------------------
# Phase 3 + 4 + 5: Deals, Stage History, Activities
# ---------------------------------------------------------------------------

def push_deals(dry_run: bool = False) -> int:
    conn = get_connection(_DB_PATH)
    proposals = _fetch_proposals(conn)
    conn.close()

    print(f"\n[Phase 3] Deals + Stage History + Activities — {len(proposals)} proposals")
    created = 0

    for prop in proposals:
        prop_id    = prop["id"]
        company    = prop.get("company_name") or prop_id
        scope      = prop.get("service_scope") or "Commercial Cleaning"
        monthly    = prop.get("monthly_value") or 0.0
        annual     = round(monthly * 12, 2)
        lead_src   = prop.get("lead_source") or ""
        contact_id = prop.get("contact_canonical_id")

        title = f"{company} — {scope}"
        patch_stages, final_status, _ = _stage_patches(prop)
        all_stages = [_S_NEW_LEAD] + patch_stages

        if dry_run:
            stage_names = " → ".join(
                next((k for k, v in _STAGES.items() if v == s), str(s))
                for s in all_stages
            )
            print(f"  [dry-run] {prop_id}: '{title[:50]}' | ${annual:,.0f}/yr | "
                  f"{stage_names} | final={final_status or 'open'}")
            created += 1
            continue

        existing_id = get_tool_id(prop_id, "pipedrive", db_path=_DB_PATH)
        if existing_id:
            # Verify the deal actually exists in Pipedrive (guards against stale/fake mappings)
            try:
                check = _get(f"/deals/{existing_id}")
                if check.get("data"):
                    continue  # deal confirmed — skip
            except Exception:
                pass  # 404 or error → fall through and recreate

        # Resolve person + org IDs
        person_pd_id: Optional[int] = None
        org_pd_id: Optional[int] = None
        if contact_id:
            pid_str = get_tool_id(contact_id, "pipedrive", db_path=_DB_PATH)
            if pid_str:
                person_pd_id = int(pid_str)
        org_pd_id = _company_to_org_id.get(company)

        # --- Phase 3: Create deal at New Lead ---
        deal_payload: dict = {
            "title":       title,
            "value":       annual,
            "currency":    "USD",
            "pipeline_id": _PIPELINE_ID,
            "stage_id":    _S_NEW_LEAD,
            _DEAL_FIELDS["Client Type"]:             "commercial",
            _DEAL_FIELDS["Estimated Monthly Value"]: monthly,
        }
        if lead_src:
            deal_payload[_DEAL_FIELDS["Lead Source"]] = lead_src
        if person_pd_id:
            deal_payload["person_id"] = person_pd_id
        if org_pd_id:
            deal_payload["org_id"] = org_pd_id

        try:
            deal_data = _post("/deals", deal_payload)
            deal_id   = deal_data.get("id")
            if not deal_id:
                print(f"  [WARN] No deal ID returned for {prop_id}")
                continue
        except Exception as exc:
            print(f"  [WARN] Deal create failed for {prop_id}: {exc}")
            continue

        register_mapping(prop_id, "pipedrive", str(deal_id), db_path=_DB_PATH)
        created += 1
        print(f"  Created deal {deal_id}: '{title[:50]}' | ${annual:,.0f}/yr")

        # --- Phase 4: Sequential stage patches ---
        for stage_id in patch_stages:
            stage_name = next((k for k, v in _STAGES.items() if v == stage_id), str(stage_id))
            try:
                _put(f"/deals/{deal_id}", {"stage_id": stage_id})
            except Exception as exc:
                print(f"  [WARN] Stage patch deal {deal_id} → {stage_name}: {exc}")

        # --- Phase 5: One activity per stage traversed ---
        total = len(all_stages)
        for idx, stage_id in enumerate(all_stages):
            meta = _STAGE_META.get(stage_id)
            if not meta:
                continue
            act_type, stage_label, outcome = meta
            is_past  = (final_status is not None) or (idx < total - 1)
            due_date = _compute_due_date(prop, idx, total)
            note_type = _ACT_TO_NOTE_TYPE.get(act_type, "call_recap")

            try:
                note_text = generate_pipedrive_activity_note(
                    activity_type=note_type,
                    deal_stage=stage_label,
                    outcome=outcome,
                )
            except Exception:
                note_text = f"{stage_label} activity completed."

            activity_payload: dict = {
                "subject":  f"{company} — {stage_label}",
                "type":     act_type,
                "due_date": due_date,
                "done":     1 if is_past else 0,
                "deal_id":  deal_id,
                "note":     note_text,
            }
            if person_pd_id:
                activity_payload["person_id"] = person_pd_id

            try:
                _post("/activities", activity_payload)
            except Exception as exc:
                print(f"  [WARN] Activity create failed (deal {deal_id}, {stage_label}): {exc}")

    print(f"[Phase 3] Done — {created} deals created")
    return created


# ---------------------------------------------------------------------------
# Phase 6: Mark won / lost
# ---------------------------------------------------------------------------

def finalize_deals(dry_run: bool = False) -> tuple[int, int]:
    conn = get_connection(_DB_PATH)
    proposals = _fetch_proposals(conn)
    conn.close()

    print(f"\n[Phase 6] Finalizing won/lost deals...")
    won_count = lost_count = 0

    for prop in proposals:
        prop_id = prop["id"]
        _, final_status, lost_reason = _stage_patches(prop)

        if final_status is None:
            continue

        if dry_run:
            suffix = f" (reason: {lost_reason})" if lost_reason else ""
            print(f"  [dry-run] Would mark {prop_id} as {final_status}{suffix}")
            won_count  += (final_status == "won")
            lost_count += (final_status == "lost")
            continue

        deal_id = get_tool_id(prop_id, "pipedrive", db_path=_DB_PATH)
        if not deal_id:
            print(f"  [WARN] No Pipedrive deal ID for {prop_id} — skipping")
            continue

        payload: dict = {"status": final_status}
        if final_status == "lost" and lost_reason:
            payload["lost_reason"] = lost_reason

        try:
            _put(f"/deals/{deal_id}", payload)
            if final_status == "won":
                won_count += 1
            else:
                lost_count += 1
        except Exception as exc:
            print(f"  [WARN] Could not mark deal {deal_id} as {final_status}: {exc}")

    print(f"[Phase 6] Done — {won_count} won, {lost_count} lost")
    return won_count, lost_count


# ---------------------------------------------------------------------------
# Phase 7: Verification
# ---------------------------------------------------------------------------

def verify_pipeline() -> None:
    """
    Verify deal counts by fetching status for each of the 48 mapped deal IDs.
    This avoids counting pre-existing deals that may appear in pipeline queries.
    """
    print(f"\n[Phase 7] Verifying pipeline {_PIPELINE_ID}...")

    conn = get_connection(_DB_PATH)
    rows = conn.execute("""
        SELECT ctm.tool_specific_id
        FROM cross_tool_mapping ctm
        JOIN commercial_proposals cp ON cp.id = ctm.canonical_id
        WHERE ctm.tool_name = 'pipedrive'
    """).fetchall()
    conn.close()

    our_deal_ids = [r[0] for r in rows]
    if not our_deal_ids:
        print("  [WARN] No Pipedrive deal mappings found in DB — nothing to verify.")
        return

    won = lost = open_ = errors = 0
    for deal_id in our_deal_ids:
        try:
            body = _get(f"/deals/{deal_id}")
            d = body.get("data") or {}
            status = d.get("status", "unknown")
            if status == "won":
                won += 1
            elif status == "lost":
                lost += 1
            elif status == "open":
                open_ += 1
            else:
                errors += 1
        except Exception as exc:
            print(f"  [WARN] Could not fetch deal {deal_id}: {exc}")
            errors += 1

    total = won + lost + open_
    print(f"  Total deals verified  : {total}  (expected 48)")
    print(f"    won  : {won}   (expected 10)")
    print(f"    lost : {lost}  (expected 23)")
    print(f"    open : {open_}  (expected 15)")
    if errors:
        print(f"    errors/unknown: {errors}")

    if total == 48 and won == 10 and lost == 23 and open_ == 15:
        print("  [OK] All counts match.")
    else:
        print("  [WARN] Count mismatch — review logs for skipped or failed deals.")


# ---------------------------------------------------------------------------
# Gap report
# ---------------------------------------------------------------------------

def print_gap_report() -> None:
    print("\n[Gap Report] Pipedrive mapping coverage:")
    for entity_type, label in [
        ("PROP",   "commercial proposals"),
        ("CLIENT", "commercial clients"),
        ("LEAD",   "leads"),
    ]:
        try:
            unmapped = find_unmapped(entity_type, "pipedrive", db_path=_DB_PATH)
            if unmapped:
                print(f"  [GAP] {len(unmapped)} {label} missing Pipedrive mapping "
                      f"(first 5: {unmapped[:5]})")
            else:
                print(f"  [OK]  All {label} mapped in Pipedrive")
        except Exception as exc:
            print(f"  [WARN] Gap check failed for {label}: {exc}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(dry_run: bool = False) -> None:
    global _api_token, _session

    print("=" * 60)
    print("  Sparkle & Shine → Pipedrive CRM")
    if dry_run:
        print("  MODE: DRY RUN (no data will be written)")
    print("=" * 60)

    get_client("pipedrive")  # validates credentials; raises on failure
    _api_token = get_credential("PIPEDRIVE_API_TOKEN")
    _session   = _build_session()

    push_persons(dry_run=dry_run)
    push_organizations(dry_run=dry_run)
    push_deals(dry_run=dry_run)
    finalize_deals(dry_run=dry_run)

    if not dry_run:
        verify_pipeline()
        print_gap_report()
        stats = PIPEDRIVE.stats()
        print(f"\n  Total Pipedrive API calls: {sum(stats.values())}")

    print("\n[Done] Pipedrive push complete.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Push Sparkle & Shine commercial proposals to Pipedrive CRM"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be pushed without making any API calls",
    )
    args = parser.parse_args()
    main(dry_run=args.dry_run)
