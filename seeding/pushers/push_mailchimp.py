"""
Sync all active clients and unconverted leads to a Mailchimp audience, then
create campaign records for the 5 Sparkle & Shine email campaigns.

Full run:  python seeding/pushers/push_mailchimp.py
Dry run:   python seeding/pushers/push_mailchimp.py --dry-run

Push phases:
  Phase 1 — Members   (active clients + churned clients + leads via /3.0/batches)
  Phase 2 — Poll      (wait for the Mailchimp batch job to finish)
  Phase 3 — Campaigns (5 campaign records from marketing_campaigns table)
  Phase 4 — Verify    (GET /3.0/lists/{id} and print member_count)

Auth is handled by auth.get_client("mailchimp") (mailchimp_marketing SDK).

Subscriber groups:
  Active residential  → subscribed, tags: [residential-client, active, <freq-tag>]
  Churned clients     → unsubscribed, tags: [residential-client, churned]
  Commercial clients  → subscribed, tags: [commercial-client, active]
  Leads               → subscribed, tags: [lead]

Merge fields (must be pre-created in the audience):
  FNAME, LNAME, PHONE, NEIGHBORHOOD, CLIENT_TYPE, SERVICE_TYPE, LEAD_SOURCE

Note: Mailchimp sandbox does not support actual sends.  Campaigns are created
as 'draft' status; open_rate / click_rate cannot be set via the REST API.
"""

import json
import os
import sys
import time
from typing import Optional

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from auth import get_client                                                          # noqa: E402
from credentials import get_credential                                               # noqa: E402
from database.schema import get_connection                                           # noqa: E402
from database.mappings import register_mapping, get_tool_id                         # noqa: E402
from seeding.utils.throttler import MAILCHIMP                                        # noqa: E402

_DB_PATH = os.path.join(_PROJECT_ROOT, "sparkle_shine.db")

_AUDIENCE_ID: str = ""   # populated in main() from MAILCHIMP_LIST_ID env var

_BATCH_POLL_INTERVAL = 5   # seconds between batch status polls
_BATCH_POLL_TIMEOUT  = 600  # seconds before giving up on a batch

_FREQ_TAG = {
    "weekly":   "recurring-weekly",
    "biweekly": "recurring-biweekly",
    "monthly":  "recurring-monthly",
}

_FROM_NAME  = "Sparkle & Shine"
_REPLY_TO   = "hello@sparkleshineaustin.com"


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def _fetch_active_residential(conn) -> list:
    """Active (non-churned) residential clients with recurring frequency."""
    rows = conn.execute("""
        SELECT
            c.id, c.first_name, c.last_name, c.email, c.phone,
            c.neighborhood, c.acquisition_source, c.client_type,
            ra.frequency
        FROM clients c
        LEFT JOIN (
            SELECT client_id, frequency FROM recurring_agreements
            WHERE status = 'active'
            GROUP BY client_id
        ) ra ON ra.client_id = c.id
        WHERE c.client_type = 'residential'
          AND c.status IN ('active', 'occasional')
        ORDER BY c.id
    """).fetchall()
    return [dict(r) for r in rows]


def _fetch_churned_clients(conn) -> list:
    """Churned residential and commercial clients."""
    rows = conn.execute("""
        SELECT
            c.id, c.first_name, c.last_name, c.email, c.phone,
            c.neighborhood, c.acquisition_source, c.client_type
        FROM clients c
        WHERE c.status = 'churned'
        ORDER BY c.id
    """).fetchall()
    return [dict(r) for r in rows]


def _fetch_commercial_clients(conn) -> list:
    """Active commercial clients."""
    rows = conn.execute("""
        SELECT
            c.id, c.first_name, c.last_name, c.company_name,
            c.email, c.phone, c.neighborhood, c.acquisition_source, c.client_type
        FROM clients c
        WHERE c.client_type = 'commercial'
          AND c.status IN ('active', 'occasional')
        ORDER BY c.id
    """).fetchall()
    return [dict(r) for r in rows]


def _fetch_leads(conn) -> list:
    """All unconverted leads (new, contacted, qualified — not lost)."""
    rows = conn.execute("""
        SELECT
            id, first_name, last_name, email, phone, lead_type, source
        FROM leads
        WHERE status != 'lost'
        ORDER BY id
    """).fetchall()
    return [dict(r) for r in rows]


def _fetch_campaigns(conn) -> list:
    """All Mailchimp campaigns from the marketing_campaigns table."""
    rows = conn.execute("""
        SELECT id, name, campaign_type, subject_line, send_date,
               recipient_count, open_rate, click_rate
        FROM marketing_campaigns
        WHERE platform = 'mailchimp'
        ORDER BY send_date ASC
    """).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Member payload builders
# ---------------------------------------------------------------------------

def _residential_member(c: dict) -> dict:
    """Build a Mailchimp member dict for an active residential client."""
    freq = c.get("frequency")
    tags = ["residential-client", "active"]
    if freq and freq in _FREQ_TAG:
        tags.append(_FREQ_TAG[freq])

    return {
        "email_address": c["email"],
        "status":        "subscribed",
        "merge_fields": {
            "FNAME":         c.get("first_name") or "",
            "LNAME":         c.get("last_name") or "",
            "PHONE":         c.get("phone") or "",
            "NEIGHBORHOOD":  c.get("neighborhood") or "",
            "CLIENT_TYPE":   c.get("client_type") or "residential",
            "SERVICE_TYPE":  _FREQ_TAG.get(freq or "", "one-time"),
            "LEAD_SOURCE":   c.get("acquisition_source") or "",
        },
        "tags": tags,
    }


def _churned_member(c: dict) -> dict:
    """Build a Mailchimp member dict for a churned client (unsubscribed)."""
    return {
        "email_address": c["email"],
        "status":        "unsubscribed",
        "merge_fields": {
            "FNAME":         c.get("first_name") or "",
            "LNAME":         c.get("last_name") or "",
            "PHONE":         c.get("phone") or "",
            "NEIGHBORHOOD":  c.get("neighborhood") or "",
            "CLIENT_TYPE":   c.get("client_type") or "residential",
            "SERVICE_TYPE":  "",
            "LEAD_SOURCE":   c.get("acquisition_source") or "",
        },
        "tags": ["residential-client", "churned"],
    }


def _commercial_member(c: dict) -> dict:
    """Build a Mailchimp member dict for an active commercial client."""
    return {
        "email_address": c["email"],
        "status":        "subscribed",
        "merge_fields": {
            "FNAME":         c.get("first_name") or "",
            "LNAME":         c.get("last_name") or (c.get("company_name") or ""),
            "PHONE":         c.get("phone") or "",
            "NEIGHBORHOOD":  c.get("neighborhood") or "",
            "CLIENT_TYPE":   "commercial",
            "SERVICE_TYPE":  "commercial-nightly",
            "LEAD_SOURCE":   c.get("acquisition_source") or "",
        },
        "tags": ["commercial-client", "active"],
    }


def _lead_member(lead: dict) -> dict:
    """Build a Mailchimp member dict for an unconverted lead."""
    return {
        "email_address": lead["email"],
        "status":        "subscribed",
        "merge_fields": {
            "FNAME":         lead.get("first_name") or "",
            "LNAME":         lead.get("last_name") or "",
            "PHONE":         lead.get("phone") or "",
            "NEIGHBORHOOD":  "",
            "CLIENT_TYPE":   "lead",
            "SERVICE_TYPE":  "",
            "LEAD_SOURCE":   lead.get("source") or "",
        },
        "tags": ["lead"],
    }


# ---------------------------------------------------------------------------
# Phase 1: Members via Mailchimp batches endpoint
# ---------------------------------------------------------------------------

def push_members(mc, dry_run: bool = False) -> Optional[str]:
    """
    Build one large Mailchimp batch covering all subscriber groups and submit it.
    Returns the Mailchimp batch ID (or None on dry run / failure).
    """
    conn = get_connection(_DB_PATH)
    active_res  = _fetch_active_residential(conn)
    churned     = _fetch_churned_clients(conn)
    commercial  = _fetch_commercial_clients(conn)
    leads       = _fetch_leads(conn)
    conn.close()

    print(
        f"\n[Phase 1] Members — "
        f"{len(active_res)} active residential, "
        f"{len(churned)} churned, "
        f"{len(commercial)} commercial, "
        f"{len(leads)} leads"
    )

    operations = []

    def _add_op(member_body: dict) -> None:
        if not member_body.get("email_address"):
            return
        operations.append({
            "method": "POST",
            "path":   f"/lists/{_AUDIENCE_ID}/members",
            "body":   json.dumps(member_body),
        })

    for c in active_res:
        _add_op(_residential_member(c))

    for c in churned:
        _add_op(_churned_member(c))

    for c in commercial:
        _add_op(_commercial_member(c))

    for lead in leads:
        _add_op(_lead_member(lead))

    total_ops = len(operations)
    print(f"  Total batch operations: {total_ops}")

    if dry_run:
        print(f"  [dry-run] Would submit {total_ops} operations to Mailchimp batches endpoint")
        return None

    if not operations:
        print("  [WARN] No operations to submit — skipping batch")
        return None

    MAILCHIMP.wait()
    try:
        result = mc.batches.start({"operations": operations})
    except Exception as exc:
        print(f"  [ERROR] Failed to submit Mailchimp batch: {exc}")
        return None

    batch_id = result.get("id")
    print(f"  Batch submitted — ID: {batch_id}")
    return batch_id


# ---------------------------------------------------------------------------
# Phase 2: Poll for batch completion
# ---------------------------------------------------------------------------

def poll_batch(mc, batch_id: str) -> bool:
    """
    Poll the batch status endpoint until the batch is finished or the timeout
    expires.  Returns True if the batch finished successfully.
    """
    print(f"\n[Phase 2] Polling batch {batch_id} (timeout: {_BATCH_POLL_TIMEOUT}s)...")
    deadline = time.monotonic() + _BATCH_POLL_TIMEOUT

    while time.monotonic() < deadline:
        time.sleep(_BATCH_POLL_INTERVAL)
        MAILCHIMP.wait()
        try:
            status = mc.batches.status(batch_id)
        except Exception as exc:
            print(f"  [WARN] Poll error: {exc}")
            continue

        phase = status.get("status", "unknown")
        total = status.get("total_operations", 0)
        finished = status.get("finished_operations", 0)
        errored = status.get("errored_operations", 0)
        print(f"  {phase}: {finished}/{total} done, {errored} errors")

        if phase == "finished":
            if errored:
                print(
                    f"  [WARN] Batch completed with {errored} errors "
                    f"(often duplicate addresses — not fatal)."
                )
            return True

    print(f"  [WARN] Batch did not finish within {_BATCH_POLL_TIMEOUT}s")
    return False


# ---------------------------------------------------------------------------
# Phase 3: Campaigns
# ---------------------------------------------------------------------------

def push_campaigns(mc, dry_run: bool = False) -> int:
    """
    Create 5 Mailchimp campaign records from the marketing_campaigns table.
    Returns the number of campaigns created.

    Note: Mailchimp sandbox does not support actual sends.  Campaigns are
    created in 'draft' status.  The open_rate / click_rate stats recorded in
    the local DB cannot be replicated via the REST API.
    """
    conn = get_connection(_DB_PATH)
    campaigns = _fetch_campaigns(conn)
    conn.close()

    print(f"\n[Phase 3] Campaigns — {len(campaigns)} records")
    created = 0

    for camp in campaigns:
        canonical_id = camp["id"]

        # Idempotency: skip if already mapped
        if not dry_run and get_tool_id(canonical_id, "mailchimp", db_path=_DB_PATH):
            print(f"  [SKIP] {canonical_id} already mapped")
            continue

        if dry_run:
            print(f"  [dry-run] Would create campaign: {camp['name']!r}")
            created += 1
            continue

        body = {
            "type": "regular",
            "recipients": {
                "list_id": _AUDIENCE_ID,
            },
            "settings": {
                "subject_line": camp.get("subject_line") or camp["name"],
                "from_name":    _FROM_NAME,
                "reply_to":     _REPLY_TO,
                "title":        camp["name"],
            },
        }

        MAILCHIMP.wait()
        try:
            result = mc.campaigns.create(body)
        except Exception as exc:
            print(f"  [WARN] Campaign create failed for {camp['name']!r}: {exc}")
            continue

        mc_campaign_id = result.get("id")
        if not mc_campaign_id:
            print(f"  [WARN] No campaign ID returned for {camp['name']!r}")
            continue

        try:
            register_mapping(canonical_id, "mailchimp", mc_campaign_id, db_path=_DB_PATH)
        except Exception as exc:
            print(f"  [WARN] Mapping registration failed for {canonical_id}: {exc}")

        created += 1
        print(
            f"  Created {canonical_id} → MC {mc_campaign_id!r}: {camp['name']!r} "
            f"(send_date: {camp.get('send_date', 'N/A')}, "
            f"open_rate: {camp.get('open_rate', 0):.1f}%, "
            f"click_rate: {camp.get('click_rate', 0):.1f}% — stats are local only)"
        )

    print(f"[Phase 3] Done — {created} campaigns created")
    return created


# ---------------------------------------------------------------------------
# Phase 4: Verify audience count
# ---------------------------------------------------------------------------

def verify_audience(mc) -> None:
    """GET /3.0/lists/{id} and print the member_count."""
    print(f"\n[Phase 4] Verifying audience count for list {_AUDIENCE_ID}...")
    MAILCHIMP.wait()
    try:
        info = mc.lists.get_list(_AUDIENCE_ID)
        stats = info.get("stats", {})
        member_count = stats.get("member_count", "N/A")
        total_count  = info.get("stats", {}).get("member_count", 0)
        print(f"  member_count : {member_count}")
        print(f"  (target: 300–350; actual: {total_count})")
        if isinstance(total_count, int):
            if 300 <= total_count <= 350:
                print("  [OK] Member count is within expected range.")
            else:
                print(
                    f"  [WARN] Member count {total_count} is outside the expected 300–350 range.  "
                    f"Check for duplicate emails or missing records."
                )
    except Exception as exc:
        print(f"  [WARN] Could not fetch audience stats: {exc}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(dry_run: bool = False) -> None:
    global _AUDIENCE_ID

    print("=" * 60)
    print("  Sparkle & Shine → Mailchimp")
    if dry_run:
        print("  MODE: DRY RUN (no data will be written)")
    print("=" * 60)

    _AUDIENCE_ID = get_credential("MAILCHIMP_LIST_ID")

    # Validate auth and get the SDK client
    mc = get_client("mailchimp")

    batch_id = push_members(mc, dry_run=dry_run)

    if not dry_run and batch_id:
        finished = poll_batch(mc, batch_id)
        if not finished:
            print("[WARN] Continuing to campaign creation despite batch timeout.")

    push_campaigns(mc, dry_run=dry_run)

    if not dry_run:
        verify_audience(mc)

    print("\n[Done] Mailchimp push complete.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Sync Sparkle & Shine clients and leads to Mailchimp"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be pushed without making any API calls",
    )
    args = parser.parse_args()
    main(dry_run=args.dry_run)
