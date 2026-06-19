"""
tests/test_automations/test_hubspot_contact_pruner.py

Unit tests for HubSpotContactPruner — the daily job that archives oldest
safe-pool contacts from HubSpot to stay under the free-tier 1,000 cap.

All HubSpot calls are mocked (no live API). Uses the shared mock_db /
mock_clients fixtures from conftest.py. Live-API behaviour is not exercised
here; integration coverage would be gated behind RUN_INTEGRATION.
"""
import os
import sys

import pytest

_PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from automations.hubspot_contact_pruner import HubSpotContactPruner


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

class _FakeApiException(Exception):
    """Stand-in for hubspot.crm.contacts.exceptions.ApiException."""

    def __init__(self, status: int):
        super().__init__(f"ApiException status={status}")
        self.status = status


def _set_live_count(mock_clients, n: int) -> None:
    """Make the HubSpot search API report a live contact total of n."""
    mock_clients.hubspot.crm.contacts.search_api.do_search.return_value.total = n


def _archive_calls(mock_clients) -> list:
    """Return the ordered list of HubSpot IDs passed to basic_api.archive."""
    return [
        call.args[0]
        for call in mock_clients.hubspot.crm.contacts.basic_api.archive.call_args_list
    ]


def _seed_pool(db) -> None:
    """Seed a known safe pool plus protected contacts, all with HubSpot mappings.

    Eligible (oldest-first by created_at):
        SS-CLIENT-0010  churned   2025-01-01  hs=1010
        SS-LEAD-0010    lost      2025-01-15  hs=2010
        SS-CLIENT-0011  churned   2025-02-01  hs=1011
        SS-LEAD-0011    new/NULL  2025-02-15  hs=2011
        SS-CLIENT-0012  churned   2025-03-01  hs=1012

    Protected (must NEVER be selected, even though some are older):
        SS-CLIENT-0013  churned + active recurring agreement  2024-01-01  hs=1013
        SS-CLIENT-0014  active                                2024-06-01  hs=1014
        SS-LEAD-0012    contacted                             2024-02-01  hs=2012
        SS-LEAD-0013    qualified                             2024-03-01  hs=2013
        SS-CLIENT-0015  churned but NO HubSpot mapping        2023-01-01  (none)
    """
    clients = [
        # id, type, email, status, created_at
        ("SS-CLIENT-0010", "residential", "c10@ex.com", "churned", "2025-01-01 00:00:00"),
        ("SS-CLIENT-0011", "residential", "c11@ex.com", "churned", "2025-02-01 00:00:00"),
        ("SS-CLIENT-0012", "residential", "c12@ex.com", "churned", "2025-03-01 00:00:00"),
        ("SS-CLIENT-0013", "residential", "c13@ex.com", "churned", "2024-01-01 00:00:00"),
        ("SS-CLIENT-0014", "residential", "c14@ex.com", "active",  "2024-06-01 00:00:00"),
        ("SS-CLIENT-0015", "residential", "c15@ex.com", "churned", "2023-01-01 00:00:00"),
    ]
    with db:
        for cid, ctype, email, status, created in clients:
            db.execute(
                """
                INSERT INTO clients (id, client_type, email, status, created_at)
                VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
                """,
                (cid, ctype, email, status, created),
            )

    leads = [
        # id, type, email, status, created_at, last_activity_at
        ("SS-LEAD-0010", "residential", "l10@ex.com", "lost",      "2025-01-15 00:00:00", None),
        ("SS-LEAD-0011", "residential", "l11@ex.com", "new",       "2025-02-15 00:00:00", None),
        ("SS-LEAD-0012", "residential", "l12@ex.com", "contacted", "2024-02-01 00:00:00", "2024-02-02 00:00:00"),
        ("SS-LEAD-0013", "residential", "l13@ex.com", "qualified", "2024-03-01 00:00:00", "2024-03-02 00:00:00"),
    ]
    with db:
        for lid, ltype, email, status, created, last_act in leads:
            db.execute(
                """
                INSERT INTO leads (id, lead_type, email, status, created_at, last_activity_at)
                VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
                """,
                (lid, ltype, email, status, created, last_act),
            )

    # Active recurring agreement protecting the otherwise-churned SS-CLIENT-0013
    with db:
        db.execute(
            """
            INSERT INTO recurring_agreements
                (id, client_id, service_type_id, frequency, price_per_visit, start_date, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
            """,
            ("SS-RECUR-0010", "SS-CLIENT-0013", "std-residential", "biweekly", 150.0,
             "2024-01-01", "active"),
        )

    mappings = [
        ("SS-CLIENT-0010", "CLIENT", "1010"),
        ("SS-CLIENT-0011", "CLIENT", "1011"),
        ("SS-CLIENT-0012", "CLIENT", "1012"),
        ("SS-CLIENT-0013", "CLIENT", "1013"),
        ("SS-CLIENT-0014", "CLIENT", "1014"),
        # SS-CLIENT-0015 intentionally has NO hubspot mapping
        ("SS-LEAD-0010", "LEAD", "2010"),
        ("SS-LEAD-0011", "LEAD", "2011"),
        ("SS-LEAD-0012", "LEAD", "2012"),
        ("SS-LEAD-0013", "LEAD", "2013"),
    ]
    with db:
        for cid, etype, tid in mappings:
            db.execute(
                """
                INSERT INTO cross_tool_mapping (canonical_id, entity_type, tool_name, tool_specific_id)
                VALUES (%s, %s, 'hubspot', %s) ON CONFLICT DO NOTHING
                """,
                (cid, etype, tid),
            )


def _mapping_exists(db, canonical_id: str) -> bool:
    row = db.execute(
        "SELECT 1 FROM cross_tool_mapping WHERE canonical_id = %s AND tool_name = 'hubspot'",
        (canonical_id,),
    ).fetchone()
    return row is not None


def _log_rows(db, action_name: str) -> list:
    return db.execute(
        "SELECT status, trigger_detail FROM automation_log "
        "WHERE automation_name = 'HubSpotContactPruner' AND action_name = %s",
        (action_name,),
    ).fetchall()


# ──────────────────────────────────────────────────────────────────────────────
# Tests
# ──────────────────────────────────────────────────────────────────────────────

def test_below_threshold_is_noop(mock_db, mock_clients):
    _seed_pool(mock_db)
    _set_live_count(mock_clients, 900)  # == threshold, not above

    HubSpotContactPruner(mock_clients, mock_db, dry_run=False).run()

    assert _archive_calls(mock_clients) == []
    noop = _log_rows(mock_db, "check_contact_count")
    assert any(r["status"] == "skipped" for r in noop)


def test_deletes_oldest_safe_first(mock_db, mock_clients):
    _seed_pool(mock_db)
    # Any live_total above PRUNE_THRESHOLD (900) yields to_delete = 10 (since the
    # 900→850 gap exceeds MAX_DELETIONS_PER_RUN), so the 5-contact pool drains in
    # strict oldest-first order. Protected contacts are never present.
    _set_live_count(mock_clients, 905)

    HubSpotContactPruner(mock_clients, mock_db, dry_run=False).run()

    # All five safe candidates, in chronological (oldest-first) order:
    # SS-CLIENT-0010 (1010) 2025-01-01, SS-LEAD-0010 (2010) 2025-01-15,
    # SS-CLIENT-0011 (1011) 2025-02-01, SS-LEAD-0011 (2011) 2025-02-15,
    # SS-CLIENT-0012 (1012) 2025-03-01
    assert _archive_calls(mock_clients) == ["1010", "2010", "1011", "2011", "1012"]

    # Their hubspot mappings are gone; canonical rows remain.
    assert not _mapping_exists(mock_db, "SS-CLIENT-0010")
    assert not _mapping_exists(mock_db, "SS-LEAD-0010")
    client = mock_db.execute(
        "SELECT id FROM clients WHERE id = 'SS-CLIENT-0010'"
    ).fetchone()
    assert client is not None


def test_protected_contacts_never_selected(mock_db, mock_clients):
    _seed_pool(mock_db)
    _set_live_count(mock_clients, 920)  # to_delete = 10, pool = 5 → drains pool

    HubSpotContactPruner(mock_clients, mock_db, dry_run=False).run()

    archived = set(_archive_calls(mock_clients))
    # Protected HubSpot IDs must never be archived:
    #   1013 (churned + active agreement), 1014 (active),
    #   2012 (contacted lead), 2013 (qualified lead)
    assert archived.isdisjoint({"1013", "1014", "2012", "2013"})
    # Their mappings are all intact.
    for cid in ("SS-CLIENT-0013", "SS-CLIENT-0014", "SS-LEAD-0012", "SS-LEAD-0013"):
        assert _mapping_exists(mock_db, cid)


def test_safe_pool_exhausted_alerts(mock_db, mock_clients):
    _seed_pool(mock_db)
    _set_live_count(mock_clients, 920)  # needs 10, pool only has 5

    HubSpotContactPruner(mock_clients, mock_db, dry_run=False).run()

    # All 5 eligible archived, none of the protected.
    assert len(_archive_calls(mock_clients)) == 5
    # Slack alert posted to #operations.
    assert mock_clients.slack.chat_postMessage.called
    # safe_pool_exhausted logged as a warning.
    rows = _log_rows(mock_db, "safe_pool_exhausted")
    assert rows and rows[0]["status"] == "skipped"


def test_archive_404_treated_as_success(mock_db, mock_clients):
    _seed_pool(mock_db)
    _set_live_count(mock_clients, 905)

    def _archive(contact_id):
        if contact_id == "1010":
            raise _FakeApiException(404)  # already gone from HubSpot
        return None

    mock_clients.hubspot.crm.contacts.basic_api.archive.side_effect = _archive

    HubSpotContactPruner(mock_clients, mock_db, dry_run=False).run()

    # 404 is treated as success → its dangling mapping is still cleaned up.
    assert not _mapping_exists(mock_db, "SS-CLIENT-0010")
    assert not _mapping_exists(mock_db, "SS-LEAD-0010")


def test_non_404_failure_keeps_mapping(mock_db, mock_clients):
    _seed_pool(mock_db)
    _set_live_count(mock_clients, 905)

    def _archive(contact_id):
        if contact_id == "1010":
            raise _FakeApiException(500)  # transient server error
        return None

    mock_clients.hubspot.crm.contacts.basic_api.archive.side_effect = _archive

    HubSpotContactPruner(mock_clients, mock_db, dry_run=False).run()

    # Failed archive must NOT orphan the mapping — it stays for tomorrow's retry.
    assert _mapping_exists(mock_db, "SS-CLIENT-0010")
    # The other two still succeed.
    assert not _mapping_exists(mock_db, "SS-LEAD-0010")


def test_dry_run_makes_no_writes(mock_db, mock_clients):
    _seed_pool(mock_db)
    _set_live_count(mock_clients, 920)

    HubSpotContactPruner(mock_clients, mock_db, dry_run=True).run()

    # No archive calls, no Slack post, all mappings intact.
    assert _archive_calls(mock_clients) == []
    assert not mock_clients.slack.chat_postMessage.called
    for cid in ("SS-CLIENT-0010", "SS-LEAD-0010", "SS-CLIENT-0011"):
        assert _mapping_exists(mock_db, cid)
