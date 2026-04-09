from unittest.mock import MagicMock

from automations.new_client_onboarding import NewClientOnboarding


class _FakeCursor:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _FakeDB:
    def __init__(self, proposal_row):
        self._proposal_row = proposal_row
        self.calls = []

    def execute(self, sql, params):
        self.calls.append((sql, params))
        if "FROM commercial_proposals" in sql:
            return _FakeCursor(self._proposal_row)
        raise AssertionError(f"Unexpected SQL: {sql}")


def test_get_or_create_canonical_id_uses_client_linked_to_existing_proposal():
    db = _FakeDB({"client_id": "SS-CLIENT-0042", "lead_id": None})
    auto = NewClientOnboarding(clients=MagicMock(), db=db, dry_run=False)
    auto.reverse_resolve_id = MagicMock(return_value="SS-PROP-0001")

    canonical_id = auto._get_or_create_canonical_id(
        deal_id="deal-123",
        email="client@example.com",
        first_name="Casey",
        last_name="Jones",
        client_type="commercial",
    )

    assert canonical_id == "SS-CLIENT-0042"


def test_get_or_create_canonical_id_promotes_lead_linked_to_existing_proposal():
    db = _FakeDB({"client_id": None, "lead_id": "SS-LEAD-0042"})
    auto = NewClientOnboarding(clients=MagicMock(), db=db, dry_run=False)
    auto.reverse_resolve_id = MagicMock(return_value="SS-PROP-0001")
    auto._promote_lead_to_client = MagicMock(return_value="SS-CLIENT-0099")

    canonical_id = auto._get_or_create_canonical_id(
        deal_id="deal-123",
        email="client@example.com",
        first_name="Casey",
        last_name="Jones",
        client_type="commercial",
    )

    assert canonical_id == "SS-CLIENT-0099"
    auto._promote_lead_to_client.assert_called_once_with(
        lead_id="SS-LEAD-0042",
        deal_id="deal-123",
        first_name="Casey",
        last_name="Jones",
        email="client@example.com",
        client_type="commercial",
    )
