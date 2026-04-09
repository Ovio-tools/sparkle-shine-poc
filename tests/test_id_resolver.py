import pytest

from automations.utils.id_resolver import register_mapping


class _FakeCursor:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _FakeDB:
    def __init__(self, existing_row=None):
        self._existing_row = existing_row
        self.calls = []

    def execute(self, sql, params):
        self.calls.append((sql, params))
        if "SELECT canonical_id FROM cross_tool_mapping" in sql:
            return _FakeCursor(self._existing_row)
        return _FakeCursor(None)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_register_mapping_rejects_non_ss_canonical_ids_before_db_write():
    fake_db = _FakeDB()

    with pytest.raises(ValueError, match="known SS-TYPE"):
        register_mapping(fake_db, "INVOICE:JOB:SS-JOB-0001", "quickbooks", "8734")

    assert fake_db.calls == []


def test_register_mapping_uses_entity_type_from_ss_prefix():
    fake_db = _FakeDB()

    register_mapping(fake_db, "SS-INV-FAKE-01", "quickbooks", "8734")

    assert fake_db.calls[0][1] == ("quickbooks", "8734")
    assert fake_db.calls[1][1] == ("SS-INV-FAKE-01", "INV", "quickbooks", "8734")
