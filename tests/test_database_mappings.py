from database.mappings import get_canonical_id


class _FakeCursor:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _FakeConn:
    def __init__(self, row):
        self._row = row
        self.sql = None
        self.params = None
        self.closed = False

    def execute(self, sql, params):
        self.sql = sql
        self.params = params
        return _FakeCursor(self._row)

    def close(self):
        self.closed = True


def test_get_canonical_id_filters_by_entity_type(monkeypatch):
    fake_conn = _FakeConn({"canonical_id": "SS-CLIENT-0001"})
    monkeypatch.setattr("database.mappings.get_connection", lambda db_path="sparkle_shine.db": fake_conn)

    result = get_canonical_id("quickbooks", "123", entity_type="client")

    assert result == "SS-CLIENT-0001"
    assert "entity_type = %s" in fake_conn.sql
    assert fake_conn.params == ("quickbooks", "123", "CLIENT")
    assert fake_conn.closed is True


def test_get_canonical_id_without_entity_type_keeps_legacy_lookup(monkeypatch):
    fake_conn = _FakeConn({"canonical_id": "SS-INV-0009"})
    monkeypatch.setattr("database.mappings.get_connection", lambda db_path="sparkle_shine.db": fake_conn)

    result = get_canonical_id("quickbooks", "456")

    assert result == "SS-INV-0009"
    assert "entity_type = %s" not in fake_conn.sql
    assert fake_conn.params == ("quickbooks", "456")
    assert fake_conn.closed is True
