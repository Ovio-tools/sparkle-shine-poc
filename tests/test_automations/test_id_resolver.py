"""
tests/test_automations/test_id_resolver.py

Cross-tool ID resolution — specifically the entity-type preference when one
tool-specific ID is mapped by multiple canonical records (a lead later
promoted to a client). Regression test for the SS-LEAD-0335 / SS-CLIENT-0519
dual mapping that stranded SS-JOB-5981's invoice.
"""
import pytest

from automations.utils.id_resolver import (
    MappingNotFoundError,
    register_mapping,
    resolve,
    reverse_resolve,
)


def _seed_dual_mapping(db, tool_id="jc-dual-1"):
    """One Jobber client ID mapped by both a stale LEAD and the real CLIENT."""
    with db:
        for cid, etype in [("SS-LEAD-0900", "LEAD"), ("SS-CLIENT-0900", "CLIENT")]:
            db.execute(
                "INSERT INTO cross_tool_mapping "
                "(canonical_id, entity_type, tool_name, tool_specific_id) "
                "VALUES (%s, %s, 'jobber', %s) ON CONFLICT DO NOTHING",
                (cid, etype, tool_id),
            )
    return tool_id


def test_reverse_resolve_prefers_client_over_lead(mock_db):
    tool_id = _seed_dual_mapping(mock_db)
    assert reverse_resolve(mock_db, tool_id, "jobber") == "SS-CLIENT-0900"


def test_reverse_resolve_explicit_entity_type_still_finds_lead(mock_db):
    tool_id = _seed_dual_mapping(mock_db)
    assert (
        reverse_resolve(mock_db, tool_id, "jobber", entity_type="LEAD")
        == "SS-LEAD-0900"
    )


def test_reverse_resolve_missing_still_raises(mock_db):
    with pytest.raises(MappingNotFoundError):
        reverse_resolve(mock_db, "nope-does-not-exist", "jobber")


def test_reverse_resolve_single_mapping_unchanged(mock_db):
    # SS-JOB-0001 → jobber 601 is seeded by conftest
    assert reverse_resolve(mock_db, "601", "jobber") == "SS-JOB-0001"
