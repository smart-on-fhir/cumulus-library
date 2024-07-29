"""unit tests for completion tracking support in the core study"""

from collections.abc import Iterable

import duckdb
import pytest

from tests import testbed_utils


def _table_ids(con: duckdb.DuckDBPyConnection, table: str) -> set[str]:
    return {e[0] for e in con.sql(f"SELECT id FROM {table}").fetchall()}


def _assert_encounter_ids(
    testbed: testbed_utils.LocalTestbed, expected_complete_ids: Iterable[str] | None
) -> None:
    con = testbed.build()

    expected_complete_ids = set(expected_complete_ids or [])
    complete_ids = _table_ids(con, "core__encounter")
    assert complete_ids == expected_complete_ids

    all_ids = testbed.ids.get("encounter", set())
    expected_incomplete_ids = all_ids - complete_ids
    incomplete_ids = _table_ids(con, "core__incomplete_encounter")
    assert incomplete_ids == expected_incomplete_ids


@pytest.mark.parametrize(
    "missing,enc_is_valid",
    [
        (None, True),
        ({"condition"}, False),
    ],
)
def test_completion_missing_resource(tmp_path, missing, enc_is_valid):
    testbed = testbed_utils.LocalTestbed(tmp_path)

    testbed.add_encounter("A")
    testbed.add_etl_completion(group="G", exclude=missing, time="2020")
    testbed.add_etl_completion_encounters(group="G", ids=["A"], time="2020")

    _assert_encounter_ids(testbed, {"A"} if enc_is_valid else None)


@pytest.mark.parametrize(
    "with_completion_schemas",
    [False, True],
)
def test_completion_loose_encounter(tmp_path, with_completion_schemas):
    """Verify we gracefully handle encounter rows without completion tracking."""
    testbed = testbed_utils.LocalTestbed(tmp_path)

    testbed.add_encounter("A")
    testbed.add_encounter("B")  # is "loose" (i.e. not completion-tracked)

    if with_completion_schemas:  # register schema, but not the B encounter
        testbed.add_etl_completion(group="Other", time="2020")
        testbed.add_etl_completion_encounters(group="G", ids=["A"], time="2020")
        _assert_encounter_ids(testbed, {"B"})
        testbed.add_etl_completion(group="G", time="2020")

    _assert_encounter_ids(testbed, {"A", "B"})


@pytest.mark.parametrize(
    "cond_time,other_time,enc_is_valid",
    [
        ("2023", "2022", True),
        ("2022", "2022", True),
        # with old conditions - we haven't yet loaded them for the encounter!
        ("2021", "2022", False),
        # confirm we parse fractional seconds, here with an old timestamp
        ("2015-02-07T13:28:17.238Z", "2015-02-07T13:28:17.239Z", False),
        # here with the same instant but in different timezones
        ("2015-02-07T11:28:17.239-02:00", "2015-02-07T13:28:17.239Z", True),
    ],
)
def test_completion_time_gaps(tmp_path, cond_time, other_time, enc_is_valid):
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add_encounter("A")
    testbed.add_etl_completion_encounters(group="G", ids=["A"], time=other_time)

    # Most things are loaded at `other_time`
    testbed.add_etl_completion(group="G", exclude={"condition"}, time=other_time)
    # While condition gets its own time
    testbed.add_etl_completion(group="G", include={"condition"}, time=cond_time)

    _assert_encounter_ids(testbed, {"A"} if enc_is_valid else None)


@pytest.mark.parametrize(
    "group_name,enc_is_valid",
    [
        ("G", True),
        ("Other", False),
    ],
)
def test_completion_no_completions_recorded(tmp_path, group_name, enc_is_valid):
    """Verify that we ignore encounters without completion group data"""
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add_encounter("A")
    testbed.add_etl_completion_encounters(group="G", ids=["A"], time="2020")
    testbed.add_etl_completion(group=group_name, time="2020")

    _assert_encounter_ids(testbed, {"A"} if enc_is_valid else None)


def test_completion_multiple_groups(tmp_path):
    """
    Verify that multiple groups being defined for the same encounter doesn't confuse us

    An encounter is included if each table we care about is loaded,
    at a later timestamp.
    """
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add_encounter("Complete")
    testbed.add_encounter("Incomplete")  # early observation load (G1)

    testbed.add_etl_completion_encounters(group="G1", ids=["Incomplete"], time="2021")
    testbed.add_etl_completion_encounters(group="G2", ids=["Complete", "Incomplete"], time="2021")
    testbed.add_etl_completion_encounters(group="G3", ids=["Complete"], time="2021")
    testbed.add_etl_completion_encounters(
        group="Unreferenced", ids=["Complete", "Incomplete"], time="2021"
    )

    testbed.add_etl_completion(group="G1", include={"observation"}, time="2020")
    _assert_encounter_ids(testbed, set())  # only old observations are loaded so far

    testbed.add_etl_completion(group="G2", exclude={"observation"}, time="2021")
    _assert_encounter_ids(testbed, set())  # no enc has valid observations yet

    testbed.add_etl_completion(group="G3", include={"observation"}, time="2021")
    _assert_encounter_ids(testbed, {"Complete"})


def test_completion_new_encounter(tmp_path):
    """Verify that when a new encounter appears in a group, there's no race condition"""
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add_encounter("L")  # starts loose, but then becomes tracked

    # Initial group load, with just the A encounter
    testbed.add_etl_completion_encounters(group="G", ids=["A"], time="2020")
    testbed.add_encounter("A")
    testbed.add_etl_completion(group="G", time="2020")
    _assert_encounter_ids(testbed, {"A", "L"})

    # While loading re-exported encounters, a new B encounter appears.
    # Add L too, even though it was uploaded earlier - now it's tracked.
    testbed.add_etl_completion_encounters(group="G", ids=["B", "L"], time="2021")
    testbed.add_encounter("B")
    _assert_encounter_ids(testbed, {"A"})

    # And update the export time for observations (as if that got loaded next).
    # (just to test there isn't a race condition around partial updates)
    testbed.add_etl_completion(group="G", include={"observation"}, time="2021")
    # still no B because other resources are missing
    _assert_encounter_ids(testbed, {"A"})

    # Finish the new ETL job for all resources
    testbed.add_etl_completion(group="G", exclude={"observation"}, time="2021")
    _assert_encounter_ids(testbed, {"A", "B", "L"})
