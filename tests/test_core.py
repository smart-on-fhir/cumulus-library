"""unit tests for counts generation"""

import datetime  # noqa: F401
from pathlib import Path

import pytest
import toml

from cumulus_library.cli import StudyRunner
from cumulus_library.studies.core.core_templates import core_templates
from tests.conftest import modify_resource_column


def get_sorted_table_data(cursor, table):
    num_cols = cursor.execute(
        f"SELECT count(*) FROM information_schema.columns WHERE table_name='{table}'"
    ).fetchone()[0]
    return cursor.execute(
        f"SELECT * FROM '{table}' ORDER BY " f"{','.join(map(str, range(1,num_cols)))}"
    ).fetchall()


@pytest.mark.parametrize(
    "table",
    [
        ("core__condition"),
        ("core__documentreference"),
        ("core__encounter"),
        ("core__encounter_type"),
        ("core__medication"),
        ("core__medicationrequest"),
        ("core__observation"),
        ("core__observation_lab"),
        ("core__count_condition_month"),
        ("core__count_documentreference_month"),
        ("core__count_encounter_month"),
        ("core__count_encounter_type_month"),
        ("core__count_observation_lab_month"),
        ("core__count_medicationrequest_month"),
        ("core__count_patient"),
    ],
)
def test_core_tables(mock_db_core, table):
    cursor = mock_db_core.cursor()

    # The schema check is to ensure we have a consistent order for the data in
    # these files, mostly for making git history simpler in case of minor changes
    table_rows = get_sorted_table_data(cursor, table)

    # For regenerating data if needed
    # with open(f'./tests/test_data/core/{table}.txt','wt', encoding="UTF-8") as f:
    #     for row in table_rows:
    #         f.write(str(f"{row}\n"))

    with open(f"./tests/test_data/core/{table}.txt", encoding="UTF-8") as f:
        ref_table = []
        for row in f.readlines():
            ref_table.append(eval(row))  # pylint: disable=eval-used
    for row in ref_table:
        assert row in table_rows
    for row in table_rows:
        assert row in ref_table
    assert len(table_rows) == len(ref_table)


def test_core_count_missing_data(tmp_path, mock_db):
    null_code_class = {
        "id": None,
        "code": None,
        "display": None,
        "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
        "userSelected": None,
        "version": None,
    }
    cursor = mock_db.cursor()
    modify_resource_column(cursor, "encounter", "class", null_code_class)

    builder = StudyRunner(mock_db, f"{tmp_path}/data_path/")
    builder.clean_and_build_study(
        f"{Path(__file__).parent.parent}/cumulus_library/studies/core",
        stats_build=False,
    )
    table_rows = get_sorted_table_data(cursor, "core__count_encounter_month")
    # For regenerating data if needed
    # with open(
    #     f"./tests/test_data/core/core__count_encounter_month_missing_data.txt",
    #     "wt",
    #     encoding="UTF-8",
    # ) as f:
    #     for row in table_rows:
    #         f.write(str(f"{row}\n"))
    with open(
        "./tests/test_data/core/core__count_encounter_month_missing_data.txt",
        encoding="UTF-8",
    ) as f:
        ref_table = []
        for row in f.readlines():
            ref_table.append(eval(row))  # pylint: disable=eval-used
    for row in ref_table:
        assert row in table_rows
    assert len(table_rows) == len(ref_table)


def test_core_counts_exported(mock_db_core):
    manifest = toml.load(
        f"{Path(__file__).parent.parent}/cumulus_library/studies/core/manifest.toml"
    )
    manifest["export_config"]["export_list"].remove("core__meta_version")
    manifest["export_config"]["export_list"].remove("core__meta_date")
    count_tables = (
        mock_db_core.cursor()
        .execute(
            "SELECT distinct(table_name) FROM information_schema.tables "
            "WHERE table_name LIKE 'core__count_%'"
        )
        .fetchall()
    )
    count_tables = [x[0] for x in count_tables]
    assert set(manifest["export_config"]["export_list"]) == set(count_tables)


# for this one, since the query output is very long and likely to change
# since it's a study table, we're going to do targeted comparisons around the
# polymorphism and not validate the whole thing


# omitting the double false case since we don't call thison that condition
@pytest.mark.parametrize(
    "medication_datasources,contains,omits",
    [
        (
            {
                "by_contained_ref": True,
                "by_external_ref": False,
            },
            ["LIKE '#%'", "contained_medication"],
            ["LIKE 'Medication/%'", "UNION", "external_medication"],
        ),
        (
            {
                "by_contained_ref": False,
                "by_external_ref": True,
            },
            ["LIKE 'Medication/%'", "external_medication"],
            ["LIKE '#%'", "UNION", "contained_medication"],
        ),
        (
            {
                "by_contained_ref": True,
                "by_external_ref": True,
            },
            [
                "LIKE '#%'",
                "LIKE 'Medication/%'",
                "UNION",
                "contained_medication",
                "external_medication",
            ],
            [],
        ),
    ],
)
def test_core_medication_query(medication_datasources, contains, omits):
    query = core_templates.get_core_template(
        "medication", config={"medication_datasources": medication_datasources}
    )
    for item in contains:
        assert item in query
    for item in omits:
        assert item not in query
