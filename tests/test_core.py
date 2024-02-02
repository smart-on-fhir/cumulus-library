"""unit tests for counts generation"""

import datetime  # pylint: disable=unused-import

from pathlib import Path

import pytest
import toml

from cumulus_library.cli import StudyRunner
from cumulus_library.studies.core.core_templates import core_templates
from tests.conftest import modify_resource_column
from tests.conftest import ResourceTableIdPos as idpos  # pylint: disable=unused-import


def get_sorted_table_data(cursor, table):
    num_cols = cursor.execute(
        "SELECT count(*) FROM information_schema.columns " f"WHERE table_name='{table}'"
    ).fetchone()[0]
    return cursor.execute(
        f"SELECT * FROM '{table}' ORDER BY " f"{','.join(map(str, range(1,num_cols)))}"
    )


@pytest.mark.parametrize(
    "table",
    [
        ("core__condition"),
        ("core__documentreference"),
        ("core__encounter"),
        ("core__medication"),
        ("core__medicationrequest"),
        ("core__observation"),
        ("core__count_condition_month"),
        ("core__count_documentreference_month"),
        ("core__count_encounter_month"),
        ("core__count_observation_lab_month"),
        ("core__count_medicationrequest_month"),
        ("core__count_patient"),
    ],
)
def test_core_tables(mock_db_core, table):
    cursor = mock_db_core.cursor()
    # The schema check is to ensure we have a consistent order for the data in
    # these files, mostly for making git history simpler in case of minor changes
    num_cols = cursor.execute(
        "select count(*) from information_schema.columns " f"where table_name='{table}'"
    ).fetchone()[0]
    table_rows = cursor.execute(
        f"SELECT * FROM {table} ORDER BY {','.join(map(str, range(1,num_cols)))}"
    ).fetchall()

    # For regenerating data if needed

    # TODO: rework after moving id to first column
    # with open(f'./tests/test_data/core/{table}.txt','wt', encoding="UTF-8") as f:
    #     if table.startswith('core__count'):
    #         sortfn = lambda x: int(x[0])
    #     # non-counts tables are sorted by the primary FHIR resource key
    #     # TODO: the primary key should be first in these tables
    #     elif table == 'core__condition':
    #         sortfn = lambda x: x[idpos.CONDITION.value]
    #     elif table == 'core__documentreference':
    #         sortfn = lambda x: x[idpos.DOCUMENTREFERENCE.value]
    #     elif table == 'core__encounter':
    #         sortfn = lambda x: x[idpos.ENCOUNTER.value]
    #     elif table == 'core__medication':
    #         sortfn = lambda x: x[idpos.MEDICATION.value]
    #     elif table == 'core__medicationrequest':
    #         sortfn = lambda x: x[idpos.MEDICATIONREQUEST.value]
    #     elif table == 'core__observation':
    #         sortfn = lambda x: x[idpos.OBSERVATION.value]
    #     elif table == 'core__patient':
    #         sortfn = lambda x: x[idpos.PATIENT.value]
    #     for row in sorted(table_rows, key = sortfn):
    #         f.write(str(f"{row}\n"))

    with open(f"./tests/test_data/core/{table}.txt", "r", encoding="UTF-8") as f:
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
    num_cols = cursor.execute(
        "select count(*) from information_schema.columns "
        f"where table_name='core__count_encounter_month'"
    ).fetchone()[0]
    table_rows = cursor.execute(
        "SELECT * FROM core__count_encounter_month ORDER BY "
        f"{','.join(map(str, range(1,num_cols)))}"
    ).fetchall()
    # For regenerating data if needed
    # note that, by design, count queries are returned in an arbitrary order,
    # and sorted outside of the database during export.
    # with open(
    #     f"./tests/test_data/core/core__count_encounter_month_missing_data.txt",
    #     "wt",
    #     encoding="UTF-8",
    # ) as f:
    #     for row in sorted(table_rows, key=lambda x: int(x[0])):
    #         f.write(str(f"{row}\n"))
    with open(
        "./tests/test_data/core/core__count_encounter_month_missing_data.txt",
        "r",
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
