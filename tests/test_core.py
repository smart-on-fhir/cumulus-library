"""unit tests for counts generation"""

import datetime  # pylint: disable=unused-import

from pathlib import Path

import pytest
import toml

from cumulus_library.cli import StudyBuilder
from tests.conftest import modify_resource_column
from tests.conftest import ResourceTableIdPos as idpos  # pylint: disable=unused-import


@pytest.mark.parametrize(
    "table",
    [
        ("core__condition"),
        ("core__documentreference"),
        ("core__encounter"),
        ("core__observation"),
        ("core__patient"),
        ("core__count_condition_month"),
        ("core__count_documentreference_month"),
        ("core__count_encounter_month"),
        ("core__count_observation_lab_month"),
        ("core__count_patient"),
    ],
)
def test_core_tables(mock_db_core, table):
    cursor = mock_db_core.cursor()
    table_rows = cursor.execute(f"SELECT * FROM {table}").fetchall()
    # For regenerating data if needed
    # note that, by design, count queries are returned in an arbitrary order,
    # and sorted outside of the database during export.
    # with open(f'./tests/test_data/core/{table}.txt','wt', encoding="UTF-8") as f:
    #     # TODO: cutover to switch/case on min python version 3.10
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

    builder = StudyBuilder(mock_db, f"{tmp_path}/data_path/")
    builder.clean_and_build_study(
        f"{Path(__file__).parent.parent}/cumulus_library/studies/core",
        f"{tmp_path}/data_path/",
        False,
    )
    table_rows = cursor.execute("SELECT * FROM core__count_encounter_month").fetchall()
    # For regenerating data if needed
    # note that, by design, count queries are returned in an arbitrary order,
    # and sorted outside of the database during export.
    # with open(f'./tests/test_data/core/core__count_encounter_month_missing_data.txt','wt', encoding="UTF-8") as f:
    #    for row in sorted(table_rows, key = lambda x: int(x[0])):
    #        f.write(str(f"{row}\n"))
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
