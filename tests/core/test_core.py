"""unit tests for counts generation"""

import datetime  # noqa: F401
import tomllib

import pytest

from tests import conftest, testbed_utils


@pytest.mark.parametrize(
    "table",
    [
        ("core__allergyintolerance"),
        ("core__condition"),
        ("core__diagnosticreport"),
        ("core__documentreference"),
        ("core__encounter"),
        ("core__medicationrequest"),
        ("core__observation"),
        ("core__observation_lab"),
        ("core__observation_vital_signs"),
        ("core__patient"),
        ("core__procedure"),
        ("core__count_allergyintolerance_month"),
        ("core__count_condition_month"),
        ("core__count_diagnosticreport_month"),
        ("core__count_documentreference_month"),
        ("core__count_encounter_month"),
        ("core__count_encounter_all_types_month"),
        ("core__count_observation_lab_month"),
        ("core__count_medicationrequest_month"),
        ("core__count_patient"),
        ("core__count_procedure_month"),
    ],
)
def test_core_tables(mock_db_core, table):
    cursor = mock_db_core.cursor()

    # The schema check is to ensure we have a consistent order for the data in
    # these files, mostly for making git history simpler in case of minor changes
    table_rows, cols = conftest.get_sorted_table_data(cursor, table)
    # For regenerating data if needed
    # with open(f"./tests/test_data/core/{table}.txt", "wt", encoding="UTF-8") as f:
    #     for row in table_rows:
    #         f.write(str(f"{row}\n"))
    with open(f"./tests/test_data/core/{table}.txt", encoding="UTF-8") as f:
        ref_table = []
        for row in f.readlines():
            ref_table.append(eval(row))  # pylint: disable=eval-used
    try:
        for row in ref_table:
            assert row in table_rows
        for row in table_rows:
            assert row in ref_table
        assert len(table_rows) == len(ref_table)
    except Exception as e:
        conftest.debug_diff_tables(cols, table_rows, ref_table, pos=0)
        raise e


def test_core_count_missing_data(tmp_path):
    testbed = testbed_utils.LocalTestbed(tmp_path)
    for i in range(10):
        row_id = str(i)
        testbed.add_patient(row_id)
        testbed.add_encounter(
            row_id,
            patient=row_id,
            **{
                "status": "finished",
                "class": {
                    # Note: no other fields here
                    "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
                },
            },
        )
    cursor = testbed.build()

    table_rows, cols = conftest.get_sorted_table_data(cursor, "core__count_encounter_month")
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
    try:
        for row in ref_table:
            assert row in table_rows
        for row in table_rows:
            assert row in ref_table
    except Exception:
        conftest.debug_diff_tables(cols, table_rows, ref_table, pos=0)
        raise
    assert len(table_rows) == len(ref_table)


def test_core_counts_exported(mock_db_core):
    with open(f"{conftest.LIBRARY_ROOT}/studies/core/manifest.toml", "rb") as f:
        manifest = tomllib.load(f)
    manifest["export_config"]["meta_list"].remove("core__meta_version")
    manifest["export_config"]["meta_list"].remove("core__meta_date")
    count_tables = (
        mock_db_core.cursor()
        .execute(
            "SELECT distinct(table_name) FROM information_schema.tables "
            "WHERE table_name LIKE 'core__count_%'"
        )
        .fetchall()
    )
    count_tables = [x[0] for x in count_tables]
    assert set(manifest["export_config"]["count_list"]) == set(count_tables)


def test_core_empty_database(tmp_path):
    """Verify that we can still generate core tables with no data filled in"""
    testbed = testbed_utils.LocalTestbed(tmp_path, with_patient=False)
    testbed.build()


def test_core_tiny_database(tmp_path):
    """Verify that we can generate core tables with some minimal data filled in"""
    testbed = testbed_utils.LocalTestbed(tmp_path)
    # Just add bare resources, with minimal data
    testbed.add_allergy_intolerance("Allergy")
    testbed.add_condition("ConA")
    testbed.add_encounter("EncA")
    testbed.add_medication_request("MedReqA")
    con = testbed.build()
    patients = con.sql("SELECT id FROM core__patient").fetchall()
    assert {e[0] for e in patients} == {"A"}
    rows = con.sql("SELECT id FROM core__allergyintolerance").fetchall()
    assert {r[0] for r in rows} == {"Allergy"}
    conditions = con.sql("SELECT id FROM core__condition").fetchall()
    assert {c[0] for c in conditions} == {"ConA"}
    encounters = con.sql("SELECT id FROM core__encounter").fetchall()
    assert {e[0] for e in encounters} == {"EncA"}
    rows = con.sql("SELECT id FROM core__medicationrequest").fetchall()
    assert {r[0] for r in rows} == {"MedReqA"}


def test_core_multiple_doc_encounters(tmp_path):
    """Verify that a DocRef with multiple encounters resolves to multiple entries"""
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add_encounter("A")
    testbed.add_encounter("B")
    testbed.add_document_reference("NoEnc")
    testbed.add_document_reference(
        "OneEnc",
        context={
            "encounter": [
                {"reference": "Encounter/A"},
            ],
        },
    )
    testbed.add_document_reference(
        "TwoEnc",
        context={
            "encounter": [{"reference": "Encounter/A"}, {"reference": "Encounter/B"}],
        },
    )
    con = testbed.build()
    docs = con.sql("SELECT id, encounter_ref FROM core__documentreference").fetchall()
    # We should see one row per encounter, including null encounters
    expected = {
        ("NoEnc", None),
        ("OneEnc", "Encounter/A"),
        ("TwoEnc", "Encounter/A"),
        ("TwoEnc", "Encounter/B"),
    }
    assert expected == set(docs)
