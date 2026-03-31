"""unit tests for counts generation"""

import datetime  # noqa: F401
import pathlib
import tomllib
from unittest import mock

import pytest

import cumulus_library
from cumulus_library import cli
from tests import conftest, testbed_utils


@pytest.mark.parametrize(
    "table,minimum",
    [
        ("core__allergyintolerance", True),
        ("core__condition", True),
        ("core__diagnosticreport", True),
        ("core__documentreference", True),
        ("core__encounter", True),
        ("core__episodeofcare", True),
        ("core__incomplete_encounter", True),
        ("core__medicationrequest", True),
        ("core__observation", True),
        ("core__observation_lab", True),
        ("core__observation_vital_signs", True),
        ("core__patient", True),
        ("core__procedure", True),
        ("core__servicerequest", True),
        ("core__specimen", True),
        ("core__count_allergyintolerance_month", False),
        ("core__count_condition_month", False),
        ("core__count_diagnosticreport_month", False),
        ("core__count_documentreference_month", False),
        ("core__count_encounter_month", False),
        ("core__count_encounter_all_types_month", False),
        ("core__count_observation_lab_month", False),
        ("core__count_medicationrequest_month", False),
        ("core__count_patient", False),
        ("core__count_procedure_month", False),
        ("core__count_servicerequest_month", True),
        ("core__count_specimen_month", True),
    ],
)
def test_core_tables(tmp_path, mock_db, table, minimum):
    # Historically, these tests used 10-patient minimums.
    # But that's a bit annoying, because it requires having enough data to pass those minimums
    # and not all resources have easy ways to generate them (i.e. synthea might not support the
    # resource). In general, for new resources, you should probably set minimum as True.
    min_count = 1 if minimum else 10

    config = cumulus_library.StudyConfig(db=mock_db, schema="main")
    builder = cli.StudyRunner(config, data_path=tmp_path)
    with mock.patch("cumulus_library.builders.counts_builder.DEFAULT_MIN_SUBJECT", new=min_count):
        builder.clean_and_build_study(
            pathlib.Path(__file__).parent.parent.parent.parent / "cumulus_library/studies/core",
            options={},
        )

    cursor = mock_db.cursor()

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
    db = testbed.build()

    table_rows, cols = conftest.get_sorted_table_data(db.connection, "core__count_encounter_month")
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
    actions = manifest["stages"]["build_core"]
    expected_count_tables = []
    for action in actions:
        if action["type"] == "export:counts":
            expected_count_tables = expected_count_tables + action["tables"]
    count_tables = (
        mock_db_core.cursor()
        .execute(
            "SELECT distinct(table_name) FROM information_schema.tables "
            "WHERE table_name LIKE 'core__count_%'"
        )
        .fetchall()
    )
    count_tables = [x[0] for x in count_tables]
    assert set(expected_count_tables) == set(count_tables)


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
    db = testbed.build()
    patients = db.connection.sql("SELECT id FROM core__patient").fetchall()
    assert {e[0] for e in patients} == {"A"}
    rows = db.connection.sql("SELECT id FROM core__allergyintolerance").fetchall()
    assert {r[0] for r in rows} == {"Allergy"}
    conditions = db.connection.sql("SELECT id FROM core__condition").fetchall()
    assert {c[0] for c in conditions} == {"ConA"}
    encounters = db.connection.sql("SELECT id FROM core__encounter").fetchall()
    assert {e[0] for e in encounters} == {"EncA"}
    rows = db.connection.sql("SELECT id FROM core__medicationrequest").fetchall()
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
    db = testbed.build()
    docs = db.connection.sql("SELECT id, encounter_ref FROM core__documentreference").fetchall()
    # We should see one row per encounter, including null encounters
    expected = {
        ("NoEnc", None),
        ("OneEnc", "Encounter/A"),
        ("TwoEnc", "Encounter/A"),
        ("TwoEnc", "Encounter/B"),
    }
    assert expected == set(docs)


def test_entered_in_error_skipped(tmp_path):
    resources = {
        "allergy_intolerance": {
            "verificationStatus": {
                "coding": [
                    {
                        "code": "entered-in-error",
                        "system": "http://terminology.hl7.org/CodeSystem/allergyintolerance-verification",
                    }
                ]
            }
        },
        "condition": {
            "verificationStatus": {
                "coding": [
                    {
                        "code": "entered-in-error",
                        "system": "http://terminology.hl7.org/CodeSystem/condition-ver-status",
                    }
                ]
            }
        },
        "diagnostic_report": None,
        "document_reference": None,
        "encounter": None,
        "medication_request": None,
        "observation": None,
        "procedure": None,
    }

    testbed = testbed_utils.LocalTestbed(tmp_path)
    for res_slug, status in resources.items():
        if not status:
            status = {"status": "entered-in-error"}
        method = getattr(testbed, f"add_{res_slug}")
        method("good")
        method("bad", **status)

    db = testbed.build()

    for res_slug in resources:
        table = res_slug.replace("_", "")
        ids = db.connection.sql(f"SELECT id FROM core__{table}").fetchall()
        assert ids == [("good",)], res_slug


def test_core_build_source(tmp_path):
    """Verify that we can generate core tables with some minimal data filled in"""
    testbed = testbed_utils.LocalTestbed(tmp_path)
    # Just add bare resources, with minimal data
    db = testbed.build()
    log = db.connection.sql("SELECT * FROM core__lib_build_source").fetchall()
    expected = [
        ("default", "core__meta_version", "TABLE"),
        ("default", "core__fhir_act_encounter_code_v3", "TABLE"),
        ("default", "core__fhir_mapping_code_system_uri", "TABLE"),
        ("default", "core__fhir_mapping_resource_uri", "TABLE"),
        ("default", "core__fhir_mapping_expected_act_encounter_code_v3", "TABLE"),
        ("default", "core__allergyintolerance_dn_clinical_status", "TABLE"),
        ("default", "core__allergyintolerance_dn_verification_status", "TABLE"),
        ("default", "core__allergyintolerance_dn_code", "TABLE"),
        ("default", "core__allergyintolerance_dn_reaction_substance", "TABLE"),
        ("default", "core__allergyintolerance_dn_reaction_manifestation", "TABLE"),
        ("default", "core__condition_dn_category", "TABLE"),
        ("default", "core__condition_dn_clinical_status", "TABLE"),
        ("default", "core__condition_codable_concepts_display", "TABLE"),
        ("default", "core__condition_codable_concepts_all", "TABLE"),
        ("default", "core__condition_dn_verification_status", "TABLE"),
        ("default", "core__diagnosticreport_dn_category", "TABLE"),
        ("default", "core__diagnosticreport_dn_code", "TABLE"),
        ("default", "core__diagnosticreport_dn_conclusioncode", "TABLE"),
        ("default", "core__documentreference_dn_type", "TABLE"),
        ("default", "core__documentreference_dn_category", "TABLE"),
        ("default", "core__documentreference_dn_format", "TABLE"),
        ("default", "core__encounter_dn_type", "TABLE"),
        ("default", "core__encounter_dn_servicetype", "TABLE"),
        ("default", "core__encounter_dn_priority", "TABLE"),
        ("default", "core__encounter_dn_reasoncode", "TABLE"),
        ("default", "core__encounter_dn_dischargedisposition", "TABLE"),
        ("default", "core__episodeofcare_dn_type", "TABLE"),
        ("default", "core__location_dn_type", "TABLE"),
        ("default", "core__medication_dn_code", "TABLE"),
        ("default", "core__medicationrequest_dn_inline_code", "TABLE"),
        ("default", "core__medicationrequest_dn_contained_code", "TABLE"),
        ("default", "core__medicationrequest_dn_category", "TABLE"),
        ("default", "core__observation_dn_category", "TABLE"),
        ("default", "core__observation_dn_code", "TABLE"),
        ("default", "core__observation_component_code", "TABLE"),
        ("default", "core__observation_component_dataabsentreason", "TABLE"),
        ("default", "core__observation_component_interpretation", "TABLE"),
        ("default", "core__observation_component_valuecodeableconcept", "TABLE"),
        ("default", "core__observation_dn_interpretation", "TABLE"),
        ("default", "core__observation_dn_valuecodeableconcept", "TABLE"),
        ("default", "core__observation_dn_dataabsentreason", "TABLE"),
        ("default", "core__organization_dn_type", "TABLE"),
        ("default", "core__patient_ext_race", "TABLE"),
        ("default", "core__patient_ext_ethnicity", "TABLE"),
        ("default", "core__practitioner_dn_qualification_code", "TABLE"),
        ("default", "core__practitionerrole_dn_code", "TABLE"),
        ("default", "core__practitionerrole_dn_specialty", "TABLE"),
        ("default", "core__procedure_dn_category", "TABLE"),
        ("default", "core__procedure_dn_code", "TABLE"),
        ("default", "core__servicerequest_dn_category", "TABLE"),
        ("default", "core__servicerequest_dn_code", "TABLE"),
        ("default", "core__specimen_dn_type", "TABLE"),
        ("default", "core__patient", "TABLE"),
        ("default", "core__allergyintolerance", "TABLE"),
        ("default", "core__condition", "TABLE"),
        ("default", "core__diagnosticreport", "TABLE"),
        ("default", "core__documentreference", "TABLE"),
        ("default", "core__encounter", "TABLE"),
        ("default", "core__incomplete_encounter", "TABLE"),
        ("default", "core__episodeofcare", "TABLE"),
        ("default", "core__location", "TABLE"),
        ("default", "core__medicationrequest", "TABLE"),
        ("default", "core__observation", "TABLE"),
        ("default", "core__observation_component_valuequantity", "TABLE"),
        ("default", "core__organization", "TABLE"),
        ("default", "core__practitioner", "TABLE"),
        ("default", "core__practitionerrole", "TABLE"),
        ("default", "core__procedure", "TABLE"),
        ("default", "core__servicerequest", "TABLE"),
        ("default", "core__specimen", "TABLE"),
        ("default", "core__observation_lab", "TABLE"),
        ("default", "core__observation_vital_signs", "TABLE"),
        ("default", "core__meta_date", "TABLE"),
        ("default", "core__count_allergyintolerance_month", "TABLE"),
        ("default", "core__count_condition_month", "TABLE"),
        ("default", "core__count_diagnosticreport_month", "TABLE"),
        ("default", "core__count_documentreference_month", "TABLE"),
        ("default", "core__count_encounter_month", "TABLE"),
        ("default", "core__count_encounter_all_types", "TABLE"),
        ("default", "core__count_encounter_all_types_month", "TABLE"),
        ("default", "core__count_encounter_type_month", "TABLE"),
        ("default", "core__count_encounter_priority_month", "TABLE"),
        ("default", "core__count_encounter_service_month", "TABLE"),
        ("default", "core__count_medicationrequest_month", "TABLE"),
        ("default", "core__count_observation_lab_month", "TABLE"),
        ("default", "core__count_patient", "TABLE"),
        ("default", "core__count_procedure_month", "TABLE"),
        ("default", "core__count_servicerequest_month", "TABLE"),
        ("default", "core__count_specimen_month", "TABLE"),
    ]
    for row in expected:
        assert row in log
    for row in log:
        assert row in expected
