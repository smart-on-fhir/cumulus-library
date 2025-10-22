"""Tests for core__medicationrequest"""

import json

from tests import conftest, testbed_utils


def test_core_med_all_types(tmp_path):
    """Verify that we handle all types of medications"""
    testbed = testbed_utils.LocalTestbed(tmp_path)
    med_args = {
        "authoredOn": "2021-10-16T12:00:00Z",
        "category": [
            {
                "coding": [
                    {
                        "code": "outpatient",
                        "system": "http://terminology.hl7.org/CodeSystem/medicationrequest-category",
                        "display": "Outpatient",
                    },
                ],
            },
        ],
        "codings": [
            {
                "code": "c",
                "system": "letters",
                "display": "C",
            },
        ],
        "encounter": {"reference": "Encounter/E"},
        "intent": "order",
        "reportedBoolean": False,
        "reportedReference": {"reference": "Patient/Q"},
        "status": "active",
        "subject": {"reference": "Patient/P"},
        "requester": {"reference": "Practitioner/P"},
    }
    testbed.add_medication_request("Inline", mode="inline", **med_args)
    testbed.add_medication_request("Contained", mode="contained", **med_args)
    testbed.add_medication_request("External", mode="external", **med_args)

    con = testbed.build()
    df = con.sql("SELECT * FROM core__medicationrequest ORDER BY id").df()
    rows = json.loads(df.to_json(orient="records"))

    body = {
        "status": "active",
        "intent": "order",
        "category_code": "outpatient",
        "category_system": "http://terminology.hl7.org/CodeSystem/medicationrequest-category",
        "category_display": "Outpatient",
        "reportedBoolean": False,
        "reported_ref": "Patient/Q",
        "subject_ref": "Patient/P",
        "encounter_ref": "Encounter/E",
        "authoredOn": conftest.date_to_epoch(2021, 10, 16),
        "authoredOn_month": conftest.date_to_epoch(2021, 10, 1),
        "medication_code": "c",
        "medication_system": "letters",
        "medication_display": "C",
        "requester_ref": "Practitioner/P",
    }
    assert [
        {"id": "Contained", "medicationrequest_ref": "MedicationRequest/Contained", **body},
        {"id": "External", "medicationrequest_ref": "MedicationRequest/External", **body},
        {"id": "Inline", "medicationrequest_ref": "MedicationRequest/Inline", **body},
    ] == rows


def test_core_med_multiple_categories(tmp_path):
    """Verify that we report all categories for a med"""
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add_medication_request(
        "TestMed",
        category=[
            {
                "coding": [
                    {
                        "code": "outpatient",
                        "system": "http://terminology.hl7.org/CodeSystem/medicationrequest-category",
                        "display": "Outpatient",
                    },
                    {
                        "code": "inpatient",
                        "system": "http://terminology.hl7.org/CodeSystem/medicationrequest-category",
                        "display": "Inpatient",
                    },
                ],
            },
        ],
    )
    con = testbed.build()
    df = con.sql(
        "SELECT id, category_code FROM core__medicationrequest ORDER BY category_code"
    ).df()
    rows = json.loads(df.to_json(orient="records"))
    assert [
        {"id": "TestMed", "category_code": "inpatient"},
        {"id": "TestMed", "category_code": "outpatient"},
    ] == rows
