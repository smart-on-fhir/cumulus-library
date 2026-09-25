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

    db = testbed.build()
    df = db.connection.sql("SELECT * FROM core__medicationrequest ORDER BY id").df()
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
        "authoredOn": conftest.timestamp_to_epoch(2021, 10, 16, 12, 0, 0, 0),
        "authoredOn_month": conftest.date_to_epoch(2021, 10, 1),
        "medication_code": "c",
        "medication_system": "letters",
        "medication_display": "C",
        "requester_ref": "Practitioner/P",
        "status_reason_code": None,
        "status_reason_system": None,
        "status_reason_display": None,
        "status_reason_text": None,
        "course_of_therapy_code": None,
        "course_of_therapy_system": None,
        "course_of_therapy_display": None,
        "course_of_therapy_text": None,
        "dispense_refills_allowed": None,
        "dispense_quantity_value": None,
        "dispense_quantity_unit": None,
        "dispense_quantity_system": None,
        "dispense_quantity_code": None,
        "expected_supply_duration_value": None,
        "expected_supply_duration_unit": None,
        "validity_period_start": None,
        "validity_period_end": None,
        "prior_prescription_ref": None,
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
    db = testbed.build()
    df = db.connection.sql(
        "SELECT id, category_code FROM core__medicationrequest ORDER BY category_code"
    ).df()
    rows = json.loads(df.to_json(orient="records"))
    assert [
        {"id": "TestMed", "category_code": "inpatient"},
        {"id": "TestMed", "category_code": "outpatient"},
    ] == rows


def test_core_med_dosage_route_codings(tmp_path):
    """Verify that route codings don't fan out the dosage table"""
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add_medication_request(
        "TwoSystems",
        dosageInstruction=[
            {
                "route": {
                    "text": "Oral",
                    "coding": [
                        {"code": "26643006", "system": "http://snomed.info/sct"},
                        {
                            "code": "15",
                            "system": "urn:oid:1.2.840.114350.1.13.100.2.7.4.798268.7025",
                        },
                    ],
                },
            },
        ],
    )
    testbed.add_medication_request(
        "TextOnly",
        dosageInstruction=[{"route": {"text": "By mouth"}}],
    )

    db = testbed.build()
    dosage = db.connection.sql(
        "SELECT id, row, dose_row, dosage_route_text "
        "FROM core__medicationrequest_dosageinstruction ORDER BY id, row"
    ).fetchall()
    assert dosage == [
        ("TextOnly", 1, None, "By mouth"),
        ("TwoSystems", 1, None, "Oral"),
    ]

    routes = db.connection.sql(
        "SELECT id, row, system, code FROM core__medicationrequest_dn_dosage_route "
        "ORDER BY id, row, system"
    ).fetchall()
    assert routes == [
        ("TwoSystems", 1, "http://snomed.info/sct", "26643006"),
        (
            "TwoSystems",
            1,
            "urn:oid:1.2.840.114350.1.13.100.2.7.4.798268.7025",
            "15",
        ),
    ]


def test_core_med_dose_and_rate_entries(tmp_path):
    """Verify that multiple doseAndRate entries get their own dose_row"""
    testbed = testbed_utils.LocalTestbed(tmp_path)
    dose_type = "http://terminology.hl7.org/CodeSystem/dose-rate-type"
    testbed.add_medication_request(
        "TwoDoses",
        dosageInstruction=[
            {
                "sequence": 2,
                "route": {
                    "coding": [
                        {"code": "a", "system": "sys1"},
                        {"code": "b", "system": "sys2"},
                    ],
                },
                "doseAndRate": [
                    {
                        "type": {
                            "text": "Ordered",
                            "coding": [
                                {"code": "ordered", "system": dose_type},
                                {"code": "ORD", "system": "other"},
                            ],
                        },
                        "doseQuantity": {"value": 2, "unit": "tablet"},
                    },
                    {
                        "type": {
                            "text": "Calculated",
                            "coding": [{"code": "calculated", "system": dose_type}],
                        },
                        "doseQuantity": {"value": 4, "unit": "tablet"},
                    },
                ],
            },
        ],
    )

    db = testbed.build()
    rows = db.connection.sql(
        "SELECT id, row, dose_row, dosage_sequence, dosage_dose_rate_type_text, "
        "  dosage_dose_value "
        "FROM core__medicationrequest_dosageinstruction ORDER BY id, row, dose_row"
    ).fetchall()
    assert rows == [
        ("TwoDoses", 1, 1, 2, "Ordered", 2.0),
        ("TwoDoses", 1, 2, 2, "Calculated", 4.0),
    ]

    types = db.connection.sql(
        "SELECT id, row, dose_row, system, code "
        "FROM core__medicationrequest_dn_dose_rate_type ORDER BY dose_row, code"
    ).fetchall()
    assert types == [
        ("TwoDoses", 1, 1, "other", "ORD"),
        ("TwoDoses", 1, 1, dose_type, "ordered"),
        ("TwoDoses", 1, 2, dose_type, "calculated"),
    ]

    duplicate_entries = db.connection.sql(
        "SELECT id, row, dose_row FROM core__medicationrequest_dosageinstruction "
        "GROUP BY id, row, dose_row HAVING count(*) > 1"
    ).fetchall()
    assert duplicate_entries == []
