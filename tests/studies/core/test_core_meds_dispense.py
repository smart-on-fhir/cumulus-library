"""Tests for core__medicationdispense"""

import json

from tests import conftest, testbed_utils


def test_core_med_dispense_all_types(tmp_path):
    """Verify that we handle all types of medication dispenses"""
    testbed = testbed_utils.LocalTestbed(tmp_path)
    dispense_args = {
        "category": {
            "coding": [
                {
                    "code": "outpatient",
                    "system": "http://terminology.hl7.org/CodeSystem/medicationdispense-category",
                    "display": "Outpatient",
                },
            ],
        },
        "codings": [
            {
                "code": "c",
                "system": "letters",
                "display": "C",
            },
        ],
        # R4 MedicationDispense links to the Encounter via 'context'
        "context": {"reference": "Encounter/E"},
        "daysSupply": {"value": 30, "unit": "d"},
        "quantity": {"value": 90, "unit": "tablet"},
        "status": "completed",
        "subject": {"reference": "Patient/P"},
        "type": {
            "coding": [
                {
                    "code": "RF",
                    "system": "http://terminology.hl7.org/CodeSystem/v3-ActPharmacySupplyType",
                    "display": "Refill",
                },
            ],
        },
        "when_handed_over": "2021-10-16T12:00:00Z",
        "whenPrepared": "2021-10-15T08:00:00Z",
    }
    testbed.add_medication_dispense("Inline", mode="inline", **dispense_args)
    testbed.add_medication_dispense("Contained", mode="contained", **dispense_args)
    testbed.add_medication_dispense("External", mode="external", **dispense_args)

    db = testbed.build()
    df = db.connection.sql("SELECT * FROM core__medicationdispense ORDER BY id").df()
    rows = json.loads(df.to_json(orient="records"))

    body = {
        "status": "completed",
        "category_code": "outpatient",
        "category_system": "http://terminology.hl7.org/CodeSystem/medicationdispense-category",
        "category_display": "Outpatient",
        "type_code": "RF",
        "type_system": "http://terminology.hl7.org/CodeSystem/v3-ActPharmacySupplyType",
        "type_display": "Refill",
        "medication_code": "c",
        "medication_system": "letters",
        "medication_display": "C",
        "medicationrequest_ref": None,
        "performer_ref": None,
        "quantity_value": 90.0,
        "quantity_unit": "tablet",
        "quantity_system": None,
        "quantity_code": None,
        "days_supply_value": 30.0,
        "days_supply_unit": "d",
        "days_supply_system": None,
        "days_supply_code": None,
        "whenPrepared": conftest.timestamp_to_epoch(2021, 10, 15, 8, 0, 0, 0),
        "whenPrepared_month": conftest.date_to_epoch(2021, 10, 1),
        "whenHandedOver": conftest.timestamp_to_epoch(2021, 10, 16, 12, 0, 0, 0),
        "whenHandedOver_day": conftest.date_to_epoch(2021, 10, 16),
        "whenHandedOver_week": conftest.date_to_epoch(2021, 10, 11),
        "whenHandedOver_month": conftest.date_to_epoch(2021, 10, 1),
        "whenHandedOver_year": conftest.date_to_epoch(2021, 1, 1),
        "subject_ref": "Patient/P",
        "encounter_ref": "Encounter/E",
    }
    assert [
        {
            "id": "Contained",
            "medicationdispense_ref": "MedicationDispense/Contained",
            **body,
        },
        {
            "id": "External",
            "medicationdispense_ref": "MedicationDispense/External",
            **body,
        },
        {"id": "Inline", "medicationdispense_ref": "MedicationDispense/Inline", **body},
    ] == rows


def test_core_med_dispense_multiple_categories(tmp_path):
    """Verify that we report all category codings for a dispense

    MedicationDispense.category is 0..1 in R4,
    so the codings are all in one CodeableConcept.
    """
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add_medication_dispense(
        "TestDispense",
        category={
            "coding": [
                {
                    "code": "outpatient",
                    "system": "http://terminology.hl7.org/CodeSystem/medicationdispense-category",
                    "display": "Outpatient",
                },
                {
                    "code": "inpatient",
                    "system": "http://terminology.hl7.org/CodeSystem/medicationdispense-category",
                    "display": "Inpatient",
                },
            ],
        },
    )
    db = testbed.build()
    df = db.connection.sql(
        "SELECT id, category_code FROM core__medicationdispense ORDER BY category_code"
    ).df()
    rows = json.loads(df.to_json(orient="records"))
    assert [
        {"id": "TestDispense", "category_code": "inpatient"},
        {"id": "TestDispense", "category_code": "outpatient"},
    ] == rows


def test_core_med_dispense_entered_in_error(tmp_path):
    """Verify that we drop entered-in-error dispenses"""
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add_medication_dispense("Good")
    testbed.add_medication_dispense("Bad", status="entered-in-error")

    db = testbed.build()
    dispenses = db.connection.sql("SELECT id FROM core__medicationdispense").fetchall()
    assert {d[0] for d in dispenses} == {"Good"}


def test_core_med_dispense_dosage_instructions(tmp_path):
    """Verify dosage instructions, in both dose[x] representations

    dose[x] is a choice of Quantity or Range, and US Core supports both.
    """
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add_medication_dispense(
        "Taper",
        dosageInstruction=[
            {
                "text": "2 tablets daily",
                "route": {
                    "text": "Oral",
                    "coding": [
                        {
                            "code": "26643006",
                            "system": "http://snomed.info/sct",
                            "display": "Oral route",
                        },
                    ],
                },
                "timing": {
                    "code": {"text": "QD"},
                    "repeat": {"frequency": 1, "period": 1, "periodUnit": "d"},
                },
                "doseAndRate": [
                    {
                        "doseQuantity": {
                            "value": 2,
                            "unit": "tablet",
                            "system": "http://unitsofmeasure.org",
                            "code": "{tbl}",
                        },
                    },
                ],
            },
            {
                "text": "1 to 2 tablets as needed",
                "doseAndRate": [
                    {
                        "doseRange": {
                            "low": {"value": 1, "unit": "tablet"},
                            "high": {"value": 2, "unit": "tablet"},
                        },
                    },
                ],
            },
        ],
    )
    testbed.add_medication_dispense("NoDosage")

    db = testbed.build()
    df = db.connection.sql(
        "SELECT "
        "  id, row, dose_row, dosage_text, dosage_route_text, dosage_timing_text, "
        "  dosage_timing_frequency, dosage_timing_period_unit, dosage_dose_type, "
        "  dosage_dose_value, dosage_dose_low_value, dosage_dose_high_value, "
        "  dosage_dose_unit, subject_ref "
        "FROM core__medicationdispense_dosageinstruction "
        "ORDER BY id, row"
    ).df()
    rows = json.loads(df.to_json(orient="records"))
    assert [
        {
            "id": "Taper",
            "row": 1,
            "dose_row": 1,
            "dosage_text": "2 tablets daily",
            "dosage_route_text": "Oral",
            "dosage_timing_text": "QD",
            "dosage_timing_frequency": 1,
            "dosage_timing_period_unit": "d",
            "dosage_dose_type": "quantity",
            "dosage_dose_value": 2.0,
            "dosage_dose_low_value": None,
            "dosage_dose_high_value": None,
            "dosage_dose_unit": "tablet",
            "subject_ref": "Patient/A",
        },
        {
            "id": "Taper",
            "row": 2,
            "dose_row": 1,
            "dosage_text": "1 to 2 tablets as needed",
            "dosage_route_text": None,
            "dosage_timing_text": None,
            "dosage_timing_frequency": None,
            "dosage_timing_period_unit": None,
            "dosage_dose_type": "range",
            "dosage_dose_value": None,
            "dosage_dose_low_value": 1.0,
            "dosage_dose_high_value": 2.0,
            "dosage_dose_unit": "tablet",
            "subject_ref": "Patient/A",
        },
    ] == rows

    # The dispense itself should not be duplicated by its dosage instructions
    dispenses = db.connection.sql("SELECT id FROM core__medicationdispense").fetchall()
    assert {d[0] for d in dispenses} == {"Taper", "NoDosage"}


def test_core_med_dispense_dosage_route_codings(tmp_path):
    """Verify that route codings don't fan out the dosage table

    Coded routes live in the dn table with every system kept, while a route
    with only text (as some vendors send) just fills dosage_route_text.
    """
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add_medication_dispense(
        "TwoSystems",
        dosageInstruction=[
            {
                "route": {
                    "text": "Oral",
                    "coding": [
                        {
                            "code": "26643006",
                            "system": "http://snomed.info/sct",
                            "display": "Oral route",
                        },
                        {
                            "code": "15",
                            "system": "urn:oid:1.2.840.114350.1.13.100.2.7.4.798268.7025",
                            "display": "Oral",
                        },
                    ],
                },
            },
        ],
    )
    testbed.add_medication_dispense(
        "TextOnly",
        dosageInstruction=[{"route": {"text": "By mouth"}}],
    )

    db = testbed.build()
    dosage = db.connection.sql(
        "SELECT id, row, dose_row, dosage_route_text "
        "FROM core__medicationdispense_dosageinstruction ORDER BY id, row"
    ).fetchall()
    assert dosage == [
        ("TextOnly", 1, None, "By mouth"),
        ("TwoSystems", 1, None, "Oral"),
    ]

    routes = db.connection.sql(
        "SELECT id, row, system, code FROM core__medicationdispense_dn_dosage_route "
        "ORDER BY id, row, system"
    ).fetchall()
    assert routes == [
        (
            "TwoSystems",
            1,
            "http://snomed.info/sct",
            "26643006",
        ),
        (
            "TwoSystems",
            1,
            "urn:oid:1.2.840.114350.1.13.100.2.7.4.798268.7025",
            "15",
        ),
    ]


def test_core_med_dispense_dose_and_rate_entries(tmp_path):
    """Verify that multiple doseAndRate entries get their own dose_row

    Each entry can carry a type (e.g. ordered vs calculated), which is kept as
    text on the dosage table and as codings in the dose_rate_type table.
    """
    testbed = testbed_utils.LocalTestbed(tmp_path)
    dose_type = "http://terminology.hl7.org/CodeSystem/dose-rate-type"
    testbed.add_medication_dispense(
        "TwoDoses",
        dosageInstruction=[
            {
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
    testbed.add_medication_dispense(
        "OneDose",
        dosageInstruction=[
            {"doseAndRate": [{"doseQuantity": {"value": 1, "unit": "tablet"}}]},
            {"text": "no dose"},
        ],
    )

    db = testbed.build()
    rows = db.connection.sql(
        "SELECT id, row, dose_row, dosage_dose_rate_type_text, dosage_dose_value "
        "FROM core__medicationdispense_dosageinstruction ORDER BY id, row, dose_row"
    ).fetchall()
    assert rows == [
        ("OneDose", 1, 1, None, 1.0),
        ("OneDose", 2, None, None, None),
        ("TwoDoses", 1, 1, "Ordered", 2.0),
        ("TwoDoses", 1, 2, "Calculated", 4.0),
    ]

    types = db.connection.sql(
        "SELECT id, row, dose_row, system, code "
        "FROM core__medicationdispense_dn_dose_rate_type ORDER BY dose_row, code"
    ).fetchall()
    assert types == [
        ("TwoDoses", 1, 1, "other", "ORD"),
        ("TwoDoses", 1, 1, dose_type, "ordered"),
        ("TwoDoses", 1, 2, dose_type, "calculated"),
    ]

    duplicate_entries = db.connection.sql(
        "SELECT id, row, dose_row FROM core__medicationdispense_dosageinstruction "
        "GROUP BY id, row, dose_row HAVING count(*) > 1"
    ).fetchall()
    assert duplicate_entries == []


def test_core_med_dispense_dosage_parity_columns(tmp_path):
    """Verify sequence, patient instruction and as needed propagate"""
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add_medication_dispense(
        "Parity",
        dosageInstruction=[
            {
                "sequence": 3,
                "patientInstruction": "Take with food",
                "asNeededBoolean": True,
            },
        ],
    )

    db = testbed.build()
    rows = db.connection.sql(
        "SELECT dosage_sequence, dosage_patient_instruction, dosage_as_needed_bool "
        "FROM core__medicationdispense_dosageinstruction"
    ).fetchall()
    assert rows == [(3, "Take with food", True)]
