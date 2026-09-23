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
        "  id, row, dosage_text, dosage_route_display, dosage_timing_text, "
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
            "dosage_text": "2 tablets daily",
            "dosage_route_display": "Oral route",
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
            "dosage_text": "1 to 2 tablets as needed",
            "dosage_route_display": None,
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
