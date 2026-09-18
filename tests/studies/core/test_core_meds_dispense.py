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
        "quantity_value": 90,
        "quantity_unit": "tablet",
        "days_supply_value": 30,
        "days_supply_unit": "d",
        "whenPrepared": conftest.timestamp_to_epoch(2021, 10, 15, 8, 0, 0, 0),
        "whenPrepared_month": conftest.date_to_epoch(2021, 10, 1),
        "whenHandedOver": conftest.timestamp_to_epoch(2021, 10, 16, 12, 0, 0, 0),
        "whenHandedOver_day": conftest.date_to_epoch(2021, 10, 16),
        # 2021-10-16 is a Saturday; date_trunc('week') lands on the Monday
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

    Note that unlike MedicationRequest, MedicationDispense.category is 0..1 in
    R4, so the several codings all live inside one CodeableConcept.
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


def test_core_med_dispense_authorizing_prescriptions(tmp_path):
    """Verify that authorizing prescriptions land in their own table

    A dispense may cite several orders, so they are kept out of the main table
    to avoid fanning it out.
    """
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add_medication_dispense(
        "MultiRx",
        authorizingPrescription=[
            {"reference": "MedicationRequest/ReqA"},
            {"reference": "MedicationRequest/ReqB"},
        ],
    )
    testbed.add_medication_dispense("NoRx")

    db = testbed.build()
    df = db.connection.sql(
        "SELECT id, row, medicationrequest_ref "
        "FROM core__medicationdispense_authorizingprescription "
        "ORDER BY id, row"
    ).df()
    rows = json.loads(df.to_json(orient="records"))
    assert [
        {"id": "MultiRx", "row": 1, "medicationrequest_ref": "MedicationRequest/ReqA"},
        {"id": "MultiRx", "row": 2, "medicationrequest_ref": "MedicationRequest/ReqB"},
    ] == rows

    # The dispense itself should not be duplicated by its prescriptions
    dispenses = db.connection.sql("SELECT id FROM core__medicationdispense").fetchall()
    assert {d[0] for d in dispenses} == {"MultiRx", "NoRx"}


def test_core_med_dispense_entered_in_error(tmp_path):
    """Verify that we drop entered-in-error dispenses"""
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add_medication_dispense("Good")
    testbed.add_medication_dispense("Bad", status="entered-in-error")

    db = testbed.build()
    dispenses = db.connection.sql("SELECT id FROM core__medicationdispense").fetchall()
    assert {d[0] for d in dispenses} == {"Good"}
