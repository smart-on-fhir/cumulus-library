"""Tests for core__servicerequest"""

import json

from tests import conftest, testbed_utils


def test_core_servicerequest_simple(tmp_path):
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add(
        "servicerequest",
        {
            "resourceType": "ServiceRequest",
            "id": "simple",
            "status": "draft",
            "intent": "order",
            "category": [
                {"coding": [{"system": "cat-sys", "code": "cat-code", "display": "cat-dis"}]},
            ],
            "code": {
                "coding": [{"system": "code-sys", "code": "code-code", "display": "code-dis"}],
            },
            "subject": {"reference": "Patient/pat"},
            "encounter": {"reference": "Encounter/enc"},
            "occurrenceDateTime": "2022-06-17T10:23:00Z",
            "occurrencePeriod": {"start": "2022-06-16T10:23:00Z", "end": "2022-06-19T10:24:00Z"},
            "authoredOn": "2022-06-20T11:23:00Z",
            "requester": {"reference": "Practitioner/pract"},
            "specimen": [{"reference": "Specimen/spec"}],
        },
    )

    db = testbed.build()
    df = db.connection.sql("SELECT * FROM core__servicerequest").df()
    rows = json.loads(df.to_json(orient="records"))

    assert rows == [
        {
            "id": "simple",
            "status": "draft",
            "intent": "order",
            "category_code": "cat-code",
            "category_system": "cat-sys",
            "category_display": "cat-dis",
            "code_code": "code-code",
            "code_system": "code-sys",
            "code_display": "code-dis",
            "occurrenceDateTime_day": conftest.date_to_epoch(2022, 6, 17),
            "occurrenceDateTime_week": conftest.date_to_epoch(2022, 6, 13),
            "occurrenceDateTime_month": conftest.date_to_epoch(2022, 6, 1),
            "occurrenceDateTime_year": conftest.date_to_epoch(2022, 1, 1),
            "occurrencePeriod_start_day": conftest.date_to_epoch(2022, 6, 16),
            "occurrencePeriod_start_week": conftest.date_to_epoch(2022, 6, 13),
            "occurrencePeriod_start_month": conftest.date_to_epoch(2022, 6, 1),
            "occurrencePeriod_start_year": conftest.date_to_epoch(2022, 1, 1),
            "occurrencePeriod_end_day": conftest.date_to_epoch(2022, 6, 19),
            "occurrencePeriod_end_week": conftest.date_to_epoch(2022, 6, 13),
            "occurrencePeriod_end_month": conftest.date_to_epoch(2022, 6, 1),
            "occurrencePeriod_end_year": conftest.date_to_epoch(2022, 1, 1),
            "authoredOn_day": conftest.date_to_epoch(2022, 6, 20),
            "authoredOn_week": conftest.date_to_epoch(2022, 6, 20),
            "authoredOn_month": conftest.date_to_epoch(2022, 6, 1),
            "authoredOn_year": conftest.date_to_epoch(2022, 1, 1),
            "servicerequest_ref": "ServiceRequest/simple",
            "subject_ref": "Patient/pat",
            "encounter_ref": "Encounter/enc",
            "requester_ref": "Practitioner/pract",
            "specimen_ref": "Specimen/spec",
        }
    ]


def test_core_servicerequest_minimal(tmp_path):
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add("servicerequest", {"resourceType": "ServiceRequest", "id": "nothing"})

    db = testbed.build()
    df = db.connection.sql("SELECT * FROM core__servicerequest").df()
    rows = json.loads(df.to_json(orient="records"))

    assert rows == [
        {
            "id": "nothing",
            "status": None,
            "intent": None,
            "category_code": None,
            "category_system": None,
            "category_display": None,
            "code_code": None,
            "code_system": None,
            "code_display": None,
            "occurrencePeriod_start_day": None,
            "occurrencePeriod_start_week": None,
            "occurrencePeriod_start_month": None,
            "occurrencePeriod_start_year": None,
            "occurrencePeriod_end_day": None,
            "occurrencePeriod_end_week": None,
            "occurrencePeriod_end_month": None,
            "occurrencePeriod_end_year": None,
            "occurrenceDateTime_day": None,
            "occurrenceDateTime_week": None,
            "occurrenceDateTime_month": None,
            "occurrenceDateTime_year": None,
            "authoredOn_day": None,
            "authoredOn_week": None,
            "authoredOn_month": None,
            "authoredOn_year": None,
            "servicerequest_ref": "ServiceRequest/nothing",
            "subject_ref": None,
            "encounter_ref": None,
            "requester_ref": None,
            "specimen_ref": None,
        }
    ]
