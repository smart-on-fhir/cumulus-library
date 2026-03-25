"""Tests for core__specimen"""

import json

from tests import conftest, testbed_utils


def test_core_specimen_simple(tmp_path):
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add(
        "specimen",
        {
            "resourceType": "Specimen",
            "id": "simple",
            "status": "unavailable",
            "type": {
                "coding": [{"system": "type-sys", "code": "type-code", "display": "type-dis"}],
            },
            "subject": {"reference": "Patient/pat"},
        },
    )

    db = testbed.build()
    df = db.connection.sql("SELECT * FROM core__specimen").df()
    rows = json.loads(df.to_json(orient="records"))

    assert rows == [
        {
            "id": "simple",
            "status": "unavailable",
            "type_code": "type-code",
            "type_system": "type-sys",
            "type_display": "type-dis",
            "specimen_ref": "Specimen/simple",
            "subject_ref": "Patient/pat",
        }
    ]


def test_core_specimen_minimal(tmp_path):
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add("specimen", {"resourceType": "Specimen", "id": "nothing"})

    db = testbed.build()
    df = db.connection.sql("SELECT * FROM core__specimen").df()
    rows = json.loads(df.to_json(orient="records"))

    assert rows == [
        {
            "id": "nothing",
            "status": None,
            "type_code": None,
            "type_system": None,
            "type_display": None,
            "specimen_ref": "Specimen/nothing",
            "subject_ref": None,
        }
    ]
