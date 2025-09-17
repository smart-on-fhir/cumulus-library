"""Tests for core__location"""

import json

from tests import testbed_utils


def test_core_location_simple(tmp_path):
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add(
        "location",
        {
            "resourceType": "Location",
            "id": "simple",
            "identifier": [{"system": "id-sys", "value": "id-val"}],
            "status": "active",
            "name": "Test Location",
            "type": [
                {"coding": [{"system": "type-sys", "code": "type-code", "display": "type-dis"}]}
            ],
            "managingOrganization": {"reference": "Organization/owner"},
            "partOf": {"reference": "Location/parent"},
        },
    )

    con = testbed.build()
    df = con.sql("SELECT * FROM core__location").df()
    rows = json.loads(df.to_json(orient="records"))

    assert rows == [
        {
            "id": "simple",
            "identifier_value": "id-val",
            "identifier_system": "id-sys",
            "status": "active",
            "name": "Test Location",
            "type_code": "type-code",
            "type_system": "type-sys",
            "type_display": "type-dis",
            "location_ref": "Location/simple",
            "managing_organization_ref": "Organization/owner",
            "part_of_ref": "Location/parent",
        }
    ]


def test_core_location_minimal(tmp_path):
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add("location", {"resourceType": "Location", "id": "nothing"})

    con = testbed.build()
    df = con.sql("SELECT * FROM core__location").df()
    rows = json.loads(df.to_json(orient="records"))

    assert rows == [
        {
            "id": "nothing",
            "identifier_value": None,
            "identifier_system": None,
            "status": None,
            "name": None,
            "type_code": None,
            "type_system": None,
            "type_display": None,
            "location_ref": "Location/nothing",
            "managing_organization_ref": None,
            "part_of_ref": None,
        }
    ]
