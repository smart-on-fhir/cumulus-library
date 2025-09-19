"""Tests for core__organization"""

import json

from tests import testbed_utils


def test_core_organization_simple(tmp_path):
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add(
        "organization",
        {
            "resourceType": "Organization",
            "id": "simple",
            "identifier": [{"system": "id-sys", "value": "id-val"}],
            "active": True,
            "type": [
                {"coding": [{"system": "type-sys", "code": "type-code", "display": "type-dis"}]}
            ],
            "name": "Test Organization",
            "alias": ["second name"],
            "partOf": {"reference": "Organization/parent"},
        },
    )

    con = testbed.build()
    df = con.sql("SELECT * FROM core__organization").df()
    rows = json.loads(df.to_json(orient="records"))

    assert rows == [
        {
            "id": "simple",
            "identifier_value": "id-val",
            "identifier_system": "id-sys",
            "active": True,
            "type_code": "type-code",
            "type_system": "type-sys",
            "type_display": "type-dis",
            "name": "Test Organization",
            "alias": "Test Organization, second name",
            "organization_ref": "Organization/simple",
            "part_of_ref": "Organization/parent",
        }
    ]


def test_core_organization_minimal(tmp_path):
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add("organization", {"resourceType": "Organization", "id": "nothing"})

    con = testbed.build()
    df = con.sql("SELECT * FROM core__organization").df()
    rows = json.loads(df.to_json(orient="records"))

    assert rows == [
        {
            "id": "nothing",
            "identifier_value": None,
            "identifier_system": None,
            "active": None,
            "type_code": None,
            "type_system": None,
            "type_display": None,
            "name": None,
            "alias": None,
            "organization_ref": "Organization/nothing",
            "part_of_ref": None,
        }
    ]
