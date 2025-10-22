"""Tests for core__practitionerrole"""

import json

from tests import testbed_utils


def test_core_practitionerrole_simple(tmp_path):
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add(
        "practitionerrole",
        {
            "resourceType": "PractitionerRole",
            "id": "simple",
            "identifier": [{"system": "id-sys", "value": "id-val"}],
            "active": True,
            "practitioner": {"reference": "Practitioner/prac"},
            "organization": {"reference": "Organization/org"},
            "code": [
                {"coding": [{"system": "code-sys", "code": "code-code", "display": "code-dis"}]}
            ],
            "specialty": [
                {"coding": [{"system": "spec-sys", "code": "spec-code", "display": "spec-dis"}]},
            ],
            "location": [{"reference": "Location/loc"}],
        },
    )

    con = testbed.build()
    df = con.sql("SELECT * FROM core__practitionerrole").df()
    rows = json.loads(df.to_json(orient="records"))

    assert rows == [
        {
            "id": "simple",
            "identifier_value": "id-val",
            "identifier_system": "id-sys",
            "active": True,
            "code_code": "code-code",
            "code_system": "code-sys",
            "code_display": "code-dis",
            "specialty_code": "spec-code",
            "specialty_system": "spec-sys",
            "specialty_display": "spec-dis",
            "practitionerrole_ref": "PractitionerRole/simple",
            "practitioner_ref": "Practitioner/prac",
            "organization_ref": "Organization/org",
            "location_ref": "Location/loc",
        }
    ]


def test_core_practitionerrole_minimal(tmp_path):
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add("practitionerrole", {"resourceType": "PractitionerRole", "id": "nothing"})

    con = testbed.build()
    df = con.sql("SELECT * FROM core__practitionerrole").df()
    rows = json.loads(df.to_json(orient="records"))

    assert rows == [
        {
            "id": "nothing",
            "identifier_value": None,
            "identifier_system": None,
            "active": None,
            "code_code": None,
            "code_system": None,
            "code_display": None,
            "specialty_code": None,
            "specialty_system": None,
            "specialty_display": None,
            "practitionerrole_ref": "PractitionerRole/nothing",
            "practitioner_ref": None,
            "organization_ref": None,
            "location_ref": None,
        }
    ]
