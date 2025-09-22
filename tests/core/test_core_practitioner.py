"""Tests for core__practitioner"""

import json

from tests import testbed_utils


def test_core_practitioner_simple(tmp_path):
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add(
        "practitioner",
        {
            "resourceType": "Practitioner",
            "id": "simple",
            "identifier": [{"system": "id-sys", "value": "id-val"}],
            "active": True,
            "qualification": [
                {
                    "code": {
                        "coding": [
                            {"system": "qual-sys", "code": "qual-code", "display": "qual-dis"}
                        ],
                    },
                }
            ],
        },
    )

    con = testbed.build()
    df = con.sql("SELECT * FROM core__practitioner").df()
    rows = json.loads(df.to_json(orient="records"))

    assert rows == [
        {
            "id": "simple",
            "identifier_value": "id-val",
            "identifier_system": "id-sys",
            "active": True,
            "qualification_code_code": "qual-code",
            "qualification_code_system": "qual-sys",
            "qualification_code_display": "qual-dis",
            "practitioner_ref": "Practitioner/simple",
        }
    ]


def test_core_practitioner_minimal(tmp_path):
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add("practitioner", {"resourceType": "Practitioner", "id": "nothing"})

    con = testbed.build()
    df = con.sql("SELECT * FROM core__practitioner").df()
    rows = json.loads(df.to_json(orient="records"))

    assert rows == [
        {
            "id": "nothing",
            "identifier_value": None,
            "identifier_system": None,
            "active": None,
            "qualification_code_code": None,
            "qualification_code_system": None,
            "qualification_code_display": None,
            "practitioner_ref": "Practitioner/nothing",
        }
    ]
