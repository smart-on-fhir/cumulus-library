"""unit tests for core observation support"""

import datetime  # noqa: F401
import json

from tests import testbed_utils


def _assert_valuequantity_schema(con) -> None:
    schema = con.sql(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'core__observation_component_valuequantity'
        """
    ).fetchall()
    assert [
        ("id", "VARCHAR"),
        ("row", "BIGINT"),
        ("value", "DOUBLE"),
        ("comparator", "VARCHAR"),
        ("unit", "VARCHAR"),
        ("code_system", "VARCHAR"),
        ("code", "VARCHAR"),
    ] == schema


def test_core_observation_component_low_schema(tmp_path):
    """Verify that we don't explode if no components exist in the schema/data"""
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add_observation("No Component")
    con = testbed.build()  # most importantly, this shouldn't blow up
    # Spot check some tables (a basic one, then the custom weird valuequantity one)
    rows = con.sql("SELECT id, row FROM core__observation_component_code").fetchall()
    assert 0 == len(rows)
    rows = con.sql("SELECT id FROM core__observation_component_valuequantity").fetchall()
    assert 0 == len(rows)
    _assert_valuequantity_schema(con)


def test_core_observation_component_valuequantity_low_schema(tmp_path):
    """Verify that we correctly handle 'value' being a float even if not present"""
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add_observation(
        "A",
        component=[
            {
                "valueQuantity": {
                    "code": "mmHg",
                    "system": "http://unitsofmeasure.org",
                },
            }
        ],
    )
    con = testbed.build()  # most importantly, this shouldn't blow up
    rows = con.sql("SELECT id FROM core__observation_component_valuequantity").fetchall()
    assert 1 == len(rows)
    _assert_valuequantity_schema(con)


def test_core_observation_component(tmp_path):
    """Verify that we capture Observation components correctly"""
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add_observation("No components")
    testbed.add_observation(
        "All fields filled in",
        component=[
            {
                "code": {
                    "coding": [
                        {"display": "Hello!", "code": "hello", "system": "hi-codes"},
                    ],
                },
                "dataAbsentReason": {
                    "coding": [{"display": "D", "code": "d", "system": "letters"}],
                },
                "interpretation": [
                    {
                        "coding": [{"display": "I", "code": "i", "system": "letters"}],
                    }
                ],
                "valueCodeableConcept": {
                    "coding": [{"display": "V", "code": "v", "system": "letters"}],
                },
                "valueQuantity": {
                    "code": "a-code",
                    "comparator": "<",
                    "system": "a-system",
                    "unit": "m",
                    "value": 3,
                },
            },
        ],
    )
    testbed.add_observation(
        "Multiple components",
        component=[
            {
                "code": {
                    "coding": [
                        {"code": "34", "system": "codesys"},
                        {"code": "thirty-four", "system": "codesys-alpha"},
                    ],
                },
                "dataAbsentReason": {
                    "coding": [
                        {"display": "gone", "code": "shrug", "system": "s"},
                        {"display": "dog ate it", "code": "dog", "system": "s"},
                    ],
                },
                "interpretation": [
                    {
                        "coding": [{"code": "low", "system": "high-or-low"}],
                    }
                ],
                "valueCodeableConcept": {
                    "coding": [{"display": "homework"}],
                },
            },
            {
                "code": {
                    "coding": [
                        {"code": "42", "system": "codesys"},
                        {"code": "forty-two", "system": "codesys-alpha"},
                    ],
                },
                "interpretation": [
                    {
                        "coding": [
                            {"display": "Good", "code": "good", "system": "quality"},
                            {"display": "Pumped about this one"},
                        ],
                    },
                    {"coding": [{"code": "high", "system": "high-or-low"}]},
                ],
                "valueQuantity": {"code": "here", "unit": "mmHg"},
            },
        ],
    )
    con = testbed.build()

    df = con.sql("SELECT * FROM core__observation_component_code ORDER BY id, row, code").df()
    rows = json.loads(df.to_json(orient="records"))
    assert [
        {
            "id": "All fields filled in",
            "row": 1,
            "code": "hello",
            "code_system": "hi-codes",
            "display": "Hello!",
            "userSelected": None,
        },
        {
            "id": "Multiple components",
            "row": 1,
            "code": "34",
            "code_system": "codesys",
            "display": None,
            "userSelected": None,
        },
        {
            "id": "Multiple components",
            "row": 1,
            "code": "thirty-four",
            "code_system": "codesys-alpha",
            "display": None,
            "userSelected": None,
        },
        {
            "id": "Multiple components",
            "row": 2,
            "code": "42",
            "code_system": "codesys",
            "display": None,
            "userSelected": None,
        },
        {
            "id": "Multiple components",
            "row": 2,
            "code": "forty-two",
            "code_system": "codesys-alpha",
            "display": None,
            "userSelected": None,
        },
    ] == rows

    df = con.sql(
        "SELECT * FROM core__observation_component_dataabsentreason " "ORDER BY id, row, code"
    ).df()
    rows = json.loads(df.to_json(orient="records"))
    assert [
        {
            "id": "All fields filled in",
            "row": 1,
            "code": "d",
            "code_system": "letters",
            "display": "D",
            "userSelected": None,
        },
        {
            "id": "Multiple components",
            "row": 1,
            "code": "dog",
            "code_system": "s",
            "display": "dog ate it",
            "userSelected": None,
        },
        {
            "id": "Multiple components",
            "row": 1,
            "code": "shrug",
            "code_system": "s",
            "display": "gone",
            "userSelected": None,
        },
    ] == rows

    df = con.sql(
        "SELECT * FROM core__observation_component_interpretation " "ORDER BY id, row, code"
    ).df()
    rows = json.loads(df.to_json(orient="records"))
    assert [
        {
            "id": "All fields filled in",
            "row": 1,
            "code": "i",
            "code_system": "letters",
            "display": "I",
            "userSelected": None,
        },
        {
            "id": "Multiple components",
            "row": 1,
            "code": "low",
            "code_system": "high-or-low",
            "display": None,
            "userSelected": None,
        },
        {
            "id": "Multiple components",
            "row": 2,
            "code": "good",
            "code_system": "quality",
            "display": "Good",
            "userSelected": None,
        },
        {
            "id": "Multiple components",
            "row": 2,
            "code": "high",
            "code_system": "high-or-low",
            "display": None,
            "userSelected": None,
        },
        {
            "id": "Multiple components",
            "row": 2,
            "code": None,
            "code_system": None,
            "display": "Pumped about this one",
            "userSelected": None,
        },
    ] == rows

    df = con.sql(
        "SELECT * FROM core__observation_component_valuecodeableconcept " "ORDER BY id, row, code"
    ).df()
    rows = json.loads(df.to_json(orient="records"))
    assert [
        {
            "id": "All fields filled in",
            "row": 1,
            "code": "v",
            "code_system": "letters",
            "display": "V",
            "userSelected": None,
        },
        {
            "id": "Multiple components",
            "row": 1,
            "code": None,
            "code_system": None,
            "display": "homework",
            "userSelected": None,
        },
    ] == rows

    df = con.sql(
        "SELECT * FROM core__observation_component_valuequantity " "ORDER BY id, row, code"
    ).df()
    rows = json.loads(df.to_json(orient="records"))
    assert [
        {
            "id": "All fields filled in",
            "row": 1,
            "code": "a-code",
            "code_system": "a-system",
            "comparator": "<",
            "unit": "m",
            "value": 3.0,
        },
        {
            "id": "Multiple components",
            "row": 2,
            "code": "here",
            "code_system": None,
            "comparator": None,
            "unit": "mmHg",
            "value": None,
        },
    ] == rows
    _assert_valuequantity_schema(con)
