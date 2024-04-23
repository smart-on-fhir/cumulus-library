"""Tests for core__patient"""

import pytest

from tests import testbed_utils


@pytest.mark.parametrize(
    "addresses,expected",
    [
        (None, "cumulus__none"),  # no address
        ([{"city": "Boston"}], "cumulus__none"),  # partial, but useless address
        (  # multiple addresses
            [
                {"city": "Boston"},  # null postal code - should not be picked up
                {"postalCode": "12345"},
                {"postalCode": "00000"},
            ],
            "123",
        ),
    ],
)
def test_core_patient_addresses(tmp_path, addresses, expected):
    """Verify that addresses are parsed out"""
    testbed = testbed_utils.LocalTestbed(tmp_path, with_patient=False)
    testbed.add_patient("A", address=addresses)
    con = testbed.build()
    codes = con.sql("SELECT postalCode_3 FROM core__patient").fetchall()
    assert [(expected,)] == codes


@pytest.mark.parametrize(
    "extensions,expected_ethnicity,expected_race",
    [
        (None, "unknown", "unknown"),  # no extension
        (  # basic ombCategory
            [
                {
                    "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity",
                    "extension": [
                        {
                            "url": "detailed",  # ignored in favor of ombCategory
                            "valueCoding": {"display": "EthDetailed"},
                        },
                        {
                            "url": "ombCategory",
                            "valueCoding": {"display": "EthA"},
                        },
                    ],
                },
                {
                    "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race",
                    "extension": [
                        {
                            "url": "ombCategory",
                            "valueCoding": {"display": "RaceA"},
                        },
                        {
                            "url": "detailed",  # ignored in favor of ombCategory
                            "valueCoding": {"display": "RaceDetailed"},
                        },
                    ],
                },
            ],
            "etha",
            "racea",
        ),
        (  # will use detailed if we must
            [
                {
                    "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity",
                    "extension": [
                        {
                            "url": "detailed",
                            "valueCoding": {"display": "EthDetailed"},
                        }
                    ],
                },
                {
                    "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race",
                    "extension": [
                        {
                            "url": "detailed",
                            "valueCoding": {"display": "RaceDetailed"},
                        }
                    ],
                },
            ],
            "ethdetailed",
            "racedetailed",
        ),
        (  # will ignore entries without a display
            [
                {
                    "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity",
                    "extension": [
                        {
                            "url": "ombCategory",
                            "valueCoding": {
                                "display": ""  # empty string (instead of null)
                            },
                        },
                        {
                            "url": "ombCategory",
                            "valueCoding": {"display": "EthB"},
                        },
                    ],
                },
                {
                    "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race",
                    "extension": [
                        {
                            "url": "ombCategory",
                            "valueCoding": {"code": "just-a-code"},
                        },
                        {
                            "url": "detailed",
                            "valueCoding": {"display": "RaceDetailed"},
                        },
                    ],
                },
            ],
            "ethb",
            "racedetailed",
        ),
        (  # multiples get joined
            [
                {
                    "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity",
                    "extension": [
                        {
                            "url": "detailed",
                            "valueCoding": {"display": "EthB"},
                        },
                        {
                            "url": "detailed",
                            "valueCoding": {"display": "EthA"},
                        },
                    ],
                },
                {
                    "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race",
                    "extension": [
                        {
                            "url": "ombCategory",
                            "valueCoding": {"display": "RaceA"},
                        },
                        {
                            "url": "ombCategory",
                            "valueCoding": {"display": "RaceB"},
                        },
                    ],
                },
            ],
            "etha; ethb",
            "racea; raceb",
        ),
    ],
)
def test_core_patient_extensions(
    tmp_path, extensions, expected_ethnicity, expected_race
):
    """Verify that we grab race & ethnicity correctly"""
    testbed = testbed_utils.LocalTestbed(tmp_path, with_patient=False)
    testbed.add_patient("A", extension=extensions)
    con = testbed.build()
    displays = con.sql(
        "SELECT ethnicity_display, race_display FROM core__patient"
    ).fetchall()
    assert [(expected_ethnicity, expected_race)] == displays
