"""Tests for core__allergyintolerance"""

import functools
import itertools
import json
from unittest import mock

from tests import conftest, testbed_utils


def combine_dictionaries(*combos: list[dict]) -> list[dict]:
    return [
        functools.reduce(lambda x, y: x | y, tuple_of_dicts)
        for tuple_of_dicts in itertools.product(*combos)
    ]


def dict_set_from_list(rows: list[dict]) -> set[tuple]:
    return {tuple(sorted(row.items())) for row in rows}


def test_core_allergy_many_cases(tmp_path):
    """Verify that we multiply rows as needed when multiple options appear"""
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add_allergy_intolerance("Nothing")
    testbed.add_allergy_intolerance(
        "Multiple Rows",
        clinicalStatus={
            "coding": [
                {
                    "code": "inactive",
                    "system": "http://terminology.hl7.org/CodeSystem/allergyintolerance-clinical",
                    "display": "Inactive",
                },
                {
                    "code": "resolved",
                    "system": "http://terminology.hl7.org/CodeSystem/allergyintolerance-clinical",
                    "display": "Resolved",
                },
                {"code": "extra", "system": "http://example.com/", "display": "Extra"},
            ],
        },
        verificationStatus={
            "coding": [
                {
                    "code": "unconfirmed",
                    "system": "http://terminology.hl7.org/CodeSystem/allergyintolerance-verification",
                    "display": "Unconfirmed",
                },
                {
                    "code": "presumed",
                    "system": "http://terminology.hl7.org/CodeSystem/allergyintolerance-verification",
                    "display": "Presumed",
                },
                {"code": "extra", "system": "http://example.com/", "display": "Extra"},
            ],
        },
        type="allergy",
        category=["food", "environment"],
        criticality="low",
        code={
            "coding": [
                {
                    "code": "10156",
                    "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
                    "display": "sucralfate",
                },
                {"code": "extra", "system": "http://example.com/", "display": "Extra"},
            ],
        },
        patient={"reference": "Patient/P1"},
        encounter={"reference": "Encounter/E1"},
        recordedDate="2019-12-11T10:10:10+05:00",
        reaction=[
            {
                "manifestation": [
                    {
                        "coding": [
                            {"code": "0", "system": "http://example.com/", "display": "Zero"},
                        ]
                    },
                ],
            },
            {
                "substance": {
                    "coding": [
                        {"code": "1", "system": "http://example.com/", "display": "One"},
                        {"code": "2", "system": "http://example.com/", "display": "Two"},
                    ]
                },
                "manifestation": [
                    {
                        "coding": [
                            {"code": "3", "system": "http://example.com/", "display": "Three"},
                            {"code": "4", "system": "http://example.com/", "display": "Four"},
                        ]
                    },
                    {
                        "coding": [
                            {"code": "5", "system": "http://example.com/", "display": "Five"},
                        ]
                    },
                ],
                "severity": "mild",
            },
        ],
    )

    con = testbed.build()
    df = con.sql("SELECT * FROM core__allergyintolerance").df()
    rows = json.loads(df.to_json(orient="records"))

    assert 29 == len(rows)

    nothing = {
        "id": "Nothing",
        "allergyintolerance_ref": "AllergyIntolerance/Nothing",
        "clinicalStatus_code": None,
        "verificationStatus_code": None,
        "type": None,
        "category": None,
        "criticality": None,
        "code_code": None,
        "code_system": None,
        "code_display": None,
        "patient_ref": None,
        "encounter_ref": None,
        "recordedDate": conftest.date_to_epoch(2020, 1, 1),
        "recordedDate_week": conftest.date_to_epoch(2019, 12, 30),
        "recordedDate_month": conftest.date_to_epoch(2020, 1, 1),
        "recordedDate_year": conftest.date_to_epoch(2020, 1, 1),
        "reaction_row": None,
        "reaction_substance_code": None,
        "reaction_substance_system": None,
        "reaction_substance_display": None,
        "reaction_manifestation_code": None,
        "reaction_manifestation_system": None,
        "reaction_manifestation_display": None,
        "reaction_severity": None,
    }
    combos = combine_dictionaries(
        # Start with a list of size one - all the consistent elements across all rows
        [
            {
                "id": "Multiple Rows",
                "allergyintolerance_ref": "AllergyIntolerance/Multiple Rows",
                # These next two only have one value in the results, despite having two matching
                # code systems. This is because the builder code filters out extra codes and only
                # keeps one (which is fine for this use case).
                "clinicalStatus_code": "inactive",
                "verificationStatus_code": "presumed",
                "type": "allergy",
                "criticality": "low",
                "patient_ref": "Patient/P1",
                "encounter_ref": "Encounter/E1",
                "recordedDate": conftest.date_to_epoch(2019, 12, 11),
                "recordedDate_week": conftest.date_to_epoch(2019, 12, 9),
                "recordedDate_month": conftest.date_to_epoch(2019, 12, 1),
                "recordedDate_year": conftest.date_to_epoch(2019, 1, 1),
            },
        ],
        [
            {"category": "food"},
            {"category": "environment"},
        ],
        [
            {
                "code_code": "10156",
                "code_system": "http://www.nlm.nih.gov/research/umls/rxnorm",
                "code_display": "sucralfate",
            },
            {"code_code": "extra", "code_system": "http://example.com/", "code_display": "Extra"},
        ],
        [
            {
                "reaction_row": 1.0,
                "reaction_substance_code": None,
                "reaction_substance_system": None,
                "reaction_substance_display": None,
                "reaction_manifestation_code": "0",
                "reaction_manifestation_system": "http://example.com/",
                "reaction_manifestation_display": "Zero",
                "reaction_severity": None,
            },
            *combine_dictionaries(
                [
                    # consistent elements
                    {
                        "reaction_row": 2.0,
                        "reaction_severity": "mild",
                    },
                ],
                [
                    {
                        "reaction_substance_code": "1",
                        "reaction_substance_system": "http://example.com/",
                        "reaction_substance_display": "One",
                    },
                    {
                        "reaction_substance_code": "2",
                        "reaction_substance_system": "http://example.com/",
                        "reaction_substance_display": "Two",
                    },
                ],
                [
                    {
                        "reaction_manifestation_code": "3",
                        "reaction_manifestation_system": "http://example.com/",
                        "reaction_manifestation_display": "Three",
                    },
                    {
                        "reaction_manifestation_code": "4",
                        "reaction_manifestation_system": "http://example.com/",
                        "reaction_manifestation_display": "Four",
                    },
                    {
                        "reaction_manifestation_code": "5",
                        "reaction_manifestation_system": "http://example.com/",
                        "reaction_manifestation_display": "Five",
                    },
                ],
            ),
        ],
    )
    assert 28 == len(combos)  # sanity check our product math

    expected_set = dict_set_from_list(combos)
    expected_set |= dict_set_from_list([nothing])
    assert expected_set == dict_set_from_list(rows)


@mock.patch("cumulus_library.builders.counts.DEFAULT_MIN_SUBJECT", new=1)
def test_core_count_allergy_intolerance_by_patient(tmp_path):
    """Verify that our basic allergy count is per-patient"""
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add_allergy_intolerance(
        "No Patient",
        code={"coding": [{"display": "Peanuts"}]},
    )
    testbed.add_allergy_intolerance(
        "Patient 1, eggs",
        patient={"reference": "Patient/1"},
        code={"coding": [{"display": "Eggs"}]},
    )
    testbed.add_allergy_intolerance(
        "Patient 1, eggs (duplicate, ignored)",
        patient={"reference": "Patient/1"},
        code={"coding": [{"display": "Eggs"}]},
    )
    testbed.add_allergy_intolerance(
        "Patient 2, eggs",
        category=["food"],
        patient={"reference": "Patient/2"},
        code={"coding": [{"display": "Eggs"}]},
        reaction=[{"manifestation": [{"coding": [{"display": "Rash"}]}]}],
    )
    testbed.add_allergy_intolerance(
        "Patient 2, fish",
        patient={"reference": "Patient/2"},
        code={"coding": [{"display": "Fish"}]},
    )

    con = testbed.build()
    rows = set(con.sql("SELECT * FROM core__count_allergyintolerance_month").fetchall())
    expected = {
        # Everything (note: two, not three overall results, because we count by patient)
        (2, None, None, None, None),
        # Eggs
        (2, None, None, "Eggs", None),
        (1, "food", "2020-01-01", "Eggs", "Rash"),
        (1, "cumulus__none", "2020-01-01", "Eggs", "cumulus__none"),
        # Fish
        (1, None, None, "Fish", None),
        (1, "cumulus__none", "2020-01-01", "Fish", "cumulus__none"),
    }
    assert expected.issubset(rows)
    assert (1, None, None, None, "Peanuts") not in rows
