"""Tests for core__medicationrequest"""

import json

import pytest

from tests import testbed_utils

RXNORM = "http://www.nlm.nih.gov/research/umls/rxnorm"


@pytest.mark.parametrize(
    "dosage,expected",
    [
        (None, [None]),  # No dosage
        ([{"sequence": 12}], [None]),  # Irrelevant dosage
        ([{"text": "One"}], ["One"]),  # Single dosage
        (  # Multiple dosages
            [{"text": "Multi1"}, {"text": "Multi2"}, {"sequence": 1}],
            ["Multi1", "Multi2"],
        ),
    ],
)
def test_core_medreq_dosage(tmp_path, dosage, expected):
    """Verify that dosage text is optionally included"""
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add_medication_request("A", dosageInstruction=dosage)
    con = testbed.build()
    texts = con.sql(
        "SELECT dosageInstruction_text FROM core__medicationrequest "
        "ORDER BY dosageInstruction_text"
    ).fetchall()
    assert [x[0] for x in texts] == expected


@pytest.mark.parametrize(
    "codings,expected",
    [
        ([{"code": "A", "system": RXNORM}], ["A"]),  # one code
        ([{"code": "A", "system": "nope"}], []),  # skip non-rxnorm
        (
            [{"code": "A", "system": RXNORM}, {"code": "B", "system": "nope"}],
            ["A"],  # ignores non-rxnorm, but keeps good ones
        ),
        (
            [{"code": "A", "system": RXNORM}, {"code": "B", "system": RXNORM}],
            ["A", "B"],
        ),
    ],
)
def test_core_medreq_only_rxnorm(tmp_path, codings, expected):
    """Verify that we only include rxnorm codes

    Note: this test was written against the found behavior at the time.
    It's not clear this is how we *want* this table to behave.
    """
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add_medication_request("A", codings=codings)
    con = testbed.build()
    codes = con.sql(
        "SELECT medication_code FROM core__medicationrequest "
        "ORDER BY medication_code"
    ).fetchall()
    assert [x[0] for x in codes] == expected


def test_core_medreq_only_inline(tmp_path):
    """Verify that we only include inline medication requests

    Note: this test was written against the found behavior at the time.
    It's not clear this is how we *want* this table to behave.
    """
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add_medication_request("Inline", mode="inline")
    testbed.add_medication_request("Contained", mode="contained")
    testbed.add_medication_request("External", mode="external")
    con = testbed.build()
    ids = con.sql("SELECT id FROM core__medicationrequest ORDER BY id").fetchall()
    assert [x[0] for x in ids] == ["Inline"]


def test_core_med_all_types(tmp_path):
    """Verify that we handle all types of medications"""
    testbed = testbed_utils.LocalTestbed(tmp_path)
    med_args = {
        "codings": [
            {
                "code": "c",
                "system": "letters",
                "display": "C",
                "userSelected": True,
            }
        ],
        "encounter": {"reference": "Encounter/E"},
        "subject": {"reference": "Patient/P"},
    }
    testbed.add_medication_request("Inline", mode="inline", **med_args)
    testbed.add_medication_request("Contained", mode="contained", **med_args)
    testbed.add_medication_request("External", mode="external", **med_args)

    con = testbed.build()
    df = con.sql("SELECT * FROM core__medication ORDER BY id").df()
    rows = json.loads(df.to_json(orient="records"))

    expected_body = {
        "code": "c",
        "code_system": "letters",
        "display": "C",
        "encounter_ref": "Encounter/E",
        "patient_ref": "Patient/P",
        "userSelected": True,
    }
    assert [
        {"id": "Contained", **expected_body},
        {"id": "External", **expected_body},
        {"id": "Inline", **expected_body},
    ] == rows
