"""Tests for core__procedure"""

import functools
import itertools
import json

from tests import conftest, testbed_utils


def combine_dictionaries(*combos: list[dict]) -> list[dict]:
    return [
        functools.reduce(lambda x, y: x | y, tuple_of_dicts)
        for tuple_of_dicts in itertools.product(*combos)
    ]


def dict_set_from_list(rows: list[dict]) -> set[tuple]:
    return {tuple(sorted(row.items())) for row in rows}


def test_core_procedure_many_cases(tmp_path):
    """Verify that we handle multiply rows as needed when multiple options appear"""
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add_procedure(
        "Multiple-Rows",
        status="final",
        category={
            "coding": [
                {"code": "cat1", "system": "sys:cat", "display": "Cat One"},
                {"code": "cat2", "system": "sys:cat", "display": "Cat Two"},
            ],
        },
        code={
            "coding": [
                {"code": "code1", "system": "sys:code", "display": "Code One"},
                {"code": "code2", "system": "sys:code", "display": "Code Two"},
            ],
        },
        subject={"reference": "Patient/P1"},
        encounter={"reference": "Encounter/E1"},
        performedDateTime="2019-12-11T10:10:10+05:00",
        performedPeriod={
            "start": "2019-12-10T10:10:10+05:00",
            "end": "2019-12-12T10:10:10+05:00",
        },
    )

    con = testbed.build()
    df = con.sql("SELECT * FROM core__procedure").df()
    rows = json.loads(df.to_json(orient="records"))
    assert len(rows) == 4

    combos = combine_dictionaries(
        # Start with a list of size one - all the consistent elements across all rows
        [
            {
                "id": "Multiple-Rows",
                "status": "final",
                "performedDateTime_day": conftest.date_to_epoch(2019, 12, 11),
                "performedDateTime_week": conftest.date_to_epoch(2019, 12, 9),
                "performedDateTime_month": conftest.date_to_epoch(2019, 12, 1),
                "performedDateTime_year": conftest.date_to_epoch(2019, 1, 1),
                "performedPeriod_start_day": conftest.date_to_epoch(2019, 12, 10),
                "performedPeriod_start_week": conftest.date_to_epoch(2019, 12, 9),
                "performedPeriod_start_month": conftest.date_to_epoch(2019, 12, 1),
                "performedPeriod_start_year": conftest.date_to_epoch(2019, 1, 1),
                "performedPeriod_end_day": conftest.date_to_epoch(2019, 12, 12),
                "performedPeriod_end_week": conftest.date_to_epoch(2019, 12, 9),
                "performedPeriod_end_month": conftest.date_to_epoch(2019, 12, 1),
                "performedPeriod_end_year": conftest.date_to_epoch(2019, 1, 1),
                "procedure_ref": "Procedure/Multiple-Rows",
                "subject_ref": "Patient/P1",
                "encounter_ref": "Encounter/E1",
            },
        ],
        [
            {"category_code": "cat1", "category_system": "sys:cat", "category_display": "Cat One"},
            {"category_code": "cat2", "category_system": "sys:cat", "category_display": "Cat Two"},
        ],
        [
            {"code_code": "code1", "code_system": "sys:code", "code_display": "Code One"},
            {"code_code": "code2", "code_system": "sys:code", "code_display": "Code Two"},
        ],
    )
    assert len(combos) == 4  # sanity check our product math

    assert dict_set_from_list(rows) == dict_set_from_list(combos)


def test_core_procedure_minimal(tmp_path):
    """Verify that no actual content works fine"""
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add_procedure("Nothing")

    con = testbed.build()
    df = con.sql("SELECT * FROM core__procedure").df()
    rows = json.loads(df.to_json(orient="records"))

    assert rows == [
        {
            "id": "Nothing",
            "status": None,
            "category_code": None,
            "category_system": None,
            "category_display": None,
            "code_code": None,
            "code_system": None,
            "code_display": None,
            "performedDateTime_day": None,
            "performedDateTime_week": None,
            "performedDateTime_month": None,
            "performedDateTime_year": None,
            "performedPeriod_start_day": None,
            "performedPeriod_start_week": None,
            "performedPeriod_start_month": None,
            "performedPeriod_start_year": None,
            "performedPeriod_end_day": None,
            "performedPeriod_end_week": None,
            "performedPeriod_end_month": None,
            "performedPeriod_end_year": None,
            "procedure_ref": "Procedure/Nothing",
            "subject_ref": None,
            "encounter_ref": None,
        }
    ]
