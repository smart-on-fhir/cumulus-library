"""Tests for core__diagnosticreport"""

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


def test_core_diag_report_many_cases(tmp_path):
    """Verify that we handle multiply rows as needed when multiple options appear"""
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add_diagnostic_report(
        "Multiple-Rows",
        status="final",
        category=[
            {"coding": [{"code": "cat1", "system": "sys:cat", "display": "Cat One"}]},
            {"coding": [{"code": "cat2", "system": "sys:cat", "display": "Cat Two"}]},
        ],
        code={
            "coding": [
                {"code": "code1", "system": "sys:code", "display": "Code One"},
                {"code": "code2", "system": "sys:code", "display": "Code Two"},
            ],
        },
        subject={"reference": "Patient/P1"},
        encounter={"reference": "Encounter/E1"},
        effectiveDateTime="2019-12-11T10:10:10+05:00",
        issued="2019-12-12T10:10:10+05:00",
        performer=[{"reference": "Practitioner/P1"}, {"reference": "Practitioner/P2"}],
        result=[
            {"reference": "Observation/result1"},
            {"reference": "Observation/result2"},
        ],
        conclusionCode=[
            {"coding": [{"code": "conc1", "system": "sys:conc", "display": "Conclusion One"}]},
            {"coding": [{"code": "conc2", "system": "sys:conc", "display": "Conclusion Two"}]},
        ],
    )

    con = testbed.build()
    df = con.sql("SELECT * FROM core__diagnosticreport").df()
    rows = json.loads(df.to_json(orient="records"))
    assert len(rows) == 32

    combos = combine_dictionaries(
        # Start with a list of size one - all the consistent elements across all rows
        [
            {
                "id": "Multiple-Rows",
                "diagnosticreport_ref": "DiagnosticReport/Multiple-Rows",
                "status": "final",
                "subject_ref": "Patient/P1",
                "encounter_ref": "Encounter/E1",
                "effectiveDateTime_day": conftest.date_to_epoch(2019, 12, 11),
                "effectiveDateTime_week": conftest.date_to_epoch(2019, 12, 9),
                "effectiveDateTime_month": conftest.date_to_epoch(2019, 12, 1),
                "effectiveDateTime_year": conftest.date_to_epoch(2019, 1, 1),
                "effectivePeriod_start_day": None,
                "effectivePeriod_start_week": None,
                "effectivePeriod_start_month": None,
                "effectivePeriod_start_year": None,
                "effectivePeriod_end_day": None,
                "effectivePeriod_end_week": None,
                "effectivePeriod_end_month": None,
                "effectivePeriod_end_year": None,
                "issued_day": conftest.date_to_epoch(2019, 12, 12),
                "issued_week": conftest.date_to_epoch(2019, 12, 9),
                "issued_month": conftest.date_to_epoch(2019, 12, 1),
                "issued_year": conftest.date_to_epoch(2019, 1, 1),
                "aux_has_text": False,
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
        [
            {
                "conclusionCode_code": "conc1",
                "conclusionCode_system": "sys:conc",
                "conclusionCode_display": "Conclusion One",
            },
            {
                "conclusionCode_code": "conc2",
                "conclusionCode_system": "sys:conc",
                "conclusionCode_display": "Conclusion Two",
            },
        ],
        [
            {"result_ref": "Observation/result1"},
            {"result_ref": "Observation/result2"},
        ],
        [
            {"performer_ref": "Practitioner/P1"},
            {"performer_ref": "Practitioner/P2"},
        ],
    )
    assert len(combos) == 32  # sanity check our product math

    assert dict_set_from_list(rows) == dict_set_from_list(combos)


def test_core_diag_report_minimal(tmp_path):
    """Verify that no actual content works fine"""
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add_diagnostic_report("Nothing")

    con = testbed.build()
    df = con.sql("SELECT * FROM core__diagnosticreport").df()
    rows = json.loads(df.to_json(orient="records"))

    assert rows == [
        {
            "id": "Nothing",
            "diagnosticreport_ref": "DiagnosticReport/Nothing",
            "status": None,
            "category_code": None,
            "category_system": None,
            "category_display": None,
            "code_code": None,
            "code_system": None,
            "code_display": None,
            "subject_ref": None,
            "encounter_ref": None,
            "effectiveDateTime_day": None,
            "effectiveDateTime_week": None,
            "effectiveDateTime_month": None,
            "effectiveDateTime_year": None,
            "effectivePeriod_start_day": None,
            "effectivePeriod_start_week": None,
            "effectivePeriod_start_month": None,
            "effectivePeriod_start_year": None,
            "effectivePeriod_end_day": None,
            "effectivePeriod_end_week": None,
            "effectivePeriod_end_month": None,
            "effectivePeriod_end_year": None,
            "issued_day": None,
            "issued_week": None,
            "issued_month": None,
            "issued_year": None,
            "aux_has_text": False,
            "performer_ref": None,
            "result_ref": None,
            "conclusionCode_code": None,
            "conclusionCode_system": None,
            "conclusionCode_display": None,
        }
    ]


def test_core_diag_report_period(tmp_path):
    """Verify that we parse the period correctly"""
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add_diagnostic_report(
        "Period",
        effectivePeriod={
            "start": "2023-10-06T09:30:00Z",
            "end": "2023-10-06T10:30:00Z",
        },
    )

    con = testbed.build()
    df = con.sql("SELECT * FROM core__diagnosticreport").df()
    rows = json.loads(df.to_json(orient="records"))

    assert len(rows) == 1
    effective_fields = {k: v for k, v in rows[0].items() if "effective" in k}
    assert effective_fields == {
        "effectiveDateTime_day": None,
        "effectiveDateTime_week": None,
        "effectiveDateTime_month": None,
        "effectiveDateTime_year": None,
        "effectivePeriod_start_day": conftest.date_to_epoch(2023, 10, 6),
        "effectivePeriod_start_week": conftest.date_to_epoch(2023, 10, 2),
        "effectivePeriod_start_month": conftest.date_to_epoch(2023, 10, 1),
        "effectivePeriod_start_year": conftest.date_to_epoch(2023, 1, 1),
        "effectivePeriod_end_day": conftest.date_to_epoch(2023, 10, 6),
        "effectivePeriod_end_week": conftest.date_to_epoch(2023, 10, 2),
        "effectivePeriod_end_month": conftest.date_to_epoch(2023, 10, 1),
        "effectivePeriod_end_year": conftest.date_to_epoch(2023, 1, 1),
    }


def test_core_diag_report_aux_has_text(tmp_path):
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add_diagnostic_report(
        "has-data-ext",
        presentedForm=[
            {
                "contentType": "text/html",
                "_data": {
                    "extension": [
                        {"url": "bogus"},
                        {
                            "url": "http://hl7.org/fhir/StructureDefinition/data-absent-reason",
                            "valueCode": "masked",
                        },
                    ],
                },
            },
        ],
    )
    testbed.add_diagnostic_report(
        "has-data",
        presentedForm=[{"contentType": "text/html", "data": "blarg"}],
    )
    testbed.add_diagnostic_report("no-data", presentedForm=[{"contentType": "text/html"}])
    testbed.add_diagnostic_report("no-content")

    con = testbed.build()
    df = con.sql("SELECT * FROM core__diagnosticreport").df()
    rows = json.loads(df.to_json(orient="records"))
    fields = {row["id"]: row["aux_has_text"] for row in rows}

    assert fields == {
        "has-data-ext": True,
        "has-data": True,
        "no-data": False,
        "no-content": False,
    }
