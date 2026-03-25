"""Tests for core__episodeofcare"""

import json

from tests import conftest, testbed_utils


def test_core_episodeofcare_simple(tmp_path):
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add(
        "episodeofcare",
        {
            "resourceType": "EpisodeOfCare",
            "id": "simple",
            "status": "finished",
            "type": [
                {
                    "coding": [{"system": "type-sys", "code": "type-code", "display": "type-dis"}],
                }
            ],
            "patient": {"reference": "Patient/pat"},
            "period": {"start": "2022-06-16T10:23:00Z", "end": "2022-06-19T10:24:00Z"},
        },
    )

    db = testbed.build()
    df = db.connection.sql("SELECT * FROM core__episodeofcare").df()
    rows = json.loads(df.to_json(orient="records"))

    assert rows == [
        {
            "id": "simple",
            "status": "finished",
            "type_code": "type-code",
            "type_system": "type-sys",
            "type_display": "type-dis",
            "period_start_day": conftest.date_to_epoch(2022, 6, 16),
            "period_start_week": conftest.date_to_epoch(2022, 6, 13),
            "period_start_month": conftest.date_to_epoch(2022, 6, 1),
            "period_start_year": conftest.date_to_epoch(2022, 1, 1),
            "period_end_day": conftest.date_to_epoch(2022, 6, 19),
            "period_end_week": conftest.date_to_epoch(2022, 6, 13),
            "period_end_month": conftest.date_to_epoch(2022, 6, 1),
            "period_end_year": conftest.date_to_epoch(2022, 1, 1),
            "episodeofcare_ref": "EpisodeOfCare/simple",
            "patient_ref": "Patient/pat",
        }
    ]


def test_core_episodeofcare_minimal(tmp_path):
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add("episodeofcare", {"resourceType": "EpisodeOfCare", "id": "nothing"})

    db = testbed.build()
    df = db.connection.sql("SELECT * FROM core__episodeofcare").df()
    rows = json.loads(df.to_json(orient="records"))

    assert rows == [
        {
            "id": "nothing",
            "status": None,
            "type_code": None,
            "type_system": None,
            "type_display": None,
            "period_start_day": None,
            "period_start_week": None,
            "period_start_month": None,
            "period_start_year": None,
            "period_end_day": None,
            "period_end_week": None,
            "period_end_month": None,
            "period_end_year": None,
            "episodeofcare_ref": "EpisodeOfCare/nothing",
            "patient_ref": None,
        }
    ]
