""" tests for propensity score matching generation """

from datetime import datetime
from pathlib import Path

import pytest
from freezegun import freeze_time

from cumulus_library import cli
from cumulus_library.statistics import psm


@freeze_time("2024-01-01")
@pytest.mark.parametrize(
    "toml_def,pos_set,neg_set,expected_first_record,expected_last_record",
    [
        (
            "psm_config.toml",
            28,
            129,
            {
                "encounter_ref": "Encounter/03e34b19-2889-b828-792d-2a83400c55be0",
                "example_diagnosis": "1",
                "instance_count": 1,
                "gender": "female",
                "race": "white",
                "code": "33737001",
            },
            {
                "encounter_ref": "Encounter/ed151e04-3dd6-8cb7-a3e5-777c8a8667f17",
                "example_diagnosis": "0",
                "instance_count": 1,
                "gender": "female",
                "race": "white",
                "code": "195662009",
            },
        ),
        (
            "psm_config_no_optional.toml",
            28,
            129,
            {
                "encounter_ref": "Encounter/03e34b19-2889-b828-792d-2a83400c55be0",
                "example_diagnosis": "1",
                "code": "33737001",
            },
            {
                "encounter_ref": "Encounter/ed151e04-3dd6-8cb7-a3e5-777c8a8667f17",
                "example_diagnosis": "0",
                "code": "195662009",
            },
        ),
    ],
)
def test_psm_create(
    tmp_path,
    mock_db_stats,
    toml_def,
    pos_set,
    neg_set,
    expected_first_record,
    expected_last_record,
):
    builder = cli.StudyRunner(mock_db_stats, data_path=Path(tmp_path))
    psmbuilder = psm.PsmBuilder(
        f"{Path(__file__).parent}/test_data/psm/{toml_def}",
        Path(tmp_path),  # config=study_parser.StudyConfig(db_type='duckdb')
    )
    mock_db_stats.cursor().execute(
        "create table psm_test__psm_cohort as (select * from core__condition "
        f"ORDER BY {psmbuilder.config.primary_ref} limit 100)"
    ).df()
    mock_db_stats.cursor().execute("select * from psm_test__psm_cohort").fetchall()
    safe_timestamp = (
        datetime.now()
        .replace(microsecond=0)
        .isoformat()
        .replace(":", "_")
        .replace("-", "_")
    )
    psmbuilder.execute_queries(
        mock_db_stats.pandas_cursor(),
        builder.schema_name,
        False,
        drop_table=True,
        table_suffix=safe_timestamp,
    )
    df = (
        mock_db_stats.cursor()
        .execute("select * from psm_test__psm_encounter_covariate")
        .df()
    )
    print(df.columns)
    ed_series = df["example_diagnosis"].value_counts()
    assert ed_series.iloc[0] == neg_set
    assert ed_series.iloc[1] == pos_set
    first_record = df.iloc[0].to_dict()
    print(first_record)
    assert first_record == expected_first_record
    last_record = df.iloc[neg_set + pos_set - 1].to_dict()
    print(last_record)
    assert last_record == expected_last_record
