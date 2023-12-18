""" tests for propensity score matching generation """

from pathlib import Path

import pytest

from cumulus_library.cli import StudyBuilder
from cumulus_library.statistics.psm import PsmBuilder


@pytest.mark.parametrize(
    "toml_def,pos_set,neg_set,expected_first_record,expected_last_record",
    [
        (
            "psm_config.toml",
            7,
            7,
            {
                "encounter_ref": "Encounter/11381dc6-0e06-da55-0735-d1e7bbf8bb35",
                "example_diagnosis": "1",
                "instance_count": 1,
                "gender": "male",
                "race": "white",
                "code": "44465007",
            },
            {
                "encounter_ref": "Encounter/ed151e04-3dd6-8cb7-a3e5-777c8a8667f1",
                "example_diagnosis": "0",
                "instance_count": 1,
                "gender": "female",
                "race": "white",
                "code": "195662009",
            },
        ),
        (
            "psm_config_no_optional.toml",
            7,
            7,
            {
                "encounter_ref": "Encounter/11381dc6-0e06-da55-0735-d1e7bbf8bb35",
                "example_diagnosis": "1",
                "code": "44465007",
            },
            {
                "encounter_ref": "Encounter/ed151e04-3dd6-8cb7-a3e5-777c8a8667f1",
                "example_diagnosis": "0",
                "code": "195662009",
            },
        ),
    ],
)
def test_psm_create(
    mock_db_core,
    toml_def,
    pos_set,
    neg_set,
    expected_first_record,
    expected_last_record,
):
    builder = StudyBuilder(mock_db_core)
    psm = PsmBuilder(f"{Path(__file__).parent}/test_data/psm/{toml_def}")
    mock_db_core.cursor().execute(
        "create table core__psm_cohort as (select * from core__condition "
        f"ORDER BY {psm.config.primary_ref} limit 10)"
    ).df()
    mock_db_core.cursor().execute("select * from core__psm_cohort").fetchall()

    psm.execute_queries(
        mock_db_core.pandas_cursor(), builder.schema_name, False, drop_table=True
    )
    df = (
        mock_db_core.cursor()
        .execute("select * from psm_test__psm_encounter_covariate")
        .df()
    )
    ed_series = df["example_diagnosis"].value_counts()
    assert ed_series.iloc[0] == neg_set
    assert ed_series.iloc[1] == pos_set
    first_record = df.iloc[0].to_dict()
    assert first_record == expected_first_record
    last_record = df.iloc[neg_set + pos_set - 1].to_dict()
    assert last_record == expected_last_record
