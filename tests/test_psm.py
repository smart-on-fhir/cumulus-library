"""tests for propensity score matching generation"""

import datetime
import pathlib
from unittest import mock

import pytest
from freezegun import freeze_time

from cumulus_library import cli, study_manifest
from cumulus_library.builders import psm_builder


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
    mock_db_stats_config,
    toml_def,
    pos_set,
    neg_set,
    expected_first_record,
    expected_last_record,
):
    builder = cli.StudyRunner(mock_db_stats_config, data_path=pathlib.Path(tmp_path))
    manifest = study_manifest.StudyManifest(
        study_path=f"{pathlib.Path(__file__).parent}/test_data/psm/"
    )
    psmbuilder = psm_builder.PsmBuilder(
        f"{pathlib.Path(__file__).parent}/test_data/psm/{toml_def}",
        pathlib.Path(tmp_path),
    )
    builder.config.db.cursor().execute(
        "create table psm_test__psm_cohort as (select * from core__condition "
        f"ORDER BY {psmbuilder.config.primary_ref} limit 100)"
    ).df()
    safe_timestamp = (
        datetime.datetime.now()
        .replace(microsecond=0)
        .isoformat()
        .replace(":", "_")
        .replace("-", "_")
    )
    psmbuilder.execute_queries(
        builder.config,
        manifest,
        drop_table=True,
        table_suffix=safe_timestamp,
    )
    df = builder.config.db.cursor().execute("select * from psm_test__psm_encounter_covariate").df()

    ed_series = df["example_diagnosis"].value_counts()
    assert ed_series.iloc[0] == neg_set
    assert ed_series.iloc[1] == pos_set
    first_record = df.iloc[0].to_dict()
    assert first_record == expected_first_record
    last_record = df.iloc[neg_set + pos_set - 1].to_dict()
    assert last_record == expected_last_record


@pytest.mark.parametrize("error", [("value"), ("zero")])
@mock.patch("psmpy.PsmPy.logistic_ps")
def test_psm_error_handling(mock_psm, error, tmp_path, mock_db_stats_config):
    match error:
        case "value":
            mock_psm.side_effect = ValueError
        case "zero":
            mock_psm.side_effect = ZeroDivisionError
    builder = cli.StudyRunner(mock_db_stats_config, data_path=pathlib.Path(tmp_path))
    manifest = study_manifest.StudyManifest(
        study_path=f"{pathlib.Path(__file__).parent}/test_data/psm/"
    )
    psmbuilder = psm_builder.PsmBuilder(
        f"{pathlib.Path(__file__).parent}/test_data/psm/psm_config.toml",
        pathlib.Path(tmp_path),
    )
    builder.config.db.cursor().execute(
        "create table psm_test__psm_cohort as (select * from core__condition "
        f"ORDER BY {psmbuilder.config.primary_ref} limit 100)"
    ).df()
    safe_timestamp = (
        datetime.datetime.now()
        .replace(microsecond=0)
        .isoformat()
        .replace(":", "_")
        .replace("-", "_")
    )
    with pytest.raises(SystemExit):
        psmbuilder.execute_queries(
            builder.config,
            manifest,
            drop_table=True,
            table_suffix=safe_timestamp,
        )


def test_psm_missing_file(tmp_path):
    with pytest.raises(SystemExit, match="PSM configuration not found"):
        psm_builder.PsmBuilder(f"{tmp_path}/does-not-exist.toml", pathlib.Path(tmp_path))


def test_psm_missing_keys(tmp_path):
    toml_file = pathlib.Path(f"{tmp_path}/empty.toml")
    toml_file.touch()
    with pytest.raises(SystemExit, match="contains missing/invalid keys"):
        psm_builder.PsmBuilder(str(toml_file), pathlib.Path(tmp_path))


def test_psm_bad_include_cols(tmp_path, mock_db_stats_config):
    """Provide too many include_cols"""
    psm_root = f"{pathlib.Path(__file__).parent}/test_data/psm/"
    with open(f"{tmp_path}/psm.toml", "w", encoding="utf8") as f:
        f.write(f"""config_type = "psm"
classification_json = "{psm_root}/dsm5_classifications.json"
pos_source_table = "psm_test__psm_cohort"
neg_source_table = "core__condition"
target_table = "psm_test__psm_encounter_covariate"
primary_ref = 'encounter_ref'
dependent_variable = "example_diagnosis"
pos_sample_size = 20
neg_sample_size = 100
[join_cols_by_table.core__encounter]
join_id = "encounter_ref"
included_cols = [
    ["race_display", "race", "age_at_visit"],  # too many columns
]
""")
    builder = cli.StudyRunner(mock_db_stats_config, data_path=tmp_path)
    manifest = study_manifest.StudyManifest(study_path=psm_root)
    psmbuilder = psm_builder.PsmBuilder(f"{tmp_path}/psm.toml", tmp_path)
    builder.config.db.cursor().execute(
        "create table psm_test__psm_cohort as (select * from core__condition "
        f"ORDER BY {psmbuilder.config.primary_ref} limit 100)"
    )
    with pytest.raises(SystemExit, match="unexpected SQL column definition"):
        psmbuilder.execute_queries(
            builder.config,
            manifest,
            drop_table=True,
            table_suffix="test",
        )
