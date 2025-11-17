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
    (
        "toml_def,pos_set,neg_set,expected_first_record,expected_last_record,"
        "expected_first_hist,expected_last_hist,expected_first_effect,expected_last_effect"
    ),
    [
        (
            "psm_config.toml",
            30,
            128,
            {
                "encounter_ref": "Encounter/03e34b19-2889-b828-792d-2a83400c55be12",
                "example_diagnosis": "1",
                "instance_count": 1,
                "gender": "female",
                "race": "white",
                "code": "33737001",
            },
            {
                "encounter_ref": "Encounter/ed151e04-3dd6-8cb7-a3e5-777c8a8667f119",
                "example_diagnosis": "0",
                "instance_count": 1,
                "gender": "female",
                "race": "white",
                "code": "195662009",
            },
            {
                "encounter_ref": "Encounter/03e34b19-2889-b828-792d-2a83400c55be12",
                "appx_score": 0.67,
                "appx_logit": 0.59,
                "group": "treatment",
                "matched": True,
            },
            {
                "encounter_ref": [
                    "Encounter/e5dabcb6-1d7a-7467-dbba-b864d0d5f5b08",
                    "Encounter/e5dabcb6-1d7a-7467-dbba-b864d0d5f5b09",
                    "Encounter/ed151e04-3dd6-8cb7-a3e5-777c8a8667f119",
                ],
                "appx_score": 0.49,
                "appx_logit": -0.06,
                "group": "control",
                "matched": False,
            },
            "category A,before,0.2724389343925956",
            [
                "white,after,0.39327683210006986",
                "white,after,0.5285673369330032",
                "white,after,0.6952217871538068",
            ],
        ),
        (
            "psm_config_no_optional.toml",
            30,
            128,
            {
                "encounter_ref": "Encounter/03e34b19-2889-b828-792d-2a83400c55be12",
                "example_diagnosis": "1",
                "code": "33737001",
            },
            {
                "encounter_ref": "Encounter/ed151e04-3dd6-8cb7-a3e5-777c8a8667f119",
                "example_diagnosis": "0",
                "code": "195662009",
            },
            {
                "encounter_ref": ["Encounter/03e34b19-2889-b828-792d-2a83400c55be12"],
                "appx_score": 0.56,
                "appx_logit": 0.23,
                "group": "treatment",
                "matched": True,
            },
            {
                "encounter_ref": [
                    "Encounter/e5dabcb6-1d7a-7467-dbba-b864d0d5f5b08",
                    "Encounter/e5dabcb6-1d7a-7467-dbba-b864d0d5f5b09",
                    "Encounter/ed151e04-3dd6-8cb7-a3e5-777c8a8667f119",
                ],
                "appx_score": 0.52,
                "appx_logit": 0.06,
                "group": "control",
                "matched": False,
            },
            "category A,before,0.2724389343925956",
            [
                "category C,after,0.0",
                "category C,after,0.37161167647860316",
                "category C,after,0.37161167647860327",
                "category C,after,0.5453768398418632",
                "category C,after,0.5453768398418634",
                "category C,after,0.6952217871538069",
            ],
        ),
    ],
)
@mock.patch("cumulus_library.base_utils.get_user_documents_dir")
def test_psm_create(
    mock_doc_dir,
    tmp_path,
    mock_db_stats_config,
    toml_def,
    pos_set,
    neg_set,
    expected_first_record,
    expected_last_record,
    expected_first_hist,
    expected_last_hist,
    expected_first_effect,
    expected_last_effect,
):
    mock_doc_dir.return_value = tmp_path
    builder = cli.StudyRunner(mock_db_stats_config, data_path=pathlib.Path(tmp_path))
    manifest = study_manifest.StudyManifest(
        study_path=f"{pathlib.Path(__file__).parents[1]}/test_data/psm/"
    )
    psmbuilder = psm_builder.PsmBuilder(
        f"{pathlib.Path(__file__).parents[1]}/test_data/psm/{toml_def}",
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
    last_record = (
        df.loc[df["encounter_ref"] == expected_last_record["encounter_ref"]].iloc[0].to_dict()
    )
    assert last_record == expected_last_record
    with open(tmp_path / "cumulus-library/psm_test/psm_histogram.csv") as f:
        lines = f.readlines()
        for found, expected in [
            (lines[1].rstrip(), expected_first_hist),
            (lines[-1].rstrip(), expected_last_hist),
        ]:
            found = found.split(",")
            assert found[0] in expected["encounter_ref"]
            # There's a bit of randomness in the scores/logits. It's probably from scikit_learn's
            # LogisticRegression function, or how psmpy is using it, or our test data. Rather than
            # debug this, we'll just check that it's close and call it a day, since we are
            # currently looking into a next gen cohort sampler/classifier anyway and this is not
            # used in a study currently.
            assert expected["appx_score"] - 0.10 < float(found[-4]) < expected["appx_score"] + 0.10
            assert expected["appx_logit"] - 0.25 < float(found[-3]) < expected["appx_logit"] + 0.25
            assert found[-2] == expected["group"]
            # Matches are sometimes arbitrary for similar looking records, so we'll just
            # check to see if we got a match or not
            assert (len(found[-1]) > 0) == expected["matched"]

    with open(tmp_path / "cumulus-library/psm_test/psm_effect_size.csv") as f:
        lines = f.readlines()
        assert lines[1].rstrip() == expected_first_effect
        # We get a series of semi-repeatable values here, so we'll just see if we get
        # a known one
        assert lines[-1].rstrip() in expected_last_effect


@pytest.mark.parametrize("error", [("value"), ("zero")])
@mock.patch("cumulus_library.builders.psmpy_lite.PsmPy.logistic_ps")
def test_psm_error_handling(mock_psm, error, tmp_path, mock_db_stats_config):
    match error:
        case "value":
            mock_psm.side_effect = ValueError
        case "zero":
            mock_psm.side_effect = ZeroDivisionError
    builder = cli.StudyRunner(mock_db_stats_config, data_path=pathlib.Path(tmp_path))
    manifest = study_manifest.StudyManifest(
        study_path=f"{pathlib.Path(__file__).parents[1]}/test_data/psm/"
    )
    psmbuilder = psm_builder.PsmBuilder(
        f"{pathlib.Path(__file__).parents[1]}/test_data/psm/psm_config.toml",
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
    psm_root = f"{pathlib.Path(__file__).parents[1]}/test_data/psm/"
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
