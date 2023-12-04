from pathlib import Path

from cumulus_library.cli import StudyBuilder
from cumulus_library.statistics.psm import PsmBuilder


def test_psm_create(mock_db_core):
    builder = StudyBuilder(mock_db_core)
    psm = PsmBuilder(f"{Path(__file__).parent}/test_data/psm/psm_config.toml")
    mock_db_core.cursor().execute(
        f"create table core__psm_cohort as (select * from core__condition ORDER BY {psm.config.primary_ref} limit 10)"
    ).df()
    cohort = mock_db_core.cursor().execute("select * from core__psm_cohort").fetchall()

    psm.execute_queries(
        mock_db_core.pandas_cursor(), builder.schema_name, False, drop_table=True
    )
    df = (
        mock_db_core.cursor()
        .execute("select * from core__psm_encounter_covariate")
        .df()
    )
    ed_series = df["example_diagnosis"].value_counts()
    assert ed_series.iloc[0] == 7
    assert ed_series.iloc[1] == 7
    first_record = df.iloc[0].to_dict()
    assert first_record == {
        "encounter_ref": "Encounter/11381dc6-0e06-da55-0735-d1e7bbf8bb35",
        "example_diagnosis": "1",
        "instance_count": 1,
        "gender": "male",
        "race": "white",
        "code": "44465007",
    }
    last_record = df.iloc[13].to_dict()
    assert last_record == {
        "encounter_ref": "Encounter/ed151e04-3dd6-8cb7-a3e5-777c8a8667f1",
        "example_diagnosis": "0",
        "instance_count": 1,
        "gender": "female",
        "race": "white",
        "code": "195662009",
    }
