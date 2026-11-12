import pathlib
import shutil

from cumulus_library import study_manifest
from cumulus_library.actions import builder

TEST_DATA_PATH = pathlib.Path(__file__).parents[1] / "test_data/iqr"


def test_iqr_builder_run(mock_db_core_config, tmp_path):
    shutil.copytree(TEST_DATA_PATH, tmp_path / "iqr", dirs_exist_ok=True)

    # let's insert an outlier into the core__patient table
    mock_db_core_config.db.connection.execute(
        "INSERT INTO core__patient(id, birth_year) VALUES ('12a',1000)"
    )
    manifest = study_manifest.StudyManifest(study_path=tmp_path / "iqr")
    builder.run_protected_table_builder(config=mock_db_core_config, manifest=manifest)
    builder.build_study(config=mock_db_core_config, manifest=manifest)
    conn = mock_db_core_config.db.connection
    result = conn.execute("SELECT * FROM iqr_test__test_distribution")
    assert [x[0] for x in result.description] == [
        "minimum",
        "median",
        "maximum",
        "q1",
        "q3",
        "iqr",
        "lower_whisker",
        "upper_whisker",
    ]
    # Just a note about what behavior we've validated here:
    # - The query has run
    # - The IQR calculation is correct (2002-1963 = 39)
    # - The 'find the nearest point inside the bound * 1.5' is correct,
    #   since 1918 is the lowest value before we insert our outlier, and
    #   it's the lowest value about the bound * 1.5 (1907)
    assert result.fetchone() == (1000, 1996, 2018, 1963, 2002, 39, 1918, 2018)
