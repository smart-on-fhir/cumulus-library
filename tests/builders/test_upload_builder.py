import pathlib
import shutil
from unittest import mock

import pandas
import pytest

from cumulus_library import errors, study_manifest
from cumulus_library.actions import builder
from cumulus_library.builders import file_upload_builder

TEST_DATA_PATH = pathlib.Path(__file__).parents[1] / "test_data/file_upload"
REF_DF = pandas.DataFrame(data={"a": [1, 4], "b": [2, 5], "c": [3, 6]})


# pandas only officially supports SQLAlchemy, but the pep-compliant DuckDB
# connection works fine
@pytest.mark.filterwarnings("ignore:pandas")
@mock.patch("platformdirs.user_cache_dir")
def test_upload_builder(mock_cache, mock_db_config, tmp_path):
    mock_cache.return_value = tmp_path / "cache"
    shutil.copytree(TEST_DATA_PATH, tmp_path / "file_upload", dirs_exist_ok=True)
    manifest = study_manifest.StudyManifest(study_path=tmp_path / "file_upload")
    builder.run_protected_table_builder(config=mock_db_config, manifest=manifest)
    builder.build_study(config=mock_db_config, manifest=manifest)
    conn = mock_db_config.db.connection
    tables = conn.execute("SELECT table_name FROM information_schema.tables").fetchall()
    for file_format in ["bsv", "csv", "parquet", "tsv", "xslx"]:
        assert (f"file_upload__{file_format}",) in tables
        df = pandas.read_sql(f"SELECT * FROM file_upload__{file_format}", conn)
        assert df.equals(REF_DF)
    # Just validating that we're not writing to the same destination for each parquet conversion
    paths = [x.name for x in (tmp_path / "cache/file_uploads/file_upload").iterdir()]
    for name in [
        "upload_bars.parquet",
        "upload_commas.parquet",
        "upload_excel.parquet",
        "upload_tabs.parquet",
    ]:
        assert name in paths


def test_snake_case():
    for name in ["snake_case", "SnakeCase", "SNAKE_CASE"]:
        assert "snake_case" == file_upload_builder.FileUploadBuilder.snake_case(name)


@mock.patch("cumulus_library.study_manifest.StudyManifest")
def test_unsupported_filetype(mock_manifest, mock_db_config, tmp_path):
    manifest = study_manifest.StudyManifest()
    with pytest.raises(errors.FileUploadError):
        builder = file_upload_builder.FileUploadBuilder(
            toml_config_path=tmp_path,
            workflow_config={"tables": {"bad_type": {"file": "upload.pdf"}}},
        )
        builder.prepare_queries(config=mock_db_config, manifest=manifest)
