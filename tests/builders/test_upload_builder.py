import csv
import pathlib
import shutil
from unittest import mock

import pandas
import pytest

from cumulus_library import base_utils, databases, errors, study_manifest
from cumulus_library.actions import builder
from cumulus_library.builders import file_upload_builder
from tests import conftest

TEST_DATA_PATH = pathlib.Path(__file__).parents[1] / "test_data/file_upload"
REF_DF = pandas.DataFrame(data={"a": ["1", "4"], "b": ["2", "5"], "c": ["3", "6"]})


# pandas only officially supports SQLAlchemy, but the pep-compliant DuckDB
# connection works fine
@pytest.mark.filterwarnings("ignore:pandas")
@mock.patch("platformdirs.user_cache_dir")
def test_upload_builder_run(mock_cache, mock_db_config, tmp_path):
    mock_cache.return_value = tmp_path / "cache"
    shutil.copytree(TEST_DATA_PATH, tmp_path / "file_upload", dirs_exist_ok=True)
    manifest = study_manifest.StudyManifest(study_path=tmp_path / "file_upload")
    builder.run_protected_table_builder(config=mock_db_config, manifest=manifest)
    builder.build_study(config=mock_db_config, manifest=manifest)
    conn = mock_db_config.db.connection
    tables = conn.execute("SELECT table_name FROM information_schema.tables").fetchall()
    for file_format in ["bsv", "csv", "parquet", "tsv", "xslx", "table_a", "table_b"]:
        assert (f"file_upload__{file_format}",) in tables
        df = pandas.read_sql(f"SELECT * FROM file_upload__{file_format}", conn)
        assert df.equals(REF_DF)
    df = pandas.read_sql("SELECT * FROM file_upload__single_multi_files order by a", conn)
    assert df.equals(
        pandas.DataFrame(data={"a": ["1", "4", "7"], "b": ["2", "5", "8"], "c": ["3", "6", "9"]})
    )
    # Just validating that we're not writing to the same destination for each parquet conversion
    paths = [x.name for x in (tmp_path / "cache/file_uploads").glob("**/*.parquet")]
    for name in [
        "upload_bars.parquet",
        "upload_commas.parquet",
        "upload_commas_part2.parquet",
        "upload_excel.parquet",
        "upload_tabs.parquet",
    ]:
        assert name in paths


def test_snake_case():
    for name in ["snake_case", "SnakeCase", "SNAKE_CASE"]:
        assert "snake_case" == file_upload_builder.FileUploadBuilder._snake_case(name)


@mock.patch("platformdirs.user_cache_dir")
def test_unsupported_filetype(mock_cache, mock_db_config, tmp_path):
    mock_cache.return_value = tmp_path / "cache"
    shutil.copytree(TEST_DATA_PATH, tmp_path / "file_upload", dirs_exist_ok=True)

    manifest = study_manifest.StudyManifest(tmp_path / "file_upload")
    conftest.write_toml(
        tmp_path / "file_upload/",
        {
            "config_type": "file_upload",
            "tables": {"bad_type": {"file": "upload.pdf"}},
        },
        filename="workflow.toml",
    )
    with pytest.raises(errors.FileUploadError):
        builder = file_upload_builder.FileUploadBuilder(
            toml_config_path=tmp_path / "file_upload/workflow.toml",
        )
        builder.prepare_queries(config=mock_db_config, manifest=manifest)


@mock.patch("platformdirs.user_cache_dir")
def test_wrong_cols_filetype(mock_cache, mock_db_config, tmp_path):
    mock_cache.return_value = tmp_path / "cache"
    shutil.copytree(TEST_DATA_PATH, tmp_path / "file_upload", dirs_exist_ok=True)

    manifest = study_manifest.StudyManifest(tmp_path / "file_upload")
    conftest.write_toml(
        tmp_path / "file_upload/",
        {
            "config_type": "file_upload",
            "tables": {"bad_cols": {"files": ["upload_commas.csv"], "col_types": ["STRING"]}},
        },
        filename="workflow.toml",
    )

    with pytest.raises(errors.FileUploadError):
        builder = file_upload_builder.FileUploadBuilder(
            toml_config_path=tmp_path / "file_upload/workflow.toml",
        )
        builder.prepare_queries(config=mock_db_config, manifest=manifest)


@mock.patch("platformdirs.user_cache_dir")
def test_multifile_mismatch(mock_cache, mock_db_config, tmp_path):
    mock_cache.return_value = tmp_path / "cache"
    shutil.copytree(TEST_DATA_PATH, tmp_path / "file_upload", dirs_exist_ok=True)

    manifest = study_manifest.StudyManifest(tmp_path / "file_upload")
    with open(tmp_path / "file_upload/upload_commas_part2.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "a",
                "b",
                "c",
            ]
        )
        writer.writerow(
            [
                7,
                "8",
                "xyz",
            ]
        )
    with pytest.raises(errors.FileUploadError, match="does not match the schema"):
        builder = file_upload_builder.FileUploadBuilder(
            toml_config_path=tmp_path / "file_upload/workflow.toml",
        )
        builder.prepare_queries(config=mock_db_config, manifest=manifest)


@mock.patch("cumulus_library.template_sql.base_templates.get_ctas_from_parquet_query")
@mock.patch("platformdirs.user_cache_dir")
@mock.patch("botocore.client")
@mock.patch("botocore.session")
def test_athena_pathing(mock_session, mock_client, mock_cache, mock_template, tmp_path):
    """We're testing the Athena calls more holistically elsewhere; all we're
    checking for with this test is that we are correctly setting the remote
    data path.
    """
    db = databases.AthenaDatabaseBackend(
        region="test",
        work_group="test",
        profile="test",
        schema_name="test",
    )
    db.connection = mock.MagicMock()
    bucket_info = {
        "WorkGroup": {
            "Configuration": {
                "ResultConfiguration": {
                    "OutputLocation": "s3://testbucket/athena/",
                    "EncryptionConfiguration": {"EncryptionOption": "aws:kms"},
                }
            }
        }
    }
    # the workgroup response is checked once per call, so we'll add a number of
    # mock side effects for it
    mock_wg_responses = []
    mock_wg_responses.extend([bucket_info] * 10)
    db.connection._client.get_work_group.side_effect = mock_wg_responses
    mock_cache.return_value = tmp_path / "cache"
    shutil.copytree(TEST_DATA_PATH, tmp_path / "file_upload", dirs_exist_ok=True)
    manifest = study_manifest.StudyManifest(study_path=tmp_path / "file_upload")
    config = base_utils.StudyConfig(db=db, schema="main", force_upload=True)
    builder = file_upload_builder.FileUploadBuilder(
        toml_config_path=tmp_path / "file_upload/workflow.toml"
    )
    builder.prepare_queries(config=config, manifest=manifest)
    remote_paths = {}
    for call in mock_template.call_args_list:
        remote_paths[call[1]["table_name"]] = call[1]["remote_location"]
    for source in [
        "bsv",
        "csv",
        "parquet",
        "tsv",
        "xslx",
        "table_a",
        "single_multi_files",
        "table_b",
    ]:
        assert remote_paths[f"file_upload__{source}"] == (
            f"s3://testbucket/athena/cumulus_user_uploads/test/file_upload/{source}"
        )
