"""Edge case testing for Athena database support"""

import json
import pathlib
import time
from concurrent import futures
from unittest import mock

import awswrangler
import botocore
import pandas
import pyathena
import pytest

from cumulus_library import base_utils, databases, study_manifest


def test_schema_parsing():
    # A sample response for table `observation`, column `component`.
    # (The original did not have any spaces.)
    schema = {
        "simple_field": "varchar",
        "simple_row": "row(id varchar, end varchar, start varchar)",
        "simple_array": "array(varchar)",
        "complex": """array(row(
            coding array(row(code varchar, display row(text varchar),
                             system varchar, userselected boolean, id varchar,
                             version varchar)),
            text varchar,
            id varchar
        ))""",
    }
    expected = {
        "simple_field": {},
        "simple_row": {"id": {}, "end": {}, "start": {}},
        "simple_array": {},
        "complex": {
            "coding": {
                "code": {},
                "display": {"text": {}},
                "system": {},
                "userselected": {},
                "id": {},
                "version": {},
            },
            "text": {},
            "id": {},
        },
    }
    parser = databases.AthenaParser()
    assert expected == parser.parse_found_schema(schema)


@mock.patch("botocore.session.Session")
def test_upload_parquet_response_handling(mock_session):
    path = pathlib.Path(__file__).resolve().parents[1]
    db = databases.AthenaDatabaseBackend(
        region="us-east-1",
        work_group="work_group",
        profile="profile",
        schema_name="db_schema",
    )
    db.connect()
    client = mock.MagicMock()
    with open(path / "test_data/aws/boto3.client.athena.get_work_group.json") as f:
        client.get_work_group.return_value = json.load(f)
    db.connection._client = client
    s3_client = mock.MagicMock()
    with open(path / "test_data/aws/boto3.client.s3.list_objects_v2.json") as f:
        s3_client.list_objects_v2.return_value = json.load(f)
    mock_session.return_value.create_client.return_value = s3_client
    resp = db.upload_file(
        file=path / "test_data/count_synthea_patient.parquet",
        study="test_study",
        topic="count_patient",
        remote_filename="count_synthea_patient.parquet",
    )
    assert resp == (
        "s3://cumulus-athena-123456789012-us-east-1/results/cumulus_user_uploads/db_schema/test_study/count_patient"
    )


@mock.patch("botocore.client")
def test_create_schema(mock_client):
    mock_clientobj = mock_client.ClientCreator.return_value.create_client.return_value
    mock_clientobj.get_database.side_effect = [
        None,
        botocore.exceptions.ClientError({}, {}),
    ]
    db = databases.AthenaDatabaseBackend(
        region="test",
        work_group="test",
        profile="test",
        schema_name="test",
    )
    db.create_schema("test_exists")
    assert mock_clientobj.get_database.called
    assert not mock_clientobj.create_database.called

    db.create_schema("test_new")
    assert mock_clientobj.create_database.called


def test_dedicated_schema_namespacing(tmp_path):
    with open(f"{tmp_path}/manifest.toml", "w", encoding="utf8") as f:
        f.write('study_prefix="foo"\n')
        f.write("[advanced_options]\n")
        f.write('dedicated_schema="foo"\n')
    manifest = study_manifest.StudyManifest(tmp_path)
    query = "CREATE TABLE foo__bar"
    result = base_utils.update_query_if_schema_specified(query, manifest)
    assert result == "CREATE TABLE foo.bar"
    query = "CREATE EXTERNAL TABLE foo.foo__bar"
    result = base_utils.update_query_if_schema_specified(query, manifest)
    assert result == "CREATE EXTERNAL TABLE foo.bar"


@mock.patch("botocore.client")
@mock.patch("awswrangler.s3")
def test_export_table(mock_wrangler, mock_client, tmp_path):
    db = databases.AthenaDatabaseBackend(
        region="test",
        work_group="test",
        profile="test",
        schema_name="test",
    )
    db.connection = mock.MagicMock()
    bucket_info = {
        "WorkGroup": {
            "Configuration": {"ResultConfiguration": {"OutputLocation": "s3://testbucket/athena"}}
        }
    }
    db.connection._client.get_work_group.side_effect = [bucket_info, bucket_info]
    mock_clientobj = mock_client.ClientCreator.return_value.create_client.return_value
    mock_clientobj.list_objects_v2.side_effect = [
        # first pass: delete found file and then cleanup
        {"Contents": [{"Key": "export/file_to_delete"}]},
        {"Contents": [{"Key": "export/table.flat.parquet"}]},
        # second pass: skip deletion
        {},
    ]
    # file found
    mock_wrangler.read_parquet.return_value = pandas.DataFrame({"A": [1, 2], "B": ["x", "y"]})
    res = db.export_table_as_parquet("table", "flat", tmp_path)
    assert res is True
    assert mock_clientobj.delete_object.call_args[1]["Key"] == "export/table.flat.parquet"

    # file not found
    mock_wrangler.read_parquet.side_effect = awswrangler.exceptions.NoFilesFound
    res = db.export_table_as_parquet("table", "flat", tmp_path)
    assert res is False


@mock.patch("cumulus_library.databases.athena.AthenaDatabaseBackend.async_cursor")
def test_parallel_write(mock_cursor_getter):
    db = databases.AthenaDatabaseBackend(
        region="test",
        work_group="test",
        profile="test",
        schema_name="test",
    )
    mock_cursor = mock.MagicMock()
    mock_cursor_getter.return_value = mock_cursor
    queries = []
    cursor_returns = []

    def mock_query_run():
        time.sleep(0.25)

    with pytest.raises(SystemExit):
        with futures.ThreadPoolExecutor(max_workers=5) as executor:
            for i in range(0, 20):
                queries.append(f"select {i} from foo")
                cursor_returns.append((i, executor.submit(mock_query_run)))
                mock_cursor.execute.side_effect = cursor_returns
            with base_utils.get_progress_bar() as progress_bar:
                task = progress_bar.add_task(
                    "test queries",
                    total=len(queries),
                    visible=True,
                )
                db.parallel_write(queries, False, progress_bar, task)
        assert len(mock_cursor.execute.call_args_list) == 20
        assert mock_cursor.execute.call_args_list[0][0][0] == "select 0 from foo"
        assert mock_cursor.execute.call_args_list[19][0][0] == "select 19 from foo"


@mock.patch("botocore.client")
def test_get_async_cursor(mock_client):
    db = databases.AthenaDatabaseBackend(
        region="test",
        work_group="test",
        profile="test",
        schema_name="test",
    )
    db.connect()
    cursor = db.async_cursor()
    assert isinstance(cursor, pyathena.async_cursor.AsyncCursor)
