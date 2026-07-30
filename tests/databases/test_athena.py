"""Edge case testing for Athena database support"""

import base64
import hashlib
import io
import json
import os
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
from tests import conftest


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

    with open(path / "test_data/upload/upload__count_synthea_patient.cube.parquet", "rb") as f:
        s3_client.get_object.return_value = {"Body": io.BytesIO(f.read())}

    mock_session.return_value.create_client.return_value = s3_client
    resp = db.upload_file(
        file=path / "test_data/upload/upload__count_synthea_patient.cube.parquet",
        study="test_study",
        topic="count_patient",
        remote_filename="count_synthea_patient.cube.parquet",
    )
    assert resp == (
        "s3://cumulus-athena-123456789012-us-east-1/results/cumulus_user_uploads/db_schema/test_study/count_patient"
    )


@pytest.mark.parametrize(
    (
        "force_upload",
        "list_objects_result",
        "remote_bytes",
        "local_bytes",
        "expected_head_object_call_count",
        "expected_put_object_call_count",
    ),
    [
        pytest.param(
            False,
            {"KeyCount": 1},
            b"same-content",
            b"same-content",
            1,
            0,
            id="checksums-match-does-not-call-put-object",
        ),
        pytest.param(
            False,
            {"KeyCount": 1},
            b"new-content",
            b"old-content",
            1,
            1,
            id="checksums-do-not-match-calls-put-object",
        ),
        pytest.param(
            False,
            {"KeyCount": 1},
            "",
            b"new-content",
            1,
            1,
            id="remote-checksum-missing-calls-put-object",
        ),
        pytest.param(
            True,
            {"KeyCount": 1},
            b"same-content",
            b"same-content",
            0,
            1,
            id="force-upload-does-not-call-head-object",
        ),
    ],
)
@mock.patch("botocore.session.Session")
def test_upload_file_behavior(
    mock_session,
    tmp_path,
    force_upload,
    list_objects_result,
    remote_bytes,
    local_bytes,
    expected_head_object_call_count,
    expected_put_object_call_count,
):
    path = pathlib.Path(__file__).resolve().parents[1]
    local_file = tmp_path / "upload_file_behavior.csv"
    local_file.write_bytes(local_bytes)

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
    s3_client.list_objects_v2.return_value = list_objects_result

    s3_client.head_object.return_value = {}
    if remote_bytes:
        s3_client.head_object.return_value = (
            {
                "ChecksumSHA256": base64.b64encode(
                    hashlib.sha256(remote_bytes, usedforsecurity=False).digest()
                ).decode("utf-8")
            },
        )

    mock_session.return_value.create_client.return_value = s3_client

    resp = db.upload_file(
        file=local_file,
        study="test_study",
        topic="upload_file_behavior",
        remote_filename="upload_file_behavior.csv",
        force_upload=force_upload,
    )

    assert resp == (
        "s3://cumulus-athena-123456789012-us-east-1/results/"
        "cumulus_user_uploads/db_schema/test_study/upload_file_behavior"
    )

    s3_client.put_object.call_count = expected_put_object_call_count
    s3_client.put_object.call_count = expected_head_object_call_count


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
    manifest_dict = {
        "study_prefix": "foo",
        "stages": {
            "stage_1": [
                {
                    "type": "build:serial",
                    "description": "action 1",
                    "files": ["foo", "bar"],
                },
            ],
        },
        "advanced_options": {"dedicated_schema": "foo"},
    }
    conftest.write_toml(tmp_path, manifest_dict)
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


@mock.patch("cumulus_library.databases.base.ParallelResult")
@mock.patch("cumulus_library.databases.athena.AthenaDatabaseBackend.async_cursor")
def test_parallel_execute(mock_cursor_getter, mock_result):
    db = databases.AthenaDatabaseBackend(
        region="test",
        work_group="test",
        profile="test",
        schema_name="test",
    )
    mock_cursor = mock.MagicMock()
    mock_cursor_getter.return_value = mock_cursor
    queries = []
    future_mock = mock.MagicMock()
    cursor_returns = [(None, future_mock), (None, future_mock), (None, future_mock)]

    def mock_query_run():
        time.sleep(0.25)

    with futures.ThreadPoolExecutor(max_workers=5) as executor:
        for i in range(0, 3):
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
    assert len(mock_result.call_args_list) == 3
    assert mock_result.call_args_list[0][1]["query"] == "select 0 from foo"
    assert mock_result.call_args_list[2][1]["query"] == "select 2 from foo"
    assert len(mock_cursor.execute.call_args_list) == 3
    assert mock_cursor.execute.call_args_list[0][0][0] == "select 0 from foo"
    assert mock_cursor.execute.call_args_list[2][0][0] == "select 2 from foo"


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


@mock.patch("botocore.client")
def test_get_remote_path(mock_client):
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
    assert db.get_remote_path() == "s3://testbucket/athena"


@mock.patch.dict(
    os.environ,
    clear=True,
)
@mock.patch("botocore.session")
def test_boto_fallback(mock_session):
    mock_session.get_session.return_value.get_credentials.return_value = (
        botocore.credentials.Credentials(access_key="access", secret_key="secret", token="token")
    )
    db = databases.AthenaDatabaseBackend(
        region="test",
        work_group="test",
        profile="test",
        schema_name="test",
    )
    db.connect()
    assert db.connect_kwargs == {
        "aws_access_key_id": "access",
        "aws_secret_access_key": "secret",
        "aws_session_token": "token",
    }
