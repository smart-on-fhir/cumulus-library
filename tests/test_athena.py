"""Edge case testing for Athena database support"""

import json
import os
import pathlib
from unittest import mock

import botocore

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


@mock.patch.dict(
    os.environ,
    clear=True,
)
@mock.patch("botocore.session.Session")
def test_upload_parquet_response_handling(mock_session):
    path = pathlib.Path(__file__).resolve().parent
    db = databases.AthenaDatabaseBackend(
        region="us-east-1",
        work_group="work_group",
        profile="profile",
        schema_name="db_schema",
    )
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
