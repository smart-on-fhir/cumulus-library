"""Tests for Athena database support"""
import json
import os
import pathlib
from unittest import mock

from cumulus_library import databases, db_config


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
def test_upload_parquet(s3_client_mock):
    path = pathlib.Path(__file__).resolve().parent
    db_config.db_type = "athena"
    cursor = mock.MagicMock()
    cursor._schema_name = "db_schema"
    cursor.connection.work_group = "workgroup"
    cursor.connection.profile_name = "profile"
    with open(path / "test_data/aws/boto3.client.athena.get_work_group.json") as f:
        cursor.connection._client.get_work_group.return_value = json.load(f)
    s3_client = mock.MagicMock()
    with open(path / "test_data/aws/boto3.client.s3.put_object.json") as f:
        s3_client.put_object.return_value = json.load(f)
    s3_client_mock.patch("botocore.client.S3", return_value=s3_client)
    resp = databases.upload_file(
        cursor=cursor,
        file=path / "test_data/count_synthea_patient.parquet",
        study="test_study",
        topic="count_patient",
        remote_filename="count_synthea_patient.parquet",
    )
    assert resp == (
        "s3://cumulus-athena-123456789012-us-east-1/results/cumulus_user_uploads/db_schema/test_study/count_patient"
    )
