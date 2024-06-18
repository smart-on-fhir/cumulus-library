"""Low level database tests

This is intended to exercise edge cases not covered via more integrated testing"""

import datetime
import os
import pathlib
from contextlib import nullcontext as does_not_raise
from unittest import mock

import pandas
import pyarrow
import pytest

from cumulus_library import databases, errors

ATHENA_KWARGS = {
    "region": "test",
    "work_group": "test",
    "profile": "test",
    "schema_name": "test",
}
DUCKDB_KWARGS = {
    "db_file": ":memory:",
}


@mock.patch.dict(
    os.environ,
    clear=True,
)
@pytest.mark.parametrize(
    "db,data,expected,raises",
    [
        (
            databases.AthenaDatabaseBackend(**ATHENA_KWARGS),
            pandas.DataFrame(
                {
                    "str": ["str"],
                    "int": [123],
                    "float": [1.23],
                    "bool": [True],
                    "datetime": [datetime.datetime.now()],
                }
            ),
            ["STRING", "INT", "DOUBLE", "BOOLEAN", "TIMESTAMP"],
            does_not_raise(),
        ),
        (
            databases.DuckDatabaseBackend(**DUCKDB_KWARGS),
            pandas.DataFrame(
                {
                    "str": ["str"],
                    "int": [123],
                    "float": [1.23],
                    "bool": [True],
                    "datetime": [datetime.datetime.now()],
                }
            ),
            [],
            does_not_raise(),
        ),
        (
            databases.AthenaDatabaseBackend(**ATHENA_KWARGS),
            pandas.DataFrame({"cat": pandas.Series(["a"], dtype="category")}),
            ["STRING", "INT", "DOUBLE", "BOOLEAN", "TIMESTAMP"],
            pytest.raises(errors.CumulusLibraryError),
        ),
    ],
)
def test_col_types_from_pandas(db, data, expected, raises):
    with raises:
        vals = db.col_parquet_types_from_pandas(data.dtypes)
        assert set(expected) == set(vals)


@mock.patch.dict(
    os.environ,
    clear=True,
)
@pytest.mark.parametrize(
    "db,data,expected,raises",
    [
        (
            databases.AthenaDatabaseBackend(**ATHENA_KWARGS),
            [
                (
                    "a",
                    "varchar",
                ),
                (
                    "b",
                    "bigint",
                ),
                (
                    "c",
                    "integer",
                ),
                (
                    "d",
                    "double",
                ),
                (
                    "e",
                    "boolean",
                ),
                (
                    "f",
                    "date",
                ),
                ("g", "timestamp"),
            ],
            [
                (
                    "a",
                    pyarrow.string(),
                ),
                (
                    "b",
                    pyarrow.int64(),
                ),
                (
                    "c",
                    pyarrow.int64(),
                ),
                (
                    "d",
                    pyarrow.float64(),
                ),
                (
                    "e",
                    pyarrow.bool_(),
                ),
                (
                    "f",
                    pyarrow.date64(),
                ),
                ("g", pyarrow.timestamp("s")),
            ],
            does_not_raise(),
        ),
        (
            databases.AthenaDatabaseBackend(**ATHENA_KWARGS),
            [("a", "other_type")],
            [],
            pytest.raises(errors.CumulusLibraryError),
        ),
        (
            databases.DuckDatabaseBackend(**DUCKDB_KWARGS),
            [
                (
                    "a",
                    "STRING",
                ),
                (
                    "b",
                    "INTEGER",
                ),
                (
                    "c",
                    "NUMBER",
                ),
                (
                    "d",
                    "DOUBLE",
                ),
                (
                    "e",
                    "boolean",
                ),
                (
                    "f",
                    "Date",
                ),
                ("g", "TIMESTAMP"),
            ],
            [
                (
                    "a",
                    pyarrow.string(),
                ),
                (
                    "b",
                    pyarrow.int64(),
                ),
                (
                    "c",
                    pyarrow.float64(),
                ),
                (
                    "d",
                    pyarrow.float64(),
                ),
                (
                    "e",
                    pyarrow.bool_(),
                ),
                (
                    "f",
                    pyarrow.date64(),
                ),
                ("g", pyarrow.timestamp("s")),
            ],
            does_not_raise(),
        ),
        (
            databases.DuckDatabaseBackend(**DUCKDB_KWARGS),
            [("a", "other_type")],
            [],
            pytest.raises(errors.CumulusLibraryError),
        ),
    ],
)
def test_pyarrow_types_from_sql(db, data, expected, raises):
    with raises:
        vals = db.col_pyarrow_types_from_sql(data)
        assert len(expected) == len(vals)
        for index in range(0, len(vals)):
            assert vals[index][-1] == expected[index][-1]


@mock.patch.dict(
    os.environ,
    clear=True,
)
@pytest.mark.parametrize(
    "args,expected_type, raises",
    [
        (
            {**{"db_type": "duckdb", "schema_name": ":memory:"}, **DUCKDB_KWARGS},
            databases.DuckDatabaseBackend,
            does_not_raise(),
        ),
        (
            {**{"db_type": "athena"}, **ATHENA_KWARGS},
            databases.AthenaDatabaseBackend,
            does_not_raise(),
        ),
        (
            {**{"db_type": "athena", "load_ndjson_dir": "file.json"}, **ATHENA_KWARGS},
            databases.AthenaDatabaseBackend,
            pytest.raises(SystemExit),
        ),
        (
            # https://en.wikipedia.org/wiki/Cornerstone_(software)
            {**{"db_type": "cornerstone", "schema_name": "test"}},
            None,
            pytest.raises(errors.CumulusLibraryError),
        ),
    ],
)
def test_create_db_backend(args, expected_type, raises):
    with raises:
        db = databases.create_db_backend(args)
        assert isinstance(db, expected_type)


def test_upload_file_default():
    db = databases.DuckDatabaseBackend(**DUCKDB_KWARGS)
    location = db.upload_file(
        file=pathlib.Path(__file__).resolve(),
        study="test",
        topic="table",
    )
    assert location is None


@mock.patch.dict(
    os.environ,
    clear=True,
)
@pytest.mark.parametrize(
    "args,sse,keycount,expected,raises",
    [
        (
            {
                "file": pathlib.Path(__file__).resolve(),
                "study": "study",
                "topic": "table",
                "remote_filename": None,
                "force_upload": False,
            },
            "SSE_KMS",
            1,
            "s3://test_bucket/test_location/cumulus_user_uploads/test/study/table",
            does_not_raise(),
        ),
        (
            {
                "file": pathlib.Path(__file__).resolve(),
                "study": "study",
                "topic": "table",
                "remote_filename": None,
                "force_upload": False,
            },
            "SSE_KMS",
            0,
            "s3://test_bucket/test_location/cumulus_user_uploads/test/study/table",
            does_not_raise(),
        ),
        (
            {
                "file": pathlib.Path(__file__).resolve(),
                "study": "study",
                "topic": "table",
                "remote_filename": None,
                "force_upload": False,
            },
            "SSE-S3",
            0,
            "s3://test_bucket/test_location/cumulus_user_uploads/test/study/table",
            pytest.raises(errors.AWSError),
        ),
        (
            {
                "file": pathlib.Path(__file__).resolve(),
                "study": "study",
                "topic": "table",
                "remote_filename": None,
                "force_upload": True,
            },
            "SSE_KMS",
            1,
            "s3://test_bucket/test_location/cumulus_user_uploads/test/study/table",
            does_not_raise(),
        ),
        (
            {
                "file": pathlib.Path(__file__).resolve(),
                "study": "study",
                "topic": "table",
                "remote_filename": "custom.name",
                "force_upload": False,
            },
            "SSE_KMS",
            0,
            "s3://test_bucket/test_location/cumulus_user_uploads/test/study/table",
            does_not_raise(),
        ),
    ],
)
@mock.patch("botocore.client")
def test_upload_file_athena(mock_botocore, args, sse, keycount, expected, raises):
    mock_data = {
        "WorkGroup": {
            "Configuration": {
                "ResultConfiguration": {
                    "OutputLocation": "s3://test_bucket/test_location/",
                    "EncryptionConfiguration": {"EncryptionOption": sse},
                }
            }
        }
    }
    mock_clientobj = mock_botocore.ClientCreator.return_value.create_client.return_value
    mock_clientobj.get_work_group.return_value = mock_data
    mock_clientobj.list_objects_v2.return_value = {"KeyCount": keycount}
    db = databases.AthenaDatabaseBackend(**ATHENA_KWARGS)
    with raises:
        location = db.upload_file(**args)
        assert location == expected
        if keycount == 0 or args["force_upload"]:
            assert mock_clientobj.put_object.called
            kwargs = mock_clientobj.put_object.call_args_list[0][1]
            if args["remote_filename"]:
                assert kwargs["Key"].endswith(args["remote_filename"])
            else:
                assert kwargs["Key"].endswith(args["file"].name)
