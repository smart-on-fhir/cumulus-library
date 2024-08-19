"""Low level database tests

This is intended to exercise edge cases not covered via more integrated testing"""

import datetime
import os
import pathlib
from contextlib import nullcontext as does_not_raise
from unittest import mock

import duckdb
import pandas
import pyarrow
import pyathena
import pytest

from cumulus_library import databases, errors
from cumulus_library.template_sql import sql_utils

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
            {**{"db_type": "duckdb", "database": ":memory:"}, **DUCKDB_KWARGS},
            databases.DuckDatabaseBackend,
            does_not_raise(),
        ),
        (
            {**{"db_type": "athena"}, **ATHENA_KWARGS},
            databases.AthenaDatabaseBackend,
            does_not_raise(),
        ),
        (
            {**{"db_type": "athena", "database": "test"}, **ATHENA_KWARGS},
            databases.AthenaDatabaseBackend,
            does_not_raise(),
        ),
        (
            {**{"db_type": "athena", "database": "testtwo"}, **ATHENA_KWARGS},
            databases.AthenaDatabaseBackend,
            pytest.raises(SystemExit),
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
        db, schema = databases.create_db_backend(args)
        assert isinstance(db, expected_type)
        if args.get("schema_name"):
            assert args["schema_name"] == schema


### Database-specific edge case testing


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


@mock.patch.dict(
    os.environ,
    clear=True,
)
@mock.patch("pyathena.connect")
def test_athena_pandas_cursor(mock_pyathena):
    mock_as_pandas = mock.MagicMock()
    mock_as_pandas.as_pandas.side_effect = [
        pandas.DataFrame(
            [
                [
                    1,
                    2,
                ],
                [3, 4],
                [5, 6],
            ]
        )
    ]
    mock_execute = mock_pyathena.return_value.cursor.return_value.execute
    mock_execute.side_effect = [
        mock_as_pandas,
    ]
    mock_execute.description = (
        (None, "A", None, None, None),
        (None, "B", None, None, None),
    )
    db = databases.AthenaDatabaseBackend(**ATHENA_KWARGS)
    res, desc = db.execute_as_pandas("ignored query")
    assert res.equals(
        pandas.DataFrame(
            [
                [1, 2],
                [3, 4],
                [5, 6],
            ]
        )
    )


@mock.patch.dict(
    os.environ,
    clear=True,
)
@mock.patch("pyathena.connect")
def test_athena_parser(mock_pyathena):
    db = databases.AthenaDatabaseBackend(**ATHENA_KWARGS)
    parser = db.parser()
    assert isinstance(parser, databases.AthenaParser)


@mock.patch.dict(
    os.environ,
    clear=True,
)
@mock.patch("pyathena.connect")
def test_athena_env_var_priority(mock_pyathena):
    os.environ["AWS_ACCESS_KEY_ID"] = "secret"
    databases.AthenaDatabaseBackend(**ATHENA_KWARGS)
    assert mock_pyathena.call_args[1]["aws_access_key_id"] == "secret"


def test_upload_file_duckdb():
    db = databases.DuckDatabaseBackend(**DUCKDB_KWARGS)
    location = db.upload_file(
        file=pathlib.Path(__file__).resolve(),
        study="test",
        topic="table",
    )
    assert location is None


def test_duckdb_pandas(mock_db):
    # pandas_cursor is treated as equivalent to a regular cursor in duckdb
    assert mock_db.cursor() == mock_db.pandas_cursor()
    # for execute_as_pandas, we are specifically checking that duckdb ignores the
    # chunksize param, but does return an iterable object for compatibility
    query = "SELECT * FROM condition"
    df, all_cols = mock_db.execute_as_pandas(query, chunksize=None)
    assert len(df.index) == 20
    df_iter, chunk_cols = mock_db.execute_as_pandas(query, chunksize=10)
    assert len(next(df_iter).index) == 20
    assert all_cols == chunk_cols


### duckdb user defined functions


def test_duckdb_to_utf8(mock_db):
    cursor = mock_db.cursor()
    hash_val = cursor.execute(
        "SELECT md5(to_utf8(table_name)) FROM information_schema.tables"
    ).fetchone()
    assert hash_val == ("47a83502f34b0d2155e1b22b19fb8431",)


@pytest.mark.parametrize(
    "data,field_type,raises",
    [
        ("2000-01-01", "VARCHAR", does_not_raise()),
        ("2000-01-01", "DATE", does_not_raise()),
        ("2000-01-01", "DATETIME", does_not_raise()),
        ("True", "BOOLEAN", pytest.raises(duckdb.duckdb.InvalidInputException)),
    ],
)
def test_duckdb_date(mock_db, data, field_type, raises):
    with raises:
        cursor = mock_db.cursor()
        field = cursor.execute(f"SELECT date(CAST('{data}' AS {field_type})) AS field").fetchall()
        assert field == [(datetime.date(2000, 1, 1),)]


@pytest.mark.parametrize(
    "pattern,expects",
    [
        ("foo", True),
        ("bar", False),
        (None, None),
    ],
)
def test_duckdb_regexp_like(mock_db, pattern, expects):
    cursor = mock_db.cursor()
    field = cursor.execute(f"SELECT regexp_like('foo', '{pattern}')").fetchall()
    assert field == [(expects,)]


@pytest.mark.parametrize(
    "array,delim,expects",
    [
        (["foo", "bar"], ",", "foo,bar"),
        (["foo", "bar"], None, "foobar"),
        (["foo", "bar"], "None", "foobar"),
        ([], ",", ""),
    ],
)
def test_duckdb_array_join(mock_db, array, delim, expects):
    cursor = mock_db.cursor()
    if array is None:
        query = f"SELECT array_join(['None'], '{delim}')"
    else:
        query = (
            f"WITH dataset AS (SELECT ARRAY {array} AS data) "
            f"SELECT array_join(data, '{delim}') FROM dataset"
        )
    joined = cursor.execute(query).fetchall()
    assert joined == [(expects,)]


### End of duckdb user defined functions

@mock.patch("cumulus_library.databases.duckdb.DuckDatabaseBackend.cursor")
@pytest.mark.parametrize(
    "error,raises",
    [
        (duckdb.OperationalError, pytest.raises(ValueError)),
        (duckdb.BinderException, pytest.raises(ValueError)),
        (errors.CumulusLibraryError,pytest.raises(errors.CumulusLibraryError))
    ],
)
def test_duckdb_operational_errors( mock_cursor, mock_db, error, raises):
    mock_cursor.return_value.execute.return_value.fetchall.side_effect = error
    with raises:
        sql_utils.validate_schema(mock_db,{"table": {"foo": "bar"}})

@mock.patch("cumulus_library.databases.athena.AthenaDatabaseBackend.cursor")
@mock.patch("botocore.client")
@pytest.mark.parametrize(
    "error,raises",
    [
        (pyathena.OperationalError,pytest.raises(ValueError)),
        (errors.CumulusLibraryError,pytest.raises(errors.CumulusLibraryError))
    ],
)
def test_athena_operational_errors(mock_botocore, mock_cursor, error, raises):
    db = databases.AthenaDatabaseBackend(**ATHENA_KWARGS)
    mock_cursor.return_value.execute.return_value.fetchall.side_effect = error
    with raises:
        sql_utils.validate_schema(db,{"table": {"foo": "bar"}})


