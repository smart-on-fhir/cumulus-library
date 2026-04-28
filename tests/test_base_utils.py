import zipfile

import numpy
import pandas
import pyarrow
import pytest
import time_machine

from cumulus_library import base_utils, errors

HIVE_TYPES = [
    "tinyint",
    "smallint",
    "int",
    "integer",
    "bigint",
    "float",
    "double",
    "double precision",
    "decimal",
    "date",
    "timestamp",
    "interval",
    "string",
    "varchar",
    "char",
    "boolean",
    "binary",
]


def test_pyarrow_from_hive():
    res = base_utils.pyarrow_types_from_hive_types(HIVE_TYPES)
    assert res == [
        pyarrow.int64(),
        pyarrow.int64(),
        pyarrow.int64(),
        pyarrow.int64(),
        pyarrow.int64(),
        pyarrow.float64(),
        pyarrow.float64(),
        pyarrow.float64(),
        pyarrow.float64(),
        pyarrow.date64(),
        pyarrow.timestamp("ns"),
        pyarrow.duration("ns"),
        pyarrow.string(),
        pyarrow.string(),
        pyarrow.string(),
        pyarrow.bool_(),
        pyarrow.bool_(),
    ]


def test_pyarrow_from_hive_error():
    with pytest.raises(errors.CumulusLibraryError):
        base_utils.pyarrow_types_from_hive_types(["EnterpriseBeanFactoryFactory"])


def test_pandas_from_hive():
    res = base_utils.pandas_types_from_hive_types(HIVE_TYPES)
    assert res == [
        pandas.Int64Dtype(),
        pandas.Int64Dtype(),
        pandas.Int64Dtype(),
        pandas.Int64Dtype(),
        pandas.Int64Dtype(),
        pandas.Float64Dtype(),
        pandas.Float64Dtype(),
        pandas.Float64Dtype(),
        pandas.Float64Dtype(),
        numpy.datetime64,
        numpy.datetime64,
        pandas.IntervalDtype(),
        pandas.StringDtype(),
        pandas.StringDtype(),
        pandas.StringDtype(),
        pandas.BooleanDtype(),
        pandas.BooleanDtype(),
    ]


def test_pandas_from_hive_error():
    with pytest.raises(errors.CumulusLibraryError):
        base_utils.pandas_types_from_hive_types(["EnterpriseBeanFactoryFactory"])


@pytest.mark.parametrize(
    "subdirs,csv,archive_path,expected",
    [
        (True, False, "data/data.zip", ["a.parquet", "subdir/b.parquet"]),
        (False, False, "data/data.zip", ["a.parquet"]),
        (False, True, "data__2024-01-01T00:00:00Z.zip", ["a.csv"]),
    ],
)
@time_machine.travel("2024-01-01T00:00:00Z", tick=False)
def test_zip_dir(tmp_path, subdirs, csv, archive_path, expected):
    data_path = tmp_path / "data"
    data_path.mkdir()
    (data_path / "a.parquet").write_text("")
    (data_path / "a.csv").write_text("")
    (data_path / "subdir").mkdir()
    (data_path / "subdir/b.parquet").write_text("")

    base_utils.zip_dir(data_path, tmp_path, "data")
    with zipfile.ZipFile(tmp_path / "data/data.zip") as z:
        assert z.namelist() == ["a.parquet", "subdir/b.parquet"]
