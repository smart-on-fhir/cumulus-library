import numpy
import pandas
import pyarrow
import pytest

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
