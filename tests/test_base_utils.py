import pyarrow
import pytest

from cumulus_library import base_utils, errors


def test_numpy_from_hive():
    hive_types = [
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
    res = base_utils.pyarrow_types_from_hive_types(hive_types)
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
