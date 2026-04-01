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
    res = base_utils.numpy_types_from_hive_types(hive_types)
    assert res == [
        "int",
        "int",
        "int",
        "int",
        "int",
        "float",
        "float",
        "float",
        "float",
        "datetime64[ns]",
        "datetime64[ns]",
        "timedelta64[ns]",
        "string",
        "string",
        "string",
        "bool",
        "bool",
    ]


def test_numpy_from_hive_error():
    with pytest.raises(errors.CumulusLibraryError):
        base_utils.numpy_types_from_hive_types(["EnterpriseBeanFactoryFactory"])
