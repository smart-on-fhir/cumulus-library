""" tests for the cli interface to studies """
import duckdb
import pytest

from contextlib import nullcontext as does_not_raise
from cumulus_library.template_sql import utils


@pytest.mark.parametrize(
    "table,base_col,expected,raises",
    [
        # coding
        ("condition", "code", True, does_not_raise()),
        # array coding
        ("condition", "category", False, does_not_raise()),
        # bare code
        ("encounter", "class", False, does_not_raise()),
        # non coding
        ("condition", "resourcetype", False, does_not_raise()),
    ],
)
def test_is_codeable_concept_populated(mock_db, table, base_col, expected, raises):
    with raises:
        res = utils.is_codeable_concept_populated(
            "main", table, base_col, mock_db.cursor()
        )
        assert res == expected


@pytest.mark.parametrize(
    "table,base_col,expected,raises",
    [
        # coding
        ("condition", "code", False, does_not_raise()),
        # array coding
        ("condition", "category", True, does_not_raise()),
        # bare code
        ("encounter", "status", False, does_not_raise()),
        # non coding
        ("condition", "resourcetype", False, does_not_raise()),
    ],
)
def test_is_codeable_concept_array_populated(
    mock_db, table, base_col, expected, raises
):
    with raises:
        res = utils.is_codeable_concept_array_populated(
            "main", table, base_col, mock_db.cursor()
        )
        assert res == expected


@pytest.mark.parametrize(
    "table,base_col,expected,raises",
    [
        # coding
        ("condition", "code", False, does_not_raise()),
        # array coding
        ("condition", "category", False, does_not_raise()),
        # bare code
        ("encounter", "class", True, does_not_raise()),
        # non coding
        ("condition", "resourcetype", False, does_not_raise()),
    ],
)
def test_is_code_populated(mock_db, table, base_col, expected, raises):
    with raises:
        res = utils.is_code_populated("main", table, base_col, mock_db.cursor())
        assert res == expected
