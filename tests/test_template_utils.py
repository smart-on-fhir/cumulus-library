""" tests for the cli interface to studies """
# TODO: cutover to duckdb

from unittest import mock

import pyathena
import pytest

from cumulus_library.template_sql.utils import (
    is_codeable_concept_array_populated,
    is_codeable_concept_populated,
)

REGION = "us-east-1"
WORKGROUP = "test_wg"
SCHEMA = "schema"


@mock.patch("pyathena.connect")
@pytest.mark.parametrize(
    "query_results,allow_partial,expected",
    [
        ((("foo"), (["coding"]), ("baz")), True, True),
        ((("foo"), (["coding"]), ("baz")), False, False),
        ((("foo"), (["coding, code, system, display"]), ("baz")), False, True),
        ((("foo"), (["coding"]), None), True, False),
        ((("foo"), (["varchar"]), None), True, False),
        ((None, (["varchar"]), None), True, False),
    ],
)
def test_is_codeable_concept_populated(
    mock_pyathena,
    query_results,
    allow_partial,
    expected,  # pylint: disable=unused-argument
):
    cursor = pyathena.connect(
        region_name=REGION,
        work_group=WORKGROUP,
        schema_name=SCHEMA,
    ).cursor()
    cursor.fetchone.side_effect = query_results
    res = is_codeable_concept_populated(
        SCHEMA, "table", "base_col", cursor, allow_partial=allow_partial
    )
    assert res == expected


@mock.patch("pyathena.connect")
@pytest.mark.parametrize(
    "query_results,allow_partial,expected",
    [
        ((("foo"), (["coding"]), ("baz")), True, True),
        ((("foo"), (["coding"]), ("baz")), False, False),
        ((("foo"), (["coding, code, system, display"]), ("baz")), False, True),
        ((("foo"), (["coding"]), None), True, False),
        ((("foo"), (["varchar"]), None), True, False),
        ((None, (["varchar"]), None), True, False),
    ],
)
def test_is_codeable_concept_array_populated(
    mock_pyathena,
    query_results,
    allow_partial,
    expected,  # pylint: disable=unused-argument
):
    cursor = pyathena.connect(
        region_name=REGION,
        work_group=WORKGROUP,
        schema_name=SCHEMA,
    ).cursor()
    cursor.fetchone.side_effect = query_results
    res = is_codeable_concept_array_populated(
        SCHEMA, "table", "base_col", cursor, allow_partial=allow_partial
    )
    assert res == expected
