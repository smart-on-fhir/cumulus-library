""" tests for the cli interface to studies """

from unittest import mock
from rich.progress import Progress

import pyathena
import pytest

from cumulus_library.helper import get_progress_bar
from cumulus_library.template_sql.utils import is_codeable_concept_populated

REGION = "us-east-1"
WORKGROUP = "test_wg"
SCHEMA = "schema"


@mock.patch("pyathena.connect")
@pytest.mark.parametrize(
    "query_results,expected",
    [
        ((("foo"), (["coding"]), ("baz")), True),
        ((("foo"), (["coding"]), None), False),
        ((("foo"), (["varchar"]), None), False),
        ((None, (["varchar"]), None), False),
    ],
)
def test_is_codeable_concept_populated(mock_pyathena, query_results, expected):
    cursor = pyathena.connect(
        region_name=REGION,
        work_group=WORKGROUP,
        schema_name=SCHEMA,
    ).cursor()
    cursor.fetchone.side_effect = query_results
    progress = Progress()
    with get_progress_bar(transient=True) as progress:
        task = progress.add_task(
            "test_task",
            total=3,
        )
        res = is_codeable_concept_populated(SCHEMA, "table", "base_col", cursor)
    assert res == expected


@mock.patch("pyathena.connect")
@pytest.mark.parametrize(
    "query_results,expected",
    [
        ((("foo"), (["coding"]), ("baz")), True),
        ((("foo"), (["coding"]), None), False),
        ((("foo"), (["varchar"]), None), False),
        ((None, (["varchar"]), None), False),
    ],
)
def test_is_codeable_concept_array_populated(mock_pyathena, query_results, expected):
    cursor = pyathena.connect(
        region_name=REGION,
        work_group=WORKGROUP,
        schema_name=SCHEMA,
    ).cursor()
    cursor.fetchone.side_effect = query_results
    progress = Progress()
    with get_progress_bar(transient=True) as progress:
        task = progress.add_task(
            "test_task",
            total=3,
        )
        res = is_codeable_concept_populated(SCHEMA, "table", "base_col", cursor)
    assert res == expected
