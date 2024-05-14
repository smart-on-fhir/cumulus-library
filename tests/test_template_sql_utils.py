"""tests for the cli interface to studies"""

from contextlib import nullcontext as does_not_raise

import pytest

from cumulus_library.template_sql import sql_utils


@pytest.mark.parametrize(
    "table,hierarchy,expected,returns,raises",
    [
        # coding
        ("condition", [("code", dict), ("coding", list)], None, True, does_not_raise()),
        # array coding
        (
            "condition",
            [("category", list), ("coding", list)],
            None,
            True,
            does_not_raise(),
        ),
        # bare code
        (
            "encounter",
            [("class", dict)],
            ["code", "system", "display"],
            True,
            does_not_raise(),
        ),
        # non coding
        ("encounter", [("period", dict)], None, False, does_not_raise()),
        # non coding with specific expected fields
        ("encounter", [("period", dict)], ["start", "end"], True, does_not_raise()),
        # deeply nested field
        (
            "encounter",
            [
                ("hospitalization", dict),
                ("dischargeDisposition", dict),
                ("coding", list),
            ],
            {
                "dischargeDisposition": {
                    "coding": ["code", "system"],
                    "text": {},
                },
            },
            True,
            does_not_raise(),
        ),
    ],
)
def test_is_field_populated(mock_db, table, hierarchy, expected, returns, raises):
    with raises:
        res = sql_utils.is_field_populated(
            database=mock_db,
            source_table=table,
            hierarchy=hierarchy,
            expected=expected,
        )
        assert res == returns
