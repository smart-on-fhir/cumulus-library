from contextlib import nullcontext as does_not_raise

import pytest

from cumulus_library.template_sql import sql_utils


@pytest.mark.parametrize(
    "hierarchy,expected_fields,expected_return,raises",
    [
        ([("meta", dict)], ["id"], True, does_not_raise()),
        ([("extension", list), ("extension", dict)], ["url"], True, does_not_raise()),
        ([("meta", dict)], ["garbage"], False, does_not_raise()),
        ([("garbage", dict)], ["id"], False, does_not_raise()),
        ([("meta", dict)], "garbage", False, pytest.raises(ValueError)),
        ([("meta", str)], ["id"], True, pytest.raises(ValueError)),
    ],
)
def test_is_field_populated(mock_db_config, hierarchy, expected_fields, expected_return, raises):
    with raises:
        res = sql_utils.is_field_populated(
            database=mock_db_config.db,
            source_table="patient",
            hierarchy=hierarchy,
            expected=expected_fields,
        )
        assert res == expected_return
