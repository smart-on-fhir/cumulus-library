import json

from contextlib import nullcontext as does_not_raise
from unittest import mock

import pytest

from cumulus_library.errors import CountsBuilderError
from cumulus_library.schema.counts import CountsBuilder

TEST_PREFIX = "test"


@pytest.mark.parametrize(
    "name,duration,expected",
    [
        ("table", None, "test__table"),
        ("table", "month", "test__table_month"),
    ],
)
def test_get_table_name(name, duration, expected):
    builder = CountsBuilder(study_prefix=TEST_PREFIX)
    output = builder.get_table_name(name, duration)
    assert output == expected


@pytest.mark.parametrize(
    "clause,min_subject,expected,raises",
    [
        (None, None, ["cnt_subject >= 10"], does_not_raise()),
        (None, 5, ["cnt_subject >= 5"], does_not_raise()),
        ("age > 5", None, ["age > 5"], does_not_raise()),
        (["age > 5", "sex =='F'"], None, ["age > 5", "sex =='F'"], does_not_raise()),
        ("age > 5", 7, ["age > 5"], does_not_raise()),
        ({"age": "5"}, None, None, pytest.raises(CountsBuilderError)),
    ],
)
def test_get_where_clauses(clause, min_subject, expected, raises):
    with raises:
        kwargs = {}
        if clause is not None:
            kwargs["clause"] = clause
        if min_subject is not None:
            kwargs["min_subject"] = min_subject
        builder = CountsBuilder(study_prefix=TEST_PREFIX)
        output = builder.get_where_clauses(**kwargs)
        assert output == expected


@pytest.mark.parametrize(
    "table_name,source_table,table_cols,kwargs,raises",
    [
        ("table", "source", ["a", "b"], {}, does_not_raise()),
        (
            "table",
            "source",
            ["a", "b"],
            {"min_subject": 10, "where_clauses": "where True", "cnt_encounter": True},
            does_not_raise(),
        ),
        (
            "table",
            "source",
            ["a", "b"],
            {"bad_key": True},
            pytest.raises(CountsBuilderError),
        ),
        (None, "source", ["a", "b"], {}, pytest.raises(CountsBuilderError)),
        ("table", None, ["a", "b"], {}, pytest.raises(CountsBuilderError)),
        ("table", "source", [], {}, pytest.raises(CountsBuilderError)),
        ("table", "source", None, {}, pytest.raises(CountsBuilderError)),
    ],
)
@mock.patch("cumulus_library.template_sql.templates.get_count_query")
def test_get_count_query(
    mock_count, table_name, source_table, table_cols, kwargs, raises
):
    with raises:
        builder = CountsBuilder(study_prefix=TEST_PREFIX)
        builder.get_count_query(table_name, source_table, table_cols, **kwargs)
        assert mock_count.called
        call_args, call_kwargs = mock_count.call_args
        assert call_args == (table_name, source_table, table_cols)
        assert call_kwargs == kwargs


@pytest.mark.parametrize(
    "table_name,source_table,table_cols,where",
    [
        ("table", "source", ["a", "b"], None),
        ("table", "source", ["a", "b"], "a = True"),
    ],
)
@mock.patch("cumulus_library.template_sql.templates.get_count_query")
def test_count_patient(mock_count, table_name, source_table, table_cols, where):
    kwargs = {}
    if where is not None:
        kwargs["where_clauses"] = where
    builder = CountsBuilder(study_prefix=TEST_PREFIX)
    builder.count_patient(table_name, source_table, table_cols, **kwargs)
    assert mock_count.called
    call_args, call_kwargs = mock_count.call_args
    assert call_args == (table_name, source_table, table_cols)
    assert call_kwargs["where_clauses"] == where


@pytest.mark.parametrize(
    "table_name,source_table,table_cols,where",
    [
        ("table", "source", ["a", "b"], None),
        ("table", "source", ["a", "b"], "a = True"),
    ],
)
@mock.patch("cumulus_library.template_sql.templates.get_count_query")
def test_count_encounter(mock_count, table_name, source_table, table_cols, where):
    kwargs = {}
    if where is not None:
        kwargs["where_clauses"] = where
    builder = CountsBuilder(study_prefix=TEST_PREFIX)
    builder.count_encounter(table_name, source_table, table_cols, **kwargs)
    assert mock_count.called
    call_args, call_kwargs = mock_count.call_args
    assert call_args == (table_name, source_table, table_cols)
    if where is None:
        assert call_kwargs["cnt_encounter"] == True
    else:
        assert call_kwargs["cnt_encounter"] == True
        assert call_kwargs["where_clauses"] == where
