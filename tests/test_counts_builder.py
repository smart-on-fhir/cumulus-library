"""tests for outputs of counts_builder module"""

from contextlib import nullcontext as does_not_raise
from unittest import mock

import pytest

from cumulus_library import errors
from cumulus_library.statistics import counts

TEST_PREFIX = "test"


@pytest.mark.parametrize(
    "name,duration,expected",
    [
        ("table", None, "test__table"),
        ("table", "month", "test__table_month"),
    ],
)
def test_get_table_name(name, duration, expected):
    builder = counts.CountsBuilder(study_prefix=TEST_PREFIX)
    output = builder.get_table_name(name, duration)
    assert output == expected


@pytest.mark.parametrize(
    "clause,min_subject,expected,raises",
    [
        (None, None, ["cnt_subject_ref >= 10"], does_not_raise()),
        (None, 5, ["cnt_subject_ref >= 5"], does_not_raise()),
        ("age > 5", None, ["age > 5"], does_not_raise()),
        (["age > 5", "sex =='F'"], None, ["age > 5", "sex =='F'"], does_not_raise()),
        ("age > 5", 7, ["age > 5"], does_not_raise()),
        ({"age": "5"}, None, None, pytest.raises(errors.CountsBuilderError)),
    ],
)
def test_get_where_clauses(clause, min_subject, expected, raises):
    with raises:
        kwargs = {}
        if clause is not None:
            kwargs["clause"] = clause
        if min_subject is not None:
            kwargs["min_subject"] = min_subject
        builder = counts.CountsBuilder(study_prefix=TEST_PREFIX)
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
            {
                "min_subject": 5,
                "where_clauses": "where True",
                "fhir_resource": "encounter",
            },
            does_not_raise(),
        ),
        (
            "table",
            "source",
            ["a", "b"],
            {"bad_key": True},
            pytest.raises(errors.CountsBuilderError),
        ),
        (
            None,
            "source",
            ["a", "b"],
            {},
            pytest.raises(errors.CountsBuilderError),
        ),
        (
            "table",
            None,
            ["a", "b"],
            {},
            pytest.raises(errors.CountsBuilderError),
        ),
        ("table", "source", [], {}, pytest.raises(errors.CountsBuilderError)),
        ("table", "source", None, {}, pytest.raises(errors.CountsBuilderError)),
    ],
)
@mock.patch("cumulus_library.statistics.statistics_templates.counts_templates.get_count_query")
def test_get_count_query(mock_count, table_name, source_table, table_cols, kwargs, raises):
    with raises:
        builder = counts.CountsBuilder(study_prefix=TEST_PREFIX)
        builder.get_count_query(table_name, source_table, table_cols, **kwargs)
        assert mock_count.called
        call_args, call_kwargs = mock_count.call_args
        assert call_args == (table_name, source_table, table_cols)
        assert call_kwargs == kwargs


@pytest.mark.parametrize(
    "table_name,source_table,table_cols,where,min_subject,method,fhir_resource",
    [
        ("table", "source", ["a", "b"], None, None, "count_condition", "condition"),
        ("table", "source", ["a", "b"], "a = True", 5, "count_condition", "condition"),
        (
            "table",
            "source",
            ["a", "b"],
            None,
            None,
            "count_documentreference",
            "documentreference",
        ),
        (
            "table",
            "source",
            ["a", "b"],
            "a = True",
            5,
            "count_documentreference",
            "documentreference",
        ),
        ("table", "source", ["a", "b"], None, None, "count_encounter", "encounter"),
        ("table", "source", ["a", "b"], "a = True", 5, "count_encounter", "encounter"),
        (
            "table",
            "source",
            ["a", "b"],
            None,
            None,
            "count_medicationrequest",
            "medicationrequest",
        ),
        (
            "table",
            "source",
            ["a", "b"],
            "a = True",
            5,
            "count_medicationrequest",
            "medicationrequest",
        ),
        ("table", "source", ["a", "b"], None, None, "count_patient", "patient"),
        ("table", "source", ["a", "b"], "a = True", 5, "count_patient", "patient"),
        ("table", "source", ["a", "b"], None, None, "count_observation", "observation"),
        (
            "table",
            "source",
            ["a", "b"],
            "a = True",
            5,
            "count_observation",
            "observation",
        ),
    ],
)
@mock.patch("cumulus_library.statistics.statistics_templates.counts_templates.get_count_query")
def test_count_wrappers(
    mock_count,
    table_name,
    source_table,
    table_cols,
    where,
    min_subject,
    method,
    fhir_resource,
):
    kwargs = {}
    if where is not None:
        kwargs["where_clauses"] = where
    if min_subject is not None:
        kwargs["min_subject"] = min_subject
    builder = counts.CountsBuilder(study_prefix=TEST_PREFIX)
    wrapper = getattr(builder, method)
    wrapper(table_name, source_table, table_cols, **kwargs)
    assert mock_count.called
    call_args, call_kwargs = mock_count.call_args
    assert call_args == (table_name, source_table, table_cols)
    assert call_kwargs["fhir_resource"] == fhir_resource
    if where is not None:
        assert call_kwargs["where_clauses"] == where
    if min_subject is not None:
        assert call_kwargs["min_subject"] == min_subject


def test_null_initialization():
    with pytest.raises(errors.CountsBuilderError):
        counts.CountsBuilder()


def test_write_queries(tmp_path):
    builder = counts.CountsBuilder(study_prefix="foo")
    builder.queries = ["SELECT * FROM FOO", "SELECT * FROM BAR"]
    builder.write_counts(tmp_path / "output.sql")
    with open(tmp_path / "output.sql") as f:
        found = f.read()
    expected = """-- noqa: disable=all
SELECT * FROM FOO

-- ###########################################################

SELECT * FROM BAR
"""
    assert found == expected
