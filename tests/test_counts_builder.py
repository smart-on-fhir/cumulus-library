"""tests for outputs of counts_builder module"""

from contextlib import nullcontext as does_not_raise
from unittest import mock

import pytest

from cumulus_library import errors, study_manifest
from cumulus_library.builders import counts

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
    "name,duration,expected",
    [
        ("table", None, "test__table"),
        ("table", "month", "test__table_month"),
    ],
)
def test_deprecation_patch(name, duration, expected):
    from cumulus_library.builders.counts import CountsBuilder

    builder = CountsBuilder(study_prefix=TEST_PREFIX)
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


# @pytest.mark.parametrize(
#     "table_name,fhir_resource,new_name,warns",
#     [
#         ("count_encounter_table", None, "test__count_encounter_table", False),
#         ("count_encounter_table", "encounter", "test__count_encounter_table", False),
#         ("count_encounter_table", "condition", "test__count_encounter_table", True),
#         ("table", "encounter", "test__count_encounter_table", True),
#         ("table_count_encounter", None, "test__count_encounter_table", True),
#     ],
# )
# @mock.patch("rich.console.Console")
# def test_coerce_name(mock_console, table_name, fhir_resource, new_name, warns):
#     builder = counts.CountsBuilder(study_prefix=TEST_PREFIX)
#     name = builder.coerce_table_name(table_name, fhir_resource)
#     assert name == new_name
#     for thing in mock_console.mock_calls:
#         print(thing)
#         print(type(thing))
#     print("------")
#     for thing in mock_console.print().mock_calls:
#         print(thing)
#         print(type(thing))
#     assert mock_console.print.called == warns


@pytest.mark.parametrize(
    "table_name,source_table,table_cols,kwargs,raises",
    [
        ("count_encounter_table", "source", ["a", "b"], {}, does_not_raise()),
        (
            "count_encounter_table",
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
            "count_encounter_table",
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
            "count_encounter_table",
            None,
            ["a", "b"],
            {},
            pytest.raises(errors.CountsBuilderError),
        ),
        ("count_encounter_table", "source", [], {}, pytest.raises(errors.CountsBuilderError)),
        ("count_encounter_table", "source", None, {}, pytest.raises(errors.CountsBuilderError)),
    ],
)
@mock.patch("cumulus_library.builders.statistics_templates.counts_templates.get_count_query")
def test_get_count_query(mock_count, table_name, source_table, table_cols, kwargs, raises):
    with raises:
        manifest = study_manifest.StudyManifest()
        manifest._study_prefix = TEST_PREFIX
        builder = counts.CountsBuilder(manifest=manifest)
        builder.get_count_query(table_name, source_table, table_cols, **kwargs)
        assert mock_count.called
        call_args, call_kwargs = mock_count.call_args
        assert call_args == (table_name, source_table, table_cols)
        assert call_kwargs == kwargs


@pytest.mark.parametrize(
    "table_name,source_table,table_cols,where,min_subject,method,fhir_resource",
    [
        ("count_condition_table", "source", ["a", "b"], None, None, "count_condition", "condition"),
        (
            "count_condition_table",
            "source",
            ["a", "b"],
            "a = True",
            5,
            "count_condition",
            "condition",
        ),
        (
            "count_documentreference_table",
            "source",
            ["a", "b"],
            None,
            None,
            "count_documentreference",
            "documentreference",
        ),
        (
            "count_documentreference_table",
            "source",
            ["a", "b"],
            "a = True",
            5,
            "count_documentreference",
            "documentreference",
        ),
        ("count_encounter_table", "source", ["a", "b"], None, None, "count_encounter", "encounter"),
        (
            "count_encounter_table",
            "source",
            ["a", "b"],
            "a = True",
            5,
            "count_encounter",
            "encounter",
        ),
        (
            "count_medicationrequest_table",
            "source",
            ["a", "b"],
            None,
            None,
            "count_medicationrequest",
            "medicationrequest",
        ),
        (
            "count_medicationrequest_table",
            "source",
            ["a", "b"],
            "a = True",
            5,
            "count_medicationrequest",
            "medicationrequest",
        ),
        ("count_patient_table", "source", ["a", "b"], None, None, "count_patient", "patient"),
        ("count_patient_table", "source", ["a", "b"], "a = True", 5, "count_patient", "patient"),
        (
            "count_observation_table",
            "source",
            ["a", "b"],
            None,
            None,
            "count_observation",
            "observation",
        ),
        (
            "count_observation_table",
            "source",
            ["a", "b"],
            "a = True",
            5,
            "count_observation",
            "observation",
        ),
    ],
)
@mock.patch("cumulus_library.builders.statistics_templates.counts_templates.get_count_query")
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
    manifest = study_manifest.StudyManifest()
    manifest._study_prefix = TEST_PREFIX
    builder = counts.CountsBuilder(manifest=manifest)
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


def test_null_study_prefix():
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
