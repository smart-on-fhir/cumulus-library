"""tests for outputs of counts_builder module"""

from contextlib import nullcontext as does_not_raise
from unittest import mock

import pytest

import cumulus_library
from cumulus_library import base_utils, errors, study_manifest
from cumulus_library.builders import counts
from tests import testbed_utils

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


def test_exclude_docstatus():
    manifest = study_manifest.StudyManifest()
    manifest._study_prefix = TEST_PREFIX
    builder = counts.CountsBuilder(manifest=manifest)
    query = builder.count_documentreference("table", "source", ["col_a"])
    assert "s.docStatus" in query
    assert "s.status" in query
    query = builder.count_documentreference("table", "source", ["col_a"], skip_status_filter=True)
    assert "s.docStatus" not in query
    assert "s.status" not in query


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


@pytest.mark.parametrize(
    "annotation,expected",
    [
        (None, [(2, "female"), (3, None), (1, "male")]),
        (
            cumulus_library.CountAnnotation(
                field="gender",
                join_table="code_map",
                join_field="gender",
                columns=[("label", None), ("description", None)],
            ),
            [(2, "female", "label_1", "desc_1"), (1, "male", "label_2", "desc_1")],
        ),
        (
            cumulus_library.CountAnnotation(
                field="gender",
                join_table="code_map",
                join_field="gender",
                columns=[("label", None), ("description", None)],
                alt_target="label",
            ),
            [(2, "label_1", "desc_1"), (1, "label_2", "desc_1")],
        ),
    ],
)
def test_count_annotation(tmp_path, annotation, expected):
    testbed = testbed_utils.LocalTestbed(path=tmp_path, with_patient=False)
    for p in [("A", "female"), ("B", "female"), ("C", "male")]:
        testbed.add_patient(row_id=p[0], gender=p[1])
    db = testbed.build()
    db.cursor().execute("""CREATE TABLE code_map AS SELECT * FROM
    (VALUES 
        ('female', 'label_1','desc_1'), 
        ('male', 'label_2','desc_1')
    ) AS code_map(gender,label,description)
""")
    config = base_utils.StudyConfig(db=db, schema="main")
    manifest = study_manifest.StudyManifest()
    manifest._study_prefix = TEST_PREFIX
    builder = counts.CountsBuilder(manifest=manifest)
    builder.queries.append(
        builder.count_patient(
            table_name=f"{TEST_PREFIX}__annotation",
            source_table="core__patient",
            table_cols=["gender"],
            min_subject=1,
            annotation=annotation,
        )
    )
    builder.execute_queries(config=config, manifest=manifest)
    results = db.cursor().execute("select * from test__annotation").fetchall()
    for line in expected:
        assert line in results
