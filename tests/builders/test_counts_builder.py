"""tests for outputs of counts_builder module"""

import pathlib
from contextlib import nullcontext as does_not_raise
from unittest import mock

import pytest

from cumulus_library import base_utils, errors, study_manifest
from cumulus_library.actions import builder as build_action
from cumulus_library.builders import counts
from cumulus_library.builders.statistics_templates import counts_templates
from cumulus_library.template_sql import base_templates
from tests import conftest, testbed_utils

TEST_PREFIX = "test"


@pytest.mark.parametrize(
    "name,duration,expected",
    [
        ("table", None, "test__table"),
        ("table", "month", "test__table_month"),
    ],
)
def test_get_table_name(name, duration, expected):
    manifest = study_manifest.StudyManifest()
    manifest._study_prefix = TEST_PREFIX
    builder = counts.CountsBuilder(manifest=manifest)
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
        manifest = study_manifest.StudyManifest()
        manifest._study_prefix = TEST_PREFIX
        builder = counts.CountsBuilder(manifest=manifest)
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
        for k, v in kwargs.items():
            assert v == call_kwargs[k]


def test_write_queries(tmp_path, mock_db_config):
    manifest = study_manifest.StudyManifest()
    manifest._study_prefix = "foo"
    builder = counts.CountsBuilder(config=mock_db_config, manifest=manifest)
    builder.queries = ["SELECT * FROM FOO", "SELECT * FROM BAR"]
    builder.write_counts(config=mock_db_config, manifest=manifest, filepath=tmp_path / "output.sql")
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
            {
                "field": "gender",
                "join_table": "code_map",
                "join_field": "gender",
                "columns": [("label", "varchar"), ("description", "varchar")],
            },
            [(2, "female", "label_1", "desc_1"), (1, "male", "label_2", "desc_1")],
        ),
        (
            {
                "field": "gender",
                "join_table": "code_map",
                "join_field": "gender",
                "columns": [["label", "varchar"], ["description", "varchar"]],
                "alt_target": "label",
            },
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
    conftest.write_toml(
        tmp_path,
        {
            "study_prefix": TEST_PREFIX,
            "stages": {"stage_1": [{"type": "build:serial", "files": ["annotation.workflow"]}]},
        },
    )
    data = {
        "config_type": "counts",
        "tables": {
            "annotation": {
                "source_table": "core__patient",
                "table_cols": [
                    "gender",
                ],
                "min_subject": 1,
            }
        },
    }
    if annotation is not None:
        data["tables"]["annotation"]["annotation"] = annotation
    conftest.write_toml(tmp_path, data, filename="annotation.workflow")
    manifest = study_manifest.StudyManifest(tmp_path)
    build_action.run_protected_table_builder(config=config, manifest=manifest)
    build_action.build_study(config, manifest=manifest)
    results = db.cursor().execute("select * from test__annotation").fetchall()
    for line in expected:
        assert line in results


def test_counts_workflow(mock_db_core_config):
    manifest = study_manifest.StudyManifest(pathlib.Path(__file__).parents[1] / "test_data/counts/")
    conn = mock_db_core_config.db.connection
    snomed_query = base_templates.get_ctas_query(
        schema_name="main",
        table_name="snomed",
        dataset=[
            ["160903007", "http://snomed.info/sct", "Full-time employment (finding)"],
            ["3595000", "http://snomed.info/sct", "Stress (finding)"],
            ["422650009", "http://snomed.info/sct", "Social isolation (finding)"],
        ],
        table_cols=["code", "system", "display"],
    )
    conn.execute(snomed_query)
    build_action.run_protected_table_builder(config=mock_db_core_config, manifest=manifest)
    build_action.build_study(config=mock_db_core_config, manifest=manifest)

    res = conn.execute("select * from counts__basic_count order by all desc").fetchall()
    assert res[0] == (50, None, None, None)
    assert res[-1] == (10, None, None, "672")

    res = conn.execute("select * from counts__wheres order by all desc").fetchall()
    # this response contains one row
    assert res[0] == (29, "female", None, None)

    res = conn.execute("select * from counts__wheres_min_subject order by all desc").fetchall()
    assert res[0] == (29, "female", None, None)
    assert res[-1] == (5, "female", None, "672")

    res = conn.execute("select * from counts__primary_id order by all desc").fetchall()
    assert res[0] == (22, "final", None)
    assert res[-1] == (10, None, "34533-0")

    res = conn.execute("select * from counts__secondary_table order by all desc").fetchall()
    assert res[0] == (50, None, None, None, "finished")
    assert res[-1] == (10, None, None, "672", None)

    res = conn.execute("select * from counts__annotated order by all desc").fetchall()
    assert res[0] == (
        15,
        None,
        None,
        None,
        None,
    )
    assert res[-1] == (2, "422650009", None, "http://snomed.info/sct", "Social isolation (finding)")

    res = conn.execute("select * from counts__filtered order by all desc").fetchall()
    assert res[0] == (14, None, "resolved")
    assert res[-1] == (2, "422650009", None)
    # let's also validate filter properties - i.e. the filtered col should be a specified value
    # or None (from cubing)
    nulls = 0
    resolves = 0
    for row in res:
        if row[-1] is None:
            nulls += 1
        elif row[-1] == "resolved":
            resolves += 1
    assert nulls == 4
    assert resolves == 4


def test_count_invalid_param(mock_db_config, tmp_path):
    conftest.write_toml(
        tmp_path,
        {
            "study_prefix": "foo",
            "stages": {"stage_1": [{"type": "build:serial", "files": ["count.workflow"]}]},
        },
    )
    conftest.write_toml(
        tmp_path, {"config_type": "counts", "unexpected_key": "whoops"}, filename="count.workflow"
    )
    with pytest.raises(SystemExit):
        manifest = study_manifest.StudyManifest(tmp_path)
        build_action.build_study(config=mock_db_config, manifest=manifest)


def test_primary_id_override():
    query = counts_templates.get_count_query(
        table_name="test",
        source_table="source",
        table_cols="col",
    )
    assert "subject_ref" in query


def test_col_casts():
    count_col = counts_templates.CountColumn(name="name", db_type="VARCHAR", alias=None)
    assert counts_templates._cast_table_col("name") == count_col
    assert counts_templates._cast_table_col(count_col) == count_col
    assert counts_templates._cast_table_col(("name", "VARCHAR")) == count_col
    count_col.alias = "alias"
    assert counts_templates._cast_table_col(("name", "VARCHAR", "alias")) == count_col
    filter_col = counts_templates.FilterColumn(name="name", values=["a", "b"], include_nulls=True)
    assert counts_templates._cast_filter_col(["name", ["a", "b"], True]) == filter_col
    assert counts_templates._cast_filter_col(("name", ["a", "b"], True)) == filter_col
    assert counts_templates._cast_filter_col(filter_col) == filter_col
