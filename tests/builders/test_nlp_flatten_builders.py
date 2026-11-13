"""Tests for the wide and highlight builders that flatten NLP result tables"""

import pathlib
import textwrap
from types import SimpleNamespace

import duckdb
import pytest
import sqlglot

from cumulus_library import cli, errors
from cumulus_library.builders import nlp_highlight_builder, nlp_wide_builder
from cumulus_library.builders.nlp import base_flatten_utils
from tests.conftest import duckdb_args

PREFIX = "flat"
MANIFEST = SimpleNamespace(get_study_prefix=lambda: PREFIX)

WIDE_TOML = """
config_type = "nlp_wide"

[tables.thing]
task_versions = [2]
unnest_paths = ["mentions"]
columns = ["thing", { path = "kind", transform = "UPPER", alias = "kind_upper" }]
"""

HIGHLIGHT_TOML = """
config_type = "nlp_highlight"

[tables.thing]
task_versions = [2]

[[tables.thing.mentions]]
ls_variable_name = "Thing"
ls_subvalue_name = "Thing"
path = "thing"
span_path = "spans"
within = ["mentions"]
"""


def write(tmp_path: pathlib.Path, text: str, name: str = "config.toml") -> pathlib.Path:
    path = tmp_path / name
    path.write_text(textwrap.dedent(text))
    return path


def make_nlp_table(db, task: str, model: str = "gpt_oss_120b", *, with_result: bool = True):
    """Creates a minimal NLP result table, shaped like the ones NlpBuilder writes"""
    result = (
        "{'mentions': ["
        "{'thing': 'a', 'kind': 'x', 'spans': [[0, 3], [5, 9]]},"
        "{'thing': 'b', 'kind': 'y', 'spans': [[10, 12]]}"
        "]}"
        if with_result
        else "'no result column here'"
    )
    col = "result" if with_result else "other"
    db.cursor().execute(
        f"CREATE TABLE {PREFIX}__nlp_{task}_{model} AS SELECT "
        "'DocumentReference/1' AS note_ref, 'Encounter/1' AS encounter_ref, "
        "'Patient/1' AS subject_ref, '2026-01-01' AS generated_on, 2 AS task_version, "
        f"'fp' AS system_fingerprint, {result} AS {col}"
    )


def run(builder, config):
    """Builds and executes a builder's tables, returning its queries"""
    builder.prepare_queries(config=config, manifest=MANIFEST)
    for query in builder.queries:
        config.db.cursor().execute(query)
    return builder.queries


def rows(config, sql):
    return config.db.cursor().execute(sql).fetchall()


###############################################################################
# Loading the config
###############################################################################
def test_wide_config_loads(tmp_path):
    builder = nlp_wide_builder.NlpWideBuilder(toml_config_path=write(tmp_path, WIDE_TOML))

    normalized = builder._normalized["thing"]
    assert [c["alias"] for c in normalized["output_columns"]] == ["thing", "kind_upper"]
    assert normalized["output_columns"][1]["transform"] == "UPPER"
    assert normalized["unnest_paths"][0]["alias"] == "mentions"


@pytest.mark.parametrize(
    ("builder_cls", "toml", "message"),
    [
        (nlp_wide_builder.NlpWideBuilder, 'config_type = "nlp_wide"\n', "tables"),
        (
            nlp_wide_builder.NlpWideBuilder,
            'config_type = "nlp_wide"\n[tables.a]\ncolumns = ["x"]\nbogus = 1',
            "bogus",
        ),
        (
            nlp_wide_builder.NlpWideBuilder,
            WIDE_TOML.replace("nlp_wide", "nlp_highlight"),
            "not 'nlp_wide'",
        ),
        (
            nlp_highlight_builder.NlpHighlightBuilder,
            HIGHLIGHT_TOML.replace('path = "thing"', 'path = "thing"\nrequire = "typo"'),
            "typo",
        ),
        (nlp_wide_builder.NlpWideBuilder, 'config_type = "nlp_wide"\n[tables]', "no tables"),
    ],
)
def test_invalid_config_exits_with_a_message(tmp_path, builder_cls, toml, message):
    with pytest.raises(SystemExit, match=message):
        builder_cls(toml_config_path=write(tmp_path, toml))


def test_config_semantic_errors_fail_at_load_not_at_build(tmp_path):
    toml = 'config_type = "nlp_wide"\n[tables.a]\ncolumns = ["x.note_ref"]'
    with pytest.raises(errors.NlpTableConfigError, match="note_ref"):
        nlp_wide_builder.NlpWideBuilder(toml_config_path=write(tmp_path, toml))


def test_prefix_is_required(tmp_path, mock_db_config):
    builder = nlp_wide_builder.NlpWideBuilder(toml_config_path=write(tmp_path, WIDE_TOML))
    with pytest.raises(errors.NlpTableConfigError, match="study_prefix"):
        builder.prepare_queries(
            config=mock_db_config, manifest=SimpleNamespace(get_study_prefix=lambda: None)
        )


###############################################################################
# Running against a real (DuckDB) database
###############################################################################
def test_wide_table_builds_and_has_expected_rows(tmp_path, mock_db_config):
    make_nlp_table(mock_db_config.db, "thing")
    builder = nlp_wide_builder.NlpWideBuilder(toml_config_path=write(tmp_path, WIDE_TOML))

    [query] = run(builder, mock_db_config)

    assert sqlglot.parse_one(query) is not None
    assert rows(
        mock_db_config,
        f"SELECT note_ref, origin, task_version, thing, kind_upper FROM {PREFIX}__llm_thing_wide"
        " ORDER BY thing",
    ) == [
        ("DocumentReference/1", f"{PREFIX}__nlp_thing_gpt_oss_120b", 2, "a", "X"),
        ("DocumentReference/1", f"{PREFIX}__nlp_thing_gpt_oss_120b", 2, "b", "Y"),
    ]


def test_wide_table_filters_task_versions(tmp_path, mock_db_config):
    make_nlp_table(mock_db_config.db, "thing")
    toml = WIDE_TOML.replace("[2]", "[3]")
    builder = nlp_wide_builder.NlpWideBuilder(toml_config_path=write(tmp_path, toml))

    run(builder, mock_db_config)

    assert rows(mock_db_config, f"SELECT * FROM {PREFIX}__llm_thing_wide") == []


def test_highlight_table_builds_one_row_per_span(tmp_path, mock_db_config):
    make_nlp_table(mock_db_config.db, "thing")
    builder = nlp_highlight_builder.NlpHighlightBuilder(
        toml_config_path=write(tmp_path, HIGHLIGHT_TOML)
    )

    run(builder, mock_db_config)

    assert rows(
        mock_db_config,
        "SELECT label, span, sublabel_name, sublabel_value"
        f" FROM {PREFIX}__llm_thing_highlights ORDER BY span",
    ) == [
        ("Thing", "0:3", "Thing", "a"),
        ("Thing", "10:12", "Thing", "b"),
        ("Thing", "5:9", "Thing", "a"),
    ]


def test_highlight_columns_match_the_declared_shape(tmp_path, mock_db_config):
    make_nlp_table(mock_db_config.db, "thing")
    builder = nlp_highlight_builder.NlpHighlightBuilder(
        toml_config_path=write(tmp_path, HIGHLIGHT_TOML)
    )
    run(builder, mock_db_config)

    cols = rows(mock_db_config, f"DESCRIBE {PREFIX}__llm_thing_highlights")
    assert [c[0] for c in cols] == base_flatten_utils.HIGHLIGHT_COLUMNS


def test_one_table_per_entry_and_source_task_override(tmp_path, mock_db_config):
    """One task can feed several tables, when each names the task it reads from"""
    make_nlp_table(mock_db_config.db, "genetics")
    toml = """
        config_type = "nlp_wide"
        [tables.genetics_a]
        source_task = "genetics"
        unnest_paths = ["mentions"]
        columns = ["thing"]
        [tables.genetics_b]
        source_task = "genetics"
        unnest_paths = ["mentions"]
        columns = ["kind"]
    """
    builder = nlp_wide_builder.NlpWideBuilder(toml_config_path=write(tmp_path, toml))

    run(builder, mock_db_config)

    assert rows(mock_db_config, f"SELECT thing FROM {PREFIX}__llm_genetics_a_wide ORDER BY 1") == [
        ("a",),
        ("b",),
    ]
    assert rows(mock_db_config, f"SELECT kind FROM {PREFIX}__llm_genetics_b_wide ORDER BY 1") == [
        ("x",),
        ("y",),
    ]


###############################################################################
# Finding source tables
###############################################################################
def test_all_models_are_unioned_and_ordered(tmp_path, mock_db_config):
    for model in ("gpt_oss_120b", "claude_sonnet45"):
        make_nlp_table(mock_db_config.db, "thing", model)
    builder = nlp_wide_builder.NlpWideBuilder(toml_config_path=write(tmp_path, WIDE_TOML))

    [query] = run(builder, mock_db_config)

    assert query.index("claude_sonnet45") < query.index("gpt_oss_120b")
    assert query.count("UNION ALL") == 1
    assert {r[0] for r in rows(mock_db_config, f"SELECT origin FROM {PREFIX}__llm_thing_wide")} == {
        f"{PREFIX}__nlp_thing_claude_sonnet45",
        f"{PREFIX}__nlp_thing_gpt_oss_120b",
    }


def test_source_models_restricts_the_tables_used(tmp_path, mock_db_config):
    for model in ("gpt_oss_120b", "claude_sonnet45"):
        make_nlp_table(mock_db_config.db, "thing", model)
    toml = WIDE_TOML.replace("task_versions", 'source_models = ["gpt_oss_120b"]\ntask_versions')
    builder = nlp_wide_builder.NlpWideBuilder(toml_config_path=write(tmp_path, toml))

    [query] = run(builder, mock_db_config)

    assert "claude_sonnet45" not in query


def test_source_models_that_do_not_exist_are_skipped(tmp_path, mock_db_config):
    make_nlp_table(mock_db_config.db, "thing")
    toml = WIDE_TOML.replace(
        "task_versions", 'source_models = ["gpt_oss_120b", "nope"]\ntask_versions'
    )
    builder = nlp_wide_builder.NlpWideBuilder(toml_config_path=write(tmp_path, toml))

    [query] = run(builder, mock_db_config)

    assert "nope" not in query


def test_tables_without_a_result_column_are_ignored(tmp_path, mock_db_config):
    make_nlp_table(mock_db_config.db, "thing", "good")
    make_nlp_table(mock_db_config.db, "thing", "bad", with_result=False)
    builder = nlp_wide_builder.NlpWideBuilder(toml_config_path=write(tmp_path, WIDE_TOML))

    [query] = run(builder, mock_db_config)

    assert "nlp_thing_good" in query
    assert "nlp_thing_bad" not in query


def test_a_task_does_not_claim_the_tables_of_a_longer_task_name(tmp_path, mock_db_config):
    """'lab' and 'lab_panel' share a stem, but 'lab_panel_*' tables are not 'lab' tables"""
    make_nlp_table(mock_db_config.db, "lab")
    make_nlp_table(mock_db_config.db, "lab_panel")
    toml = """
        config_type = "nlp_wide"
        [tables.lab]
        unnest_paths = ["mentions"]
        columns = ["thing"]
        [tables.lab_panel]
        unnest_paths = ["mentions"]
        columns = ["thing"]
    """
    builder = nlp_wide_builder.NlpWideBuilder(toml_config_path=write(tmp_path, toml))

    lab_query, panel_query = run(builder, mock_db_config)

    assert f"{PREFIX}__nlp_lab_gpt_oss_120b" in lab_query
    assert "lab_panel" not in lab_query.split("CREATE TABLE")[1].split("FROM")[1]
    assert f"{PREFIX}__nlp_lab_panel_gpt_oss_120b" in panel_query
    assert f"{PREFIX}__nlp_lab_gpt_oss_120b" not in panel_query


###############################################################################
# No source tables yet
###############################################################################
def test_wide_empty_table_has_the_same_columns_and_types(tmp_path, mock_db_config):
    toml = """
        config_type = "nlp_wide"
        [tables.thing]
        unnest_paths = ["mentions"]
        columns = [
            "thing",
            { path = "count", sql_type = "integer" },
            { expr = "1 = 1", alias = "flag", sql_type = "boolean" },
        ]
    """
    builder = nlp_wide_builder.NlpWideBuilder(toml_config_path=write(tmp_path, toml))

    run(builder, mock_db_config)

    cols = rows(mock_db_config, f"DESCRIBE {PREFIX}__llm_thing_wide")
    assert [(c[0], c[1]) for c in cols] == [
        *[(c, "VARCHAR") for c in base_flatten_utils.METADATA_COLUMNS],
        ("thing", "VARCHAR"),
        ("count", "INTEGER"),
        ("flag", "BOOLEAN"),
    ]
    assert rows(mock_db_config, f"SELECT * FROM {PREFIX}__llm_thing_wide") == []


def test_highlight_empty_table_has_the_standard_columns(tmp_path, mock_db_config):
    builder = nlp_highlight_builder.NlpHighlightBuilder(
        toml_config_path=write(tmp_path, HIGHLIGHT_TOML)
    )

    run(builder, mock_db_config)

    cols = rows(mock_db_config, f"DESCRIBE {PREFIX}__llm_thing_highlights")
    assert [c[0] for c in cols] == base_flatten_utils.HIGHLIGHT_COLUMNS


###############################################################################
# Through the CLI (manifest parsing, dispatch, execution)
###############################################################################
def test_full_build_through_the_cli(tmp_path):
    study = tmp_path / "studies" / PREFIX
    study.mkdir(parents=True)
    (study / "manifest.toml").write_text(
        textwrap.dedent(
            f"""
            study_prefix = "{PREFIX}"

            [[stages.flatten]]
            files = ["wide.toml", "highlights.toml"]
            """
        )
    )
    (study / "wide.toml").write_text(textwrap.dedent(WIDE_TOML))
    (study / "highlights.toml").write_text(textwrap.dedent(HIGHLIGHT_TOML))

    # NLP result tables are made by an earlier NLP run, so stand one in
    db = duckdb.connect(f"{tmp_path}/duck.db")
    db.execute(
        f"CREATE TABLE {PREFIX}__nlp_thing_gpt_oss_120b AS SELECT "
        "'DocumentReference/1' AS note_ref, 'Encounter/1' AS encounter_ref, "
        "'Patient/1' AS subject_ref, '2026-01-01' AS generated_on, 2 AS task_version, "
        "'fp' AS system_fingerprint, "
        "{'mentions': [{'thing': 'a', 'kind': 'x', 'spans': [[0, 3]]}]} AS result"
    )
    db.close()

    cli.main(
        cli_args=duckdb_args(
            ["build", "-t", PREFIX, "-s", str(tmp_path / "studies")],
            tmp_path,
        )
    )

    db = duckdb.connect(f"{tmp_path}/duck.db")
    assert db.execute(f"SELECT thing FROM {PREFIX}__llm_thing_wide").fetchall() == [("a",)]
    assert db.execute(f"SELECT span FROM {PREFIX}__llm_thing_highlights").fetchall() == [("0:3",)]
