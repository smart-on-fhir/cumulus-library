"""
Round-trip test: TOML configs must render the SQL the IBD study's own builders generated.

We compare parsed SQL rather than text, so that formatting and comments don't matter but a
changed filter, alias, join or sentinel does.
"""

import pathlib
from types import SimpleNamespace

import pytest
import sqlglot

from cumulus_library import db_config
from cumulus_library.builders import nlp_highlight_builder, nlp_wide_builder

DATA = pathlib.Path(__file__).parent.parent / "test_data/nlp_flatten_ibd"
PREFIX = "ibd"
SOURCE_TABLE = f"{PREFIX}__nlp_treatment_gpt_oss_120b"
MANIFEST = SimpleNamespace(get_study_prefix=lambda: PREFIX)

CASES = {
    "wide": (nlp_wide_builder.NlpWideBuilder, "treatment_wide", "ibd__llm_treatment_wide"),
    "highlights": (
        nlp_highlight_builder.NlpHighlightBuilder,
        "treatment_highlights",
        "ibd__llm_treatment_highlights",
    ),
}


@pytest.fixture
def athena_config(mock_db_config, monkeypatch):
    """
    A DuckDB config with the treatment NLP result table present, rendering for Athena.

    The IBD fixtures were generated for Athena, and the highlight template branches on the
    process-wide dialect, so pin it (undone after the test) rather than inherit DuckDB's.
    """
    mock_db_config.db.cursor().execute(
        f"CREATE TABLE {SOURCE_TABLE} AS SELECT "
        "'DocumentReference/1' AS note_ref, 'Encounter/1' AS encounter_ref, "
        "'Patient/1' AS subject_ref, '2026-01-01' AS generated_on, 7 AS task_version, "
        "'fp' AS system_fingerprint, {'rx_annotations': []} AS result"
    )
    monkeypatch.setattr(db_config, "db_type", "athena")
    return mock_db_config


def render(builder_cls, toml_path: pathlib.Path, config) -> str:
    """
    Runs a builder's real prepare_queries path and returns its one query
    """
    builder = builder_cls(toml_config_path=toml_path)
    builder.prepare_queries(config=config, manifest=MANIFEST)
    [query] = builder.queries
    return query


def normalize(sql: str) -> str:
    """
    Parses SQL and prints it back canonically, without comments, so that diffs are readable
    """
    return sqlglot.parse_one(sql, dialect="athena").sql(
        dialect="athena", pretty=True, comments=False
    )


@pytest.mark.parametrize("kind", CASES)
def test_toml_renders_the_sql_ibd_generated(athena_config, kind):
    """
    The IBD study's own builders generated the SQL in the first place, so this
    ensures that our migration to this new builder code doesn't change anything.
    """
    builder_cls, toml_stem, table = CASES[kind]

    rendered = render(builder_cls, DATA / f"{toml_stem}.toml", athena_config)

    expected = (DATA / f"{table}.sql").read_text()
    assert normalize(rendered) == normalize(expected)


@pytest.mark.parametrize(
    ("kind", "old", "new"),
    [
        # A changed sentinel filter
        ("highlights", 'require = "nota"', 'require = "not_null"'),
        # A changed join
        ("wide", 'join = "left"', 'join = "cross"'),
        # A changed output column
        ("wide", '{ path = "ade", root = "ade" }', '{ path = "ade", root = "ade", alias = "x" }'),
        # A changed task version filter
        ("highlights", "task_versions = [7]", "task_versions = [8]"),
    ],
)
def test_comparison_detects_a_changed_config(athena_config, tmp_path, kind, old, new):
    """Guards the test above against passing vacuously (say, comparing something to itself)"""
    builder_cls, toml_stem, table = CASES[kind]
    toml = (DATA / f"{toml_stem}.toml").read_text()
    assert old in toml
    changed = tmp_path / "changed.toml"
    changed.write_text(toml.replace(old, new))

    rendered = render(builder_cls, changed, athena_config)

    expected = (DATA / f"{table}.sql").read_text()
    assert normalize(rendered) != normalize(expected)
