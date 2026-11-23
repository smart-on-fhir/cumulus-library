"""
Tests for the NLP builder: workflow config, table selection and naming, and the output it writes.
"""

import contextlib
import io
import json
import os
import pathlib
from unittest import mock

import cumulus_fhir_support as cfs
import pandas
import pytest

import cumulus_library
from cumulus_library import cli, databases, errors, note_utils
from cumulus_library.builders import nlp_builder
from cumulus_library.builders.nlp import driver, models, workflow
from tests import conftest, nlp_utils
from tests.conftest import duckdb_args
from tests.nlp_utils import SALT_BYTES, add_doc, add_dxr, read_rows


@pytest.fixture(autouse=True)
def _autouse_cache_dir(mock_cache_dir):
    """
    Autouse this fixture to keep NLP's on-disk cache inside tmp_path.
    Delegates to the shared ``mock_cache_dir`` fixture defined in (tests/conftest.py).
    """
    yield


def test_unexpected_config_field(tmp_path, note_source):
    workflow_path = conftest.write_toml(
        tmp_path,
        {
            "config_type": "nlp",
            "extra_field": "yup",
        },
        "nlp.workflow",
    )

    with pytest.raises(SystemExit, match="contains unknown field `extra_field`"):
        nlp_builder.NlpBuilder(toml_config_path=workflow_path, notes=note_source)


def test_task_without_schema(tmp_path, note_source):
    workflow_path = conftest.write_toml(
        tmp_path,
        {
            "config_type": "nlp",
            "tables": {"test": {}},
        },
        "nlp.workflow",
    )
    with pytest.raises(ValueError, match="response schema must be provided for table 'test'"):
        nlp_builder.NlpBuilder(toml_config_path=workflow_path, notes=note_source)


def test_empty_note_dir(tmp_path):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    with pytest.raises(SystemExit, match="early because an NLP workflow was encountered"):
        nlp_builder.NlpBuilder(toml_config_path=workflow_path, notes=note_utils.NoteSource())


@nlp_utils.mock_env()
@mock.patch("openai.OpenAI")
def test_table_filter_but_no_salt(mock_client, tmp_path, note_source):
    db, _schema = databases.create_db_backend(
        {
            "db_type": "athena",
            "region": "test",
            "work_group": "test",
            "profile": "test",
            "schema_name": "testdb",
        }
    )
    db.connection = mock.MagicMock()
    study_config = cumulus_library.StudyConfig(db=db, schema="main")
    workflow_path = conftest.write_toml(
        tmp_path,
        {
            "config_type": "nlp",
            "tables": {
                "test": {
                    "select_by_table": "table",
                    "response_schema": nlp_utils.EMPTY_SCHEMA,
                }
            },
        },
        "nlp.workflow",
    )
    model = nlp_utils.MockModel(mock_client, make_codebook=False)
    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )
    err_msg = "Cannot calculate anonymized resource IDs without a PHI dir defined"
    with pytest.raises(RuntimeError, match=err_msg):
        builder.execute_queries(study_config, None)


def test_flattened_config(tmp_path, note_source):
    workflow_path = conftest.write_toml(
        tmp_path,
        {
            "config_type": "nlp",
            "shared": {
                "system_prompt": "hello",
            },
            "tables": {
                "override": {
                    "system_prompt": "bye",
                    "response_schema": nlp_utils.EMPTY_SCHEMA,
                },
                "fallthrough": {
                    "response_schema": nlp_utils.EMPTY_SCHEMA,
                },
            },
        },
        "nlp.workflow",
    )
    builder = nlp_builder.NlpBuilder(toml_config_path=workflow_path, notes=note_source)
    assert builder._workflow_config.tables["override"].system_prompt == "bye"
    assert builder._workflow_config.tables["fallthrough"].system_prompt == "hello"


@mock.patch("openai.OpenAI")
def test_filter(mock_client, tmp_path, mock_db_config):
    workflow_path = conftest.write_toml(
        tmp_path,
        {
            "config_type": "nlp",
            "tables": {
                "filtered": {
                    "select_by_word": ["fever"],
                    "reject_by_word": ["cold"],
                    "select_by_table": "prev_table",
                    "response_schema": nlp_utils.EMPTY_SCHEMA,
                },
                "all": {
                    "response_schema": nlp_utils.EMPTY_SCHEMA,
                },
            },
        },
        "nlp.workflow",
    )

    with open(f"{tmp_path}/dxr.ndjson", "w", encoding="utf8") as f:
        add_dxr("1", None, f)  # no text, will be skipped
        add_dxr("2", "hello world", f)  # ignored by filters
        add_dxr("3", "has fever", f)  # selected by filters
        add_dxr("4", "has fever and cold", f)  # rejected by filters
        add_dxr("5", "has fever", f)  # would be selected but is excluded by table

    expected_stats = """ Notes processed:
  Available:                5
  Had text:                 4
  Considered (filtered):    1
  Failed (filtered):        0
  Got response (filtered):  1
    from cache:             0
    fresh LLM calls:        1
  Considered (all):         4
  Failed (all):             0
  Got response (all):       4
    from cache:             1
    fresh LLM calls:        3"""

    mock_db_config.db.cursor().execute(f"""
        CREATE TABLE prev_table AS SELECT * FROM (
            VALUES
            ('{cfs.anon_id("1", SALT_BYTES)}'),
            ('{cfs.anon_id("2", SALT_BYTES)}'),
            ('{cfs.anon_id("3", SALT_BYTES)}'),
            ('{cfs.anon_id("4", SALT_BYTES)}')
        )
        AS t (diagnosticreport_id)
    """)

    source = note_utils.NoteSource([tmp_path])
    model = nlp_utils.MockModel(mock_client)

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=source, nlp_config=model.nlp_config()
    )

    console_output = io.StringIO()
    with contextlib.redirect_stdout(console_output):
        builder.execute_queries(mock_db_config, None)
    assert expected_stats in nlp_utils.strip_trailing_whitespace(console_output.getvalue())
    table = (
        mock_db_config.db.cursor()
        .execute(
            """SELECT note_ref,encounter_ref,subject_ref 
        FROM example_nlp__nlp_all_gpt_oss_120b
        ORDER BY note_ref DESC """
        )
        .fetchall()
    )
    assert table == [
        (
            "DiagnosticReport/b5f3542891693362bfdc76cc58c869989eec4ac5df7c6939265ecf0280edc3c5",
            "Encounter/99dc09ba05c815768052720a0004132a58fbe14801f6666f85baaad778de6341",
            None,
        ),
        (
            "DiagnosticReport/7754781daf881b5c7006fcd5452df13ff49db01114bcec4a80c6394d745454f5",
            "Encounter/99dc09ba05c815768052720a0004132a58fbe14801f6666f85baaad778de6341",
            None,
        ),
        (
            "DiagnosticReport/4457e00465fa53651f67203ddb28280ddcb5693599ad9044177b64f65d5b6512",
            "Encounter/99dc09ba05c815768052720a0004132a58fbe14801f6666f85baaad778de6341",
            None,
        ),
        (
            "DiagnosticReport/0d9af682d646605ca219a1ee5177506d282ecdf873c89f3e312440624fcebbe7",
            "Encounter/99dc09ba05c815768052720a0004132a58fbe14801f6666f85baaad778de6341",
            None,
        ),
    ]


@pytest.mark.parametrize(
    "model_id,expected_table,expected_folder",
    [
        # Hyphens in a model ID get converted to underscores, since the model is part of a
        # SQL table name (and this matches how the ETL names its own NLP tables).
        ("gpt-oss-120b", "example_nlp__nlp_task_gpt_oss_120b", "nlp_task_gpt_oss_120b_v2"),
        ("gpt4o", "example_nlp__nlp_task_gpt4o", "nlp_task_gpt4o_v2"),
    ],
)
@mock.patch("openai.OpenAI")
def test_naming_conventions(
    mock_client, model_id, expected_table, expected_folder, tmp_path, mock_db_config
):
    """NLP tables & upload folders should look like the ETL's: nlp prefix, task, model, version"""
    model = nlp_utils.MockModel(mock_client, model_id=model_id)
    nlp_config = model.nlp_config()
    task = workflow.NlpTask(version=2)

    assert driver.get_table_name_for_task("task", nlp_config) == expected_table
    assert driver._get_upload_name_for_task("task", task, nlp_config) == expected_folder
    # duckdb has no remote upload location, so results land in the local cache dir
    path = driver.get_output_path_for_task(nlp_config, "task", task, mock_db_config.db)
    assert str(path) == f"{tmp_path}/nlp/example_nlp/{expected_folder}"


@mock.patch("openai.OpenAI")
def test_naming_requires_a_model(mock_client):
    """We can't name a table without knowing the model, so complain early"""
    model = nlp_utils.MockModel(mock_client)
    nlp_config = model.nlp_config()
    nlp_config.model = None

    with pytest.raises(errors.CumulusLibraryError, match="An NLP model ID must be provided"):
        driver.get_table_name_for_task("task", nlp_config)

    # The model factory guards against this too, with the same message
    with pytest.raises(errors.CumulusLibraryError, match="An NLP model ID must be provided"):
        models.create_model(nlp_config)


@mock.patch("openai.OpenAI")
def test_clean_only_removes_this_task_and_model(mock_client, tmp_path, mock_db_config, note_source):
    """--clean-nlp should leave other tasks & models alone"""
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client)

    root = cfs.FsPath(tmp_path, "nlp", "example_nlp")
    old_version = root.joinpath("nlp_task_gpt_oss_120b_v9")  # same task & model - gets cleaned
    other_model = root.joinpath("nlp_task_gpt4o_v0")
    other_task = root.joinpath("nlp_other_gpt_oss_120b_v0")
    for folder in [old_version, other_model, other_task]:
        folder.makedirs()

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config(clean=True)
    )
    builder.execute_queries(mock_db_config, None)

    assert not old_version.exists()
    assert other_model.exists()
    assert other_task.exists()


@mock.patch("openai.OpenAI")
def test_already_uploaded(mock_client, tmp_path, mock_db_config, note_source):
    """Verify that we skip notes that we've already uploaded before"""
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client)

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )
    builder.execute_queries(mock_db_config, None)
    assert builder.stats.got_response[0] == 1

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config(clean=False)
    )
    builder.execute_queries(mock_db_config, None)
    assert builder.stats.had_text == 1
    assert builder.stats.considered[0] == 0


@mock.patch("openai.OpenAI")
@mock.patch.dict(os.environ, clear=True)
@mock.patch("cumulus_library.builders.nlp_builder.NlpBuilder")
def test_args_passed_down(mock_builder, mock_client, tmp_path):
    os.makedirs(f"{tmp_path}/notes")
    with open(f"{tmp_path}/notes/dxr.ndjson", "w", encoding="utf8") as f:
        dxr = {"resourceType": "DiagnosticReport", "id": "1"}
        json.dump(dxr, f)

    mock_builder.side_effect = RuntimeError("nope")

    mock_model = nlp_utils.MockModel(mock_client)

    # Build core first (example_nlp__cohort table needs it)
    build_args = duckdb_args(
        [
            "build",
            str(tmp_path),
            "--target=core",
        ],
        tmp_path,
    )
    cli.main(cli_args=build_args)

    # Now build NLP
    build_args = duckdb_args(
        [
            "build",
            str(tmp_path),
            "--target=example_nlp",
            f"--note-dir={tmp_path}",
            "--dev",
            "--nlp-subtask=age",
            "--nlp-subtask=race",
            *mock_model.cli_args(),
        ],
        tmp_path,
    )

    with pytest.raises(RuntimeError, match="nope"):
        cli.main(cli_args=build_args)

    config = mock_builder.call_args[1]["nlp_config"]
    assert config.salt == SALT_BYTES
    assert config.subtasks == ["age", "race"]

    source = mock_builder.call_args[1]["notes"]
    assert list(source.progress_iter("label")) == [dxr]


@mock.patch("openai.OpenAI")
def test_unreachable_vllm(mock_client, tmp_path, note_source, mock_db_config):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client)
    model.mock_openai_model_list(fail=True)
    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )
    with pytest.raises(errors.CumulusLibraryError, match="Try running 'docker compose up"):
        builder.execute_queries(mock_db_config, None)


@mock.patch("openai.OpenAI")
def test_cached_response(mock_client, tmp_path, mock_db_config):
    workflow_path = conftest.write_toml(
        tmp_path,
        {
            "config_type": "nlp",
            "tables": {
                "hello_world": {
                    "response_schema": '{"title":"test", "type": "object", '
                    '"properties": {"hello": {"type": "integer"}}}',
                },
            },
        },
        "nlp.workflow",
    )

    with open(f"{tmp_path}/dxr.ndjson", "w", encoding="utf8") as f:
        add_dxr("1", "say hello to the world", f)

    source = note_utils.NoteSource([tmp_path])

    model = nlp_utils.MockModel(mock_client)
    model.mock_openai_response({"hello": 3})

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=source, nlp_config=model.nlp_config()
    )
    builder.execute_queries(mock_db_config, None)

    assert builder.stats.got_response[0] == 1

    # Confirm that we cache the response and don't hit the endpoint again
    model.mock_openai_response({}, fail=True)

    # Add a new note to sanity check that we do actually fail on the new one
    with open(f"{tmp_path}/dxr.ndjson", "a", encoding="utf8") as f:
        add_dxr("2", "goodbye", f)

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=source, nlp_config=model.nlp_config()
    )
    builder.execute_queries(mock_db_config, None)
    assert builder.stats.considered[0] == 2
    assert builder.stats.got_response[0] == 1  # still got our cached result


def _multi_table_workflow(tmp_path, *task_names: str) -> pathlib.Path:
    """Writes a workflow with several identical integer-valued tables, for --nlp-table tests"""
    schema = '{"title":"test", "type": "object", "properties": {"hello": {"type": "integer"}}}'
    return conftest.write_toml(
        tmp_path,
        {
            "config_type": "nlp",
            "tables": {slug: {"response_schema": schema} for slug in task_names},
        },
        "nlp.workflow",
    )


def _table_names(mock_db_config) -> set[str]:
    return {row[0] for row in mock_db_config.db.cursor().execute("show tables").fetchall()}


@mock.patch("openai.OpenAI")
def test_select_subtasks_builds_subset(mock_client, tmp_path, mock_db_config):
    """--nlp-subtask restricts the build to the named tables, leaving the rest unbuilt."""
    workflow_path = _multi_table_workflow(tmp_path, "first_table", "second_table")
    with open(f"{tmp_path}/dxr.ndjson", "w", encoding="utf8") as f:
        add_dxr("1", "say hello to the world", f)
    source = note_utils.NoteSource([tmp_path])

    model = nlp_utils.MockModel(mock_client)
    model.mock_openai_response({"hello": 1})
    nlp_config = model.nlp_config()
    nlp_config.subtasks = ["first_table"]

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=source, nlp_config=nlp_config
    )
    builder.execute_queries(mock_db_config, None)

    tables = _table_names(mock_db_config)
    assert driver.get_table_name_for_task("first_table", nlp_config) in tables
    assert driver.get_table_name_for_task("second_table", nlp_config) not in tables
    assert read_rows(mock_db_config, driver.get_table_name_for_task("first_table", nlp_config))[0][
        "result"
    ] == {"hello": 1}

    # Checking a second subtask does not remove an existing subtask
    nlp_config.subtasks = ["second_table"]
    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=source, nlp_config=nlp_config
    )
    builder.execute_queries(mock_db_config, None)

    tables = _table_names(mock_db_config)
    assert driver.get_table_name_for_task("first_table", nlp_config) in tables
    assert driver.get_table_name_for_task("second_table", nlp_config) in tables


@mock.patch("openai.OpenAI")
def test_select_subtasks_builds_subset_with_select_tables(mock_client, tmp_path, mock_db_config):
    """
    --nlp-subtask restricts the build to the named tables, leaving the rest unbuilt.
    --select-by-table overides workflows parameters
    """
    workflow_path = _multi_table_workflow(tmp_path, "kept_table", "dropped_table")
    with open(f"{tmp_path}/dxr.ndjson", "w", encoding="utf8") as f:
        add_dxr("1", "say hello to the world", f)
    source = note_utils.NoteSource([tmp_path])

    model = nlp_utils.MockModel(mock_client)

    nlp_config = model.nlp_config()
    nlp_config.subtasks = ["kept_table"]
    nlp_config.select_by_table = "test1"

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=source, nlp_config=nlp_config
    )

    for key, task in builder._workflow_config.tables.items():
        assert task.select_by_table == "test1"


@mock.patch("openai.OpenAI")
def test_select_multiple_tables(mock_client, tmp_path, mock_db_config):
    """--nlp-table may be repeated to build more than one table (but not all of them)."""
    workflow_path = _multi_table_workflow(tmp_path, "one", "two", "three")
    with open(f"{tmp_path}/dxr.ndjson", "w", encoding="utf8") as f:
        add_dxr("1", "say hello to the world", f)
    source = note_utils.NoteSource([tmp_path])

    model = nlp_utils.MockModel(mock_client)
    model.mock_openai_response({"hello": 1})
    nlp_config = model.nlp_config()
    nlp_config.subtasks = ["one", "three"]

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=source, nlp_config=nlp_config
    )
    builder.execute_queries(mock_db_config, None)

    tables = _table_names(mock_db_config)
    assert driver.get_table_name_for_task("one", nlp_config) in tables
    assert driver.get_table_name_for_task("three", nlp_config) in tables
    assert driver.get_table_name_for_task("two", nlp_config) not in tables


@mock.patch("openai.OpenAI")
def test_select_unknown_table_errors(mock_client, tmp_path, note_source):
    """Asking for a table not in the workflow fails loudly, listing what is available."""
    workflow_path = _multi_table_workflow(tmp_path, "real")
    model = nlp_utils.MockModel(mock_client)
    nlp_config = model.nlp_config()
    nlp_config.subtasks = ["real", "bogus"]

    with pytest.raises(SystemExit, match="were not found in the workflow: bogus"):
        nlp_builder.NlpBuilder(
            toml_config_path=workflow_path, notes=note_source, nlp_config=nlp_config
        )


@mock.patch("openai.OpenAI")
def test_span_correction(mock_client, tmp_path, mock_db_config):
    workflow_path = conftest.write_toml(
        tmp_path,
        {
            "config_type": "nlp",
            "tables": {
                "hello_world": {
                    # Make spans array deeply nested, to prove we can find it anywhere
                    "response_schema": """{
                        "title":"test", "type": "object", "properties": {
                            "parent_list": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "parent_dict": {
                                            "type": "object",
                                            "properties": {
                                                "spans": {
                                                    "type": "array",
                                                    "items": {"type": "string"}
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }""",
                },
            },
        },
        "nlp.workflow",
    )

    with open(f"{tmp_path}/dxr.ndjson", "w", encoding="utf8") as f:
        add_dxr("dxr", "First, second \n\nthird  fourth.", f)

    source = note_utils.NoteSource([tmp_path])

    model = nlp_utils.MockModel(mock_client)
    model.mock_openai_response(
        {"parent_list": [{"parent_dict": {"spans": [" first,   ", "second third", "forth"]}}]}
    )

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=source, nlp_config=model.nlp_config()
    )

    console_output = io.StringIO()
    with contextlib.redirect_stdout(console_output):
        builder.execute_queries(mock_db_config, None)

    rows = read_rows(mock_db_config, "example_nlp__nlp_hello_world_gpt_oss_120b")
    assert rows[0]["result"] == {"parent_list": [{"parent_dict": {"spans": [[0, 5], [7, 21]]}}]}

    failure_msg = "Could not match span received from NLP server for DiagnosticReport/dxr: forth"
    assert failure_msg in console_output.getvalue()


@mock.patch("openai.OpenAI")
def test_degenerate_spans_do_not_match_everywhere(mock_client, tmp_path, mock_db_config):
    """A span of only punctuation/whitespace must contribute no spans, not one per character.

    Such a span strips to "", which as a regex matches at every offset in the note. Before the
    guard in _fix_spans, a single one of these emitted len(text)+1 span pairs -- held in memory
    until the chunk flushed, then written to parquet -- and reported itself as found.
    """
    workflow_path = conftest.write_toml(
        tmp_path,
        {
            "config_type": "nlp",
            "tables": {
                "hello_world": {
                    "response_schema": """{
                        "title":"test", "type": "object", "properties": {
                            "spans": {"type": "array", "items": {"type": "string"}}
                        }
                    }""",
                },
            },
        },
        "nlp.workflow",
    )

    note_text = "First, second third fourth."
    with open(f"{tmp_path}/dxr.ndjson", "w", encoding="utf8") as f:
        add_dxr("dxr", note_text, f)

    source = note_utils.NoteSource([tmp_path])

    model = nlp_utils.MockModel(mock_client)
    # "second" is real and must still be found; the rest all strip to empty.
    model.mock_openai_response({"spans": ["second", "...", "   ", "", " -- "]})

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=source, nlp_config=model.nlp_config()
    )

    console_output = io.StringIO()
    with contextlib.redirect_stdout(console_output):
        builder.execute_queries(mock_db_config, None)

    rows = read_rows(mock_db_config, "example_nlp__nlp_hello_world_gpt_oss_120b")
    # Only the real span survives. Without the guard this would hold 4 * (len(note_text) + 1)
    # extra pairs on top of it.
    assert rows[0]["result"] == {"spans": [[7, 13]]}

    # Each degenerate span is reported as unmatched rather than silently counted as found.
    output = console_output.getvalue()
    assert output.count("Could not match span received from NLP server") == 4


@mock.patch("openai.OpenAI")
def test_writes_out_at_chunksize(mock_client, tmp_path, mock_db_config):
    with open(f"{tmp_path}/doc.ndjson", "w", encoding="utf8") as f:
        add_doc("1", "Note one", f)
        add_doc("2", "Note two", f)
        add_doc("3", "Note three", f)

    source = note_utils.NoteSource([tmp_path])
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client)
    config = model.nlp_config()
    config.chunksize = 2

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path,
        notes=source,
        nlp_config=config,
    )

    with mock.patch("cumulus_library.builders.nlp.driver._add_upload_refs_for_task") as mock_write:
        # Fake an error too, to confirm we gracefully handle that and print message
        mock_write.side_effect = [RuntimeError("test1"), RuntimeError("test2")]
        console_output = io.StringIO()
        with contextlib.redirect_stdout(console_output):
            builder.execute_queries(mock_db_config, None)

    assert mock_write.call_count == 2
    assert "Failed to process note: test1" in console_output.getvalue()
    assert "Failed to finalize notes: test2" in console_output.getvalue()


@mock.patch("openai.OpenAI")
def test_various_value_types(mock_client, tmp_path, mock_db_config, note_source):
    workflow_path = conftest.write_toml(
        tmp_path,
        {
            "config_type": "nlp",
            "tables": {
                "task": {
                    "response_schema": '{"title":"test", "type": "object", "properties": {'
                    '"float": {"type": "number"},'
                    '"int": {"type": "integer"},'
                    '"str": {"type": "string"},'
                    '"bool": {"type": "boolean"},'
                    '"enum": {"enum": ["red", "amber", "green"]}'
                    "}}",
                },
            },
        },
        "nlp.workflow",
    )

    results = {"float": 1.5, "int": 3, "str": "a", "bool": True, "enum": "red"}
    model = nlp_utils.MockModel(mock_client)
    model.mock_openai_response(results)

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path,
        notes=note_source,
        nlp_config=model.nlp_config(),
    )
    builder.execute_queries(mock_db_config, None)

    rows = read_rows(mock_db_config, "example_nlp__nlp_task_gpt_oss_120b")
    assert rows[0]["result"] == results


@mock.patch("openai.OpenAI")
def test_disabling_stats(mock_client, tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client)

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config(stats=False)
    )

    console_output = io.StringIO()
    with contextlib.redirect_stdout(console_output):
        builder.execute_queries(mock_db_config, None)

    assert builder.stats.got_response[0] == 1
    assert "Notes processed:" not in console_output.getvalue()
    assert "Token usage:" not in console_output.getvalue()


@mock.patch("openai.OpenAI")
def test_failed_notes_are_counted_per_table(mock_client, tmp_path, mock_db_config):
    """A note whose call raises is counted against its own table, and only that table.

    How the count is printed is covered in test_nlp_summary.py.
    """
    workflow_path = conftest.write_toml(
        tmp_path,
        {
            "config_type": "nlp",
            "tables": {
                "age": {"system_prompt": "age", "response_schema": nlp_utils.EMPTY_SCHEMA},
                "favorite_sushi": {
                    "system_prompt": "favorite_sushi",
                    "response_schema": nlp_utils.EMPTY_SCHEMA,
                },
            },
        },
        "nlp.workflow",
    )
    with open(f"{tmp_path}/dxr.ndjson", "w", encoding="utf8") as f:
        add_dxr("1", "fine note", f)
        add_dxr("2", "failing note", f)
        add_dxr("3", "another failing note", f)

    model = nlp_utils.MockModel(mock_client)

    def cond_fail_handler(**kwargs):
        # Only "favorite_sushi" fails, so the count has to land on the right table
        is_favorite_sushi = kwargs["messages"][0]["content"] == "favorite_sushi"
        if is_favorite_sushi and "failing" in model.note_text_of(kwargs):
            raise ValueError("server exploded")
        return {}

    model.mock_openai_handler(cond_fail_handler)

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path,
        notes=note_utils.NoteSource([tmp_path]),
        nlp_config=model.nlp_config(),
    )
    builder.execute_queries(mock_db_config, None)

    assert builder.stats.considered == [3, 3]
    assert builder.stats.failed == [0, 2]
    assert builder.stats.got_response == [3, 1]


@nlp_utils.mock_env()
@mock.patch("botocore.client")
@mock.patch("openai.OpenAI")
def test_write_to_athena(mock_openai_client, mock_boto_client, tmp_path, note_source, mock_s3):
    db, _schema = databases.create_db_backend(
        {
            "db_type": "athena",
            "region": "test",
            "work_group": "test",
            "profile": "test",
            "schema_name": "testdb",
        }
    )
    db.connection = mock.MagicMock()
    bucket_info = {
        "WorkGroup": {
            "Configuration": {
                "ResultConfiguration": {"OutputLocation": f"s3://{conftest.MOCK_S3_BUCKET}/athena/"}
            }
        }
    }
    db.connection._client.get_work_group.return_value = bucket_info

    study_config = cumulus_library.StudyConfig(db=db, schema="main")
    workflow_path = nlp_utils.basic_workflow(tmp_path, version=1)
    model = nlp_utils.MockModel(mock_openai_client)
    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )
    builder.execute_queries(study_config, None)

    assert builder.stats.got_response[0] == 1

    # Confirm we wrote the parquet file out correctly, into an ETL-style upload folder
    # (nlp prefix, task name, model, and task version)
    upload_dir = (
        f"s3://{conftest.MOCK_S3_BUCKET}/athena/cumulus_user_uploads/testdb/example_nlp/"
        "nlp_task_gpt_oss_120b_v1"
    )
    with mock_s3.open(f"{upload_dir}/nlp.0.parquet", "rb") as f:
        df = pandas.read_parquet(f)
        rows = json.loads(df.to_json(orient="records"))

    assert len(rows) == 1
    assert (
        rows[0]["note_ref"]
        == "DiagnosticReport/eee10367d972edc0fbbdf0cc6664eb595e4768e193279cd657cfb9cd82b0329b"
    )

    # And the id file
    with mock_s3.open(f"{upload_dir}.ids", "r") as f:
        assert (
            f.read()
            == "DiagnosticReport/eee10367d972edc0fbbdf0cc6664eb595e4768e193279cd657cfb9cd82b0329b\n"
        )

    # And confirm the query looks right
    assert builder.queries == [
        "CREATE EXTERNAL TABLE IF NOT EXISTS `main`.`example_nlp__nlp_task_gpt_oss_120b` "
        "( `note_ref` STRING, `encounter_ref` STRING, `subject_ref` STRING, "
        "`generated_on` STRING, `task_version` INT, `model` STRING, `system_fingerprint` STRING, "
        "`result` STRUCT<ignored: STRING>\n)\n"
        "STORED AS PARQUET\n"
        f"LOCATION '{upload_dir}'\n"
        'tblproperties ("parquet.compression"="SNAPPY");',
    ]

    # Then check that a version update works successfully
    workflow_path = nlp_utils.basic_workflow(tmp_path, version=2)
    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )
    builder.execute_queries(study_config, None)

    assert builder.stats.got_response[0] == 1
    upload_dir = (
        f"s3://{conftest.MOCK_S3_BUCKET}/athena/cumulus_user_uploads/testdb/example_nlp/"
        "nlp_task_gpt_oss_120b_v2"
    )
    with mock_s3.open(f"{upload_dir}/nlp.0.parquet", "rb") as f:
        df = pandas.read_parquet(f)
        rows = json.loads(df.to_json(orient="records"))

    assert len(rows) == 1
    assert (
        rows[0]["note_ref"]
        == "DiagnosticReport/eee10367d972edc0fbbdf0cc6664eb595e4768e193279cd657cfb9cd82b0329b"
    )
    assert builder.queries == [
        "CREATE EXTERNAL TABLE IF NOT EXISTS `main`.`example_nlp__nlp_task_gpt_oss_120b` "
        "( `note_ref` STRING, `encounter_ref` STRING, `subject_ref` STRING, "
        "`generated_on` STRING, `task_version` INT, `model` STRING, `system_fingerprint` STRING, "
        "`result` STRUCT<ignored: STRING>\n)\n"
        "STORED AS PARQUET\n"
        f"LOCATION '{upload_dir}'\n"
        'tblproperties ("parquet.compression"="SNAPPY");',
    ]


@mock.patch.dict(os.environ, clear=True)
@mock.patch("cumulus_fhir_support.FsPath.register_options", side_effect=RuntimeError("boom"))
def test_aws_profile_env_is_set(mock_register, tmp_path):
    """Confirm that we set the AWS_PROFILE env var from the CLI if provided.

    This way FsPath instances will see the env var."""
    assert "AWS_PROFILE" not in os.environ

    build_args = duckdb_args(
        [
            "build",
            str(tmp_path),
            "--target=core",
            "--profile=test-profile",
        ],
        tmp_path,
    )
    with pytest.raises(RuntimeError, match="boom"):
        cli.main(cli_args=build_args)

    assert os.environ.get("AWS_PROFILE") == "test-profile"


@nlp_utils.mock_env()
@mock.patch("openai.OpenAI")
def test_invalid_study_name(mock_client, tmp_path, note_source):
    db, _schema = databases.create_db_backend(
        {
            "db_type": "athena",
            "region": "test",
            "work_group": "test",
            "profile": "test",
            "schema_name": "testdb",
        }
    )
    db.connection = mock.MagicMock()
    study_config = cumulus_library.StudyConfig(db=db, schema="main")

    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client)
    nlp_config = model.nlp_config()
    nlp_config.target = "blarg"
    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path,
        notes=note_source,
        nlp_config=nlp_config,
    )
    with pytest.raises(RuntimeError, match="The 'blarg' study is not authorized to run NLP"):
        builder.execute_queries(study_config, None)
