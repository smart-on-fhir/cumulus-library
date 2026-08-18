"""
Tests for the NLP builder.

These tests are all in the same xdist "group" because when run across xdist workers, we saw flaky
test failures. We weren't able to debug why, so we grouped these tests up. TODO: investigate that
"""

import binascii
import contextlib
import hashlib
import io
import json
import os
import pathlib
import threading
import time
from collections.abc import Iterator
from types import SimpleNamespace
from unittest import mock

import botocore.exceptions
import cumulus_fhir_support as cfs
import fsspec.implementations.memory
import httpx
import openai
import pandas
import pytest

import cumulus_library
from cumulus_library import cli, databases, errors, note_utils
from cumulus_library.builders import nlp_builder
from cumulus_library.builders.nlp import dispatch as nlp_dispatch
from cumulus_library.builders.nlp import driver, models, workflow
from cumulus_library.builders.nlp.models import OpenAIProvider
from tests import conftest, nlp_utils
from tests.conftest import duckdb_args
from tests.nlp_utils import add_doc, add_dxr

SALT_STR = "e359191164cd209708d93551f481edd048946a9d844c51dea1b64d3f83dfd1fa"
SALT_BYTES = binascii.unhexlify(SALT_STR)


@pytest.fixture
def note_source(tmp_path) -> Iterator[note_utils.NoteSource]:
    """Just make a sample note source with a row - contents not important"""
    with open(f"{tmp_path}/dxr.ndjson", "w", encoding="utf8") as f:
        add_dxr("hello", "hello world", f)
    yield note_utils.NoteSource([tmp_path])


@pytest.fixture(autouse=True)
def _autouse_cache_dir(mock_cache_dir):
    """
    Autouse this fixture to keep NLP's on-disk cache inside tmp_path.
    Delegates to the shared ``mock_cache_dir`` fixture defined in (tests/conftest.py).
    """
    yield


def read_rows(db, table: str) -> list[dict]:
    df = db.db.connection.sql(f"SELECT * FROM {table} ORDER BY note_ref").df()
    return json.loads(df.to_json(orient="records"))


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
  Got response (filtered):  1 
  Considered (all):         4 
  Got response (all):       4 """

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
    assert expected_stats in console_output.getvalue()


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

    assert driver.table_name_for_task("task", nlp_config) == expected_table
    assert driver.upload_slug_for_task("task", task, nlp_config) == expected_folder
    # duckdb has no remote upload location, so results land in the local cache dir
    path = driver.output_path_for_task(nlp_config, "task", task, mock_db_config.db)
    assert str(path) == f"{tmp_path}/nlp/example_nlp/{expected_folder}"


@mock.patch("openai.OpenAI")
def test_naming_requires_a_model(mock_client):
    """We can't name a table without knowing the model, so complain early"""
    model = nlp_utils.MockModel(mock_client)
    nlp_config = model.nlp_config()
    nlp_config.model = None

    with pytest.raises(errors.CumulusLibraryError, match="An NLP model ID must be provided"):
        driver.table_name_for_task("task", nlp_config)

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
            "--nlp-table=age",
            "--nlp-table=race",
            *mock_model.cli_args(),
        ],
        tmp_path,
    )

    with pytest.raises(RuntimeError, match="nope"):
        cli.main(cli_args=build_args)

    config = mock_builder.call_args[1]["nlp_config"]
    assert config.salt == SALT_BYTES
    assert config.tables == ["age", "race"]

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


def _multi_table_workflow(tmp_path, *table_slugs: str) -> pathlib.Path:
    """Writes a workflow with several identical integer-valued tables, for --nlp-table tests"""
    schema = '{"title":"test", "type": "object", "properties": {"hello": {"type": "integer"}}}'
    return conftest.write_toml(
        tmp_path,
        {
            "config_type": "nlp",
            "tables": {slug: {"response_schema": schema} for slug in table_slugs},
        },
        "nlp.workflow",
    )


def _table_names(mock_db_config) -> set[str]:
    return {row[0] for row in mock_db_config.db.cursor().execute("show tables").fetchall()}


@mock.patch("openai.OpenAI")
def test_select_tables_builds_subset(mock_client, tmp_path, mock_db_config):
    """--nlp-table restricts the build to the named tables, leaving the rest unbuilt."""
    workflow_path = _multi_table_workflow(tmp_path, "kept_table", "dropped_table")
    with open(f"{tmp_path}/dxr.ndjson", "w", encoding="utf8") as f:
        add_dxr("1", "say hello to the world", f)
    source = note_utils.NoteSource([tmp_path])

    model = nlp_utils.MockModel(mock_client)
    model.mock_openai_response({"hello": 1})
    nlp_config = model.nlp_config()
    nlp_config.tables = ["kept_table"]

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=source, nlp_config=nlp_config
    )
    builder.execute_queries(mock_db_config, None)

    tables = _table_names(mock_db_config)
    assert driver.table_name_for_task("kept_table", nlp_config) in tables
    assert driver.table_name_for_task("dropped_table", nlp_config) not in tables
    assert read_rows(mock_db_config, driver.table_name_for_task("kept_table", nlp_config))[0][
        "result"
    ] == {"hello": 1}


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
    nlp_config.tables = ["one", "three"]

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=source, nlp_config=nlp_config
    )
    builder.execute_queries(mock_db_config, None)

    tables = _table_names(mock_db_config)
    assert driver.table_name_for_task("one", nlp_config) in tables
    assert driver.table_name_for_task("three", nlp_config) in tables
    assert driver.table_name_for_task("two", nlp_config) not in tables


@mock.patch("openai.OpenAI")
def test_select_unknown_table_errors(mock_client, tmp_path, note_source):
    """Asking for a table not in the workflow fails loudly, listing what is available."""
    workflow_path = _multi_table_workflow(tmp_path, "real")
    model = nlp_utils.MockModel(mock_client)
    nlp_config = model.nlp_config()
    nlp_config.tables = ["real", "bogus"]

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

    with mock.patch("cumulus_library.builders.nlp.driver.add_upload_refs_for_task") as mock_write:
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
def test_no_batching_support(mock_client, tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client)

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path,
        notes=note_source,
        nlp_config=model.nlp_config(batching=True),
    )

    with pytest.raises(errors.CumulusLibraryError, match="does not support batching"):
        builder.execute_queries(mock_db_config, None)


@mock.patch("openai.OpenAI")
def test_no_phi_dir(mock_client, tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client)
    config = model.nlp_config()
    config.phi_dir = None

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=config
    )

    with pytest.raises(errors.CumulusLibraryError, match="Please provide a PHI dir"):
        builder.execute_queries(mock_db_config, None)


@mock.patch("openai.OpenAI")
def test_bad_nlp_model(mock_client, tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client)
    config = model.nlp_config()

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=config
    )

    config.model = "nope"
    with pytest.raises(errors.CumulusLibraryError, match="Unknown NLP model ID"):
        builder.execute_queries(mock_db_config, None)

    config.model = None
    with pytest.raises(errors.CumulusLibraryError, match="An NLP model ID must be provided"):
        builder.execute_queries(mock_db_config, None)


@mock.patch("openai.OpenAI")
def test_missing_nlp_model_but_deployment_works(mock_client, tmp_path, mock_db_config, note_source):
    # The model ID isn't in the server's model list, but the deployment fallback request
    # succeeds (the Azure workaround), so we should proceed without error.
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client)
    model.mock_openai_model_list(models=[])

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    builder.execute_queries(mock_db_config, None)


@mock.patch("openai.OpenAI")
def test_missing_nlp_model(mock_client, tmp_path, mock_db_config, note_source):
    # The model ID isn't in the server's model list, and the deployment fallback request also
    # fails with an APIError, so we should surface a CumulusLibraryError.
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client)
    model.mock_openai_model_list(models=[])
    model.mock_openai_deployment_check(fail=True)

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    with pytest.raises(errors.CumulusLibraryError, match="NLP server does not have model ID"):
        builder.execute_queries(mock_db_config, None)


@mock.patch("openai.OpenAI")
def test_bad_stop(mock_client, tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client)
    model.mock_openai_response({}, finish_reason="content_filter")

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    console_output = io.StringIO()
    with contextlib.redirect_stdout(console_output):
        builder.execute_queries(mock_db_config, None)

    assert builder.stats.got_response[0] == 0
    assert "did not complete, with finish reason: content_filter" in console_output.getvalue()


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
def test_cloud_model_but_local_provider(mock_client, tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client, model_id="gpt5")

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    with pytest.raises(errors.CumulusLibraryError, match="does not support the 'local' provider"):
        builder.execute_queries(mock_db_config, None)


@nlp_utils.mock_env("azure")
@mock.patch("openai.AzureOpenAI")
def test_azure_happy_path(mock_client, tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client, provider="azure")

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    builder.execute_queries(mock_db_config, None)
    assert builder.stats.got_response[0] == 1


@mock.patch("openai.AzureOpenAI")
def test_azure_bad_model(mock_client, tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client, model_id="claude-sonnet45", provider="azure")

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    with pytest.raises(errors.CumulusLibraryError, match="does not support the 'azure' provider"):
        builder.execute_queries(mock_db_config, None)


@mock.patch("openai.AzureOpenAI")
def test_azure_no_env(mock_client, tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client, provider="azure")

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    with pytest.raises(errors.CumulusLibraryError, match="Missing Azure environment variables"):
        builder.execute_queries(mock_db_config, None)


@nlp_utils.mock_env("azure")
@mock.patch("openai.AzureOpenAI")
def test_azure_no_schema_support(mock_client, tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client, provider="azure", model_id="gpt35")

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path,
        notes=note_source,
        nlp_config=model.nlp_config(),
    )

    builder.execute_queries(mock_db_config, None)

    # Confirm that we requested just "give us json please" if model doesn't support schemas
    last_kwargs = model.openai.chat.completions.parse.call_args[1]
    assert last_kwargs["response_format"] == {"type": "json_object"}


@mock.patch("boto3.client")
def test_bedrock_happy_path(mock_client, tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client, provider="bedrock", model_id="claude-sonnet45")

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    builder.execute_queries(mock_db_config, None)
    assert builder.stats.got_response[0] == 1


@mock.patch("boto3.client")
def test_bedrock_bad_stop(mock_client, tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client, provider="bedrock")
    model.mock_bedrock_response({}, stop_reason="content_filter")

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    console_output = io.StringIO()
    with contextlib.redirect_stdout(console_output):
        builder.execute_queries(mock_db_config, None)

    assert builder.stats.got_response[0] == 0
    assert "did not complete, with stop reason: content_filter" in console_output.getvalue()


@mock.patch("boto3.client")
def test_bedrock_bad_model(mock_client, tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client, provider="bedrock", model_id="gpt5")

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    with pytest.raises(errors.CumulusLibraryError, match="does not support the 'bedrock' provider"):
        builder.execute_queries(mock_db_config, None)


@mock.patch("boto3.client")
def test_bedrock_skips_wrapper_in_response(mock_client, tmp_path, mock_db_config, note_source):
    """Confirm we drop a "parameter" wrapper object in response"""
    workflow_path = conftest.write_toml(
        tmp_path,
        {
            "config_type": "nlp",
            "tables": {
                "hello_world": {
                    "response_schema": '{"title":"test", "type": "object", '
                    '"properties": {"hello": {"type": "string"}}}',
                }
            },
        },
        "nlp.workflow",
    )

    model = nlp_utils.MockModel(mock_client, provider="bedrock")
    model.mock_bedrock_response({"parameter": {"hello": "world"}})

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )
    builder.execute_queries(mock_db_config, None)

    rows = read_rows(mock_db_config, "example_nlp__nlp_hello_world_gpt_oss_120b")
    assert rows[0]["result"] == {"hello": "world"}


@mock.patch("boto3.client")
def test_bedrock_text_response(mock_client, tmp_path, mock_db_config, note_source):
    """Confirm we find json inside a text response"""
    workflow_path = conftest.write_toml(
        tmp_path,
        {
            "config_type": "nlp",
            "tables": {
                "hello_world": {
                    "response_schema": '{"title":"test", "type": "object", '
                    '"properties": {"hello": {"type": "number"}}}',
                }
            },
        },
        "nlp.workflow",
    )

    model = nlp_utils.MockModel(mock_client, provider="bedrock")
    model.mock_bedrock_response(
        """
Preamble...

```json
{"hello": 0.5}
```

Summary.
""",
        mode="text",
    )

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    builder.execute_queries(mock_db_config, None)

    rows = read_rows(mock_db_config, "example_nlp__nlp_hello_world_gpt_oss_120b")
    assert rows[0]["result"] == {"hello": 0.5}


@mock.patch("boto3.client")
def test_bedrock_no_response(mock_client, tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client, provider="bedrock")
    model.mock_bedrock_response("", mode="none")

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    console_output = io.StringIO()
    with contextlib.redirect_stdout(console_output):
        builder.execute_queries(mock_db_config, None)

    assert builder.stats.got_response[0] == 0
    assert "Failed to process note: no response content found" in console_output.getvalue()


@nlp_utils.mock_env()
@mock.patch("botocore.client")
@mock.patch("openai.OpenAI")
def test_write_to_athena(mock_openai_client, mock_boto_client, tmp_path, note_source):
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
            "Configuration": {"ResultConfiguration": {"OutputLocation": "s3://testbucket/athena/"}}
        }
    }
    db.connection._client.get_work_group.return_value = bucket_info

    study_config = cumulus_library.StudyConfig(db=db, schema="main")
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_openai_client)

    # Mock out FsPath's s3 filesystem (it should grow a fancier mock itself ideally)
    mem_fs = fsspec.implementations.memory.MemoryFileSystem()
    with mock.patch.dict(cfs.FsPath._fsspecs, {"s3": mem_fs}):
        builder = nlp_builder.NlpBuilder(
            toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
        )
        builder.execute_queries(study_config, None)

    assert builder.stats.got_response[0] == 1

    # Confirm we wrote the parquet file out correctly, into an ETL-style upload folder
    # (nlp prefix, task name, model, and task version)
    upload_dir = (
        "s3://testbucket/athena/cumulus_user_uploads/testdb/example_nlp/nlp_task_gpt_oss_120b_v0"
    )
    with mem_fs.open(f"{upload_dir}/nlp.0.parquet", "rb") as f:
        df = pandas.read_parquet(f)
        rows = json.loads(df.to_json(orient="records"))

    assert len(rows) == 1
    assert rows[0]["note_ref"] == "DiagnosticReport/hello"

    # And the id file
    with mem_fs.open(f"{upload_dir}.ids", "r") as f:
        assert f.read() == "DiagnosticReport/hello\n"

    # And confirm the query looks right
    assert builder.queries == [
        "CREATE EXTERNAL TABLE IF NOT EXISTS `main`.`example_nlp__nlp_task_gpt_oss_120b` "
        "( note_ref STRING, "
        "encounter_ref STRING, subject_ref STRING, generated_on STRING, task_version INT, "
        "model STRING, system_fingerprint STRING, result STRUCT<ignored: STRING>\n)\n"
        "STORED AS PARQUET\n"
        f"LOCATION 'memory://{upload_dir}'\n"
        'tblproperties ("parquet.compression"="SNAPPY");',
    ]


def batch_line(contents: str, answer: str = "answer") -> str:
    checksum = hashlib.sha256(contents.encode("utf8"), usedforsecurity=False).hexdigest()
    return json.dumps(
        {
            "custom_id": checksum,
            "response": {
                "body": {
                    "id": f"blarg-{checksum}",
                    "choices": [
                        {
                            "index": 0,
                            "finish_reason": "stop",
                            "message": {
                                "role": "assistant",
                                "content": json.dumps({"ignored": answer}),
                            },
                        }
                    ],
                    "created": 1000000,
                    "model": "gpt-4o",
                    "object": "chat.completion",
                },
            },
        },
    )


def mock_files_content(model: nlp_utils.MockModel, contents: list | None = None) -> None:
    if contents is None:
        contents = [
            batch_line("hello world"),
        ]
    model.openai.files.content.return_value = openai.HttpxBinaryResponseContent(
        httpx.Response(status_code=200, text="\n".join(contents)),
    )


@nlp_utils.mock_env("azure")
@mock.patch("openai.AzureOpenAI")
def test_azure_batching_happy_path(mock_client, tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client, provider="azure", model_id="gpt4o")

    def upload_file(**kwargs):
        assert kwargs["purpose"] == "batch"
        file_text = cfs.FsPath(str(kwargs["file"])).read_text()
        lines = [json.loads(line) for line in file_text.split("\n") if line]
        assert len(lines) == 1
        assert (
            lines[0]["custom_id"]
            == "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        )
        assert lines[0]["method"] == "POST"
        assert lines[0]["url"] == "/v1/chat/completions"
        assert lines[0]["body"]["model"] == "gpt-4o"
        assert lines[0]["body"]["messages"][1]["content"] == "hello world"
        return SimpleNamespace(id="input")

    model.openai.files.create = upload_file
    model.openai.batches.create.return_value = SimpleNamespace(id="batch")
    model.openai.batches.retrieve.return_value = SimpleNamespace(
        id="batch", status="completed", error_file_id=None, output_file_id="output"
    )
    mock_files_content(model)

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path,
        notes=note_source,
        nlp_config=model.nlp_config(batching=True),
    )

    builder.execute_queries(mock_db_config, None)
    assert builder.stats.got_response[0] == 1

    rows = read_rows(mock_db_config, "example_nlp__nlp_task_gpt4o")
    assert rows[0]["result"] == {"ignored": "answer"}

    assert model.openai.batches.create.call_args_list[0][1] == {
        "completion_window": "24h",
        "endpoint": "/v1/chat/completions",
        "input_file_id": "input",
    }
    assert model.openai.batches.retrieve.call_args_list[0][1] == {
        "batch_id": "batch",
    }


@nlp_utils.mock_env("azure")
@mock.patch("openai.AzureOpenAI")
def test_azure_resume_batching(mock_client, tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client, provider="azure", model_id="gpt4o")

    # Note that the cache namespace does not include the nlp prefix/model naming that the
    # result tables use - renaming it would orphan every previously cached NLP response.
    path_dir = f"{model.phi}/nlp-cache/example_nlp__task_v0_gpt4o"
    os.makedirs(path_dir)
    with open(f"{path_dir}/metadata.json", "w", encoding="utf8") as f:
        json.dump({"batches-azure": ["b1", "b2"]}, f)

    # Just mock the retrieval bits, make the creation bits blow up
    model.openai.files.create.side_effect = RuntimeError
    model.openai.batches.retrieve.return_value = SimpleNamespace(
        id="batch", status="completed", error_file_id=None, output_file_id="output"
    )
    mock_files_content(model)

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path,
        notes=note_source,
        nlp_config=model.nlp_config(batching=True),
    )

    builder.execute_queries(mock_db_config, None)
    assert builder.stats.got_response[0] == 1

    rows = read_rows(mock_db_config, "example_nlp__nlp_task_gpt4o")
    assert rows[0]["result"] == {"ignored": "answer"}


@nlp_utils.mock_env("azure")
@mock.patch("openai.AzureOpenAI")
@mock.patch("time.sleep", new=lambda x: None)
def test_azure_batching_errors(mock_client, tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client, provider="azure", model_id="gpt4o")

    model.openai.files.create.return_value = SimpleNamespace(id="input")
    model.openai.batches.create.return_value = SimpleNamespace(id="batch")
    model.openai.batches.retrieve.side_effect = [
        SimpleNamespace(id="batch", status="validating"),
        SimpleNamespace(id="batch", status="in_progress"),
        SimpleNamespace(id="batch", status="finalizing"),
        SimpleNamespace(
            # Will still process error/output files when failed, just prints a message
            id="batch",
            status="failed",
            error_file_id="error",
            output_file_id="output",
        ),
    ]
    model.openai.files.content.side_effect = [
        openai.HttpxBinaryResponseContent(  # error file
            httpx.Response(
                status_code=200,
                text="\n".join(
                    [
                        # Test all the various ways we can stuff errors in there
                        json.dumps({"error": {"message": {"error": {"message": "error1"}}}}),
                        "{'blarg'",  # invalid json
                    ],
                ),
            ),
        ),
        openai.HttpxBinaryResponseContent(  # output file
            httpx.Response(
                status_code=200,
                text="\n".join(
                    [
                        # Test all the various ways we can stuff errors in there
                        json.dumps({"error": {"message": "error2"}}),
                        json.dumps({"response": {"status_code": 400}}),
                        json.dumps({"response": {"body": {"model": "gpt-4o"}}}),  # no custom_id
                        json.dumps({"custom_id": "xx", "response": {"id": "yy"}}),  # no body
                    ],
                ),
            ),
        ),
    ]

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path,
        notes=note_source,
        nlp_config=model.nlp_config(batching=True),
    )

    console_output = io.StringIO()
    with contextlib.redirect_stdout(console_output):
        builder.execute_queries(mock_db_config, None)

    assert "Batch did not complete, got status: 'failed'" in console_output.getvalue()
    assert "Error from NLP: error1" in console_output.getvalue()
    assert "Could not process error message: '{'blarg''" in console_output.getvalue()
    assert "Error from NLP: error2" in console_output.getvalue()
    assert "Unexpected status code from NLP: 400" in console_output.getvalue()
    assert "Unexpected response from NLP: missing data" in console_output.getvalue()


@mock.patch.object(OpenAIProvider, "AZURE_MAX_BATCH_COUNT", 2)
@mock.patch.object(OpenAIProvider, "AZURE_MAX_BATCH_BYTES", 6000)
@nlp_utils.mock_env("azure")
@mock.patch("openai.AzureOpenAI")
def test_azure_splitting_batch(mock_client, tmp_path, mock_db_config):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client, provider="azure", model_id="gpt4o")

    long_str = "a" * 5900  # very long string that hits size limit

    with open(f"{tmp_path}/dxr.ndjson", "w", encoding="utf8") as f:
        add_dxr("hello1", "world1", f)
        add_dxr("hello2", "world2", f)
        # Now there will be a break because of max count of 2 rows
        add_dxr("hello3", long_str, f)
        # Now there will be a break because of max byte limit
        add_dxr("hello4", "world4", f)
    note_source = note_utils.NoteSource([tmp_path])

    model.openai.files.create.side_effect = [
        SimpleNamespace(id="input1"),
        SimpleNamespace(id="input2"),
        SimpleNamespace(id="input3"),
    ]
    model.openai.batches.create.side_effect = [
        SimpleNamespace(id="batch1"),
        SimpleNamespace(id="batch2"),
        SimpleNamespace(id="batch3"),
    ]
    model.openai.batches.retrieve.side_effect = [
        SimpleNamespace(
            id="batch1", status="completed", error_file_id=None, output_file_id="output1"
        ),
        SimpleNamespace(
            id="batch2", status="completed", error_file_id=None, output_file_id="output2"
        ),
        SimpleNamespace(
            id="batch3", status="completed", error_file_id=None, output_file_id="output3"
        ),
    ]
    model.openai.files.content.side_effect = [
        openai.HttpxBinaryResponseContent(
            httpx.Response(
                status_code=200,
                text="\n".join(
                    [batch_line("world1", answer="w1"), batch_line("world2", answer="w2")]
                ),
            ),
        ),
        openai.HttpxBinaryResponseContent(
            httpx.Response(status_code=200, text=batch_line(long_str, answer="w3")),
        ),
        openai.HttpxBinaryResponseContent(
            httpx.Response(status_code=200, text=batch_line("world4", answer="w4")),
        ),
    ]

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path,
        notes=note_source,
        nlp_config=model.nlp_config(batching=True),
    )

    builder.execute_queries(mock_db_config, None)
    assert builder.stats.got_response[0] == 4

    rows = read_rows(mock_db_config, "example_nlp__nlp_task_gpt4o")
    assert [row["result"] for row in rows] == [
        {"ignored": "w1"},
        {"ignored": "w2"},
        {"ignored": "w3"},
        {"ignored": "w4"},
    ]


@nlp_utils.mock_env("azure")
@mock.patch("openai.AzureOpenAI")
def test_azure_batches_with_bad_notes(mock_client, tmp_path, mock_db_config):
    """Just confirm that the batch flow handles it gracefully too, since it iterates notes"""
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client, provider="azure", model_id="gpt4o")

    with open(f"{tmp_path}/dxr.ndjson", "w", encoding="utf8") as f:
        add_dxr("hello1", None, f)
        add_dxr("hello2", "world2", f)
    note_source = note_utils.NoteSource([tmp_path])

    model.openai.files.create.return_value = SimpleNamespace(id="input1")
    model.openai.batches.create.return_value = SimpleNamespace(id="batch1")
    model.openai.batches.retrieve.return_value = SimpleNamespace(
        id="batch1", status="completed", error_file_id=None, output_file_id="output1"
    )
    model.openai.files.content.return_value = openai.HttpxBinaryResponseContent(
        httpx.Response(status_code=200, text=batch_line("world2", answer="w2")),
    )

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path,
        notes=note_source,
        nlp_config=model.nlp_config(batching=True),
    )

    builder.execute_queries(mock_db_config, None)
    assert builder.stats.available == 2
    assert builder.stats.got_response[0] == 1


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


####################
# Concurrency tests
####################


# Helpers
def _write_notes(tmp_path, count: int) -> note_utils.NoteSource:
    """Drops `count` distinct notes on disk, named so responses can be keyed to them."""
    with open(f"{tmp_path}/doc.ndjson", "w", encoding="utf8") as f:
        for index in range(count):
            add_doc(str(index), f"Note {index}", f)
    return note_utils.NoteSource([tmp_path])


def _run(tmp_path, config, source, mock_db_config) -> nlp_builder.NlpBuilder:
    builder = nlp_builder.NlpBuilder(
        toml_config_path=nlp_utils.basic_workflow(tmp_path),
        notes=source,
        nlp_config=config,
    )
    builder.execute_queries(mock_db_config, None)
    return builder


def test_concurrency_defaults_to_one_worker_per_deployment():
    """The default should parallelize across deployments without needing a second flag."""
    assert note_utils.NlpConfig({}).concurrency == 1
    assert note_utils.NlpConfig({"azure_deployments": ["a", "b", "c"]}).concurrency == 3
    # An explicit value always wins, so a single endpoint can still be driven concurrently.
    assert note_utils.NlpConfig({"nlp_concurrency": 8}).concurrency == 8
    assert note_utils.NlpConfig({"azure_deployments": ["a"], "nlp_concurrency": 4}).concurrency == 4


@nlp_utils.mock_env("azure")
@mock.patch("openai.AzureOpenAI")
def test_requests_spread_across_deployments(mock_client, tmp_path, mock_db_config):
    """Every deployment should get work, and no note should be sent to more than one."""
    source = _write_notes(tmp_path, 12)
    model = nlp_utils.MockModel(mock_client, provider="azure")

    # Let's create a little handler that records which deployment got which note,
    # so we can confirm the work was spread.
    lock = threading.Lock()
    seen = []

    def handler(**kwargs):
        with lock:
            seen.append((kwargs["model"], model.note_text_of(kwargs)))
        return {}

    # Add this handler to the mock model, so every request will go through it.
    model.mock_openai_handler(handler)

    config = model.nlp_config(deployments=["dep-a", "dep-b", "dep-c"])
    builder = _run(tmp_path, config, source, mock_db_config)

    # We should have gotten a response for every note, and nothing should have been dropped.
    assert builder.stats.got_response[0] == 12
    # Each deployment pulled some work off the shared queue.
    assert {deployment for deployment, _ in seen} == {"dep-a", "dep-b", "dep-c"}
    # And each note was requested exactly once, not once per deployment.
    notes = sorted(note for _, note in seen)
    assert notes == sorted(f"Note {index}" for index in range(12))


@nlp_utils.mock_env("azure")
@mock.patch("openai.AzureOpenAI")
def test_never_exceeds_configured_concurrency(mock_client, tmp_path, mock_db_config):
    """In-flight requests must stay under our concurrency limit."""
    source = _write_notes(tmp_path, 20)
    model = nlp_utils.MockModel(mock_client, provider="azure")

    in_flight = 0
    max_inflight = 0
    lock = threading.Lock()

    def handler(**kwargs):
        # Reference the outer variables so we can update them in this closure.
        nonlocal in_flight, max_inflight
        with lock:
            in_flight += 1
            max_inflight = max(max_inflight, in_flight)
        time.sleep(0.01)  # hold the slot long enough for overlap to actually show up
        with lock:
            in_flight -= 1
        return {}

    model.mock_openai_handler(handler)

    config = model.nlp_config(deployments=["dep-a", "dep-b"], concurrency=4)
    builder = _run(tmp_path, config, source, mock_db_config)

    assert builder.stats.got_response[0] == 20
    # Confirm that we never exceeded the configured concurrency limit.
    assert max_inflight <= config.concurrency
    # And confirm we really did run in parallel, otherwise the cap check proves nothing.
    assert max_inflight > 1


@nlp_utils.mock_env("azure")
@mock.patch("openai.AzureOpenAI")
def test_output_is_identical_regardless_of_concurrency(mock_client, tmp_path, mock_db_config):
    """Rows must land in submission order, no matter which worker finished first."""

    def run_once(sub_dir: pathlib.Path, concurrency: int) -> tuple[list[str], list[str]]:
        sub_dir.mkdir()
        source = _write_notes(sub_dir, 15)
        model = nlp_utils.MockModel(mock_client, provider="azure")

        finished = []
        lock = threading.Lock()

        def handler(**kwargs):
            # Make later notes finish *sooner*, so completion order actively fights
            # submission order. That's the thing the ordered drain has to paper over.
            note = model.note_text_of(kwargs)
            # All these dummy notes are just "Note x", so we can parse the index out of the string.
            index = int(note.removeprefix("Note "))
            # The sleep time is chosen so that every 5th note will finish first,
            # then the next 5th, etc.
            time.sleep((4 - (index % 5)) * 0.02)
            with lock:
                finished.append(note)
            return {}

        model.mock_openai_handler(handler)
        config = model.nlp_config(deployments=["dep-a", "dep-b"], concurrency=concurrency)
        _run(sub_dir, config, source, mock_db_config)

        folder = driver.output_path_for_task(
            config, "task", SimpleNamespace(version=0), mock_db_config.db
        )
        rows = []
        for path in sorted(str(p) for p in folder.ls()):
            rows.extend(pandas.read_parquet(path).to_dict(orient="records"))
        return [row["note_ref"] for row in rows], finished

    serial, serial_finished = run_once(tmp_path / "serial", 1)
    parallel, parallel_finished = run_once(tmp_path / "parallel", 4)

    # Sanity check that this test can actually fail: the concurrent run really did complete
    # requests out of order, while the serial run by definition did not.
    assert parallel_finished != serial_finished
    # Yet both wrote their rows in note order, which is the order they were submitted in.
    expected = [f"DocumentReference/{index}" for index in range(15)]
    assert serial == expected
    assert parallel == expected


@nlp_utils.mock_env("azure")
@mock.patch("openai.AzureOpenAI")
def test_rate_limit_retries_after_cooldown(mock_client, tmp_path, mock_db_config):
    """A throttled note should be retried, not dropped."""
    source = _write_notes(tmp_path, 3)
    model = nlp_utils.MockModel(mock_client, provider="azure")

    failed_once = set()
    lock = threading.Lock()

    def handler(**kwargs):
        note = model.note_text_of(kwargs)
        with lock:
            first_time = note not in failed_once
            # After the first failure, the note will succeed on retry.
            failed_once.add(note)
        if first_time:
            raise openai.RateLimitError(
                "slow down",
                response=httpx.Response(429, request=httpx.Request("POST", "/")),
                body=None,
            )
        return {}

    model.mock_openai_handler(handler)

    config = model.nlp_config(concurrency=2)
    # The default cooldown is 5 seconds; we patch it down to 0.01s so the test runs quickly.
    with mock.patch.object(nlp_dispatch, "DEFAULT_COOLDOWN_SECONDS", 0.01):
        builder = _run(tmp_path, config, source, mock_db_config)

    # Every note eventually succeeded, and nothing was reported as dropped.
    assert builder.stats.got_response[0] == 3
    assert builder.stats.throttle_dropped == 0


@nlp_utils.mock_env("azure")
@mock.patch("openai.AzureOpenAI")
def test_persistent_rate_limiting_is_reported(mock_client, tmp_path, mock_db_config):
    """Giving up on a note has to be loud - a silent partial table is the bad outcome."""
    source = _write_notes(tmp_path, 2)
    model = nlp_utils.MockModel(mock_client, provider="azure")

    def handler(**kwargs):
        # Always fail, so the dispatcher will eventually give up and drop the note.
        raise openai.RateLimitError(
            "slow down", response=httpx.Response(429, request=httpx.Request("POST", "/")), body=None
        )

    model.mock_openai_handler(handler)

    config = model.nlp_config(concurrency=2)
    console_output = io.StringIO()
    # The default cooldown is 5 seconds; we patch it down to 0.01s so the test runs quickly.
    with mock.patch.object(nlp_dispatch, "DEFAULT_COOLDOWN_SECONDS", 0.01):
        with contextlib.redirect_stdout(console_output):
            builder = _run(tmp_path, config, source, mock_db_config)

    assert builder.stats.got_response[0] == 0
    assert builder.stats.throttle_dropped == 2
    output = console_output.getvalue()
    assert "2 notes dropped after repeated rate limiting" in output
    assert "--nlp-concurrency" in output


@nlp_utils.mock_env("azure")
@mock.patch("openai.AzureOpenAI")
def test_non_rate_limit_errors_are_not_retried(mock_client, tmp_path, mock_db_config):
    """Only throttling earns a retry, not other errors"""
    source = _write_notes(tmp_path, 2)
    model = nlp_utils.MockModel(mock_client, provider="azure")

    calls = []
    lock = threading.Lock()

    def handler(**kwargs):
        with lock:
            calls.append(model.note_text_of(kwargs))
        # Raise a generic API error, which is not retried by the dispatcher.
        raise openai.APIError("nope", mock.MagicMock(), body=None)

    model.mock_openai_handler(handler)

    config = model.nlp_config(concurrency=2)
    console_output = io.StringIO()
    with contextlib.redirect_stdout(console_output):
        builder = _run(tmp_path, config, source, mock_db_config)

    assert builder.stats.got_response[0] == 0
    assert builder.stats.throttle_dropped == 0  # these weren't throttles that dropped
    assert len(calls) == 2  # one attempt each, no retries
    assert "Failed to process note:" in console_output.getvalue()


@nlp_utils.mock_env("azure")
@mock.patch("openai.AzureOpenAI")
def test_batching_rejects_multiple_deployments(mock_client, tmp_path, mock_db_config, note_source):
    """Batch resume state is keyed per-provider, so several deployments would collide."""
    model = nlp_utils.MockModel(mock_client, provider="azure", model_id="gpt4o")
    config = model.nlp_config(batching=True, deployments=["dep-a", "dep-b"])

    builder = nlp_builder.NlpBuilder(
        toml_config_path=nlp_utils.basic_workflow(tmp_path),
        notes=note_source,
        nlp_config=config,
    )
    with pytest.raises(errors.CumulusLibraryError, match="does not support multiple"):
        builder.execute_queries(mock_db_config, None)


@mock.patch("openai.AzureOpenAI")
def test_token_stats_summed_across_deployments(mock_client, tmp_path, mock_db_config):
    """Usage from every endpoint has to roll up, or cost estimates read low."""
    source = _write_notes(tmp_path, 6)
    model = nlp_utils.MockModel(mock_client, provider="azure", model_id="gpt4o")
    model.mock_openai_handler(lambda **kwargs: {})

    config = model.nlp_config(deployments=["dep-a", "dep-b", "dep-c"])
    builder = _run(tmp_path, config, source, mock_db_config)

    # The mock reports 19 prompt tokens (5 cached) and 10 completion tokens per request.
    assert builder.stats.got_response[0] == 6
    assert builder.stats.token_stats.output_tokens == 60
    assert builder.stats.token_stats.cache_read_input_tokens == 30
    assert builder.stats.token_stats.new_input_tokens == 6 * (19 - 5)


@mock.patch("boto3.client")
def test_bedrock_throttling_is_recognized_and_retried(mock_client, tmp_path, mock_db_config):
    """Bedrock signals rate limits with a botocore ClientError, not an openai exception.

    Worth an end-to-end test rather than just a unit test of the predicate, because the whole
    point is that the dispatcher's cooldown path triggers for a completely different error
    shape than the Azure/local one.
    """
    source = _write_notes(tmp_path, 3)
    model = nlp_utils.MockModel(mock_client, provider="bedrock")

    throttled = set()
    lock = threading.Lock()
    ok_response = model._boto.converse.return_value

    def converse(**kwargs):
        # The note text is the only per-request thing we can key on here.
        note = kwargs["messages"][0]["content"][0]["text"]
        with lock:
            first_time = note not in throttled
            throttled.add(note)
        if first_time:
            raise botocore.exceptions.ClientError(
                {"Error": {"Code": "ThrottlingException", "Message": "slow down"}}, "Converse"
            )
        return ok_response

    model._boto.converse.side_effect = converse

    config = model.nlp_config(concurrency=2)
    with mock.patch.object(nlp_dispatch, "DEFAULT_COOLDOWN_SECONDS", 0.01):
        builder = _run(tmp_path, config, source, mock_db_config)

    # Each note was throttled once, backed off, and then succeeded.
    assert builder.stats.got_response[0] == 3
    assert builder.stats.throttle_dropped == 0


def test_bedrock_non_throttle_errors_are_not_treated_as_rate_limits():
    """Only the throttling codes earn a retry - everything else is a real failure."""

    def client_error(code):
        return botocore.exceptions.ClientError({"Error": {"Code": code}}, "Converse")

    assert models.is_rate_limit_error(client_error("ThrottlingException"))
    assert models.is_rate_limit_error(client_error("TooManyRequestsException"))
    assert not models.is_rate_limit_error(client_error("AccessDeniedException"))
    assert not models.is_rate_limit_error(ValueError("something else entirely"))


def test_retry_after_header_drives_cooldown():
    """A server-suggested wait should win over our default cooldown."""

    def rate_limit(headers: dict | None = None):
        return openai.RateLimitError(
            "slow down plz and thx",
            response=httpx.Response(429, headers=headers or {}, request=httpx.Request("POST", "/")),
            body=None,
        )

    assert models.retry_after_seconds(rate_limit({"Retry-After": "0.25"})) == 0.25
    # An HTTP-date is legal in Retry-After, but we don't parse those - fall back to the default.
    http_date = rate_limit({"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"})
    assert models.retry_after_seconds(http_date) is None
    # And a response that made no suggestion at all leaves us to pick - whether it carried no
    # headers whatsoever, or headers that simply didn't include one.
    assert models.retry_after_seconds(rate_limit()) is None
    assert models.retry_after_seconds(rate_limit({"x-request-id": "abc"})) is None

    # Confirm the suggestion actually reaches the endpoint, rather than the default. The bound
    # is loose on purpose - what matters is that we waited ~0.25s and not DEFAULT_COOLDOWN's 5.
    endpoint = nlp_dispatch.Endpoint(model=mock.MagicMock(), name="dep-a")
    before = time.monotonic()
    endpoint.start_cooldown(models.retry_after_seconds(rate_limit({"Retry-After": "0.25"})))
    assert 0 < endpoint.cooldown_until - before < 1

    # A server asking for longer than we're willing to wait gets clamped.
    endpoint.start_cooldown(nlp_dispatch.MAX_COOLDOWN_SECONDS * 10)
    assert endpoint.cooldown_until - time.monotonic() <= nlp_dispatch.MAX_COOLDOWN_SECONDS


@nlp_utils.mock_env("azure")
@mock.patch("openai.AzureOpenAI")
def test_note_that_cannot_be_prompted_is_skipped(mock_client, tmp_path, mock_db_config):
    """A note that fails before it is ever queued shouldn't take the run down with it."""
    source = _write_notes(tmp_path, 3)
    model = nlp_utils.MockModel(mock_client, provider="azure")
    model.mock_openai_handler(lambda **kwargs: {})

    real_make_prompt = driver.NlpNotePool._make_prompt
    seen = []

    def flaky_make_prompt(self, table_slug, task, text):
        seen.append(text)
        if len(seen) == 2:  # blow up on the second note only
            raise RuntimeError("bad prompt")
        return real_make_prompt(self, table_slug, task, text)

    # Capture the console output so we can confirm the error was logged, but not fatal.
    console_output = io.StringIO()
    with mock.patch.object(driver.NlpNotePool, "_make_prompt", flaky_make_prompt):
        with contextlib.redirect_stdout(console_output):
            builder = _run(tmp_path, model.nlp_config(), source, mock_db_config)

    # The other two notes still made it through.
    assert builder.stats.got_response[0] == 2
    assert "Failed to process note: bad prompt" in console_output.getvalue()
