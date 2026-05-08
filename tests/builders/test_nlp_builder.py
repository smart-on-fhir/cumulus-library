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
from types import SimpleNamespace
from unittest import mock

import cumulus_fhir_support as cfs
import fsspec.implementations.memory
import httpx
import openai
import pandas
import pytest

import cumulus_library
from cumulus_library import cli, databases, errors, note_utils
from cumulus_library.builders import nlp_builder
from cumulus_library.builders.nlp.models import OpenAIProvider
from tests import conftest, nlp_utils
from tests.conftest import duckdb_args
from tests.nlp_utils import add_doc, add_dxr

SALT_STR = "e359191164cd209708d93551f481edd048946a9d844c51dea1b64d3f83dfd1fa"
SALT_BYTES = binascii.unhexlify(SALT_STR)


@pytest.fixture
def note_source(tmp_path) -> note_utils.NoteSource:
    """Just make a sample note source with a row - contents not important"""
    with open(f"{tmp_path}/dxr.ndjson", "w", encoding="utf8") as f:
        add_dxr("hello", "hello world", f)
    yield note_utils.NoteSource([tmp_path])


def read_rows(db, table: str) -> list[dict]:
    df = db.db.connection.sql(f"SELECT * FROM {table} ORDER BY note_ref").df()
    return json.loads(df.to_json(orient="records"))


@pytest.mark.xdist_group(name="nlp_builder")
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


@pytest.mark.xdist_group(name="nlp_builder")
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


@pytest.mark.xdist_group(name="nlp_builder")
def test_sketch_schema_path(tmp_path, note_source):
    workflow_path = conftest.write_toml(
        tmp_path,
        {
            "config_type": "nlp",
            "tables": {"test": {"response_schema": "../../../passwd"}},
        },
        "nlp.workflow",
    )
    with pytest.raises(ValueError, match="response_schema must be a simple filename"):
        nlp_builder.NlpBuilder(toml_config_path=workflow_path, notes=note_source)


@pytest.mark.xdist_group(name="nlp_builder")
def test_empty_note_dir(tmp_path):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    with pytest.raises(SystemExit, match="there are no notes to work with"):
        nlp_builder.NlpBuilder(toml_config_path=workflow_path, notes=note_utils.NoteSource())


@pytest.mark.xdist_group(name="nlp_builder")
@nlp_utils.mock_env()
def test_table_filter_but_no_salt(tmp_path, note_source):
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
    builder = nlp_builder.NlpBuilder(toml_config_path=workflow_path, notes=note_source)
    err_msg = "Cannot calculate anonymized resource IDs without a PHI dir defined"
    with pytest.raises(RuntimeError, match=err_msg):
        builder.execute_queries(study_config, None)


@pytest.mark.xdist_group(name="nlp_builder")
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


@pytest.mark.xdist_group(name="nlp_builder")
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


@pytest.mark.xdist_group(name="nlp_builder")
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


@pytest.mark.xdist_group(name="nlp_builder")
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
            *mock_model.cli_args(),
        ],
        tmp_path,
    )

    with pytest.raises(RuntimeError, match="nope"):
        cli.main(cli_args=build_args)

    config = mock_builder.call_args[1]["nlp_config"]
    assert config.salt == SALT_BYTES

    source = mock_builder.call_args[1]["notes"]
    assert list(source.progress_iter("label")) == [dxr]


@pytest.mark.xdist_group(name="nlp_builder")
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


@pytest.mark.xdist_group(name="nlp_builder")
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


@pytest.mark.xdist_group(name="nlp_builder")
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

    rows = read_rows(mock_db_config, "test__hello_world")
    assert rows[0]["result"] == {"parent_list": [{"parent_dict": {"spans": [[0, 5], [7, 21]]}}]}

    failure_msg = "Could not match span received from NLP server for DiagnosticReport/dxr: forth"
    assert failure_msg in console_output.getvalue()


@pytest.mark.xdist_group(name="nlp_builder")
@mock.patch("openai.OpenAI")
def test_writes_out_at_note_limit(mock_client, tmp_path, mock_db_config):
    with open(f"{tmp_path}/doc.ndjson", "w", encoding="utf8") as f:
        add_doc("1", "Note one", f)
        add_doc("2", "Note two", f)
        add_doc("3", "Note three", f)

    source = note_utils.NoteSource([tmp_path])
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client)
    config = model.nlp_config()
    config.note_limit = 2

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


@pytest.mark.xdist_group(name="nlp_builder")
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

    rows = read_rows(mock_db_config, "test__task")
    assert rows[0]["result"] == results


@pytest.mark.xdist_group(name="nlp_builder")
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


@pytest.mark.xdist_group(name="nlp_builder")
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


@pytest.mark.xdist_group(name="nlp_builder")
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


@pytest.mark.xdist_group(name="nlp_builder")
@mock.patch("openai.OpenAI")
def test_missing_nlp_model(mock_client, tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client)
    model.mock_openai_model_list(models=[])

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    with pytest.raises(errors.CumulusLibraryError, match="NLP server does not have model ID"):
        builder.execute_queries(mock_db_config, None)


@pytest.mark.xdist_group(name="nlp_builder")
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


@pytest.mark.xdist_group(name="nlp_builder")
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


@pytest.mark.xdist_group(name="nlp_builder")
@mock.patch("openai.OpenAI")
def test_cloud_model_but_local_provider(mock_client, tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client, model_id="gpt5")

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    with pytest.raises(errors.CumulusLibraryError, match="does not support the 'local' provider"):
        builder.execute_queries(mock_db_config, None)


@pytest.mark.xdist_group(name="nlp_builder")
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


@pytest.mark.xdist_group(name="nlp_builder")
@mock.patch("openai.AzureOpenAI")
def test_azure_bad_model(mock_client, tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client, model_id="claude-sonnet45", provider="azure")

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    with pytest.raises(errors.CumulusLibraryError, match="does not support the 'azure' provider"):
        builder.execute_queries(mock_db_config, None)


@pytest.mark.xdist_group(name="nlp_builder")
@mock.patch("openai.AzureOpenAI")
def test_azure_no_env(mock_client, tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client, provider="azure")

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    with pytest.raises(errors.CumulusLibraryError, match="Missing Azure environment variables"):
        builder.execute_queries(mock_db_config, None)


@pytest.mark.xdist_group(name="nlp_builder")
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


@pytest.mark.xdist_group(name="nlp_builder")
@mock.patch("boto3.client")
def test_bedrock_happy_path(mock_client, tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client, provider="bedrock", model_id="claude-sonnet45")

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    builder.execute_queries(mock_db_config, None)
    assert builder.stats.got_response[0] == 1


@pytest.mark.xdist_group(name="nlp_builder")
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


@pytest.mark.xdist_group(name="nlp_builder")
@mock.patch("boto3.client")
def test_bedrock_bad_model(mock_client, tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client, provider="bedrock", model_id="gpt5")

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    with pytest.raises(errors.CumulusLibraryError, match="does not support the 'bedrock' provider"):
        builder.execute_queries(mock_db_config, None)


@pytest.mark.xdist_group(name="nlp_builder")
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

    rows = read_rows(mock_db_config, "test__hello_world")
    assert rows[0]["result"] == {"hello": "world"}


@pytest.mark.xdist_group(name="nlp_builder")
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

    rows = read_rows(mock_db_config, "test__hello_world")
    assert rows[0]["result"] == {"hello": 0.5}


@pytest.mark.xdist_group(name="nlp_builder")
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


@pytest.mark.xdist_group(name="nlp_builder")
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

    # Confirm we wrote the parquet file out correctly
    path = "s3://testbucket/athena/cumulus_user_uploads/testdb/test/task_v0/nlp.0.parquet"
    with mem_fs.open(path, "rb") as f:
        df = pandas.read_parquet(f)
        rows = json.loads(df.to_json(orient="records"))

    assert len(rows) == 1
    assert rows[0]["note_ref"] == "DiagnosticReport/hello"

    # And the id file
    id_path = "s3://testbucket/athena/cumulus_user_uploads/testdb/test/task_v0.ids"
    with mem_fs.open(id_path, "r") as f:
        assert f.read() == "DiagnosticReport/hello\n"

    # And confirm the query looks right
    assert builder.queries == [
        "CREATE EXTERNAL TABLE IF NOT EXISTS `main`.`test__task` ( note_ref STRING, "
        "encounter_ref STRING, subject_ref STRING, generated_on STRING, task_version INT, "
        "model STRING, system_fingerprint STRING, result STRUCT<ignored: STRING>\n)\n"
        "STORED AS PARQUET\n"
        "LOCATION 'memory://s3://testbucket/athena/cumulus_user_uploads/"
        "testdb/test/task_v0'\n"
        'tblproperties ("parquet.compression"="SNAPPY");'
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


@pytest.mark.xdist_group(name="nlp_builder")
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

    rows = read_rows(mock_db_config, "test__task")
    assert rows[0]["result"] == {"ignored": "answer"}

    assert model.openai.batches.create.call_args_list[0][1] == {
        "completion_window": "24h",
        "endpoint": "/v1/chat/completions",
        "input_file_id": "input",
    }
    assert model.openai.batches.retrieve.call_args_list[0][1] == {
        "batch_id": "batch",
    }


@pytest.mark.xdist_group(name="nlp_builder")
@nlp_utils.mock_env("azure")
@mock.patch("openai.AzureOpenAI")
def test_azure_resume_batching(mock_client, tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client, provider="azure", model_id="gpt4o")

    path_dir = f"{model.phi}/nlp-cache/test__task_v0_gpt4o"
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

    rows = read_rows(mock_db_config, "test__task")
    assert rows[0]["result"] == {"ignored": "answer"}


@pytest.mark.xdist_group(name="nlp_builder")
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


@pytest.mark.xdist_group(name="nlp_builder")
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

    rows = read_rows(mock_db_config, "test__task")
    assert [row["result"] for row in rows] == [
        {"ignored": "w1"},
        {"ignored": "w2"},
        {"ignored": "w3"},
        {"ignored": "w4"},
    ]


@pytest.mark.xdist_group(name="nlp_builder")
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
