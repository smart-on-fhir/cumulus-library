import base64
import binascii
import contextlib
import io
import json
import os
from unittest import mock

import cumulus_fhir_support as cfs
import fsspec.implementations.memory
import pandas
import pytest
import respx

import cumulus_library
from cumulus_library import cli, databases, errors, note_utils
from cumulus_library.builders import nlp_builder
from tests import conftest, nlp_utils
from tests.conftest import duckdb_args

SALT_STR = "e359191164cd209708d93551f481edd048946a9d844c51dea1b64d3f83dfd1fa"
SALT_BYTES = binascii.unhexlify(SALT_STR)


def add_doc(id_val: str, text: str | None, file) -> None:
    doc = {
        "resourceType": "DocumentReference",
        "id": id_val,
        "context": {"encounter": [{"reference": "Encounter/enc1"}]},
    }
    if text is not None:
        doc["content"] = [
            {
                "attachment": {
                    "contentType": "text/plain",
                    "data": base64.standard_b64encode(text.encode()).decode(),
                },
            },
        ]
    json.dump(doc, file)
    file.write("\n")


def add_dxr(id_val: str, text: str | None, file) -> None:
    dxr = {
        "resourceType": "DiagnosticReport",
        "id": id_val,
        "encounter": {"reference": "Encounter/enc1"},
    }
    if text is not None:
        dxr["presentedForm"] = [
            {
                "contentType": "text/plain",
                "data": base64.standard_b64encode(text.encode()).decode(),
            }
        ]
    json.dump(dxr, file)
    file.write("\n")


@pytest.fixture
def note_source(tmp_path) -> note_utils.NoteSource:
    """Just make a sample note source with a row - contents not important"""
    with open(f"{tmp_path}/dxr.ndjson", "w", encoding="utf8") as f:
        add_dxr("hello", "hello world", f)
    yield note_utils.NoteSource([tmp_path])


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


def test_empty_note_dir(tmp_path):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    with pytest.raises(SystemExit, match="there are no notes to work with"):
        nlp_builder.NlpBuilder(toml_config_path=workflow_path, notes=note_utils.NoteSource())


def test_table_filter_but_no_salt(tmp_path, note_source, mock_db_config):
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
        builder.execute_queries(mock_db_config, None)


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


@respx.mock
def test_filter(tmp_path, mock_db_config):
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
    model = nlp_utils.MockModel()

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=source, nlp_config=model.nlp_config()
    )

    console_output = io.StringIO()
    with contextlib.redirect_stdout(console_output):
        builder.execute_queries(mock_db_config, None)
    assert expected_stats in console_output.getvalue()


@respx.mock
def test_already_uploaded(tmp_path, mock_db_config, note_source):
    """Verify that we skip notes that we've already uploaded before"""
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel()

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


@mock.patch.dict(os.environ, clear=True)
@mock.patch("cumulus_library.builders.nlp_builder.NlpBuilder")
def test_args_passed_down(mock_builder, tmp_path):
    os.makedirs(f"{tmp_path}/notes")
    with open(f"{tmp_path}/notes/dxr.ndjson", "w", encoding="utf8") as f:
        dxr = {"resourceType": "DiagnosticReport", "id": "1"}
        json.dump(dxr, f)

    mock_builder.side_effect = RuntimeError("nope")

    mock_model = nlp_utils.MockModel()

    build_args = duckdb_args(
        [
            "build",
            str(tmp_path),
            "--target=example_nlp",
            "--stage=nlp",
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


def test_unreachable_vllm(tmp_path, note_source, mock_db_config):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel()
    model.mock_openai_model_list(status_code=500)
    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )
    with pytest.raises(errors.CumulusLibraryError, match="Try running 'docker compose up"):
        builder.execute_queries(mock_db_config, None)


@respx.mock
def test_cached_response(tmp_path, mock_db_config):
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

    model = nlp_utils.MockModel()
    model.mock_openai_response({"hello": 3})

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=source, nlp_config=model.nlp_config()
    )
    builder.execute_queries(mock_db_config, None)

    assert builder.stats.got_response[0] == 1

    # Confirm that we cache the response and don't hit the endpoint again
    model.mock_openai_response({}, status_code=500)

    # Add a new note to sanity check that we do actually fail on the new one
    with open(f"{tmp_path}/dxr.ndjson", "a", encoding="utf8") as f:
        add_dxr("2", "goodbye", f)

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=source, nlp_config=model.nlp_config()
    )
    builder.execute_queries(mock_db_config, None)
    assert builder.stats.considered[0] == 2
    assert builder.stats.got_response[0] == 1  # still got our cached result


@respx.mock
def test_span_correction(tmp_path, mock_db_config):
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

    model = nlp_utils.MockModel()
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


@respx.mock
def test_writes_out_at_note_limit(tmp_path, mock_db_config):
    with open(f"{tmp_path}/doc.ndjson", "w", encoding="utf8") as f:
        add_doc("1", "Note one", f)
        add_doc("2", "Note two", f)
        add_doc("3", "Note three", f)

    source = note_utils.NoteSource([tmp_path])
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel()
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
    assert "Failed to process note: test2" in console_output.getvalue()


@respx.mock
def test_various_value_types(tmp_path, mock_db_config, note_source):
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
    model = nlp_utils.MockModel()
    model.mock_openai_response(results)

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path,
        notes=note_source,
        nlp_config=model.nlp_config(),
    )
    builder.execute_queries(mock_db_config, None)

    rows = read_rows(mock_db_config, "test__task")
    assert rows[0]["result"] == results


@respx.mock
def test_no_batching_support(tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel()

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path,
        notes=note_source,
        nlp_config=model.nlp_config(batching=True),
    )

    with pytest.raises(errors.CumulusLibraryError, match="does not support batching"):
        builder.execute_queries(mock_db_config, None)


@respx.mock
def test_no_phi_dir(tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel()
    config = model.nlp_config()
    config.phi_dir = None

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=config
    )

    with pytest.raises(errors.CumulusLibraryError, match="Please provide a PHI dir"):
        builder.execute_queries(mock_db_config, None)


@respx.mock
def test_bad_nlp_model(tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel()
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


@respx.mock
def test_missing_nlp_model(tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel()
    model.mock_openai_model_list(models=[])

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    with pytest.raises(errors.CumulusLibraryError, match="NLP server does not have model ID"):
        builder.execute_queries(mock_db_config, None)


@respx.mock
def test_bad_stop(tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel()
    model.mock_openai_response({}, finish_reason="bad_reason")

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    console_output = io.StringIO()
    with contextlib.redirect_stdout(console_output):
        builder.execute_queries(mock_db_config, None)

    assert builder.stats.got_response[0] == 0
    assert "did not complete, with finish reason: bad_reason" in console_output.getvalue()


@respx.mock
def test_disabling_stats(tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel()

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config(stats=False)
    )

    console_output = io.StringIO()
    with contextlib.redirect_stdout(console_output):
        builder.execute_queries(mock_db_config, None)

    assert builder.stats.got_response[0] == 1
    assert "Notes processed:" not in console_output.getvalue()
    assert "Token usage:" not in console_output.getvalue()


@respx.mock
def test_cloud_model_but_local_provider(tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(model_id="gpt5")

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    with pytest.raises(errors.CumulusLibraryError, match="does not support the 'local' provider"):
        builder.execute_queries(mock_db_config, None)


@respx.mock
@nlp_utils.mock_env("azure")
def test_azure_happy_path(tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(provider="azure")

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    builder.execute_queries(mock_db_config, None)
    assert builder.stats.got_response[0] == 1


def test_azure_bad_model(tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(model_id="claude-sonnet45", provider="azure")

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    with pytest.raises(errors.CumulusLibraryError, match="does not support the 'azure' provider"):
        builder.execute_queries(mock_db_config, None)


def test_azure_no_env(tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(provider="azure")

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    with pytest.raises(errors.CumulusLibraryError, match="Missing Azure environment variables"):
        builder.execute_queries(mock_db_config, None)


@respx.mock
@nlp_utils.mock_env("azure")
def test_azure_no_schema_support(tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(provider="azure", model_id="gpt35")

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path,
        notes=note_source,
        nlp_config=model.nlp_config(),
    )

    builder.execute_queries(mock_db_config, None)

    # Confirm that we requested just "give us json please" if model doesn't support schemas
    last_json = json.loads(respx.calls.last.request.content)
    assert last_json["response_format"] == {"type": "json_object"}


def test_bedrock_happy_path(tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(provider="bedrock", model_id="claude-sonnet45")

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    builder.execute_queries(mock_db_config, None)
    assert builder.stats.got_response[0] == 1


def test_bedrock_bad_stop(tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(provider="bedrock")
    model.mock_bedrock_response({}, stop_reason="bad_reason")

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    console_output = io.StringIO()
    with contextlib.redirect_stdout(console_output):
        builder.execute_queries(mock_db_config, None)

    assert builder.stats.got_response[0] == 0
    assert "did not complete, with stop reason: bad_reason" in console_output.getvalue()


def test_bedrock_bad_model(tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(provider="bedrock", model_id="gpt5")

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    with pytest.raises(errors.CumulusLibraryError, match="does not support the 'bedrock' provider"):
        builder.execute_queries(mock_db_config, None)


def test_bedrock_skips_wrapper_in_response(tmp_path, mock_db_config, note_source):
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

    model = nlp_utils.MockModel(provider="bedrock")
    model.mock_bedrock_response({"parameter": {"hello": "world"}})

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )
    builder.execute_queries(mock_db_config, None)

    rows = read_rows(mock_db_config, "test__hello_world")
    assert rows[0]["result"] == {"hello": "world"}


def test_bedrock_text_response(tmp_path, mock_db_config, note_source):
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

    model = nlp_utils.MockModel(provider="bedrock")
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


def test_bedrock_no_response(tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(provider="bedrock")
    model.mock_bedrock_response("", mode="none")

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    console_output = io.StringIO()
    with contextlib.redirect_stdout(console_output):
        builder.execute_queries(mock_db_config, None)

    assert builder.stats.got_response[0] == 0
    assert "Failed to process note: no response content found" in console_output.getvalue()


@respx.mock
@nlp_utils.mock_env()
@mock.patch("botocore.client")
def test_write_to_athena(mock_client, tmp_path, note_source):
    db = databases.AthenaDatabaseBackend(
        region="test",
        work_group="test",
        profile="test",
        schema_name="testdb",
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
    model = nlp_utils.MockModel()

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
        'CREATE TABLE IF NOT EXISTS "main"."test__task" AS SELECT "note_ref", '
        '"encounter_ref", "subject_ref", "generated_on", "task_version", "model", '
        '"system_fingerprint", "result"\n'
        "FROM read_parquet('memory://s3://testbucket/athena/cumulus_user_uploads/"
        "testdb/test/task_v0/*.parquet')"
    ]
