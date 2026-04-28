import base64
import binascii
import contextlib
import io
import json
import os
from unittest import mock

import cumulus_fhir_support as cfs
import pytest
import respx

from cumulus_library import cli, errors, note_utils
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


def test_task_without_name(tmp_path, note_source):
    workflow_path = conftest.write_toml(
        tmp_path,
        {
            "config_type": "nlp",
            "task": [{}],
        },
        "nlp.workflow",
    )
    with pytest.raises(ValueError, match="A task name must be provided"):
        nlp_builder.NlpBuilder(toml_config_path=workflow_path, notes=note_source)


def test_task_without_schema(tmp_path, note_source):
    workflow_path = conftest.write_toml(
        tmp_path,
        {
            "config_type": "nlp",
            "task": [{"name": "test"}],
        },
        "nlp.workflow",
    )
    with pytest.raises(ValueError, match="response schema must be provided for task 'test'"):
        nlp_builder.NlpBuilder(toml_config_path=workflow_path, notes=note_source)


def test_sketch_schema_path(tmp_path, note_source):
    workflow_path = conftest.write_toml(
        tmp_path,
        {
            "config_type": "nlp",
            "task": [{"name": "test", "response_schema": "../../../passwd"}],
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
            "task": [
                {
                    "name": "test",
                    "select_by_table": "table",
                    "response_schema": nlp_utils.EMPTY_SCHEMA,
                }
            ],
        },
        "nlp.workflow",
    )
    builder = nlp_builder.NlpBuilder(toml_config_path=workflow_path, notes=note_source)
    err_msg = "Cannot calculate anonymized resource IDs without a PHI dir defined"
    with pytest.raises(RuntimeError, match=err_msg):
        builder.prepare_queries(config=mock_db_config)


def test_flattened_config(tmp_path, note_source):
    workflow_path = conftest.write_toml(
        tmp_path,
        {
            "config_type": "nlp",
            "shared": {
                "system_prompt": "hello",
            },
            "task": [
                {
                    "name": "override",
                    "system_prompt": "bye",
                    "response_schema": nlp_utils.EMPTY_SCHEMA,
                },
                {
                    "name": "fallthrough",
                    "response_schema": nlp_utils.EMPTY_SCHEMA,
                },
            ],
        },
        "nlp.workflow",
    )
    builder = nlp_builder.NlpBuilder(toml_config_path=workflow_path, notes=note_source)
    assert builder._workflow_config.task[0].system_prompt == "bye"
    assert builder._workflow_config.task[1].system_prompt == "hello"


@respx.mock
def test_filter(tmp_path, mock_db_config):
    workflow_path = conftest.write_toml(
        tmp_path,
        {
            "config_type": "nlp",
            "task": [
                {
                    "name": "filtered",
                    "select_by_word": ["fever"],
                    "reject_by_word": ["cold"],
                    "select_by_table": "prev_table",
                    "response_schema": nlp_utils.EMPTY_SCHEMA,
                },
                {
                    "name": "all",
                    "response_schema": nlp_utils.EMPTY_SCHEMA,
                },
            ],
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
        builder.prepare_queries(config=mock_db_config)
    assert expected_stats in console_output.getvalue()


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
        builder.prepare_queries(config=mock_db_config)


@respx.mock
def test_cached_response(tmp_path, mock_db_config):
    workflow_path = conftest.write_toml(
        tmp_path,
        {
            "config_type": "nlp",
            "task": [
                {
                    "name": "hello_world",
                    "response_schema": '{"title":"test", "type": "object", '
                    '"properties": {"hello": {"type": "string"}}}',
                },
            ],
        },
        "nlp.workflow",
    )

    with open(f"{tmp_path}/dxr.ndjson", "w", encoding="utf8") as f:
        add_dxr("1", "say hello to the world", f)

    source = note_utils.NoteSource([tmp_path])

    model = nlp_utils.MockModel()
    model.mock_openai_response({"hello": "world"})

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=source, nlp_config=model.nlp_config()
    )
    builder.prepare_queries(config=mock_db_config)

    assert builder.stats.got_response[0] == 1

    # Confirm that we cache the response and don't hit the endpoint again
    # (and add a new note to sanity check that we do actually fail on the new one)
    model.mock_openai_response({}, status_code=500)

    with open(f"{tmp_path}/dxr.ndjson", "a", encoding="utf8") as f:
        add_dxr("2", "goodbye", f)

    builder.prepare_queries(config=mock_db_config)
    assert builder.stats.considered[0] == 2
    assert builder.stats.got_response[0] == 1  # still got our cached result


@respx.mock
def test_span_correction(tmp_path, mock_db_config):
    workflow_path = conftest.write_toml(
        tmp_path,
        {
            "config_type": "nlp",
            "task": [
                {
                    "name": "hello_world",
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
            ],
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
        builder.prepare_queries(config=mock_db_config)

    assert builder.stats.got_response[0] == 1
    assert "'spans': [(0, 5), (7, 21)]" in console_output.getvalue()
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

    with mock.patch("cumulus_library.builders.nlp.driver.NlpNotePool._write_to_disk") as mock_write:
        # Fake an error too, to confirm we gracefully handle that and print message
        mock_write.side_effect = [RuntimeError("test1"), RuntimeError("test2")]
        console_output = io.StringIO()
        with contextlib.redirect_stdout(console_output):
            builder.prepare_queries(config=mock_db_config)

    assert mock_write.call_count == 2
    assert "Failed to process note: test1" in console_output.getvalue()
    assert "Failed to process note: test2" in console_output.getvalue()


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
        builder.prepare_queries(config=mock_db_config)


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
        builder.prepare_queries(config=mock_db_config)


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
        builder.prepare_queries(config=mock_db_config)

    config.model = None
    with pytest.raises(errors.CumulusLibraryError, match="An NLP model ID must be provided"):
        builder.prepare_queries(config=mock_db_config)


@respx.mock
def test_missing_nlp_model(tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel()
    model.mock_openai_model_list(models=[])

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    with pytest.raises(errors.CumulusLibraryError, match="NLP server does not have model ID"):
        builder.prepare_queries(config=mock_db_config)


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
        builder.prepare_queries(config=mock_db_config)

    assert builder.stats.got_response[0] == 0
    assert "did not complete, with finish reason: bad_reason" in console_output.getvalue()


@respx.mock
def test_cloud_model_but_local_provider(tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(model_id="gpt5")

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    with pytest.raises(errors.CumulusLibraryError, match="does not support the 'local' provider"):
        builder.prepare_queries(config=mock_db_config)


@respx.mock
@nlp_utils.mock_env("azure")
def test_azure_happy_path(tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(provider="azure")

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    builder.prepare_queries(config=mock_db_config)
    assert builder.stats.got_response[0] == 1


def test_azure_bad_model(tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(model_id="claude-sonnet45", provider="azure")

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    with pytest.raises(errors.CumulusLibraryError, match="does not support the 'azure' provider"):
        builder.prepare_queries(config=mock_db_config)


def test_azure_no_env(tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(provider="azure")

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    with pytest.raises(errors.CumulusLibraryError, match="Missing Azure environment variables"):
        builder.prepare_queries(config=mock_db_config)


def test_bedrock_happy_path(tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(provider="bedrock", model_id="claude-sonnet45")

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    builder.prepare_queries(config=mock_db_config)
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
        builder.prepare_queries(config=mock_db_config)

    assert builder.stats.got_response[0] == 0
    assert "did not complete, with stop reason: bad_reason" in console_output.getvalue()


def test_bedrock_bad_model(tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(provider="bedrock", model_id="gpt5")

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    with pytest.raises(errors.CumulusLibraryError, match="does not support the 'bedrock' provider"):
        builder.prepare_queries(config=mock_db_config)


def test_bedrock_skips_wrapper_in_response(tmp_path, mock_db_config, note_source):
    """Confirm we drop a "parameter" wrapper object in response"""
    workflow_path = conftest.write_toml(
        tmp_path,
        {
            "config_type": "nlp",
            "task": [
                {
                    "name": "hello_world",
                    "response_schema": '{"title":"test", "type": "object", '
                    '"properties": {"hello": {"type": "string"}}}',
                }
            ],
        },
        "nlp.workflow",
    )

    model = nlp_utils.MockModel(provider="bedrock")
    model.mock_bedrock_response({"parameter": {"hello": "world"}})

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    console_output = io.StringIO()
    with contextlib.redirect_stdout(console_output):
        builder.prepare_queries(config=mock_db_config)

    assert builder.stats.got_response[0] == 1
    assert "'result': {'hello': 'world'}" in console_output.getvalue()


def test_bedrock_text_response(tmp_path, mock_db_config, note_source):
    """Confirm we find json inside a text response"""
    workflow_path = conftest.write_toml(
        tmp_path,
        {
            "config_type": "nlp",
            "task": [
                {
                    "name": "hello_world",
                    "response_schema": '{"title":"test", "type": "object", '
                    '"properties": {"hello": {"type": "string"}}}',
                }
            ],
        },
        "nlp.workflow",
    )

    model = nlp_utils.MockModel(provider="bedrock")
    model.mock_bedrock_response(
        """
Preamble...

```json
{"hello": "goodbye"}
```

Summary.
""",
        mode="text",
    )

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    console_output = io.StringIO()
    with contextlib.redirect_stdout(console_output):
        builder.prepare_queries(config=mock_db_config)

    assert builder.stats.got_response[0] == 1
    assert "'result': {'hello': 'goodbye'}" in console_output.getvalue()


def test_bedrock_no_response(tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(provider="bedrock")
    model.mock_bedrock_response("", mode="none")

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    console_output = io.StringIO()
    with contextlib.redirect_stdout(console_output):
        builder.prepare_queries(config=mock_db_config)

    assert builder.stats.got_response[0] == 0
    assert "Failed to process note: no response content found" in console_output.getvalue()
