"""
Tests for NLP models and providers (local, Azure, Bedrock): setup checks, request options, and
how each provider's responses and errors are handled.
"""

import contextlib
import io
from unittest import mock

import botocore.exceptions
import httpx
import openai
import pydantic
import pytest

from cumulus_library import errors
from cumulus_library.builders import nlp_builder
from cumulus_library.builders.nlp import models
from tests import conftest, nlp_utils
from tests.nlp_utils import read_rows


@pytest.fixture(autouse=True)
def _autouse_cache_dir(mock_cache_dir):
    """
    Autouse this fixture to keep NLP's on-disk cache inside tmp_path.
    Delegates to the shared ``mock_cache_dir`` fixture defined in (tests/conftest.py).
    """
    yield


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


@pytest.mark.parametrize(
    "model_id,sends_temperature",
    [
        ("gpt4o", True),
        # The reasoning models only accept their default temperature, and hard-error on ours.
        ("gpt54", False),
    ],
)
@nlp_utils.mock_env("azure")
@mock.patch("openai.AzureOpenAI")
def test_azure_temperature_support(
    mock_client, model_id, sends_temperature, tmp_path, mock_db_config, note_source
):
    """Only models that still accept a temperature should be sent one"""
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client, provider="azure", model_id=model_id)

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )
    builder.execute_queries(mock_db_config, None)
    assert builder.stats.got_response[0] == 1

    last_kwargs = model.openai.chat.completions.parse.call_args[1]
    assert ("temperature" in last_kwargs) is sends_temperature
    # Seed is honored everywhere, and is the only reproducibility lever left on the models that
    # dropped temperature - so it should go out with every request, whatever the model.
    assert last_kwargs["seed"] == 12345


@pytest.mark.parametrize(
    "model_id,inference_config",
    [
        ("claude-sonnet45", {"temperature": 0}),
        # Opus 4.7 and up hard-error on a temperature, and that was the only field we set,
        # so the whole inferenceConfig should be gone.
        ("claude-opus48", None),
    ],
)
@mock.patch("boto3.client")
def test_bedrock_temperature_support(
    mock_client, model_id, inference_config, tmp_path, mock_db_config, note_source
):
    """Only models that still accept a temperature should be sent one"""
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client, provider="bedrock", model_id=model_id)

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )
    builder.execute_queries(mock_db_config, None)
    assert builder.stats.got_response[0] == 1

    last_kwargs = model._boto.converse.call_args[1]
    assert last_kwargs.get("inferenceConfig") == inference_config
    # Dropping the temperature shouldn't have disturbed the rest of the request
    assert last_kwargs["modelId"].startswith("us.anthropic.")
    assert "toolConfig" in last_kwargs


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
    assert builder.stats.failed[0] == 1
    assert "Failed to process note: no response content found" in console_output.getvalue()


@mock.patch("boto3.client")
def test_bedrock_raises_exception_when_invalid_request(
    mock_client, tmp_path, mock_db_config, note_source
):
    mock_client.exceptions.ClientError = botocore.exceptions.ClientError
    mock_client.converse.side_effect = mock_client.exceptions.ClientError(
        {"Error": {"Code": 0, "Message": ""}}, ""
    )
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client, provider="bedrock")
    model._boto.converse.side_effect = mock_client.exceptions.ClientError(
        {"Error": {"Code": 0, "Message": ""}}, ""
    )
    model._boto.exceptions.ClientError = botocore.exceptions.ClientError

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )
    console_output = io.StringIO()

    with contextlib.redirect_stdout(console_output):
        builder.execute_queries(mock_db_config, None)
    output = console_output.getvalue()

    assert "Failed to process" in output
    # Our wrapped error, not the raw ClientError (so we know the request error handling ran)
    assert "Exception raised when handling hello world" in output


@pytest.mark.parametrize(
    "code,wrapped",
    [
        ("ValidationException", True),
        ("AccessDeniedException", True),
        # Rate limits must stay raw ClientErrors, or the dispatcher won't cool down and retry
        ("ThrottlingException", False),
        ("TooManyRequestsException", False),
    ],
)
@mock.patch("boto3.client")
def test_bedrock_client_errors_are_wrapped_unless_throttled(mock_client, code, wrapped):
    mock_client.return_value.exceptions.ClientError = botocore.exceptions.ClientError
    mock_client.return_value.converse.side_effect = botocore.exceptions.ClientError(
        {"Error": {"Code": code, "Message": "nope"}}, "Converse"
    )
    provider = models.BedrockProvider("test-model")

    class Answer(pydantic.BaseModel):
        ignored: str | None = None

    expected = errors.CumulusLibraryError if wrapped else botocore.exceptions.ClientError
    with pytest.raises(expected, match=code) as exc_info:
        provider.prompt("system", "my note", Answer)

    if wrapped:
        assert str(exc_info.value) == f"{code} Exception raised when handling my note:\n\nnope"
        assert isinstance(exc_info.value.__cause__, botocore.exceptions.ClientError)


@nlp_utils.mock_env("azure")
@mock.patch("openai.AzureOpenAI")
def test_openai_raises_exception_when_invalid_request(
    mock_client, tmp_path, mock_db_config, note_source
):
    exception = openai.BadRequestError(
        "",
        response=httpx.Response(
            status_code=400,
            json={
                "error": {
                    "message": "Invalid model ID or parameter",
                    "type": "invalid_request_error",
                    "param": "model",
                    "code": "invalid_model",
                }
            },
            request=httpx.Request("POST", "https://openai.com"),
        ),
        body="",
    )

    mock_client.chat.completions.parse.side_effect = exception
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client, provider="azure")
    model.openai.chat.completions.parse.side_effect = exception

    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=note_source, nlp_config=model.nlp_config()
    )

    console_output = io.StringIO()

    with contextlib.redirect_stdout(console_output):
        builder.execute_queries(mock_db_config, None)
    output = console_output.getvalue()

    assert "Failed to process" in output
