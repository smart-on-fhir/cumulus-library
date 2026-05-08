import base64
import functools
import json
import os
import pathlib
import tempfile
from types import SimpleNamespace
from unittest import mock

import openai
from openai.types import chat, completion_usage

from cumulus_library import note_utils
from tests import conftest

# Not fully empty, because parquet doesn't like that. But just throw an unused property in there.
EMPTY_SCHEMA = '{"title":"test", "type": "object", "properties": {"ignored": {"type": "string"}}}'


def mock_env(provider: str = "local"):
    if provider == "azure":
        new = {
            "AZURE_OPENAI_API_KEY": "azure-key",
            "AZURE_OPENAI_ENDPOINT": "https://example.com/azure",
        }
    else:
        new = {}

    def outer(func):
        @functools.wraps(func)
        def inner(*args, **kwargs):
            with mock.patch.dict(os.environ, new, clear=True):
                return func(*args, **kwargs)

        return inner

    return outer


def mock_model(model_id: str = "gpt-oss-120b", provider: str = "local"):
    def outer(func):
        def inner(*args, **kwargs):
            model = MockModel(model_id, provider)
            return func(*args, model, **kwargs)

        return inner

    return outer


class MockModel:
    def __init__(
        self,
        mock_client,
        model_id: str = "gpt-oss-120b",
        provider: str = "local",
        make_codebook: bool = True,
    ):
        self.model_id = model_id
        self.provider = provider

        # Create a codebook for any NLP we do, to keep consistent anon IDs
        self.tempdir = tempfile.TemporaryDirectory()
        self.phi = pathlib.Path(self.tempdir.name, "phi")
        self.phi.mkdir()
        if make_codebook:
            with open(self.phi / "codebook.json", "w", encoding="utf8") as f:
                salt = "e359191164cd209708d93551f481edd048946a9d844c51dea1b64d3f83dfd1fa"
                json.dump({"id_salt": salt}, f)

        # Determine the server to use for this model
        if provider == "bedrock":
            self._boto = mock.MagicMock()
            mock_client.return_value = self._boto
            self.mock_bedrock_response({})
        else:
            self.openai = mock.MagicMock()
            mock_client.return_value = self.openai

            # Do some basic always-on mocking
            self.mock_openai_model_list()
            self.mock_openai_response({})

    def cli_args(self, *args, **kwargs) -> list[str]:
        config = self.nlp_config(*args, **kwargs)
        args = [
            f"--nlp-model={config.model}",
            f"--nlp-provider={config.provider}",
            f"--etl-phi-dir={config.phi_dir}",
        ]
        if config.clean:
            args.append("--clean-nlp")
        if config.use_batching:
            args.append("--batch-nlp")
        if not config.show_stats:
            args.append("--no-nlp-stats")
        return args

    def nlp_config(
        self, batching: bool = False, clean: bool = True, stats: bool = True
    ) -> note_utils.NlpConfig:
        args = {
            "nlp_model": self.model_id,
            "nlp_provider": self.provider,
            "etl_phi_dir": self.phi,
            "target": "test",
            "batch_nlp": batching,
            "clean_nlp": clean,
            "nlp_stats": stats,
        }
        return note_utils.NlpConfig(args)

    def mock_bedrock_response(
        self, value: dict | str, stop_reason: str = "tool_use", mode: str = "json"
    ) -> None:
        response = {
            "stopReason": stop_reason,
            "usage": {
                "cacheReadInputTokens": 100,
                "cacheWriteInputTokens": 101,
                "inputTokens": 43,
                "outputTokens": 44,
            },
        }
        if mode == "text":
            response["output"] = {"message": {"content": [{"text": value}]}}
        elif mode == "json":
            response["output"] = {"message": {"content": [{"toolUse": {"input": value}}]}}
        else:
            response["output"] = {"message": {"content": []}}
        self._boto.converse.return_value = response

    def mock_openai_model_list(self, models: list[str] | None = None, fail: bool = False) -> None:
        if models is None:
            model_aliases = {
                "gpt-oss-120b": ["gpt-oss-120b", "openai.gpt-oss-120b-1:0", "openai/gpt-oss-120b"],
                "gpt35": ["gpt35", "gpt-35-turbo-0125"],
                "gpt4o": ["gpt4o", "gpt-4o"],
            }
            models = model_aliases.get(self.model_id, [])

        data = [SimpleNamespace(id=alias) for alias in models]
        self.openai.models.list.return_value = data

        if fail:
            error = openai.APIError("test failure", mock.MagicMock(), body=None)
            self.openai.models.list.side_effect = error

    def _completion_for_value(
        self, value: dict, finish_reason: str = "stop"
    ) -> chat.ParsedChatCompletion:
        return chat.ParsedChatCompletion(
            id="test-completion",
            choices=[
                chat.ParsedChoice(
                    finish_reason=finish_reason,
                    index=0,
                    message=chat.ParsedChatCompletionMessage(
                        role="assistant", content=json.dumps(value)
                    ),
                )
            ],
            usage=completion_usage.CompletionUsage(
                completion_tokens=10,
                prompt_tokens=19,
                total_tokens=29,
                prompt_tokens_details=completion_usage.PromptTokensDetails(cached_tokens=5),
            ),
            created=1741569952,
            model=self.model_id,
            object="chat.completion",
            system_fingerprint="test-fp",
        )

    def mock_openai_response(
        self, values: dict | list[dict], finish_reason: str = "stop", fail: bool = False
    ) -> None:
        if fail:
            error = openai.APIError("test failure", mock.MagicMock(), body=None)
            self.openai.chat.completions.parse.side_effect = error
        elif isinstance(values, dict):
            completion = self._completion_for_value(values, finish_reason=finish_reason)
            self.openai.chat.completions.parse.return_value = completion
        else:
            completions = [
                self._completion_for_value(value, finish_reason=finish_reason) for value in values
            ]
            self.openai.chat.completions.parse.side_effect = completions


def basic_workflow(tmp_path) -> pathlib.Path:
    """Returns a dead simple valid config, for when you don't super care what it looks like"""
    return conftest.write_toml(
        tmp_path,
        {
            "config_type": "nlp",
            "tables": {
                "task": {
                    "response_schema": EMPTY_SCHEMA,
                },
            },
        },
        "nlp.workflow",
    )


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
