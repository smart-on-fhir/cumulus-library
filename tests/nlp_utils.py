import functools
import json
import os
import pathlib
import tempfile
import weakref
from unittest import mock

import respx

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


class MockModel:
    def __init__(self, model_id: str = "gpt-oss-120b", provider: str = "local"):
        self.model_id = model_id
        self.provider = provider

        # Create a codebook for any NLP we do, to keep consistent anon IDs
        self.tempdir = tempfile.TemporaryDirectory()
        self.phi = pathlib.Path(self.tempdir.name, "phi")
        self.phi.mkdir()
        with open(self.phi / "codebook.json", "w", encoding="utf8") as f:
            salt = "e359191164cd209708d93551f481edd048946a9d844c51dea1b64d3f83dfd1fa"
            json.dump({"id_salt": salt}, f)

        # Determine the server to use for this model
        if provider == "bedrock":
            boto_patcher = mock.patch("boto3.client")
            weakref.finalize(self, boto_patcher.stop)
            self._boto = mock.MagicMock()
            client_mock = boto_patcher.start()
            client_mock.return_value = self._boto
            self.mock_bedrock_response({})
        else:
            if provider == "azure":
                url = "https://example.com/azure/openai"
                chat_prefix = "/deployments/.*"
                params = {"api-version": "2024-10-21"}
            else:
                urls = {
                    "gpt-oss-120b": "http://localhost:8086/v1",
                    "llama4-scout": "http://localhost:8087/v1",
                }
                url = urls.get(model_id, "nope://invalid")
                chat_prefix = ""
                params = {}

            # Do some basic always-on mocking
            self._list_route = respx.get(f"{url}/models", params=params)
            self._chat_route = respx.post(
                url__regex=f"{url}{chat_prefix}/chat/completions", params=params
            )
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

    def mock_openai_model_list(
        self, models: list[str] | None = None, status_code: int = 200
    ) -> None:
        if models is None:
            model_aliases = {
                "gpt-oss-120b": ["gpt-oss-120b", "openai.gpt-oss-120b-1:0", "openai/gpt-oss-120b"],
                "gpt35": ["gpt35", "gpt-35-turbo-0125"],
            }
            models = model_aliases.get(self.model_id, [])

        data = [{"id": alias} for alias in models]
        self._list_route.respond(status_code=status_code, json={"object": "list", "data": data})

    def mock_openai_response(
        self, value: dict, finish_reason: str = "stop", status_code: int = 200
    ) -> None:
        # https://developers.openai.com/api/reference/chat-completions/overview
        self._chat_route.respond(
            status_code=status_code,
            json={
                "id": "test-completion",
                "object": "chat.completion",
                "created": 1741569952,
                "model": self.model_id,
                "choices": [
                    {
                        "index": 0,
                        "message": {
                            "role": "assistant",
                            "content": json.dumps(value),
                        },
                        "finish_reason": finish_reason,
                    },
                ],
                "usage": {
                    "prompt_tokens": 19,
                    "completion_tokens": 10,
                    "total_tokens": 29,
                    "prompt_tokens_details": {
                        "cached_tokens": 5,
                    },
                },
            },
        )


def basic_workflow(tmp_path) -> pathlib.Path:
    """Returns a dead simple valid config, for when you don't super care what it looks like"""
    return conftest.write_toml(
        tmp_path,
        {
            "config_type": "nlp",
            "task": [
                {
                    "name": "task",
                    "response_schema": EMPTY_SCHEMA,
                },
            ],
        },
        "nlp.workflow",
    )
