import base64
import binascii
import contextlib
import functools
import io
import json
import os
import pathlib
import tempfile
from collections.abc import Iterable
from types import SimpleNamespace
from unittest import mock

import openai
from openai.types import chat, completion_usage

import cumulus_library
from cumulus_library import note_utils
from cumulus_library.builders import nlp_builder
from tests import conftest

# Not fully empty, because parquet doesn't like that. But just throw an unused property in there.
EMPTY_SCHEMA = '{"title":"test", "type": "object", "properties": {"ignored": {"type": "string"}}}'

# Default Bedrock batch settings handed to MockModel configs (the bucket exists under mock_s3)
BEDROCK_BATCH_S3_URI = f"s3://{conftest.MOCK_S3_BUCKET}/nlp-batches"
BEDROCK_BATCH_ROLE = "arn:aws:iam::123456789012:role/test-bedrock-batch"

# The salt MockModel configs use to anonymize note refs
SALT_STR = "e359191164cd209708d93551f481edd048946a9d844c51dea1b64d3f83dfd1fa"
SALT_BYTES = binascii.unhexlify(SALT_STR)


def read_rows(db, table: str) -> list[dict]:
    """Reads an NLP output table back from duckdb, ordered by note so tests can compare rows."""
    df = db.db.connection.sql(f"SELECT * FROM {table} ORDER BY note_ref").df()
    return json.loads(df.to_json(orient="records"))


def strip_trailing_whitespace(text: str) -> str:
    """
    Rich pads every table cell with a trailing space, which editors like to strip from
    expected strings. Comparing without it keeps the expected blocks readable and stable.
    """
    return "\n".join(line.rstrip() for line in text.splitlines())


def mock_env(provider: str = "local"):
    if provider == "azure":
        new = {
            "AZURE_OPENAI_API_KEY": "azure-key",
            "AZURE_OPENAI_ENDPOINT": "https://example.com/azure",
        }
    else:
        new = {}

    new.update(dict(conftest.MOCKED_AWS_ENV))

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
                json.dump({"id_salt": SALT_STR}, f)

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
            f"--nlp-chunksize={config.chunksize}",
            f"--nlp-concurrency={config.concurrency}",
        ]
        for deployment in config.azure_deployments:
            args.append(f"--azure-deployment={deployment}")
        if config.clean:
            args.append("--clean-nlp")
        if config.use_batching:
            args.append("--batch-nlp")
        if config.bedrock_batch_s3_uri:
            args.append(f"--bedrock-batch-s3-uri={config.bedrock_batch_s3_uri}")
        if config.bedrock_batch_role:
            args.append(f"--bedrock-batch-role={config.bedrock_batch_role}")
        if not config.show_stats:
            args.append("--no-nlp-stats")
        return args

    def nlp_config(
        self,
        batching: bool = False,
        clean: bool = True,
        stats: bool = True,
        deployments: list[str] | None = None,
        concurrency: int | None = None,
        bedrock_batch_s3_uri: str | None = BEDROCK_BATCH_S3_URI,
        bedrock_batch_role: str | None = BEDROCK_BATCH_ROLE,
    ) -> note_utils.NlpConfig:
        args = {
            "nlp_model": self.model_id,
            "nlp_provider": self.provider,
            "etl_phi_dir": self.phi,
            "target": "example_nlp",  # use the example_nlp name, so we pass the allowlisting
            "batch_nlp": batching,
            "clean_nlp": clean,
            "nlp_stats": stats,
            "azure_deployments": deployments,
            "nlp_concurrency": concurrency,
        }
        if self.provider == "bedrock":
            args["bedrock_batch_s3_uri"] = bedrock_batch_s3_uri
            args["bedrock_batch_role"] = bedrock_batch_role
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
                "gpt54": ["gpt54", "gpt-5.4"],
                "gpt54-mini": ["gpt54-mini", "gpt-5.4-mini"],
                "gpt54-nano": ["gpt54-nano", "gpt-5.4-nano"],
            }
            models = model_aliases.get(self.model_id, [])

        data = [SimpleNamespace(id=alias) for alias in models]
        self.openai.models.list.return_value = data

        if fail:
            error = openai.APIError("test failure", mock.MagicMock(), body=None)
            self.openai.models.list.side_effect = error

    def mock_openai_deployment_check(self, fail: bool = False) -> None:
        """Controls the Azure-style deployment fallback request in post_init_check.

        This is the request made when a model ID isn't in the server's model list, to check
        whether a deployment of that name still works (a workaround for Azure).
        """
        if fail:
            error = openai.APIError("test failure", mock.MagicMock(), body=None)
            self.openai.chat.completions.create.side_effect = error
        else:
            self.openai.chat.completions.create.side_effect = None

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

    def mock_openai_handler(self, handler) -> None:
        """Responds based on the content of each request, rather than on call order.

        The usual ``mock_openai_response`` list form keys responses to call order, which only
        works when requests are issued serially. With concurrency, the order that workers pick
        requests up is not deterministic, so concurrent tests need to key off the request.

        ``handler`` is called with the keyword args of the underlying ``parse()`` call - most
        usefully ``messages`` (whose second entry holds the note text) and ``model`` (which
        holds the deployment name). It returns the dict to answer with, or raises to simulate
        a server-side failure.
        """

        def side_effect(**kwargs):
            value = handler(**kwargs)
            return self._completion_for_value(value)

        self.openai.chat.completions.parse.side_effect = side_effect

    @staticmethod
    def note_text_of(kwargs: dict) -> str:
        """Pulls the clinical note out of a recorded ``parse()`` call."""
        return kwargs["messages"][1]["content"]

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


def object_schema(title: str = "test", **properties: str) -> str:
    """Returns a JSON response schema for an object, given each property's JSON type.

    e.g. object_schema(hello="integer") for {"hello": 5}
    """
    return json.dumps(
        {
            "title": title,
            "type": "object",
            "properties": {name: {"type": kind} for name, kind in properties.items()},
        }
    )


def make_workflow(
    tmp_path,
    tables: Iterable[str] | dict[str, dict],
    *,
    schema: str = EMPTY_SCHEMA,
    shared: dict | None = None,
) -> pathlib.Path:
    """Writes an NLP workflow config and returns its path.

    :param tables: table names, or table names mapped to their config. Any table that doesn't
        give a response_schema gets `schema`.
    :param schema: the default response schema for each table
    :param shared: the workflow's [shared] section, if any
    """
    if not isinstance(tables, dict):
        tables = {name: {} for name in tables}
    workflow = {
        "config_type": "nlp",
        "tables": {name: {"response_schema": schema, **task} for name, task in tables.items()},
    }
    if shared:
        workflow["shared"] = shared
    return conftest.write_toml(tmp_path, workflow, "nlp.workflow")


def basic_workflow(tmp_path, version=None) -> pathlib.Path:
    """Returns a dead simple valid config, for when you don't super care what it looks like"""
    return make_workflow(tmp_path, {"task": {"version": version} if version else {}})


def run_workflow(
    workflow_path: pathlib.Path,
    study_config: cumulus_library.StudyConfig,
    *,
    notes: note_utils.NoteSource,
    nlp_config: note_utils.NlpConfig,
) -> tuple[nlp_builder.NlpBuilder, str]:
    """Builds and runs an NLP workflow, returning the builder and everything it printed.

    Errors from the run propagate, so wrap this in pytest.raises to test one.
    """
    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=notes, nlp_config=nlp_config
    )
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        builder.execute_queries(study_config, None)
    return builder, output.getvalue()


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
