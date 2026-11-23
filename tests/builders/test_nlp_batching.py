"""
Tests for NLP batch mode (--batch-nlp) on Azure and Bedrock: creating, resuming, splitting, and
reading back batches, and attributing their spend.
"""

import hashlib
import json
import os
from types import SimpleNamespace
from unittest import mock

import botocore.exceptions
import cumulus_fhir_support as cfs
import httpx
import openai
import pytest

from cumulus_library import errors, note_utils
from cumulus_library.builders.nlp import models
from cumulus_library.builders.nlp.models import OpenAIProvider
from tests import nlp_utils
from tests.nlp_utils import add_dxr, read_rows


@pytest.fixture(autouse=True)
def _autouse_cache_dir(mock_cache_dir):
    """
    Autouse this fixture to keep NLP's on-disk cache inside tmp_path.
    Delegates to the shared ``mock_cache_dir`` fixture defined in (tests/conftest.py).
    """
    yield


def batch_line(contents: str, answer: str = "answer", usage: bool = True) -> str:
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
                    # Real batch responses report usage, and leaving it out meant batch token
                    # accounting went untested entirely. Some servers omit it, though, hence
                    # the toggle.
                    **(
                        {
                            "usage": {
                                "prompt_tokens": 19,
                                "completion_tokens": 10,
                                "total_tokens": 29,
                            }
                        }
                        if usage
                        else {}
                    ),
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
def test_batch_without_usage_still_counts_as_fresh(
    mock_client, tmp_path, mock_db_config, note_source
):
    """A server that doesn't report usage should cost us the count, not the note."""
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client, provider="azure", model_id="gpt4o")
    model.openai.files.create = lambda **kwargs: SimpleNamespace(id="input")
    model.openai.batches.create.return_value = SimpleNamespace(id="batch")
    model.openai.batches.retrieve.return_value = SimpleNamespace(
        id="batch", status="completed", error_file_id=None, output_file_id="output"
    )
    mock_files_content(model, [batch_line("hello world", usage=False)])

    builder, _ = nlp_utils.run_workflow(
        workflow_path, mock_db_config, notes=note_source, nlp_config=model.nlp_config(batching=True)
    )

    stats = builder.stats
    # We still know the note cost a call, even though we can't say what it cost.
    assert stats.from_model[0] == 1
    assert stats.from_cache[0] == 0
    assert stats.token_stats.new_input_tokens == 0


@nlp_utils.mock_env("azure")
@mock.patch("openai.AzureOpenAI")
def test_batch_tokens_are_attributed_to_their_table(
    mock_client, tmp_path, mock_db_config, note_source
):
    """Batch spend has to reach the per-table totals, not just the pooled one.

    In batch mode the responses are fetched into the cache before the note pass runs, so every
    note then looks like a cache hit. Attribution has to happen while the batch is being
    fetched, or a batch run reports real pooled totals against zeroed-out tables.
    """
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client, provider="azure", model_id="gpt4o")
    model.openai.files.create = lambda **kwargs: SimpleNamespace(id="input")
    model.openai.batches.create.return_value = SimpleNamespace(id="batch")
    model.openai.batches.retrieve.return_value = SimpleNamespace(
        id="batch", status="completed", error_file_id=None, output_file_id="output"
    )
    mock_files_content(model)

    builder, _ = nlp_utils.run_workflow(
        workflow_path, mock_db_config, notes=note_source, nlp_config=model.nlp_config(batching=True)
    )

    stats = builder.stats
    assert stats.got_response[0] == 1
    # The batch paid for this note during this run, so it is not a cache hit.
    assert stats.from_model[0] == 1
    assert stats.from_cache[0] == 0

    # The invariant that catches attribution drift: the parts equal the whole.
    assert stats.token_stats.new_input_tokens == 19
    per_table = models.sum_token_stats(stats.token_stats_by_table.values())
    assert per_table == stats.token_stats


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
        # The batch body is built by the same code that builds a live request, so the
        # sampling args should match what we'd have sent one note at a time.
        assert lines[0]["body"]["temperature"] == 0
        assert lines[0]["body"]["seed"] == 12345
        return SimpleNamespace(id="input")

    model.openai.files.create = upload_file
    model.openai.batches.create.return_value = SimpleNamespace(id="batch")
    model.openai.batches.retrieve.return_value = SimpleNamespace(
        id="batch", status="completed", error_file_id=None, output_file_id="output"
    )
    mock_files_content(model)

    builder, _ = nlp_utils.run_workflow(
        workflow_path, mock_db_config, notes=note_source, nlp_config=model.nlp_config(batching=True)
    )
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

    builder, _ = nlp_utils.run_workflow(
        workflow_path, mock_db_config, notes=note_source, nlp_config=model.nlp_config(batching=True)
    )
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

    _, output = nlp_utils.run_workflow(
        workflow_path, mock_db_config, notes=note_source, nlp_config=model.nlp_config(batching=True)
    )

    assert "Batch did not complete, got status: 'failed'" in output
    assert "Error from NLP: error1" in output
    assert "Could not process error message: '{'blarg''" in output
    assert "Error from NLP: error2" in output
    assert "Unexpected status code from NLP: 400" in output
    assert "Unexpected response from NLP: missing data" in output


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

    builder, _ = nlp_utils.run_workflow(
        workflow_path, mock_db_config, notes=note_source, nlp_config=model.nlp_config(batching=True)
    )
    assert builder.stats.got_response[0] == 4

    rows = read_rows(mock_db_config, "example_nlp__nlp_task_gpt4o")
    expected = [
        {"ignored": "w1"},
        {"ignored": "w2"},
        {"ignored": "w3"},
        {"ignored": "w4"},
    ]
    results = [row["result"] for row in rows]
    assert [x in expected for x in results]
    assert [x in results for x in expected]


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

    builder, _ = nlp_utils.run_workflow(
        workflow_path, mock_db_config, notes=note_source, nlp_config=model.nlp_config(batching=True)
    )
    assert builder.stats.available == 2
    assert builder.stats.got_response[0] == 1


HELLO_WORLD_CHECKSUM = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"


def bedrock_batch_output(answer: dict, stop_reason: str = "tool_use") -> dict:
    """A Converse response body, as found in a batch output record's modelOutput field"""
    return {
        "stopReason": stop_reason,
        "usage": {"inputTokens": 43, "outputTokens": 44},
        "output": {"message": {"content": [{"toolUse": {"input": answer}}]}},
    }


class FakeBedrockBatches:
    """Plays the part of Bedrock's batch inference API, on top of a moto-mocked S3.

    Creating a job reads the uploaded input records and immediately writes the output files
    where real Bedrock would put them ({output_uri}/{job_id}/{input_name}.out).
    """

    def __init__(
        self,
        model: nlp_utils.MockModel,
        *,
        respond=None,
        statuses: list[str] | None = None,
        final_status: str = "Completed",
        message: str | None = None,
    ):
        self.jobs: dict[str, dict] = {}  # job ARN -> job description
        self.inputs: list[list[dict]] = []  # input records, per created job
        self.create_calls: list[dict] = []
        # Maps one input record to its output record (default: a successful answer)
        self.respond = respond or (
            lambda record: {
                "recordId": record["recordId"],
                "modelInput": record["modelInput"],
                "modelOutput": bedrock_batch_output({"ignored": "batched"}),
            }
        )
        self.statuses = list(statuses or [])  # reported (in order) before the final status
        self.final_status = final_status
        self.message = message
        model._boto.create_model_invocation_job.side_effect = self.create
        model._boto.get_model_invocation_job.side_effect = self.get

    @staticmethod
    def job_description(arn: str, input_uri: str, output_uri: str) -> dict:
        return {
            "jobArn": arn,
            "inputDataConfig": {"s3InputDataConfig": {"s3Uri": input_uri}},
            "outputDataConfig": {"s3OutputDataConfig": {"s3Uri": output_uri}},
        }

    @staticmethod
    def write_output(output_uri: str, job_id: str, records: list[dict | str]) -> None:
        out_dir = cfs.FsPath(output_uri, job_id)
        lines = [r if isinstance(r, str) else json.dumps(r) for r in records]
        out_dir.joinpath("input.jsonl.out").write_text("\n".join(lines) + "\n")
        out_dir.joinpath("manifest.json.out").write_text('{"totalRecordCount": 1}')

    def create(self, **kwargs) -> dict:
        self.create_calls.append(kwargs)
        input_uri = kwargs["inputDataConfig"]["s3InputDataConfig"]["s3Uri"]
        output_uri = kwargs["outputDataConfig"]["s3OutputDataConfig"]["s3Uri"]
        records = [json.loads(line) for line in cfs.FsPath(input_uri).read_text().splitlines()]
        self.inputs.append(records)

        job_id = f"job{len(self.jobs)}"
        arn = f"arn:aws:bedrock:us-east-1:123456789012:model-invocation-job/{job_id}"
        self.write_output(output_uri, job_id, [self.respond(record) for record in records])
        self.jobs[arn] = self.job_description(arn, input_uri, output_uri)
        return {"jobArn": arn}

    def get(self, *, jobIdentifier: str) -> dict:
        status = self.statuses.pop(0) if self.statuses else self.final_status
        job = {**self.jobs[jobIdentifier], "status": status}
        if self.message:
            job["message"] = self.message
        return job


def s3_batch_files() -> set[cfs.FsPath]:
    batch_root = cfs.FsPath(nlp_utils.BEDROCK_BATCH_S3_URI)
    if not batch_root.exists():  # S3 has no real folders, so an emptied one is just gone
        return set()
    return batch_root.ls(include_dirs=False, recursive=True)


@mock.patch.object(models.BedrockProvider, "BEDROCK_MIN_BATCH_COUNT", 1)
@mock.patch("boto3.client")
def test_bedrock_batching_happy_path(mock_client, tmp_path, mock_db_config, note_source, mock_s3):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client, provider="bedrock", model_id="claude-sonnet45")
    batches = FakeBedrockBatches(model)

    builder, _ = nlp_utils.run_workflow(
        workflow_path, mock_db_config, notes=note_source, nlp_config=model.nlp_config(batching=True)
    )

    # The job itself
    assert len(batches.create_calls) == 1
    create_args = batches.create_calls[0]
    assert create_args["modelId"] == "us.anthropic.claude-sonnet-4-5-20250929-v1:0"
    assert create_args["modelInvocationType"] == "Converse"
    assert create_args["roleArn"] == nlp_utils.BEDROCK_BATCH_ROLE
    input_uri = create_args["inputDataConfig"]["s3InputDataConfig"]["s3Uri"]
    output_uri = create_args["outputDataConfig"]["s3OutputDataConfig"]["s3Uri"]
    assert input_uri.startswith(f"{nlp_utils.BEDROCK_BATCH_S3_URI}/cumulus-nlp-")
    assert input_uri.endswith("/input.jsonl")
    assert output_uri == input_uri.removesuffix("input.jsonl") + "output/"
    assert create_args["inputDataConfig"]["s3InputDataConfig"]["s3InputFormat"] == "JSONL"

    # The records, which should be the same Converse body we'd send live, minus cache points
    assert len(batches.inputs[0]) == 1
    record = batches.inputs[0][0]
    assert record["recordId"] == HELLO_WORLD_CHECKSUM
    model_input = record["modelInput"]
    assert "modelId" not in model_input  # that's set on the job instead
    assert model_input["system"] == [{"text": ""}]
    assert model_input["messages"] == [{"role": "user", "content": [{"text": "hello world"}]}]
    assert model_input["toolConfig"]["toolChoice"] == {"tool": {"name": "to_json"}}
    assert [
        tool.get("toolSpec", {}).get("name") for tool in model_input["toolConfig"]["tools"]
    ] == ["to_json"]
    assert model_input["inferenceConfig"] == {"temperature": 0}

    # Results came from the batch, not a live call
    model._boto.converse.assert_not_called()
    rows = read_rows(mock_db_config, "example_nlp__nlp_task_claude_sonnet45")
    assert [row["result"] for row in rows] == [{"ignored": "batched"}]

    stats = builder.stats
    assert stats.got_response[0] == 1
    assert stats.from_model[0] == 1
    assert stats.from_cache[0] == 0
    assert stats.token_stats.new_input_tokens == 43
    assert stats.token_stats.output_tokens == 44
    assert models.sum_token_stats(stats.token_stats_by_table.values()) == stats.token_stats
    assert stats.token_prices.multiplier == 0.5
    # And the discount didn't leak into the shared, class-level prices
    assert models.ClaudeSonnet45Model.BEDROCK_PRICES.multiplier == 1

    # S3 copies (which hold note text) are cleaned up, and nothing is left to resume
    assert s3_batch_files() == set()
    metadata_path = model.phi / "nlp-cache/example_nlp__task_v0_claude-sonnet45/metadata.json"
    assert json.loads(metadata_path.read_text()) == {"batches-bedrock": []}


@mock.patch.object(models.BedrockProvider, "BEDROCK_MIN_BATCH_COUNT", 1)
@mock.patch("boto3.client")
def test_bedrock_batching_multiple_tables(
    mock_client, tmp_path, mock_db_config, note_source, mock_s3
):
    """Each table gets its own batch, answered with (and cached under) its own schema"""
    workflow_path = nlp_utils.make_workflow(
        tmp_path,
        {
            "alpha": {"response_schema": nlp_utils.object_schema("alpha", a="string")},
            "beta": {"response_schema": nlp_utils.object_schema("beta", b="string")},
        },
    )
    model = nlp_utils.MockModel(mock_client, provider="bedrock", model_id="claude-sonnet45")

    def respond(record: dict) -> dict:
        tool = record["modelInput"]["toolConfig"]["tools"][0]["toolSpec"]
        field = next(iter(tool["inputSchema"]["json"]["properties"]))
        return {"recordId": record["recordId"], "modelOutput": bedrock_batch_output({field: "x"})}

    batches = FakeBedrockBatches(model, respond=respond)

    builder, _ = nlp_utils.run_workflow(
        workflow_path, mock_db_config, notes=note_source, nlp_config=model.nlp_config(batching=True)
    )

    assert len(batches.create_calls) == 2
    model._boto.converse.assert_not_called()
    assert read_rows(mock_db_config, "example_nlp__nlp_alpha_claude_sonnet45")[0]["result"] == {
        "a": "x"
    }
    assert read_rows(mock_db_config, "example_nlp__nlp_beta_claude_sonnet45")[0]["result"] == {
        "b": "x"
    }
    assert builder.stats.from_model == [1, 1]


@mock.patch.object(models.BedrockProvider, "BEDROCK_MIN_BATCH_COUNT", 1)
@mock.patch.object(models.BedrockProvider, "BEDROCK_MAX_BATCH_COUNT", 2)
@mock.patch("boto3.client")
def test_bedrock_splitting_batch(mock_client, tmp_path, mock_db_config, mock_s3):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client, provider="bedrock", model_id="claude-sonnet45")
    batches = FakeBedrockBatches(model)

    with open(f"{tmp_path}/dxr.ndjson", "w", encoding="utf8") as f:
        add_dxr("hello1", "world1", f)
        add_dxr("hello2", "world2", f)
        add_dxr("hello3", "world1", f)  # same text as the first - should only be asked once
        add_dxr("hello4", "world3", f)
    note_source = note_utils.NoteSource([tmp_path])

    builder, _ = nlp_utils.run_workflow(
        workflow_path, mock_db_config, notes=note_source, nlp_config=model.nlp_config(batching=True)
    )

    # Three unique notes, two per batch
    assert [len(records) for records in batches.inputs] == [2, 1]
    model._boto.converse.assert_not_called()
    assert builder.stats.got_response[0] == 4
    assert s3_batch_files() == set()


@mock.patch("boto3.client")
def test_bedrock_batch_under_minimum_goes_live(
    mock_client, tmp_path, mock_db_config, note_source, mock_s3
):
    """Too few notes for a Bedrock batch job, so they get sent one at a time instead"""
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client, provider="bedrock", model_id="claude-sonnet45")
    batches = FakeBedrockBatches(model)

    builder, output = nlp_utils.run_workflow(
        workflow_path, mock_db_config, notes=note_source, nlp_config=model.nlp_config(batching=True)
    )

    output = " ".join(output.split())  # undo any console line-wrapping
    assert "under Bedrock's minimum of 100 per batch job" in output
    assert batches.create_calls == []
    assert s3_batch_files() == set()
    assert model._boto.converse.call_count == 1
    assert builder.stats.got_response[0] == 1


@mock.patch.object(models.BedrockProvider, "BEDROCK_MIN_BATCH_COUNT", 1)
@mock.patch("time.sleep", new=lambda x: None)
@mock.patch("boto3.client")
def test_bedrock_batching_errors(mock_client, tmp_path, mock_db_config, note_source, mock_s3):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client, provider="bedrock", model_id="claude-sonnet45")

    def respond(record: dict) -> dict:
        return {
            "recordId": record["recordId"],
            "modelInput": record["modelInput"],
            "modelOutput": bedrock_batch_output({}, stop_reason="max_tokens"),
        }

    batches = FakeBedrockBatches(
        model,
        respond=respond,
        statuses=["Submitted", "Validating", "Scheduled", "InProgress"],
        final_status="PartiallyCompleted",
        message="some records failed",
    )

    # Mix some broken records in with the real (but unusable) answer
    real_create = batches.create

    def create_with_junk(**kwargs):
        result = real_create(**kwargs)
        output_uri = kwargs["outputDataConfig"]["s3OutputDataConfig"]["s3Uri"]
        records = [
            {"recordId": "a", "error": {"errorCode": 400, "errorMessage": "bad request"}},
            "{'blarg'",  # invalid json
            {"recordId": "b"},  # no output
            {"modelOutput": bedrock_batch_output({})},  # no record ID
            respond(batches.inputs[-1][0]),
        ]
        FakeBedrockBatches.write_output(output_uri, "job0", records)
        return result

    model._boto.create_model_invocation_job.side_effect = create_with_junk

    builder, output = nlp_utils.run_workflow(
        workflow_path, mock_db_config, notes=note_source, nlp_config=model.nlp_config(batching=True)
    )
    output = " ".join(output.split())  # undo any console line-wrapping

    assert model._boto.get_model_invocation_job.call_count == 5
    assert (
        "Batch did not complete, got status: 'PartiallyCompleted' (some records failed)" in output
    )
    assert "Error from NLP: bad request" in output
    assert "Could not parse a batch output record from NLP" in output
    assert output.count("Unexpected response from NLP: missing data") == 2
    assert "Failed to process batch response from NLP: did not complete" in output

    # The note the batch couldn't answer still gets answered, live
    assert model._boto.converse.call_count == 1
    assert builder.stats.got_response[0] == 1
    assert s3_batch_files() == set()


@mock.patch("boto3.client")
def test_bedrock_resume_batching(mock_client, tmp_path, mock_db_config, note_source, mock_s3):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client, provider="bedrock", model_id="claude-sonnet45")
    batches = FakeBedrockBatches(model)

    # A job from a previous, interrupted run
    arn = "arn:aws:bedrock:us-east-1:123456789012:model-invocation-job/old"
    folder = f"{nlp_utils.BEDROCK_BATCH_S3_URI}/cumulus-nlp-old"
    input_uri = f"{folder}/input.jsonl"
    output_uri = f"{folder}/output/"
    cfs.FsPath(input_uri).write_text("{}\n")
    batches.write_output(
        output_uri,
        "old",
        [{"recordId": HELLO_WORLD_CHECKSUM, "modelOutput": bedrock_batch_output({"ignored": "a"})}],
    )
    batches.jobs[arn] = batches.job_description(arn, input_uri, output_uri)

    path_dir = f"{model.phi}/nlp-cache/example_nlp__task_v0_claude-sonnet45"
    os.makedirs(path_dir)
    with open(f"{path_dir}/metadata.json", "w", encoding="utf8") as f:
        json.dump({"batches-bedrock": [arn]}, f)

    nlp_utils.run_workflow(
        workflow_path, mock_db_config, notes=note_source, nlp_config=model.nlp_config(batching=True)
    )

    # Nothing new needed to be sent - the resumed job had the answer
    assert batches.create_calls == []
    model._boto.converse.assert_not_called()
    rows = read_rows(mock_db_config, "example_nlp__nlp_task_claude_sonnet45")
    assert rows[0]["result"] == {"ignored": "a"}
    assert s3_batch_files() == set()


@mock.patch.object(models.BedrockProvider, "BEDROCK_MIN_BATCH_COUNT", 1)
@mock.patch("boto3.client")
def test_bedrock_batch_create_failure_cleans_up(
    mock_client, tmp_path, mock_db_config, note_source, mock_s3
):
    """If the job can't be started, don't leave the notes sitting in S3"""
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client, provider="bedrock", model_id="claude-sonnet45")
    error = botocore.exceptions.ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "no"}}, "CreateModelInvocationJob"
    )
    model._boto.create_model_invocation_job.side_effect = error

    with pytest.raises(botocore.exceptions.ClientError, match="AccessDeniedException"):
        nlp_utils.run_workflow(
            workflow_path,
            mock_db_config,
            notes=note_source,
            nlp_config=model.nlp_config(batching=True),
        )

    assert s3_batch_files() == set()


@pytest.mark.parametrize(
    "s3_uri,role,error",
    [
        (None, nlp_utils.BEDROCK_BATCH_ROLE, "Pass both --bedrock-batch-s3-uri"),
        (nlp_utils.BEDROCK_BATCH_S3_URI, None, "Pass both --bedrock-batch-s3-uri"),
        ("/local/folder", nlp_utils.BEDROCK_BATCH_ROLE, "must be an s3:// URL"),
    ],
)
@mock.patch("boto3.client")
def test_bedrock_batching_needs_config(
    mock_client, s3_uri, role, error, tmp_path, mock_db_config, note_source
):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client, provider="bedrock", model_id="claude-sonnet45")
    config = model.nlp_config(batching=True, bedrock_batch_s3_uri=s3_uri, bedrock_batch_role=role)

    with pytest.raises(errors.CumulusLibraryError, match=error):
        nlp_utils.run_workflow(workflow_path, mock_db_config, notes=note_source, nlp_config=config)


@mock.patch("boto3.client")
def test_bedrock_batching_unsupported_model(mock_client, tmp_path, mock_db_config, note_source):
    workflow_path = nlp_utils.basic_workflow(tmp_path)
    model = nlp_utils.MockModel(mock_client, provider="bedrock", model_id="claude-opus55")

    with pytest.raises(errors.CumulusLibraryError, match="does not support batching"):
        nlp_utils.run_workflow(
            workflow_path,
            mock_db_config,
            notes=note_source,
            nlp_config=model.nlp_config(batching=True),
        )
