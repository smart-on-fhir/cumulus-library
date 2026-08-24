"""
Tests for MLflow experiment tracking of NLP workflows.

These run against a real MLflow instance backed by a local sqlite file, rather than mocking the
mlflow API. Tracking is almost entirely about whether numbers land on the right run, and a mock
would happily accept a call that a real server would reject or file somewhere else.

Like the other NLP builder tests, these share an xdist group - they mutate MLflow's global
tracking URI, so they must not run beside each other.
"""

import pathlib
from unittest import mock

import mlflow
import pytest

from cumulus_library import errors, note_utils
from cumulus_library.builders import nlp_builder
from cumulus_library.builders.nlp import driver, models, tracking
from tests import conftest, nlp_utils
from tests.nlp_utils import add_dxr

pytestmark = pytest.mark.xdist_group("nlp_tracking")

# Every mocked OpenAI completion reports these, see nlp_utils._completion_for_value
TOKENS_PER_CALL = models.TokenStats(
    new_input_tokens=14,  # prompt_tokens 19 - cached 5
    cache_read_input_tokens=5,
    output_tokens=10,
)


@pytest.fixture
def tracking_uri(tmp_path) -> str:
    """A throwaway MLflow backend.

    Sqlite rather than a file:// store: MLflow 3 put the filesystem backend into maintenance
    mode and refuses it unless you opt out with an environment variable.
    """
    uri = f"sqlite:///{tmp_path}/mlflow.db"
    yield uri
    # Don't leak this run's URI into the next test.
    mlflow.set_tracking_uri(None)


def make_workflow(tmp_path, *table_slugs: str) -> pathlib.Path:
    schema = '{"title":"test", "type": "object", "properties": {"hello": {"type": "integer"}}}'
    return conftest.write_toml(
        tmp_path,
        {
            "config_type": "nlp",
            "shared": {"system_prompt": "you are a helpful assistant"},
            "tables": {slug: {"response_schema": schema, "version": 3} for slug in table_slugs},
        },
        "nlp.workflow",
    )


def make_notes(tmp_path, count: int = 1) -> note_utils.NoteSource:
    with open(f"{tmp_path}/dxr.ndjson", "w", encoding="utf8") as f:
        for index in range(count):
            add_dxr(str(index), f"say hello to the world {index}", f)
    return note_utils.NoteSource([tmp_path])


def tracked_config(model, tracking_uri, **overrides) -> note_utils.NlpConfig:
    config = model.nlp_config()
    config.mlflow = True
    config.mlflow_uri = tracking_uri
    config.mlflow_experiment = "test-experiment"
    for key, value in overrides.items():
        setattr(config, key, value)
    return config


def build(tmp_path, mock_db_config, nlp_config, notes, *table_slugs):
    workflow_path = make_workflow(tmp_path, *table_slugs)
    builder = nlp_builder.NlpBuilder(
        toml_config_path=workflow_path, notes=notes, nlp_config=nlp_config
    )
    builder.execute_queries(mock_db_config, None)
    return builder


def runs_by_table(experiment: str = "test-experiment") -> dict[str, "mlflow.entities.Run"]:
    exp = mlflow.get_experiment_by_name(experiment)
    assert exp, f"experiment '{experiment}' was never created"
    found = mlflow.search_runs(
        experiment_ids=[exp.experiment_id], output_format="list", run_view_type=3
    )
    return {run.data.tags["table"]: run for run in found}


@mock.patch("openai.OpenAI")
def test_one_run_per_table(mock_client, tmp_path, mock_db_config, tracking_uri):
    """Each workflow table gets its own run, carrying its own setup params."""
    model = nlp_utils.MockModel(mock_client)
    model.mock_openai_response({"hello": 1})
    config = tracked_config(model, tracking_uri)

    build(tmp_path, mock_db_config, config, make_notes(tmp_path), "age", "race")

    runs = runs_by_table()
    assert set(runs) == {"age", "race"}

    age = runs["age"]
    assert age.info.status == "FINISHED"
    assert age.info.run_name == "age_v3_gpt-oss-120b"
    assert age.data.params["study"] == "example_nlp"
    assert age.data.params["task_version"] == "3"
    assert age.data.params["model_id"] == "gpt-oss-120b"
    assert age.data.tags["source"] == "cumulus-library"
    # The two tables share a system prompt, so they should agree on its digest - that is the
    # point of logging one (grouping runs by "same prompt" without diffing the full text).
    assert age.data.params["system_prompt_sha256"]
    assert (
        age.data.params["system_prompt_sha256"] == runs["race"].data.params["system_prompt_sha256"]
    )

    artifacts = {f.path for f in mlflow.MlflowClient().list_artifacts(age.info.run_id, "prompts")}
    assert artifacts == {
        "prompts/system_prompt.txt",
        "prompts/user_prompt.txt",
        "prompts/response_schema.json",
    }


@mock.patch("openai.OpenAI")
def test_tokens_are_attributed_per_table(mock_client, tmp_path, mock_db_config, tracking_uri):
    """Each table's run reports its own spend, and the parts add up to the pooled total."""
    model = nlp_utils.MockModel(mock_client)
    model.mock_openai_response({"hello": 1})
    config = tracked_config(model, tracking_uri)

    notes = make_notes(tmp_path, count=3)
    builder = build(tmp_path, mock_db_config, config, notes, "age", "race")

    # 3 notes x 2 tables = 6 calls, split evenly.
    per_table = 3
    for table in ("age", "race"):
        run = runs_by_table()[table]
        assert run.data.metrics["tokens.new_input"] == TOKENS_PER_CALL.new_input_tokens * per_table
        assert run.data.metrics["tokens.output"] == TOKENS_PER_CALL.output_tokens * per_table
        assert run.data.metrics["notes.with_results"] == per_table

    # The invariant that makes per-table attribution trustworthy: nothing is double counted
    # and nothing is lost relative to the number we print to the console.
    by_table = builder.stats.token_stats_by_table
    assert set(by_table) == {"age", "race"}
    total = models.sum_token_stats(by_table.values())
    assert total == builder.stats.token_stats


@mock.patch("openai.OpenAI")
def test_cached_notes_report_no_spend(mock_client, tmp_path, mock_db_config, tracking_uri):
    """A note served from the cache costs nothing, and must not be billed to the run."""
    model = nlp_utils.MockModel(mock_client)
    model.mock_openai_response({"hello": 1})
    config = tracked_config(model, tracking_uri)

    # First pass populates the NLP cache.
    build(tmp_path, mock_db_config, config, make_notes(tmp_path), "age")
    first = runs_by_table()["age"]
    assert first.data.metrics["tokens.total"] > 0

    # Second pass answers the same note from cache. It still produces a result, but the model
    # is never called, so the run should show a result with zero tokens spent.
    config2 = tracked_config(model, tracking_uri, mlflow_experiment="second-pass")
    build(tmp_path, mock_db_config, config2, make_notes(tmp_path), "age")

    second = runs_by_table("second-pass")["age"]
    assert second.data.metrics["notes.with_results"] == 1
    assert second.data.metrics["tokens.total"] == 0
    assert "cost.estimated_usd" not in second.data.metrics or (
        second.data.metrics["cost.estimated_usd"] == 0
    )


@mock.patch("openai.OpenAI")
def test_traces_land_on_their_own_table_run(mock_client, tmp_path, mock_db_config, tracking_uri):
    """Traces follow the table that asked for them, not whichever run started last.

    MLflow's active-run state is thread-local while trace association is not, so without an
    explicit link every trace would pile onto one run. This is the regression test for that.
    """
    model = nlp_utils.MockModel(mock_client)
    model.mock_openai_response({"hello": 1})
    # Concurrency matters here: the bug this guards against only shows up once prompts for
    # different tables are running on different threads.
    config = tracked_config(model, tracking_uri, mlflow_log_traces=True, concurrency=2)

    build(tmp_path, mock_db_config, config, make_notes(tmp_path, count=2), "age", "race")
    mlflow.flush_trace_async_logging()

    runs = runs_by_table()
    for table in ("age", "race"):
        traces = mlflow.search_traces(run_id=runs[table].info.run_id, return_type="list")
        assert len(traces) == 2, f"{table} should own exactly its own 2 traces"
        for trace in traces:
            assert trace.info.tags.get("table") == table


@mock.patch("openai.OpenAI")
def test_no_tracking_without_the_flag(mock_client, tmp_path, mock_db_config, tracking_uri):
    """The normal build path creates no runs at all."""
    model = nlp_utils.MockModel(mock_client)
    model.mock_openai_response({"hello": 1})
    config = model.nlp_config()  # no mlflow settings

    build(tmp_path, mock_db_config, config, make_notes(tmp_path), "age")

    assert mlflow.get_experiment_by_name("test-experiment") is None


@mock.patch("openai.OpenAI")
def test_missing_uri_is_rejected_before_any_nlp(mock_client, tmp_path, mock_db_config):
    """--mlflow without a server should fail up front, not after paying for a pass."""
    model = nlp_utils.MockModel(mock_client)
    model.mock_openai_response({"hello": 1})
    config = model.nlp_config()
    config.mlflow = True
    config.mlflow_uri = None

    with pytest.raises(errors.CumulusLibraryError, match="MLFLOW_TRACKING_URI"):
        build(tmp_path, mock_db_config, config, make_notes(tmp_path), "age")

    # And the model was never contacted.
    assert not model.openai.chat.completions.parse.called


@mock.patch("openai.OpenAI")
def test_failed_pass_marks_runs_failed(mock_client, tmp_path, mock_db_config, tracking_uri):
    """If the NLP pass blows up, the runs say so rather than sitting there looking finished."""
    model = nlp_utils.MockModel(mock_client)
    model.mock_openai_response({"hello": 1})
    config = tracked_config(model, tracking_uri)

    with mock.patch.object(driver, "run_nlp", side_effect=RuntimeError("nope")):
        with pytest.raises(RuntimeError, match="nope"):
            build(tmp_path, mock_db_config, config, make_notes(tmp_path), "age")

    assert runs_by_table()["age"].info.status == "FAILED"


@mock.patch("openai.OpenAI")
def test_logging_failure_does_not_kill_the_run(mock_client, tmp_path, mock_db_config, tracking_uri):
    """Tracking is best-effort once the pass is underway - it must not lose us the NLP results."""
    model = nlp_utils.MockModel(mock_client)
    model.mock_openai_response({"hello": 1})
    config = tracked_config(model, tracking_uri)

    with mock.patch.object(
        tracking.MlflowTracker, "_log_results", side_effect=ValueError("mlflow is sad")
    ):
        builder = build(tmp_path, mock_db_config, config, make_notes(tmp_path), "age")

    # The NLP results survived the logging failure.
    assert builder.stats.got_response[0] == 1


def test_tags_parse_as_explicit_pairs():
    """KEY=VALUE parsing, including values that Cumulus ETL's run-name splitting mangles.

    ETL derives tags by splitting the run name on "-" and then "_", so a value like
    "gpt-oss-120b" silently loses everything after the first hyphen. Being explicit avoids it.
    """
    config = note_utils.NlpConfig(
        {"mlflow_tags": ["model=gpt-oss-120b", "phase=pilot_2", "note=has=equals"]}
    )
    assert config.mlflow_tags == {
        "model": "gpt-oss-120b",  # hyphens survive
        "phase": "pilot_2",  # underscores survive
        "note": "has=equals",  # only the first "=" splits
    }


@pytest.mark.parametrize("bad", ["novalue", "=novalue"])
def test_bad_tags_are_rejected(bad):
    with pytest.raises(errors.CumulusLibraryError, match="KEY=VALUE"):
        note_utils.NlpConfig({"mlflow_tags": [bad]})
