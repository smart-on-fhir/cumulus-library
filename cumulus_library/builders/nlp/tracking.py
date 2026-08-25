"""MLflow experiment tracking for NLP workflows.

MLflow is an optional dependency. Nothing in this module is imported unless tracking is turned
on with --mlflow, and importing mlflow itself is deferred until then.
"""

import contextlib
import dataclasses
import datetime
import hashlib
import json
import logging
import types
import typing
from collections.abc import Callable, Iterator

from cumulus_library import base_utils, errors
from cumulus_library.builders.nlp import workflow
from cumulus_library.note_utils import NlpConfig

if typing.TYPE_CHECKING:  # pragma: no cover
    # Import-time only. models imports this module for traced(), and driver imports models, so
    # importing driver for real here would close that loop. NlpStats is needed purely as a type.
    from cumulus_library.builders.nlp import driver

INSTALL_HINT = (
    "MLflow experiment tracking requires the optional 'mlflow' dependency.\n"
    "Install it with: pip install 'cumulus-library[mlflow]'"
)
EXPERIMENT_DEFAULT = "cumulus-library-nlp-dev"

# MLflow rejects param values past a certain length, and prompts routinely blow past it.
# We log the full text as an artifact and keep a short digest as the filterable param.
MAX_PARAM_LEN = 500

# Trace metadata key MLflow uses to associate a trace with a run. Setting it explicitly is the
# only reliable way to attribute traces here: MLflow's active-run state is thread-local, but
# trace-to-run association is not derived from it, so concurrent prompts for different tables
# would otherwise all land on whichever run was started last.
SOURCE_RUN_KEY = "mlflow.sourceRun"


def import_mlflow() -> types.ModuleType:
    """Imports mlflow, turning the ImportError into an actionable message."""
    try:
        import mlflow

        return mlflow
    except ImportError as exc:  # pragma: no cover - depends on install state
        raise errors.CumulusLibraryError(INSTALL_HINT) from exc


@dataclasses.dataclass
class TraceInfo:
    """Says which MLflow run a given prompt's traces belong to.

    This travels with the prompt rather than living in ambient state, because prompts for
    several tables are in flight on different threads at once.
    """

    run_id: str
    tags: dict[str, str] = dataclasses.field(default_factory=dict)


def traced(method: Callable, trace: TraceInfo | None) -> Callable:
    """Wraps a provider call so its auto-logged spans land on the right run.

    Returns the method untouched when tracing is off, so the non-tracking path stays free of
    both the wrapper and the mlflow import. The wrapper is applied around the *provider* call
    rather than the cache lookup, so cache hits don't generate empty traces.
    """
    if trace is None:
        return method

    mlflow = import_mlflow()

    def wrapper(*args, **kwargs) -> typing.Any:
        with mlflow.start_span(name="nlp_prompt"):
            # Spans auto-logged by mlflow.openai nest inside this one, so stamping the trace
            # here covers the model call too.
            mlflow.update_current_trace(metadata={SOURCE_RUN_KEY: trace.run_id}, tags=trace.tags)
            return method(*args, **kwargs)

    return wrapper


def _digest(text: str | None) -> str:
    """Hashes text for easy grouping and filtering w/o information overload in params."""
    if not text:
        return ""
    return hashlib.sha256(text.encode("utf8"), usedforsecurity=False).hexdigest()[:16]


def _clip(value: str | None) -> str:
    """Clips a param value to MLflow's max length, adding an ellipsis if it was too long."""
    if value is None:
        return ""
    if len(value) <= MAX_PARAM_LEN:
        return value
    return value[: MAX_PARAM_LEN - 1] + "…"


class MlflowTracker:
    """Records one MLflow run per workflow table."""

    def __init__(
        self,
        nlp_config: NlpConfig,
        *,
        tables: dict[str, workflow.NlpTask],
        model_id: str,
    ):
        self._config = nlp_config
        self._tables = tables
        self._model_id = model_id
        # Experiment name priority: the CLI arg, else the study name, else a generic default.
        self._experiment = nlp_config.mlflow_experiment or nlp_config.target or EXPERIMENT_DEFAULT
        self._mlflow: types.ModuleType | None = None
        self._run_ids: dict[str, str] = {}
        self._start: datetime.datetime | None = None
        self._end: datetime.datetime | None = None

    @property
    def run_ids(self) -> dict[str, str]:
        return dict(self._run_ids)

    def _run_name(self, table_slug: str, task: workflow.NlpTask) -> str:
        if base := self._config.mlflow_run_name:
            return f"{base}_{table_slug}"
        return f"{table_slug}_v{task.version}_{self._model_id}"

    def _tags(self, table_slug: str) -> dict[str, str]:
        # User tags first, so they can add whatever they like - but the identity tags below win.
        # Runs are grouped and looked up by "table", so letting --mlflow-tag overwrite it would
        # quietly mislabel the run rather than doing anything useful.
        tags = dict(self._config.mlflow_tags)
        tags.update(
            {
                "study": self._config.target or "",
                "table": table_slug,
                "model": self._model_id,
                "provider": self._config.provider,
                "source": "cumulus-library",
            }
        )
        return {key: str(value) for key, value in tags.items()}

    ###########
    # Lifecycle
    ###########

    def start(self) -> None:
        """Connects, creates the experiment, and opens one run per table.

        Raises if the tracking server can't be reached.
        """
        self._mlflow = import_mlflow()
        self._start = base_utils.get_utc_datetime()

        try:
            self._mlflow.set_tracking_uri(self._config.mlflow_uri)
            self._mlflow.set_experiment(self._experiment)
        except Exception as exc:
            raise errors.CumulusLibraryError(
                f"Could not reach the MLflow tracking server at '{self._config.mlflow_uri}':\n{exc}"
            ) from exc

        # Patches the OpenAI client so model calls are captured as spans. Only meaningful
        # for the azure/local providers - Bedrock goes through boto3, which this doesn't
        # touch. See the PHI warning on the --mlflow-log-traces flag.
        if self._config.mlflow_log_traces:
            import mlflow.openai

            mlflow.openai.autolog()

        for table_slug, task in self._tables.items():
            run = self._mlflow.start_run(
                run_name=self._run_name(table_slug, task),
                tags=self._tags(table_slug),
            )
            self._run_ids[table_slug] = run.info.run_id
            # End the fluent run right away: we log through the client from here on, so that
            # nothing depends on which thread happens to be active.
            self._mlflow.end_run()
            self._log_setup(table_slug, task)

    def trace_for(self, table_slug: str) -> TraceInfo | None:
        """The trace target for prompts belonging to this table, if tracing is on."""
        if not self._config.mlflow_log_traces:
            return None
        run_id = self._run_ids.get(table_slug)
        if not run_id:
            return None
        return TraceInfo(run_id=run_id, tags={"table": table_slug})

    def finish(self, stats: "driver.NlpStats") -> None:
        """Logs the numbers from a completed pass and closes every run."""
        self._end = base_utils.get_utc_datetime()
        for table_slug in self._tables:
            with self._soft_fail(f"metrics for '{table_slug}'"):
                self._log_results(table_slug, stats)
        self._terminate("FINISHED")

    def fail(self) -> None:
        """Marks every run as failed, for when the pass raised."""
        self._end = base_utils.get_utc_datetime()
        self._terminate("FAILED")

    def _terminate(self, status: str) -> None:
        """Closes every run with the given status, flushing traces if needed."""
        if not self._mlflow:
            return
        client = self._mlflow.MlflowClient()
        for run_id in self._run_ids.values():
            with self._soft_fail(f"closing run {run_id}"):
                client.set_terminated(run_id, status=status)
        if self._config.mlflow_log_traces:
            with self._soft_fail("flushing traces"):
                self._mlflow.flush_trace_async_logging()

    @contextlib.contextmanager
    def _soft_fail(self, msg: str) -> Iterator[None]:
        """Tracking problems warn; they never take the NLP run down with them."""
        try:
            yield
        except Exception as exc:
            logging.warning("MLflow logging failed for %s (non-fatal): %s", msg, exc)

    #########
    # Logging
    #########

    def _log_setup(self, table_slug: str, task: workflow.NlpTask) -> None:
        """Logs everything knowable before the pass runs: config, prompts, schema."""
        with self._soft_fail(f"params for '{table_slug}'"):
            client = self._mlflow.MlflowClient()
            run_id = self._run_ids[table_slug]

            schema = task.response_schema.model_json_schema()
            params = {
                "study": self._config.target or "",
                "table": table_slug,
                "task_version": task.version,
                "model_id": self._model_id,
                "provider": self._config.provider,
                "batch_mode": self._config.use_batching,
                "concurrency": self._config.concurrency,
                "deployments": ",".join(self._config.azure_deployments) or "(default)",
                "run_started": _iso(self._start),
                # Digests make it possible to group runs by "same prompt" without paging
                # through the full text, which lives in the artifacts below.
                "system_prompt_sha256": _digest(task.system_prompt),
                "user_prompt_sha256": _digest(task.user_prompt),
                "response_schema_sha256": _digest(json.dumps(schema, sort_keys=True)),
                "select_by_word": _clip(_join(task.select_by_word)),
                "select_by_regex": _clip(_join(task.select_by_regex)),
                "select_by_table": task.select_by_table or "",
                "reject_by_word": _clip(_join(task.reject_by_word)),
                "reject_by_regex": _clip(_join(task.reject_by_regex)),
            }
            for key, value in params.items():
                client.log_param(run_id, key, value)

            # Full text as artifacts - easier to diff across runs than a truncated param.
            client.log_text(run_id, task.system_prompt or "", "prompts/system_prompt.txt")
            user_prompt = task.user_prompt or "%CLINICAL-NOTE%"
            client.log_text(run_id, user_prompt, "prompts/user_prompt.txt")
            client.log_text(run_id, json.dumps(schema, indent=2), "prompts/response_schema.json")

    def _log_results(self, table_slug: str, stats: "driver.NlpStats") -> None:
        client = self._mlflow.MlflowClient()
        run_id = self._run_ids[table_slug]
        index = list(self._tables).index(table_slug)

        considered = stats.considered[index]
        with_results = stats.got_response[index]
        yield_rate = with_results / stats.had_text if stats.had_text else 0.0

        metrics = {
            # ETL's metric names, so runs from both projects sit side by side in one experiment.
            "notes.seen": stats.available,
            "notes.with_text": stats.had_text,
            "notes.considered": considered,
            "notes.with_results": with_results,
            "notes.yield_rate": round(yield_rate, 4),
            # Workflow-wide, not per-table - see the module docstring.
            "workflow.throttle_dropped": stats.throttle_dropped,
        }

        tokens = stats.token_stats_by_table[table_slug]
        total_input = tokens.new_input_tokens + tokens.cache_read_input_tokens
        total_tokens = total_input + tokens.output_tokens
        cache_hit_rate = tokens.cache_read_input_tokens / total_input if total_input else 0.0
        metrics.update(
            {
                "tokens.new_input": tokens.new_input_tokens,
                "tokens.cache_read": tokens.cache_read_input_tokens,
                "tokens.cache_written": tokens.cache_written_input_tokens,
                "tokens.output": tokens.output_tokens,
                "tokens.total": total_tokens,
                "tokens.cache_hit_rate": round(cache_hit_rate, 4),
            }
        )

        # Whole seconds: get_utc_datetime() truncates microseconds, and an NLP pass is
        # measured in minutes, so sub-second precision would be invented.
        elapsed = (self._end - self._start).total_seconds()
        metrics["runtime.wall_seconds"] = elapsed
        metrics["runtime.tokens_per_second"] = round(total_tokens / elapsed, 1) if elapsed else 0.0
        metrics["runtime.seconds_per_note"] = (
            round(elapsed / with_results, 3) if with_results and elapsed else 0.0
        )

        if prices := stats.token_prices:
            cost = (
                tokens.new_input_tokens * prices.new_input_tokens
                + tokens.cache_read_input_tokens * prices.cache_read_input_tokens
                + tokens.cache_written_input_tokens * prices.cache_written_input_tokens
                + tokens.output_tokens * prices.output_tokens
            )
            cost = cost / 1_000 * prices.multiplier  # prices are per 1,000 tokens
            metrics["cost.estimated_usd"] = round(cost, 6)

        for key, value in metrics.items():
            client.log_metric(run_id, key, value)
        client.log_param(run_id, "run_ended", _iso(self._end))


def _join(values: list[str] | None) -> str:
    return ", ".join(values or [])


def _iso(stamp: datetime.datetime | None) -> str:
    return stamp.isoformat() if stamp else ""
