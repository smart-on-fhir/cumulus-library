"""Per-prompt MLflow tracing.

Kept separate from `tracking` so that `models` can reach tracing without importing the tracker:
`tracking` needs `models` for its stat types, so the two would otherwise import each other.
"""

import dataclasses
import types
import typing
from collections.abc import Callable

from cumulus_library import errors

INSTALL_HINT = (
    "MLflow experiment tracking requires the optional 'mlflow' dependency.\n"
    "Install it with: pip install 'cumulus-library[mlflow]'"
)

# Trace metadata key MLflow uses to associate a trace with a run. Setting it explicitly is the
# only reliable way to attribute traces here: MLflow's active-run state is thread-local, but
# trace-to-run association is not derived from it, so concurrent prompts for different tables
# would otherwise all land on whichever run was started last.
SOURCE_RUN_KEY = "mlflow.sourceRun"


@dataclasses.dataclass
class TraceInfo:
    """Says which MLflow run a given prompt's traces belong to.

    This travels with the prompt rather than living in ambient state, because prompts for
    several tables are in flight on different threads at once.
    """

    run_id: str
    tags: dict[str, str] = dataclasses.field(default_factory=dict)


# MLflow is an optional dependency, and importing it is deferred until tracing is actually on.
def import_mlflow() -> types.ModuleType:
    """Imports mlflow, turning the ImportError into an actionable message."""
    try:
        import mlflow

        return mlflow
    except ImportError as exc:  # pragma: no cover - depends on install state
        raise errors.CumulusLibraryError(INSTALL_HINT) from exc


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
