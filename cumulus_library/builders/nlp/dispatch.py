"""Dispatches NLP prompts across endpoints, on a bounded set of worker threads.

This module spreads those requests over a fixed number of worker threads, each pinned to
one endpoint (an Azure deployment, or the single endpoint that Bedrock and local vLLM offer).

Two properties matter as much as the speedup:
- There are never more than `concurrency` requests outstanding, so raising the
  worker count cannot flood the endpoint.
- Results are handed back in *submission* order, so the parquet rows and upload refs a run
  produces are identical to what a serial run would have produced.
"""

import collections
import concurrent.futures
import dataclasses
import queue
import threading
import time

from cumulus_library import note_utils
from cumulus_library.builders.nlp import models

# How long to hold an endpoint back after it rate-limits us, when the server doesn't tell us.
DEFAULT_COOLDOWN_SECONDS = 5
# Ceiling on a server-provided Retry-After of 5m, so runs don't stall for hours.
MAX_COOLDOWN_SECONDS = 300
# How many times to re-issue a single note that keeps getting rate limited. This is on top of
# the retries the client SDKs already do internally (see models.MAX_RETRIES).
MAX_THROTTLE_RETRIES = 3
# Sanity check on user input, to avoid OOMing the machine
MAX_CONCURRENCY = 16


class ThrottledError(Exception):
    """
    Raised when a note kept getting rate limited and we gave up on it.
    Deliberately not a CumulusLibraryError: this never escapes the NLP driver.
    """


class Endpoint:
    """
    One place we can send requests, plus the state used to back off from it.
    Tracks lock state, cooldown, and the model object that actually issues requests.
    """

    def __init__(self, model: models.Model, name: str):
        self.model = model
        self.name = name
        # Wall clock (monotonic) before which no worker should send to this endpoint. Set by
        # whichever worker sees a rate limit, and respected by every worker sharing the
        # endpoint, so the whole dispatcher eases off rather than each worker finding out alone.
        self.cooldown_until = 0.0
        self.lock = threading.Lock()

    def wait_for_cooldown(self) -> None:
        with self.lock:
            remaining = self.cooldown_until - time.monotonic()
        if remaining > 0:
            time.sleep(remaining)

    def start_cooldown(self, seconds: float | None) -> None:
        seconds = DEFAULT_COOLDOWN_SECONDS if seconds is None else seconds
        seconds = min(seconds, MAX_COOLDOWN_SECONDS)
        with self.lock:
            self.cooldown_until = max(self.cooldown_until, time.monotonic() + seconds)


@dataclasses.dataclass
class _WorkItem:
    future: concurrent.futures.Future
    prompt: models.Prompt
    # Opaque payload handed straight back to the caller alongside the response, so the driver
    # can remember which note and table a result belongs to without us knowing about either.
    context: object


@dataclasses.dataclass
class Result:
    """One finished prompt: either a response or the error that stopped it.

    Failures are returned rather than raised so that one bad note can't abort the submission
    that happened to drain it. The caller decides what a failure means, note by note.
    """

    context: object
    response: models.PromptResponse | None = None
    error: Exception | None = None


class PromptDispatcher:
    """Runs prompts across a fixed set of workers, returning results in submission order.

    Both submit() and finish() hand back finished Results - submit() returns whatever it had
    to drain in order to make room, and finish() returns everything still outstanding:

        dispatcher = PromptDispatcher(nlp_config)
        for prompt, context in work:
            handle(dispatcher.submit(prompt, context))  # blocks once enough is outstanding
        handle(dispatcher.finish())
    """

    def __init__(self, nlp_config: note_utils.NlpConfig):
        self.endpoints = self._create_endpoints(nlp_config)
        self.concurrency = max(1, min(nlp_config.concurrency, MAX_CONCURRENCY))
        # Notes rate limited to the point of giving up. Surfaced to the user at the end of a
        # run, because otherwise a throttled run quietly produces a partial table.
        self.throttle_dropped = 0

        # Bound how far the note reader may run ahead of the workers. This is the memory
        # ceiling for in-flight work, and keeps the progress bar honest.
        self._max_pending = 2 * self.concurrency
        self._pending: collections.deque[_WorkItem] = collections.deque()

        # Lock to guard throttle_dropped, because it's incremented by multiple workers.
        self._throttle_lock = threading.Lock()
        # A queue of work items for the workers to pull from, and a deque of pending items in
        # submission order. The deque is what lets us drain in order, and the queue is
        # what lets the workers block on work without busy-waiting.
        self._queue = queue.Queue()
        # Workers are started here, and each one runs until finish() is called and it sees a None.
        self._workers = []

        # Start the workers. Each one is pinned to a single endpoint, so that if one endpoint
        # is throttled, the others can keep going. The dispatcher's concurrency is spread across
        # the endpoints in round-robin fashion, so if you have 3 deployments and
        # concurrency=6, each deployment gets 2 workers.
        for index in range(self.concurrency):
            endpoint = self.endpoints[index % len(self.endpoints)]
            worker = threading.Thread(
                target=self._worker_loop, args=(endpoint,), daemon=True, name=f"nlp-worker-{index}"
            )
            worker.start()
            self._workers.append(worker)

    @staticmethod
    def _create_endpoints(nlp_config: note_utils.NlpConfig) -> list[Endpoint]:
        """Builds one model per endpoint, validating each as we go.

        Workers sharing an endpoint share its model: the openai and boto3 clients are safe to
        call concurrently, and token stats are lock-guarded (see models.Provider.record_usage).
        Doing it per-endpoint rather than per-worker also keeps post_init_check to one live
        request each, which usefully catches a typo'd deployment name before any notes are sent.
        """
        deployments = nlp_config.azure_deployments or [None]
        endpoints = []
        for deployment in deployments:
            model = models.create_model(nlp_config, deployment=deployment)
            endpoints.append(Endpoint(model=model, name=deployment or nlp_config.model))
        return endpoints

    @property
    def token_stats(self) -> models.TokenStats:
        return models.sum_token_stats(endpoint.model.stats for endpoint in self.endpoints)

    @property
    def token_prices(self) -> models.TokenPrices | None:
        # Every endpoint runs the same model, so prices are identical across them.
        return self.endpoints[0].model.prices

    def submit(self, prompt: models.Prompt, context: object) -> list[Result]:
        """Queues a prompt, returning any results that had to be drained to make room.

        Blocks when the dispatcher is saturated - that back pressure is what keeps this a
        fixed-size pipe instead of an unbounded queue.
        """
        results = []
        while len(self._pending) >= self._max_pending:
            results.append(self._finish_one())

        item = _WorkItem(future=concurrent.futures.Future(), prompt=prompt, context=context)
        self._pending.append(item)
        self._queue.put(item)
        return results

    def finish(self) -> list[Result]:
        """Finish everything still outstanding and shuts the workers down."""
        results = []
        while self._pending:
            results.append(self._finish_one())

        for _ in self._workers:
            self._queue.put(None)
        for worker in self._workers:
            worker.join()
        self._workers = []

        return results

    def _finish_one(self) -> Result:
        """Pops the oldest outstanding item and blocks until it resolves.

        Finishing strictly left-to-right is what makes output order match a serial run. It
        costs some head-of-line blocking, but under throttling that stall is desirable anyway.
        """
        item = self._pending.popleft()
        try:
            return Result(context=item.context, response=item.future.result())
        except Exception as exc:
            return Result(context=item.context, error=exc)

    def _worker_loop(self, endpoint: Endpoint) -> None:
        while (item := self._queue.get()) is not None:
            try:
                item.future.set_result(self._run_with_retries(endpoint, item.prompt))
            except BaseException as exc:
                item.future.set_exception(exc)

    def _run_with_retries(self, endpoint: Endpoint, prompt: models.Prompt) -> models.PromptResponse:
        for attempt in range(MAX_THROTTLE_RETRIES + 1):
            endpoint.wait_for_cooldown()
            try:
                return endpoint.model.prompt(prompt)
            except Exception as exc:
                if not models.is_rate_limit_error(exc):
                    raise
                # Hold the whole endpoint back, not just this worker, then retry in place.
                # Stalling here *is* the back pressure, and it leaves ordering untouched
                # because the future stays pending the entire time.
                endpoint.start_cooldown(models.retry_after_seconds(exc))
                if attempt == MAX_THROTTLE_RETRIES:
                    with self._throttle_lock:
                        self.throttle_dropped += 1
                    raise ThrottledError(
                        f"gave up after {attempt + 1} rate-limited attempts "
                        f"against '{endpoint.name}'"
                    ) from exc
