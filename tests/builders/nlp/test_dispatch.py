"""
Tests for concurrent NLP dispatch: spreading work across deployments, rate-limit cooldowns and
retries, and abandoning stuck or throttled notes.
"""

import collections
import concurrent.futures
import contextlib
import io
import pathlib
import queue
import threading
import time
from types import SimpleNamespace
from unittest import mock

import botocore.exceptions
import httpx
import openai
import pandas
import pytest

from cumulus_library import errors, note_utils
from cumulus_library.builders import nlp_builder
from cumulus_library.builders.nlp import dispatch as nlp_dispatch
from cumulus_library.builders.nlp import driver, models
from tests import nlp_utils
from tests.nlp_utils import add_doc


####################
# Concurrency tests
####################


# Helpers
def _write_notes(tmp_path, count: int) -> note_utils.NoteSource:
    """Drops `count` distinct notes on disk, named so responses can be keyed to them."""
    with open(f"{tmp_path}/doc.ndjson", "w", encoding="utf8") as f:
        for index in range(count):
            add_doc(str(index), f"Note {index}", f)
    return note_utils.NoteSource([tmp_path])


def _run(tmp_path, config, source, mock_db_config) -> tuple[nlp_builder.NlpBuilder, str]:
    """Runs the basic one-table workflow, returning the builder and what it printed."""
    return nlp_utils.run_workflow(
        nlp_utils.basic_workflow(tmp_path), mock_db_config, notes=source, nlp_config=config
    )


def test_concurrency_defaults_to_one_worker_per_deployment():
    """The default should parallelize across deployments without needing a second flag."""
    assert note_utils.NlpConfig({}).concurrency == 1
    assert note_utils.NlpConfig({"azure_deployments": ["a", "b", "c"]}).concurrency == 3
    # An explicit value always wins, so a single endpoint can still be driven concurrently.
    assert note_utils.NlpConfig({"nlp_concurrency": 8}).concurrency == 8
    assert note_utils.NlpConfig({"azure_deployments": ["a"], "nlp_concurrency": 4}).concurrency == 4


@nlp_utils.mock_env("azure")
@mock.patch("openai.AzureOpenAI")
def test_requests_spread_across_deployments(mock_client, tmp_path, mock_db_config):
    """Every deployment should get work, and no note should be sent to more than one."""
    source = _write_notes(tmp_path, 12)
    model = nlp_utils.MockModel(mock_client, provider="azure")

    # Let's create a little handler that records which deployment got which note,
    # so we can confirm the work was spread.
    lock = threading.Lock()
    seen = []

    def handler(**kwargs):
        with lock:
            seen.append((kwargs["model"], model.note_text_of(kwargs)))
        return {}

    # Add this handler to the mock model, so every request will go through it.
    model.mock_openai_handler(handler)

    config = model.nlp_config(deployments=["dep-a", "dep-b", "dep-c"])
    builder, _ = _run(tmp_path, config, source, mock_db_config)

    # We should have gotten a response for every note, and nothing should have been dropped.
    assert builder.stats.got_response[0] == 12
    # Each deployment pulled some work off the shared queue.
    assert {deployment for deployment, _ in seen} == {"dep-a", "dep-b", "dep-c"}
    # And each note was requested exactly once, not once per deployment.
    notes = sorted(note for _, note in seen)
    assert notes == sorted(f"Note {index}" for index in range(12))


@nlp_utils.mock_env("azure")
@mock.patch("openai.AzureOpenAI")
def test_never_exceeds_configured_concurrency(mock_client, tmp_path, mock_db_config):
    """In-flight requests must stay under our concurrency limit."""
    source = _write_notes(tmp_path, 20)
    model = nlp_utils.MockModel(mock_client, provider="azure")

    in_flight = 0
    max_inflight = 0
    lock = threading.Lock()

    def handler(**kwargs):
        # Reference the outer variables so we can update them in this closure.
        nonlocal in_flight, max_inflight
        with lock:
            in_flight += 1
            max_inflight = max(max_inflight, in_flight)
        time.sleep(0.01)  # hold the slot long enough for overlap to actually show up
        with lock:
            in_flight -= 1
        return {}

    model.mock_openai_handler(handler)

    config = model.nlp_config(deployments=["dep-a", "dep-b"], concurrency=4)
    builder, _ = _run(tmp_path, config, source, mock_db_config)

    assert builder.stats.got_response[0] == 20
    # Confirm that we never exceeded the configured concurrency limit.
    assert max_inflight <= config.concurrency
    # And confirm we really did run in parallel, otherwise the cap check proves nothing.
    assert max_inflight > 1


@pytest.mark.parametrize(
    "concurrency,warns",
    [
        (nlp_dispatch.WARN_CONCURRENCY, False),
        (nlp_dispatch.WARN_CONCURRENCY + 1, True),
    ],
)
@nlp_utils.mock_env("azure")
@mock.patch("openai.AzureOpenAI")
def test_warns_over_concurrency_threshold(mock_client, concurrency, warns, capsys):
    """Going past the warning threshold should warn, but still honor the requested value."""
    model = nlp_utils.MockModel(mock_client, provider="azure")
    config = model.nlp_config(concurrency=concurrency)

    dispatcher = nlp_dispatch.PromptDispatcher(config)
    dispatcher.finish()

    assert dispatcher.concurrency == concurrency  # not capped
    assert ("--nlp-concurrency" in capsys.readouterr().out) == warns


@nlp_utils.mock_env("azure")
@mock.patch("openai.AzureOpenAI")
def test_output_is_identical_regardless_of_concurrency(mock_client, tmp_path, mock_db_config):
    """Rows must land in submission order, no matter which worker finished first."""

    def run_once(sub_dir: pathlib.Path, concurrency: int) -> tuple[list[str], list[str]]:
        sub_dir.mkdir()
        source = _write_notes(sub_dir, 15)
        model = nlp_utils.MockModel(mock_client, provider="azure")

        finished = []
        lock = threading.Lock()

        def handler(**kwargs):
            # Make later notes finish *sooner*, so completion order actively fights
            # submission order. That's the thing the ordered drain has to paper over.
            note = model.note_text_of(kwargs)
            # All these dummy notes are just "Note x", so we can parse the index out of the string.
            index = int(note.removeprefix("Note "))
            # The sleep time is chosen so that every 5th note will finish first,
            # then the next 5th, etc.
            time.sleep((4 - (index % 5)) * 0.02)
            with lock:
                finished.append(note)
            return {}

        model.mock_openai_handler(handler)
        config = model.nlp_config(deployments=["dep-a", "dep-b"], concurrency=concurrency)
        _run(sub_dir, config, source, mock_db_config)

        folder = driver.get_output_path_for_task(
            config, "task", SimpleNamespace(version=0), mock_db_config.db
        )
        rows = []
        for path in sorted(str(p) for p in folder.ls()):
            rows.extend(pandas.read_parquet(path).to_dict(orient="records"))
        return [row["note_ref"] for row in rows], finished

    serial, serial_finished = run_once(tmp_path / "serial", 1)
    parallel, parallel_finished = run_once(tmp_path / "parallel", 4)

    # Sanity check that this test can actually fail: the concurrent run really did complete
    # requests out of order, while the serial run by definition did not.
    assert parallel_finished != serial_finished
    # Yet both wrote their rows in note order, which is the order they were submitted in.
    # These are the expected anon_ids for ids in the range 1-15.
    expected = [
        "DocumentReference/24373d9896b2bf8aa35c10dba24782046d501a9281afe5605abf4dce1f5a99c1",
        "DocumentReference/69123f5b2305aba4bc734b41c66cedab639b3e81d4ae8eeb9569d6dc1476a1e7",
        "DocumentReference/4457e00465fa53651f67203ddb28280ddcb5693599ad9044177b64f65d5b6512",
        "DocumentReference/7754781daf881b5c7006fcd5452df13ff49db01114bcec4a80c6394d745454f5",
        "DocumentReference/0d9af682d646605ca219a1ee5177506d282ecdf873c89f3e312440624fcebbe7",
        "DocumentReference/b5f3542891693362bfdc76cc58c869989eec4ac5df7c6939265ecf0280edc3c5",
        "DocumentReference/a59ce867c6988ea302931407b25a22e7abed6fe461525f4caee636cf90ab0289",
        "DocumentReference/263e5d438b0e21eb66feea65e206e3beaebb0cdb86d508700453a24999d5d6fc",
        "DocumentReference/daeaa53ba126e4c429179896c4362e86ab2e695375002fcc946d40bbe06106d0",
        "DocumentReference/cb9e1f3efb1b384fd227d811056fe4abd4ff88e9ca433f734710869dfcdb2eda",
        "DocumentReference/5adbd1ba26f993e91f7f3b5246e3d7242892041057d7dbf1b61c71452a208d5d",
        "DocumentReference/32ebbf192d1993b6593cb0e0fbf1f03d36a6f27521cf9d1aac17d9fcc28ffd6e",
        "DocumentReference/27846faaa73185574ab039f00cc72091d0433b4214219edfea6573a4554af7ac",
        "DocumentReference/c72fc0b9617e52b6bbb3e517203c0fc660586e5e238c504ee53bd1f0095f7695",
        "DocumentReference/06aec3b4fc7ee610b84a76032082531757b27d7d0a44460eab2eef341901e3f9",
    ]
    assert serial == expected
    assert parallel == expected


@nlp_utils.mock_env("azure")
@mock.patch("openai.AzureOpenAI")
def test_rate_limit_retries_after_cooldown(mock_client, tmp_path, mock_db_config):
    """A throttled note should be retried, not dropped."""
    source = _write_notes(tmp_path, 3)
    model = nlp_utils.MockModel(mock_client, provider="azure")

    failed_once = set()
    lock = threading.Lock()

    def handler(**kwargs):
        note = model.note_text_of(kwargs)
        with lock:
            first_time = note not in failed_once
            # After the first failure, the note will succeed on retry.
            failed_once.add(note)
        if first_time:
            raise openai.RateLimitError(
                "slow down",
                response=httpx.Response(429, request=httpx.Request("POST", "/")),
                body=None,
            )
        return {}

    model.mock_openai_handler(handler)

    config = model.nlp_config(concurrency=2)
    # The default cooldown is 5 seconds; we patch it down to 0.01s so the test runs quickly.
    with mock.patch.object(nlp_dispatch, "DEFAULT_COOLDOWN_SECONDS", 0.01):
        builder, _ = _run(tmp_path, config, source, mock_db_config)

    # Every note eventually succeeded, and nothing was reported as dropped.
    assert builder.stats.got_response[0] == 3
    assert builder.stats.throttle_dropped == 0


@nlp_utils.mock_env("azure")
@mock.patch("openai.AzureOpenAI")
def test_persistent_rate_limiting_is_reported(mock_client, tmp_path, mock_db_config):
    """Giving up on a note has to be loud - a silent partial table is the bad outcome."""
    source = _write_notes(tmp_path, 2)
    model = nlp_utils.MockModel(mock_client, provider="azure")

    def handler(**kwargs):
        # Always fail, so the dispatcher will eventually give up and drop the note.
        raise openai.RateLimitError(
            "slow down", response=httpx.Response(429, request=httpx.Request("POST", "/")), body=None
        )

    model.mock_openai_handler(handler)

    config = model.nlp_config(concurrency=2)
    # The default cooldown is 5 seconds; we patch it down to 0.01s so the test runs quickly.
    with mock.patch.object(nlp_dispatch, "DEFAULT_COOLDOWN_SECONDS", 0.01):
        builder, output = _run(tmp_path, config, source, mock_db_config)

    assert builder.stats.got_response[0] == 0
    assert builder.stats.throttle_dropped == 2
    assert "2 notes dropped after repeated rate limiting" in output
    assert "--nlp-concurrency" in output


@nlp_utils.mock_env("azure")
@mock.patch("openai.AzureOpenAI")
def test_non_rate_limit_errors_are_not_retried(mock_client, tmp_path, mock_db_config):
    """Only throttling earns a retry, not other errors"""
    source = _write_notes(tmp_path, 2)
    model = nlp_utils.MockModel(mock_client, provider="azure")

    calls = []
    lock = threading.Lock()

    def handler(**kwargs):
        with lock:
            calls.append(model.note_text_of(kwargs))
        # Raise a generic API error, which is not retried by the dispatcher.
        raise openai.APIError("nope", mock.MagicMock(), body=None)

    model.mock_openai_handler(handler)

    config = model.nlp_config(concurrency=2)
    builder, output = _run(tmp_path, config, source, mock_db_config)

    assert builder.stats.got_response[0] == 0
    assert builder.stats.throttle_dropped == 0  # these weren't throttles that dropped
    assert len(calls) == 2  # one attempt each, no retries
    assert "Failed to process note:" in output


@nlp_utils.mock_env("azure")
@mock.patch("openai.AzureOpenAI")
def test_batching_rejects_multiple_deployments(mock_client, tmp_path, mock_db_config, note_source):
    """Batch resume state is keyed per-provider, so several deployments would collide."""
    model = nlp_utils.MockModel(mock_client, provider="azure", model_id="gpt4o")
    config = model.nlp_config(batching=True, deployments=["dep-a", "dep-b"])

    with pytest.raises(errors.CumulusLibraryError, match="does not support multiple"):
        _run(tmp_path, config, note_source, mock_db_config)


@nlp_utils.mock_env("azure")
@mock.patch("openai.AzureOpenAI")
def test_token_stats_summed_across_deployments(mock_client, tmp_path, mock_db_config):
    """Usage from every endpoint has to roll up, or cost estimates read low."""
    source = _write_notes(tmp_path, 6)
    model = nlp_utils.MockModel(mock_client, provider="azure", model_id="gpt4o")
    model.mock_openai_handler(lambda **kwargs: {})

    config = model.nlp_config(deployments=["dep-a", "dep-b", "dep-c"])
    builder, _ = _run(tmp_path, config, source, mock_db_config)

    # The mock reports 19 prompt tokens (5 cached) and 10 completion tokens per request.
    assert builder.stats.got_response[0] == 6
    assert builder.stats.token_stats.output_tokens == 60
    assert builder.stats.token_stats.cache_read_input_tokens == 30
    assert builder.stats.token_stats.new_input_tokens == 6 * (19 - 5)


@mock.patch("boto3.client")
def test_bedrock_throttling_is_recognized_and_retried(mock_client, tmp_path, mock_db_config):
    """Bedrock signals rate limits with a botocore ClientError, not an openai exception.

    Worth an end-to-end test rather than just a unit test of the predicate, because the whole
    point is that the dispatcher's cooldown path triggers for a completely different error
    shape than the Azure/local one.
    """
    source = _write_notes(tmp_path, 3)
    model = nlp_utils.MockModel(mock_client, provider="bedrock")
    model._boto.exceptions.ClientError = botocore.exceptions.ClientError

    throttled = set()
    lock = threading.Lock()
    ok_response = model._boto.converse.return_value

    def converse(**kwargs):
        # The note text is the only per-request thing we can key on here.
        note = kwargs["messages"][0]["content"][0]["text"]
        with lock:
            first_time = note not in throttled
            throttled.add(note)
        if first_time:
            raise botocore.exceptions.ClientError(
                {"Error": {"Code": "ThrottlingException", "Message": "slow down"}}, "Converse"
            )
        return ok_response

    model._boto.converse.side_effect = converse

    config = model.nlp_config(concurrency=2)
    with mock.patch.object(nlp_dispatch, "DEFAULT_COOLDOWN_SECONDS", 0.01):
        builder, _ = _run(tmp_path, config, source, mock_db_config)

    # Each note was throttled once, backed off, and then succeeded.
    assert builder.stats.got_response[0] == 3
    assert builder.stats.throttle_dropped == 0


def test_bedrock_non_throttle_errors_are_not_treated_as_rate_limits():
    """Only the throttling codes earn a retry - everything else is a real failure."""

    def client_error(code):
        return botocore.exceptions.ClientError({"Error": {"Code": code}}, "Converse")

    assert models.is_rate_limit_error(client_error("ThrottlingException"))
    assert models.is_rate_limit_error(client_error("TooManyRequestsException"))
    assert not models.is_rate_limit_error(client_error("AccessDeniedException"))
    assert not models.is_rate_limit_error(ValueError("something else entirely"))


def test_retry_after_header_drives_cooldown():
    """A server-suggested wait should win over our default cooldown."""

    def rate_limit(headers: dict | None = None):
        return openai.RateLimitError(
            "slow down plz and thx",
            response=httpx.Response(429, headers=headers or {}, request=httpx.Request("POST", "/")),
            body=None,
        )

    assert models.retry_after_seconds(rate_limit({"Retry-After": "0.25"})) == 0.25
    # An HTTP-date is legal in Retry-After, but we don't parse those - fall back to the default.
    http_date = rate_limit({"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"})
    assert models.retry_after_seconds(http_date) is None
    # And a response that made no suggestion at all leaves us to pick - whether it carried no
    # headers whatsoever, or headers that simply didn't include one.
    assert models.retry_after_seconds(rate_limit()) is None
    assert models.retry_after_seconds(rate_limit({"x-request-id": "abc"})) is None

    # Confirm the suggestion actually reaches the endpoint, rather than the default. The bound
    # is loose on purpose - what matters is that we waited ~0.25s and not DEFAULT_COOLDOWN's 5.
    endpoint = nlp_dispatch.Endpoint(model=mock.MagicMock(), name="dep-a")
    before = time.monotonic()
    endpoint.start_cooldown(models.retry_after_seconds(rate_limit({"Retry-After": "0.25"})))
    assert 0 < endpoint.cooldown_until - before < 1

    # A server asking for longer than we're willing to wait gets clamped.
    endpoint.start_cooldown(nlp_dispatch.MAX_COOLDOWN_SECONDS * 10)
    assert endpoint.cooldown_until - time.monotonic() <= nlp_dispatch.MAX_COOLDOWN_SECONDS


@nlp_utils.mock_env("azure")
@mock.patch("openai.AzureOpenAI")
def test_note_that_cannot_be_prompted_is_skipped(mock_client, tmp_path, mock_db_config):
    """A note that fails before it is ever queued shouldn't take the run down with it."""
    source = _write_notes(tmp_path, 3)
    model = nlp_utils.MockModel(mock_client, provider="azure")
    model.mock_openai_handler(lambda **kwargs: {})

    real_make_prompt = driver.NlpNotePool._make_prompt
    seen = []

    def flaky_make_prompt(self, task_name, task, text):
        seen.append(text)
        if len(seen) == 2:  # blow up on the second note only
            raise RuntimeError("bad prompt")
        return real_make_prompt(self, task_name, task, text)

    # Check the console output too, to confirm the error was logged, but not fatal.
    with mock.patch.object(driver.NlpNotePool, "_make_prompt", flaky_make_prompt):
        builder, output = _run(tmp_path, model.nlp_config(), source, mock_db_config)

    # The other two notes still made it through.
    assert builder.stats.got_response[0] == 2
    assert "Failed to process note: bad prompt" in output


def test_default_cooldown_backs_off_exponentially():
    """With no Retry-After to go on, each attempt should wait longer than the last.

    Retrying at a flat interval against an endpoint that already said "too fast" tends to just
    earn another rate limit, so the fallback doubles per attempt.
    """
    waits = []
    for attempt in range(nlp_dispatch.MAX_THROTTLE_RETRIES + 1):
        endpoint = nlp_dispatch.Endpoint(model=mock.MagicMock(), name="dep-a")
        before = time.monotonic()
        endpoint.start_cooldown(None, attempt=attempt)  # None = server gave no guidance
        waits.append(endpoint.cooldown_until - before)

    base = nlp_dispatch.DEFAULT_COOLDOWN_SECONDS
    assert [round(w) for w in waits] == [base * 2**n for n in range(len(waits))]
    assert waits == sorted(waits), "each attempt should back off further than the last"

    # A server-provided wait is trusted as-is, not multiplied by the attempt number.
    endpoint = nlp_dispatch.Endpoint(model=mock.MagicMock(), name="dep-a")
    before = time.monotonic()
    endpoint.start_cooldown(1.5, attempt=3)
    assert round(endpoint.cooldown_until - before, 1) == 1.5


def test_capped_cooldown_is_explained():
    """Ignoring the server's requested wait shouldn't be silent."""
    endpoint = nlp_dispatch.Endpoint(model=mock.MagicMock(), name="dep-a")

    console_output = io.StringIO()
    with contextlib.redirect_stdout(console_output):
        endpoint.start_cooldown(nlp_dispatch.MAX_COOLDOWN_SECONDS * 4)
        # Only said once per endpoint - repeating it per throttled note would be noise.
        endpoint.start_cooldown(nlp_dispatch.MAX_COOLDOWN_SECONDS * 4)

    output = console_output.getvalue()
    assert output.count("asked us to wait") == 1
    assert "dep-a" in output
    assert f"Capping at {nlp_dispatch.MAX_COOLDOWN_SECONDS}s" in output
    assert endpoint.cooldown_until - time.monotonic() <= nlp_dispatch.MAX_COOLDOWN_SECONDS

    # Our own backoff is ours to cap, so hitting the ceiling that way says nothing.
    quiet = nlp_dispatch.Endpoint(model=mock.MagicMock(), name="dep-b")
    console_output = io.StringIO()
    with contextlib.redirect_stdout(console_output):
        quiet.start_cooldown(None, attempt=99)
    assert console_output.getvalue() == ""
    assert quiet.cooldown_until - time.monotonic() <= nlp_dispatch.MAX_COOLDOWN_SECONDS


def test_stuck_worker_does_not_freeze_the_run(monkeypatch):
    """A worker that never returns must cost us one note, not the whole run."""
    monkeypatch.setattr(nlp_dispatch, "STUCK_NOTE_SECONDS", 0.1)
    monkeypatch.setattr(nlp_dispatch, "MAX_STUCK_EXTENSIONS", 0)

    release = threading.Event()

    def prompt(prompt_obj):
        if prompt_obj.user == "stuck":
            release.wait()  # never set until teardown: this worker is gone for the run
            return "unreachable"
        return f"ok:{prompt_obj.user}"

    model = mock.MagicMock()
    model.prompt.side_effect = prompt
    model.MODEL_ID = "gpt-oss-120b"
    endpoint = nlp_dispatch.Endpoint(model=model, name="dep-a")

    dispatcher = nlp_dispatch.PromptDispatcher.__new__(nlp_dispatch.PromptDispatcher)
    dispatcher.endpoints = [endpoint]
    dispatcher.concurrency = 2
    dispatcher.throttle_dropped = 0
    dispatcher.stuck_dropped = 0
    dispatcher._max_pending = 4
    dispatcher._pending = collections.deque()
    dispatcher._throttle_lock = threading.Lock()
    dispatcher._queue = queue.Queue()
    dispatcher._workers = []
    for index in range(dispatcher.concurrency):
        worker = threading.Thread(
            target=dispatcher._worker_loop, args=(endpoint,), daemon=True, name=f"w{index}"
        )
        worker.start()
        dispatcher._workers.append(worker)

    try:
        for user in ["stuck", "second", "third"]:
            dispatcher.submit(SimpleNamespace(user=user), user)
        results = dispatcher.finish()

        # The stuck note comes back as a failure rather than never coming back at all...
        assert [result.context for result in results] == ["stuck", "second", "third"]
        assert isinstance(results[0].error, nlp_dispatch.StuckError)
        assert dispatcher.stuck_dropped == 1

        # ...and crucially, the notes queued behind it still completed.
        assert [result.response for result in results[1:]] == ["ok:second", "ok:third"]
        assert [result.error for result in results[1:]] == [None, None]
    finally:
        release.set()


def test_cooldown_extends_the_stuck_deadline(monkeypatch):
    """A note waiting out a rate limit is patient, not wedged, and must not be dropped."""
    monkeypatch.setattr(nlp_dispatch, "STUCK_NOTE_SECONDS", 0.05)

    endpoint = nlp_dispatch.Endpoint(model=mock.MagicMock(), name="dep-a")
    dispatcher = nlp_dispatch.PromptDispatcher.__new__(nlp_dispatch.PromptDispatcher)
    dispatcher.endpoints = [endpoint]
    dispatcher.stuck_dropped = 0
    dispatcher._pending = collections.deque()

    item = nlp_dispatch._WorkItem(
        future=concurrent.futures.Future(), prompt=mock.MagicMock(), context="slow"
    )
    dispatcher._pending.append(item)

    # Hold the endpoint back, then resolve the note while it is still cooling down.
    endpoint.start_cooldown(2)

    def resolve_later():
        time.sleep(0.3)  # several STUCK_NOTE_SECONDS worth of extensions
        item.future.set_result("late but fine")

    finisher = threading.Thread(target=resolve_later, daemon=True)
    finisher.start()

    result = dispatcher._finish_one()
    finisher.join(timeout=5)

    assert result.response == "late but fine"
    assert result.error is None
    assert dispatcher.stuck_dropped == 0


@nlp_utils.mock_env("azure")
@mock.patch("openai.AzureOpenAI")
def test_stuck_notes_are_reported(mock_client, tmp_path, mock_db_config):
    """Abandoning a stuck note has to be loud, for the same reason throttling is."""
    source = _write_notes(tmp_path, 1)
    model = nlp_utils.MockModel(mock_client, provider="azure")

    STUCK_NOTE_SECONDS = 0.05
    MAX_STUCK_EXTENSIONS = 0

    def handler(**kwargs):
        # Outlast the patched wait, but still return, so the worker can shut down.
        time.sleep(STUCK_NOTE_SECONDS * 6)
        return {}

    model.mock_openai_handler(handler)

    config = model.nlp_config(concurrency=1)
    with mock.patch.object(nlp_dispatch, "STUCK_NOTE_SECONDS", STUCK_NOTE_SECONDS):
        with mock.patch.object(nlp_dispatch, "MAX_STUCK_EXTENSIONS", MAX_STUCK_EXTENSIONS):
            builder, output = _run(tmp_path, config, source, mock_db_config)

    assert builder.stats.got_response[0] == 0
    assert builder.stats.stuck_dropped == 1
    assert "1 note abandoned after the request stopped responding" in output
    assert "dropped connection" in output


def test_stuck_note_is_dropped_when_nothing_is_cooling_down(monkeypatch):
    """Extensions exist to wait out cooldowns, so an idle endpoint buys a stuck note nothing."""
    monkeypatch.setattr(nlp_dispatch, "STUCK_NOTE_SECONDS", 0.01)
    monkeypatch.setattr(nlp_dispatch, "MAX_STUCK_EXTENSIONS", 2)

    # The note is waiting on a worker that is not cooling down, so it is stuck rather than patient.
    endpoint = nlp_dispatch.Endpoint(model=mock.MagicMock(), name="dep-a")
    dispatcher = nlp_dispatch.PromptDispatcher.__new__(nlp_dispatch.PromptDispatcher)
    dispatcher.endpoints = [endpoint]
    dispatcher.stuck_dropped = 0
    dispatcher._pending = collections.deque()
    dispatcher._pending.append(
        nlp_dispatch._WorkItem(
            future=concurrent.futures.Future(), prompt=mock.MagicMock(), context="stuck"
        )
    )

    # Nothing is holding the endpoint back, so the note is stuck rather than merely patient.
    assert dispatcher._any_endpoint_cooling_down() is False

    result = dispatcher._finish_one()
    assert result.context == "stuck"
    assert isinstance(result.error, nlp_dispatch.StuckError)
    assert dispatcher.stuck_dropped == 1


def test_request_timeout_is_not_counted_as_stuck(monkeypatch):
    """A request that timed out on its own answered us - the worker isn't stuck."""
    monkeypatch.setattr(nlp_dispatch, "STUCK_NOTE_SECONDS", 5)

    dispatcher = nlp_dispatch.PromptDispatcher.__new__(nlp_dispatch.PromptDispatcher)
    dispatcher.endpoints = [nlp_dispatch.Endpoint(model=mock.MagicMock(), name="dep-a")]
    dispatcher.stuck_dropped = 0
    dispatcher._pending = collections.deque()

    item = nlp_dispatch._WorkItem(
        future=concurrent.futures.Future(), prompt=mock.MagicMock(), context="slow"
    )

    # Test what happens if a model raises a TimeoutError; reaches us through the future
    # so it comes back as that error instead of costing an extension on stuck/waiting.
    error = TimeoutError("request timed out")
    item.future.set_exception(error)
    dispatcher._pending.append(item)

    result = dispatcher._finish_one()

    assert result.context == "slow"
    assert result.error is error
    assert result.response is None
    assert dispatcher.stuck_dropped == 0


@mock.patch("boto3.client")
def test_bedrock_concurrency(mock_client):
    nlp_mocker = nlp_utils.MockModel(
        mock_client,
        model_id="claude-sonnet45",
        provider="bedrock",
    )

    nlp_config = nlp_mocker.nlp_config()
    # Calling the model generation will assign a boto client, but we don't care
    # about using the model itself
    models.ClaudeSonnet45Model(nlp_config)
    assert nlp_config.cli_concurrency is None
    assert nlp_config.concurrency == 1
    passed_config = mock_client.call_args[1]["config"]._user_provided_options
    assert "max_pool_connections" not in passed_config.keys()

    nlp_config = nlp_mocker.nlp_config(concurrency=10)
    models.ClaudeSonnet45Model(nlp_config)
    assert nlp_config.cli_concurrency == 10
    assert nlp_config.concurrency == 10
    passed_config = mock_client.call_args[1]["config"]._user_provided_options
    assert passed_config["max_pool_connections"] == 10
