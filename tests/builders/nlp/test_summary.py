"""
Tests for the console summary printed at the end of an NLP run.
"""

import contextlib
import datetime
import io
import re
from unittest import mock

import pytest

from cumulus_library import note_utils
from cumulus_library.builders.nlp import models, summary
from tests import nlp_utils
from tests.nlp_utils import add_doc


def make_stats(*, available: int = 3, had_text: int = 3, **per_table: list[int]):
    """Builds an NlpStats, sized by whichever per-table lists are given."""
    size = len(next(iter(per_table.values()), []))
    stats = models.NlpStats(size)
    stats.available = available
    stats.had_text = had_text
    for field, values in per_table.items():
        setattr(stats, field, values)
    return stats


def render(func, *args, **kwargs) -> str:
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        func(*args, **kwargs)
    return nlp_utils.strip_trailing_whitespace(output.getvalue())


def test_note_stats_rows_per_table():
    """Each table gets its own block, with failures listed before the responses."""
    stats = make_stats(
        considered=[3, 3],
        failed=[0, 2],
        got_response=[3, 1],
        from_cache=[0, 0],
        from_model=[3, 1],
    )

    output = render(summary.print_note_stats, stats, names=["age", "favorite_sushi"])

    assert (
        """ Notes processed:
  Available:                      3
  Had text:                       3
  Considered (age):               3
  Failed (age):                   0
  Got response (age):             3
    from cache:                   0
    fresh LLM calls:              3
  Considered (favorite_sushi):    3
  Failed (favorite_sushi):        2
  Got response (favorite_sushi):  1
    from cache:                   0
    fresh LLM calls:              1"""
        in output
    )


def test_failed_warning_names_only_the_failing_table():
    stats = make_stats(considered=[3, 3], failed=[0, 2], got_response=[3, 1])

    output = render(summary.print_note_stats, stats, names=["age", "favorite_sushi"])

    # Only the start of the warning: the test console is 80 columns wide, so rich wraps the rest.
    assert "WARNING: 2 notes failed for 'favorite_sushi'" in output
    assert "failed for 'age'" not in output


def test_failed_warning_is_singular_for_one_note():
    stats = make_stats(considered=[1], failed=[1], got_response=[0])

    output = render(summary.print_note_stats, stats, names=["age"])

    assert "WARNING: 1 note failed for 'age'" in output


def test_no_warnings_for_a_clean_run():
    stats = make_stats(considered=[3], got_response=[3], from_model=[3])

    output = render(summary.print_note_stats, stats, names=["age"])

    assert "WARNING" not in output


def test_throttled_and_stuck_notes_get_their_own_warnings():
    stats = make_stats(considered=[3], failed=[3], got_response=[0])
    stats.throttle_dropped = 2
    stats.stuck_dropped = 1

    output = render(summary.print_note_stats, stats, names=["age"])

    # Both are part of the failed count, and also called out with their more specific causes.
    assert "WARNING: 3 notes failed for 'age'" in output
    assert "WARNING: 2 notes dropped after repeated rate limiting" in output
    assert "WARNING: 1 note abandoned after the request stopped responding" in output


def test_no_notes_warning_replaces_the_filtered_out_warning():
    """With nothing to filter, the per-table "no notes matched" warning would only add noise."""
    stats = make_stats(available=0, had_text=0, considered=[0])

    output = render(summary.print_note_stats, stats, names=["age"])

    assert "WARNING: No notes were found to process." in output
    assert "matched the filters" not in output


def test_no_text_warning():
    stats = make_stats(available=2, had_text=0, considered=[0])

    output = render(summary.print_note_stats, stats, names=["age"])

    assert "WARNING: 2 notes found, but none had any text." in output
    assert "matched the filters" not in output


def test_filtered_out_warning_says_where_the_notes_went():
    stats = make_stats(available=5, had_text=4, considered=[0, 4], filtered_out=[4, 0])

    output = render(summary.print_note_stats, stats, names=["filtered", "all"])

    assert "WARNING: No notes matched the filters for 'filtered'." in output
    assert "Of 5 notes, 1 had no text and 4 were rejected" in output
    assert "for 'all'" not in output


def test_token_stats_hide_cache_writes_when_unused():
    stats = make_stats(considered=[1])
    stats.token_stats = models.TokenStats(new_input_tokens=14, output_tokens=10)

    output = render(summary.print_token_stats, stats)

    assert re.search(r"New input tokens:\s+14$", output, re.MULTILINE)
    assert re.search(r"Output tokens:\s+10$", output, re.MULTILINE)
    # Only Bedrock writes to the prompt cache, so the row only shows up once something did
    assert "written to cache" not in output
    assert "Estimated cost" not in output


def test_token_stats_show_cache_writes_and_cost():
    stats = make_stats(considered=[1])
    stats.token_stats = models.TokenStats(
        new_input_tokens=1_000, cache_written_input_tokens=500, output_tokens=1_000
    )
    stats.token_prices = models.TokenPrices(
        date=datetime.date(2026, 1, 1),
        new_input_tokens=1.0,
        cache_written_input_tokens=2.0,
        output_tokens=2.0,
        multiplier=0.5,
    )

    output = render(summary.print_token_stats, stats)

    assert re.search(r"Input tokens written to cache:\s+500$", output, re.MULTILINE)
    # (1,000 x $1 + 500 x $2 + 1,000 x $2) per 1,000 tokens, at the 50% batch multiplier
    assert re.search(r"Estimated cost \(as of Jan 2026\):\s+\$2\.00$", output, re.MULTILINE)


def test_print_summary_prints_both_blocks():
    stats = make_stats(considered=[1], got_response=[1], from_model=[1])

    output = render(summary.print_summary, stats, names=["age"])

    assert output.index("Notes processed:") < output.index("Token usage:")


####################
# No-match warning tests
####################


def _run_for_output(tmp_path, mock_client, mock_db_config, *, notes, tasks) -> str:
    """Runs a workflow with the given notes (id, text) and task configs, returning stdout."""
    workflow_path = nlp_utils.make_workflow(tmp_path, tasks)
    with open(f"{tmp_path}/doc.ndjson", "w", encoding="utf8") as f:
        for note_id, text in notes:
            add_doc(note_id, text, f)

    model = nlp_utils.MockModel(mock_client)
    _, output = nlp_utils.run_workflow(
        workflow_path,
        mock_db_config,
        notes=note_utils.NoteSource([tmp_path]),
        nlp_config=model.nlp_config(),
    )
    return output


@mock.patch("openai.OpenAI")
def test_warns_when_no_notes_found(mock_client, tmp_path, mock_db_config):
    output = _run_for_output(tmp_path, mock_client, mock_db_config, notes=[], tasks={"task": {}})
    assert "No notes were found to process" in output


@mock.patch("openai.OpenAI")
def test_warns_when_no_notes_have_text(mock_client, tmp_path, mock_db_config):
    output = _run_for_output(
        tmp_path,
        mock_client,
        mock_db_config,
        notes=[("1", None), ("2", None)],
        tasks={"task": {}},
    )
    assert "2 notes found, but none had any text" in output
    assert "matched the filters" not in output


@mock.patch("openai.OpenAI")
def test_warns_when_filters_match_nothing(mock_client, tmp_path, mock_db_config):
    output = _run_for_output(
        tmp_path,
        mock_client,
        mock_db_config,
        notes=[("1", None), ("2", "has a cold"), ("3", "has a cough")],
        tasks={"picky": {"select_by_word": ["fever"]}, "open": {}},
    )
    # Only the task that matched nothing warns
    assert "No notes matched the filters for 'picky'" in output
    assert "Of 3 notes, 1 had no text and 2 were rejected by the select/reject filters" in output
    # and it separates no-text from filtered notes.
    assert "for 'open'" not in output


@mock.patch("openai.OpenAI")
def test_no_warning_when_notes_match(mock_client, tmp_path, mock_db_config):
    output = _run_for_output(
        tmp_path,
        mock_client,
        mock_db_config,
        notes=[("1", "has a fever")],
        tasks={"task": {"select_by_word": ["fever"]}},
    )
    assert "WARNING" not in output
