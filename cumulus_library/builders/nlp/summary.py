"""
Console summary of an NLP run, printed once the pass over the notes is done.

This is the console counterpart of tracking.py: both read the same NlpStats, one for a person
watching the build and one for MLflow. Nothing here touches the builder or the database, so it
can be tested by handing it an NlpStats directly.
"""

import rich
import rich.table

from cumulus_library.builders.nlp import models


def print_summary(stats: models.NlpStats, *, names: list[str]) -> None:
    """Prints the note counts, any warnings about them, and the token usage."""
    print_note_stats(stats, names=names)
    print_token_stats(stats)


def print_note_stats(stats: models.NlpStats, *, names: list[str]) -> None:
    """Prints where every note went, per table, followed by warnings about anything missing.

    :param names: the table names, in the same order as the per-table lists in stats
    """
    rich.print(" Notes processed:")
    table = rich.table.Table("", "", box=None, show_header=False)
    table.add_row(" Available:", f"{stats.available:,}")
    table.add_row(" Had text:", f"{stats.had_text:,}")
    for idx, name in enumerate(names):
        suffix = f" ({name})" if name else ""
        table.add_row(f" Considered{suffix}:", f"{stats.considered[idx]:,}")
        table.add_row(f" Failed{suffix}:", f"{stats.failed[idx]:,}")
        table.add_row(f" Got response{suffix}:", f"{stats.got_response[idx]:,}")
        # Split the responses by what they cost, so a nearly-free re-run is obvious rather
        # than something you infer from the token block being small.
        table.add_row("   from cache:", f"{stats.from_cache[idx]:,}")
        table.add_row("   fresh LLM calls:", f"{stats.from_model[idx]:,}")
    rich.get_console().print(table)
    if not _print_no_notes_warning(stats):
        _print_filtered_out_warning(stats, names=names)
    _print_failed_warning(stats, names=names)
    _print_throttle_warning(stats)
    _print_stuck_warning(stats)


def print_token_stats(stats: models.NlpStats) -> None:
    """Prints the pooled token usage, and an estimated cost when the model has prices."""
    tokens = stats.token_stats
    rich.print("\n Token usage:")
    table = rich.table.Table("", "", box=None, show_header=False)
    table.add_row(" New input tokens:", f"{tokens.new_input_tokens:,}")
    table.add_row(" Input tokens read from cache:", f"{tokens.cache_read_input_tokens:,}")
    if tokens.cache_written_input_tokens:
        # This stat is only relevant for bedrock, so only show it if it's used
        table.add_row(" Input tokens written to cache:", f"{tokens.cache_written_input_tokens:,}")
    table.add_row(" Output tokens:", f"{tokens.output_tokens:,}")

    # Estimate cost, if provided
    if prices := stats.token_prices:
        cost = (
            tokens.new_input_tokens * prices.new_input_tokens
            + tokens.cache_read_input_tokens * prices.cache_read_input_tokens
            + tokens.cache_written_input_tokens * prices.cache_written_input_tokens
            + tokens.output_tokens * prices.output_tokens
        )
        cost /= 1_000  # all prices are "per 1,000 tokens"
        cost *= prices.multiplier
        when = prices.date.strftime("%b %Y")
        table.add_row(f" Estimated cost (as of {when}):", f"${cost:.2f}")

    rich.get_console().print(table)


##########
# Warnings
##########


def _print_warning(message: str) -> None:
    """Prints a warning in the shared style. Highlighting is off, since it lights up numbers."""
    rich.get_console().print(f"\n 🚨[bold red] WARNING:[/] {message}", highlight=False)


def _print_no_notes_warning(stats: models.NlpStats) -> bool:
    """
    Calls out a run that never saw any usable notes, which otherwise looks like a fast run.

    Returns whether it printed a warning, so the caller can skip printing the
    filtered-out warning (which is redundant if there were no notes at all).
    """
    if not stats.available:
        _print_warning(
            "No notes were found to process.\n"
            " Check the note folder and the resource types it contains."
        )
        return True
    if not stats.had_text:
        plural = "" if stats.available == 1 else "s"
        _print_warning(
            f"{stats.available:,} note{plural} found, but none had any text.\n"
            " Check that notes include attachments in a supported content type."
        )
        return True
    return False


def _print_filtered_out_warning(stats: models.NlpStats, *, names: list[str]) -> None:
    """Calls out tasks whose filters matched nothing, with where the other notes went."""
    no_text = stats.available - stats.had_text
    for idx, name in enumerate(names):
        if stats.considered[idx] or stats.already_done[idx]:
            continue
        label = f" for '{name}'" if name else ""
        _print_warning(
            f"No notes matched the filters{label}.\n"
            f" Of {stats.available:,} notes, {no_text:,} had no text and "
            f"{stats.filtered_out[idx]:,} were rejected by the select/reject filters."
        )


def _print_failed_warning(stats: models.NlpStats, *, names: list[str]) -> None:
    """Calls out tasks with notes that failed, which otherwise only show up as log lines.

    The count includes throttled and stuck notes, which get their own, more specific
    warnings below.
    """
    for idx, name in enumerate(names):
        if not (failed := stats.failed[idx]):
            continue
        label = f" for '{name}'" if name else ""
        plural = "" if failed == 1 else "s"
        _print_warning(
            f"{failed:,} note{plural} failed{label}, and are missing from the results.\n"
            " See the 'Failed to process note' messages above for details. "
            "Re-run to retry them (cached notes are free)."
        )


def _print_throttle_warning(stats: models.NlpStats) -> None:
    """Calls out notes we abandoned to rate limiting, which otherwise leave a silent gap.

    A throttled run still produces a table, just an incomplete one, so this needs to be
    hard to miss. Re-running is cheap because every note that did succeed is cached.
    """
    if not stats.throttle_dropped:
        return
    plural = "" if stats.throttle_dropped == 1 else "s"
    _print_warning(
        f"{stats.throttle_dropped:,} note{plural} dropped after "
        "repeated rate limiting, and are missing from the results.\n"
        " Re-run to pick them up (cached notes are free), and consider lowering "
        "--nlp-concurrency or adding another --azure-deployment."
    )


def _print_stuck_warning(stats: models.NlpStats) -> None:
    """Calls out notes abandoned because their worker stopped responding."""
    if not stats.stuck_dropped:
        return
    plural = "" if stats.stuck_dropped == 1 else "s"
    _print_warning(
        f"{stats.stuck_dropped:,} note{plural} abandoned after "
        "the request stopped responding, and are missing from the results.\n"
        " This usually means a dropped connection rather than a busy endpoint."
    )
