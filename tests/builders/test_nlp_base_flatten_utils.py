"""Tests for the wide/highlight table config vocabulary and its normalization"""

import msgspec
import pytest

from cumulus_library import errors
from cumulus_library.builders.nlp import base_flatten_utils


def highlight(**kwargs) -> base_flatten_utils.HighlightSpec:
    base = {
        "ls_variable_name": "Thing",
        "ls_subvalue_name": "Thing Kind",
        "path": "thing.kind",
        "span_path": "thing.spans",
    }
    return msgspec.convert(base | kwargs, base_flatten_utils.HighlightSpec)


###############################################################################
# Wide: unnests
###############################################################################
def test_unnest_defaults():
    [unnest] = base_flatten_utils.normalize_unnests(["a.mentions"], owner="t")
    assert unnest == {
        "path": "a.mentions",
        "alias": "mentions",
        "root": "nlp.result",
        "join": "cross",
    }


def test_unnests_chain_off_the_previous_alias():
    first, second = base_flatten_utils.normalize_unnests(
        [
            msgspec.convert({"path": "meds", "alias": "med"}, base_flatten_utils.UnnestSpec),
            msgspec.convert({"path": "events", "join": "left"}, base_flatten_utils.UnnestSpec),
        ],
        owner="t",
    )
    assert first["root"] == "nlp.result"
    assert second == {"path": "events", "alias": "events", "root": "med", "join": "left"}


def test_unnest_result_shorthand():
    [unnest] = base_flatten_utils.normalize_unnests(
        [msgspec.convert({"path": "x", "root": "result"}, base_flatten_utils.UnnestSpec)], owner="t"
    )
    assert unnest["root"] == "nlp.result"


def test_unnest_auto_alias_numbers_entries():
    unnests = base_flatten_utils.normalize_unnests(["a", "b"], owner="t", auto_alias=True)
    assert [u["alias"] for u in unnests] == ["mention0", "mention1"]
    assert unnests[1]["root"] == "mention0"


def test_bad_join_kind_is_rejected_when_decoded():
    with pytest.raises(msgspec.ValidationError, match=r"Invalid enum value 'inner'"):
        msgspec.convert({"path": "m", "join": "inner"}, base_flatten_utils.UnnestSpec)


###############################################################################
# Wide: columns
###############################################################################
def columns(*entries, unnests=()):
    cols = [
        e if isinstance(e, str) else msgspec.convert(e, base_flatten_utils.ColumnSpec)
        for e in entries
    ]
    return base_flatten_utils.normalize_wide_table(list(unnests), cols, owner="t")


def test_string_column_defaults():
    _, [col] = columns("thing.value")
    assert col == {
        "path": "thing.value",
        "alias": "value",
        "root": "nlp.result",
        "sql_type": "varchar",
    }


def test_columns_root_on_the_last_unnest_by_default():
    _, [col] = columns("kind", unnests=["mentions"])
    assert col["root"] == "mentions"


def test_column_options():
    _, [col] = columns(
        {"path": "k.k", "transform": "UPPER", "sql_type": "integer", "alias": "kk"},
    )
    assert col["transform"] == "UPPER"
    assert col["sql_type"] == "integer"
    assert col["alias"] == "kk"


def test_expr_column_is_kept_verbatim():
    _, [col] = columns({"expr": "nlp.result.a OR nlp.result.b", "alias": "either"})
    assert col == {"expr": "nlp.result.a OR nlp.result.b", "alias": "either", "sql_type": "varchar"}


@pytest.mark.parametrize(
    ("entries", "message"),
    [
        ([], "must declare `columns`"),
        (["a.value", "b.value"], "duplicate output columns"),
        (["a.note_ref"], "note_ref"),
        ([{"path": "x", "root": "nope"}], "not nlp.result or an unnest alias"),
        ([{"expr": "1"}], "explicit alias"),
        ([{"expr": "1", "alias": "one", "root": "result"}], "can't also set"),
        ([{"expr": "1", "path": "a", "alias": "one"}], "not both"),
        ([{"alias": "nothing"}], "needs a `path` or an `expr`"),
    ],
)
def test_invalid_columns_are_rejected(entries, message):
    with pytest.raises(errors.NlpTableConfigError, match=message):
        columns(*entries)


def test_unknown_column_field_is_rejected_when_decoded():
    with pytest.raises(msgspec.ValidationError, match="paht"):
        msgspec.convert({"paht": "x"}, base_flatten_utils.ColumnSpec)


###############################################################################
# Highlights
###############################################################################
def test_highlight_defaults():
    [normalized] = base_flatten_utils.normalize_highlights([highlight()], owner="t")

    assert normalized["root"] == "nlp.result"
    assert normalized["mention_path"] == "thing"
    assert normalized["merge_spans"] is False
    assert normalized["require"] is None
    assert normalized["sentinel"] is None
    assert normalized["unnests"] == []


def test_highlight_mention_that_is_the_value_is_its_own_parent():
    [normalized] = base_flatten_utils.normalize_highlights([highlight(path="surgery")], owner="t")
    assert normalized["mention_path"] == "surgery"


def test_highlight_within_resolves_unnest_chain():
    [normalized] = base_flatten_utils.normalize_highlights(
        [highlight(within=["meds", "events"], path="ade", span_path="spans")], owner="t"
    )
    assert normalized["root"] == "mention1"
    assert [(u["path"], u["alias"], u["root"]) for u in normalized["unnests"]] == [
        ("meds", "mention0", "nlp.result"),
        ("events", "mention1", "mention0"),
    ]


def test_highlight_list_span_path_merges():
    [normalized] = base_flatten_utils.normalize_highlights(
        [highlight(span_path=["a.spans", "b.spans"])], owner="t"
    )
    assert normalized["merge_spans"] is True


@pytest.mark.parametrize(
    ("require", "sentinel"),
    [
        ("not_null", None),
        ("not_mentioned", "NOT_MENTIONED"),
        ("nota", "NONE_OF_THE_ABOVE"),
    ],
)
def test_highlight_require_sentinels(require, sentinel):
    [normalized] = base_flatten_utils.normalize_highlights([highlight(require=require)], owner="t")
    assert normalized["require"] == require
    assert normalized["sentinel"] == sentinel


def test_misspelled_require_is_rejected_when_decoded():
    """The template has no else branch, so a typo would otherwise silently drop the filter"""
    with pytest.raises(msgspec.ValidationError, match=r"Invalid enum value 'none_of_the_above'"):
        highlight(require="none_of_the_above")


def test_highlight_without_mentions_is_rejected():
    with pytest.raises(errors.NlpTableConfigError, match="must declare `mentions`"):
        base_flatten_utils.normalize_highlights([], owner="t")


@pytest.mark.parametrize("span_path", ["", []])
def test_highlight_with_empty_span_path_is_rejected(span_path):
    with pytest.raises(errors.NlpTableConfigError, match="span_path"):
        base_flatten_utils.normalize_highlights([highlight(span_path=span_path)], owner="t")


def test_highlight_missing_required_field_is_rejected_when_decoded():
    with pytest.raises(msgspec.ValidationError, match="ls_variable_name"):
        msgspec.convert({"path": "a", "span_path": "b"}, base_flatten_utils.HighlightSpec)
