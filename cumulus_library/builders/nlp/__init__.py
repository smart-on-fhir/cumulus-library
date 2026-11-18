"""
NLP building blocks: the response-schema primitives and constants that studies and builders share

Import these from this package (`from cumulus_library.builders.nlp import SpanAugmentedMention`),
rather than from the submodules they happen to be defined in.
"""

from cumulus_library.builders.nlp.base_flatten_utils import (
    FILTER_KINDS,
    FILTER_SENTINELS,
    HIGHLIGHT_COLUMNS,
    METADATA_COLUMNS,
    RESULT_ROOT,
    SOURCE_ALIAS,
    FilterKind,
    JoinKind,
)
from cumulus_library.builders.nlp.pydantic_primitives import (
    HAS_MENTION_FIELD,
    NONE_OF_THE_ABOVE,
    NOT_MENTIONED,
    SPANS_FIELD,
    DatePrecision,
    SpanAugmentedMention,
)

__all__ = [
    "FILTER_KINDS",
    "FILTER_SENTINELS",
    "HAS_MENTION_FIELD",
    "HIGHLIGHT_COLUMNS",
    "METADATA_COLUMNS",
    "NONE_OF_THE_ABOVE",
    "NOT_MENTIONED",
    "RESULT_ROOT",
    "SOURCE_ALIAS",
    "SPANS_FIELD",
    "DatePrecision",
    "FilterKind",
    "JoinKind",
    "SpanAugmentedMention",
]
