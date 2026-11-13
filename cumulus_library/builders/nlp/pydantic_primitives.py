"""Shared building blocks for NLP response schemas

Studies define their own response models, but nearly every one needs the same few pieces: a
mention that carries the text spans backing it, a way to express how precisely a date was
stated, and the sentinel values that mean "the note doesn't say". They live here so that every
study speaks the same dialect and the table builders can rely on it.
"""

import enum

import pydantic

# Sentinel enum values. Enums for LLM choices conventionally end with NONE_OF_THE_ABOVE, as the
# "nothing here applies" escape hatch (so the model is never forced into a wrong answer).
# NOT_MENTIONED is the equivalent for a tri-state enum where absence needs its own value.
NONE_OF_THE_ABOVE = "NONE_OF_THE_ABOVE"
NOT_MENTIONED = "NOT_MENTIONED"

# The two field names that make a model "span augmented". Kept in one place because
# `is_span_augmented` and the driver's span fixing must agree on them.
HAS_MENTION_FIELD = "has_mention"
SPANS_FIELD = "spans"


class SpanAugmentedMention(pydantic.BaseModel):
    """
    A mention of a particular concept in the text, augmented with the character spans
    where the mention was found.
    This allows for validation of LLM-generated findings, as well as the ability to link
    mentions back to the original text for review and auditing purposes.
    """

    has_mention: bool = pydantic.Field(
        ..., description="Indicates whether the concept was mentioned in the text."
    )
    spans: list[str] = pydantic.Field(
        ..., description="The verbatim text where this concept was mentioned."
    )


class DatePrecision(enum.StrEnum):
    """How precisely the source text pins down a date.

    Pair this with an ISO date field (always emitted as a full YYYY-MM-DD, rounding down to the
    first possible date), so that consumers can tell "2021-01-01" the day from "2021" the year.
    Use it as the type of a ``<your_date>_precision`` field, with a description like:

        "Precision actually supported by the source text for <your_date>. "
        "DAY: day, month, and year were explicitly stated; "
        "MONTH: month and year were explicitly stated; "
        "YEAR: only year was explicitly stated; "
        "Use Null when <your_date> is null."
    """

    DAY = "DAY"
    MONTH = "MONTH"
    YEAR = "YEAR"


def is_span_augmented(model_cls: type[pydantic.BaseModel]) -> bool:
    """Whether a Pydantic model looks like a SpanAugmentedMention (has both span-bearing fields).

    This is duck-typed on purpose: a study that built its model from a JSON schema has
    no inheritance link to check, but is still a span-augmented mention.
    """
    fields = model_cls.model_fields
    return HAS_MENTION_FIELD in fields and SPANS_FIELD in fields
