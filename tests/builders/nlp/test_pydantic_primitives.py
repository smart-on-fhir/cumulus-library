"""Tests for the shared NLP response-schema primitives"""

import enum

import pydantic

from cumulus_library.builders.nlp import pydantic_primitives


class ExampleMention(pydantic_primitives.SpanAugmentedMention):
    thing: str | None = None


def test_span_augmented_mention_schema_is_stable():
    """Studies' JSON schemas (and thus their task versions) are derived from this shape.

    If this fails, you are changing the schema of every study task that subclasses the primitive.
    Make sure that is intended and that those tasks bump their workflow `version`.
    """
    assert pydantic_primitives.SpanAugmentedMention.model_json_schema()["properties"] == {
        "has_mention": {
            "description": "Indicates whether the concept was mentioned in the text.",
            "title": "Has Mention",
            "type": "boolean",
        },
        "spans": {
            "description": "The verbatim text where this concept was mentioned.",
            "items": {"type": "string"},
            "title": "Spans",
            "type": "array",
        },
    }
    assert pydantic_primitives.SpanAugmentedMention.model_json_schema()["required"] == [
        "has_mention",
        "spans",
    ]


def test_subclass_inherits_span_fields():
    mention = ExampleMention(has_mention=True, spans=["some text"], thing="x")
    assert mention.spans == ["some text"]
    assert list(ExampleMention.model_fields)[:2] == ["has_mention", "spans"]


def test_date_precision_values():
    assert issubclass(pydantic_primitives.DatePrecision, enum.StrEnum)
    assert [p.value for p in pydantic_primitives.DatePrecision] == ["DAY", "MONTH", "YEAR"]


def test_sentinels_are_spelled_as_builders_expect():
    # The highlight builder's `require` filters match on these literal strings
    assert pydantic_primitives.NONE_OF_THE_ABOVE == "NONE_OF_THE_ABOVE"
    assert pydantic_primitives.NOT_MENTIONED == "NOT_MENTIONED"


def test_is_span_augmented():
    class Unrelated(pydantic.BaseModel):
        spans: list[str]

    assert pydantic_primitives.is_span_augmented(pydantic_primitives.SpanAugmentedMention)
    assert pydantic_primitives.is_span_augmented(ExampleMention)
    # Duck typing: a model with the right fields counts even without the base class
    assert pydantic_primitives.is_span_augmented(
        pydantic.create_model("Dynamic", has_mention=(bool, ...), spans=(list[str], ...))
    )
    assert not pydantic_primitives.is_span_augmented(Unrelated)
