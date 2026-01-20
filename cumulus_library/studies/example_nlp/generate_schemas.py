#!/usr/bin/env python3

"""Define schemas for the example_nlp study"""

import json
import os

import pydantic


class AgeMention(pydantic.BaseModel):
    has_mention: bool | None = pydantic.Field(None)
    spans: list[str] = pydantic.Field(default_factory=list, description="Supporting text spans")
    age: int | None = pydantic.Field(None, description="The age of the patient")


if __name__ == "__main__":
    basedir = os.path.dirname(__file__)

    with open(f"{basedir}/age.json", "w", encoding="utf8") as f:
        json.dump(AgeMention.model_json_schema(), f, indent=2)
