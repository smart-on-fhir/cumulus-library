"""Tests for example_nlp"""

import json

import duckdb
import pandas

from tests import testbed_utils


def test_empty_build(tmp_path):
    testbed = testbed_utils.LocalTestbed(tmp_path)
    con = testbed.build("example_nlp")
    df = con.sql("SELECT * FROM example_nlp__range_labels").df()
    assert df.empty  # should exist, but be empty


def test_merging_two_sources(tmp_path):
    # Set up two of the input tables
    testbed = testbed_utils.LocalTestbed(tmp_path)
    db_file = testbed.get_db_file("example_nlp")
    con = duckdb.connect(db_file)

    gpt4o_df = pandas.json_normalize(  # noqa: F841
        [
            {
                "note_ref": "noteA",
                "subject_ref": "patA",
                "result": {"spans": [(1, 5), (8, 12)], "age": 1},
            },
            {
                "note_ref": "noteB",
                "subject_ref": "patA",
                "result": {"spans": [(32, 55)], "age": -13},
            },
        ],
        max_level=0,
    )
    con.sql("CREATE TABLE example_nlp__nlp_gpt4o AS SELECT * FROM gpt4o_df")
    llama4_df = pandas.json_normalize(  # noqa: F841
        [
            {  # duplicate of above, but from different source
                "note_ref": "noteA",
                "subject_ref": "patA",
                "result": {"spans": [(1, 5)], "age": 1},
            },
            {
                "note_ref": "noteB",
                "subject_ref": "patA",
                "result": {"spans": [(34, 50)], "age": 54},
            },
        ],
        max_level=0,
    )
    con.sql("CREATE TABLE example_nlp__nlp_llama4_scout AS SELECT * FROM llama4_df")

    con = testbed.build("example_nlp")
    df = con.sql("SELECT * FROM example_nlp__range_labels ORDER BY note_ref, origin, span").df()
    rows = json.loads(df.to_json(orient="records"))
    assert rows == [
        {
            "note_ref": "noteA",
            "subject_ref": "patA",
            "label": "infant (0-1)",
            "span": "1:5",
            "origin": "example_nlp__nlp_gpt4o",
        },
        {
            "note_ref": "noteA",
            "subject_ref": "patA",
            "label": "infant (0-1)",
            "span": "8:12",
            "origin": "example_nlp__nlp_gpt4o",
        },
        {
            "note_ref": "noteA",
            "subject_ref": "patA",
            "label": "infant (0-1)",
            "span": "1:5",
            "origin": "example_nlp__nlp_llama4_scout",
        },
        {
            "note_ref": "noteB",
            "subject_ref": "patA",
            "label": "unknown",
            "span": "32:55",
            "origin": "example_nlp__nlp_gpt4o",
        },
        {
            "note_ref": "noteB",
            "subject_ref": "patA",
            "label": "middle aged (45-64)",
            "span": "34:50",
            "origin": "example_nlp__nlp_llama4_scout",
        },
    ]
