"""Tests for example_nlp"""

from unittest import mock

import duckdb
import pytest

from cumulus_library import cli
from tests import nlp_utils
from tests.conftest import duckdb_args


@pytest.mark.xdist_group(name="nlp_builder")
@mock.patch("openai.OpenAI")
def test_full_build(mock_client, tmp_path):
    with open(f"{tmp_path}/dxr.ndjson", "w", encoding="utf8") as f:
        nlp_utils.add_dxr("1", "Three year old white female", f)

    model = nlp_utils.MockModel(mock_client, make_codebook=False)
    model.mock_openai_response(
        [
            {"has_mention": True, "spans": ["three"], "age": 3},
            {"has_mention": True, "spans": ["white"], "race": "white"},
        ]
    )

    # Build core first
    build_args = duckdb_args(
        [
            "build",
            "-t",
            "core",
            str(tmp_path),
        ],
        tmp_path,
        ndjson_dir=str(tmp_path),
    )
    cli.main(cli_args=build_args)

    # Then build example_nlp
    build_args = duckdb_args(
        [
            "build",
            "-t",
            "example_nlp",
            str(tmp_path),
            "--note-dir",
            str(tmp_path),
            *model.cli_args(),
        ],
        tmp_path,
    )
    cli.main(cli_args=build_args)

    db = duckdb.connect(f"{tmp_path}/duck.db")
    age_rows = db.cursor().execute("select result from example_nlp__age").fetchall()
    race_rows = db.cursor().execute("select result from example_nlp__race").fetchall()
    label_rows = db.cursor().execute("select label from example_nlp__range_labels").fetchall()

    assert age_rows[0][0] == {"age": 3, "has_mention": True, "spans": [[0, 5]]}
    assert race_rows[0][0] == {"race": "white", "has_mention": True, "spans": [[15, 20]]}
    assert label_rows[0][0] == "child (2-12)"
