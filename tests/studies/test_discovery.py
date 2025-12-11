import csv
import os
import pathlib
from unittest import mock

import pytest

from cumulus_library import cli, databases
from cumulus_library.studies.discovery.discovery_templates import discovery_templates
from tests import conftest


def test_discovery(tmp_path):
    cli.main(
        cli_args=conftest.duckdb_args(
            [
                "build",
                "-t",
                "core",
                "-s",
                "./tests/test_data",
                "--database",
                "test",
            ],
            tmp_path,
        )
    )
    cli.main(
        cli_args=conftest.duckdb_args(
            [
                "build",
                "-t",
                "discovery",
                "-s",
                "./tests/test_data",
                "--database",
                f"{tmp_path}/duck.db",
            ],
            tmp_path,
        )
    )
    db = databases.DuckDatabaseBackend(f"{tmp_path}/duck.db")
    db.connect()
    cursor = db.cursor()
    table_rows, cols = conftest.get_sorted_table_data(cursor, "discovery__code_sources")
    table_rows = [tuple(x or "" for x in row) for row in table_rows]

    # For regenerating test data
    # with open(
    #     f"{pathlib.Path(__file__).resolve().parents[1]}"
    #     "/test_data/discovery/discovery__code_sources.csv",
    #     "w",
    # ) as f:
    #     writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
    #     for row in table_rows:
    #         writer.writerow(row)

    try:
        with open(
            f"{pathlib.Path(__file__).resolve().parents[1]}"
            "/test_data/discovery/discovery__code_sources.csv",
        ) as ref:
            reader = csv.reader(ref)
            for row in reader:
                assert tuple(row) in table_rows
    except Exception as e:
        conftest.debug_diff_tables(cols, table_rows, ref, pos=0)
        raise e


def test_get_system_pairs():
    expected = """
CREATE TABLE discovery__tmp_arrays_acol AS

    SELECT DISTINCT
        'arrays' AS table_name,
        'acol' AS column_name,
        table_2.col_2.code,
        table_2.col_2.display,
        table_2.col_2.system
    FROM arrays,
    UNNEST(acol) AS table_1 (col_1),
    UNNEST(col_1.coding) as table_2 (col_2);
CREATE TABLE discovery__tmp_dictarray_col AS

    SELECT DISTINCT
        'dictarray' AS table_name,
        'col' AS column_name,
        table_1.col_1.code,
        table_1.col_1.display,
        table_1.col_1.system
    FROM dictarray,
    UNNEST(col.coding) AS table_1 (col_1);
CREATE TABLE discovery__tmp_bare_bcol AS

    SELECT DISTINCT
        'bare' AS table_name,
        'bcol' AS column_name,
        bcol.coding.code,
        bcol.coding.display,
        bcol.coding.system
    FROM bare;
CREATE TABLE discovery__tmp_bare_nested_coding_dcol_code AS

    SELECT DISTINCT
        'bare_nested_coding' AS table_name,
        'dcol.code' AS column_name,
        table_2.col_2.code,
        table_2.col_2.display,
        table_2.col_2.system
    FROM bare_nested_coding,
    UNNEST(dcol) AS table_1 (col_1),
    UNNEST(col_1.code.coding) as table_2 (col_2);
CREATE TABLE discovery__tmp_empty_empty AS
SELECT *
FROM (
    VALUES (
        'empty',
        'empty',
        '',
        '',
        ''
    )
)
    AS t (table_name, column_name, code, display, system);
CREATE TABLE output_table AS
--noqa: disable=LTO2,LT09,CV06
SELECT
    table_name,
    column_name,
    code,
    display,
    system
FROM discovery__tmp_arrays_acol
UNION ALL
SELECT
    table_name,
    column_name,
    code,
    display,
    system
FROM discovery__tmp_dictarray_col
UNION ALL
SELECT
    table_name,
    column_name,
    code,
    display,
    system
FROM discovery__tmp_bare_bcol
UNION ALL
SELECT
    table_name,
    column_name,
    code,
    display,
    system
FROM discovery__tmp_bare_nested_coding_dcol_code
UNION ALL
SELECT
    table_name,
    column_name,
    code,
    display,
    system
FROM discovery__tmp_empty_empty
;"""
    query = discovery_templates.get_system_pairs(
        "output_table",
        [
            {
                "table_name": "arrays",
                "column_hierarchy": [("acol", list), ("coding", list)],
                "has_data": True,
            },
            {
                "table_name": "dictarray",
                "column_hierarchy": [("col", dict), ("coding", list)],
                "has_data": True,
            },
            {
                "table_name": "bare",
                "column_hierarchy": [("bcol", dict), ("coding", dict)],
                "has_data": True,
            },
            {
                "table_name": "bare_nested_coding",
                "column_hierarchy": [("dcol", list), ("code", dict), ("coding", dict)],
                "has_data": True,
            },
            {
                "table_name": "empty",
                "column_hierarchy": [("empty", dict), ("coding", dict)],
                "has_data": False,
            },
        ],
    )
    assert query == expected


@mock.patch.dict(os.environ, clear=True)
@mock.patch("cumulus_library.studies.discovery.code_definitions.code_list", new=[{}])
def test_bad_code_definition(tmp_path):
    with pytest.raises(KeyError, match="Expected table_name and column_hierarchy keys"):
        cli.main(
            cli_args=conftest.duckdb_args(
                [
                    "build",
                    "--target=discovery",
                ],
                tmp_path,
            )
        )
