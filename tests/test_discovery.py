import os
import pathlib
from unittest import mock

from cumulus_library import cli, databases
from cumulus_library.studies.discovery.discovery_templates import discovery_templates
from tests import conftest


@mock.patch.dict(
    os.environ,
    clear=True,
)
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
    cursor = db.cursor()
    table_rows = conftest.get_sorted_table_data(cursor, "discovery__code_sources")

    # For regenerating test data
    # with open(
    #     f"{pathlib.Path(__file__).resolve().parents[0]}"
    #     "/test_data/discovery/discovery__code_sources.txt",
    #     "w",
    # ) as f:
    #     for row in table_rows:
    #         f.write(f"{','.join(str(x) for x in row)}\n")

    with open(
        f"{pathlib.Path(__file__).resolve().parents[0]}"
        "/test_data/discovery/discovery__code_sources.txt",
    ) as ref:
        for row in ref:
            ref_row = row.rstrip().split(",")
            for pos in range(0, len(ref_row)):
                if ref_row[pos] == "None":
                    ref_row[pos] = None
            assert tuple(ref_row) in table_rows


def test_get_code_system_pairs():
    expected = """CREATE TABLE output_table AS
SELECT DISTINCT
    'arrays' AS table_name,
    'acol' AS column_name,
    table_2.col_2.code,
    table_2.col_2.display,
    table_2.col_2.system
FROM arrays,
UNNEST(acol) AS table_1 (col_1),
UNNEST(col_1.coding) as table_2 (col_2)

UNION

SELECT DISTINCT
    'dictarray' AS table_name,
    'col' AS column_name,
    table_1.col_1.code,
    table_1.col_1.display,
    table_1.col_1.system
FROM dictarray,
UNNEST(col.coding) AS table_1 (col_1)

UNION

SELECT DISTINCT
    'bare' AS table_name,
    'bcol' AS column_name,
    bcol.coding.code,
    bcol.coding.display,
    bcol.coding.system
FROM bare

UNION

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
    AS t (table_name, column_name, code, display, system)


"""
    query = discovery_templates.get_code_system_pairs(
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
                "table_name": "empty",
                "column_hierarchy": [("empty", dict), ("coding", dict)],
                "has_data": False,
            },
        ],
    )
    assert query == expected
