import os
import pathlib
from unittest import mock

from cumulus_library import cli, databases
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
    with open(
        f"{pathlib.Path(__file__).resolve().parents[0]}"
        "/test_data/discovery/discovery__code_sources.txt",
        "w",
    ) as f:
        for row in table_rows:
            f.write(f"{','.join(str(x) for x in row)}\n")

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
