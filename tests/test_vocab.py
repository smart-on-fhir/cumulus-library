import os
import pathlib
from unittest import mock

from cumulus_library import cli, databases
from tests import conftest


@mock.patch.dict(
    os.environ,
    clear=True,
)
def test_vocab(tmp_path):
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
                "vocab",
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
    table_rows, cols = conftest.get_sorted_table_data(cursor, "vocab__icd")
    expected_cols = ["CUI", "TTY", "CODE", "SAB", "STR"]
    for col in expected_cols:
        assert any(col_schema[0] == col for col_schema in cols)
    for col in cols:
        assert col[0] in expected_cols
    icd_dir = pathlib.Path("./cumulus_library/studies/vocab/icd/")
    bsv_row_count = 0
    for file in icd_dir.glob("*.bsv"):
        with open(file) as f:
            bsv_row_count += len(f.readlines())
    assert len(table_rows) == conftest.VOCAB_ICD_ROW_COUNT
