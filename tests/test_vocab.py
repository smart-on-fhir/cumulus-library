import os
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
    expected_cols = {"CUI", "TTY", "CODE", "SAB", "STR"}
    found_cols = {col_schema[0] for col_schema in cols}
    assert expected_cols == found_cols
    assert len(table_rows) == conftest.VOCAB_ICD_ROW_COUNT
    assert table_rows[0] == ("C0000727", "ICD10CM", "PT", "R10.0", "Acute abdomen")
    assert table_rows[-1] == ("C5700317", "ICD10CM", "HT", "M91.3", "Pseudocoxalgia")
