from cumulus_library import cli, databases
from tests import conftest


def test_static_file(tmp_path):
    cli.main(
        cli_args=conftest.duckdb_args(
            [
                "build",
                "-t",
                "study_static_file",
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
    table_rows, cols = conftest.get_sorted_table_data(cursor, "study_static_file__table")
    expected_cols = {"CUI", "TTY", "CODE", "SAB", "STR"}
    found_cols = {col_schema[0] for col_schema in cols}
    assert expected_cols == found_cols
    assert len(table_rows) == 6
    assert table_rows[0] == ("C0000727", "ICD10CM", "PT", "R10.0", "Acute abdomen")
    assert table_rows[-1] == ("C0010332", "ICD10PCS", "PT", "GZ2ZZZZ", "Crisis Intervention")
