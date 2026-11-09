import datetime
import zipfile

import pandas
import pytest
import time_machine

from cumulus_library import errors
from cumulus_library.actions import (
    importer,
)


@time_machine.travel("2024-01-01T00:00:00", tick=False)
def test_import_study(tmp_path, mock_db_config):
    test_data = {
        "string": ["a", "b", None],
        "int": [1, 2, pandas.NA],
        "float": [1.1, 2.2, pandas.NA],
        "bool": [True, False, pandas.NA],
        "datetime": [datetime.datetime.now(), datetime.datetime.now(), None],
    }
    df = pandas.DataFrame(test_data)
    (tmp_path / "archive").mkdir()
    df.to_parquet(tmp_path / "archive/test__table.parquet")
    df.to_csv(tmp_path / "archive/test__table.csv")
    with zipfile.ZipFile(tmp_path / "archive/test.zip", "w") as archive:
        archive.write(tmp_path / "archive/test__table.parquet")
        archive.write(tmp_path / "archive/test__table.csv")
    (tmp_path / "archive/test__table.parquet").unlink()
    (tmp_path / "archive/test__table.csv").unlink()
    mock_db_config.schema = "main"
    importer.import_archive(config=mock_db_config, archive_path=tmp_path / "archive/test.zip")
    assert len(list((tmp_path / "archive").glob("*"))) == 1
    test_table = mock_db_config.db.cursor().execute("SELECT * FROM test__table").fetchall()
    assert test_table == [
        ("a", 1, 1.1, True, datetime.datetime(2024, 1, 1, 0, 0)),
        ("b", 2, 2.2, False, datetime.datetime(2024, 1, 1, 0, 0)),
        (None, None, None, None, None),
    ]
    with pytest.raises(errors.StudyImportError):
        importer.import_archive(
            config=mock_db_config, archive_path=tmp_path / "archive/missing.zip"
        )
    with pytest.raises(errors.StudyImportError):
        with open(tmp_path / "archive/empty.zip", "w"):
            pass
        importer.import_archive(config=mock_db_config, archive_path=tmp_path / "archive/empty.zip")
    with pytest.raises(errors.StudyImportError):
        importer.import_archive(config=mock_db_config, archive_path=tmp_path / "duck.db")
    with pytest.raises(errors.StudyImportError):
        df.to_parquet(tmp_path / "archive/test__table.parquet")
        df.to_parquet(tmp_path / "archive/other_test__table.parquet")
        with zipfile.ZipFile(tmp_path / "archive/two_studies.zip", "w") as archive:
            archive.write(tmp_path / "archive/test__table.parquet")
            archive.write(tmp_path / "archive/other_test__table.parquet")
        importer.import_archive(
            config=mock_db_config, archive_path=tmp_path / "archive/two_studies.zip"
        )
    with pytest.raises(errors.StudyImportError):
        df.to_parquet(tmp_path / "archive/table.parquet")
        with zipfile.ZipFile(tmp_path / "archive/no_dunder.zip", "w") as archive:
            archive.write(tmp_path / "archive/table.parquet")
        importer.import_archive(
            config=mock_db_config, archive_path=tmp_path / "archive/no_dunder.zip"
        )


@pytest.mark.parametrize(
    "column",
    [
        "plain_name",
        'quote"in"name',
        'x" FROM (SELECT 1 AS x); CREATE TABLE extra_table(ok INTEGER); SELECT 1 AS "y',
    ],
)
def test_import_escapes_column_names(tmp_path, mock_db_config, column):
    def list_tables():
        return {row[0] for row in cursor.execute("SHOW TABLES").fetchall()}

    cursor = mock_db_config.db.cursor()
    tables_before = list_tables()
    df = pandas.DataFrame({column: [1, 2]})
    (tmp_path / "archive").mkdir()
    df.to_parquet(tmp_path / "archive/test__table.parquet")
    with zipfile.ZipFile(tmp_path / "archive/test.zip", "w") as archive:
        archive.write(tmp_path / "archive/test__table.parquet", arcname="test__table.parquet")
    importer.import_archive(config=mock_db_config, archive_path=tmp_path / "archive/test.zip")
    assert list_tables() - tables_before == {"test__table"}
    cursor.execute("SELECT * FROM test__table")
    assert [col[0] for col in cursor.description] == [column]
    assert cursor.fetchall() == [(1,), (2,)]


@pytest.mark.parametrize(
    "member,error",
    [
        ('test__ta"ble.parquet', "invalid table name"),
        ("te'st__table.parquet", "invalid study name"),
        ("te|st__table.parquet", "invalid study name"),
    ],
)
def test_import_rejects_invalid_names(tmp_path, mock_db_config, member, error):
    pandas.DataFrame({"a": [1]}).to_parquet(tmp_path / "data.parquet")
    with zipfile.ZipFile(tmp_path / "test.zip", "w") as archive:
        archive.write(tmp_path / "data.parquet", arcname=member)
    with pytest.raises(errors.StudyImportError, match=error):
        importer.import_archive(config=mock_db_config, archive_path=tmp_path / "test.zip")


@pytest.mark.parametrize(
    "member", ["test__table.parquet", "dir/test__table.parquet", "a'b/test__table.parquet"]
)
def test_import_ignores_archive_directories(tmp_path, mock_db_config, member):
    cursor = mock_db_config.db.cursor()
    tables_before = {row[0] for row in cursor.execute("SHOW TABLES").fetchall()}
    pandas.DataFrame({"a": [1]}).to_parquet(tmp_path / "data.parquet")
    with zipfile.ZipFile(tmp_path / "test.zip", "w") as archive:
        archive.write(tmp_path / "data.parquet", arcname=member)
    importer.import_archive(config=mock_db_config, archive_path=tmp_path / "test.zip")
    tables_after = {row[0] for row in cursor.execute("SHOW TABLES").fetchall()}
    assert tables_after - tables_before == {"test__table"}
