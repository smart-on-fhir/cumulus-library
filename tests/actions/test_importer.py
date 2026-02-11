import datetime
import zipfile

import pandas
import pytest
import time_machine

from cumulus_library import errors
from cumulus_library.actions import (
    importer,
)


@time_machine.travel("2024-01-01T00:00:00Z", tick=False)
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
        ("a", 1, 1.1, True, datetime.datetime(2023, 12, 31, 19, 0)),
        ("b", 2, 2.2, False, datetime.datetime(2023, 12, 31, 19, 0)),
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
