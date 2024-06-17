import builtins
import datetime
import pathlib
import shutil
import zipfile
from contextlib import nullcontext as does_not_raise
from pathlib import Path
from unittest import mock

import pandas
import pytest
import requests
import responses
from freezegun import freeze_time

from cumulus_library import base_utils, enums, errors, study_parser
from cumulus_library.actions import (
    builder,
    cleaner,
    exporter,
    file_generator,
    importer,
    uploader,
)


@pytest.mark.parametrize(
    "schema,verbose,prefix,confirm,stats,target,raises",
    [
        ("main", True, None, None, False, "study_valid__table", does_not_raise()),
        ("main", False, None, None, False, "study_valid__table", does_not_raise()),
        ("main", None, None, None, False, "study_valid__table", does_not_raise()),
        (None, True, None, None, False, None, pytest.raises(SystemExit)),
        ("main", None, None, None, False, "study_valid__etl_table", does_not_raise()),
        ("main", None, None, None, False, "study_valid__nlp_table", does_not_raise()),
        ("main", None, None, None, False, "study_valid__lib_table", does_not_raise()),
        ("main", None, None, None, False, "study_valid__lib", does_not_raise()),
        ("main", None, "foo", "y", False, "foo_table", does_not_raise()),
        ("main", None, "foo", "n", False, "foo_table", pytest.raises(SystemExit)),
        ("main", True, None, "y", True, "study_valid__table", does_not_raise()),
        (
            "main",
            True,
            None,
            "n",
            True,
            "study_valid__table",
            pytest.raises(SystemExit),
        ),
    ],
)
def test_clean_study(mock_db, schema, verbose, prefix, confirm, stats, target, raises):
    with raises:
        protected_strs = [x.value for x in enums.ProtectedTableKeywords]
        with mock.patch.object(builtins, "input", lambda _: confirm):
            parser = study_parser.StudyManifestParser("./tests/test_data/study_valid/")
            builder.run_protected_table_builder(
                parser,
                mock_db.cursor(),
                schema,
                config=base_utils.StudyConfig(db=mock_db),
            )

            # We're mocking stats tables since creating them programmatically
            # is very slow and we're trying a lot of conditions
            mock_db.cursor().execute(
                f"CREATE TABLE {parser.get_study_prefix()}__"
                f"{enums.ProtectedTables.STATISTICS.value} "
                "AS SELECT 'study_valid' as study_name, "
                "'study_valid__123' AS table_name"
            )
            mock_db.cursor().execute("CREATE TABLE study_valid__123 (test int)")

            if target is not None:
                mock_db.cursor().execute(f"CREATE TABLE {target} (test int);")
            cleaner.clean_study(
                parser,
                mock_db.cursor(),
                schema,
                verbose=verbose,
                prefix=prefix,
                stats_clean=stats,
            )
            remaining_tables = (
                mock_db.cursor()
                .execute("select distinct(table_name) from information_schema.tables")
                .fetchall()
            )
            if any(x in target for x in protected_strs):
                assert (target,) in remaining_tables
            else:
                assert (target,) not in remaining_tables
            assert (
                f"{parser.get_study_prefix()}__{enums.ProtectedTables.TRANSACTIONS.value}",
            ) in remaining_tables
            if stats:
                assert (
                    f"{parser.get_study_prefix()}__{enums.ProtectedTables.STATISTICS.value}",
                ) not in remaining_tables
                assert ("study_valid__123",) not in remaining_tables
            else:
                assert (
                    f"{parser.get_study_prefix()}__{enums.ProtectedTables.STATISTICS.value}",
                ) in remaining_tables
                assert ("study_valid__123",) in remaining_tables


@pytest.mark.parametrize(
    "study_path,stats",
    [
        ("./tests/test_data/study_valid/", False),
        ("./tests/test_data/psm/", True),
    ],
)
def test_run_protected_table_builder(mock_db, study_path, stats):
    parser = study_parser.StudyManifestParser(study_path)
    builder.run_protected_table_builder(
        parser, mock_db.cursor(), "main", config=base_utils.StudyConfig(db=mock_db)
    )
    tables = (
        mock_db.cursor()
        .execute("SELECT distinct(table_name) FROM information_schema.tables ")
        .fetchall()
    )
    assert (
        f"{parser.get_study_prefix()}__{enums.ProtectedTables.TRANSACTIONS.value}",
    ) in tables
    if stats:
        assert (
            f"{parser.get_study_prefix()}__{enums.ProtectedTables.STATISTICS.value}",
        ) in tables
    else:
        assert (
            f"{parser.get_study_prefix()}__{enums.ProtectedTables.STATISTICS.value}",
        ) not in tables


@pytest.mark.parametrize(
    "study_path,verbose,expects,raises",
    [
        (
            "./tests/test_data/study_python_valid/",
            True,
            ("study_python_valid__table",),
            does_not_raise(),
        ),
        (
            "./tests/test_data/study_python_valid/",
            False,
            ("study_python_valid__table",),
            does_not_raise(),
        ),
        (
            "./tests/test_data/study_python_valid/",
            None,
            ("study_python_valid__table",),
            does_not_raise(),
        ),
        (
            "./tests/test_data/study_python_no_subclass/",
            True,
            (),
            pytest.raises(errors.StudyManifestParsingError),
        ),
        (
            "./tests/test_data/study_python_local_template/",
            False,
            ("study_python_valid__table_duckdb_foo",),
            does_not_raise(),
        ),
    ],
)
def test_table_builder(mock_db, study_path, verbose, expects, raises):
    with raises:
        parser = study_parser.StudyManifestParser(pathlib.Path(study_path))
        builder.run_table_builder(
            parser,
            mock_db.cursor(),
            "main",
            verbose=verbose,
            config=base_utils.StudyConfig(db=mock_db),
        )
        tables = (
            mock_db.cursor()
            .execute("SELECT distinct(table_name) FROM information_schema.tables ")
            .fetchall()
        )
        assert expects in tables


@pytest.mark.parametrize(
    "study_path,verbose,expects,raises",
    [
        (
            "./tests/test_data/study_valid/",
            True,
            ("study_valid__table",),
            does_not_raise(),
        ),
        (
            "./tests/test_data/study_valid/",
            False,
            ("study_valid__table",),
            does_not_raise(),
        ),
        (
            "./tests/test_data/study_valid/",
            None,
            ("study_valid__table",),
            does_not_raise(),
        ),
        (
            "./tests/test_data/study_wrong_prefix/",
            None,
            [],
            pytest.raises(errors.StudyManifestQueryError),
        ),
        (
            "./tests/test_data/study_invalid_no_dunder/",
            True,
            (),
            pytest.raises(errors.StudyManifestQueryError),
        ),
        (
            "./tests/test_data/study_invalid_two_dunder/",
            True,
            (),
            pytest.raises(errors.StudyManifestQueryError),
        ),
        (
            "./tests/test_data/study_invalid_reserved_word/",
            True,
            (),
            pytest.raises(errors.StudyManifestQueryError),
        ),
    ],
)
def test_build_study(mock_db, study_path, verbose, expects, raises):
    with raises:
        parser = study_parser.StudyManifestParser(study_path)
        builder.build_study(
            parser,
            mock_db.cursor(),
            verbose=verbose,
            config=base_utils.StudyConfig(db=mock_db),
        )
        tables = (
            mock_db.cursor()
            .execute("SELECT distinct(table_name) FROM information_schema.tables ")
            .fetchall()
        )
        assert expects in tables


@pytest.mark.parametrize(
    "study_path,stats,expects,raises",
    [
        (
            "./tests/test_data/psm/",
            False,
            ("psm_test__psm_encounter_covariate",),
            does_not_raise(),
        ),
        (
            "./tests/test_data/psm/",
            True,
            ("psm_test__psm_encounter_covariate",),
            does_not_raise(),
        ),
    ],
)
def test_run_statistics_builders(
    tmp_path, mock_db_stats, study_path, stats, expects, raises
):
    with raises:
        parser = study_parser.StudyManifestParser(study_path, data_path=tmp_path)
        config = base_utils.StudyConfig(db=mock_db_stats, stats_build=stats)
        builder.run_protected_table_builder(
            parser, mock_db_stats.cursor(), "main", config=config
        )
        builder.build_study(parser, mock_db_stats.cursor(), config=config)
        builder.run_statistics_builders(
            parser, mock_db_stats.cursor(), "main", config=config
        )
        tables = (
            mock_db_stats.cursor()
            .execute("SELECT distinct(table_name) FROM information_schema.tables")
            .fetchall()
        )
        print(tables)
        if stats:
            assert expects in tables
        else:
            assert expects not in tables


def test_export_study(tmp_path, mock_db_core):
    parser = study_parser.StudyManifestParser(
        f"{Path(__file__).parent.parent}/cumulus_library/studies/core",
        data_path=f"{tmp_path}/export",
    )
    exporter.export_study(
        parser, mock_db_core, None, f"{tmp_path}/export", False, chunksize=20
    )
    for file in Path(f"{tmp_path}/export").glob("*.*"):
        assert file in parser.get_export_table_list()


@freeze_time("2024-01-01")
def test_import_study(tmp_path, mock_db):
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
    args = {"schema_name": "main", "verbose": False}
    config = base_utils.StudyConfig(db=mock_db)
    importer.import_archive(
        archive_path=tmp_path / "archive/test.zip", args=args, config=config
    )
    assert len(list((tmp_path / "archive").glob("*"))) == 1
    test_table = mock_db.cursor().execute("SELECT * FROM test__table").fetchall()
    assert test_table == [
        ("a", 1, 1.1, True, datetime.datetime(2024, 1, 1, 0, 0)),
        ("b", 2, 2.2, False, datetime.datetime(2024, 1, 1, 0, 0)),
        (None, None, None, None, None),
    ]
    with pytest.raises(errors.StudyImportError):
        importer.import_archive(
            archive_path=tmp_path / "archive/missing.zip", args=args, config=config
        )
    with pytest.raises(errors.StudyImportError):
        importer.import_archive(
            archive_path=tmp_path / "duck.db", args=args, config=config
        )
    with pytest.raises(errors.StudyImportError):
        df.to_parquet(tmp_path / "archive/other_test__table.parquet")
        with zipfile.ZipFile(tmp_path / "archive/test.zip", "w") as archive:
            archive.write(tmp_path / "archive/other_test__table.parquet")
        importer.import_archive(
            archive_path=tmp_path / "duck.db", args=args, config=config
        )
    with pytest.raises(errors.StudyImportError):
        df.to_parquet(tmp_path / "archive/table.parquet")
        with zipfile.ZipFile(tmp_path / "archive/no_dunder.zip", "w") as archive:
            archive.write(tmp_path / "archive/table.parquet")
        importer.import_archive(
            archive_path=tmp_path / "archive/no_dunder.zip", args=args, config=config
        )
    with pytest.raises(errors.StudyImportError):
        (tmp_path / "archive/empty.zip")
        importer.import_archive(
            archive_path=tmp_path / "archive/empty.zip", args=args, config=config
        )


@pytest.mark.parametrize(
    "user,id_token,status,login_error,raises",
    [
        (None, None, 204, False, pytest.raises(SystemExit)),
        ("user", "id", 204, False, does_not_raise()),
        (
            "user",
            "id",
            500,
            False,
            pytest.raises(requests.exceptions.HTTPError),
        ),
        (
            "baduser",
            "badid",
            204,
            True,
            pytest.raises(requests.exceptions.HTTPError),
        ),
        (
            "user",
            "id",
            204,
            False,
            does_not_raise(),
        ),
    ],
)
@responses.activate
def test_upload_data(user, id_token, status, login_error, raises):
    with raises:
        if login_error:
            responses.add(responses.POST, "https://upload.url.test/upload/", status=401)
        else:
            responses.add(
                responses.POST,
                "https://upload.url.test/upload/",
                json={"url": "https://presigned.url.test", "fields": {"a": "b"}},
            )
        args = {
            "data_path": pathlib.Path.cwd() / "tests/test_data",
            "id": id_token,
            "preview": False,
            "target": "core",
            "url": "https://upload.url.test/upload/",
            "user": user,
        }
        responses.add(responses.POST, "https://presigned.url.test", status=status)
        uploader.upload_files(args)


@mock.patch("sysconfig.get_path")
def test_generate_sql(mock_path, mock_db, tmp_path):
    mock_path.return_value = f"{tmp_path}/study_python_valid/"
    with does_not_raise():
        shutil.copytree(
            f"{Path(__file__).resolve().parents[0]}/test_data/study_python_valid",
            f"{tmp_path}/study_python_valid/",
        )
        parser = study_parser.StudyManifestParser(
            study_path=pathlib.Path(f"{tmp_path}/study_python_valid/")
        )
        file_generator.run_generate_sql(
            manifest_parser=parser,
            cursor=mock_db.cursor(),
            schema="main",
            config=base_utils.StudyConfig(db=mock_db),
        )
        files = list(
            pathlib.Path(f"{tmp_path}/study_python_valid/reference_sql/").glob("*")
        )
        files = [str(x) for x in files]
        assert len(files) == 2
        assert "module1.sql" in ",".join(files)
        for file in files:
            if file.endswith("module1.sql"):
                with open(file) as f:
                    query = "\n".join(line.rstrip() for line in f)
        assert "This sql was autogenerated" in query
        assert "CREATE TABLE IF NOT EXISTS study_python_valid__table" in query


@mock.patch("sysconfig.get_path")
def test_generate_md(mock_path, mock_db, tmp_path):
    mock_path.return_value = f"{tmp_path}/study_python_valid/"
    with does_not_raise():
        shutil.copytree(
            f"{Path(__file__).resolve().parents[0]}/test_data/study_python_valid",
            f"{tmp_path}/study_python_valid/",
        )
        parser = study_parser.StudyManifestParser(
            study_path=pathlib.Path(f"{tmp_path}/study_python_valid/")
        )
        builder.run_table_builder(
            parser,
            mock_db.cursor(),
            "main",
            config=base_utils.StudyConfig(db=mock_db),
        )
        file_generator.run_generate_markdown(
            manifest_parser=parser,
            cursor=mock_db.cursor(),
            schema="main",
        )
        with open(
            f"{tmp_path}/study_python_valid/study_python_valid_generated.md"
        ) as f:
            generated_md = f.read()
        expected_table = """### study_python_valid__table

|Column| Type  |Description|
|------|-------|-----------|
|test  |INTEGER|           |"""
        assert expected_table in generated_md
