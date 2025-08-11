import builtins
import contextlib
import datetime
import io
import json
import os
import pathlib
import shutil
import zipfile
from contextlib import nullcontext as does_not_raise
from unittest import mock

import pandas
import pytest
import requests
import responses
from freezegun import freeze_time
from responses import matchers

from cumulus_library import base_utils, enums, errors, log_utils, study_manifest
from cumulus_library.actions import (
    builder,
    cleaner,
    exporter,
    file_generator,
    importer,
    uploader,
)
from cumulus_library.template_sql import base_templates, sql_utils


@pytest.mark.parametrize(
    "verbose,prefix,confirm,stats,target,raises",
    [
        (True, None, None, False, "study_valid__table", does_not_raise()),
        (False, None, None, False, "study_valid__table", does_not_raise()),
        (None, None, None, False, "study_valid__table", does_not_raise()),
        (None, None, None, False, "study_valid__etl_table", does_not_raise()),
        (None, None, None, False, "study_valid__nlp_table", does_not_raise()),
        (None, None, None, False, "study_valid__lib_table", does_not_raise()),
        (None, None, None, False, "study_valid__lib", does_not_raise()),
        (None, "foo", "y", False, "foo_table", does_not_raise()),
        (None, "foo", "n", False, "foo_table", pytest.raises(SystemExit)),
        (True, None, "y", True, "study_valid__table", does_not_raise()),
        (
            True,
            None,
            "n",
            True,
            "study_valid__table",
            pytest.raises(SystemExit),
        ),
    ],
)
def test_clean_study(mock_db_config, verbose, prefix, confirm, stats, target, raises):
    with raises:
        mock_db_config.stats_clean = stats
        protected_strs = [x.value for x in enums.ProtectedTableKeywords]
        with mock.patch.object(builtins, "input", lambda _: confirm):
            manifest = study_manifest.StudyManifest("./tests/test_data/study_valid/")
            builder.run_protected_table_builder(
                config=mock_db_config,
                manifest=manifest,
            )

            # We're mocking stats tables since creating them programmatically
            # is very slow and we're trying a lot of conditions
            mock_db_config.db.cursor().execute(
                f"CREATE TABLE {manifest.get_study_prefix()}__"
                f"{enums.ProtectedTables.STATISTICS.value} "
                "AS SELECT 'study_valid' as study_name, "
                "'study_valid__123' AS table_name"
            )
            mock_db_config.db.cursor().execute("CREATE TABLE study_valid__123 (test int)")
            mock_db_config.db.cursor().execute(
                "CREATE VIEW study_valid__456 AS SELECT * FROM study_valid__123"
            )

            if target is not None:
                mock_db_config.db.cursor().execute(f"CREATE TABLE {target} (test int);")
            cleaner.clean_study(config=mock_db_config, manifest=manifest, prefix=prefix)
            remaining_tables = (
                mock_db_config.db.cursor()
                .execute("select distinct(table_name) from information_schema.tables")
                .fetchall()
            )

            if any(x in target for x in protected_strs):
                assert (target,) in remaining_tables
            else:
                assert (target,) not in remaining_tables
            assert (
                f"{manifest.get_study_prefix()}__{enums.ProtectedTables.TRANSACTIONS.value}",
            ) in remaining_tables
            if stats:
                assert (
                    f"{manifest.get_study_prefix()}__{enums.ProtectedTables.STATISTICS.value}",
                ) not in remaining_tables
                assert ("study_valid__123",) not in remaining_tables
            else:
                assert (
                    f"{manifest.get_study_prefix()}__{enums.ProtectedTables.STATISTICS.value}",
                ) in remaining_tables
                assert ("study_valid__123",) in remaining_tables
            if not prefix:
                assert ("study_valid__456",) not in remaining_tables


def test_clean_dedicated_schema(mock_db_config):
    with mock.patch.object(builtins, "input", lambda _: False):
        mock_db_config.schema = "dedicated"
        manifest = study_manifest.StudyManifest("./tests/test_data/study_dedicated_schema/")
        mock_db_config.db.cursor().execute("CREATE SCHEMA dedicated")
        builder.run_protected_table_builder(
            config=mock_db_config,
            manifest=manifest,
        )
        mock_db_config.db.cursor().execute("CREATE TABLE dedicated.table_1 (test int)")
        mock_db_config.db.cursor().execute(
            "CREATE VIEW dedicated.view_2 AS SELECT * FROM dedicated.table_1"
        )
        cleaner.clean_study(config=mock_db_config, manifest=manifest)
        remaining_tables = (
            mock_db_config.db.cursor()
            .execute("select distinct(table_name) from information_schema.tables")
            .fetchall()
        )
        assert (f"{enums.ProtectedTables.TRANSACTIONS.value}",) in remaining_tables
        assert ("table_1",) not in remaining_tables
        assert ("view_2",) not in remaining_tables


def test_clean_throws_error_on_missing_params(mock_db_config):
    with pytest.raises(errors.CumulusLibraryError):
        cleaner.clean_study(config=mock_db_config, manifest=None)


@pytest.mark.parametrize(
    "study_path,stats",
    [
        ("./tests/test_data/study_valid/", False),
        ("./tests/test_data/psm/", True),
    ],
)
def test_run_protected_table_builder(mock_db_config, study_path, stats):
    manifest = study_manifest.StudyManifest(study_path)
    builder.run_protected_table_builder(config=mock_db_config, manifest=manifest)
    tables = (
        mock_db_config.db.cursor()
        .execute("SELECT distinct(table_name) FROM information_schema.tables ")
        .fetchall()
    )
    assert (f"{manifest.get_study_prefix()}__{enums.ProtectedTables.TRANSACTIONS.value}",) in tables
    if stats:
        assert (
            f"{manifest.get_study_prefix()}__{enums.ProtectedTables.STATISTICS.value}",
        ) in tables
    else:
        assert (
            f"{manifest.get_study_prefix()}__{enums.ProtectedTables.STATISTICS.value}",
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
def test_table_builder(mock_db_config, study_path, verbose, expects, raises):
    with raises:
        manifest = study_manifest.StudyManifest(pathlib.Path(study_path))
        builder.build_study(config=mock_db_config, manifest=manifest, data_path=None, prepare=False)
        tables = (
            mock_db_config.db.cursor()
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
            "./tests/test_data/study_invalid_bad_query/",
            None,
            ("study_valid__table",),
            pytest.raises(SystemExit),
        ),
        (
            "./tests/test_data/study_wrong_prefix/",
            None,
            [],
            pytest.raises(SystemExit),
        ),
        (
            "./tests/test_data/study_invalid_no_dunder/",
            True,
            (),
            pytest.raises(SystemExit),
        ),
        (
            "./tests/test_data/study_invalid_two_dunder/",
            True,
            (),
            pytest.raises(SystemExit),
        ),
        (
            "./tests/test_data/study_invalid_reserved_word/",
            True,
            (),
            pytest.raises(SystemExit),
        ),
    ],
)
def test_build_study(mock_db_config, study_path, verbose, expects, raises):
    with raises:
        manifest = study_manifest.StudyManifest(study_path)
        table = sql_utils.TransactionsTable()
        query = base_templates.get_ctas_empty_query(
            schema_name="main",
            table_name=f"{manifest.get_study_prefix()}__lib_transactions",
            table_cols=table.columns,
            table_cols_types=table.column_types,
        )
        mock_db_config.db.cursor().execute(query)
        builder.build_study(config=mock_db_config, manifest=manifest, data_path=None, prepare=False)
        tables = (
            mock_db_config.db.cursor()
            .execute("SELECT distinct(table_name) FROM information_schema.tables ")
            .fetchall()
        )
        assert expects in tables


@freeze_time("2024-01-01")
@pytest.mark.parametrize(
    "study_path,stats,previous,expects,raises",
    [
        (
            "./tests/test_data/psm/",
            False,
            False,
            ("psm_test__psm_encounter_covariate_2024_01_01T00_00_00_00_00",),
            does_not_raise(),
        ),
        (
            "./tests/test_data/psm/",
            False,
            True,
            ("psm_test__psm_encounter_covariate_2024_01_01T00_00_00_00_00",),
            does_not_raise(),
        ),
        (
            "./tests/test_data/psm/",
            True,
            False,
            ("psm_test__psm_encounter_covariate_2024_01_01T00_00_00_00_00",),
            does_not_raise(),
        ),
    ],
)
def test_run_psm_statistics_builders(
    tmp_path, mock_db_stats_config, study_path, stats, previous, expects, raises
):
    with raises:
        manifest = study_manifest.StudyManifest(pathlib.Path(study_path), data_path=tmp_path)
        config = base_utils.StudyConfig(
            db=mock_db_stats_config.db,
            schema=mock_db_stats_config.schema,
            stats_build=stats,
        )
        builder.run_protected_table_builder(
            config=config,
            manifest=manifest,
        )
        if previous:
            log_utils.log_statistics(
                config=config,
                manifest=manifest,
                table_type="psm",
                table_name="psm_test__psm_encounter_2023_06_15",
                view_name="psm_test__psm_encounter_covariate",
            )
        builder.build_study(config=config, manifest=manifest, prepare=False, data_path=None)
        tables = (
            mock_db_stats_config.db.cursor()
            .execute("SELECT distinct(table_name) FROM information_schema.tables")
            .fetchall()
        )
        if previous:
            assert expects not in tables
        else:
            assert expects in tables


@mock.patch("cumulus_library.builders.valueset_builder.ValuesetBuilder.execute_queries")
def test_invoke_valueset_builder(mock_builder, mock_db_config, tmp_path):
    manifest = study_manifest.StudyManifest(
        pathlib.Path(__file__).parent / "test_data/valueset", data_path=tmp_path
    )
    builder.run_protected_table_builder(
        config=mock_db_config,
        manifest=manifest,
    )
    config = base_utils.StudyConfig(
        db=mock_db_config.db, schema=mock_db_config.schema, stats_build=True
    )
    builder.build_study(config=config, manifest=manifest, prepare=False, data_path=None)
    assert mock_builder.is_called()


@mock.patch.dict(
    os.environ,
    clear=True,
)
def test_export_study(tmp_path, mock_db_core_config):
    manifest = study_manifest.StudyManifest(
        pathlib.Path(__file__).parent.parent / "cumulus_library/studies/core",
        data_path=tmp_path / "export",
    )
    export_path = tmp_path / "export/core/"
    export_path.mkdir(exist_ok=True, parents=True)
    with open(export_path / "to_be_deleted.file", "w") as f:
        f.write("foo")
    exporter.export_study(
        config=mock_db_core_config,
        manifest=manifest,
        data_path=tmp_path / "export",
        archive=False,
        chunksize=20,
    )

    for file in pathlib.Path(tmp_path / "export/core").glob("*.*"):
        if file.suffix == ".csv":
            assert any(
                file.name.split(".")[0] == entry.name for entry in manifest.get_export_table_list()
            )

        elif file.suffix == ".zip":
            archive = zipfile.ZipFile(export_path / "core.zip")
            archive_list = archive.namelist()
            assert len(archive_list) == len(manifest.get_export_table_list()) + 1
            for name in archive_list:
                assert (
                    any(
                        name.split(".")[0] == entry.name
                        for entry in manifest.get_export_table_list()
                    )
                    or name == "manifest.toml"
                )
        else:
            raise Exception("Unexpected file type in export dir")


@freeze_time("2024-01-01")
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


def test_builder_init_error(mock_db_config):
    manifest = study_manifest.StudyManifest(pathlib.Path(__file__).parent / "test_data/study_valid")
    builder.run_protected_table_builder(config=mock_db_config, manifest=manifest)
    console_output = io.StringIO()
    with pytest.raises(SystemExit):
        with contextlib.redirect_stdout(console_output):
            builder._query_error(
                mock_db_config, manifest, ["mock query", "mock_file.txt"], "Catalog Error"
            )
    assert "https://docs.smarthealthit.org/" in console_output.getvalue()


def do_upload(
    *,
    login_error: bool = False,
    user: str = "user",
    id_token: str = "id",
    network: str | None = None,
    preview: bool = True,
    raises: contextlib.AbstractContextManager = does_not_raise(),
    status: int = 204,
    version: str = "12345.0",
    call_count: int = 2,
    data_path: pathlib.Path | None = pathlib.Path.cwd() / "tests/test_data/upload/",
    study: str = "upload",
    transaction=None,
    transaction_mismatch: bool = False,
):
    url = "https://upload.url.test/"
    if network:
        url += network
    with raises:
        if login_error:
            responses.add(responses.POST, url, status=401)
        elif transaction_mismatch:
            responses.add(
                responses.POST, url, status=412, json=json.dumps("Study in processing, try again")
            )
        else:
            responses.add(
                responses.POST,
                url,
                match=[
                    matchers.json_params_matcher(
                        {
                            "study": study,
                            "data_package_version": int(float(version)),
                            "filename": f"{user}_{study}.zip",
                        }
                    )
                ],
                json={"url": "https://presigned.url.test", "fields": {"a": "b"}},
            )
        args = {
            "data_path": data_path,
            "id": id_token,
            "preview": preview,
            "target": [study],
            "network": network,
            "url": "https://upload.url.test/",
            "user": user,
        }
        responses.add(responses.POST, "https://presigned.url.test/", status=status)
        uploader.upload_files(args)
        responses.assert_call_count(url, call_count)


@pytest.mark.parametrize(
    "user,id_token,status,network,login_error,preview,call_count,raises",
    [
        (None, None, 204, None, False, False, None, pytest.raises(SystemExit)),
        ("user", "id", 204, None, False, False, 1, does_not_raise()),
        ("user", "id", 204, "network", False, False, 1, does_not_raise()),
        (
            "user",
            "id",
            500,
            None,
            False,
            False,
            None,
            pytest.raises(requests.exceptions.HTTPError),
        ),
        (
            "baduser",
            "badid",
            204,
            None,
            True,
            False,
            None,
            pytest.raises(requests.exceptions.HTTPError),
        ),
        (
            "user",
            "id",
            204,
            None,
            False,
            True,
            1,
            does_not_raise(),
        ),
    ],
)
@responses.activate
def test_upload_data(
    user,
    id_token,
    status,
    network,
    preview,
    login_error,
    call_count,
    raises,
    transaction=None,
    transaction_mismatch=None,
):
    do_upload(
        user=user,
        id_token=id_token,
        status=status,
        network=network,
        preview=preview,
        login_error=login_error,
        call_count=call_count,
        raises=raises,
        transaction=transaction,
        transaction_mismatch=transaction_mismatch,
    )


def test_upload_data_no_path():
    with pytest.raises(SystemExit):
        do_upload(data_path=None)


@responses.activate
def test_upload_data_no_version(tmp_path):
    src = pathlib.Path(__file__).resolve().parents[0] / "test_data/upload/upload.zip"
    dest = pathlib.Path(tmp_path) / "upload"
    dest.mkdir()
    shutil.copy(src, dest)
    do_upload(data_path=dest, version="0", call_count=1)


@responses.activate
def test_upload_data_no_meta_date(tmp_path):
    with pytest.raises(SystemExit):
        src = (
            pathlib.Path(__file__).resolve().parents[0]
            / "test_data/upload_no_date/upload_no_date.zip"
        )
        dest = pathlib.Path(tmp_path) / "upload"
        dest.mkdir()
        shutil.copy(src, dest)
        do_upload(data_path=dest, version="0", call_count=1)


@responses.activate
def test_upload_discovery(tmp_path):
    src = pathlib.Path(__file__).resolve().parents[0] / "test_data/upload/upload.zip"
    dest = pathlib.Path(tmp_path) / "discovery/discovery.zip"
    dest.parent.mkdir()
    shutil.copyfile(src, dest)
    do_upload(data_path=dest.parent, call_count=1, study="discovery")


@responses.activate
def test_upload_transaction_in_progress(tmp_path):
    src = pathlib.Path(__file__).resolve().parents[0] / "test_data/upload/upload.zip"
    dest = pathlib.Path(tmp_path) / "upload"
    dest.mkdir()
    shutil.copy(src, dest)
    with pytest.raises(SystemExit):
        do_upload(
            data_path=dest,
            version="0",
            call_count=1,
            preview=False,
            transaction_mismatch=True,
        )


@pytest.mark.parametrize(
    "study,external", [("study_python_valid", False), ("study_python_s3", True)]
)
def test_generate_sql(mock_db_config, tmp_path, study, external):
    with does_not_raise():
        shutil.copytree(
            f"{pathlib.Path(__file__).resolve().parents[0]}/test_data/{study}",
            f"{tmp_path}/{study}/",
        )
        manifest = study_manifest.StudyManifest(study_path=pathlib.Path(f"{tmp_path}/{study}/"))
        file_generator.run_generate_sql(manifest=manifest, config=mock_db_config)
        files = list(pathlib.Path(f"{tmp_path}/{study}/reference_sql/").glob("*"))
        files = [str(x) for x in files]
        assert len(files) == 2
        assert "module1.sql" in ",".join(files)
        for file in files:
            if file.endswith("module1.sql"):
                with open(file) as f:
                    query = "\n".join(line.rstrip() for line in f)
        assert "This sql was autogenerated" in query
        assert f"{study}__table" in query
        if external:
            assert "specific_bucket/databases/db1" not in query
            assert "bucket/db_path" in query


def test_generate_md(mock_db_config, tmp_path):
    with does_not_raise():
        shutil.copytree(
            f"{pathlib.Path(__file__).resolve().parents[0]}/test_data/study_python_valid",
            f"{tmp_path}/study_python_valid/",
        )
        manifest = study_manifest.StudyManifest(
            study_path=pathlib.Path(f"{tmp_path}/study_python_valid/")
        )
        builder.build_study(config=mock_db_config, manifest=manifest, prepare=False, data_path=None)
        file_generator.run_generate_markdown(config=mock_db_config, manifest=manifest)
        with open(f"{tmp_path}/study_python_valid/study_python_valid_generated.md") as f:
            generated_md = f.read()
        expected_table = """### study_python_valid__table

|Column| Type  |Description|
|------|-------|-----------|
|test  |INTEGER|           |"""
        assert expected_table in generated_md
