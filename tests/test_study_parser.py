""" tests for study parser against mocks in test_data """

import builtins
import pathlib
from contextlib import nullcontext as does_not_raise
from pathlib import Path
from unittest import mock

import pytest

from cumulus_library import base_utils, enums, errors, study_parser
from tests.test_data.parser_mock_data import get_mock_toml, mock_manifests


@pytest.mark.parametrize(
    "manifest_path,raises",
    [
        ("test_data/study_valid", does_not_raise()),
        (None, does_not_raise()),
        (
            "test_data/study_missing_prefix",
            pytest.raises(errors.StudyManifestParsingError),
        ),
        ("test_data/study_wrong_type", pytest.raises(errors.StudyManifestParsingError)),
        ("", pytest.raises(errors.StudyManifestFilesystemError)),
        (".", pytest.raises(errors.StudyManifestFilesystemError)),
    ],
)
def test_load_manifest(manifest_path, raises):
    with raises:
        if manifest_path is not None:
            path = f"{pathlib.Path(__file__).resolve().parents[0]}/{manifest_path}"
        else:
            path = None
        study_parser.StudyManifestParser(path)


@pytest.mark.parametrize(
    "manifest_key, raises",
    [
        ("valid", does_not_raise()),
        ("valid_empty_arrays", does_not_raise()),
        ("valid_null_arrays", does_not_raise()),
        ("valid_only_prefix", does_not_raise()),
        ("invalid_bad_export_names", pytest.raises(errors.StudyManifestParsingError)),
        ("invalid_none", pytest.raises(TypeError)),
    ],
)
def test_manifest_data(manifest_key, raises):
    with mock.patch(
        "builtins.open", mock.mock_open(read_data=get_mock_toml(manifest_key))
    ):
        with raises:
            if manifest_key == "invalid_none":
                parser = study_parser.StudyManifestParser()
            else:
                parser = study_parser.StudyManifestParser("./path")
            expected = mock_manifests[manifest_key]
            assert parser.get_study_prefix() == expected["study_prefix"]
            if "sql_config" in expected.keys():
                if expected["sql_config"]["file_names"] is None:
                    assert parser.get_sql_file_list() == []
                else:
                    assert (
                        parser.get_sql_file_list()
                        == expected["sql_config"]["file_names"]
                    )
            else:
                assert parser.get_sql_file_list() == []
            if "export_config" in expected.keys():
                if expected["export_config"]["export_list"] is None:
                    assert parser.get_export_table_list() == []
                else:
                    assert (
                        parser.get_export_table_list()
                        == expected["export_config"]["export_list"]
                    )
            else:
                assert parser.get_export_table_list() == []


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
            parser.run_protected_table_builder(
                mock_db.cursor(),
                schema,
                config=base_utils.StudyConfig(db_backend=mock_db),
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
            parser.clean_study(
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
    parser.run_protected_table_builder(
        mock_db.cursor(), "main", config=base_utils.StudyConfig(db_backend=mock_db)
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
        parser = study_parser.StudyManifestParser(study_path)
        parser.run_table_builder(
            mock_db.cursor(),
            "main",
            verbose=verbose,
            config=base_utils.StudyConfig(db_backend=mock_db),
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
        parser.build_study(
            mock_db.cursor(),
            verbose=verbose,
            config=base_utils.StudyConfig(db_backend=mock_db),
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
        config = base_utils.StudyConfig(db_backend=mock_db_stats, stats_build=stats)
        parser.run_protected_table_builder(
            mock_db_stats.cursor(), "main", config=config
        )
        parser.build_study(mock_db_stats.cursor(), config=config)
        parser.run_statistics_builders(mock_db_stats.cursor(), "main", config=config)
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
    parser.export_study(mock_db_core, None, f"{tmp_path}/export", False)
    for file in Path(f"{tmp_path}/export").glob("*.*"):
        assert file in parser.get_export_table_list()
