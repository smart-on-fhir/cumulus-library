""" tests for study parser against mocks in test_data """
import builtins
import pathlib

from contextlib import nullcontext as does_not_raise
from pathlib import Path
from unittest import mock

import pytest

from cumulus_library.enums import PROTECTED_TABLE_KEYWORDS
from cumulus_library.study_parser import StudyManifestParser, StudyManifestParsingError
from tests.test_data.parser_mock_data import get_mock_toml, mock_manifests


@pytest.mark.parametrize(
    "manifest_path,raises",
    [
        ("test_data/study_valid", does_not_raise()),
        (None, does_not_raise()),
        ("test_data/study_missing_prefix", pytest.raises(StudyManifestParsingError)),
        ("test_data/study_wrong_type", pytest.raises(StudyManifestParsingError)),
        ("", pytest.raises(StudyManifestParsingError)),
        (".", pytest.raises(StudyManifestParsingError)),
    ],
)
def test_load_manifest(manifest_path, raises):
    with raises:
        if manifest_path is not None:
            path = f"{pathlib.Path(__file__).resolve().parents[0]}/{manifest_path}"
        else:
            path = None
        StudyManifestParser(path)


@pytest.mark.parametrize(
    "manifest_key, raises",
    [
        ("valid", does_not_raise()),
        ("valid_empty_arrays", does_not_raise()),
        ("valid_null_arrays", does_not_raise()),
        ("valid_only_prefix", does_not_raise()),
        ("invalid_bad_export_names", pytest.raises(StudyManifestParsingError)),
        ("invalid_none", pytest.raises(TypeError)),
    ],
)
def test_manifest_data(manifest_key, raises):
    with mock.patch(
        "builtins.open", mock.mock_open(read_data=get_mock_toml(manifest_key))
    ):
        with raises:
            if manifest_key == "invalid_none":
                parser = StudyManifestParser()
            else:
                parser = StudyManifestParser("./path")
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
    "schema,verbose,prefix,confirm,target,raises",
    [
        ("main", True, None, None, "study_valid__table", does_not_raise()),
        ("main", False, None, None, "study_valid__table", does_not_raise()),
        ("main", None, None, None, "study_valid__table", does_not_raise()),
        (None, True, None, None, None, pytest.raises(ValueError)),
        ("main", None, None, None, "study_valid__etl_table", does_not_raise()),
        ("main", None, None, None, "study_valid__nlp_table", does_not_raise()),
        ("main", None, None, None, "study_valid__lib_table", does_not_raise()),
        ("main", None, None, None, "study_valid__lib", does_not_raise()),
        ("main", None, "foo", "y", "foo_table", does_not_raise()),
        ("main", None, "foo", "n", "foo_table", pytest.raises(SystemExit)),
    ],
)
def test_clean_study(mock_db, schema, verbose, prefix, confirm, target, raises):
    with raises:
        protected_strs = [x.value for x in PROTECTED_TABLE_KEYWORDS]
        with mock.patch.object(builtins, "input", lambda _: confirm):
            parser = StudyManifestParser("./tests/test_data/study_valid/")
            if target is not None:
                mock_db.cursor().execute(f"CREATE TABLE {target} (test int);")
            parser.clean_study(mock_db.cursor(), schema, verbose=verbose, prefix=prefix)
            remaining_tables = (
                mock_db.cursor()
                .execute(f"select distinct(table_name) from information_schema.tables")
                .fetchall()
            )
            if any(x in target for x in protected_strs):
                assert (target,) in remaining_tables
            else:
                assert (target,) not in remaining_tables


"""
        ("./tests/test_data/study_python_valid/", True, ('study_python_valid__table',), does_not_raise()),

        (
            "./tests/test_data/study_python_no_subclass/",
            True,
            (),
            does_not_raise(),
        ),

"""


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
        ("./tests/test_data/study_wrong_prefix/", None, [], pytest.raises(SystemExit)),
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
def test_build_study(mock_db, study_path, verbose, expects, raises):
    with raises:
        parser = StudyManifestParser(study_path)
        parser.build_study(mock_db.cursor(), verbose)
        tables = (
            mock_db.cursor()
            .execute("SELECT distinct(table_name) FROM information_schema.tables ")
            .fetchall()
        )
        assert expects in tables


def test_export_study(tmp_path, mock_db_core):
    parser = StudyManifestParser(
        f"{Path(__file__).parent.parent}/cumulus_library/studies/core",
        data_path=f"{tmp_path}/export",
    )
    parser.export_study(mock_db_core, f"{tmp_path}/export")
    for file in Path(f"{tmp_path}/export").glob("*.*"):
        assert file in parser.get_export_table_list()
