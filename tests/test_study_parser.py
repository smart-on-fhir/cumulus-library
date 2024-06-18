"""tests for study parser against mocks in test_data"""

import pathlib
from contextlib import nullcontext as does_not_raise
from unittest import mock

import pytest

from cumulus_library import errors, study_parser
from tests.test_data.parser_mock_data import get_mock_toml, mock_manifests


@pytest.mark.parametrize(
    "manifest_path,expected,raises",
    [
        (
            "test_data/study_valid",
            (
                "{'study_prefix': 'study_valid', 'sql_config': {'file_names': "
                "['test.sql', 'test2.sql']}, 'export_config': {'export_list': "
                "['study_valid__table', 'study_valid__table2']}}"
            ),
            does_not_raise(),
        ),
        (None, "{}", does_not_raise()),
        (
            "test_data/study_missing_prefix",
            "{}",
            pytest.raises(errors.StudyManifestParsingError),
        ),
        (
            "test_data/study_wrong_type",
            "{}",
            pytest.raises(errors.StudyManifestParsingError),
        ),
        ("", "{}", pytest.raises(errors.StudyManifestFilesystemError)),
        (".", "{}", pytest.raises(errors.StudyManifestFilesystemError)),
    ],
)
def test_load_manifest(manifest_path, expected, raises):
    with raises:
        if manifest_path is not None:
            path = f"{pathlib.Path(__file__).resolve().parents[0]}/{manifest_path}"
        else:
            path = None
        manifest = study_parser.StudyManifestParser(path)
        assert str(manifest) == expected


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
