"""tests for study parser against mocks in test_data"""

import pathlib
from contextlib import nullcontext as does_not_raise
from unittest import mock

import pytest

from cumulus_library import errors, study_manifest
from tests.test_data.parser_mock_data import get_mock_toml, mock_manifests


@pytest.mark.parametrize(
    "manifest_path,expected,raises",
    [
        (
            "test_data/study_valid",
            (
                "{'study_prefix': 'study_valid', 'file_config': {'file_names': "
                "['test.sql', 'test2.sql']}, 'export_config': {"
                "'count_list': ['study_valid__table', 'study_valid__table2']}}"
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
        manifest = study_manifest.StudyManifest(path)
        assert str(manifest) == expected


@pytest.mark.parametrize(
    "manifest_key, raises",
    [
        ("valid", does_not_raise()),
        ("valid_empty_arrays", does_not_raise()),
        ("valid_null_arrays", does_not_raise()),
        ("invalid_only_prefix", pytest.raises(errors.StudyManifestParsingError)),
        ("invalid_bad_export_names", pytest.raises(errors.StudyManifestParsingError)),
        ("invalid_bad_table_names", pytest.raises(errors.StudyManifestParsingError)),
        ("invalid_none", pytest.raises(TypeError)),
    ],
)
@mock.patch("builtins.open")
@mock.patch("tomllib.load")
def test_manifest_data(mock_load, mock_open, manifest_key, raises):
    mock_load.return_value = get_mock_toml(manifest_key)
    with raises:
        if manifest_key == "invalid_none":
            manifest = study_manifest.StudyManifest()
        else:
            manifest = study_manifest.StudyManifest("./path")
        expected = mock_manifests[manifest_key]
        assert manifest.get_study_prefix() == expected["study_prefix"]
        if "file_config" in expected.keys():
            if expected["file_config"]["file_names"] is None:
                assert manifest.get_file_list() == []
            else:
                assert manifest.get_file_list() == expected["file_config"]["file_names"]
        else:
            assert manifest.get_file_list() == []
        if "export_config" in expected.keys():
            if expected["export_config"]["export_list"] is None:
                assert manifest.get_export_table_list() == []
            else:
                table_list = [x.name for x in manifest.get_export_table_list()]
                assert table_list == expected["export_config"]["export_list"]
        else:
            assert manifest.get_export_table_list() == []


@pytest.mark.parametrize(
    "manifest_path,prefix",
    [
        ("test_data/study_valid", "study_valid__"),
        ("test_data/study_dedicated_schema", "dedicated."),
    ],
)
def test_get_prefix_with_seperator(manifest_path, prefix):
    path = f"{pathlib.Path(__file__).resolve().parents[0]}/{manifest_path}"
    manifest = study_manifest.StudyManifest(path)
    assert prefix == manifest.get_prefix_with_seperator()
