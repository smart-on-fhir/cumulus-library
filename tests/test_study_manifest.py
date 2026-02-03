"""tests for study parser against mocks in test_data"""

import pathlib
from contextlib import nullcontext as does_not_raise

import pytest

from cumulus_library import errors, study_manifest
from tests import conftest


@pytest.mark.parametrize(
    "manifest_path,expected,raises",
    [
        (
            "test_data/study_valid",
            (
                {
                    "study_prefix": "study_valid",
                    "build_types": {"default": ["stage_1"], "all": ["stage_1"]},
                    "stages": {"stage_1": [{"files": ["test.sql", "test2.sql"]}]},
                    "export_config": {"count_list": ["study_valid__table", "study_valid__table2"]},
                }
            ),
            does_not_raise(),
        ),
        (None, {}, does_not_raise()),
        (
            "test_data/study_missing_prefix",
            {},
            pytest.raises(errors.StudyManifestParsingError),
        ),
        (
            "test_data/study_wrong_type",
            {},
            pytest.raises(errors.StudyManifestParsingError),
        ),
        ("", {}, pytest.raises(errors.StudyManifestFilesystemError)),
        (".", {}, pytest.raises(errors.StudyManifestFilesystemError)),
    ],
)
def test_load_manifest(manifest_path, expected, raises):
    with raises:
        if manifest_path is not None:
            path = f"{pathlib.Path(__file__).resolve().parents[0]}/{manifest_path}"
        else:
            path = None
        manifest = study_manifest.StudyManifest(path)
        assert manifest._study_config == expected


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


def test_custom_pathing(tmp_path):
    manifest_dict = {"study_prefix": "custom", "stages": {"stage_1": [{"files": ["bar"]}]}}
    conftest.write_toml(tmp_path, manifest_dict, "custom.toml")
    with pytest.raises(errors.StudyManifestFilesystemError):
        manifest = study_manifest.StudyManifest(tmp_path)
    manifest = study_manifest.StudyManifest(tmp_path / "custom.toml")
    assert manifest.get_study_prefix() == "custom"
    manifest_dict["study_prefix"] = "manifest"
    conftest.write_toml(tmp_path, manifest_dict)
    manifest = study_manifest.StudyManifest(tmp_path)
    assert manifest.get_study_prefix() == "manifest"
    manifest = study_manifest.StudyManifest(tmp_path / "manifest.toml")
    assert manifest.get_study_prefix() == "manifest"


def test_submanifests(tmp_path):
    manifest_dict = {
        "study_prefix": "primary",
        "build_types": {"default": ["stage_1", "stage_2"]},
        "stages": {
            "stage_1": [
                {
                    "description": "action 1",
                    "files": ["foo", "bar"],
                },
                {
                    "description": "action 2",
                    "files": ["baz"],
                },
            ],
            "stage_2": [{"action_type": "submanifest", "files": ["file.submanifest"]}],
        },
    }
    conftest.write_toml(tmp_path, manifest_dict)
    conftest.write_toml(
        tmp_path,
        {"actions": [{"description": "subaction 1", "files": ["foobar"]}]},
        "file.submanifest",
    )
    manifest = study_manifest.StudyManifest(tmp_path)
    assert manifest._study_config == {
        "study_prefix": "primary",
        "build_types": {"default": ["stage_1", "stage_2"], "all": ["stage_1", "stage_2"]},
        "stages": {
            "stage_1": [
                {"description": "action 1", "files": ["foo", "bar"]},
                {"description": "action 2", "files": ["baz"]},
            ],
            "stage_2": [{"description": "subaction 1", "files": ["foobar"]}],
        },
    }


def test_copy_manifest(tmp_path):
    conftest.write_toml(tmp_path, {"study_prefix": "copy"})
    manifest = study_manifest.StudyManifest(tmp_path)
    manifest.copy_manifest(tmp_path / "dest")
    new_manifest = study_manifest.StudyManifest(tmp_path / "dest/copy")
    assert new_manifest.get_study_prefix() == "copy"


def test_wrong_export_prefix(tmp_path):
    conftest.write_toml(
        tmp_path, {"study_prefix": "foo", "export_config": {"count_list": "bar__table"}}
    )
    manifest = study_manifest.StudyManifest(tmp_path)
    with pytest.raises(errors.StudyManifestParsingError):
        manifest.get_export_table_list()


def test_reserved_stage_name(tmp_path):
    conftest.write_toml(tmp_path, {"study_prefix": "foo", "build_types": {"all": ["stage_1"]}})

    #    with pytest.raises(errors.StudyManifestParsingError):
    study_manifest.StudyManifest(tmp_path)
