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
                    "stages": {
                        "all": [
                            {"type": "build:serial", "files": ["test.sql", "test2.sql"]},
                            {
                                "type": "export:counts",
                                "tables": ["study_valid__table", "study_valid__table2"],
                            },
                        ],
                        "stage_1": [
                            {"type": "build:serial", "files": ["test.sql", "test2.sql"]},
                            {
                                "type": "export:counts",
                                "tables": ["study_valid__table", "study_valid__table2"],
                            },
                        ],
                    },
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
    "manifest_path,prefix, prefix_with_sep",
    [
        ("test_data/study_valid", "study_valid", "study_valid__"),
        ("test_data/study_dedicated_schema", "", ""),
    ],
)
def test_schema_aware(manifest_path, prefix, prefix_with_sep):
    path = f"{pathlib.Path(__file__).resolve().parents[0]}/{manifest_path}"
    manifest = study_manifest.StudyManifest(path)
    assert prefix == manifest.get_schema_aware_prefix()
    assert prefix_with_sep == manifest.get_schema_aware_prefix_with_seperator()


def test_custom_pathing(tmp_path):
    manifest_dict = {
        "study_prefix": "custom",
        "stages": {"stage_1": [{"type": "build:serial", "files": ["bar"]}]},
    }
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
        "stages": {
            "stage_1": [
                {
                    "type": "build:serial",
                    "label": "action 1",
                    "files": ["foo", "bar"],
                },
                {
                    "type": "build:serial",
                    "label": "action 2",
                    "files": ["baz"],
                },
            ],
            "stage_2": [{"type": "submanifest", "files": ["file.submanifest"]}],
        },
    }
    conftest.write_toml(tmp_path, manifest_dict)
    conftest.write_toml(
        tmp_path,
        {"actions": [{"type": "build:serial", "label": "subaction 1", "files": ["foobar"]}]},
        "file.submanifest",
    )
    manifest = study_manifest.StudyManifest(tmp_path)
    assert manifest._study_config == {
        "study_prefix": "primary",
        "stages": {
            "all": [
                {"label": "action 1", "type": "build:serial", "files": ["foo", "bar"]},
                {"label": "action 2", "type": "build:serial", "files": ["baz"]},
                {"label": "subaction 1", "type": "build:serial", "files": ["foobar"]},
            ],
            "stage_1": [
                {"label": "action 1", "type": "build:serial", "files": ["foo", "bar"]},
                {"label": "action 2", "type": "build:serial", "files": ["baz"]},
            ],
            "stage_2": [{"label": "subaction 1", "type": "build:serial", "files": ["foobar"]}],
        },
    }


def test_copy_manifest(tmp_path):
    conftest.write_toml(
        tmp_path,
        {
            "study_prefix": "copy",
            "stages": {"default": [{"type": "build:serial", "files": ["foo"]}]},
        },
    )
    manifest = study_manifest.StudyManifest(tmp_path)
    manifest.copy_manifest(tmp_path / "dest")
    new_manifest = study_manifest.StudyManifest(tmp_path / "dest/copy")
    assert new_manifest.get_study_prefix() == "copy"


def test_wrong_export_prefix(tmp_path):
    conftest.write_toml(
        tmp_path,
        {
            "study_prefix": "foo",
            "stages": {"stage_1": [{"type": "export:counts", "tables": ["bar__table"]}]},
        },
    )
    manifest = study_manifest.StudyManifest(tmp_path)
    with pytest.raises(errors.StudyManifestParsingError):
        manifest.get_export_table_list()


def test_reserved_stage_name(tmp_path):
    conftest.write_toml(tmp_path, {"study_prefix": "foo", "stage": {"all": ["test.sql"]}})

    with pytest.raises(errors.StudyManifestParsingError):
        study_manifest.StudyManifest(tmp_path)


def test_missing_stage(tmp_path):
    conftest.write_toml(tmp_path, {"study_prefix": "foo"})
    with pytest.raises(errors.StudyManifestParsingError):
        study_manifest.StudyManifest(tmp_path)


def test_missing_action(tmp_path):
    conftest.write_toml(
        tmp_path,
        {
            "study_prefix": "foo",
            "stages": {"stage_1": [{"label": "action 1", "files": ["foo"], "type": "invalid"}]},
        },
    )
    with pytest.raises(errors.StudyManifestParsingError):
        study_manifest.StudyManifest(tmp_path)


def test_all_protected(tmp_path):
    manifest_dict = {
        "study_prefix": "test",
        "stages": {"all": [{"files": ["foo"], "type": "build:serial"}]},
    }
    conftest.write_toml(tmp_path, manifest_dict, "manifest.toml")
    with pytest.raises(errors.StudyManifestParsingError):
        study_manifest.StudyManifest(tmp_path)


def test_default_handling(tmp_path):
    manifest_dict = {
        "study_prefix": "test",
        "stages": {
            "one": [{"files": ["foo"], "type": "build:serial"}],
            "two": [{"files": ["bar"], "type": "build:serial"}],
        },
    }
    conftest.write_toml(tmp_path, manifest_dict, "manifest.toml")
    manifest = study_manifest.StudyManifest(tmp_path)
    stage = manifest.get_stage("default")
    assert stage == [
        {"type": "build:serial", "files": ["foo"]},
        {"type": "build:serial", "files": ["bar"]},
    ]
    manifest_dict = {
        "study_prefix": "test",
        "stages": {
            "default": [{"files": ["foo"], "type": "build:serial"}],
            "two": [{"files": ["bar"], "type": "build:serial"}],
        },
    }
    conftest.write_toml(tmp_path, manifest_dict, "manifest.toml")
    manifest = study_manifest.StudyManifest(tmp_path)
    stage = manifest.get_stage("default")
    assert stage == [
        {"type": "build:serial", "files": ["foo"]},
    ]


def test_empty_stage(mock_db_config, tmp_path):
    manifest_dict = {
        "study_prefix": "test",
        "stages": {},
    }
    conftest.write_toml(tmp_path, manifest_dict, "manifest.toml")
    with pytest.raises(errors.StudyManifestParsingError):
        study_manifest.StudyManifest(tmp_path)


def test_formatted_study_prefix(tmp_path):
    conftest.write_toml(
        tmp_path,
        {
            "study_prefix": "foo",
            "stages": {"stage_1": [{"type": "build:serial", "files": ["foo"]}]},
        },
    )
    manifest = study_manifest.StudyManifest(tmp_path)
    assert manifest.get_formatted_study_prefix() == "foo__"
    conftest.write_toml(
        tmp_path,
        {
            "study_prefix": "foo",
            "stages": {"stage_1": [{"type": "build:serial", "files": ["foo"]}]},
            "advanced_options": {"dedicated_schema": "bar"},
        },
    )
    manifest = study_manifest.StudyManifest(tmp_path)
    assert manifest.get_formatted_study_prefix() == "bar."


@pytest.mark.parametrize(
    "action,raises",
    [
        ({"type": "build:serial", "files": ["file.txt"]}, does_not_raise()),
        ({"type": "export:counts", "tables": ["table__name"]}, does_not_raise()),
        (
            {"type": "build:serial", "tables": ["file.txt"]},
            pytest.raises(errors.StudyManifestParsingError, match="expected key, 'files'"),
        ),
        (
            {"type": "export:counts", "files": ["table__name"]},
            pytest.raises(errors.StudyManifestParsingError, match="expected key, 'tables'"),
        ),
    ],
)
def test_validate_action(action, raises, tmp_path):
    with raises:
        manifest = study_manifest.StudyManifest()
        manifest._validate_action(action=action, source=tmp_path)
