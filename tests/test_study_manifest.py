"""tests for study parser against mocks in test_data"""

import json
import pathlib
from contextlib import nullcontext as does_not_raise

import pytest
import tomli_w

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
            {
                "type": "export:counts",
                "files": ["table__name", {"name": "table__other_name", "description": "text"}],
            },
            pytest.raises(errors.StudyManifestParsingError, match="expected key, 'tables'"),
        ),
        (
            {"type": "build:serial", "files": ["table__name"], "label": "foo [bar]"},
            pytest.raises(errors.StudyManifestParsingError, match="square brackets"),
        ),
    ],
)
def test_validate_action(action, raises, tmp_path):
    with raises:
        manifest = study_manifest.StudyManifest()
        manifest._validate_action(action=action, source=tmp_path)


@pytest.mark.parametrize(
    "stages,stage,expected,raises",
    [
        (
            {
                "stage_one": [
                    {
                        "type": "export:counts",
                        "tables": ["test__name", {"name": "test__name2", "description": "text"}],
                    }
                ]
            },
            None,
            [
                study_manifest.ManifestExport(
                    name="test__name", export_type="cube", description=None
                ),
                study_manifest.ManifestExport(
                    name="test__name2", export_type="cube", description="text"
                ),
            ],
            does_not_raise(),
        ),
        (
            {
                "stage_one": [
                    {
                        "type": "export:counts",
                        "tables": ["test__name", {"name": "test__name2", "description": "text"}],
                    }
                ]
            },
            "stage_one",
            [
                study_manifest.ManifestExport(
                    name="test__name", export_type="cube", description=None
                ),
                study_manifest.ManifestExport(
                    name="test__name2", export_type="cube", description="text"
                ),
            ],
            does_not_raise(),
        ),
        (
            {
                "stage_one": [{"type": "export:counts", "tables": ["test__name"]}],
                "stage_two": [{"type": "export:counts", "tables": ["test__name2"]}],
            },
            "stage_one",
            [
                study_manifest.ManifestExport(
                    name="test__name", export_type="cube", description=None
                ),
            ],
            does_not_raise(),
        ),
        (
            {
                "stage_one": [{"type": "export:counts", "tables": ["test__name"]}],
                "stage_two": [{"type": "export:counts", "tables": ["test__name2"]}],
            },
            "all",
            [
                study_manifest.ManifestExport(
                    name="test__name", export_type="cube", description=None
                ),
                study_manifest.ManifestExport(
                    name="test__name2", export_type="cube", description=None
                ),
            ],
            does_not_raise(),
        ),
        (
            {
                "stage_one": [{"type": "export:counts", "tables": ["bad_prefix__name"]}],
            },
            None,
            [],
            pytest.raises(errors.StudyManifestParsingError),
        ),
        (
            {
                "stage_one": [
                    {"type": "export:counts", "tables": ["test__counts"]},
                    {"type": "export:annotated_counts", "tables": ["test__annotated_counts"]},
                    {"type": "export:flat", "tables": ["test__flat"]},
                    {"type": "export:meta", "tables": ["test__meta"]},
                ],
            },
            None,
            [
                study_manifest.ManifestExport(
                    name="test__counts", export_type="cube", description=None
                ),
                study_manifest.ManifestExport(
                    name="test__annotated_counts", export_type="annotated_counts", description=None
                ),
                study_manifest.ManifestExport(
                    name="test__flat", export_type="flat", description=None
                ),
                study_manifest.ManifestExport(
                    name="test__meta", export_type="meta", description=None
                ),
            ],
            does_not_raise(),
        ),
    ],
)
def test_export_list(stages, stage, expected, raises, tmp_path):
    with raises:
        conftest.write_toml(tmp_path, {"study_prefix": "test", "stages": stages})
        manifest = study_manifest.StudyManifest(tmp_path)
        if stage is None:
            export_list = manifest.get_export_table_list()
        else:
            export_list = manifest.get_export_table_list(stage)
        assert export_list == expected


@pytest.mark.parametrize(
    "data,file_type,expected,raises",
    [
        ("name,display\na,b", "csv", [{"display": "b", "name": "a"}], does_not_raise()),
        (
            {"fields": [{"name": "a", "display": "b"}]},
            "json",
            [{"display": "b", "name": "a"}],
            does_not_raise(),
        ),
        (
            {"fields": [{"name": "a", "display": "b"}]},
            "toml",
            [{"display": "b", "name": "a"}],
            does_not_raise(),
        ),
        (
            {
                "fields": [
                    {
                        "name": "a",
                        "display": "b",
                        "description": "c",
                        "details": "d",
                        "type": "string",
                    }
                ]
            },
            "json",
            [{"name": "a", "display": "b", "description": "c", "details": "d", "type": "string"}],
            does_not_raise(),
        ),
        (
            {"fields": [{"name": "a", "foo": "bar"}]},
            "json",
            None,
            pytest.raises(errors.StudyManifestParsingError),
        ),
        (
            {"fields": [{"name": "a", "display": "b"}]},
            "docx",
            None,
            pytest.raises(errors.StudyManifestParsingError),
        ),
        (
            {"fields": [{"description": "b"}]},
            "json",
            None,
            pytest.raises(errors.StudyManifestParsingError),
        ),
        (
            {"fields": [{"name": "a", "type": "EnterpriseBeanFactory"}]},
            "json",
            None,
            pytest.raises(errors.StudyManifestParsingError),
        ),
    ],
)
def test_data_dictionary(tmp_path, data, file_type, expected, raises):
    with raises:
        with open(tmp_path / f"data.{file_type}", "w") as f:
            match file_type:
                case "csv":
                    f.write(data)
                case "json":
                    f.write(json.dumps(data))
                case "toml":
                    f.write(tomli_w.dumps(data))
        conftest.write_toml(
            tmp_path,
            {
                "study_prefix": "test",
                "data_dictionary": f"data.{file_type}",
                "stages": {"stage_one": [{"type": "export:counts", "tables": ["test__name"]}]},
            },
        )
        manifest = study_manifest.StudyManifest(tmp_path)
        assert expected == manifest._study_config["data_dictionary"]


def test_manifest_materialization(tmp_path):
    conftest.write_toml(
        tmp_path,
        {
            "study_prefix": "test",
            "stages": {
                "stage_one": [
                    {"type": "export:counts", "tables": ["test__name", "counts.workflow"]}
                ]
            },
        },
    )
    conftest.write_toml(
        tmp_path,
        {
            "config_type": "counts",
            "tables": {
                "count_1": {
                    "source_table": "patient",
                    "description": "A table",
                    "table_cols": ["foo"],
                },
                "count_2": {"source_table": "patient", "table_cols": ["foo"]},
            },
        },
        "counts.workflow",
    )
    manifest = study_manifest.StudyManifest(tmp_path)
    manifest.materialize_counts_builder_exports()
    assert manifest._study_config["stages"]["all"] == [
        {
            "type": "export:counts",
            "tables": [
                "test__name",
                {"name": "count_1", "description": "A table"},
                {"name": "count_2", "description": None},
            ],
        }
    ]
