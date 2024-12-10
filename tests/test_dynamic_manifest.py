"""tests for using dynamic values in study manifests"""

import builtins
import os
import pathlib
import shutil
from contextlib import nullcontext as does_not_raise
from pathlib import Path
from unittest import mock

import duckdb
import pytest

from cumulus_library import cli, errors, study_manifest
from tests.conftest import duckdb_args

STUDY_ARGS = [
    f"--study-dir={Path(__file__).parent}/test_data/study_dynamic_prefix",
    "--target=dynamic",
]


@pytest.mark.parametrize(
    "options,result",
    [
        ({"prefix": "my_study"}, "my_study"),  # simply happy path
        ({"prefix": "STUDY"}, "study"),  # lowercased
        ({"prefix": ""}, pytest.raises(errors.StudyManifestParsingError)),  # empty prefix
        ({"prefix": "..."}, pytest.raises(errors.StudyManifestParsingError)),  # bad prefix
        ({"prefix": "123"}, pytest.raises(errors.StudyManifestParsingError)),  # bad prefix
    ],
)
def test_manifest_with_dynamic_prefix(options, result):
    if isinstance(result, str):
        raises = does_not_raise()
    else:
        raises = result
    with raises:
        path = pathlib.Path("tests/test_data/study_dynamic_prefix")
        manifest = study_manifest.StudyManifest(path, options=options)
        assert manifest.get_study_prefix() == result


@mock.patch("sys.executable", new=None)
def test_manifest_with_dynamic_prefix_and_no_executable():
    """sys.executable must be valid for us to run a Python script"""
    with pytest.raises(RuntimeError):
        study_manifest.StudyManifest(pathlib.Path("tests/test_data/study_dynamic_prefix"))


def test_cli_clean_with_dynamic_prefix(tmp_path):
    cli.main(cli_args=duckdb_args(["build", *STUDY_ARGS, "--option=prefix:dynamic2"], tmp_path))
    cli.main(cli_args=duckdb_args(["build", *STUDY_ARGS], tmp_path))

    # Confirm that both prefixes got built
    db = duckdb.connect(f"{tmp_path}/duck.db")
    tables = {row[0] for row in db.cursor().execute("show tables").fetchall()}
    assert "dynamic__meta_version" in tables
    assert "dynamic2__meta_version" in tables

    # Clean 2nd table
    with mock.patch.object(builtins, "input", lambda _: "y"):
        cli.main(cli_args=duckdb_args(["clean", *STUDY_ARGS, "--option=prefix:dynamic2"], tmp_path))

    db = duckdb.connect(f"{tmp_path}/duck.db")
    tables = {row[0] for row in db.cursor().execute("show tables").fetchall()}
    assert "dynamic__meta_version" in tables
    assert "dynamic2__meta_version" not in tables


def test_cli_export_with_dynamic_prefix(tmp_path):
    cli.main(cli_args=duckdb_args(["build", *STUDY_ARGS, "--option=prefix:abc"], tmp_path))
    cli.main(cli_args=duckdb_args(["export", *STUDY_ARGS, "--option=prefix:abc"], tmp_path))
    assert set(os.listdir(f"{tmp_path}/export")) == {"abc"}
    assert set(os.listdir(f"{tmp_path}/export/abc")) == {
        "abc__counts.cube.csv",
        "abc__counts.cube.parquet",
        "abc__meta_version.cube.csv",
        "abc__meta_version.cube.parquet",
    }


@mock.patch.dict(
    os.environ,
    clear=True,
)
def test_cli_generate_sql_with_dynamic_prefix(tmp_path):
    shutil.copytree(
        pathlib.Path(__file__).parent / "test_data/study_dynamic_prefix",
        tmp_path,
        dirs_exist_ok=True,
    )
    study_args = [f"--study-dir={tmp_path}", "--target=dynamic"]
    cli.main(cli_args=duckdb_args(["generate-sql", *study_args, "--option=prefix:abc"], tmp_path))
    assert set(os.listdir(f"{tmp_path}/reference_sql")) == {"counts.sql", "meta.sql"}

    with open(f"{tmp_path}/reference_sql/meta.sql", encoding="utf8") as f:
        sql = f.readlines()
    assert sql[-1] == "CREATE TABLE abc__meta_version AS SELECT 1 AS data_package_version;\n"
