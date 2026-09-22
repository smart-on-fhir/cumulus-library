import contextlib
import os
import pathlib
import tomllib
import zipfile
from unittest import mock

import cumulus_fhir_support as cfs
import pytest

import cumulus_library
from cumulus_library import cli
from cumulus_library.actions import (
    exporter,
)


@pytest.mark.parametrize("is_remote_path", [False, True])
@mock.patch.dict(
    os.environ,
    clear=True,
)
@mock.patch("cumulus_library.builders.counts_builder.DEFAULT_MIN_SUBJECT", new=1)
def test_export_study(tmp_path, mock_db, is_remote_path):
    config = cumulus_library.StudyConfig(db=mock_db, schema="main")
    builder = cli.StudyRunner(config, data_path=f"{tmp_path}/data_path")
    study_path = pathlib.Path(__file__).parents[2] / "cumulus_library/studies/core"
    builder.clean_and_build_study(study_path, options={})

    with contextlib.ExitStack() as stack:
        if is_remote_path:
            # memory:// is essentially a test mock for s3:// in cfs
            data_path = cfs.FsPath(f"memory://{tmp_path.name}/export")
            stack.callback(data_path.rm)
        else:
            data_path = cfs.FsPath(tmp_path, "export")

        manifest = cumulus_library.StudyManifest(study_path, data_path=str(data_path))

        study_dir = data_path.joinpath("core")
        study_dir.makedirs()
        study_dir.joinpath("to_be_deleted.file").write_text("foo")

        exporter.export_study(
            config=config,
            manifest=manifest,
            data_path=data_path,
            archive=False,
            chunksize=20,
        )

        export_tables = {entry.name for entry in manifest.get_export_table_list()}
        with study_dir.joinpath("core.zip").open("rb", compression=None) as archive_file:
            archive = zipfile.ZipFile(archive_file)
            archive_list = archive.namelist()
            assert len(archive_list) == len(export_tables) + 1
            for name in archive_list:
                assert name.split(".")[0] in export_tables or name == "manifest.toml"
            exported_manifest = tomllib.loads(archive.read("manifest.toml").decode())
        assert exported_manifest["study_prefix"] == "core"
