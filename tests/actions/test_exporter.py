import os
import pathlib
import tomllib
import zipfile
from unittest import mock

import cumulus_library
from cumulus_library import cli
from cumulus_library.actions import (
    exporter,
)


@mock.patch.dict(
    os.environ,
    clear=True,
)
@mock.patch("cumulus_library.builders.counts_builder.DEFAULT_MIN_SUBJECT", new=1)
def test_export_study(tmp_path, mock_db):
    config = cumulus_library.StudyConfig(db=mock_db, schema="main")
    builder = cli.StudyRunner(config, data_path=f"{tmp_path}/data_path")
    builder.clean_and_build_study(
        pathlib.Path(__file__).parent.parent.parent / "cumulus_library/studies/core",
        options={},
    )
    manifest = cumulus_library.StudyManifest(
        pathlib.Path(__file__).parents[2] / "cumulus_library/studies/core",
        data_path=tmp_path / "export",
    )
    export_path = tmp_path / "export/core/"
    export_path.mkdir(exist_ok=True, parents=True)
    with open(export_path / "to_be_deleted.file", "w") as f:
        f.write("foo")
    exporter.export_study(
        config=config,
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
    archive.extract("manifest.toml", tmp_path)
    with open(tmp_path / "manifest.toml", "rb") as file:
        manifest = tomllib.load(file)
    assert manifest["study_prefix"] == "core"
