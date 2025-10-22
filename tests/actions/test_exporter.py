import os
import pathlib
import zipfile
from unittest import mock

from cumulus_library import study_manifest
from cumulus_library.actions import (
    exporter,
)


@mock.patch.dict(
    os.environ,
    clear=True,
)
def test_export_study(tmp_path, mock_db_core_config):
    manifest = study_manifest.StudyManifest(
        pathlib.Path(__file__).parents[2] / "cumulus_library/studies/core",
        data_path=tmp_path / "export",
    )
    export_path = tmp_path / "export/core/"
    export_path.mkdir(exist_ok=True, parents=True)
    with open(export_path / "to_be_deleted.file", "w") as f:
        f.write("foo")
    exporter.export_study(
        config=mock_db_core_config,
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
