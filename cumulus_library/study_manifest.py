"""Class for loading study configuration data from manifest.toml files"""

import dataclasses
import pathlib
import re
import shutil
import subprocess
import sys
import tomllib

from cumulus_library import errors


@dataclasses.dataclass(kw_only=True)
class ManifestExport:
    name: str
    export_type: str


class StudyManifest:
    """Handles interactions with study directories and manifest files"""

    PREFIX_REGEX = re.compile(r"[a-zA-Z][a-zA-Z0-9_]*")

    def __init__(
        self,
        study_path: pathlib.Path | None = None,
        data_path: pathlib.Path | None = None,
        *,
        options: dict[str, str] | None = None,
    ):
        """Instantiates a StudyManifest.

        :param study_path: A Path object pointing to the dir of the manifest, optional
        :param data_path: A Path object pointing to the dir to save/load ancillary files from,
            optional
        :param options: Command line study-specific options for dynamic manifest values, optional
        """
        self._study_prefix = None
        self._study_path = None
        self._study_config = {}
        if study_path is not None:
            self._load_study_manifest(study_path, options or {})
        self.data_path = data_path

    def __repr__(self):
        return str(self._study_config)

    ### toml parsing helper functions
    def _load_study_manifest(self, study_path: pathlib.Path, options: dict[str, str]) -> None:
        """Reads in a config object from a directory containing a manifest.toml

        :param study_path: A pathlib.Path object pointing to a study directory
        :param options: Command line study-specific options (--option=A:B)
        :raises StudyManifestParsingError: the manifest.toml is malformed or missing.
        """
        try:
            with open(f"{study_path}/manifest.toml", "rb") as file:
                config = tomllib.load(file)
        except FileNotFoundError as e:
            raise errors.StudyManifestFilesystemError(
                f"Missing or invalid manifest found at {study_path}"
            ) from e
        except tomllib.TOMLDecodeError as e:
            # just unify the error classes for convenience of catching them
            raise errors.StudyManifestParsingError(str(e)) from e

        self._study_config = config
        self._study_path = study_path

        if dynamic_study_prefix := config.get("dynamic_study_prefix"):
            self._study_prefix = self._run_dynamic_script(dynamic_study_prefix, options)
        elif config.get("study_prefix") and isinstance(config["study_prefix"], str):
            self._study_prefix = config["study_prefix"]

        if not self._study_prefix or not re.fullmatch(self.PREFIX_REGEX, self._study_prefix):
            raise errors.StudyManifestParsingError(f"Invalid prefix in manifest at {study_path}")
        self._study_prefix = self._study_prefix.lower()

    def get_study_prefix(self) -> str | None:
        """Reads the name of a study prefix from the in-memory study config

        :returns: A string of the prefix in the manifest, or None if not found
        """
        return self._study_prefix

    def get_dedicated_schema(self) -> str | None:
        """Reads the contents of the dedicated schema in the options dict

        :returns: A dictionary of objects, or None if not found
        """
        options = self._study_config.get("advanced_options", {})
        return options.get("dedicated_schema")

    def get_file_list(self, continue_from: str | None = None) -> list[str | dict] | None:
        """Reads the contents of the file_config array or dict from the manifest

        :returns: An array of files from the manifest, or None if not found.
        """
        if "file_config" not in self._study_config:
            raise errors.StudyManifestParsingError(
                "The study manifest does not contain a [file_config] key.\n"
                "This may mean that your study contains an older version the manifest.\n "
                "Please update your manifest to convert keys like 'sql_config' and "
                "'table_builder_config to be in one of the valid file config formats.\n"
                "For more details, consult the library docs at "
                "https://docs.smarthealthit.org/cumulus/library/"
            )
        config = self._study_config.get("file_config")
        items = config.get("file_names", []) or []
        if continue_from:
            if isinstance(items, dict):
                for item in list(items.keys()):
                    if continue_from.split(".", 1)[0] in [x.split(".", 1)[0] for x in items[item]]:
                        return items
                    items.pop(item)
                raise errors.StudyManifestParsingError(f"No files matching '{continue_from}' found")
            else:
                for pos, item in enumerate(items):
                    if continue_from.split(".", 1)[0] == item.split(".", 1)[0]:
                        items = items[pos:]
                        return items
                raise errors.StudyManifestParsingError(f"No files matching '{continue_from}' found")
        return items

    def get_export_table_list(self) -> list[ManifestExport] | None:
        """Reads the contents of the export_list array from the manifest

        :returns: An array of tuples (table, export type) to export from the manifest,
        or None if not found.
        """
        export_config = self._study_config.get("export_config", {})
        export_table_list = []

        for section in [
            ("export_list", "cube"),
            ("count_list", "cube"),
            ("flat_list", "flat"),
            ("meta_list", "meta"),
            ("annotated_count_list", "annotated_cube"),
        ]:
            section_list = export_config.get(section[0], []) or []
            for table in section_list:
                if table.startswith(f"{self.get_study_prefix()}__"):
                    export_table_list.append(ManifestExport(name=table, export_type=section[1]))
                elif "__" in table:  # has a prefix, just the wrong one
                    raise errors.StudyManifestParsingError(
                        f"{table} in export list does not start with prefix "
                        f"{self.get_study_prefix()}__ - check your manifest file."
                    )
                else:
                    # Add the prefix for them (helpful in dynamic prefix cases where the prefix
                    # is not known ahead of time)
                    export_table_list.append(
                        ManifestExport(
                            name=f"{self.get_study_prefix()}__{table}", export_type=section[1]
                        )
                    )
        found_name = set()
        for export in export_table_list:
            if export.name in found_name:
                raise errors.StudyManifestParsingError(
                    f"Table {export.name} is defined in multiple export sections. Tables should "
                    "only be defined in one export section in the study manifest"
                )
            else:
                found_name.add(export.name)
        return export_table_list

    def get_all_files(self, file_type: str):
        """Convenience method for getting files of a type from a manifest"""
        files = self.get_file_list()

        if isinstance(files, dict):
            filtered_files = []
            for key, value in files.items():
                key_files = [file for file in value if file.endswith(file_type)]
                filtered_files = filtered_files + key_files
            return filtered_files
        return [file for file in files if file.endswith(file_type)]

    def get_all_generators(self) -> list[str]:
        """Convenience method for getting builder-based files"""
        return self.get_all_files(".py")

    def get_all_workflows(self) -> list[str]:
        """Convenience method for getting workflow config files"""
        return self.get_all_files(".toml")

    def get_prefix_with_seperator(self) -> str:
        """Convenience method for getting the appropriate prefix for tables"""
        if dedicated := self.get_dedicated_schema():
            return f"{dedicated}."
        return f"{self.get_study_prefix()}__"

    def write_manifest(self, path: pathlib.Path):
        """Writes a copy of the manifest to the provided path.

        A manifest is expected to be a read only artifact, so the intent of this is
        to get a copy of the manifest at runtime and copy it for purposes of exporting
        as part of an upload.
        """
        shutil.copy(
            self._study_path / "manifest.toml", path / f"{self._study_prefix}/manifest.toml"
        )

    ### Dynamic Python code support

    def _run_dynamic_script(self, filename: str, options: dict[str, str]) -> str:
        if not sys.executable:
            raise RuntimeError("Unknown Python executable, dynamic manifest values not supported")

        full_path = f"{self._study_path}/{filename}"
        option_args = [f"--{key}={value}" for key, value in options.items()]
        result = subprocess.run(  # noqa: S603
            [sys.executable, full_path, *option_args],
            check=True,
            capture_output=True,
        )

        return result.stdout.decode("utf8").strip()
