"""Class for loading study configuration data from manifest.toml files"""

import dataclasses
import pathlib
import re
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

    def get_file_list(self, continue_from: str | None = None) -> list[str] | None:
        """Reads the contents of the file_config array from the manifest

        :returns: An array of files from the manifest, or None if not found.
        """
        config = self._study_config.get("file_config", {})
        files = config.get("file_names", []) or []
        if not files:
            files = (
                self.get_table_builder_file_list()
                + self.get_sql_file_list()
                + self.get_counts_builder_file_list()
                + self.get_statistics_file_list()
            )
        if continue_from:
            for pos, file in enumerate(files):
                if continue_from.split(".", 1)[0] == file.split(".", 1)[0]:
                    files = files[pos:]
                    break
            else:
                raise errors.StudyManifestParsingError(f"No files matching '{continue_from}' found")
        return files

    # The following four functions are considered deprecated, and can be removed
    # after we update studies to use the new methodology
    def get_sql_file_list(self, continue_from: str | None = None) -> list[str] | None:
        """Reads the contents of the sql_config array from the manifest

        :returns: An array of sql files from the manifest, or None if not found.
        """
        sql_config = self._study_config.get("sql_config", {})
        sql_files = sql_config.get("file_names", []) or []
        if continue_from:  # pragma: no cover
            for pos, file in enumerate(sql_files):
                if continue_from.replace(".sql", "") == file.replace(".sql", ""):
                    sql_files = sql_files[pos:]
                    break
            else:
                raise errors.StudyManifestParsingError(
                    f"No tables matching '{continue_from}' found"
                )
        return sql_files

    def get_table_builder_file_list(self) -> list[str] | None:
        """Reads the contents of the table_builder_config array from the manifest

        :returns: An array of sql files from the manifest, or None if not found.
        """
        sql_config = self._study_config.get("table_builder_config", {})
        return sql_config.get("file_names", [])

    def get_counts_builder_file_list(self) -> list[str] | None:
        """Reads the contents of the counts_builder_config array from the manifest

        :returns: An array of sql files from the manifest, or None if not found.
        """
        sql_config = self._study_config.get("counts_builder_config", {})
        return sql_config.get("file_names", [])

    def get_statistics_file_list(self) -> list[str] | None:
        """Reads the contents of the statistics_config array from the manifest

        :returns: An array of statistics toml files from the manifest,
          or None if not found.
        """
        stats_config = self._study_config.get("statistics_config", {})
        return stats_config.get("file_names", [])

    # End of deprecated section

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
