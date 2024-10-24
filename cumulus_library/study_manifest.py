"""Class for loading study configuration data from manifest.toml files"""

import pathlib
import tomllib

from cumulus_library import errors


class StudyManifest:
    """Handles interactions with study directories and manifest files"""

    def __init__(
        self,
        study_path: pathlib.Path | None = None,
        data_path: pathlib.Path | None = None,
    ):
        """Instantiates a StudyManifest.

        :param study_path: A Path object pointing to the dir of the manifest, optional
        :param data_path: A Path object pointing to the dir to save/load ancillary files from,
            optional
        """
        self._study_path = None
        self._study_config = {}
        if study_path is not None:
            self.load_study_manifest(study_path)
        self.data_path = data_path

    def __repr__(self):
        return str(self._study_config)

    ### toml parsing helper functions
    def load_study_manifest(self, study_path: pathlib.Path) -> None:
        """Reads in a config object from a directory containing a manifest.toml

        :param study_path: A pathlib.Path object pointing to a study directory
        :raises StudyManifestParsingError: the manifest.toml is malformed or missing.
        """
        try:
            with open(f"{study_path}/manifest.toml", "rb") as file:
                config = tomllib.load(file)
                if not config.get("study_prefix") or not isinstance(config["study_prefix"], str):
                    raise errors.StudyManifestParsingError(
                        f"Invalid prefix in manifest at {study_path}"
                    )
                self._study_config = config
            self._study_path = study_path
        except FileNotFoundError as e:
            raise errors.StudyManifestFilesystemError(
                f"Missing or invalid manifest found at {study_path}"
            ) from e
        except tomllib.TOMLDecodeError as e:
            # just unify the error classes for convenience of catching them
            raise errors.StudyManifestParsingError(str(e)) from e

    def get_study_prefix(self) -> str | None:
        """Reads the name of a study prefix from the in-memory study config

        :returns: A string of the prefix in the manifest, or None if not found
        """
        return self._study_config.get("study_prefix")

    def get_dedicated_schema(self) -> str | None:
        """Reads the contents of the dedicated schema in the options dict

        :returns: A dictionary of objects, or None if not found
        """
        options = self._study_config.get("advanced_options", {})
        return options.get("dedicated_schema")

    def get_sql_file_list(self, continue_from: str | None = None) -> list[str] | None:
        """Reads the contents of the sql_config array from the manifest

        :returns: An array of sql files from the manifest, or None if not found.
        """
        sql_config = self._study_config.get("sql_config", {})
        sql_files = sql_config.get("file_names", []) or []
        if continue_from:
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

    def get_export_table_list(self) -> list[str] | None:
        """Reads the contents of the export_list array from the manifest

        :returns: An array of tables to export from the manifest, or None if not found.
        """
        export_config = self._study_config.get("export_config", {})
        export_table_list = export_config.get("export_list", []) or []
        for table in export_table_list:
            if not table.startswith(f"{self.get_study_prefix()}__"):
                raise errors.StudyManifestParsingError(
                    f"{table} in export list does not start with prefix "
                    f"{self.get_study_prefix()}__ - check your manifest file."
                )
        return export_table_list

    def get_all_generators(self) -> list[str]:
        """Convenience method for getting files that generate sql queries"""
        return (
            self.get_table_builder_file_list()
            + self.get_counts_builder_file_list()
            + self.get_statistics_file_list()
        )

    def get_prefix_with_seperator(self) -> str:
        """Convenience method for getting the appropriate prefix for tables"""
        if dedicated := self.get_dedicated_schema():
            return f"{dedicated}."
        return f"{self.get_study_prefix()}__"
