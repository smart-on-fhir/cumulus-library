"""Class for loading study configuration data from manifest.toml files"""

import dataclasses
import pathlib
import re
import shutil
import subprocess
import sys
import tomllib

import msgspec

from cumulus_library import enums, errors


@dataclasses.dataclass(kw_only=True)
class ManifestExport:
    name: str
    export_type: str


# These msgspec Structs define the expected formats for manifests & submanifests


class ManifestAction(msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True):
    type: str | None = None
    description: str | None = None
    label: str | None = None
    files: list[str] | None = None
    tables: list[str] | None = None


class ManifestAdvancedOptions(msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True):
    dedicated_schema: str | None = None
    dynamic_study_prefix: str | None = None


class ManifestConfig(msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True):
    study_prefix: str | None = None
    description: str | None = None
    stages: dict[str, list[ManifestAction]] | None = None
    advanced_options: ManifestAdvancedOptions | None = None


class SubmanifestConfig(msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True):
    actions: list[ManifestAction]


class StudyManifest:
    """Handles interactions with study directories and manifest files"""

    PREFIX_REGEX = re.compile(r"[a-zA-Z][a-zA-Z0-9_]*")
    VALID_ACTIONS = ("serial", "parallel", "submanifest")

    def __init__(
        self,
        study_path: pathlib.Path | str | None = None,
        data_path: pathlib.Path | str | None = None,
        *,
        options: dict[str, str] | None = None,
    ):
        """Instantiates a StudyManifest.

        Note: while you can supply a custom path to load an arbitrary toml file, only a file
        named 'manifest.toml' will be detected automatically. Other filenames can be used
        for experimentation, or for rendering out artifacts that can then be packaged as part
        of a study for distribution.

        :param study_path: A Path object pointing to the directory, or a toml path, optional
        :param data_path: A Path object pointing to the dir to save/load ancillary files from,
            optional
        :param options: Command line study-specific options for dynamic manifest values, optional
        """
        self._study_prefix = None
        self._study_path = None
        self.data_path = None
        self._study_config = {}
        self._has_stats = False
        if study_path is not None:
            self._load_study_manifest(pathlib.Path(study_path), options or {})
        if data_path is not None:
            self.data_path = pathlib.Path(data_path)

    def __repr__(self):  # pragma: no cover
        return str(self._study_config)

    def _read_toml(self, path, doc_type) -> dict:
        """Runs a toml file through a format checker and returns a python dict"""
        try:
            with open(path, "rb") as file:
                file_bytes = file.read()
                return msgspec.to_builtins(msgspec.toml.decode(file_bytes, type=doc_type))
        except FileNotFoundError as e:
            raise errors.StudyManifestFilesystemError(
                f"Missing or invalid manifest found at {path}"
            ) from e
        except (msgspec.ValidationError, msgspec.DecodeError) as e:
            raise errors.StudyManifestParsingError(
                f"Manifest formatting error at path {path}: {e!s}. \n"
                "You may be using an different version of a study manifest. See"
                "https://docs.smarthealthit.org/cumulus/library/study-configuration.html "
                "for more information about what fields are expected in a manifest."
            )

    def _validate_action(self, action: dict, source: pathlib.Path):
        action_type = action.get("type")
        if action_type is not None and action_type not in [e.value for e in enums.ManifestActions]:
            raise errors.StudyManifestParsingError(
                f"Action type '{action_type}' in {source} is not a valid action.\n"
                f"Valid action types: {', '.join(enums.ManifestActions)}."
            )
        if action_type is None or action_type.startswith("build:"):
            if action.get("files") is None:
                raise errors.StudyManifestParsingError(
                    f"The following action in {source} is missing an expected key, 'files':\n"
                    f"{action}"
                )
        elif action_type.startswith("export:"):
            if action.get("tables") is None:
                raise errors.StudyManifestParsingError(
                    f"The following action in {source} is missing an expected key, 'tables':\n"
                    f"{action}"
                )

    def _has_stats_workflows(self) -> bool:
        for stage in self._study_config.get("stages", {}).values():
            for action in stage:
                for file in action.get("files", []):
                    if file.endswith(".toml") or file.endswith(".workflow"):
                        with open(self._study_path / file, "rb") as f:
                            workflow_conf = tomllib.load(f)
                            if workflow_conf["config_type"] in [
                                x.value for x in enums.StatisticsTypes
                            ]:
                                return True
        return False

    def _format_action(self, action):
        """Handles optional param coercion for actions"""
        if action.get("type") is None:
            action["type"] = "build:serial"
        if action.get("label") is None and action.get("description") is not None:
            action["label"] = action["description"]
        return action

    ### toml parsing helper functions
    def _load_study_manifest(self, study_path: pathlib.Path, options: dict[str, str]) -> None:
        """Reads in a config object from a directory containing a manifest.toml

        :param study_path: A pathlib.Path object pointing to a study directory or a toml file
        :param options: Command line study-specific options (--option=A:B)
        :raises StudyManifestParsingError: the manifest.toml is malformed or missing.
        """
        if study_path.is_dir():
            study_path = study_path / "manifest.toml"
        elif not study_path.name.endswith(".toml"):  # pragma: no cover
            raise errors.CumulusLibraryError(f"{study_path} is not a valid toml file")
        config = self._read_toml(study_path, ManifestConfig)

        defined_stages = list(config.get("stages", {}).keys())

        if len(defined_stages) == 0:
            raise errors.StudyManifestParsingError(
                f"{study_path} does not contain any stage definitions.\n"
                "See https://docs.smarthealthit.org/cumulus/library/study-configuration.html "
                "for more details about creating manifests."
            )

        # If we're importing from submanifest, we'll do an inline swap out of the submanifest for
        # the actions defined in the submanifest.
        # This means we can assume everywhere else that we don't need to worry about submanifests.
        all_actions = []
        for stage in defined_stages:
            if stage == "all":
                raise errors.StudyManifestParsingError(
                    "'all' is a reserved word for stage name. Please select a different name."
                )
            actions = []
            for action in config["stages"][stage]:
                if action.get("type") == "submanifest":
                    for submanifest in action.get("files", []):
                        subconfig = self._read_toml(
                            study_path.parent / submanifest, SubmanifestConfig
                        )
                        for subaction in subconfig.get("actions"):
                            self._validate_action(subaction, study_path.parent / submanifest)
                            actions.append(self._format_action(subaction))
                            all_actions.append(subaction)
                else:
                    self._validate_action(action, study_path)
                    actions.append(self._format_action(action))
                    all_actions.append(action)
            config["stages"][stage] = actions

        # if there isn't a default stage, we'll assume that we should run the first one
        if "default" not in defined_stages:
            config["stages"]["default"] = config["stages"][defined_stages[0]]

        config["stages"]["all"] = all_actions

        # We'll set these to class vars now so we can reuse the class getters
        # to finish setup and validation
        self._study_config = config
        self._study_path = study_path.parent

        if dynamic_study_prefix := config.get("advanced_options", {}).get("dynamic_study_prefix"):
            self._study_prefix = self._run_dynamic_script(dynamic_study_prefix, options)
        elif config.get("study_prefix") and isinstance(config["study_prefix"], str):
            self._study_prefix = config["study_prefix"]

        if not self._study_prefix or not re.fullmatch(self.PREFIX_REGEX, self._study_prefix):
            raise errors.StudyManifestParsingError(f"Invalid prefix in manifest at {study_path}")
        self._study_prefix = self._study_prefix.lower()

        # Finally, let's see if we're using a sampling workflow

        self._has_stats = self._has_stats_workflows()

    def get_study_prefix(self) -> str | None:
        """Reads the name of a study prefix from the in-memory study config

        :returns: A string of the prefix in the manifest, or None if not found
        """
        return self._study_prefix

    def get_formatted_study_prefix(self) -> str | None:
        """Returns the appropriately formatted value for a study prefix"""
        if dedicated := self._study_config.get("advanced_options", {}).get("dedicated_schema"):
            return f"{dedicated}."
        return f"{self._study_prefix}__"

    def get_dedicated_schema(self) -> str | None:
        """Reads the contents of the dedicated schema in the options dict

        :returns: A dictionary of objects, or None if not found
        """
        options = self._study_config.get("advanced_options", {})
        return options.get("dedicated_schema")

    def get_stages(self) -> list:
        """Returns the names of all stages defined in the manifest"""
        return list(self._study_config.get("stages", {}).keys())

    def get_stage(self, stage) -> list:
        """Returns the contents of the specified stage"""
        return self._study_config.get("stages", {}).get(stage, [])

    def get_file_list(
        self,
        stage_name: str = "default",
        continue_from: str | None = None,
    ) -> list[str | dict] | None:
        """Reads the contents of the file_config array or dict from the manifest
        :param stage_name: the stage_name to get the files of ('default' if not specified)
        :continue_from: a specific file to pick up a build from.
        :returns: An array of files from the manifest
        :raises StudyManifestParsingError: If the file in continue_from does not exist
        """
        files = []
        found_continue_point = False
        stage = self.get_stage(stage_name)
        for action in stage:
            if not action.get("type", "").startswith("build:"):
                continue
            if continue_from and not found_continue_point:
                file_stems = [x.split(".", 1)[0] for x in action["files"]]
                if continue_from.split(".", 1)[0] in file_stems:
                    found_continue_point = True
                    if action.get("type") == enums.ManifestActions.PARALLEL.value:
                        files = files + action["files"]
                    else:
                        pos = file_stems.index(continue_from.split(".", 1)[0])
                        files = files + action["files"][pos:]
            else:
                files = files + action["files"]
        if continue_from and len(files) == 0:
            raise errors.StudyManifestParsingError(f"No files matching '{continue_from}' found")
        return files

    def get_export_table_list(self, stage_name: str = "default") -> list[ManifestExport] | None:
        """Reads the exportable tables from the manifest

        :param stage_name: if present, just search the stages defined in the stage_name list
        :returns: An array of tuples (table, export type) to export from the manifest,
        or None if not found.
        """
        export_table_list = []
        stage = self.get_stage(stage_name)

        for action in stage:
            if not action.get("type", "").startswith("export:"):
                continue
            export_type = action["type"].split(":")[1]
            if export_type == "counts":
                # the aggregator is looking for the cube keyword, so we'll swap it out
                export_type = "cube"
            for table in action.get("tables", []):
                if table.startswith(f"{self.get_study_prefix()}__"):
                    export_table_list.append(ManifestExport(name=table, export_type=export_type))
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
                            name=f"{self.get_study_prefix()}__{table}", export_type=export_type
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

    def has_stats(self):
        return self._has_stats

    def get_all_files(self, file_type: str, stage_name: str = "default"):
        """Convenience method for getting files of a type from a manifest"""
        files = self.get_file_list(stage_name)
        return [file for file in files if file.endswith(file_type)]

    def get_all_generators(self, stage_name: str = "default") -> list[str]:
        """Convenience method for getting builder-based files"""
        return self.get_all_files(".py", stage_name)

    def get_all_workflows(self, stage_name: str = "default") -> list[str]:
        """Convenience method for getting workflow config files"""
        return self.get_all_files(".toml", stage_name)

    def get_prefix_with_seperator(self) -> str:
        """Convenience method for getting the appropriate prefix for tables"""
        if dedicated := self.get_dedicated_schema():
            return f"{dedicated}."
        return f"{self.get_study_prefix()}__"

    def copy_manifest(self, path: pathlib.Path):
        """Writes a copy of the manifest to the provided path.

        The intent of this is to get a copy of the manifest at runtime and copy it
        for purposes of exporting as part of an upload.
        """
        (path / self._study_prefix).mkdir(exist_ok=True, parents=True)
        shutil.copy(
            self._study_path / "manifest.toml", path / f"{self._study_prefix}/manifest.toml"
        )

    def write_manifest(self, path: pathlib.Path):
        if path.is_dir():
            path = path / "manifest.toml"
        path.parent.mkdir(exist_ok=True, parents=True)
        with open(path, "wb") as f:
            # we'll remove the all key we auto created before writing out a copy
            config = self._study_config
            config["stages"].pop("all", None)
            f.write(msgspec.toml.encode(ManifestConfig(self._study_config)))

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
