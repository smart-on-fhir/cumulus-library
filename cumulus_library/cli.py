#!/usr/bin/env python3
"""Utility for building/retrieving data views in AWS Athena"""

import copy
import json
import os
import pathlib
import sys
import sysconfig

import requests
import rich

from cumulus_library import (
    __version__,
    base_utils,
    cli_parser,
    databases,
    enums,
    errors,
    log_utils,
    study_manifest,
)
from cumulus_library.actions import (
    builder,
    cleaner,
    exporter,
    file_generator,
    importer,
    uploader,
)


class StudyRunner:
    """Class for managing cursors and invoking actions"""

    verbose = False
    schema_name = None

    def __init__(self, config: base_utils.StudyConfig, data_path: str):
        self.config = config
        self.data_path = data_path

    def get_config(self, manifest=study_manifest.StudyManifest):
        schema = base_utils.get_schema(self.config, manifest)
        if schema == self.config.schema:
            return self.config
        else:
            config = copy.copy(self.config)
            config.schema = schema
            return config

    ### Creating studies

    def clean_study(
        self,
        targets: list[str],
        study_dict: dict,
        *,
        prefix: bool = False,
    ) -> None:
        """Removes study table/views from Athena.

        While this is usually not required, since it it done as part of a build,
        this can be useful for cleaning up tables if a study prefix is changed
        for some reason.

        :param target: The study prefix to use for IDing tables to remove
        :param study_dict: The dictionary of available study targets
        :keyword prefix: If True, does a search by string prefix in place of study name
        """
        if targets is None:
            sys.exit(
                "Explicit targets for cleaning not provided. "
                "Provide one or more explicit study prefixes to remove."
            )

        for target in targets:
            if prefix:
                manifest = study_manifest.StudyManifest()
                cleaner.clean_study(
                    config=self.get_config(manifest), manifest=manifest, prefix=target
                )
            else:
                manifest = study_manifest.StudyManifest(study_dict[target])
                cleaner.clean_study(config=self.get_config(manifest), manifest=manifest)

    def clean_and_build_study(
        self,
        target: pathlib.Path,
        *,
        continue_from: str | None = None,
    ) -> None:
        """Recreates study views/tables

        :param target: A path to the study directory
        :keyword continue_from: Restart a run from a specific sql file (for dev only)
        """
        manifest = study_manifest.StudyManifest(target, self.data_path)
        try:
            builder.run_protected_table_builder(config=self.get_config(manifest), manifest=manifest)
            if not continue_from:
                log_utils.log_transaction(
                    config=self.get_config(manifest),
                    manifest=manifest,
                    status=enums.LogStatuses.STARTED,
                )
                cleaner.clean_study(
                    config=self.get_config(manifest),
                    manifest=manifest,
                )
                builder.run_table_builder(config=self.get_config(manifest), manifest=manifest)

            else:
                log_utils.log_transaction(
                    config=self.get_config(manifest),
                    manifest=manifest,
                    status=enums.LogStatuses.RESUMED,
                )

            builder.build_study(
                config=self.get_config(manifest),
                manifest=manifest,
                continue_from=continue_from,
            )
            builder.run_counts_builders(config=self.get_config(manifest), manifest=manifest)
            builder.run_statistics_builders(
                config=self.get_config(manifest),
                manifest=manifest,
            )
            log_utils.log_transaction(
                config=self.get_config(manifest),
                manifest=manifest,
                status=enums.LogStatuses.FINISHED,
            )

        except errors.StudyManifestFilesystemError as e:
            # This should be thrown prior to any database connections, so
            # skipping logging
            raise e  # pragma: no cover
        except Exception as e:
            log_utils.log_transaction(
                config=self.get_config(manifest),
                manifest=manifest,
                status=enums.LogStatuses.ERROR,
            )
            raise e

    def run_matching_table_builder(
        self,
        target: pathlib.Path,
        table_builder_name: str,
    ) -> None:
        """Runs a single table builder

        :param target: A path to the study directory
        :param table_builder_name: a builder file referenced in the study's manifest
        """
        manifest = study_manifest.StudyManifest(target)
        builder.run_matching_table_builder(
            config=self.get_config(manifest),
            manifest=manifest,
            builder=table_builder_name,
        )

    ### Data exporters
    def export_study(self, target: pathlib.Path, data_path: pathlib.Path, archive: bool) -> None:
        """Exports aggregates defined in a manifest

        :param target: A path to the study directory
        :param datapath: The location to export data to
        :param archive: If true, will export all tables, otherwise uses manifest list
        """
        manifest = study_manifest.StudyManifest(target, data_path)
        exporter.export_study(
            config=self.get_config(manifest),
            manifest=manifest,
            data_path=data_path,
            archive=archive,
        )

    def generate_study_sql(
        self,
        target: pathlib.Path,
        *,
        builder: str | None = None,
    ) -> None:
        """Materializes study sql from templates

        :param target: A path to the study directory
        :keyword builder: Specify a single builder to generate sql from
        """
        manifest = study_manifest.StudyManifest(target)
        file_generator.run_generate_sql(config=self.get_config(manifest), manifest=manifest)

    def generate_study_markdown(
        self,
        target: pathlib.Path,
    ) -> None:
        """Materializes study sql from templates

        :param target: A path to the study directory
        """
        manifest = study_manifest.StudyManifest(target)
        file_generator.run_generate_markdown(config=self.get_config(manifest), manifest=manifest)


def get_abs_path(path: str) -> pathlib.Path:
    """Convenience method for handling abs vs rel paths"""
    if path[0] == "/":
        return pathlib.Path(path)
    else:
        return pathlib.Path(pathlib.Path.cwd(), path)


def get_study_dict(alt_dir_paths: list) -> dict[str, pathlib.Path] | None:
    """Gets valid study targets from ./studies/, and any pip installed studies

    :returns: A list of Path objects
    """
    manifest_studies = {}
    cli_path = pathlib.Path(__file__).resolve().parents[0]

    # first, we'll get any installed public studies
    with open(
        pathlib.Path(cli_path, "./module_allowlist.json"), encoding="utf-8"
    ) as study_allowlist_json:
        study_allowlist = json.load(study_allowlist_json)["allowlist"]
    site_packages_dir = sysconfig.get_path("purelib")
    for study, subdir in study_allowlist.items():
        study_path = pathlib.Path(site_packages_dir, subdir)
        if study_path.exists():
            manifest_studies[study] = study_path

    # then we'll get all studies inside the project directory, followed by
    # any user supplied paths last. These take precedence.
    paths = [pathlib.Path(cli_path, "studies")]
    if alt_dir_paths is not None:
        paths = paths + alt_dir_paths
    for path in paths:
        found_studies = get_studies_by_manifest_path(path)
        manifest_studies.update(found_studies)
    return manifest_studies


def get_studies_by_manifest_path(path: pathlib.Path) -> dict[str, pathlib.Path]:
    """Recursively search for manifest.toml files from a given path"""
    manifest_paths = {}
    for child_path in path.iterdir():
        if child_path.is_dir():
            manifest_paths.update(get_studies_by_manifest_path(child_path))
        elif child_path.name == "manifest.toml":
            try:
                manifest = study_manifest.StudyManifest(path)
                manifest_paths[manifest.get_study_prefix()] = path
            except errors.StudyManifestParsingError as exc:
                rich.print(f"[bold red] Ignoring study in '{path}': {exc}")
    return manifest_paths


def run_cli(args: dict):
    """Controls which library tasks are run based on CLI arguments"""
    console = rich.console.Console()
    if args["action"] == "upload":
        try:
            uploader.upload_files(args)
        except requests.RequestException as e:
            print(str(e))
            sys.exit()

    # all other actions require connecting to the database
    else:
        # if we're targeting a builder explicitly, we always want to
        # run in drop mode
        if builder := args.get("builder"):
            drop_table = builder
        else:
            drop_table = args.get("drop_table")
        db, schema = databases.create_db_backend(args)
        config = base_utils.StudyConfig(
            db=db,
            schema=schema,
            drop_table=drop_table,
            force_upload=args.get("replace_existing", False),
            verbose=args.get("verbose"),
            stats_build=args.get("stats_build", False),
            stats_clean=args.get("stats_clean", False),
            umls_key=args.get("umls_key"),
            options=args.get("options"),
        )
        try:
            runner = StudyRunner(config, data_path=args.get("data_path"))
            console.print("[italic] Connecting to database...")
            runner.config.db.cursor().execute("SHOW DATABASES")
            study_dict = get_study_dict(args.get("study_dir"))
            if "prefix" not in args.keys():
                if args.get("target"):
                    for target in args["target"]:
                        if target not in study_dict:
                            sys.exit(
                                f"{target} was not found in available studies: "
                                f"{list(study_dict.keys())}.\n\n"
                                "If you are trying to run a custom study, make sure "
                                "you include `-s path/to/study/dir` as an argument."
                            )
            if args["action"] == "clean":
                runner.clean_study(
                    targets=args["target"],
                    study_dict=study_dict,
                    prefix=args["prefix"],
                )
            elif args["action"] == "build":
                for target in args["target"]:
                    if args["builder"]:
                        runner.run_matching_table_builder(study_dict[target], args["builder"])
                    else:
                        runner.clean_and_build_study(
                            study_dict[target],
                            continue_from=args["continue_from"],
                        )

            elif args["action"] == "export":
                if args["archive"]:
                    warning_text = (
                        "🚨[bold red] This will export all study tables [/bold red]🚨"
                        "\n\nDepending on your study definition, this data may contain "
                        "data that would be characterized as a [italic]limited data "
                        "set[/italic], primarily dates, on a per patient level.\n\n"
                        "[bold]By doing this, you are assuming the responsibility for "
                        "meeting your organization's security requirements for "
                        "storing this data in a secure manner.[/bold]\n\n"
                        "Type Y to proceed, or any other value to quit.\n"
                    )
                    console.print(warning_text)
                    response = input()
                    if response.lower() != "y":
                        sys.exit()
                for target in args["target"]:
                    runner.export_study(study_dict[target], args["data_path"], args["archive"])

            elif args["action"] == "import":
                for archive in args["archive_path"]:
                    archive = get_abs_path(archive)
                    importer.import_archive(config, archive_path=archive)

            elif args["action"] == "generate-sql":
                for target in args["target"]:
                    runner.generate_study_sql(study_dict[target], builder=args["builder"])

            elif args["action"] == "generate-md":
                for target in args["target"]:
                    runner.generate_study_markdown(study_dict[target])
        finally:
            config.db.close()
        # For unit testing only
        return config


def main(cli_args=None):
    """Reads CLI input/environment variables and invokes library calls"""

    parser = cli_parser.get_parser()
    args = vars(parser.parse_args(cli_args))
    if args["version"]:
        print(__version__)
        sys.exit(0)
    if args["action"] is None:
        parser.print_usage()
        sys.exit(1)

    arg_env_pairs = (
        ("data_path", "CUMULUS_LIBRARY_DATA_PATH"),
        ("db_type", "CUMULUS_LIBRARY_DB_TYPE"),
        ("id", "CUMULUS_AGGREGATOR_ID"),
        ("load_ndjson_dir", "CUMULUS_LIBRARY_LOAD_NDJSON_DIR"),
        ("profile", "CUMULUS_LIBRARY_PROFILE"),
        ("region", "CUMULUS_LIBRARY_REGION"),
        ("schema_name", "CUMULUS_LIBRARY_SCHEMA_NAME"),
        ("database", "CUMULUS_LIBRARY_DATABASE"),
        ("study_dir", "CUMULUS_LIBRARY_STUDY_DIR"),
        ("umls_key", "UMLS_API_KEY"),
        ("url", "CUMULUS_AGGREGATOR_URL"),
        ("user", "CUMULUS_AGGREGATOR_USER"),
        ("work_group", "CUMULUS_LIBRARY_WORKGROUP"),
    )
    read_env_vars = []
    for pair in arg_env_pairs:
        if env_val := os.environ.get(pair[1]):
            if pair[0] == "study_dir":
                args[pair[0]] = [env_val]
            else:
                args[pair[0]] = env_val
            read_env_vars.append([pair[1], env_val])

    if len(read_env_vars) > 0:
        table = rich.table.Table(title="Values read from environment variables")
        table.add_column("Environment Variable", style="green")
        table.add_column("Value", style="cyan")
        for row in read_env_vars:
            if row[0] == "CUMULUS_AGGREGATOR_ID":
                table.add_row(row[0], "#########")  # pragma: no cover
            else:
                table.add_row(row[0], row[1])
        console = rich.console.Console()
        console.print(table)

    options = {}
    cli_options = args.get("options") or []
    for c_arg in cli_options:
        c_arg = c_arg.split(":", 2)
        if len(c_arg) == 1:
            sys.exit(
                f"Custom argument '{c_arg}' is not validly formatted.\n"
                "Custom arguments should be of the form 'argname:value'."
            )
        options[c_arg[0]] = c_arg[1]
    args["options"] = options

    if args.get("data_path"):
        args["data_path"] = get_abs_path(args["data_path"])

    if args.get("study_dir"):
        posix_paths = []
        for path in args["study_dir"]:
            posix_paths.append(get_abs_path(path))
        args["study_dir"] = posix_paths

    return run_cli(args)


def main_cli():  # called by the generated wrapper scripts
    main()  # pragma: no cover


if __name__ == "__main__":
    main()  # pragma: no cover
