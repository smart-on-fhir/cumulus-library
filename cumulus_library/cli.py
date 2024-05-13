#!/usr/bin/env python3
"""Utility for building/retrieving data views in AWS Athena"""

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
    protected_table_builder,
    study_parser,
    upload,
)
from cumulus_library.template_sql import base_templates


class StudyRunner:
    """Class for managing cursors and executing Cumulus queries"""

    verbose = False
    schema_name = None

    def __init__(self, db: databases.DatabaseBackend, data_path: str):
        self.db = db
        self.data_path = data_path
        self.cursor = db.cursor()
        self.schema_name = db.schema_name

    def update_transactions(self, prefix: str, status: str):
        """Adds a record to a study's transactions table"""
        self.cursor.execute(
            base_templates.get_insert_into_query(
                f"{prefix}__{enums.ProtectedTables.TRANSACTIONS.value}",
                protected_table_builder.TRANSACTIONS_COLS,
                [
                    [
                        prefix,
                        __version__,
                        status,
                        base_utils.get_utc_datetime(),
                    ]
                ],
                {"event_time": "TIMESTAMP"},
            )
        )

    ### Creating studies

    def clean_study(
        self,
        targets: list[str],
        study_dict: dict,
        *,
        stats_clean: bool,
        prefix: bool = False,
    ) -> None:
        """Removes study table/views from Athena.

        While this is usually not required, since it it done as part of a build,
        this can be useful for cleaning up tables if a study prefix is changed
        for some reason.

        :param target: The study prefix to use for IDing tables to remove
        :param study_dict: The dictionary of available study targets
        :param stats_clean: If true, removes previous stats runs
        :keyword prefix: If True, does a search by string prefix in place of study name
        """
        if targets is None or targets == ["all"]:
            sys.exit(
                "Explicit targets for cleaning not provided. "
                "Provide one or more explicit study prefixes to remove."
            )
        for target in targets:
            if prefix:
                parser = study_parser.StudyManifestParser()
                parser.clean_study(
                    self.cursor,
                    self.schema_name,
                    verbose=self.verbose,
                    stats_clean=stats_clean,
                    prefix=target,
                )
            else:
                parser = study_parser.StudyManifestParser(study_dict[target])
                parser.clean_study(
                    self.cursor,
                    self.schema_name,
                    verbose=self.verbose,
                    stats_clean=stats_clean,
                )

    def clean_and_build_study(
        self,
        target: pathlib.Path,
        *,
        config: base_utils.StudyConfig,
        continue_from: str | None = None,
    ) -> None:
        """Recreates study views/tables

        :param target: A path to the study directory
        :param config: A StudyConfig object containing optional params
        :keyword continue_from: Restart a run from a specific sql file (for dev only)
        """
        studyparser = study_parser.StudyManifestParser(target, self.data_path)
        try:
            if not continue_from:
                studyparser.run_protected_table_builder(
                    self.cursor, self.schema_name, verbose=self.verbose, config=config
                )
                self.update_transactions(studyparser.get_study_prefix(), "started")
                cleaned_tables = studyparser.clean_study(
                    self.cursor,
                    self.schema_name,
                    verbose=self.verbose,
                    stats_clean=False,
                )
                # If the study hasn't been created before, force stats table generation
                if len(cleaned_tables) == 0:
                    config.stats_build = True
                studyparser.run_table_builder(
                    self.cursor,
                    self.schema_name,
                    verbose=self.verbose,
                    parser=self.db.parser(),
                    config=config,
                )
            else:
                self.update_transactions(studyparser.get_study_prefix(), "resumed")

            studyparser.build_study(
                self.cursor,
                verbose=self.verbose,
                continue_from=continue_from,
                config=config,
            )
            studyparser.run_counts_builders(
                self.cursor, self.schema_name, verbose=self.verbose, config=config
            )
            studyparser.run_statistics_builders(
                self.cursor,
                self.schema_name,
                verbose=self.verbose,
                config=config,
            )
            self.update_transactions(studyparser.get_study_prefix(), "finished")

        except errors.StudyManifestFilesystemError as e:
            # This should be thrown prior to any database connections, so
            # skipping logging
            raise e
        except Exception as e:
            self.update_transactions(studyparser.get_study_prefix(), "error")
            raise e

    def run_matching_table_builder(
        self,
        target: pathlib.Path,
        table_builder_name: str,
        config: base_utils.StudyConfig,
    ) -> None:
        """Runs a single table builder

        :param target: A path to the study directory
        :param table_builder_name: a builder file referenced in the study's manifest
        :param config: A StudyConfig object containing optional params
        """
        studyparser = study_parser.StudyManifestParser(target)
        studyparser.run_matching_table_builder(
            self.cursor,
            self.schema_name,
            table_builder_name,
            verbose=self.verbose,
            parser=self.db.parser(),
            config=config,
        )

    def clean_and_build_all(
        self, study_dict: dict, config: base_utils.StudyConfig
    ) -> None:
        """Builds tables for all studies.

        NOTE: By design, this method will always exclude the `template` study dir,
        since 99% of the time you don't need a live copy in the database.

        :param study_dict: A dict of paths
        :param config: A StudyConfig object containing optional params
        """
        study_dict = dict(study_dict)
        study_dict.pop("template")
        for precursor_study in ["vocab", "core"]:
            self.clean_and_build_study(study_dict[precursor_study], config=config)
            study_dict.pop(precursor_study)
        for key in study_dict:
            self.clean_and_build_study(study_dict[key], config=config)

    ### Data exporters
    def export_study(
        self, target: pathlib.Path, data_path: pathlib.Path, archive: bool
    ) -> None:
        """Exports aggregates defined in a manifest

        :param target: A path to the study directory
        """
        if data_path is None:
            sys.exit("Missing destination - please provide a path argument.")
        studyparser = study_parser.StudyManifestParser(target, data_path)
        studyparser.export_study(self.db, self.schema_name, data_path, archive)

    def export_all(self, study_dict: dict, data_path: pathlib.Path, archive: bool):
        """Exports all defined count tables to disk"""
        for key in study_dict.keys():
            self.export_study(study_dict[key], data_path, archive)

    def generate_study_sql(
        self,
        target: pathlib.Path,
        *,
        config: base_utils.StudyConfig,
        builder: str | None = None,
    ) -> None:
        """Materializes study sql from templates

        :param target: A path to the study directory
        :param config: A StudyConfig object containing optional params
        :param builder: Specify a single builder to generate sql from
        """
        studyparser = study_parser.StudyManifestParser(target)
        studyparser.run_generate_sql(
            cursor=self.cursor,
            schema=self.schema_name,
            builder=builder,
            verbose=self.verbose,
            parser=self.db.parser(),
            config=config,
        )

    def generate_study_markdown(
        self,
        target: pathlib.Path,
    ) -> None:
        """Materializes study sql from templates

        :param target: A path to the study directory
        """
        studyparser = study_parser.StudyManifestParser(target)
        studyparser.run_generate_markdown(
            self.cursor,
            self.schema_name,
            verbose=self.verbose,
            parser=self.db.parser(),
        )


def get_abs_posix_path(path: str) -> pathlib.Path:
    """Convenience method for handling abs vs rel paths"""
    if path[0] == "/":
        return pathlib.Path(path)
    else:
        return pathlib.Path(pathlib.Path.cwd(), path)


def create_template(path: str) -> None:
    """Creates a manifest in target dir if one doesn't exist"""
    abs_path = get_abs_posix_path(path)
    manifest_path = pathlib.Path(abs_path, "manifest.toml")
    if manifest_path.exists():
        sys.exit(f"A manifest.toml already exists at {abs_path}, skipping creation")
    abs_path.mkdir(parents=True, exist_ok=True)

    copy_lists = [
        ["studies/template/manifest.toml", "manifest.toml"],
        [".sqlfluff", ".sqlfluff"],
    ]
    for source, dest in copy_lists:
        source_path = pathlib.Path(pathlib.Path(__file__).resolve().parents[0], source)
        dest_path = pathlib.Path(abs_path, dest)
        dest_path.write_bytes(source_path.read_bytes())


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
                manifest = study_parser.StudyManifestParser(path)
                manifest_paths[manifest.get_study_prefix()] = path
            except errors.StudyManifestParsingError as exc:
                rich.print(f"[bold red] Ignoring study in '{path}': {exc}")
    return manifest_paths


def run_cli(args: dict):
    """Controls which library tasks are run based on CLI arguments"""
    console = rich.console.Console()
    if args["action"] == "create":
        create_template(args["create_dir"])

    elif args["action"] == "upload":
        try:
            upload.upload_files(args)
        except requests.RequestException as e:
            print(str(e))
            sys.exit()

    # all other actions require connecting to the database
    else:
        config = base_utils.StudyConfig(
            db=databases.create_db_backend(args),
            force_upload=args.get("replace_existing", False),
            stats_build=args.get("stats_build", False),
            umls_key=args.get("umls_key"),
        )
        try:
            runner = StudyRunner(config.db, data_path=args.get("data_path"))
            if args.get("verbose"):
                runner.verbose = True
            console.print("[italic] Connecting to database...")
            runner.cursor.execute("SHOW DATABASES")
            study_dict = get_study_dict(args["study_dir"])
            if "prefix" not in args.keys():
                if args["target"]:
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
                    args["target"],
                    study_dict,
                    stats_clean=args["stats_clean"],
                    prefix=args["prefix"],
                )
            elif args["action"] == "build":
                if "all" in args["target"]:
                    runner.clean_and_build_all(study_dict, config=config)
                else:
                    for target in args["target"]:
                        if args["builder"]:
                            runner.run_matching_table_builder(
                                study_dict[target], args["builder"], config=config
                            )
                        else:
                            runner.clean_and_build_study(
                                study_dict[target],
                                config=config,
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
                        "storing this data in a secure manager.[/bold]\n\n"
                        "Type Y to proceed, or any other value to quit.\n"
                    )
                    console.print(warning_text)
                    response = input()
                    if response.lower() != "y":
                        sys.exit()
                if "all" in args["target"]:
                    runner.export_all(study_dict, args["data_path"], args["archive"])
                else:
                    for target in args["target"]:
                        runner.export_study(
                            study_dict[target], args["data_path"], args["archive"]
                        )

            elif args["action"] == "generate-sql":
                for target in args["target"]:
                    runner.generate_study_sql(
                        study_dict[target], builder=args["builder"], config=config
                    )

            elif args["action"] == "generate-md":
                for target in args["target"]:
                    runner.generate_study_markdown(study_dict[target])
        finally:
            config.db.close()


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
    if args.get("target"):
        for target in args["target"]:
            if target == "all":
                args["target"] = ["all"]
                break

    arg_env_pairs = (
        ("data_path", "CUMULUS_LIBRARY_DATA_PATH"),
        ("db_type", "CUMULUS_LIBRARY_DB_TYPE"),
        ("id", "CUMULUS_AGGREGATOR_ID"),
        ("load_ndjson_dir", "CUMULUS_LIBRARY_LOAD_NDJSON_DIR"),
        ("profile", "CUMULUS_LIBRARY_PROFILE"),
        ("region", "CUMULUS_LIBRARY_REGION"),
        ("schema_name", "CUMULUS_LIBRARY_DATABASE"),
        ("study_dir", "CUMULUS_LIBRARY_STUDY_DIR"),
        ("umls_key", "UMLS_API_KEY"),
        ("url", "CUMULUS_AGGREGATOR_URL"),
        ("user", "CUMULUS_AGGREGATOR_USER"),
        ("workgroup", "CUMULUS_LIBRARY_WORKGROUP"),
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
                table.add_row(row[0], "#########")
            else:
                table.add_row(row[0], row[1])
        console = rich.console.Console()
        console.print(table)

    if args.get("study_dir"):
        posix_paths = []
        for path in args["study_dir"]:
            posix_paths.append(get_abs_posix_path(path))
        args["study_dir"] = posix_paths

    if args.get("data_path"):
        args["data_path"] = get_abs_posix_path(args["data_path"])
    return run_cli(args)


def main_cli():  # called by the generated wrapper scripts
    main()


if __name__ == "__main__":
    main()
