#!/usr/bin/env python3
"""Utility for building/retrieving data views in AWS Athena"""

import json
import os
import sys
import sysconfig

from pathlib import Path, PosixPath
from typing import Dict, List, Optional

import pyathena

from pyathena.pandas.cursor import PandasCursor
from rich.console import Console
from rich.table import Table

from cumulus_library import __version__
from cumulus_library.cli_parser import get_parser
from cumulus_library.study_parser import StudyManifestParser
from cumulus_library.upload import upload_files


# ** Don't delete! **
# This class isn't used in the rest of the code,
# but it is used manually as a quick & dirty alternative to the CLI.
class CumulusEnv:  # pylint: disable=too-few-public-methods
    """
    Wrapper for Cumulus Environment vars.
    Simplifies connections to StudyBuilder without requiring CLI parsing.
    """

    def __init__(self):
        self.region = os.environ.get("CUMULUS_LIBRARY_REGION", "us-east-1")
        self.workgroup = os.environ.get("CUMULUS_LIBRARY_WORKGROUP", "cumulus")
        self.profile = os.environ.get("CUMULUS_LIBRARY_PROFILE")
        self.schema_name = os.environ.get("CUMULUS_LIBRARY_DATABASE")

    def get_study_builder(self):
        """Convenience method for getting athena args from environment"""
        return StudyBuilder(self.region, self.workgroup, self.profile, self.schema_name)


class StudyBuilder:
    """Class for managing Athena cursors and executing Cumulus queries"""

    verbose = False
    schema_name = None

    def __init__(  # pylint: disable=too-many-arguments
        self, region: str, workgroup: str, profile: str, schema: str
    ):
        connect_kwargs = {}
        for aws_env_name in [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
        ]:
            if aws_env_val := os.environ.get(aws_env_name):
                connect_kwargs[aws_env_name.lower()] = aws_env_val
        # the profile may not be required, provided the above three AWS env vars
        # are set. If both are present, the env vars take precedence
        if profile is not None:
            connect_kwargs["profile_name"] = profile
        self.cursor = pyathena.connect(
            region_name=region,
            work_group=workgroup,
            schema_name=schema,
            **connect_kwargs,
        ).cursor()
        self.pandas_cursor = pyathena.connect(
            region_name=region,
            work_group=workgroup,
            schema_name=schema,
            cursor_class=PandasCursor,
            **connect_kwargs,
        ).cursor()
        self.schema_name = schema

    def reset_data_path(self, study: PosixPath) -> None:
        """
        Removes existing exports from a study's local data dir
        """
        project_path = Path(__file__).resolve().parents[1]
        path = Path(f"{str(project_path)}/data_export/{study}/")
        if path.exists():
            for file in path.glob("*"):
                file.unlink()

    ### Creating studies

    def clean_study(self, targets: List[str]) -> None:
        """Removes study table/views from Athena.

        While this is usually not required, since it it done as part of a build,
        this can be useful for cleaning up tables if a study prefix is changed
        for some reason.

        :param target: The study prefix to use for IDing tables to remove"""
        if targets is None or targets == ["all"]:
            sys.exit(
                "Explicit targets for cleaning not provided. "
                "Provide one or more explicit study prefixes to remove."
            )
        for study in targets:
            StudyManifestParser.clean_study(
                self.cursor, self.schema_name, self.verbose, prefix=f"{study}__"
            )

    def clean_and_build_study(self, target: PosixPath) -> None:
        """Recreates study views/tables

        :param target: A PosixPath to the study directory
        """
        studyparser = StudyManifestParser(target)
        studyparser.clean_study(self.cursor, self.schema_name, self.verbose)
        studyparser.run_table_builder(self.cursor, self.schema_name, self.verbose)
        studyparser.build_study(self.cursor, self.verbose)

    def run_single_table_builder(
        self, target: PosixPath, table_builder_name: str
    ) -> None:
        """Runs a single table builder

        :param target: A PosixPath to the study directory
        """
        studyparser = StudyManifestParser(target)
        studyparser.run_single_table_builder(
            self.cursor, self.schema_name, table_builder_name, self.verbose
        )

    def clean_and_build_all(self, study_dict: Dict) -> None:
        """Builds views for all studies.

        NOTE: By design, this method will always exclude the `template` study dir,
        since 99% of the time you don't need a live copy in the database.

        :param study_dict: A dict of PosixPaths
        """
        study_dict = dict(study_dict)
        study_dict.pop("template")
        for precursor_study in ["vocab", "core"]:
            self.clean_and_build_study(study_dict[precursor_study])
            study_dict.pop(precursor_study)
        for key in study_dict:
            self.clean_and_build_study(study_dict[key])

    ### Data exporters
    def export_study(self, target: PosixPath, data_path: PosixPath) -> None:
        """Exports aggregates defined in a manifest

        :param target: A PosixPath to the study directory
        """
        if data_path is None:
            sys.exit("Missing destination - please provide a path argument.")
        studyparser = StudyManifestParser(target)
        studyparser.export_study(self.pandas_cursor, data_path)

    def export_all(self, study_dict: Dict, data_path: PosixPath):
        """Exports all defined count tables to disk"""
        for key in study_dict.keys():
            self.export_study(study_dict[key], data_path)


def get_abs_posix_path(path: str) -> PosixPath:
    """Convenience method for handling abs vs rel paths"""
    if path[0] == "/":
        return Path(path)
    else:
        return Path(Path.cwd(), path)


def create_template(path: str) -> None:
    """Creates a manifest in target dir if one doesn't exist"""
    abs_path = get_abs_posix_path(path)
    manifest_path = Path(abs_path, "manifest.toml")
    if manifest_path.exists():
        sys.exit(f"A manifest.toml already exists at {abs_path}, skipping creation")
    abs_path.mkdir(parents=True, exist_ok=True)

    copy_lists = [
        ["studies/template/manifest.toml", "manifest.toml"],
        [".sqlfluff", ".sqlfluff"],
    ]
    for source, dest in copy_lists:
        source_path = Path(Path(__file__).resolve().parents[0], source)
        dest_path = Path(abs_path, dest)
        dest_path.write_bytes(source_path.read_bytes())


def get_study_dict(alt_dir_paths: List) -> Optional[Dict[str, PosixPath]]:
    """Gets valid study targets from ./studies/, and any pip installed studies

    :returns: A list of pathlib.PosixPath objects
    """
    manifest_studies = {}
    cli_path = Path(__file__).resolve().parents[0]

    # first, we'll get any installed public studies
    with open(
        Path(cli_path, "./module_allowlist.json"), "r", encoding="utf-8"
    ) as study_allowlist_json:
        study_allowlist = json.load(study_allowlist_json)["allowlist"]
    site_packages_dir = sysconfig.get_path("purelib")
    for study, subdir in study_allowlist.items():
        study_path = Path(site_packages_dir, subdir)
        if study_path.exists():
            manifest_studies[study] = study_path

    # then we'll get all studies inside the project directory, followed by
    # any user supplied paths last. These take precedence.
    paths = [Path(cli_path, "studies")]
    if alt_dir_paths is not None:
        paths = paths + alt_dir_paths
    for parent_path in paths:
        for child_path in parent_path.iterdir():
            if child_path.is_dir():
                manifest_studies[child_path.name] = child_path
            elif child_path.name == "manifest.toml":
                manifest_studies[parent_path.name] = parent_path
    return manifest_studies


def run_cli(args: Dict):
    """Controls which library tasks are run based on CLI arguments"""
    if args["action"] == "create":
        create_template(args["create_dir"])

    elif args["action"] == "upload":
        upload_files(args)

    # all other actions require connecting to AWS
    else:
        builder = StudyBuilder(
            args["region"],
            args["workgroup"],
            args["profile"],
            args["schema_name"],
        )
        if args["verbose"]:
            builder.verbose = True
        print("Testing connection to athena...")
        builder.cursor.execute("SHOW DATABASES")

        if args["action"] == "clean":
            builder.clean_study(args["target"])

        else:
            study_dict = get_study_dict(args["study_dir"])
            if args["target"]:
                for target in args["target"]:
                    if target not in study_dict:
                        sys.exit(
                            f"{target} was not found in available studies: "
                            f"{list(study_dict.keys())}.\n\n"
                            "If you are trying to run a custom study, make sure "
                            "you include `-s path/to/study/dir` as an arugment."
                        )

            if args["action"] == "build":
                if "all" in args["target"]:
                    builder.clean_and_build_all(study_dict)
                else:
                    for target in args["target"]:
                        if args["table_builder"]:
                            builder.run_single_table_builder(
                                study_dict[target], args["table_builder"]
                            )
                        else:
                            builder.clean_and_build_study(study_dict[target])

            elif args["action"] == "export":
                if "all" in args["target"]:
                    builder.export_all(study_dict, args["data_path"])
                else:
                    for target in args["target"]:
                        builder.export_study(study_dict[target], args["data_path"])

        # returning the builder for ease of unit testing
        return builder


def main(cli_args=None):
    """Reads CLI input/environment variables and invokes library calls"""

    parser = get_parser()
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
        ("profile", "CUMULUS_LIBRARY_PROFILE"),
        ("schema_name", "CUMULUS_LIBRARY_DATABASE"),
        ("workgroup", "CUMULUS_LIBRARY_WORKGROUP"),
        ("region", "CUMULUS_LIBRARY_REGION"),
        ("study_dir", "CUMULUS_LIBRARY_STUDY_DIR"),
        ("data_path", "CUMULUS_LIBRARY_DATA_PATH"),
        ("user", "CUMULUS_AGGREGATOR_USER"),
        ("id", "CUMULUS_AGGREGATOR_ID"),
        ("url", "CUMULUS_AGGREGATOR_URL"),
    )
    read_env_vars = []
    for pair in arg_env_pairs:
        if env_val := os.environ.get(pair[1]):
            args[pair[0]] = env_val
            read_env_vars.append([pair[1], env_val])

    if len(read_env_vars) > 0:
        table = Table(title="Values read from environment variables")
        table.add_column("Environment Variable", style="green")
        table.add_column("Value", style="cyan")
        for row in read_env_vars:
            if row[0] == "CUMULUS_AGGREGATOR_ID":
                table.add_row(row[0], "#########")
            else:
                table.add_row(row[0], row[1])
        console = Console()
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
