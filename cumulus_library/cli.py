#!/usr/bin/env python3
"""Utility for building/retrieving data views in AWS Athena"""

import argparse
import json
import os
import sys
import sysconfig

from pathlib import Path, PosixPath
from typing import Dict, List, Optional

import pyathena

from pyathena.pandas.cursor import PandasCursor

from cumulus_library.study_parser import StudyManifestParser


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
        self.cursor = pyathena.connect(
            region_name=region,
            work_group=workgroup,
            profile_name=profile,
            schema_name=schema,
        ).cursor()
        self.pandas_cursor = pyathena.connect(
            region_name=region,
            work_group=workgroup,
            profile_name=profile,
            schema_name=schema,
            cursor_class=PandasCursor,
        ).cursor()
        self.schema_name = schema

    def reset_export_dir(self, study: PosixPath) -> None:
        """
        Removes existing exports from a study's local data dir
        """
        project_path = Path(__file__).resolve().parents[1]
        path = Path(f"{str(project_path)}/data_export/{study}/")
        if path.exists():
            for file in path.glob("*"):
                file.unlink()

    ### Creating studies
    def clean_and_build_study(self, target: PosixPath) -> None:
        """Exports aggregates defined in a manifesty

        :param taget: A PosixPath to the study directory
        """
        studyparser = StudyManifestParser(target)
        studyparser.clean_study(self.cursor, self.schema_name, self.verbose)
        studyparser.run_python_builder(self.cursor, self.schema_name, self.verbose)
        studyparser.build_study(self.cursor, self.verbose)

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
    def export_study(self, target: PosixPath) -> None:
        """Exports aggregates defined in a manifesty

        :param taget: A PosixPath to the study directory
        """
        studyparser = StudyManifestParser(target)
        studyparser.export_study(self.pandas_cursor)

    def export_all(self, study_dict: Dict):
        """Exports all defined count tables to disk"""
        for key in study_dict.keys():
            self.export_study(study_dict[key])


def get_abs_posix_path(path: str) -> PosixPath:
    """Conveince method for hanlding abs vs rel paths"""
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
    if not args["build"] and not args["export"] and not args["create"]:
        sys.exit(
            (
                "Expecting at least one of build, export or create as arguments. "
                "See `cumulus-library --help` for more information"
            )
        )

    if args["create"]:
        create_template(args["path"])

    elif args["build"] or args["export"]:
        builder = StudyBuilder(
            args["region"],
            args["workgroup"],
            args["profile"],
            args["schema_name"],
        )
        if args["verbose"]:
            builder.verbose = True

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
        # here we invoke the cursor once to confirm valid connections
        builder.cursor.execute("show tables")

        if args["build"]:
            if "all" in args["target"]:
                builder.clean_and_build_all(study_dict)
            else:
                for target in args["target"]:
                    builder.clean_and_build_study(study_dict[target])

        if args["export"]:
            if "all" in args["target"]:
                builder.export_all(study_dict)
            else:
                for target in args["target"]:
                    builder.export_study(study_dict[target])

        # returning the builder for ease of unit testing
        return builder


def get_parser():
    """Provides parser for handling CLI arguments"""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""Generates study tables and views from post-Cumulus ETL data.

cumulus_library will attempt to create a connection to AWS athena. The
following order of preference is used to select credentials:
  - explict command line arguments
  - cumulus environment variables (CUMULUS_LIBRARY_PROFILE, 
    CUMULUS_LIBRARY_DATABASE, CUMULUS_LIBRARY_REGION)
  - Normal boto profile order (AWS env vars, ~/.aws/credentials, ~/.aws/config)""",
    )

    studygen = parser.add_argument_group("Creating study templates")
    studygen.add_argument(
        "-c",
        "--create",
        action="store_true",
        default=False,
        help=("Create a study instance from a template"),
    )
    studygen.add_argument(
        "-p",
        "--path",
        default="./",
        help=(
            "The the directory the study will be created in. Default is "
            "the current directory."
        ),
    )

    db = parser.add_argument_group("SQL database modifications")
    db.add_argument(
        "-t",
        "--target",
        action="append",
        help=(
            "Specify one or more studies to create views form. Default is to "
            "build all studies."
        ),
    )
    db.add_argument(
        "-s",
        "--study-dir",
        action="append",
        help=(
            "Optionally add one or more directories to look for study definitions in. "
            "Default is in project directory and CUMULUS_LIBRARY_PATH, if present, "
            "followed by any supplied paths. Target, and all its subdirectories, "
            "are checked for manifests. Overriding studies with the same namespace "
            "supersede earlier ones."
        ),
    )
    db.add_argument(
        "-b",
        "--build",
        default=False,
        action="store_true",
        help=("Removes and recreates Athena tables & views for the specified studies"),
    )
    db.add_argument(
        "-e",
        "--export",
        default=False,
        action="store_true",
        help=("Generates files on disk from Athena views for the specified studies"),
    )
    db.add_argument(
        "-v",
        "--verbose",
        default=False,
        action="store_true",
        help=("Prints detailed SQL query info"),
    )

    aws = parser.add_argument_group("AWS config")
    aws.add_argument("--profile", help="AWS profile", default="default")
    aws.add_argument(
        "--database",
        # internally, we use PyAthena's terminology for this but the UX term is "database"
        dest="schema_name",
        help="Cumulus Athena database name",
    )
    aws.add_argument(
        "--workgroup",
        default="cumulus",
        help="Cumulus Athena workgroup (default: cumulus)",
    )
    aws.add_argument(
        "--region",
        help="AWS region data of Athena instance (default: us-east-1)",
        default="us-east-1",
    )

    return parser


def main(cli_args=None):
    """Reads CLI input/environment variables and invokes library calls"""
    parser = get_parser()
    args = vars(parser.parse_args(cli_args))

    if args["target"] is not None:
        for target in args["target"]:
            if target == "all":
                args["target"] = ["all"]
                break
    else:
        args["target"] = ["all"]

    if args["study_dir"] is not None:
        posix_paths = []
        for path in args["study_dir"]:
            posix_paths.append(get_abs_posix_path(path))
        args["study_dir"] = posix_paths

    if profile_env := os.environ.get("CUMULUS_LIBRARY_PROFILE"):
        args["profile"] = profile_env
    if database_env := os.environ.get("CUMULUS_LIBRARY_DATABASE"):
        args["schema_name"] = database_env
    if workgroup_env := os.environ.get("CUMULUS_LIBRARY_WORKGROUP"):
        args["workgroup"] = workgroup_env
    if region_env := os.environ.get("CUMULUS_LIBRARY_REGION"):
        args["region"] = region_env
    if path_dir := os.environ.get("CUMULUS_LIBRARY_PATH"):
        args["path"] = [path_dir] + args["path"]

    return run_cli(args)


def main_cli():  # called by the generated wrapper scripts
    main()


if __name__ == "__main__":
    main()
