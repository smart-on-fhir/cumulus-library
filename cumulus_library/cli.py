#!/usr/bin/env python3
"""Utility for building/retrieving data views in AWS Athena"""

import argparse
import os
import sys

from importlib.machinery import SourceFileLoader
from pathlib import Path, PosixPath
from typing import List, Dict

import pyathena

from pyathena.pandas.cursor import PandasCursor
from rich.progress import track

# from cumulus_library import core, umls
from cumulus_library.study_parser import StudyManifestParser


class CumulusEnv:  # pylint: disable=too-few-public-methods
    """
    Wrapper for Cumulus Environment vars.
    Simplifies connections to StudyBuilder without requiring CLI parsing.
    """

    def __init__(self):
        self.bucket = os.environ.get("CUMULUS_LIBRARY_S3")
        self.region = os.environ.get("CUMULUS_LIBRARY_REGION", "us-east-1")
        self.workgroup = os.environ.get("CUMULUS_LIBRARY_WORKGROUP", "cumulus")
        self.profile = os.environ.get("CUMULUS_LIBRARY_PROFILE")
        self.schema_name = os.environ.get("CUMULUS_LIBRARY_SCHEMA")

    def get_study_builder(self):
        """Convinience method for getting athena args from environment"""
        return StudyBuilder(
            self.bucket, self.region, self.workgroup, self.profile, self.schema_name
        )


class StudyBuilder:
    """Class for managing Athena cursors and executing Cumulus queries"""

    verbose = False
    schema_name = None

    def __init__(  # pylint: disable=too-many-arguments
        self, bucket: str, region: str, workgroup: str, profile: str, schema: str
    ):
        self.cursor = pyathena.connect(
            s3_staging_dir=bucket,
            region_name=region,
            work_group=workgroup,
            profile_name=profile,
            schema_name=schema,
        ).cursor()
        self.pandas_cursor = pyathena.connect(
            s3_staging_dir=bucket,
            region_name=region,
            work_group=workgroup,
            profile_name=profile,
            schema_name=schema,
            cursor_class=PandasCursor,
        ).cursor()
        self.schema_name = schema

    def reset_export_dir(self, study:PosixPath) -> None:
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
        """Builds views for all studies"""
        study_dict.pop("template")
        for precursor_study in ["vocab", "core"]:
            self.clean_and_build_study(study_dict[precursor_study])
            study_dict.pop(precursor_study)
        for key in study_dict.keys():
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


def get_study_dict() -> Dict[str, PosixPath] -> List:
    """Convenience function for getting directories in ./studies/

    :returns: A list of pathlib.PosixPath objects
    """
    manifest_studies = {}
    library_path = Path(__file__).resolve().parents[0]
    for path in Path(f"{library_path}/studies").iterdir():
        if path.is_dir():
            manifest_studies[path.name] = path
    return manifest_studies


def run_cli(args: Dict):  # pylint: disable=too-many-branches
    """Controls which library tasks are run based on CLI arguments"""
    builder = StudyBuilder(
        args["s3_bucket"],
        args["region"],
        args["workgroup"],
        args["profile"],
        args["database"],
    )
    if args["verbose"]:
        builder.verbose = True
    # here we invoke the cursor once to confirm valid connections
    builder.cursor.execute("show tables")
    if not args["build"] and not args["export"]:
        print("Neither build nor export specified - exiting with no action")
    study_dict = get_study_dict()
    if args["build"]:
        if "all" in args["target"] or args["target"] == None:
            builder.clean_and_build_all(study_dict)
        for target in args["target"]:
            if target in study_dict.keys():
                builder.clean_and_build_study(study_dict[target])
    if args["export"]:
        if "all" in args["target"] or args["target"] == None:
            builder.export_all(study_dict)
        else:
            for target in args["target"]:
                builder.export_study(study_dict[target])

    # returning the builder for ease of unit testing
    return builder


def get_parser():
    """Provides parser for handling CLI arguments"""
    parser = argparse.ArgumentParser(
        description="""Generates study views from post-Cumulus ETL data.

        By default, make will remove, and recreate, all table views, assuming
        you have set the required  credentials, which are used in the following
        order of preference:
        - explict command line arguments
        - cumulus environment variables (CUMULUS_LIBRARY_PROFILE,
        CUMULUS_LIBRARY_SCHEMA, CUMULUS_LIBRARY_S3, CUMULUS_LIBRARY_REGION)
        - Normal boto profile order (AWS env vars, ~/.aws/credentials, ~/.aws/config)
        connecting to AWS. Passing values via the command line will override the
        environment variables """
    )
    parser.add_argument(
        "-t",
        "--target",
        action="append",
        help=("Specify one or more studies to create views for. "),
    )
    parser.add_argument(
        "-b",
        "--build",
        default=False,
        action="store_true",
        help=("Recreates Athena views from sql definitions"),
    )
    parser.add_argument(
        "-e",
        "--export",
        default=False,
        action="store_true",
        help=("Generates files on disk from Athena views"),
    )
    parser.add_argument(
        "-v",
        "--verbose",
        default=False,
        action="store_true",
        help=("Prints detailed SQL query info"),
    )

    aws = parser.add_argument_group("AWS config")
    aws.add_argument("-p", "--profile", help="AWS profile", default="default")
    aws.add_argument("-d", "--database", help="Cumulus ETL Athena DB/schema")
    aws.add_argument(
        "-w",
        "--workgroup",
        default="cumulus",
        help="Cumulus Athena workgroup (default: cumulus)",
    )
    aws.add_argument(
        "-s",
        "--s3_bucket",
        help=(
            "S3 location to store athena metadata. " "(will contain some query outputs)"
        ),
    )
    aws.add_argument(
        "-r",
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
    if profile_env := os.environ.get("CUMULUS_LIBRARY_PROFILE"):
        args["profile"] = profile_env
    if database_env := os.environ.get("CUMULUS_LIBRARY_SCHEMA"):
        args["database"] = database_env
    if workgroup_env := os.environ.get("CUMULUS_LIBRARY_WORKGROUP"):
        args["workgroup"] = workgroup_env
    if bucket_env := os.environ.get("CUMULUS_LIBRARY_S3"):
        args["s3_bucket"] = bucket_env
    if region_env := os.environ.get("CUMULUS_LIBRARY_REGION"):
        args["region"] = region_env

    return run_cli(args)


if __name__ == "__main__":
    main()
