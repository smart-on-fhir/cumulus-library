#!/usr/bin/env python3
"""Utility for building/retrieving data views in AWS Athena"""

import argparse
import os
import sys

from importlib.machinery import SourceFileLoader
from pathlib import Path
from typing import List

import pyathena

from pyathena.pandas.cursor import PandasCursor
from rich.progress import track

from cumulus_library import core, umls


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
        self.schema = os.environ.get("CUMULUS_LIBRARY_SCHEMA")

    def get_study_builder(self):
        """Convinience method for getting athena args from environment"""
        return StudyBuilder(
            self.bucket, self.region, self.workgroup, self.profile, self.schema
        )


class StudyBuilder:
    """Class for managing Athena cursors and executing Cumulus queries"""

    verbose = False

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
        self.schema = schema

    ### Athena SQL execution helpers

    def show_tables(self) -> List[str]:
        """
        :return: list of table names in the schema, including all views.
        """
        res = self.cursor.execute("show tables").fetchall()
        tables = [item[0] for item in res]
        if self.verbose:
            print(tables)
        return tables

    def list_columns(self, table: str) -> List[str]:
        """
        :param table: name of Athena table/view
        :return: list of columns
        """
        res = self.cursor.execute(f"desc {table}").fetchall()
        columns = [item[0] for item in res]
        columns = [c.split("\t")[0].strip() for c in columns]
        if self.verbose:
            print("#########################################")
            print(columns)
        return columns

    def execute_file(self, file_sql: str) -> None:
        """
        Execute SQL commands from a file.
        File may include multiple SQL statements and comments (--)
        :param file_sql:
        :return: None (fail fast - script will die on exception)
        """
        # print(f"execute( {file_sql} )")
        for _sql in core.list_sql(file_sql):
            self.cursor.execute(_sql)

    def execute_sql(self, sql_list: List[str]):
        """
        Execute SQL commands from a list of files.
        File may include multiple SQL statements and comments (--)
        :param sql_list: list of SQL commands
        :return: None (fail fast - script will die on exception)
        """
        for _sql in sql_list:
            if self.verbose:
                print("#########################################")
                print(_sql)
            self.cursor.execute(_sql)

    def execute_sql_template(self, template_path):
        """
        Execute SQL commands from a jinga template handler.

        A template module is expected to have an execute_templates method, which
        serves as the entrypoint for any logic the template needs to execute, and
        takes a cursor, a schema, and the verbose flag.

        TODO: If we adopt this pattern in a more widespread fashion, make a base
        class for this.
        :param template_path: list of SQL commands
        :return: None (fail fast - script will die on exception)
        """
        if self.verbose:
            print("#########################################")
            print(template_path)
        module = SourceFileLoader("template", template_path).load_module()
        module.execute_templates(self.cursor, self.schema, self.verbose)

    def reset_export_dir(self, study):
        """
        Removes existing exports from a study's local data dir
        """
        project_path = Path(__file__).resolve().parents[1]
        path = Path(f"{str(project_path)}/data_export/{study}/")
        if path.exists():
            for file in path.glob("*"):
                file.unlink()

    def export_table(self, table, study):
        """
        Exports a table from athena to a study's data directory
        """
        dataframe = self.pandas_cursor.execute(f"select * from {table}").as_pandas()
        project_path = Path(__file__).resolve().parents[1]
        path = Path(f"{str(project_path)}/data_export/{study}/")
        path.mkdir(parents=True, exist_ok=True)
        dataframe.to_csv(f"./data_export/{study}/{table}.csv", index=False)
        dataframe.to_parquet(f"./data_export/{study}/{table}.parquet", index=False)

    ### Athena view cleaners

    def clean_core(self):
        """
        Drop *schema* tables/views if exist.
        """
        self.execute_sql(core.list_sql("clean_core.sql"))

    def clean_umls(self):
        """
        Drop *umls* tables/views if exist.
        """
        targets = [
            "clean_umls_template.py",
        ]
        for filename in targets:
            self.execute_sql_template(umls.relpath(filename))

    def clean(self):
        """
        Drop all study tables and the schema.
        """
        self.clean_core()
        self.clean_umls()

    ### Athena view builders

    def make_core(self):
        """
        create views of FHIR objects for easier querying in the studies.
        Goal is to enable *easier* SQL query over all US-CORE FHIR resources.
        """
        targets = [
            "fhir_define.sql",
            "site_define.sql",
            "patient.sql",
            "encounter.sql",
            "documentreference.sql",
            "condition.sql",
            "observation_lab.sql",
            "study_period.sql",
        ]
        for filename in targets:
            self.execute_sql(core.list_sql(filename))

    def make_umls(self):
        """Builds UMLS tables for assiting with code parsing"""
        targets = [
            "icd_legend_template.py",
        ]
        for filename in targets:
            self.execute_sql_template(umls.relpath(filename))

    def make_all(self):
        """Builds views for all studies"""
        self.clean()
        self.make_umls()
        self.make_core()

    ### Data exporters

    def export_core(self):
        """Exports count data related to core tables to disk"""
        targets = [
            "count_core_patient",
            "count_core_encounter_month",
            "count_core_documentreference_month",
            "count_core_observation_lab_month",
            "count_core_condition_icd10_month",
            "core_meta_date",
        ]
        self.reset_export_dir("core")
        for table in track(targets, description="Exporting core counts"):
            self.export_table(table, "core")

    def export_all(self):
        """Exports all defined count tables to disk"""
        self.export_core()


def run_make(args):  # pylint: disable=too-many-branches
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
    # invoking the cursor once to confirm valid connections
    builder.cursor.execute("show tables")
    if not args["build"] and not args["export"]:
        print("Neither build nor export specified - exiting with no action")
    if args["build"]:
        for target in args["target"]:
            if target == "core":
                builder.clean_core()
                builder.make_core()
            elif target == "umls":
                builder.clean_umls()
                builder.make_umls()
            else:
                builder.make_all()
    if args["export"]:
        for target in args["target"]:
            if target not in ["all", "core", "covid"]:
                print(f"{target} has no data export currently defined")
            elif target == "core":
                builder.export_core()
            else:
                builder.export_all()
    # returning the builder for ease of unit testing
    return builder


def get_parser(make_list):
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
        help=(
            "Specify one or more studies to create views for. "
            f"Valid targets: all(default), {', '.join(make_list)}"
        ),
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
    make_list = ["core", "covid", "lyme", "suicidality", "umls"]
    parser = get_parser(make_list)
    args = vars(parser.parse_args(cli_args))
    if args["target"] is not None:
        for target in args["target"]:
            if target == "all":
                args["target"] = ["all"]
                break
            if target not in make_list:
                print(f"Invalid study: {target}")
                sys.exit(1)
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

    return run_make(args)


if __name__ == "__main__":
    main()
