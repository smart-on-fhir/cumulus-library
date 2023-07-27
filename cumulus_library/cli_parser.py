"""Manages configuration for argparse"""
import argparse


def add_target_argument(parser: argparse.ArgumentParser) -> None:
    """Adds --target arg to a subparser"""
    parser.add_argument(
        "-t",
        "--target",
        action="append",
        help=(
            "Specify one or more studies to perform actions against. "
            "Default is to use all studies."
        ),
    )


def add_table_builder_argument(parser: argparse.ArgumentParser) -> None:
    """Adds --table_builder arg to a subparser"""
    parser.add_argument(
        "--table-builder",
        help=(argparse.SUPPRESS),
    )


def add_study_dir_argument(parser: argparse.ArgumentParser) -> None:
    """Adds --study_dir arg to a subparser"""
    parser.add_argument(
        "-s",
        "--study-dir",
        action="append",
        help=(
            "Optionally add one or more directories to look for study definitions in. "
            "Default is in project directory and CUMULUS_LIBRARY_STUDY_DIR, if present, "
            "followed by any supplied paths. Target, and all its subdirectories, "
            "are checked for manifests. Overriding studies with the same namespace "
            "supersede earlier ones."
        ),
    )


def add_data_path_argument(parser: argparse.ArgumentParser) -> None:
    """Adds path arg to a subparser"""
    parser.add_argument(
        "data_path",
        default="./",
        nargs="?",
        help=(
            "The path to use for Athena counts data. "
            "Can be povided via CUMULUS_LIBRARY_DATA_PATH environment variable."
        ),
    )


def add_verbose_argument(parser: argparse.ArgumentParser) -> None:
    """Adds --verbose arg to a subparser"""
    parser.add_argument(
        "--verbose",
        default=False,
        action="store_true",
        help="Prints detailed SQL query info",
    )


def add_aws_config(parser: argparse.ArgumentParser) -> None:
    """Adds arguments related to aws credentials to a subparser"""
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


def get_parser() -> argparse.ArgumentParser:
    """Provides parser for handling CLI arguments"""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""Generates study tables and views from post-Cumulus ETL data.

cumulus-library will attempt to create a connection to AWS Athena. The
following order of preference is used to select credentials:
  - explicit command line arguments
  - cumulus environment variables (see docs for more info)
  - Normal boto profile order (AWS env vars, ~/.aws/credentials, ~/.aws/config)""",
        epilog="See 'cumulus-library -h [action]' for usage of a specific action",
    )

    parser.add_argument(
        "--version", action="store_true", help="Display cumulus-library version number"
    )

    actions = parser.add_subparsers(
        title="actions",
        help="Available library actions",
        dest="action",
    )

    create = actions.add_parser(
        "create", help="Create a study instance from a template"
    )
    create.add_argument(
        "create_dir",
        default="./",
        nargs="?",
        help=(
            "The the directory the study will be created in. Default is "
            "the current directory."
        ),
    )

    clean = actions.add_parser(
        "clean", help="Removes tables & views beginning with '[target]__' from Athena"
    )
    add_target_argument(clean)
    add_aws_config(clean)

    build = actions.add_parser(
        "build",
        help="Removes and recreates Athena tables & views for specified studies",
    )
    add_target_argument(build)
    add_table_builder_argument(build)
    add_study_dir_argument(build)
    add_verbose_argument(build)
    add_aws_config(build)

    export = actions.add_parser(
        "export", help="Generates files on disk from Athena views"
    )
    add_target_argument(export)
    add_study_dir_argument(export)
    add_data_path_argument(export)
    add_verbose_argument(export)
    add_aws_config(export)

    upload = actions.add_parser(
        "upload", help="Bulk uploads data to Cumulus aggregator"
    )
    add_target_argument(upload)
    add_data_path_argument(upload)
    upload.add_argument(
        "--user", help="Cumulus user. Default is value of CUMULUS_AGGREGATOR_USER"
    )
    upload.add_argument(
        "--id", help="Site ID. Default is value of CUMULUS_AGGREGATOR_ID"
    )
    upload.add_argument(
        "--url",
        help=(
            "Upload URL. Default is value of CUMULUS_AGGREGATOR_URL if present, "
            "or smart cumulus instance"
        ),
        default="https://aggregator.smartcumulus.org/upload/",
    )
    upload.add_argument(
        "--preview",
        default=False,
        action="store_true",
        help="Run pre-fetch and prepare upload, but log output instead of sending.",
    )

    return parser
