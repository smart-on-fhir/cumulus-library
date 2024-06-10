"""Manages configuration for argparse"""

import argparse

# Functions for arguments used by more than one sub-command


def add_aws_config(parser: argparse.ArgumentParser) -> None:
    """Adds arguments related to aws credentials to a subparser"""
    aws = parser.add_argument_group("AWS config")
    aws.add_argument("--profile", help="AWS profile", default="default")
    aws.add_argument(
        "--workgroup",
        default="cumulus",
        dest="work_group",
        help="Cumulus Athena workgroup (default: cumulus)",
    )
    aws.add_argument(
        "--region",
        help="AWS region data of Athena instance (default: us-east-1)",
        default="us-east-1",
    )


def add_custom_option(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "-o",
        "--option",
        action="append",
        dest="options",
        help=(
            'Add a study-specific option, as "arg:value". '
            "See individual study documentation for possible argument names"
        ),
    )


def add_data_path_argument(parser: argparse.ArgumentParser) -> None:
    """Adds path arg to a subparser"""
    parser.add_argument(
        "data_path",
        default="./",
        nargs="?",
        help=(
            "The path to use for exporting counts data. "
            "Can be provided via CUMULUS_LIBRARY_DATA_PATH environment variable."
        ),
    )


def add_db_config(parser: argparse.ArgumentParser, input_mode: bool = False) -> None:
    """
    Adds arguments related to database backends to a subparser

    Pass input_mode=True if the subparser is for a command that operates on input
    (e.g. a build operation) rather than output tables (e.g. a clean operation)
    """
    group = parser.add_argument_group("Database config")
    group.add_argument(
        "--db-type",
        help="Which database backend to use (default athena)",
        choices=["athena", "duckdb"],
        default="athena",
    )
    group.add_argument(
        "--database",
        # In Athena, we use this as the schema_name (which is also called a Database
        # in their UX).
        #
        # In DuckDB, we use this as the path to the filename to store tables.
        #
        # Since we started as an Athena-centric codebase, we mostly keep referring to
        # this as name "schema_name". But to the user, both uses are still conceptually
        # a "database".
        dest="schema_name",
        help="Database name (for Athena) or file (for DuckDB)",
    )

    if input_mode:
        group.add_argument(
            "--load-ndjson-dir",
            help="Load ndjson files from this folder",
            metavar="DIR",
        )

    # Backend-specific config:
    add_aws_config(parser)


def add_study_dir_argument(parser: argparse.ArgumentParser) -> None:
    """Adds --study-dir arg to a subparser"""
    parser.add_argument(
        "-s",
        "--study-dir",
        action="append",
        help=(
            "Optionally add one or more directories to look for study definitions in. "
            "Default is in project directory and CUMULUS_LIBRARY_STUDY_DIR, "
            "if present, followed by any supplied paths. Target, and all its "
            "subdirectories, are checked for manifests. Overriding studies with the"
            " same namespace supersede earlier ones."
        ),
    )


def add_table_builder_argument(parser: argparse.ArgumentParser) -> None:
    """Adds --builder arg to a subparser"""
    parser.add_argument(
        "--builder",
        help=argparse.SUPPRESS,
    )


def add_target_argument(parser: argparse.ArgumentParser) -> None:
    """Adds --target arg to a subparser"""
    parser.add_argument(
        "-t",
        "--target",
        action="append",
        help=("Specify one or more studies to perform actions against."),
    )


def add_verbose_argument(parser: argparse.ArgumentParser) -> None:
    """Adds --verbose arg to a subparser"""
    parser.add_argument(
        "--verbose",
        default=False,
        action="store_true",
        help="Prints detailed SQL query info",
    )


# Parser construction


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

    # Database cleaning

    clean = actions.add_parser(
        "clean", help="Removes tables & views beginning with '[target]__' from Athena"
    )

    add_custom_option(clean)
    add_db_config(clean)
    add_target_argument(clean)
    add_study_dir_argument(clean)
    add_verbose_argument(clean)

    clean.add_argument(
        "--prefix",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    clean.add_argument(
        "--statistics",
        action="store_true",
        help="Remove artifacts of previous statistics runs",
        dest="stats_clean",
    )

    # Database building

    build = actions.add_parser(
        "build",
        help="Removes and recreates Athena tables & views for specified studies",
    )
    add_custom_option(build)
    add_data_path_argument(build)
    add_db_config(build, input_mode=True)
    add_study_dir_argument(build)
    add_table_builder_argument(build)
    add_target_argument(build)
    add_verbose_argument(build)

    build.add_argument(
        "--continue",
        dest="continue_from",
        help=argparse.SUPPRESS,
    )
    build.add_argument(
        "--force-upload",
        action="store_true",
        help="Forces file downloads/uploads to occur, even if they already exist",
    )
    build.add_argument(
        "--statistics",
        action="store_true",
        help=(
            "Force regenerating statistics data from latest dataset. "
            "Stats are created by default when study is initially run"
        ),
        dest="stats_build",
    )
    build.add_argument(
        "--umls-key",
        help="An API Key for the UMLS API",
    )

    # Database export

    export = actions.add_parser(
        "export", help="Generates files on disk from Athena tables/views"
    )
    add_custom_option(export)
    add_target_argument(export)
    add_study_dir_argument(export)
    add_data_path_argument(export)
    add_verbose_argument(export)
    add_db_config(export)
    export.add_argument(
        "--archive",
        action="store_true",
        help="Generates archive of :all: study tables, ignoring manifest export list.",
    )

    # Database import

    importer = actions.add_parser(
        "import", help="Recreates a study from an exported archive"
    )
    add_db_config(importer)
    add_verbose_argument(importer)
    importer.add_argument(
        "-a",
        "--archive-path",
        action="append",
        help="The path to an archive generated by the export CLI subcommand",
    )
    # Aggregator upload

    upload = actions.add_parser(
        "upload", help="Bulk uploads data to Cumulus aggregator"
    )
    add_data_path_argument(upload)
    add_target_argument(upload)

    upload.add_argument(
        "--id", help="Site ID. Default is value of CUMULUS_AGGREGATOR_ID"
    )
    upload.add_argument(
        "--preview",
        default=False,
        action="store_true",
        help="Run pre-fetch and prepare upload, but log output instead of sending.",
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
        "--user", help="Cumulus user. Default is value of CUMULUS_AGGREGATOR_USER"
    )

    # Generate a study's template-driven sql

    sql = actions.add_parser(
        "generate-sql", help="Generates a study's template-driven sql for reference"
    )
    add_custom_option(sql)
    add_db_config(sql, input_mode=True)
    add_table_builder_argument(sql)
    add_target_argument(sql)
    add_study_dir_argument(sql)

    # Generate markdown tables for documentation

    markdown = actions.add_parser(
        "generate-md", help="Generates markdown tables for study documentation"
    )
    add_data_path_argument(markdown)
    add_db_config(markdown)
    add_study_dir_argument(markdown)
    add_target_argument(markdown)
    add_verbose_argument(markdown)

    return parser
