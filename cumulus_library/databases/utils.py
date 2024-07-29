import pathlib
import sys

import cumulus_fhir_support
import pyarrow

from cumulus_library import db_config, errors
from cumulus_library.databases import athena, base, duckdb


def _read_rows_for_resource(path: pathlib.Path, resource: str) -> list[dict]:
    rows = []

    # Support any ndjson files from the target folder directly
    rows += list(cumulus_fhir_support.read_multiline_json_from_dir(path, resource))

    # Also support being given an ETL output folder, and look in the table subdir
    subdir = path / resource.lower()
    rows += list(cumulus_fhir_support.read_multiline_json_from_dir(subdir, resource))

    return rows


def read_ndjson_dir(path: str) -> dict[str, pyarrow.Table]:
    """Loads a directory tree of raw ndjson into schema-ful tables.

    :param path: a directory path
    :returns: dictionary of table names (like 'documentreference') to table
      data (with schema)
    """
    all_tables = {}

    # Manually specify the list of resources because we want to create each table
    # even if the folder does not exist.
    resources = [
        "AllergyIntolerance",
        "Condition",
        "Device",
        "DiagnosticReport",
        "DocumentReference",
        "Encounter",
        "Immunization",
        "Medication",
        "MedicationRequest",
        "Observation",
        "Patient",
        "Procedure",
        "ServiceRequest",
    ]
    for resource in resources:
        table_name = resource.lower()
        rows = _read_rows_for_resource(pathlib.Path(path), resource)

        # Make a pyarrow table with full schema from the data
        schema = cumulus_fhir_support.pyarrow_schema_from_rows(resource, rows)
        all_tables[table_name] = pyarrow.Table.from_pylist(rows, schema)

    # And now some special support for a few ETL tables.
    metadata_tables = [
        "etl__completion",
        "etl__completion_encounters",
    ]
    for metadata_table in metadata_tables:
        rows = list(cumulus_fhir_support.read_multiline_json_from_dir(f"{path}/{metadata_table}"))
        if rows:
            # Auto-detecting the schema works for these simple tables
            all_tables[metadata_table] = pyarrow.Table.from_pylist(rows)

    return all_tables


def create_db_backend(args: dict[str, str]) -> (base.DatabaseBackend, str):
    """Retrieves a database backend and target schema from CLI args"""
    db_config.db_type = args["db_type"]

    if db_config.db_type == "duckdb":
        load_ndjson_dir = args.get("load_ndjson_dir")
        # TODO: reevaluate as DuckDB's local schema support evolves.
        # https://duckdb.org/docs/sql/statements/set.html#syntax
        if not (args.get("schema_name") is None or args["schema_name"] == "main"):
            print(
                "Warning - local schema names are not yet supported by duckDB's "
                "python library - using 'main' instead"
            )
        schema_name = "main"
        backend = duckdb.DuckDatabaseBackend(args["database"])
        if load_ndjson_dir:
            backend.insert_tables(read_ndjson_dir(load_ndjson_dir))
    elif db_config.db_type == "athena":
        if (
            args.get("schema_name") is not None
            and args.get("database") is not None
            and args.get("schema_name") != args.get("database")
        ):
            sys.exit(
                f"Two separate values, database: '{args['database']}' and "
                f"schema_name: '{args['schema_name']}' were supplied for "
                "an Athena database connection.\n"
                "Only one of these needs to be supplied. Both can be passed "
                "as arguments, but they must be equal"
            )
        schema_name = args["schema_name"] or args["database"]
        backend = athena.AthenaDatabaseBackend(
            args["region"],
            args["work_group"],
            args["profile"],
            schema_name,
        )
        if args.get("load_ndjson_dir"):
            sys.exit("Loading an ndjson dir is not supported with --db-type=athena.")
    else:
        raise errors.CumulusLibraryError(f"'{db_config.db_type}' is not a supported database.")

    return (backend, schema_name)
