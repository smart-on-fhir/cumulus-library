import pathlib
import sys
from collections.abc import Iterable

import cumulus_fhir_support
import pyarrow
import pyarrow.dataset
import pyarrow.json
import rich

from cumulus_library import base_utils, db_config, errors
from cumulus_library.databases import athena, base, duckdb


def _list_files_for_resource(path: pathlib.Path, resource: str) -> list[str]:
    files = []

    # Support any ndjson files from the target folder directly
    files += list(cumulus_fhir_support.list_multiline_json_in_dir(path, resource))

    # Also support being given an ETL output folder, and look in the table subdir
    subdir = path / resource.lower()
    files += list(cumulus_fhir_support.list_multiline_json_in_dir(subdir, resource))

    return files


def _rows_from_files(files: list[str]) -> Iterable[dict]:
    for file in files:
        yield from cumulus_fhir_support.read_multiline_json(file)


def _json_format():
    """Returns a Dataset format object suitable for reading FHIR NDJSON."""
    # FHIR can have very long JSON lines (think DocRefs with inlined notes). And unfortunately,
    # the way PyArrow's JSON parser works is that each entire line must fit inside a single read
    # block, or an exception is raised. I've seen 13MB JSON lines in the wild, at least.
    # So let's be generous here and allow 50MB lines. And cross our fingers that it's enough.
    read_options = pyarrow.json.ReadOptions(block_size=50 * 2**20)
    return pyarrow.dataset.JsonFileFormat(read_options=read_options)


def _load_custom_etl_table(path: str) -> pyarrow.dataset.Dataset | None:
    """Loads a non-FHIR ETL table from disk (tables like etl__completion)."""
    files = list(cumulus_fhir_support.list_multiline_json_in_dir(path))
    if not files:
        return None

    # This is a custom Cumulus ETL table, with no easy way to get a schema definition.
    # We **could** put a hard-coded shared schema in cumulus-fhir-support, but since these
    # tables are so simple, we mostly don't yet need to have that level of pre-coordination.
    #
    # Instead, we let PyArrow infer the types, with one hiccup: its JSON parser will interpret
    # datetime strings as a TIMESTAMP type, which isn't insane, but not what we want - we want to
    # keep those as strings. So we let PyArrow infer from Python objects, where it works like we
    # want.
    #
    # Thus, we sip on the data a bit to infer a schema from the first row as a Python object.
    first_row = next(iter(_rows_from_files(files)), None)
    if not first_row:
        # This path shouldn't happen, since list_multiline_json_in_dir() won't give us back
        # empty files. But just in case.
        return None  # pragma: no cover
    schema = pyarrow.Table.from_pylist([first_row]).schema

    # Now let PyArrow load the rest of the data from disk on demand, rather than loading it all
    # into memory, but using the detected schema rather than inferring it with timestamps.
    return pyarrow.dataset.dataset(files, schema=schema, format=_json_format())


def read_ndjson_dir(path: str) -> dict[str, pyarrow.dataset.Dataset]:
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
        files = _list_files_for_resource(pathlib.Path(path), resource)

        # Make a pyarrow table with full schema from the data
        schema = cumulus_fhir_support.pyarrow_schema_from_rows(resource, _rows_from_files(files))
        # Use a PyArrow Dataset (vs a Table) to avoid loading all the files in memory.
        all_tables[table_name] = pyarrow.dataset.dataset(
            files, schema=schema, format=_json_format()
        )

    # And now some special support for a few ETL tables.
    metadata_tables = [
        "etl__completion",
        "etl__completion_encounters",
    ]
    for metadata_table in metadata_tables:
        if dataset := _load_custom_etl_table(f"{path}/{metadata_table}"):
            all_tables[metadata_table] = dataset

    return all_tables


def _handle_load_ndjson_dir(args: dict[str, str], backend: base.DatabaseBackend) -> None:
    load_ndjson_dir = args.get("load_ndjson_dir")
    if not load_ndjson_dir:
        return  # nothing to do

    if db_config.db_type != "duckdb":
        sys.exit("Loading an NDJSON dir is only supported with --db-type=duckdb.")

    if backend.connection is None:
        return  # connect() was never run, we don't have a live DB connection

    with base_utils.get_progress_bar() as progress:
        progress.add_task("Detecting JSON schemas...", total=None)
        backend.insert_tables(read_ndjson_dir(load_ndjson_dir))


def create_db_backend(args: dict[str, str]) -> (base.DatabaseBackend, str):
    """Retrieves a database backend and target schema from CLI args"""
    db_config.db_type = args["db_type"]

    if db_config.db_type == "duckdb":
        # TODO: reevaluate as DuckDB's local schema support evolves.
        # https://duckdb.org/docs/sql/statements/set.html#syntax
        if not (args.get("schema_name") is None or args["schema_name"] == "main"):
            rich.print(  # pragma: no cover
                "Warning - local schema names are not yet supported by duckDB's "
                "python library - using 'main' instead"
            )
        schema_name = "main"
        backend = duckdb.DuckDatabaseBackend(args["database"])
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
    else:
        raise errors.CumulusLibraryError(f"'{db_config.db_type}' is not a supported database.")
    if "prepare" not in args.keys():
        backend.connect()
    elif not args["prepare"]:
        backend.connect()
    _handle_load_ndjson_dir(args, backend)
    return (backend, schema_name)
