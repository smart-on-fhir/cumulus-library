import pathlib
import sys
from collections.abc import Iterable
from concurrent import futures

import cumulus_fhir_support
import pyarrow
import pyarrow.dataset
import pyarrow.json
import rich

from cumulus_library import base_utils, db_config, errors
from cumulus_library.databases import athena, base, duckdb


def _list_files_for_resource(path: pathlib.Path, resource: str) -> list[str]:
    return list(cumulus_fhir_support.list_multiline_json_in_dir(path, resource, recursive=True))


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


def _load_custom_etl_table(files: list[str]) -> pyarrow.dataset.Dataset | None:
    """Loads a non-FHIR ETL table from disk (tables like etl__completion)."""

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


def get_ndjson_files(path: str) -> dict[dict[str]]:
    """Gets a list of expected possible FHIR ndjson from the target path"""

    all_tables = {}
    resources = {}
    metadata = {}
    # Manually specify the list of resources because we want to create each table
    # even if the folder does not exist.
    resource_names = [
        "AllergyIntolerance",
        "Condition",
        "Device",
        "DiagnosticReport",
        "DocumentReference",
        "Encounter",
        "Immunization",
        "Location",
        "Medication",
        "MedicationRequest",
        "Observation",
        "Organization",
        "Patient",
        "Practitioner",
        "PractitionerRole",
        "Procedure",
        "ServiceRequest",
    ]
    for resource in resource_names:
        files = _list_files_for_resource(pathlib.Path(path), resource)
        resources[resource] = files
    all_tables["resources"] = resources

    metadata_tables = [
        "etl__completion",
        "etl__completion_encounters",
    ]
    for metadata_table in metadata_tables:
        files = list(
            cumulus_fhir_support.list_multiline_json_in_dir(pathlib.Path(path) / metadata_table)
        )
        metadata[metadata_table] = files
    all_tables["metadata"] = metadata
    return all_tables


def read_ndjson_dir(
    path: str | None, fileset: dict[dict[str]] | None = None
) -> dict[str, pyarrow.dataset.Dataset]:
    """Loads a directory tree of raw ndjson into schema-ful tables.

    One of either path or fileset must be provided.

    :param path: a directory path to ndjson
    :param fileset: A dictionary of ndjson file locations (usually the output of
      get_ndjson_files)
    :returns: dictionary of table names (like 'documentreference') to table
      data (with schema)
    """
    all_tables = {}
    if not fileset and not path:
        raise errors.CumulusLibraryError(
            "databases.utils.read_ndjson_dir() requires either a path or fileset argument."
        )
    # You may already have a fileset from using get_ndjson_files() to check if
    # your FHIR data has changed, if your DB is using that to cache.
    # If not, we'll just get that list directly.
    if not fileset:
        fileset = get_ndjson_files(path)

    for resource, files in fileset["resources"].items():
        table_name = resource.lower()
        # Make a pyarrow table with full schema from the data
        schema = cumulus_fhir_support.pyarrow_schema_from_rows(resource, _rows_from_files(files))
        # Use a PyArrow Dataset (vs a Table) to avoid loading all the files in memory.
        all_tables[table_name] = pyarrow.dataset.dataset(
            files, schema=schema, format=_json_format()
        )

    # And now some special support for a few ETL tables.
    for metadata_table, meta_path in fileset["metadata"].items():
        if dataset := _load_custom_etl_table(meta_path):
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
        tables = get_ndjson_files(load_ndjson_dir)
        backend.insert_tables(tables)


def create_db_backend(
    args: dict[str, str], pyarrow_cache_path: str | None = None
) -> tuple[base.DatabaseBackend, str]:
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
        backend = duckdb.DuckDatabaseBackend(
            args["database"],
            max_concurrent=args.get("max_concurrent"),
            pyarrow_cache_path=pyarrow_cache_path,
        )
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
            args.get("max_concurrent"),
        )
    else:
        raise errors.CumulusLibraryError(f"'{db_config.db_type}' is not a supported database.")
    if "prepare" not in args.keys():
        backend.connect()
    elif not args["prepare"]:
        backend.connect()
    _handle_load_ndjson_dir(args, backend)
    return (backend, schema_name)


def handle_concurrent_errors(
    resolved_futures: list[tuple[str, futures.Future]], db_type: str
) -> None:
    """Exits with formatted errors if a query fails during parallel execution"""
    failures = []
    for f in resolved_futures:
        try:
            res = f[1].result()
            # pyathena has a helper class for resolved futures,
            # so we'll check for it's specific failure indicator
            if db_type == "athena":
                if res.state == "FAILED":
                    failures.append((f[0], res.error_message))
        except Exception:
            failures.append((f[0], str(f[1].exception())))
    if failures:
        exit_msg = ""
        for f in failures:
            exit_msg = f"{exit_msg}\n-----\n{f[0]}\n\nHad the following error:\n\n"
            exit_msg += f"{f[1]}\n"
        sys.exit(f"One or more queries failed to execute:\n{exit_msg}")
