"""Handles the creation of new tables"""

import contextlib
import importlib.util
import inspect
import pathlib
import re
import sys
import tomllib

from rich.progress import Progress, TaskID

from cumulus_library import (
    base_table_builder,
    base_utils,
    databases,
    enums,
    errors,
    log_utils,
    protected_table_builder,
    study_manifest,
)
from cumulus_library.statistics import psm


@contextlib.contextmanager
def _temporary_sys_path(add: pathlib.Path) -> None:
    orig_path = list(sys.path)
    try:
        sys.path.insert(0, str(add))
        yield
    finally:
        sys.path = orig_path


def _load_and_execute_builder(
    config: base_utils.StudyConfig,
    manifest: study_manifest.StudyManifest,
    *,
    filename: str,
    db_parser: databases.DatabaseParser = None,
    write_reference_sql: bool = False,
    doc_str: str | None = None,
) -> None:
    """Loads a table builder from a file.

    :param config: a StudyConfig object
    :param manifest: a StudyManifest object
    :keyword filename: filename of a module implementing a TableBuilder
    :keyword db_parser: an object implementing DatabaseParser for the target database
    :keyword write_reference_sql: if true, writes sql to disk inside a study's directory
    :keyword doc_string: A string to insert between queries written to disk
    """

    # Since we have to support arbitrary user-defined python files here, we
    # jump through some importlib hoops to import the module directly from
    # a source file defined in the manifest.
    spec = importlib.util.spec_from_file_location(
        "table_builder", f"{manifest._study_path}/{filename}"
    )
    table_builder_module = importlib.util.module_from_spec(spec)
    sys.modules["table_builder"] = table_builder_module
    # Inject the study dir into sys.path so that builders can import
    # from surrounding utility code, even if the study isn't installed.
    # (i.e. you're working from a git checkout and do something like `-s .`)
    with _temporary_sys_path(manifest._study_path.parent):
        spec.loader.exec_module(table_builder_module)

    # We're going to find all subclasses of BaseTableBuild in this file.
    # Since BaseTableBuilder itself is a valid subclass of BaseTableBuilder,
    # we'll detect and skip it. If we don't find any subclasses,
    # we'll punt.
    table_builder_subclasses = []
    for _, cls_obj in inspect.getmembers(table_builder_module, inspect.isclass):
        if (
            issubclass(cls_obj, base_table_builder.BaseTableBuilder)
            and cls_obj != base_table_builder.BaseTableBuilder
        ):
            table_builder_subclasses.append(cls_obj)

    if len(table_builder_subclasses) == 0:
        raise errors.StudyManifestParsingError(
            f"Error loading {manifest._study_path}{filename}\n"
            "Custom builders must extend the BaseTableBuilder class."
        )

    # Remove instances of intermediate classes, if present
    table_builder_subclasses = list(
        filter(lambda x: x.__name__ != "CountsBuilder", table_builder_subclasses)
    )

    # We'll get the subclass, initialize it, run the executor code, and then
    # remove it so it doesn't interfere with the next python module to
    # execute, since the subclass would otherwise hang around.
    table_builder_class = table_builder_subclasses[0]
    table_builder = table_builder_class()
    if write_reference_sql:
        prefix = manifest.get_study_prefix()
        table_builder.prepare_queries(config=config, manifest=manifest, parser=db_parser)
        for query_pos in range(len(table_builder.queries)):
            if "s3://" in table_builder.queries[query_pos]:
                table_builder.queries[query_pos] = re.sub(
                    f"s3://(.+){prefix}",
                    f"s3://bucket/db_path/{prefix}",
                    table_builder.queries[query_pos],
                )
        table_builder.comment_queries(doc_str=doc_str)
        new_filename = pathlib.Path(f"{filename}").stem + ".sql"
        table_builder.write_queries(
            path=pathlib.Path(f"{manifest._study_path}/reference_sql/" + new_filename)
        )
    else:
        table_builder.execute_queries(
            config=config,
            manifest=manifest,
            parser=db_parser,
        )

    # After running the executor code, we'll remove
    # it so it doesn't interfere with the next python module to
    # execute, since the subclass would otherwise hang around.
    del sys.modules[table_builder_module.__name__]
    del table_builder_module


def run_protected_table_builder(
    config: base_utils.StudyConfig,
    manifest: study_manifest.StudyManifest,
) -> None:
    """Creates protected tables for persisting selected data across runs

    :param config: a StudyConfig object
    :param manifest: a StudyManifest object
    """
    ptb = protected_table_builder.ProtectedTableBuilder()
    ptb.execute_queries(
        config=config,
        manifest=manifest,
    )


def run_table_builder(
    config: base_utils.StudyConfig,
    manifest: study_manifest.StudyManifest,
    *,
    db_parser: databases.DatabaseParser = None,
) -> None:
    """Loads modules from a manifest and executes code via BaseTableBuilder

    :param config: a StudyConfig object
    :param manifest: a StudyManifest object
    :keyword db_parser: an object implementing DatabaseParser for the target database
    """
    for file in manifest.get_table_builder_file_list():
        _load_and_execute_builder(
            config=config,
            manifest=manifest,
            filename=file,
            db_parser=db_parser,
        )


def run_counts_builders(
    config: base_utils.StudyConfig,
    manifest: study_manifest.StudyManifest,
) -> None:
    """Loads counts modules from a manifest and executes code via BaseTableBuilder

    While a count is a form of statistics, it is treated separately from other
    statistics because it is, by design, always going to be static against a
    given dataset, where other statistical methods may use sampling techniques
    or adjustable input parameters that may need to be preserved for later review.

    :param config: a StudyConfig object
    :param manifest: a StudyManifest object
    """
    for file in manifest.get_counts_builder_file_list():
        _load_and_execute_builder(
            config=config,
            manifest=manifest,
            filename=file,
        )


def run_statistics_builders(
    config: base_utils.StudyConfig,
    manifest: study_manifest.StudyManifest,
) -> None:
    """Loads statistics modules from toml definitions and executes

    :param config: a StudyConfig object
    :param manifest: a StudyManifest object
    """
    if len(manifest.get_statistics_file_list()) == 0:
        return
    existing_stats = []
    if not config.stats_build:
        existing_stats = (
            config.db.cursor()
            .execute(
                "SELECT view_name FROM "  # noqa: S608
                f"{manifest.get_study_prefix()}__{enums.ProtectedTables.STATISTICS.value}"
            )
            .fetchall()
        )
    for file in manifest.get_statistics_file_list():
        # This open is a bit redundant with the open inside of the PSM builder,
        # but we're letting it slide so that builders function similarly
        # across the board
        safe_timestamp = base_utils.get_tablename_safe_iso_timestamp()
        toml_path = pathlib.Path(f"{manifest._study_path}/{file}")
        with open(toml_path, "rb") as file:
            stats_config = tomllib.load(file)
            config_type = stats_config["config_type"]
            target_table = stats_config["target_table"]

        if (target_table,) in existing_stats and not config.stats_build:
            continue
        if config_type == "psm":
            builder = psm.PsmBuilder(
                toml_config_path=toml_path,
                config=stats_config,
                data_path=manifest.data_path / f"{manifest.get_study_prefix()}/psm",
            )
        else:
            raise errors.StudyManifestParsingError(  # pragma: no cover
                f"{toml_path} references an invalid statistics type {config_type}."
            )
        builder.execute_queries(
            config=config,
            manifest=manifest,
            table_suffix=safe_timestamp,
        )
        log_utils.log_statistics(
            config=config,
            manifest=manifest,
            table_type=config_type,
            table_name=f"{target_table}_{safe_timestamp}",
            view_name=target_table,
        )


def run_matching_table_builder(
    config: base_utils.StudyConfig,
    manifest: study_manifest.StudyManifest,
    *,
    builder: str,
    db_parser: databases.DatabaseParser = None,
):
    """targets all table builders matching a target string for running

    :param config: a StudyConfig object
    :param manifest: a StudyManifest object
    :keyword builder: filename of a module implementing a TableBuilder
    :keyword db_parser: an object implementing DatabaseParser for the target database"""
    all_generators = manifest.get_all_generators()
    for file in all_generators:
        if builder and file.find(builder) == -1:
            continue
        _load_and_execute_builder(
            config=config,
            manifest=manifest,
            filename=file,
            db_parser=db_parser,
        )


def build_study(
    config: base_utils.StudyConfig,
    manifest: study_manifest.StudyManifest,
    *,
    continue_from: str | None = None,
) -> list:
    """Creates tables in the schema by iterating through the sql_config.file_names

    :param config: a StudyConfig object
    :param manifest: a StudyManifest object
    :keyword continue_from: Name of a sql file to resume table creation from
    :returns: loaded queries (for unit testing only)
    """
    queries = []
    for file in manifest.get_sql_file_list(continue_from):
        for query in base_utils.parse_sql(base_utils.load_text(f"{manifest._study_path}/{file}")):
            queries.append([query, file])
    if len(queries) == 0:
        return []
    for query in queries:
        query[0] = base_utils.update_query_if_schema_specified(query[0], manifest)
        query[0] = query[0].replace(
            f"`{manifest.get_study_prefix()}__",
            "`",
        )
    # We'll explicitly create a cursor since recreating cursors for each
    # table in a study is slightly slower for some databases
    cursor = config.db.cursor()
    # We want to only show a progress bar if we are :not: printing SQL lines
    with base_utils.get_progress_bar(disable=config.verbose) as progress:
        task = progress.add_task(
            f"Creating {manifest.get_study_prefix()} study in db...",
            total=len(queries),
            visible=not config.verbose,
        )
        _execute_build_queries(
            config=config,
            manifest=manifest,
            cursor=cursor,
            queries=queries,
            progress=progress,
            task=task,
        )
    return queries


def _query_error(
    config: base_utils.StudyConfig,
    manifest: study_manifest.StudyManifest,
    query_and_filename: list,
    exit_message: str,
) -> None:
    print(
        "An error occured executing the following query in ",
        f"{query_and_filename[1]}:",
        file=sys.stderr,
    )
    print("--------", file=sys.stderr)
    print(query_and_filename[0], file=sys.stderr)
    print("--------", file=sys.stderr)
    log_utils.log_transaction(config=config, manifest=manifest, status=enums.LogStatuses.ERROR)
    sys.exit(exit_message)


def _execute_build_queries(
    config: base_utils.StudyConfig,
    manifest: study_manifest.StudyManifest,
    *,
    cursor: databases.DatabaseCursor,
    queries: list,
    progress: Progress,
    task: TaskID,
) -> None:
    """Handler for executing create table queries and displaying console output.

    :param manifest: a StudyManifest object
    :param cursor: A DatabaseCursor object
    :param queries: a list of queries read from files in sql_config.file_names
    :param progress: a rich progress bar renderer
    :param task: a TaskID for a given progress bar
    """
    for query in queries:
        create_line = query[0].split("\n")[0]
        if (
            f" {manifest.get_study_prefix()}__" not in create_line
            and not manifest.get_dedicated_schema()
        ):
            _query_error(
                config,
                manifest,
                query,
                "This query does not contain the study prefix. All tables should "
                f"start with a string like `{manifest.get_study_prefix()}__`, "
                "and it should be in the first line of the query.",
            )
        if any(
            f" {manifest.get_study_prefix()}__{word.value}_" in create_line
            for word in enums.ProtectedTableKeywords
        ):
            _query_error(
                config,
                manifest,
                query,
                "This query contains a table name which contains a reserved word "
                "immediately after the study prefix. Please rename this table so "
                "that is does not begin with one of these special words "
                "immediately after the double undescore.\n Reserved words: "
                f"{(word.value for word in enums.ProtectedTableKeywords)}",
            )
        if create_line.count("__") > 1:
            _query_error(
                config,
                manifest,
                query,
                "This query contains a table name with more than one '__' in it. "
                "Double underscores are reserved for special use cases. Please "
                "rename this table so the only double undercore is after the "
                f"study prefix, e.g. `{manifest.get_study_prefix()}__`",
            )
        try:
            with base_utils.query_console_output(config.verbose, query[0], progress, task):
                cursor.execute(query[0])
        except Exception as e:  # pylint: disable=broad-exception-caught
            _query_error(
                config,
                manifest,
                query,
                "You can debug issues with this query using `sqlfluff lint`, "
                "or by executing the query directly against the database.\n"
                f"Error: {e}",
            )
