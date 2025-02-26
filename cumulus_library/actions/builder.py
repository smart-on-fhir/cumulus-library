"""Handles the creation of new tables"""

import contextlib
import importlib.util
import inspect
import pathlib
import re
import sys
import tomllib
import zipfile

import rich
from rich import progress

from cumulus_library import (
    BaseTableBuilder,
    base_utils,
    databases,
    enums,
    errors,
    log_utils,
    study_manifest,
)
from cumulus_library.builders import protected_table_builder, psm_builder, valueset_builder


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
    prepare: bool = False,
    data_path: pathlib.Path,
    query_count: int | None = None,
) -> int:
    """Loads a table builder from a file.

    :param config: a StudyConfig object
    :param manifest: a StudyManifest object
    :keyword filename: filename of a module implementing a TableBuilder
    :keyword db_parser: an object implementing DatabaseParser for the target database
    :keyword write_reference_sql: if true, writes sql to disk inside a study's directory
    :keyword doc_str: A string to insert between queries written to disk
    :keyword prepare: If true, will render query instead of executing
    :keyword data_path: If prepare is true, the path to write rendered data to
    :keyword query_count: if prepare is true, the number of queries already rendered
    :returns: number of processed queries
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
        if issubclass(cls_obj, BaseTableBuilder) and cls_obj != BaseTableBuilder:
            table_builder_subclasses.append(cls_obj)

    if len(table_builder_subclasses) == 0:
        raise errors.StudyManifestParsingError(
            f"Error loading {manifest._study_path}{filename}\n"
            "Custom builders must extend the BaseTableBuilder class."
        )
    # Remove instances of intermediate classes, if present (usually not)
    table_builder_subclasses = list(
        filter(lambda x: x.__name__ != "CountsBuilder", table_builder_subclasses)
    )

    # We'll get the subclass, initialize it, run the executor code, and then
    # remove it so it doesn't interfere with the next python module to
    # execute, since the subclass would otherwise hang around.
    table_builder_class = table_builder_subclasses[0]
    table_builder = table_builder_class(manifest=manifest)
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
    elif prepare:
        table_builder.prepare_queries(
            config=config,
            manifest=manifest,
            parser=db_parser,
        )
        _render_output(
            manifest.get_study_prefix(), table_builder.queries, data_path, filename, query_count
        )
    else:
        table_builder.execute_queries(
            config=config,
            manifest=manifest,
            parser=db_parser,
        )
    num_queries = len(table_builder.queries)
    # After running the executor code, we'll remove
    # it so it doesn't interfere with the next python module to
    # execute, since the subclass would otherwise hang around.
    del sys.modules[table_builder_module.__name__]
    del table_builder_module

    return num_queries


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


def _run_workflow(
    config: base_utils.StudyConfig,
    manifest: study_manifest.StudyManifest,
    filename: str,
    prepare: str,
    data_path: pathlib.Path,
    query_count: int,
) -> int:
    """Loads workflow config from toml definitions and executes workflow

    :param config: a StudyConfig object
    :param manifest: a StudyManifest object
    :param filename: Filename of the workflow config
    :param prepare: If true, will render query instead of executing
    :param data_path: If prepare is true, the path to write rendered data to
    :param query_count: if prepare is true, the number of queries already rendered
    :returns: a count of processed queries
    """
    toml_path = pathlib.Path(f"{manifest._study_path}/{filename}")
    if prepare:
        with open(toml_path, encoding="utf-8") as file:
            workflow_config = file.read()
        _render_output(
            manifest.get_study_prefix(),
            [workflow_config],
            data_path,
            filename,
            query_count,
            is_toml=True,
        )
        return 1
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
    # This open is a bit redundant with the open inside of the PSM builder,
    # but we're letting it slide so that builders function similarly
    # across the board
    safe_timestamp = base_utils.get_tablename_safe_iso_timestamp()
    with open(toml_path, "rb") as file:
        workflow_config = tomllib.load(file)
        config_type = workflow_config["config_type"]
        target_table = workflow_config.get("target_table", workflow_config.get("table_prefix", ""))

    if (target_table,) in existing_stats and not config.stats_build:
        return 0
    match config_type:
        case "psm":
            builder = psm_builder.PsmBuilder(
                toml_config_path=toml_path,
                config=workflow_config,
                data_path=manifest.data_path / f"{manifest.get_study_prefix()}/psm",
            )
        case "valueset":
            builder = valueset_builder.ValuesetBuilder(
                toml_config_path=toml_path,
                config=workflow_config,
                data_path=manifest.data_path / f"{manifest.get_study_prefix()}/valueset",
            )
        case _:  # pragma: no cover
            raise errors.StudyManifestParsingError(
                f"{toml_path} references an invalid workflow type {config_type}."
            )
    builder.execute_queries(
        config=config,
        manifest=manifest,
        table_suffix=safe_timestamp,
    )
    if config_type in set(item.value for item in enums.StatisticsTypes):
        log_utils.log_statistics(
            config=config,
            manifest=manifest,
            table_type=config_type,
            table_name=f"{target_table}_{safe_timestamp}",
            view_name=target_table,
        )
    return len(builder.queries)


def build_matching_files(
    config: base_utils.StudyConfig,
    manifest: study_manifest.StudyManifest,
    *,
    builder: str | None,
    db_parser: databases.DatabaseParser = None,
    prepare: bool,
    data_path: pathlib.Path,
):
    """targets all table builders matching a target string for running

    :param config: a StudyConfig object
    :param manifest: a StudyManifest object
    :keyword builder: filename of a module implementing a TableBuilder
    :keyword db_parser: an object implementing DatabaseParser for the target database
    :keyword prepare: If true, will render query instead of executing
    :keyword data_path: If prepare is true, the path to write rendered data to
    """
    if prepare:
        _check_if_preparable(manifest.get_study_prefix())  # pragma: no cover
    all_generators = manifest.get_all_generators()
    matches = []
    if not builder:  # pragma: no cover
        matches = all_generators
    else:
        for file in all_generators:
            if file.find(builder) != -1:
                matches.append(file)
    build_study(
        config,
        manifest,
        db_parser=db_parser,
        file_list=matches,
        prepare=prepare,
        data_path=data_path,
    )


def build_study(
    config: base_utils.StudyConfig,
    manifest: study_manifest.StudyManifest,
    *,
    db_parser: databases.DatabaseParser = None,
    continue_from: str | None = None,
    file_list: list | None = None,
    prepare: bool,
    data_path: pathlib.Path | None,
) -> None:
    """Creates tables in the schema by iterating through the sql_config.file_names

    :param config: a StudyConfig object
    :param manifest: a StudyManifest object
    :keyword db_parser: a parser for the target database
    :keyword continue_from: Name of a file to resume table creation from
    :keyword prepare: If true, will render query instead of executing
    :keyword data_path: If prepare is true, the path to write rendered data to
    """
    if prepare:
        _check_if_preparable(manifest.get_study_prefix())
    if file_list is None:
        file_list = manifest.get_file_list(continue_from)
    if prepare:
        data_dir = data_path / manifest.get_study_prefix()
        for file in data_dir.glob("*"):
            if file.is_file():  # pragma: no cover
                file.unlink()
    query_count = 0
    for file in file_list:
        if file.endswith(".py"):
            query_count += _load_and_execute_builder(
                config=config,
                manifest=manifest,
                filename=file,
                db_parser=db_parser,
                data_path=data_path,
                prepare=prepare,
                query_count=query_count,
            )
        elif file.endswith(".toml"):
            query_count += _run_workflow(
                config=config,
                manifest=manifest,
                filename=file,
                data_path=data_path,
                prepare=prepare,
                query_count=query_count,
            )
        elif file.endswith(".sql"):
            query_count += _run_raw_queries(
                config=config,
                manifest=manifest,
                filename=file,
                data_path=data_path,
                prepare=prepare,
                query_count=query_count,
            )
        else:
            raise errors.StudyManifestParsingError
    if prepare:
        with zipfile.ZipFile(
            f"{data_path}/{manifest.get_study_prefix()}.zip", "w", zipfile.ZIP_DEFLATED
        ) as z:
            for file in data_dir.iterdir():
                z.write(file, file.relative_to(data_dir))


def _run_raw_queries(
    config: base_utils.StudyConfig,
    manifest: study_manifest.StudyManifest,
    filename: str,
    *,
    data_path: pathlib.Path | None,
    prepare: bool,
    query_count: int,
) -> int:
    """Creates tables in the schema by iterating through the sql_config.file_names

    :param config: a StudyConfig object
    :param manifest: a StudyManifest object
    :param filename: the name of the sql file to read
    :param prepare: If true, will render query instead of executing
    :param data_path: If prepare is true, the path to write rendered data to
    :param query_count: the number of queries currently processed
    :returns number of queries processed:
    """
    queries = []
    for query in base_utils.parse_sql(base_utils.load_text(f"{manifest._study_path}/{filename}")):
        queries.append([query, filename])
    for query in queries:
        query[0] = base_utils.update_query_if_schema_specified(query[0], manifest)
        query[0] = query[0].replace(
            f"`{manifest.get_study_prefix()}__",
            "`",
        )
    if prepare:
        _render_output(
            manifest.get_study_prefix(), [q[0] for q in queries], data_path, filename, query_count
        )
    else:
        # We'll explicitly create a cursor since recreating cursors for each
        # table in a study is slightly slower for some databases
        cursor = config.db.cursor()
        # We want to only show a progress bar if we are :not: printing SQL lines
        with base_utils.get_progress_bar(disable=config.verbose) as progress:
            task = progress.add_task(
                f"Building tables from {filename}...",
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
    return len(queries)


def _render_output(
    study_name: str,
    outputs: list,
    data_path: pathlib.Path,
    filename: str,
    count: int,
    *,
    is_toml: bool = False,
):
    if is_toml:
        suffix = "toml"
    else:
        suffix = "sql"
    for index, output in enumerate(outputs):
        if is_toml:
            name = "config"
        else:
            # This regex attempts to discover the table name, via looking for the first
            # dunder, and then gets the start of the line its on as a non-quote requiring
            # part of a file name. So for example, finding SQL that looks like this:
            #   CREATE TABLE foo__bar AS (varchar baz)
            # would result in `create_table_foo__bar` being assigned to name. The goal
            # is to make this at least mildly parsable at the file system level if someone
            # is reviewing a prepared study
            name = re.search(r"(.*)__\w*", output)[0].lower().replace(" ", "_")
        total_index = count + index
        new_filename = f"{total_index:04d}.{filename.rsplit('.', 1)[0]}.{index:02d}.{name}.{suffix}"
        file_path = data_path / f"{study_name}/{new_filename}"

        file_path.parent.mkdir(exist_ok=True, parents=True)
        with open(file_path, "w", encoding="UTF-8") as f:
            f.write(output)


def _check_if_preparable(prefix):
    # This list should include any study which requires interrogating the database to
    # find if data is available to query (outside of a toml-driven workflow),
    # which isn't doable as a distributed query
    if prefix in ["core", "discovery", "data-metrics"]:
        sys.exit(
            f"Study '{prefix}'' does not support prepare mode. It must be run "
            "directly against a target database."
        )


def _query_error(
    config: base_utils.StudyConfig,
    manifest: study_manifest.StudyManifest,
    query_and_filename: list,
    exit_message: str,
) -> None:
    rich.print(
        "An error occurred executing the following query in ",
        f"{query_and_filename[1]}:",
        file=sys.stderr,
    )
    rich.print("--------", file=sys.stderr)
    rich.print(query_and_filename[0], file=sys.stderr)
    rich.print("--------", file=sys.stderr)
    log_utils.log_transaction(config=config, manifest=manifest, status=enums.LogStatuses.ERROR)
    rich.print(exit_message)

    if any(init_error in exit_message for init_error in config.db.init_errors()):
        rich.print(
            "Have you initialized your database?\n"
            "https://docs.smarthealthit.org/cumulus/etl/setup/initialization.html"
        )
    sys.exit()


def _execute_build_queries(
    config: base_utils.StudyConfig,
    manifest: study_manifest.StudyManifest,
    *,
    cursor: databases.DatabaseCursor,
    queries: list,
    progress: progress.Progress,
    task: progress.TaskID,
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
