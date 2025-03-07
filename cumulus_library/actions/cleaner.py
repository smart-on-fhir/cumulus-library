import sys

import rich
from rich import progress

from cumulus_library import base_utils, databases, enums, errors, study_manifest
from cumulus_library.template_sql import base_templates


def _execute_drop_queries(
    cursor: databases.DatabaseCursor,
    verbose: bool,
    view_table_list: list,
    progress: progress.Progress,
    task: progress.TaskID,
) -> None:
    """Handler for executing drop view/table queries and displaying console output.

    :param cursor: A DatabaseCursor object
    :param verbose: toggle from progress bar to query output
    :param view_table_list: a list of views and tables beginning with
      the study prefix
    :param progress: a rich progress bar renderer
    :param task: a TaskID for a given progress bar
    """
    for view_table in view_table_list:
        drop_view_table = base_templates.get_drop_view_table(
            name=view_table[0], view_or_table=view_table[1]
        )
        with base_utils.query_console_output(verbose, drop_view_table, progress, task):
            cursor.execute(drop_view_table)


def _get_unprotected_stats_view_table(
    config: base_utils.StudyConfig,
    query: str,
    artifact_type: str,
    drop_prefix: str,
    display_prefix: str,
    stats_clean: bool,
):
    """Gets all items from the database by type, less any protected items

    :param config: A StudyConfig object
    :param query: A query to get the raw list of items from the db
    :param artifact_type: either 'table' or 'view'
    :param drop_prefix: The prefix requested to drop
    :param display_prefix: The expected study prefix
    :param stats_clean: A boolean indicating if stats tables are being cleaned

    :returns: a list of study tables to drop
    """
    cursor = config.db.cursor()
    unprotected_list = []
    db_contents = cursor.execute(query).fetchall()
    if (
        f"{drop_prefix}{enums.ProtectedTables.STATISTICS.value}",
    ) in db_contents and not stats_clean:
        protected_list = cursor.execute(
            f"""SELECT {artifact_type.lower()}_name 
            FROM {drop_prefix}{enums.ProtectedTables.STATISTICS.value}
            WHERE study_name = '{display_prefix}'"""  # noqa: S608
        ).fetchall()
        for protected_tuple in protected_list:
            if protected_tuple in db_contents:
                db_contents.remove(protected_tuple)
    for db_row_tuple in db_contents:
        # this check handles athena reporting views as also being tables,
        # so we don't waste time dropping things that don't exist
        if artifact_type == "TABLE":
            if not any(db_row_tuple[0] in iter_q_and_t for iter_q_and_t in unprotected_list):
                unprotected_list.append([db_row_tuple[0], artifact_type])
        else:
            unprotected_list.append([db_row_tuple[0], artifact_type])
    return unprotected_list


def clean_study(
    config: base_utils.StudyConfig,
    manifest: study_manifest.StudyManifest | None,
    prefix: str | None = None,
) -> list:
    """Removes tables beginning with the study prefix from the database schema

    :param config: a StudyConfig object
    :param manifest: a StudyManifest object
    :keyword prefix: override manifest-based prefix discovery with the provided prefix
    :returns: list of dropped tables (for unit testing only)

    """
    if not prefix and not manifest:
        raise errors.CumulusLibraryError(
            "Either a manifest parser or a filter prefix must be provided"
        )
    if manifest and manifest.get_dedicated_schema():
        drop_prefix = ""
        display_prefix = ""
    elif not prefix:
        drop_prefix = f"{manifest.get_study_prefix()}__"
        display_prefix = manifest.get_study_prefix()
    else:
        drop_prefix = prefix
        display_prefix = drop_prefix

    if config.stats_clean:
        confirm = input(
            "This will remove all historical stats tables in the "
            f"{display_prefix} study - are you sure? (y/N)"
        )
        if confirm is None or confirm.lower() not in ("y", "yes"):
            sys.exit("Table cleaning aborted")

    cursor = config.db.cursor()
    view_sql = base_templates.get_show_views(config.schema, drop_prefix)
    table_sql = base_templates.get_show_tables(config.schema, drop_prefix)
    view_table_list = []
    for query_and_type in [[view_sql, "VIEW"], [table_sql, "TABLE"]]:
        view_table_list += _get_unprotected_stats_view_table(
            config,
            query_and_type[0],
            query_and_type[1],
            drop_prefix,
            display_prefix,
            config.stats_clean,
        )
    if not view_table_list:
        return view_table_list

    # We'll do a pass to see if any of these tables were created outside of a
    # study builder, and remove them from the list.
    for view_table in view_table_list.copy():
        if any(
            (
                f"__{word.value}_" in view_table[0]
                or view_table[0].endswith(f"__{word.value}")
                or view_table[0].startswith(f"{word.value}_")
            )
            for word in enums.ProtectedTableKeywords
        ):
            view_table_list.remove(view_table)
    if prefix:
        rich.print("The following views/tables were selected by prefix:")
        for view_table in view_table_list:
            rich.print(f"  {view_table[0]}")
        confirm = input("Remove these tables? (y/N)")
        if confirm.lower() not in ("y", "yes"):
            sys.exit("Table cleaning aborted")
    if dedicated := manifest.get_dedicated_schema():
        view_table_list = [
            (
                # Athena uses different quoting strategies for drop view statements
                # versus drop table statements. -_-
                # TODO: Consider moving this logic to a database object?
                f"`{dedicated}`.`{x[0]}`"
                if (x[1] == "TABLE" and config.db.db_type == "athena")
                else f'"{dedicated}"."{x[0]}"',
                x[1],
            )
            for x in view_table_list
        ]
    # We want to only show a progress bar if we are :not: printing SQL lines
    with base_utils.get_progress_bar(disable=config.verbose) as progress:
        task = progress.add_task(
            f"Removing {display_prefix} study artifacts...",
            total=len(view_table_list),
            visible=not config.verbose,
        )
        _execute_drop_queries(
            cursor,
            config.verbose,
            view_table_list,
            progress,
            task,
        )
    # if we're doing a stats clean, we'll also remove the table containing the
    # list of protected tables
    if config.stats_clean:
        drop_query = base_templates.get_drop_view_table(
            f"{drop_prefix}{enums.ProtectedTables.STATISTICS.value}", "TABLE"
        )
        cursor.execute(drop_query)

    return view_table_list
