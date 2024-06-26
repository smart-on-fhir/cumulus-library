"""A set of convenience functions for database logging"""

from cumulus_library import (
    __version__,
    base_utils,
    databases,
    enums,
    errors,
    study_parser,
)
from cumulus_library.template_sql import base_templates, sql_utils


def log_transaction(
    *,
    cursor: databases.DatabaseCursor,
    schema: str,
    manifest: study_parser.StudyManifestParser,
    status: enums.LogStatuses | str | None = enums.LogStatuses.INFO,
    message: str | None = None,
):
    if isinstance(status, str):
        try:
            status = enums.LogStatuses(status)
        except ValueError as e:
            raise errors.CumulusLibraryError(
                f"Invalid event type {status} requested for transaction log.\n"
                f"Valid types: {','.join([x.value for x in enums.LogStatuses])}"
            ) from e
    _log_table(
        table=sql_utils.TransactionsTable(),
        cursor=cursor,
        schema=schema,
        manifest=manifest,
        dataset=[
            [
                manifest.get_study_prefix(),
                __version__,
                status.value,
                base_utils.get_utc_datetime(),
                message or None,
            ]
        ],
    )


def log_statistics(
    *,
    cursor: databases.DatabaseCursor,
    schema: str,
    manifest: study_parser.StudyManifestParser,
    table_type: str,
    table_name: str,
    view_name: str,
):
    _log_table(
        table=sql_utils.StatisticsTable(),
        cursor=cursor,
        schema=schema,
        manifest=manifest,
        dataset=[
            [
                manifest.get_study_prefix(),
                __version__,
                table_type,
                table_name,
                view_name,
                base_utils.get_utc_datetime(),
            ]
        ],
    )


def _log_table(
    *,
    table: sql_utils.BaseTable,
    cursor: databases.DatabaseCursor,
    schema: str,
    manifest: study_parser.StudyManifestParser,
    dataset: list[list],
):
    if manifest and manifest.get_dedicated_schema():
        db_schema = manifest.get_dedicated_schema()
        table_name = table.name
    else:
        db_schema = schema
        table_name = f"{manifest.get_study_prefix()}__{table.name}"
    query = base_templates.get_insert_into_query(
        schema=db_schema,
        table_name=table_name,
        table_cols=table.columns,
        dataset=dataset,
        type_casts=table.type_casts,
    )
    cursor.execute(query)
