"""A set of convenience functions for database logging"""

from cumulus_library import (
    __version__,
    base_utils,
    databases,
    enums,
    errors,
    study_manifest,
)
from cumulus_library.template_sql import base_templates, sql_utils


def log_transaction(
    *,
    config: base_utils.StudyConfig,
    manifest: study_manifest.StudyManifest,
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
        config=config,
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
    config: base_utils.StudyConfig,
    manifest: study_manifest.StudyManifest,
    table_type: str,
    table_name: str,
    view_name: str,
):
    _log_table(
        table=sql_utils.StatisticsTable(),
        config=config,
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
    config: base_utils.StudyConfig,
    manifest: study_manifest.StudyManifest,
    dataset: list[list],
):
    if manifest and manifest.get_dedicated_schema():
        db_schema = manifest.get_dedicated_schema()
        table_name = table.name
    else:
        db_schema = config.schema
        table_name = f"{manifest.get_study_prefix()}__{table.name}"
    query = base_templates.get_insert_into_query(
        schema=db_schema,
        table_name=table_name,
        table_cols=table.columns,
        dataset=dataset,
        type_casts=table.type_casts,
    )
    cursor = config.db.cursor()
    try:
        cursor.execute(query)
    except config.db.operational_errors() as e:
        # Migrating logging tables
        if "lib_transactions" in table_name:
            cols = cursor.execute(
                "SELECT column_name FROM information_schema.columns "  # noqa: S608
                f"WHERE table_name ='{table_name}' "
                f"AND table_schema ='{db_schema}'"
            ).fetchall()
            cols = [col[0] for col in cols]
            # Table schema pre-v3 library release
            if sorted(cols) == [
                "event_time",
                "library_version",
                "status",
                "study_name",
            ]:
                alter_query = ""
                if isinstance(config.db, databases.AthenaDatabaseBackend):
                    alter_query = (
                        f"ALTER TABLE {db_schema}.{table_name} ADD COLUMNS(message string)"
                    )
                elif isinstance(config.db, databases.DuckDatabaseBackend):
                    alter_query = f"ALTER TABLE {db_schema}.{table_name} ADD COLUMN message varchar"
                cursor.execute(alter_query)
                cursor.execute(query)
        else:
            raise e
