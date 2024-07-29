"""Builder for creating tables for tracking state/logging changes"""

from cumulus_library import (
    base_table_builder,
    base_utils,
    enums,
    study_manifest,
)
from cumulus_library.template_sql import base_templates

TRANSACTIONS_COLS = ["study_name", "library_version", "status", "event_time", "message"]
TRANSACTION_COLS_TYPES = ["varchar", "varchar", "varchar", "timestamp", "varchar"]
# while it may seem redundant, study_name and view_name are included as a column for
# ease of constructing a view of multiple transaction tables
STATISTICS_COLS = [
    "study_name",
    "library_version",
    "table_type",
    "table_name",
    "view_name",
    "created_on",
]
STATISTICS_COLS_TYPES = [
    "varchar",
    "varchar",
    "varchar",
    "varchar",
    "varchar",
    "timestamp",
]


class ProtectedTableBuilder(base_table_builder.BaseTableBuilder):
    """Builder for tables that persist across study clean/build actions"""

    display_text = "Creating/updating system tables..."

    def prepare_queries(
        self,
        config: base_utils.StudyConfig,
        manifest: study_manifest.StudyManifest | None = None,
        *args,
        study_stats: dict | None = None,
        **kwargs,
    ):
        study_stats = study_stats or {}
        if manifest and manifest.get_dedicated_schema():
            db_schema = manifest.get_dedicated_schema()
            transactions = enums.ProtectedTables.TRANSACTIONS.value
            statistics = enums.ProtectedTables.STATISTICS.value
        else:
            db_schema = config.schema
            transactions = (
                f"{manifest.get_study_prefix()}" f"__{enums.ProtectedTables.TRANSACTIONS.value}"
            )
            statistics = (
                f"{manifest.get_study_prefix()}" f"__{enums.ProtectedTables.STATISTICS.value}"
            )
        self.queries.append(
            base_templates.get_ctas_empty_query(
                db_schema,
                transactions,
                TRANSACTIONS_COLS,
                TRANSACTION_COLS_TYPES,
            )
        )
        if manifest._study_config.get("statistics_config"):
            self.queries.append(
                base_templates.get_ctas_empty_query(
                    db_schema,
                    statistics,
                    STATISTICS_COLS,
                    STATISTICS_COLS_TYPES,
                )
            )
