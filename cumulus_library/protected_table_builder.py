"""Builder for creating tables for tracking state/logging changes"""

from cumulus_library import base_table_builder, enums
from cumulus_library.template_sql import base_templates

TRANSACTIONS_COLS = ["study_name", "library_version", "status", "event_time"]
TRANSACTION_COLS_TYPES = ["varchar", "varchar", "varchar", "timestamp"]
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
        cursor: object,
        schema: str,
        study_name: str,
        study_stats: dict,
        *args,
        **kwargs,
    ):
        self.queries.append(
            base_templates.get_ctas_empty_query(
                schema,
                f"{study_name}__{enums.ProtectedTables.TRANSACTIONS.value}",
                TRANSACTIONS_COLS,
                TRANSACTION_COLS_TYPES,
            )
        )
        if study_stats:
            self.queries.append(
                base_templates.get_ctas_empty_query(
                    schema,
                    f"{study_name}__{enums.ProtectedTables.STATISTICS.value}",
                    STATISTICS_COLS,
                    STATISTICS_COLS_TYPES,
                )
            )
