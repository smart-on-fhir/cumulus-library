""" Builder for creating tables for tracking state/logging changes"""
from cumulus_library.base_table_builder import BaseTableBuilder
from cumulus_library.enums import ProtectedTables
from cumulus_library.template_sql.templates import (
    get_ctas_empty_query,
)

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


class ProtectedTableBuilder(BaseTableBuilder):
    """Builder for tables that persist across study clean/build actions"""

    display_text = "Creating/updating system tables..."

    def prepare_queries(
        self, cursor: object, schema: str, study_name: str, study_stats: dict
    ):
        self.queries.append(
            get_ctas_empty_query(
                schema,
                f"{study_name}__{ProtectedTables.TRANSACTIONS.value}",
                TRANSACTIONS_COLS,
                TRANSACTION_COLS_TYPES,
            )
        )
        if study_stats:
            self.queries.append(
                get_ctas_empty_query(
                    schema,
                    f"{study_name}__{ProtectedTables.STATISTICS.value}",
                    STATISTICS_COLS,
                    STATISTICS_COLS_TYPES,
                )
            )
