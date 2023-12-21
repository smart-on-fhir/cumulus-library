""" Builder for creating tables for tracking state/logging changes"""
from cumulus_library.base_table_builder import BaseTableBuilder
from cumulus_library.enums import ProtectedTables
from cumulus_library.template_sql.templates import (
    get_ctas_empty_query,
)

TRANSACTIONS_COLS = ["study_name", "library_version", "status", "event_time"]
STATISTICS_COLS = [
    "study_name",
    "library_version",
    "table_type",
    "table_name",
    "view_name",
    "created_on",
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
                # while it may seem redundant, study name is included for ease
                # of constructing a view of multiple transaction tables
                TRANSACTIONS_COLS,
                ["varchar", "varchar", "varchar", "timestamp"],
            )
        )
        if study_stats:
            self.queries.append(
                get_ctas_empty_query(
                    schema,
                    f"{study_name}__{ProtectedTables.STATISTICS.value}",
                    # same redundancy note about study_name, and also view_name, applies here
                    STATISTICS_COLS,
                    [
                        "varchar",
                        "varchar",
                        "varchar",
                        "varchar",
                        "varchar",
                        "timestamp",
                    ],
                )
            )
