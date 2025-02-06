"""Builder for creating tables for tracking state/logging changes"""

import pathlib
import tomllib

from cumulus_library import (
    BaseTableBuilder,
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


class ProtectedTableBuilder(BaseTableBuilder):
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
                f"{manifest.get_study_prefix()}__{enums.ProtectedTables.TRANSACTIONS.value}"
            )
            statistics = f"{manifest.get_study_prefix()}__{enums.ProtectedTables.STATISTICS.value}"
        self.queries.append(
            base_templates.get_ctas_empty_query(
                db_schema,
                transactions,
                TRANSACTIONS_COLS,
                TRANSACTION_COLS_TYPES,
            )
        )
        files = manifest.get_all_workflows()
        if len(files) == 0:
            return
        stats_types = set(item.value for item in enums.StatisticsTypes)
        # In this loop, we are just checking to see if :any: workflow is a stats
        # type workflow - if so, we'll create a table to hold data of stats runs
        # (if it doesn't already exist) outside of the study lifecycle for
        # persistence reasons
        for file in files:
            toml_path = pathlib.Path(f"{manifest._study_path}/{file}")
            with open(toml_path, "rb") as file:
                workflow_config = tomllib.load(file)
                if workflow_config["config_type"] in stats_types:
                    self.queries.append(
                        base_templates.get_ctas_empty_query(
                            db_schema,
                            statistics,
                            STATISTICS_COLS,
                            STATISTICS_COLS_TYPES,
                        )
                    )
                    return
