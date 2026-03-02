"""Builder for creating tables for tracking state/logging changes"""

import pathlib
import tomllib

from cumulus_library import (
    BaseTableBuilder,
    base_utils,
    const,
    enums,
    study_manifest,
)
from cumulus_library.template_sql import base_templates


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
            self.queries.append(f"CREATE SCHEMA IF NOT EXISTS {db_schema}")
            transactions = enums.ProtectedTables.TRANSACTIONS.value
            statistics = enums.ProtectedTables.STATISTICS.value
            build_source = enums.ProtectedTables.BUILD_SOURCE.value
        else:
            db_schema = config.schema
            transactions = (
                f"{manifest.get_study_prefix()}__{enums.ProtectedTables.TRANSACTIONS.value}"
            )
            statistics = f"{manifest.get_study_prefix()}__{enums.ProtectedTables.STATISTICS.value}"
            build_source = (
                f"{manifest.get_study_prefix()}__{enums.ProtectedTables.BUILD_SOURCE.value}"
            )
        self.queries.append(
            base_templates.get_ctas_empty_query(
                db_schema,
                transactions,
                const.TRANSACTIONS_COLS,
                const.TRANSACTION_COLS_TYPES,
            )
        )
        self.queries.append(
            base_templates.get_ctas_crud_query(
                schema_name=db_schema,
                table_name=build_source,
                remote_location=f"{config.db.get_remote_path()}/iceberg/build_source/",
                table_cols=const.BUILD_SOURCE_COLS,
                sql_col_types=const.BUILD_SOURCE_COLS_SQL_TYPE,
                athena_col_types=const.BUILD_SOURCE_COLS_ATHENA_TYPE,
            )
        )
        files = manifest.get_all_workflows(config.stage)
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
                            const.STATISTICS_COLS,
                            const.STATISTICS_COLS_TYPES,
                        )
                    )
                    return
