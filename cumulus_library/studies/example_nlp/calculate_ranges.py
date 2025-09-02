"""Module for generating age ranges table from NLP"""

import pathlib

import cumulus_library
from cumulus_library import base_utils, databases
from cumulus_library.template_sql import sql_utils


class CalculateRangesBuilder(cumulus_library.BaseTableBuilder):
    display_text = "Calculating age ranges..."

    @staticmethod
    def _is_table_valid(database: databases.DatabaseBackend, table_name: str) -> bool:
        return sql_utils.is_field_present(
            database=database,
            source_table=table_name,
            source_col="note_ref",
            expected=[],
        )

    def _get_valid_nlp_tables(self, database: databases.DatabaseBackend) -> set[str]:
        source_tables = [
            "example_nlp__nlp_gpt4",
            "example_nlp__nlp_gpt4o",
            "example_nlp__nlp_gpt5",
            "example_nlp__nlp_gpt_oss_120b",
            "example_nlp__nlp_llama4_scout",
        ]
        valid_tables = set()
        with base_utils.get_progress_bar() as progress:
            task = progress.add_task(
                "Discovering available NLP tables...",
                total=len(source_tables),
            )
            for source_table in source_tables:
                if self._is_table_valid(database, source_table):
                    valid_tables.add(source_table)
                progress.advance(task)
        return valid_tables

    def prepare_queries(
        self,
        *args,
        config: cumulus_library.StudyConfig,
        **kwargs,
    ):
        valid_tables = self._get_valid_nlp_tables(config.db)
        query = cumulus_library.get_template(
            "calculate_ranges",
            pathlib.Path(__file__).parent,
            table_names=valid_tables,
        )
        self.queries.append(query)
