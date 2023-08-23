"""Class for generating counts tables from templates"""
import sys

from pathlib import Path
from typing import Union

from cumulus_library.base_table_builder import BaseTableBuilder
from cumulus_library.study_parser import StudyManifestParser
from cumulus_library.template_sql import templates
from cumulus_library.errors import CountsBuilderError


class CountsBuilder(BaseTableBuilder):
    """Extends BaseTableBuilder for counts-related use cases"""

    def __init__(self, study_prefix: str = None):
        if study_prefix is None:
            # This slightly wonky approach will give us the path of the
            # directory of a class extending the CountsBuilder
            study_path = Path(sys.modules[self.__module__].__file__).parent

            try:
                parser = StudyManifestParser(study_path)
                self.study_prefix = parser.get_study_prefix()
            except Exception as e:
                raise CountsBuilderError(
                    "CountsBuilder must be either initiated with a study prefix, "
                    "or be in a directory with a valid manifest.toml"
                ) from e
        else:
            self.study_prefix = study_prefix
        super().__init__()

    def get_table_name(self, table_name: str, duration=None) -> str:
        """Convenience method for constructing table name"""
        if duration:
            return f"{self.study_prefix}__{table_name}_{duration}"
        else:
            return f"{self.study_prefix}__{table_name}"

    def get_where_clauses(
        self, clause: Union[list, str, None] = None, min_subject: int = 10
    ) -> str:
        """convenience method for constructing where clauses"""
        if clause is None:
            return [f"cnt_subject >= {min_subject}"]
        elif isinstance(clause, str):
            return [clause]
        elif isinstance(clause, list):
            return clause
        else:
            raise CountsBuilderError(f"get_where_clauses invalid clause {clause}")

    def get_count_query(
        self, table_name: str, source_table: str, table_cols: list, **kwargs
    ) -> str:
        """Wrapper method for generating a counts table from a template"""
        if not table_name or not source_table or not table_cols:
            raise CountsBuilderError(
                "count_query missing required arguments. " f"output table: {table_name}"
            )
        for key in kwargs:
            if key not in ["min_subject", "where_clauses", "cnt_encounter"]:
                raise CountsBuilderError(f"count_query received unexpected key: {key}")
        return templates.get_count_query(table_name, source_table, table_cols, **kwargs)

    def count_patient(
        self,
        table_name: str,
        source_table: str,
        table_cols: list,
        where_clauses=None,
    ) -> str:
        """wrapper method for constructing patient counts tables"""
        return self.get_count_query(
            table_name, source_table, table_cols, where_clauses=where_clauses
        )

    def count_encounter(
        self, table_name: str, source_table: str, table_cols: list, where_clauses=None
    ) -> str:
        """wrapper method for constructing encounter counts tables"""
        return self.get_count_query(
            table_name,
            source_table,
            table_cols,
            where_clauses=where_clauses,
            cnt_encounter=True,
        )

    def write_counts(self, filepath: str):
        """Convenience method for writing counts queries to disk"""
        self.prepare_queries(cursor=None, schema=None)
        self.comment_queries()
        self.write_queries(filename=filepath)

    def prepare_queries(self, cursor: object = None, schema: str = None):
        pass
