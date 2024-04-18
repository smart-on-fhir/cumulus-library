"""Class for generating counts tables from templates"""

import sys
from pathlib import Path

from cumulus_library.base_table_builder import BaseTableBuilder
from cumulus_library.errors import CountsBuilderError
from cumulus_library.statistics.statistics_templates import counts_templates
from cumulus_library.study_parser import StudyManifestParser


class CountsBuilder(BaseTableBuilder):
    """Extends BaseTableBuilder for counts-related use cases"""

    def __init__(self, study_prefix: str | None = None):
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
        """Convenience method for constructing table name

        :param table_name: table name to add after the study prefix
        :param duration: a time period reflecting the table binning strategy
        """
        if duration:
            return f"{self.study_prefix}__{table_name}_{duration}"
        else:
            return f"{self.study_prefix}__{table_name}"

    def get_where_clauses(
        self, clause: list | str | None = None, min_subject: int = 10
    ) -> str:
        """Convenience method for constructing arbitrary where clauses.

        :param clause: either a string or a list of sql where statements
        :param min_subject: if clause is none, the bin size for a cnt_subject_ref filter
            (deprecated, use count_[fhir_resource](min_subject) instead)
        """
        if clause is None:
            return [f"cnt_subject_ref >= {min_subject}"]
        elif isinstance(clause, str):
            return [clause]
        elif isinstance(clause, list):
            return clause
        else:
            raise CountsBuilderError(f"get_where_clauses invalid clause {clause}")

    def get_count_query(
        self, table_name: str, source_table: str, table_cols: list, **kwargs
    ) -> str:
        """Generates a counts table using a template

        :param table_name: The name of the table to create. Must start with study prefix
        :param source_table: The table to create counts data from
        :param table_cols: The columns from the source table to add to the count table
        :keyword where_clauses: An array of where clauses to use for filtering the data
        :keyword min_subject: An integer setting the minimum bin size for inclusion
            (default: 10)
        :keyword fhir_resource: The type of FHIR resource to count (see
            statistics/statistics_templates/count_templates.CountableFhirResource)
        """
        if not table_name or not source_table or not table_cols:
            raise CountsBuilderError(
                "count_query missing required arguments. " f"output table: {table_name}"
            )
        for key in kwargs:
            if key not in [
                "min_subject",
                "where_clauses",
                "fhir_resource",
                "filter_resource",
            ]:
                raise CountsBuilderError(f"count_query received unexpected key: {key}")
        return counts_templates.get_count_query(
            table_name, source_table, table_cols, **kwargs
        )

    # ----------------------------------------------------------------------
    # The following function all wrap get_count_query as convenience methods.
    # We're not trying to be overly clever about this to persist the docstrings as the
    # primary interface that study authors would see when using these functions.

    def count_condition(
        self,
        table_name: str,
        source_table: str,
        table_cols: list,
        where_clauses: list | None = None,
        min_subject: int = 10,
    ) -> str:
        """wrapper method for constructing condition counts tables

        :param table_name: The name of the table to create. Must start with study prefix
        :param source_table: The table to create counts data from
        :param table_cols: The columns from the source table to add to the count table
        :param where_clauses: An array of where clauses to use for filtering the data
        :param min_subject: An integer setting the minimum bin size for inclusion
            (default: 10)
        """
        return self.get_count_query(
            table_name,
            source_table,
            table_cols,
            where_clauses=where_clauses,
            min_subject=min_subject,
            fhir_resource="condition",
            filter_resource="encounter",
        )

    def count_documentreference(
        self,
        table_name: str,
        source_table: str,
        table_cols: list,
        where_clauses: list | None = None,
        min_subject: int = 10,
    ) -> str:
        """wrapper method for constructing documentreference counts tables

        :param table_name: The name of the table to create. Must start with study prefix
        :param source_table: The table to create counts data from
        :param table_cols: The columns from the source table to add to the count table
        :param where_clauses: An array of where clauses to use for filtering the data
        :param min_subject: An integer setting the minimum bin size for inclusion
            (default: 10)
        """
        return self.get_count_query(
            table_name,
            source_table,
            table_cols,
            where_clauses=where_clauses,
            min_subject=min_subject,
            fhir_resource="documentreference",
            filter_resource="encounter",
        )

    def count_encounter(
        self,
        table_name: str,
        source_table: str,
        table_cols: list,
        where_clauses: list | None = None,
        min_subject: int = 10,
    ) -> str:
        """wrapper method for constructing encounter counts tables

        :param table_name: The name of the table to create. Must start with study prefix
        :param source_table: The table to create counts data from
        :param table_cols: The columns from the source table to add to the count table
        :param where_clauses: An array of where clauses to use for filtering the data
        :param min_subject: An integer setting the minimum bin size for inclusion
            (default: 10)
        """
        return self.get_count_query(
            table_name,
            source_table,
            table_cols,
            where_clauses=where_clauses,
            min_subject=min_subject,
            fhir_resource="encounter",
        )

    def count_medicationrequest(
        self,
        table_name: str,
        source_table: str,
        table_cols: list,
        where_clauses: list | None = None,
        min_subject: int = 10,
    ) -> str:
        """wrapper method for constructing medicationrequests counts tables

        :param table_name: The name of the table to create. Must start with study prefix
        :param source_table: The table to create counts data from
        :param table_cols: The columns from the source table to add to the count table
        :param where_clauses: An array of where clauses to use for filtering the data
        :param min_subject: An integer setting the minimum bin size for inclusion
            (default: 10)
        """
        return self.get_count_query(
            table_name,
            source_table,
            table_cols,
            where_clauses=where_clauses,
            min_subject=min_subject,
            fhir_resource="medicationrequest",
        )

    def count_observation(
        self,
        table_name: str,
        source_table: str,
        table_cols: list,
        where_clauses: list | None = None,
        min_subject: int = 10,
    ) -> str:
        """wrapper method for constructing observation counts tables

        :param table_name: The name of the table to create. Must start with study prefix
        :param source_table: The table to create counts data from
        :param table_cols: The columns from the source table to add to the count table
        :param where_clauses: An array of where clauses to use for filtering the data
        :param min_subject: An integer setting the minimum bin size for inclusion
            (default: 10)
        """
        return self.get_count_query(
            table_name,
            source_table,
            table_cols,
            where_clauses=where_clauses,
            min_subject=min_subject,
            fhir_resource="observation",
        )

    def count_patient(
        self,
        table_name: str,
        source_table: str,
        table_cols: list,
        where_clauses: list | None = None,
        min_subject: int = 10,
    ) -> str:
        """wrapper method for constructing patient counts tables

        :param table_name: The name of the table to create. Must start with study prefix
        :param source_table: The table to create counts data from
        :param table_cols: The columns from the source table to add to the count table
        :param where_clauses: An array of where clauses to use for filtering the data
        :param min_subject: An integer setting the minimum bin size for inclusion
            (default: 10)
        """
        return self.get_count_query(
            table_name,
            source_table,
            table_cols,
            where_clauses=where_clauses,
            min_subject=min_subject,
            fhir_resource="patient",
        )

    # End of wrapper section
    # ----------------------------------------------------------------------

    def write_counts(self, filepath: str):
        """Convenience method for writing counts queries to disk

        :param filepath: path to file to write queries out to.
        """
        self.prepare_queries(cursor=None, schema=None)
        self.comment_queries()
        self.write_queries(path=Path(filepath))

    def prepare_queries(self, cursor: object | None = None, schema: str | None = None):
        """Stub implementing abstract base class

        This should be overridden in any count generator. See studies/core/count_core.py
        for an example
        """
        pass
