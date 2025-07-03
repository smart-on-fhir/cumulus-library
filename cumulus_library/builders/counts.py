"""Class for generating counts tables from templates"""

import pathlib

import rich

from cumulus_library import BaseTableBuilder, errors, study_manifest
from cumulus_library.builders.statistics_templates import counts_templates

# Defined here for easy overriding by tests
DEFAULT_MIN_SUBJECT = 10


class CountsBuilder(BaseTableBuilder):
    """Extends BaseTableBuilder for counts-related use cases"""

    def __init__(
        self, study_prefix: str | None = None, manifest: study_manifest.StudyManifest | None = None
    ):
        super().__init__()
        if manifest:
            self.study_prefix = manifest.get_study_prefix()
        elif study_prefix:
            c = rich.get_console()
            c.print(
                "[yellow]Warning: providing study_prefix to a CountsBuilder is deprecated"
                " and will be removed in a future version"
            )
            self.study_prefix = study_prefix
        else:
            raise errors.CountsBuilderError(
                "CountsBuilder should be initiated with a valid manifest.toml"
            )

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
        self, clause: list | str | None = None, min_subject: int | None = None
    ) -> list[str]:
        """Convenience method for constructing arbitrary where clauses.

        :param clause: either a string or a list of sql where statements
        :param min_subject: if clause is none, the bin size for a cnt_subject_ref filter
            (deprecated, use count_[fhir_resource](min_subject) instead)
        """
        if min_subject is None:
            min_subject = DEFAULT_MIN_SUBJECT
        if clause is None:
            return [f"cnt_subject_ref >= {min_subject}"]
        elif isinstance(clause, str):
            return [clause]
        elif isinstance(clause, list):
            return clause
        else:
            raise errors.CountsBuilderError(f"get_where_clauses invalid clause {clause}")

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
            builders/statistics_templates/count_templates.CountableFhirResource)
        """
        if not table_name or not source_table or not table_cols:
            raise errors.CountsBuilderError(
                f"count_query missing required arguments. output table: {table_name}"
            )

        for key in kwargs:
            if key not in [
                "min_subject",
                "where_clauses",
                "fhir_resource",
                "filter_resource",
                "patient_link",
                "annotation",
            ]:
                raise errors.CountsBuilderError(f"count_query received unexpected key: {key}")
        if "min_subject" in kwargs and kwargs["min_subject"] is None:
            kwargs["min_subject"] = DEFAULT_MIN_SUBJECT
        return counts_templates.get_count_query(table_name, source_table, table_cols, **kwargs)

    # ----------------------------------------------------------------------
    # The following function all wrap get_count_query as convenience methods.
    # We're not trying to be overly clever about this to persist the docstrings as the
    # primary interface that study authors would see when using these functions.

    def count_allergyintolerance(
        self,
        table_name: str,
        source_table: str,
        table_cols: list,
        where_clauses: list | None = None,
        min_subject: int | None = None,
        annotation: counts_templates.CountAnnotation | None = None,
    ) -> str:
        """wrapper method for constructing allergyintolerance counts tables

        :param table_name: The name of the table to create. Must start with study prefix
        :param source_table: The table to create counts data from
        :param table_cols: The columns from the source table to add to the count table
        :param where_clauses: An array of where clauses to use for filtering the data
        :param min_subject: An integer setting the minimum bin size for inclusion
        :param annotation: A CountAnnotation definining an external annotation source
            (default: 10)
        """
        return self.get_count_query(
            table_name,
            source_table,
            table_cols,
            where_clauses=where_clauses,
            min_subject=min_subject,
            fhir_resource="allergyintolerance",
            patient_link="patient_ref",
            annotation=annotation,
        )

    def count_condition(
        self,
        table_name: str,
        source_table: str,
        table_cols: list,
        where_clauses: list | None = None,
        min_subject: int | None = None,
        annotation: counts_templates.CountAnnotation | None = None,
    ) -> str:
        """wrapper method for constructing condition counts tables

        :param table_name: The name of the table to create. Must start with study prefix
        :param source_table: The table to create counts data from
        :param table_cols: The columns from the source table to add to the count table
        :param where_clauses: An array of where clauses to use for filtering the data
        :param min_subject: An integer setting the minimum bin size for inclusion
            (default: 10)
        :param annotation: A CountAnnotation definining an external annotation source
        """
        return self.get_count_query(
            table_name,
            source_table,
            table_cols,
            where_clauses=where_clauses,
            min_subject=min_subject,
            fhir_resource="condition",
            filter_resource=True,
            annotation=annotation,
        )

    def count_diagnosticreport(
        self,
        table_name: str,
        source_table: str,
        table_cols: list,
        where_clauses: list | None = None,
        min_subject: int | None = None,
        annotation: counts_templates.CountAnnotation | None = None,
    ) -> str:
        """wrapper method for constructing diagnosticreport counts tables

        :param table_name: The name of the table to create. Must start with study prefix
        :param source_table: The table to create counts data from
        :param table_cols: The columns from the source table to add to the count table
        :param where_clauses: An array of where clauses to use for filtering the data
        :param min_subject: An integer setting the minimum bin size for inclusion
            (default: 10)
        :param annotation: A CountAnnotation definining an external annotation source
        """
        return self.get_count_query(
            table_name,
            source_table,
            table_cols,
            where_clauses=where_clauses,
            min_subject=min_subject,
            fhir_resource="diagnosticreport",
            annotation=annotation,
        )

    def count_documentreference(
        self,
        table_name: str,
        source_table: str,
        table_cols: list,
        where_clauses: list | None = None,
        min_subject: int | None = None,
        annotation: counts_templates.CountAnnotation | None = None,
    ) -> str:
        """wrapper method for constructing documentreference counts tables

        :param table_name: The name of the table to create. Must start with study prefix
        :param source_table: The table to create counts data from
        :param table_cols: The columns from the source table to add to the count table
        :param where_clauses: An array of where clauses to use for filtering the data
        :param min_subject: An integer setting the minimum bin size for inclusion
            (default: 10)
        :param annotation: A CountAnnotation definining an external annotation source
        """
        return self.get_count_query(
            table_name,
            source_table,
            table_cols,
            where_clauses=where_clauses,
            min_subject=min_subject,
            fhir_resource="documentreference",
            filter_resource=True,
            annotation=annotation,
        )

    def count_encounter(
        self,
        table_name: str,
        source_table: str,
        table_cols: list,
        where_clauses: list | None = None,
        min_subject: int | None = None,
        annotation: counts_templates.CountAnnotation | None = None,
    ) -> str:
        """wrapper method for constructing encounter counts tables

        :param table_name: The name of the table to create. Must start with study prefix
        :param source_table: The table to create counts data from
        :param table_cols: The columns from the source table to add to the count table
        :param where_clauses: An array of where clauses to use for filtering the data
        :param min_subject: An integer setting the minimum bin size for inclusion
            (default: 10)
        :param annotation: A CountAnnotation definining an external annotation source
        """
        return self.get_count_query(
            table_name,
            source_table,
            table_cols,
            where_clauses=where_clauses,
            min_subject=min_subject,
            fhir_resource="encounter",
            annotation=annotation,
        )

    def count_medicationrequest(
        self,
        table_name: str,
        source_table: str,
        table_cols: list,
        where_clauses: list | None = None,
        min_subject: int | None = None,
        annotation: counts_templates.CountAnnotation | None = None,
    ) -> str:
        """wrapper method for constructing medicationrequests counts tables

        :param table_name: The name of the table to create. Must start with study prefix
        :param source_table: The table to create counts data from
        :param table_cols: The columns from the source table to add to the count table
        :param where_clauses: An array of where clauses to use for filtering the data
        :param min_subject: An integer setting the minimum bin size for inclusion
            (default: 10)
        :param annotation: A CountAnnotation definining an external annotation source
        """
        return self.get_count_query(
            table_name,
            source_table,
            table_cols,
            where_clauses=where_clauses,
            min_subject=min_subject,
            fhir_resource="medicationrequest",
            annotation=annotation,
        )

    def count_observation(
        self,
        table_name: str,
        source_table: str,
        table_cols: list,
        where_clauses: list | None = None,
        min_subject: int | None = None,
        annotation: counts_templates.CountAnnotation | None = None,
    ) -> str:
        """wrapper method for constructing observation counts tables

        :param table_name: The name of the table to create. Must start with study prefix
        :param source_table: The table to create counts data from
        :param table_cols: The columns from the source table to add to the count table
        :param where_clauses: An array of where clauses to use for filtering the data
        :param min_subject: An integer setting the minimum bin size for inclusion
            (default: 10)
        :param annotation: A CountAnnotation definining an external annotation source
        """
        return self.get_count_query(
            table_name,
            source_table,
            table_cols,
            where_clauses=where_clauses,
            min_subject=min_subject,
            fhir_resource="observation",
            annotation=annotation,
        )

    def count_patient(
        self,
        table_name: str,
        source_table: str,
        table_cols: list,
        where_clauses: list | None = None,
        min_subject: int | None = None,
        annotation: counts_templates.CountAnnotation | None = None,
    ) -> str:
        """wrapper method for constructing patient counts tables

        :param table_name: The name of the table to create. Must start with study prefix
        :param source_table: The table to create counts data from
        :param table_cols: The columns from the source table to add to the count table
        :param where_clauses: An array of where clauses to use for filtering the data
        :param min_subject: An integer setting the minimum bin size for inclusion
            (default: 10)
        :param annotation: A CountAnnotation definining an external annotation source
        """
        return self.get_count_query(
            table_name,
            source_table,
            table_cols,
            where_clauses=where_clauses,
            min_subject=min_subject,
            fhir_resource="patient",
            annotation=annotation,
        )

    def count_procedure(
        self,
        table_name: str,
        source_table: str,
        table_cols: list,
        where_clauses: list | None = None,
        min_subject: int | None = None,
        annotation: counts_templates.CountAnnotation | None = None,
    ) -> str:
        """wrapper method for constructing procedure counts tables

        :param table_name: The name of the table to create. Must start with study prefix
        :param source_table: The table to create counts data from
        :param table_cols: The columns from the source table to add to the count table
        :param where_clauses: An array of where clauses to use for filtering the data
        :param min_subject: An integer setting the minimum bin size for inclusion
            (default: 10)
        :param annotation: A CountAnnotation definining an external annotation source
        """
        return self.get_count_query(
            table_name,
            source_table,
            table_cols,
            where_clauses=where_clauses,
            min_subject=min_subject,
            fhir_resource="procedure",
            annotation=annotation,
        )

    # End of wrapper section
    # ----------------------------------------------------------------------

    def write_counts(self, filepath: str):
        """Convenience method for writing counts queries to disk

        :param filepath: path to file to write queries out to.
        """
        self.prepare_queries()
        self.comment_queries()
        self.write_queries(path=pathlib.Path(filepath))

    def prepare_queries(
        self, *args, manifest: study_manifest.StudyManifest | None = None, **kwargs
    ):
        """Prepare count queries

        This should be overridden in any count generator. See studies/core/count_core.py
        for an example
        """
        pass
