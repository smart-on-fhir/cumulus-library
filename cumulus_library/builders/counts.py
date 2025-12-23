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
        self,
        table_name: str,
        source_table: str,
        table_cols: str | list[list[str] | counts_templates.CountColumn],
        *args,
        min_subject: int | None = None,
        where_clauses: list[str] | None = None,
        filter_resource: bool | None = False,
        primary_id: str = "subject_ref",
        secondary_id: str | None = None,
        alt_secondary_join_id: str | None = None,
        secondary_table: str | None = None,
        secondary_cols: list[str] = [],
        annotation: counts_templates.CountAnnotation | None = None,
        filter_status: bool | None = False,
        filter_cols: list[list[str]] | counts_templates.FilterColumn | None = None,
        **kwargs,
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
        :keyword primary_id: the FHIR ID to count by. If not p
        :keyword secondary_table: a table to join to the source table
        :keyword secondary_id: a second id to striate counts by. if secondary_table is
            also present, will be used to join with the source table, assuming that
            the same column exists in both tables
        :keyword alt_secondary_join_id: if present, uses this field instead of secondary_id
            for joining with the source table
        :keyword secondary_cols: the columns to include from the secondary table
        :keyword annotation: A CountsAnnotation object describing a table to use as
            a metadata annotation source
        :keyword filter_status: if true, will create a filter block at the start of your
            query
        :keyword filter_cols: a series of FilterColumns, or list formatted like
            ['column name', ('desired val 1', 'desired val 2'), True/False for including nulls],
            used to add filtering statements to a filter section of a query. If you also
            want these columns in your output, include them in in either table_cols or
            secondary_cols
        """
        if min_subject is None:
            min_subject = DEFAULT_MIN_SUBJECT

        if not table_name or not source_table or not table_cols:
            raise errors.CountsBuilderError(
                f"count_query missing required arguments. output table: {table_name}"
            )

        for key in kwargs:
            if key == "skip_status_filter":  # pragma: no cover
                # Deprecated as of v5.1.0
                rich.print(
                    "[yellow]CountsBuilder deprecation notice[/yellow]: the behavior of "
                    "'skip_status_filter' is now the default behavior and the argument is "
                    "considered deprecated.\n"
                    "Support for it may be removed in a future version."
                )
            elif key == "patient_link":  # pragma: no cover
                # Deprecated as of v6.0.0
                rich.print(
                    "[yellow]CountsBuilder deprecation notice[/yellow]: 'patient_link' has been "
                    "renamed to 'primary_id'. Future versions may remove support for the "
                    "patient_link argument."
                )
        return counts_templates.get_count_query(
            table_name,
            source_table,
            table_cols,
            *args,
            min_subject=min_subject,
            where_clauses=where_clauses,
            primary_id=primary_id,
            secondary_id=secondary_id,
            alt_secondary_join_id=alt_secondary_join_id,
            secondary_table=secondary_table,
            secondary_cols=secondary_cols,
            annotation=annotation,
            filter_status=filter_status,
            filter_cols=filter_cols,
            **kwargs,
        )

    # ----------------------------------------------------------------------
    # The following function all wrap get_count_query as convenience methods.
    # Now that fhir_resource is no longer a param we're concerned with, the
    # utility of these has gone down somewhat, and the filter_status rules
    # may be more specific to the core study than generally.

    # For now, we're going to leave them here, but we may migrate them to
    # the core study at some point.

    def count_allergyintolerance(
        self,
        table_name: str,
        source_table: str,
        table_cols: list,
        where_clauses: list | None = None,
        min_subject: int | None = None,
        annotation: counts_templates.CountAnnotation | None = None,
        filter_status: bool | None = False,
    ) -> str:
        """wrapper method for constructing allergyintolerance counts tables

        :param table_name: The name of the table to create. Must start with study prefix
        :param source_table: The table to create counts data from
        :param table_cols: The columns from the source table to add to the count table
        :keyword where_clauses: An array of where clauses to use for filtering the data
        :keyword min_subject: An integer setting the minimum bin size for inclusion
        :keyword annotation: A CountAnnotation definining an external annotation source
            (default: 10)
        :keyword filter_status: if True, filters the results by what's commonly considered
            'finished' statuses for this resource
        """
        if filter_status:
            extra_kwargs = {
                "filter_cols": [
                    counts_templates.FilterColumn(
                        name="docStatus", values=["final", "amended"], include_nulls=True
                    ),
                    counts_templates.FilterColumn(
                        name="status",
                        values=["current"],
                        include_nulls=False,
                    ),
                ]
            }
        else:  # pragma: no cover
            extra_kwargs = {}
        return self.get_count_query(
            table_name,
            source_table,
            table_cols,
            where_clauses=where_clauses,
            min_subject=min_subject,
            primary_id="patient_ref",
            annotation=annotation,
            **extra_kwargs,
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
        :keyword where_clauses: An array of where clauses to use for filtering the data
        :keyword min_subject: An integer setting the minimum bin size for inclusion
            (default: 10)
        :keyword annotation: A CountAnnotation definining an external annotation source
        """
        return self.get_count_query(
            table_name,
            source_table,
            table_cols,
            where_clauses=where_clauses,
            min_subject=min_subject,
            filter_resource=True,
            annotation=annotation,
            secondary_id="encounter_ref",
        )

    def count_diagnosticreport(
        self,
        table_name: str,
        source_table: str,
        table_cols: list,
        where_clauses: list | None = None,
        min_subject: int | None = None,
        annotation: counts_templates.CountAnnotation | None = None,
        **kwargs,
    ) -> str:
        """wrapper method for constructing diagnosticreport counts tables

        :param table_name: The name of the table to create. Must start with study prefix
        :param source_table: The table to create counts data from
        :param table_cols: The columns from the source table to add to the count table
        :keyword where_clauses: An array of where clauses to use for filtering the data
        :keyword min_subject: An integer setting the minimum bin size for inclusion
            (default: 10)
        :keyword annotation: A CountAnnotation definining an external annotation source
        """
        return self.get_count_query(
            table_name,
            source_table,
            table_cols,
            where_clauses=where_clauses,
            min_subject=min_subject,
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
        filter_status: bool | None = False,
        **kwargs,
    ) -> str:
        """wrapper method for constructing documentreference counts tables

        :param table_name: The name of the table to create. Must start with study prefix
        :param source_table: The table to create counts data from
        :param table_cols: The columns from the source table to add to the count table
        :keyword where_clauses: An array of where clauses to use for filtering the data
        :keyword min_subject: An integer setting the minimum bin size for inclusion
            (default: 10)
        :keyword annotation: A CountAnnotation definining an external annotation source
        :keyword filter_status: if True, filters the results by what's commonly considered
            'finished' statuses for this resource
        """

        if filter_status:
            extra_kwargs = {
                "filter_cols": [
                    counts_templates.FilterColumn(
                        name="docStatus", values=["final", "amended"], include_nulls=True
                    ),
                    counts_templates.FilterColumn(
                        name="status",
                        values=["current"],
                        include_nulls=False,
                    ),
                ]
            }
        else:
            extra_kwargs = {}
        return self.get_count_query(
            table_name,
            source_table,
            table_cols,
            where_clauses=where_clauses,
            min_subject=min_subject,
            filter_resource=True,
            annotation=annotation,
            filter_status=filter_status,
            primary_id="subject_ref",
            secondary_table="core__encounter",
            secondary_id="documentreference_ref",
            alt_secondary_join_id="encounter_ref",
            secondary_cols=[counts_templates.CountColumn("class_display", "varchar", None)],
            **extra_kwargs,
        )

    def count_encounter(
        self,
        table_name: str,
        source_table: str,
        table_cols: list,
        where_clauses: list | None = None,
        min_subject: int | None = None,
        annotation: counts_templates.CountAnnotation | None = None,
        filter_status: bool | None = False,
        **kwargs,
    ) -> str:
        """wrapper method for constructing encounter counts tables

        :param table_name: The name of the table to create. Must start with study prefix
        :param source_table: The table to create counts data from
        :param table_cols: The columns from the source table to add to the count table
        :param where_clauses: An array of where clauses to use for filtering the data
        :param min_subject: An integer setting the minimum bin size for inclusion
            (default: 10)
        :param annotation: A CountAnnotation definining an external annotation source
        :param filter_status: Filters encounters by status fields.
            Note: encounters often have cancelled/entered in error statuses.
            If your study is not filtering this out in your cohort selection,
            you can set this flag to do it for you (as long as the field is present)
        """
        if filter_status:
            extra_kwargs = {
                "filter_cols": [
                    counts_templates.FilterColumn(
                        name="status", values=["finished"], include_nulls=False
                    ),
                ]
            }
        else:
            extra_kwargs = {}
        return self.get_count_query(
            table_name,
            source_table,
            table_cols,
            where_clauses=where_clauses,
            min_subject=min_subject,
            annotation=annotation,
            filter_status=filter_status,
            primary_id="subject_ref",
            secondary_id="encounter_ref",
            **extra_kwargs,
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
        :keyword where_clauses: An array of where clauses to use for filtering the data
        :keyword min_subject: An integer setting the minimum bin size for inclusion
            (default: 10)
        :keyword annotation: A CountAnnotation definining an external annotation source
        """
        return self.get_count_query(
            table_name,
            source_table,
            table_cols,
            where_clauses=where_clauses,
            min_subject=min_subject,
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
        filter_status: bool | None = False,
        **kwargs,
    ) -> str:
        """wrapper method for constructing observation counts tables

        :param table_name: The name of the table to create. Must start with study prefix
        :param source_table: The table to create counts data from
        :param table_cols: The columns from the source table to add to the count table
        :param where_clauses: An array of where clauses to use for filtering the data
        :keyword where_clauses: An array of where clauses to use for filtering the data
        :keyword min_subject: An integer setting the minimum bin size for inclusion
            (default: 10)
        :keyword annotation: A CountAnnotation definining an external annotation source
        :keyword filter_status: if True, filters the results by what's commonly considered
            'finished' statuses for this resource
        """
        if filter_status:
            extra_kwargs = {
                "filter_cols": [
                    counts_templates.FilterColumn(
                        name="status", values=["final", "amended"], include_nulls=False
                    ),
                ]
            }
        else:
            extra_kwargs = {}
        return self.get_count_query(
            table_name,
            source_table,
            table_cols,
            where_clauses=where_clauses,
            min_subject=min_subject,
            annotation=annotation,
            filter_status=filter_status,
            primary_id="subject_ref",
            secondary_table="core__encounter",
            secondary_id="observation_ref",
            alt_secondary_join_id="encounter_ref",
            secondary_cols=[counts_templates.CountColumn("class_display", "varchar", None)],
            **extra_kwargs,
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
        :keyword where_clauses: An array of where clauses to use for filtering the data
        :keyword min_subject: An integer setting the minimum bin size for inclusion
            (default: 10)
        :keyword annotation: A CountAnnotation definining an external annotation source
        """
        return self.get_count_query(
            table_name,
            source_table,
            table_cols,
            where_clauses=where_clauses,
            min_subject=min_subject,
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
        :keyword where_clauses: An array of where clauses to use for filtering the data
        :keyword min_subject: An integer setting the minimum bin size for inclusion
            (default: 10)
        :keyword annotation: A CountAnnotation definining an external annotation source
        """
        return self.get_count_query(
            table_name,
            source_table,
            table_cols,
            where_clauses=where_clauses,
            min_subject=min_subject,
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
