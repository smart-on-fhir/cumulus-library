"""Class for generating counts tables from templates"""

import pathlib
import sys

import msgspec
import rich

from cumulus_library import BaseTableBuilder, base_utils, errors, study_manifest
from cumulus_library.builders.statistics_templates import counts_templates

# Defined here for easy overriding by tests
DEFAULT_MIN_SUBJECT = 10

"""
Counts can be driven by a workflow config. A counts config looks like this:

type="counts"

## the table in db will be called 'study_prefix__name', snaked cased for sql compatibility
[tables.name] 

## The following keys must be defined in all cases

source_table= "core__patient"
table_cols = ["gender","birthdate","postalcode_3"]

## The rest of these keys are optional, and target specific use cases. You can uncomment
## the examples as you need them

## description is a user facing string that will be pushed downstream to the cumulus dashboard
#description = "A count of patients by location across the entire hospital service area"

## min_subject allows you to specify the smallest population size to include in a bucket.
## The default is 10. Decreasing this number may affect the identifiability of individuals
## in the dataset, so proceed with caution when changing this.
# min_subject = 20

## where clauses is an array of filter terms.
# where_clauses =[
#     "birthdate IS NOT NULL"
#     "postalcode_3 IS NOT NULL"
# ]

## By default, we will count by subject_ref. If you want to count a different primary ID,
## you can specify that here.
# primary_id = "encounter_ref"

## If you want to join a secondary table, you can specify that here. By default, it will
## be joined on the primary ID. 
# secondary_table="core__encounter"
# secondary_cols = ["age_at_visit"]

## You can specify a second ID type to stratify a table by. If you also have a secondary
## table, it will be added to the join clause with the primary table
# secondary_id = "subject_ref"

## If you want to not join by secondary ID, or just to specify another column to join by,
## you can use this field to declare an alternate join target
# alt_secondary_join_id = "arbitrary_field"

## if you want to use a table to annotate rows with labels from another dataset, like
## from a coding system, you can define a count annotation source. All parmeters,
## except alt_target, are required.
# [ table.name.annotation ]
## field is the column from the primary table to use to join with the annotating table
# field='loinc_code'
## join_table is the table to use as the annotating table
# join_table='loinc.loinc_groups'
## join_field is the field to join on from the annotation table.
# join_field= 'code'
## columns is a list of columns to bring over as annotations. Each column is a tuple
## of three values:
##   - the column name
##   - the sql datatype to use for the column
##   - a name to use as an alias, or None
# columns = [
#  [ "display", "varchar", None ],
#  [ "system", "varchar", "code_system"]
# ]
## alt target lets you specify a different column from the annotation table to use to
## join with the primary table. If not specified, it's assume they share the same
## column name.
# alt_target = "code"

## As an alternate form of specifying where clauses, you can supply one or more lists to
## filter_cols to handle the common case of things like 'only return rows where a column
## contains one of these values', commonly a coding system in our use case. Each
## filter contains the following:
##   - The name of the column
##   - A list of exact matches for the value in that column
##   - A boolean to indicate if nulls should be included or not
# filter_cols = [
#     ['code_system', ['http://terminology.hl7.org/CodeSystem/condition-category'], True]
# ]

[tables.other_name]
source_table= "core__condition"
table_cols = ["code","recordeddate_month"]
"""


class CountsWorkflowAnnotation(msgspec.Struct, forbid_unknown_fields=True):
    field: str
    join_table: str
    join_field: str
    columns: list[list[str]]
    alt_target: str | None = None


class CountsWorkflowTable(msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True):
    source_table: str
    table_cols: list[str]

    description: str | None = None
    where_clauses: list[str] | None = None
    min_subject: int | None = None
    primary_id: str | None = None
    secondary_table: str | None = None
    secondary_cols: list[str] | None = None
    secondary_id: str | None = None
    alt_secondary_join_id: str | None = None
    annotation: CountsWorkflowAnnotation | None = None
    filter_status: bool | None = False
    filter_cols: list[tuple[str, list[str], bool]] | None = None


class CountsWorkflow(msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True):
    config_type: str
    tables: dict[str, CountsWorkflowTable]


class CountsBuilder(BaseTableBuilder):
    """Extends BaseTableBuilder for counts-related use cases"""

    def __init__(
        self,
        manifest: study_manifest.StudyManifest,
        *args,
        toml_config_path: pathlib.Path | None = None,
        **kwargs,
    ):
        super().__init__()

        self.study_prefix = manifest.get_study_prefix()
        if toml_config_path:
            try:
                with open(toml_config_path, "rb") as file:
                    file_bytes = file.read()
                    self._workflow_config = msgspec.to_builtins(
                        msgspec.toml.decode(file_bytes, type=CountsWorkflow)
                    )

            except msgspec.ValidationError as e:
                sys.exit(
                    f"The counts workflow at {toml_config_path!s} contains an unexpected param: \n"
                    f"{e}"
                )
            # msgspec doesn't love our optional array fields, so we just take a dict in
            # and then pass it by keyword to the CountsAnnotation object manually
            for table_name, contents in self._workflow_config["tables"].items():
                if annotation := contents.get("annotation"):
                    self._workflow_config["tables"][table_name]["annotation"] = (
                        counts_templates.CountAnnotation(**annotation)
                    )

        else:
            self._workflow_config = None

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
        :keyword primary_id: the FHIR ID to count by. If not present, uses subject_ref
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

    def write_counts(
        self, config: base_utils.StudyConfig, manifest: study_manifest.StudyManifest, filepath: str
    ):
        """Convenience method for writing counts queries to disk

        :param filepath: path to file to write queries out to.
        """
        self.prepare_queries(config=config, manifest=manifest)
        self.comment_queries()
        self.write_queries(path=pathlib.Path(filepath))

    def prepare_queries(
        self,
        config: base_utils.StudyConfig,
        manifest: study_manifest.StudyManifest,
        *args,
        **kwargs,
    ):
        """Renders out count queries

        By default, prepare_queries will look for a workflow config to populate tables.
        In cases where you are subclassing to write a python counts builder, you
        should override this method with your own logic.

        """
        if not self._workflow_config:
            return
        for table_name, config in self._workflow_config["tables"].items():
            self.queries.append(
                self.get_count_query(
                    table_name=f"{manifest.get_formatted_study_prefix()}{table_name}", **config
                )
            )
