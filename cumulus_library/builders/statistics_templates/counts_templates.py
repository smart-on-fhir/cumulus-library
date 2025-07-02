from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from cumulus_library.errors import CountsBuilderError
from cumulus_library.template_sql import base_templates


class CountableFhirResource(Enum):
    """Contains FHIR types for which we have count table generation support.

    This is primarily used as a way to decide if a secondary join is needed to get the
    appropriate ID field to count. Patient (and a value of None) skip this check,
    while the other resources will add an additional source to get the proper countable
    ids.
    """

    ALLERGYINTOLERANCE = "allergyintolerance"
    CONDITION = "condition"
    DIAGNOSTICREPORT = "diagnosticreport"
    DOCUMENTREFERENCE = "documentreference"
    ENCOUNTER = "encounter"
    MEDICATION = "medication"
    MEDICATIONREQUEST = "medicationrequest"
    NONE = None
    OBSERVATION = "observation"
    PATIENT = "patient"
    PROCEDURE = "procedure"


@dataclass
class CountColumn:
    name: str
    db_type: str
    alias: str


@dataclass(kw_only=True)
class CountAnnotation:
    """Defines a table to use as a source for annotating a count dataset.

    :keyword field: the field in the powerset source table to use for the join.
        Note: in most cases, this field should not be included in your powerset.
    :keyword join_table: the name of the table to join from. If the table is
        in a different schema, use '"schema"."table' as the format
    :keyword join_field: the column in the table to join on
    :keyword list: a list of tuples like ('column_name', 'alias' or None) to
        define columns to add to your dataset.

    """

    field: str
    join_table: str
    join_field: str
    columns: list[tuple[str, str | None]]


def get_count_query(
    table_name: str,
    source_table: str,
    table_cols: list,
    min_subject: int = 10,
    where_clauses: list | None = None,
    fhir_resource: str | None = None,
    filter_resource: bool | None = True,
    patient_link: str = "subject_ref",
    annotation: CountAnnotation | None = None,
) -> str:
    """Generates count tables for generating study outputs"""
    path = Path(__file__).parent
    if fhir_resource not in {e.value for e in CountableFhirResource}:
        raise CountsBuilderError(
            f"Tried to create counts table for invalid resource {fhir_resource}."
        )

    table_col_classed = []
    for item in table_cols:
        # TODO: remove check after cutover
        if isinstance(item, list):
            table_col_classed.append(CountColumn(name=item[0], db_type=item[1], alias=item[2]))
        else:
            table_col_classed.append(CountColumn(name=item, db_type="varchar", alias=None))
    table_cols = table_col_classed

    query = base_templates.get_template(
        "count",
        path,
        table_name=table_name,
        source_table=source_table,
        table_cols=table_cols,
        min_subject=min_subject,
        where_clauses=where_clauses,
        fhir_resource=fhir_resource,
        filter_resource=filter_resource,
        patient_link=patient_link,
        annotation=annotation,
    )
    # workaround for conflicting sqlfluff enforcement
    return query.replace("-- noqa: disable=LT02\n", "")
