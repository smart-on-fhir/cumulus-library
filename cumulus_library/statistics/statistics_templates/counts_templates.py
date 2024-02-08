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

    CONDITION = "condition"
    DOCUMENTREFERENCE = "documentreference"
    ENCOUNTER = "encounter"
    NONE = None
    OBSERVATION = "observation"
    PATIENT = "patient"
    MEDICATION = "medication"
    MEDICATIONREQUEST = "medicationrequest"


@dataclass
class CountColumn:
    name: str
    db_type: str
    alias: str


def get_count_query(
    table_name: str,
    source_table: str,
    table_cols: list,
    min_subject: int = 10,
    where_clauses: list | None = None,
    fhir_resource: str | None = None,
    filter_resource: bool | None = True,
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
            table_col_classed.append(
                CountColumn(name=item[0], db_type=item[1], alias=item[2])
            )
        else:
            table_col_classed.append(
                CountColumn(name=item, db_type="varchar", alias=None)
            )
    table_cols = table_col_classed

    query = base_templates.get_base_template(
        "count",
        path,
        table_name=table_name,
        source_table=source_table,
        table_cols=table_cols,
        min_subject=min_subject,
        where_clauses=where_clauses,
        fhir_resource=fhir_resource,
        filter_resource=filter_resource,
    )
    # workaround for conflicting sqlfluff enforcement
    return query.replace("-- noqa: disable=LT02\n", "")
