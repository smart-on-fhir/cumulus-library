from enum import Enum
from pathlib import Path
from typing import Optional

from jinja2 import Template

from cumulus_library.errors import CountsBuilderError


class CountableFhirResource(Enum):
    """Contains FHIR types for which we have count table generation support"""

    CONDITION = "condition"
    DOCUMENT = "document"
    ENCOUNTER = "encounter"
    NONE = None  # This is treated as an implicit patient
    OBSERVATION = "observation"
    PATIENT = "patient"


def get_count_query(
    table_name: str,
    source_table: str,
    table_cols: list,
    min_subject: int = 10,
    where_clauses: Optional[list] = None,
    fhir_resource: Optional[str] = None,
    filter_resource: Optional[bool] = True,
) -> str:
    """Generates count tables for generating study outputs"""
    path = Path(__file__).parent
    if fhir_resource not in [e.value for e in CountableFhirResource]:
        raise CountsBuilderError(
            f"Tried to create counts table for invalid resource {fhir_resource}."
        )
    with open(f"{path}/count.sql.jinja") as count_query:
        query = Template(count_query.read()).render(
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