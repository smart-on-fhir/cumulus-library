"""utility functions related to populating jinja templates

Currently, these deal with various edge cases around complex nested objects in sql
and validating them so that the actual generated queries can be constructed more
simply. This includes, but is not limited, to:
    - Data missing entirely
    - Data present, but 'nullish' - with some structure but no value
    - Data with deep missing elements
    - Data which may or may not be in an array depending on context
"""

from dataclasses import dataclass

import duckdb
from typing import List

from cumulus_library import base_utils
from cumulus_library.template_sql import base_templates
from cumulus_library import databases


@dataclass(kw_only=True)
class CodeableConceptConfig:
    """Holds parameters for generating codableconcept tables.

    :param column_name: the column containing the codeableConcept you want to extract.
    :param is_array: whether the codeableConcept is 0...1 or 0..* in the FHIR spec
    :param source_table: the table to extract extensions from
    :param target_table: the name of the table to create
    :param source_id: the id field to use in the new table (default: 'id')
    :param filter_priority: If true, will use code systems to select a single code,
      in preference order, for use as a display value.
    :param code_systems: a list of systems, in preference order, for selecting data
      for filtering. This should not be set if filter_priority is false.
    """

    column_name: str
    is_array: bool
    source_table: str = None
    target_table: str = None
    source_id: str = "id"
    filter_priority: bool = False
    code_systems: list = None


@dataclass
class ExtensionConfig(object):
    """convenience class for holding parameters for generating extension tables.

    :param source_table: the table to extract extensions from
    :param source_id: the id column to treat as a foreign key
    :param target_table: the name of the table to create
    :param target_col_prefix: the string to prepend code/display column names with
    :param fhir_extension: the URL of the FHIR resource to select
    :param code_systems: a list of codes, in preference order, to use to select data
    :param is_array: a boolean indicating if the targeted field is an array type
    """

    source_table: str
    source_id: str
    target_table: str
    target_col_prefix: str
    fhir_extension: str
    ext_systems: List[str]
    is_array: bool = False


def _check_data_in_fields(
    schema,
    cursor,
    code_sources: List[CodeableConceptConfig],
) -> dict:
    """checks if CodeableConcept fields actually have data available

    CodeableConcept fields are mostly optional in the FHIR spec, and may be arrays
    or single objects. Additionally, the null representation can be inconsistent,
    depending on how the data is provided from an EHR and how the ETL manages
    schema inference (wide, but not deep). We :could: try to find the data and
    just catch an error, but that would mask configuration errors/unexpected
    data patterns. So, instead, we are doing the following fussy operation:

    For each column we want to check for data:
    - Check to see if there is any data in a codeableConcept field
    - Check to see if the codeableConcept field contains a coding element
    - Check if that coding element contains non-null data

    The way we do this is slightly different depending on if the field is an
    array or not (generally requiring one extra level of unnesting).

    """

    with base_utils.get_progress_bar(transient=True) as progress:
        task = progress.add_task(
            "Detecting available encounter codeableConcepts...",
            # Each column in code_sources requires at most 3 queries to
            # detect valid data is in the DB
            total=len(code_sources),
        )
        for code_source in code_sources:
            if code_source.is_array:
                code_source.has_data = is_codeable_concept_array_populated(
                    schema, code_source.source_table, code_source.column_name, cursor
                )
            else:
                code_source.has_data = is_codeable_concept_populated(
                    schema, code_source.source_table, code_source.column_name, cursor
                )
            progress.advance(task)
    return code_sources


def denormalize_codes(
    schema: str,
    cursor: databases.DatabaseCursor,
    code_sources: List[CodeableConceptConfig],
):
    queries = []
    code_sources = _check_data_in_fields(schema, cursor, code_sources)
    for code_source in code_sources:
        if code_source.has_data:
            queries.append(
                base_templates.get_codeable_concept_denormalize_query(code_source)
            )
        else:
            queries.append(
                base_templates.get_ctas_empty_query(
                    schema_name=schema,
                    table_name=code_source.target_table,
                    table_cols=["id", "code", "code_system", "display"],
                )
            )
    return queries


def is_codeable_concept_populated(
    schema: str,
    table: str,
    base_col: str,
    cursor,
    coding_element="coding",
) -> bool:
    """Check db to see if codeableconcept data exists.

    Will execute several exploratory queries to see if the column in question
    can be queried naively.

    :param schema: The schema/database name
    :param table: The table to query against
    :param base_col: the place to start validation from.
        This can be a nested element, like column.object.code
    :param cursor: a PEP-249 compliant database cursor
    :param coding_element: the place inside the code element to look for coding info.
        default: 'coding' (and :hopefully: this is always right)
    :returns: a boolean indicating if valid data is present.
    """
    try:
        if not _check_schema_if_exists(schema, table, base_col, cursor, coding_element):
            return False

        query = base_templates.get_is_table_not_empty_query(
            table,
            "t1.row1",
            [
                {
                    "source_col": f"{base_col}.coding",
                    "table_alias": "t1",
                    "row_alias": "row1",
                }
            ],
        )
        cursor.execute(query)
        if cursor.fetchone() is None:
            return False
        return True
    except duckdb.duckdb.BinderException:
        return False


def is_codeable_concept_array_populated(
    schema: str,
    table: str,
    base_col: str,
    cursor,
    coding_element="coding",
) -> bool:
    """Check db to see if an array of codeableconcept data exists.

    Will execute several exploratory queries to see if the column in question
    can be queried naively. Will advance the associated progress's task by 3 steps.

    :param schema: The schema/database name
    :param table: The table to query against
    :param base_col: the place to start validation from.
        This can be a nested element, like column.object.code
    :param cursor: a PEP-249 compliant database cursor
    :param coding_element: the place inside the code element to look for coding info.
        default: 'coding' (and :hopefully: this is always right)
    :returns: a boolean indicating if valid data is present.
    """
    try:
        if not _check_schema_if_exists(schema, table, base_col, cursor, coding_element):
            return False
        query = base_templates.get_is_table_not_empty_query(
            table,
            "t2.row2",
            [
                {
                    "source_col": base_col,
                    "table_alias": "t1",
                    "row_alias": "row1",
                },
                {
                    "source_col": "row1.coding",
                    "table_alias": "t2",
                    "row_alias": "row2",
                },
            ],
        )
        cursor.execute(query)
        if cursor.fetchone() is None:
            return False
        return True
    except duckdb.duckdb.BinderException:
        return False


def is_code_populated(
    schema: str,
    table: str,
    base_col: str,
    cursor,
) -> bool:
    """Check db to see if a bare code exists and is populated.

    Will execute several exploratory queries to see if the column in question
    can be queried naively.

    :param schema: The schema/database name
    :param table: The table to query against
    :param base_col: the place to start validation from.
        This can be a nested element, like column.object.code
    :param cursor: a PEP-249 compliant database cursor
    :returns: a boolean indicating if valid data is present.
    """

    if not _check_schema_if_exists(
        schema, table, base_col, cursor, "coding", check_missing=True
    ):
        return False
    query = base_templates.get_is_table_not_empty_query(
        table,
        base_col,
    )
    cursor.execute(query)
    if cursor.fetchone() is None:
        return False
    return True


def _check_schema_if_exists(
    schema: str,
    table: str,
    base_col: str,
    cursor,
    coding_element: str,
    check_missing: bool = False,
) -> bool:
    """Validation check for a column existing, and having the expected schema"""
    try:
        query = base_templates.get_is_table_not_empty_query(table, base_col)
        cursor.execute(query)
        if cursor.fetchone() is None:
            return False

        query = base_templates.get_column_datatype_query(schema, table, [base_col])
        cursor.execute(query)
        schema_str = str(cursor.fetchone()[1])
        if check_missing:
            # This check is for a bare coding, so we're looking for an exclusion of the
            # coding element, but still the things that are in a code
            required_fields = ["code", "system", "display"]
            if any(x not in schema_str for x in required_fields):
                return False
            if coding_element in schema_str:
                return False
        else:
            required_fields = [coding_element] + ["code", "system", "display"]
            if any(x not in schema_str for x in required_fields):
                return False
        return True

    except Exception:
        return False
