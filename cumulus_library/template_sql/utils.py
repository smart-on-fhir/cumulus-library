"""utility functions related to populating jinja templates

Currently, these deal with various edge cases around complex nested objects in sql
and validating them so that the actual generated queries can be constructed more
simply. This includes, but is not limited, to:
    - Data missing entirely
    - Data present, but 'nullish' - with some structure but no value
    - Data with deep missing elements
    - Data which may or may not be in an array depending on context
"""
from rich.progress import Progress, Task

from cumulus_library.template_sql.templates import (
    get_column_datatype_query,
    get_is_table_not_empty_query,
)


def is_codeable_concept_populated(
    schema: str,
    table: str,
    base_col: str,
    cursor,
    coding_element="coding",
    allow_partial: bool = True,
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
    :allow_partial: If true, codings which do not have fields expected by the library
        will still be included, and will need to be manually coerced.
    :returns: a boolean indicating if valid data is present.
    """

    if not _check_schema_if_exists(
        schema, table, base_col, cursor, coding_element, allow_partial
    ):
        return False

    query = get_is_table_not_empty_query(
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


def is_codeable_concept_array_populated(
    schema: str,
    table: str,
    base_col: str,
    cursor,
    coding_element="coding",
    allow_partial: bool = True,
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
    :allow_partial: If true, codings which do not have fields expected by the library
        will still be included, and will need to be manually coerced.
    :returns: a boolean indicating if valid data is present.
    """

    if not _check_schema_if_exists(
        schema, table, base_col, cursor, coding_element, allow_partial
    ):
        return False
    query = get_is_table_not_empty_query(
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


def is_code_populated(
    schema: str,
    table: str,
    base_col: str,
    cursor,
    allow_partial: bool = True,
) -> bool:
    """Check db to see if a bare code exists and is populated.

    Will execute several exploratory queries to see if the column in question
    can be queried naively.

    :param schema: The schema/database name
    :param table: The table to query against
    :param base_col: the place to start validation from.
        This can be a nested element, like column.object.code
    :param cursor: a PEP-249 compliant database cursor
    :allow_partial: If true, codings which do not have fields expected by the library
        will still be included, and will need to be manually coerced.
    :returns: a boolean indicating if valid data is present.
    """

    if not _check_schema_if_exists(
        schema, table, base_col, cursor, False, allow_partial
    ):
        return False
    query = get_is_table_not_empty_query(
        table,
        base_col,
    )
    cursor.execute(query)
    if cursor.fetchone() is None:
        return False
    return True


def _check_schema_if_exists(
    schema: str, table: str, base_col: str, cursor, coding_element, allow_partial: bool
) -> bool:
    """Validation check for a column existing, and having the expected schema"""
    try:
        query = get_is_table_not_empty_query(table, base_col)
        cursor.execute(query)
        if cursor.fetchone() is None:
            return False

        query = get_column_datatype_query(schema, table, base_col)
        cursor.execute(query)
        schema_str = str(cursor.fetchone()[0])
        required_fields = [coding_element]
        if allow_partial:
            required_fields + ["code", "system", "display"]
        if any(x not in schema_str for x in required_fields):
            return False

        return True

    except:
        return False
