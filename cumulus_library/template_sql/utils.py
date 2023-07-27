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
    query = get_is_table_not_empty_query(table, base_col)
    cursor.execute(query)
    if cursor.fetchone() is None:
        return False

    query = get_column_datatype_query(schema, table, base_col)
    cursor.execute(query)
    if coding_element not in str(cursor.fetchone()[0]):
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
    query = get_is_table_not_empty_query(table, base_col)
    cursor.execute(query)
    if cursor.fetchone() is None:
        return False

    query = get_column_datatype_query(schema, table, base_col)
    cursor.execute(query)
    if coding_element not in str(cursor.fetchone()[0]):
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
