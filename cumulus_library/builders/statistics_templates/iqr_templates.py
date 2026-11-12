from pathlib import Path

from cumulus_library.template_sql import base_templates


def get_iqr_query(
    schema_name: str,
    table_name: str,
    column_name: str,
    source_table: str,
) -> str:
    """Generates interquartile range parameters for a given column.

    See https://en.wikipedia.org/wiki/Interquartile_range for algorithm and
    expected output parameters

    :param columns: a list of ids to request
    :param source_table: the table to retrieve ids from
    :param join_id: the id column to use for joining. Expected to exist in both
      source and filter tables
    :param filter_table: a table containing ids you want to exclude
    """
    return base_templates.get_base_template(
        "iqr",
        Path(__file__).parent,
        schema_name=schema_name,
        table_name=table_name,
        column_name=column_name,
        source_table=source_table,
    )
