""" Collection of jinja template getters for common SQL queries """
from pathlib import Path
from typing import List

from jinja2 import Template


def get_drop_table(table_name: str) -> str:
    """Generates a drop table if exists query"""
    path = Path(__file__).parent
    with open(f"{path}/template_sql/drop_table.sql.jinja") as ctas:
        return Template(ctas.read()).render(
            table_name=table_name,
        )


def get_ctas_query(
    schema_name: str, table_name: str, dataset: List[List[str]], table_cols: List[str]
) -> str:
    """Generates a create table as query for inserting static data into athena

    Note that unlike other queries, the nature of the CTAS implementation in athena
    requires a schema name. This schema name should match the schema of your cursor,
    or the other queries in this template will not function correctly. All columns
    will be specified as varchar type.

    :param schema_name: The athena query to create the table in
    :param table_name: The name of the athena table to create
    :param dataset: Array of data arrays to insert, i.e. [['1','3'],['2','4']]
    :param table_cols: Comma deleniated column names, i.e. ['first,second']
    """
    path = Path(__file__).parent
    with open(f"{path}/template_sql/ctas.sql.jinja") as ctas:
        return Template(ctas.read()).render(
            schema_name=schema_name,
            table_name=table_name,
            dataset=dataset,
            table_cols=table_cols,
        )


def get_insert_into_query(
    table_name: str, table_cols: List[str], dataset: List[List[str]]
) -> str:
    """Generates an insert query for adding data to an existing athena table

    :param schema_name: The athena query to create the table in
    :param table_name: The name of the athena table to create
    :param table_cols: Comma deleniated column names, i.e. ['first','second']
    :param dataset: Array of data arrays to insert, i.e. [['1','3'],['2','4']]
    """
    path = Path(__file__).parent
    with open(f"{path}/template_sql/insert_into.sql.jinja") as insert_into:
        return Template(insert_into.read()).render(
            table_name=table_name, table_cols=table_cols, dataset=dataset
        )
