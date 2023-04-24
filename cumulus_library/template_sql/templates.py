""" Collection of jinja template getters for common SQL queries """
from enum import Enum
from pathlib import Path
from typing import List

from jinja2 import Template


class TableView(Enum):
    TABLE = "TABLE"
    VIEW = "VIEW"


def get_drop_view_table(name: str, view_or_table: str) -> str:
    """Generates a drop table if exists query"""
    if view_or_table in [e.value for e in TableView]:
        path = Path(__file__).parent
        with open(f"{path}/drop_view_table.sql.jinja") as drop_view_table:
            return Template(drop_view_table.read()).render(
                view_or_table_name=name, view_or_table=view_or_table
            )


def get_show_tables(schema_name: str, prefix: str) -> str:
    """Generates a show tables query, filtered by prefix

    The intended use case for this function is to get a list of manifest study
    tables, so that they can be individually dropped during a clean call.

    :param schema_name: The athena schema to query
    :param table_name: The prefix to filter by. Jinja template auto adds '__'.
    """
    path = Path(__file__).parent
    with open(f"{path}/show_tables.sql.jinja") as show_tables:
        return Template(show_tables.read()).render(
            schema_name=schema_name, prefix=prefix
        )


def get_show_views(schema_name: str, prefix: str) -> str:
    """Generates a show vies query, filtered by prefix

    The intended use case for this function is to get a list of manifest study
    views, so that they can be individually dropped during a clean call.

    :param schema_name: The athena schema to query
    :param table_name: The prefix to filter by. Jinja template auto adds '__'.
    """
    path = Path(__file__).parent
    with open(f"{path}/show_views.sql.jinja") as show_tables:
        return Template(show_tables.read()).render(
            schema_name=schema_name, prefix=prefix
        )


def get_ctas_query(
    schema_name: str, table_name: str, dataset: List[List[str]], table_cols: List[str]
) -> str:
    """Generates a create table as query for inserting static data into athena

    Note that unlike other queries, the nature of the CTAS implementation in athena
    requires a schema name. This schema name should match the schema of your cursor,
    or the other queries in this template will not function correctly. All columns
    will be specified as varchar type.

    :param schema_name: The athena schema to create the table in
    :param table_name: The name of the athena table to create
    :param dataset: Array of data arrays to insert, i.e. [['1','3'],['2','4']]
    :param table_cols: Comma deleniated column names, i.e. ['first,second']
    """
    path = Path(__file__).parent
    with open(f"{path}/ctas.sql.jinja") as ctas:
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
    with open(f"{path}/insert_into.sql.jinja") as insert_into:
        return Template(insert_into.read()).render(
            table_name=table_name, table_cols=table_cols, dataset=dataset
        )
