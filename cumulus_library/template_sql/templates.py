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


class ExtensionConfig(object):
    """convenience class for holding parameters for generating extension tables.

    :param source_table: the table to extract extensions from
    :param source_id: the id column to treat as a foreign key
    :param target_table: the name of the table to create
    :param target_col_prefix: the string to prepend code/display column names with
    :param fhir_extension: the URL of the FHIR resource to select
    :param code_systems: a list of codes, in preference order, to use to select data
    """

    def __init__(
        self,
        source_table: str,
        source_id: str,
        target_table: str,
        target_col_prefix: str,
        fhir_extension: str,
        code_systems: List[str],
    ):
        self.source_table = source_table
        self.source_id = source_id
        self.target_table = target_table
        self.target_col_prefix = target_col_prefix
        self.fhir_extension = fhir_extension
        self.code_systems = code_systems


def get_extension_denormalize_query(config: ExtensionConfig) -> str:
    """extracts target extension from a table into a denormalized table

    This function is targeted at a complex extension element that is at the root
    of a FHIR resource - as an example, see the 5 codes at the root node of
    http://hl7.org/fhir/us/core/STU6/StructureDefinition-us-core-patient.html.
    The template will create a new table with the extension data, in arrays,
    mapped 1-1 to the table id. You can specify multiple coding systems
    in the ExtensionConfig passed to this function. For each patient, we'll
    take the data from the first coding system we find for each patient.

    :param config: An instance of ExtensionConfig.
    """
    path = Path(__file__).parent
    with open(f"{path}/extension_denormalize.sql.jinja") as extension_denormalize:
        return Template(extension_denormalize.read()).render(
            source_table=config.source_table,
            source_id=config.source_id,
            target_table=config.target_table,
            target_col_prefix=config.target_col_prefix,
            fhir_extension=config.fhir_extension,
            code_systems=config.code_systems,
        )
