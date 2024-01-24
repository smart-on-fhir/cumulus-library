""" Collection of jinja template getters for common SQL queries """
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional

from jinja2 import Template
from pandas import DataFrame

from cumulus_library import databases
from cumulus_library.template_sql import utils

PATH = Path(__file__).parent


class TableView(Enum):
    """Convenience enum for building drop queries"""

    TABLE = "TABLE"
    VIEW = "VIEW"


# TODO: Consolidate to a generic template reader


def get_alias_table_query(source_table: str, target_table: str):
    """Creates a 1-1 alias of a given table"""
    with open(f"{PATH}/alias_table.sql.jinja") as alias_table:
        return Template(alias_table.read()).render(
            source_table=source_table, target_table=target_table
        )


def get_code_system_pairs(output_table_name: str, code_system_tables: list) -> str:
    """Extracts code system details as a standalone table"""
    with open(f"{PATH}/code_system_pairs.sql.jinja") as code_system_pairs:
        return Template(code_system_pairs.read()).render(
            output_table_name=output_table_name, code_system_tables=code_system_tables
        )


def get_codeable_concept_denormalize_query(config: utils.CodeableConceptConfig) -> str:
    """extracts codeable concepts from a specified table.

    This function is targeted at arbitrary codeableConcept elements - see
    http://hl7.org/fhir/datatypes-definitions.html#CodeableConcept for more info.
    This may be or may not be an array field depending on the context of use -
    check the specification of the specific resource you're interested in.
    See the CodeableConceptConfig for details on how to handle array vs non-
    array use cases.

    :param config: a CodableConeptConfig
    """

    # If we get a None for code systems, we want one dummy value so the jinja
    # for loop will do a single pass. This implicitly means that we're not
    # filtering, so this parameter will be otherwise ignored
    config.code_systems = config.code_systems or ["all"]

    with open(f"{PATH}/codeable_concept_denormalize.sql.jinja") as codable_concept:
        return Template(codable_concept.read()).render(
            source_table=config.source_table,
            source_id=config.source_id,
            column_name=config.column_name,
            is_array=config.is_array,
            target_table=config.target_table,
            filter_priority=config.filter_priority,
            code_systems=config.code_systems,
        )


def get_column_datatype_query(schema_name: str, table_name: str, column_names: List):
    with open(f"{PATH}/column_datatype.sql.jinja") as column_datatype:
        return Template(column_datatype.read()).render(
            schema_name=schema_name,
            table_name=table_name,
            column_names=column_names,
        )


def get_core_medication_query(
    medication_datasources: dict, has_userselected: Optional[bool] = False
):
    with open(
        f"/Users/mgarber/code/cumulus-library/cumulus_library/studies/core/core_templates/medication.sql.jinja"
    ) as core_medication:
        return Template(core_medication.read()).render(
            medication_datasources=medication_datasources,
            has_userselected=has_userselected,
        )


def get_create_view_query(
    view_name: str, dataset: List[List[str]], view_cols: List[str]
) -> str:
    """Generates a create view as query for inserting static data into athena

    :param view_name: The name of the athena table to create
    :param dataset: Array of data arrays to insert, i.e. [['1','3'],['2','4']]
    :param table_cols: Comma deleniated column names, i.e. ['first,second']
    """
    with open(f"{PATH}/create_view_as.sql.jinja") as cvas:
        return Template(cvas.read()).render(
            view_name=view_name,
            dataset=dataset,
            view_cols=view_cols,
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
    with open(f"{PATH}/ctas.sql.jinja") as ctas:
        return Template(ctas.read()).render(
            schema_name=schema_name,
            table_name=table_name,
            dataset=dataset,
            table_cols=table_cols,
        )


def get_ctas_query_from_df(schema_name: str, table_name: str, df: DataFrame) -> str:
    """Generates a create table as query from a dataframe

    This is a convenience wrapper for get_ctas_query.

    :param schema_name: The athena schema to create the table in
    :param table_name: The name of the athena table to create
    :param df: A pandas dataframe
    """
    split_dict = df.to_dict(orient="split")
    return get_ctas_query(
        schema_name, table_name, split_dict["data"], split_dict["columns"]
    )


def get_ctas_empty_query(
    schema_name: str,
    table_name: str,
    table_cols: List[str],
    table_cols_types: List[str] = None,
) -> str:
    """Generates a create table as query for initializing an empty table

    Note that unlike other queries, the nature of the CTAS implementation in athena
    requires a schema name. This schema name should match the schema of your cursor,
    or the other queries in this template will not function correctly. All columns
    will be specified as varchar type.

    :param schema_name: The athena schema to create the table in
    :param table_name: The name of the athena table to create
    :param table_cols: Comma deleniated column names, i.e. ['first,second']
    :param table_cols_types: Allows specifying a data type per column (default: all varchar)
    """
    if not table_cols_types:
        table_cols_types = ["varchar"] * len(table_cols)
    with open(f"{PATH}/ctas_empty.sql.jinja") as ctas_empty:
        return Template(ctas_empty.read()).render(
            schema_name=schema_name,
            table_name=table_name,
            table_cols=table_cols,
            table_cols_types=table_cols_types,
        )


def get_drop_view_table(name: str, view_or_table: str) -> str:
    """Generates a drop table if exists query"""
    if view_or_table in [e.value for e in TableView]:
        with open(f"{PATH}/drop_view_table.sql.jinja") as drop_view_table:
            return Template(drop_view_table.read()).render(
                view_or_table_name=name, view_or_table=view_or_table
            )


def get_extension_denormalize_query(config: utils.ExtensionConfig) -> str:
    """extracts target extension from a table into a denormalized table

    This function is targeted at a complex extension element that is at the root
    of a FHIR resource - as an example, see the 5 codes at the root node of
    http://hl7.org/fhir/us/core/STU6/StructureDefinition-us-core-patient.html.
    The template will create a new table with the extension data, in arrays,
    mapped 1-1 to the table id. You can specify multiple systems
    in the ExtensionConfig passed to this function. For each patient, we'll
    take the data from the first extension coding system we find for each patient.

    :param config: An instance of ExtensionConfig.
    """
    with open(f"{PATH}/extension_denormalize.sql.jinja") as extension_denormalize:
        return Template(extension_denormalize.read()).render(
            source_table=config.source_table,
            source_id=config.source_id,
            target_table=config.target_table,
            target_col_prefix=config.target_col_prefix,
            fhir_extension=config.fhir_extension,
            ext_systems=config.ext_systems,
            is_array=config.is_array,
        )


def get_insert_into_query(
    table_name: str,
    table_cols: List[str],
    dataset: List[List[str]],
    type_casts: Dict = {},
) -> str:
    """Generates an insert query for adding data to an existing athena table

    :param schema_name: The athena query to create the table in
    :param table_name: The name of the athena table to create
    :param table_cols: Comma deleniated column names, i.e. ['first','second']
    :param dataset: Array of data arrays to insert, i.e. [['1','3'],['2','4']]
    """
    with open(f"{PATH}/insert_into.sql.jinja") as insert_into:
        return Template(insert_into.read()).render(
            table_name=table_name,
            table_cols=table_cols,
            dataset=dataset,
            type_casts=type_casts,
        )


def get_is_table_not_empty_query(
    source_table: str,
    field: str,
    unnests: Optional[list[dict]] = [],
    conditions: Optional[list[str]] = [],
):
    with open(f"{PATH}/is_table_not_empty.sql.jinja") as is_table_not_empty:
        return Template(is_table_not_empty.read()).render(
            source_table=source_table,
            field=field,
            unnests=unnests,
            conditions=conditions,
        )


def get_select_all_query(source_table: str):
    with open(f"{PATH}/select_all.sql.jinja") as select_all:
        return Template(select_all.read()).render(source_table=source_table)


def get_show_tables(schema_name: str, prefix: str) -> str:
    """Generates a show tables query, filtered by prefix

    The intended use case for this function is to get a list of manifest study
    tables, so that they can be individually dropped during a clean call.

    :param schema_name: The athena schema to query
    :param table_name: The prefix to filter by. Jinja template auto adds '__'.
    """
    with open(f"{PATH}/show_tables.sql.jinja") as show_tables:
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
    with open(f"{PATH}/show_views.sql.jinja") as show_tables:
        return Template(show_tables.read()).render(
            schema_name=schema_name, prefix=prefix
        )
