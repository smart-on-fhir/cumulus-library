"""Collection of jinja template getters for common SQL queries"""

import enum
import pathlib

import jinja2
import pandas

from cumulus_library import db_config
from cumulus_library.template_sql import sql_utils


class TableView(enum.Enum):
    """Convenience enum for building drop queries"""

    TABLE = "TABLE"
    VIEW = "VIEW"


def get_base_template(
    filename_stem: str, path: pathlib.Path | None = None, **kwargs: dict
) -> str:
    """Abstract renderer for jinja templates

    You can use this renderer directly, but if you are designing a function
    that you expect to be commonly used, it's recommended to create a context
    specific loader function instead.

    This function will autoload macros in cumulus_library/template_sql/shared_macros,
    as well as any macros in a folder provided by path
    """
    base_path = pathlib.Path(__file__).parent
    with open(f"{path or base_path}/{filename_stem}.sql.jinja") as file:
        template = file.read()
        macro_paths = [base_path / "shared_macros"]
        if path:
            macro_paths.append(path)
        loader = jinja2.FileSystemLoader(macro_paths)
        env = jinja2.Environment(loader=loader).from_string(template)
        return env.render(**kwargs, db_type=db_config.db_type)


# All remaining functions are context-specific calls aimed at providing
# guidance around table creation for commonly used SQL functions


def get_alias_table_query(source_table: str, target_table: str):
    """Creates a view of source_table named target_table"""
    return get_base_template(
        "alias_table", source_table=source_table, target_table=target_table
    )


def get_codeable_concept_denormalize_query(
    config: sql_utils.CodeableConceptConfig,
) -> str:
    """extracts codeable concepts from a specified table.

    This function is targeted at arbitrary codeableConcept elements - see
    http://hl7.org/fhir/datatypes-definitions.html#CodeableConcept for more info.
    This may be or may not be an array field depending on the context of use -
    check the specification of the specific resource you're interested in.
    See the CodeableConceptConfig for details on how to handle array vs non-
    array use cases.

    :param config: a CodableConeptConfig
    """

    assert len(config.column_hierarchy) <= 2
    # If we get a None for code systems, we want one dummy value so the jinja
    # for loop will do a single pass. This implicitly means that we're not
    # filtering, so this parameter will be otherwise ignored
    config.code_systems = config.code_systems or ["all"]
    return get_base_template(
        "codeable_concept_denormalize",
        source_table=config.source_table,
        source_id=config.source_id,
        column_name=config.column_hierarchy[-1][0],
        parent_field=None
        if len(config.column_hierarchy) == 1
        else config.column_hierarchy[0][0],
        is_array=(config.column_hierarchy[0][1] == list),
        child_is_array=False
        if len(config.column_hierarchy) == 1
        else config.column_hierarchy[1][1] == list,
        target_table=config.target_table,
        filter_priority=config.filter_priority,
        code_systems=config.code_systems,
        extra_fields=config.extra_fields or [],
    )


def get_coding_denormalize_query(
    config: sql_utils.CodingConfig,
) -> str:
    """extracts codings from a specified table.

    This function reimplements get_codeable_concept_denormalize_query targeted
    at a bare coding element

    TODO: this is temporary and this should be replaced by a generic DN
    query.

    :param config: a CodingConfig
    """

    assert len(config.column_hierarchy) == 2
    # If we get a None for code systems, we want one dummy value so the jinja
    # for loop will do a single pass. This implicitly means that we're not
    # filtering, so this parameter will be otherwise ignored
    config.code_systems = config.code_systems or ["all"]
    return get_base_template(
        "coding_denormalize",
        source_table=config.source_table,
        source_id=config.source_id,
        column_name=config.column_hierarchy[1][0],
        parent_field=config.column_hierarchy[0][0],
        target_table=config.target_table,
        filter_priority=config.filter_priority,
        code_systems=config.code_systems,
    )


def get_column_datatype_query(
    schema_name: str,
    table_names: str | list,
    column_names: list | None = None,
    include_table_names: bool | None = False,
):
    """Gets the in-database data representation of a given column"""
    if isinstance(table_names, str):
        table_names = [table_names]
    return get_base_template(
        "column_datatype",
        schema_name=schema_name,
        table_names=table_names,
        column_names=column_names,
        include_table_names=include_table_names,
    )


def get_create_view_query(
    view_name: str, dataset: list[list[str]], view_cols: list[str]
) -> str:
    """Generates a create view as query for inserting static data into athena

    :param view_name: The name of the athena table to create
    :param dataset: Array of data arrays to insert, i.e. [['1','3'],['2','4']]
    :param table_cols: Comma deleniated column names, i.e. ['first,second']
    """
    return get_base_template(
        "create_view_as",
        view_name=view_name,
        dataset=dataset,
        view_cols=view_cols,
    )


def get_ctas_from_parquet_query(
    schema_name: str,
    table_name: str,
    local_location: str,
    remote_location: str,
    table_cols: list[str],
    remote_table_cols_types: list[str],
) -> str:
    """Generates a create table as query using a directory of parquet files as a source

    This function will generate an appropriate query for the underlying DB.
    Not all params are used by each type of database.

    :param schema_name: (athena) The schema to create the table in
    :param table_name: (all) The name of the athena table to create
    :param local_location: (duckdb) A directory containing parquet files to group
        into a table
    :param remote_location: (athena) An S3 URL to a directory containing parquert
        fiels to group into a table
    :param table_cols: (all) names of fields in your parquet to use as SQL columns
    :param remote_table_cols_types: (athena) The types to assign to the columns
        created in athena. Note that these should not be SQL types, but instead
        should be parquet types.
    """
    return get_base_template(
        "ctas_from_parquet",
        schema_name=schema_name,
        table_name=table_name,
        local_location=local_location,
        remote_location=remote_location,
        table_cols=table_cols,
        remote_table_cols_types=remote_table_cols_types,
    )


def get_ctas_query(
    schema_name: str, table_name: str, dataset: list[list[str]], table_cols: list[str]
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
    return get_base_template(
        "ctas",
        schema_name=schema_name,
        table_name=table_name,
        dataset=dataset,
        table_cols=table_cols,
    )


def get_ctas_query_from_df(
    schema_name: str, table_name: str, df: pandas.DataFrame
) -> str:
    """Generates a create table as query from a dataframe

    :param schema_name: The athena schema to create the table in
    :param table_name: The name of the athena table to create
    :param df: A pandas dataframe
    """
    split_dict = df.to_dict(orient="split")
    return get_base_template(
        "ctas",
        schema_name=schema_name,
        table_name=table_name,
        dataset=split_dict["data"],
        table_cols=split_dict["columns"],
    )


def get_ctas_empty_query(
    schema_name: str,
    table_name: str,
    table_cols: list[str],
    table_cols_types: list[str] | None = None,
) -> str:
    """Generates a create table as query for initializing an empty table

    Note that unlike other queries, the nature of the CTAS implementation in athena
    requires a schema name. This schema name should match the schema of your cursor,
    or the other queries in this template will not function correctly. All columns
    will be specified as varchar type.

    :param schema_name: The athena schema to create the table in
    :param table_name: The name of the athena table to create
    :param table_cols: Comma deleniated column names, i.e. ['first,second']
    :param table_cols_types: Allows specifying a data type per column
      (default: all varchar)
    """
    if not table_cols_types:
        table_cols_types = ["varchar"] * len(table_cols)
    return get_base_template(
        "ctas_empty",
        schema_name=schema_name,
        table_name=table_name,
        table_cols=table_cols,
        table_cols_types=table_cols_types,
    )


def get_drop_view_table(name: str, view_or_table: str) -> str:
    """Generates a drop table if exists query"""
    if view_or_table in [e.value for e in TableView]:
        return get_base_template(
            "drop_view_table", view_or_table_name=name, view_or_table=view_or_table
        )


def get_extension_denormalize_query(config: sql_utils.ExtensionConfig) -> str:
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
    return get_base_template(
        "extension_denormalize",
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
    table_cols: list[str],
    dataset: list[list[str]],
    type_casts: dict | None = None,
) -> str:
    """Generates an insert query for adding data to an existing athena table

    :param schema_name: The athena query to create the table in
    :param table_name: The name of the athena table to create
    :param table_cols: Comma deleniated column names, i.e. ['first','second']
    :param dataset: Array of data arrays to insert, i.e. [['1','3'],['2','4']]
    """
    type_casts = type_casts or {}
    return get_base_template(
        "insert_into",
        table_name=table_name,
        table_cols=table_cols,
        dataset=dataset,
        type_casts=type_casts,
    )


def get_is_table_not_empty_query(
    source_table: str,
    field: str,
    unnests: list[dict] | None = None,
    conditions: list[str] | None = None,
):
    unnests = unnests or []
    conditions = conditions or []
    return get_base_template(
        "is_table_not_empty",
        source_table=source_table,
        field=field,
        unnests=unnests,
        conditions=conditions,
    )


def get_select_all_query(source_table: str):
    return get_base_template(
        "select_all",
        source_table=source_table,
    )


def get_show_tables(schema_name: str, prefix: str) -> str:
    """Generates a show tables query, filtered by prefix

    The intended use case for this function is to get a list of manifest study
    tables, so that they can be individually dropped during a clean call.

    :param schema_name: The athena schema to query
    :param table_name: The prefix to filter by. Jinja template auto adds '__'.
    """
    return get_base_template("show_tables", schema_name=schema_name, prefix=prefix)


def get_show_views(schema_name: str, prefix: str) -> str:
    """Generates a show vies query, filtered by prefix

    The intended use case for this function is to get a list of manifest study
    views, so that they can be individually dropped during a clean call.

    :param schema_name: The athena schema to query
    :param table_name: The prefix to filter by. Jinja template auto adds '__'.
    """
    return get_base_template("show_views", schema_name=schema_name, prefix=prefix)
