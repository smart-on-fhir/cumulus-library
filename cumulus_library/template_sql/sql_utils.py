"""utility functions related to populating jinja templates

Currently, these deal with various edge cases around complex nested objects in sql
and validating them so that the actual generated queries can be constructed more
simply. This includes, but is not limited, to:
    - Data missing entirely
    - Data present, but 'nullish' - with some structure but no value
    - Data with deep missing elements
    - Data which may or may not be in an array depending on context
"""

from dataclasses import dataclass, field

from cumulus_library import base_utils, databases
from cumulus_library.template_sql import base_templates

# *** Some convenience constants for providing to validate_schema() ***
# * Only include necessary fields that we actually use.
# * If any sub-field is not present, we may ignore the parent-field.
# * Though, cumulus-fhir-support does guarantee a full Coding or Concept.
CODING = ["code", "display", "system", "userSelected", "version"]
# If you think you need CODEABLE_CONCEPT, you probably need a de-normalization table
# instead, since CodeableConcepts can contain multiple entries.
CODEABLE_CONCEPT = {"coding": CODING, "text": {}}
REFERENCE = ["reference"]


@dataclass(kw_only=True)
class BaseConfig:
    """Base class for handling table detection/denormalization"""

    source_table: str = None
    source_id: str = "id"
    target_table: str = None
    has_data: bool = False


@dataclass(kw_only=True)
class CodeableConceptConfig(BaseConfig):
    """Holds parameters for generating codableconcept tables.

    :keyword column_name: the column containing the codeableConcept you want to extract.
    :keyword is_array: whether the codeableConcept is 0...1 or 0..* in the FHIR spec
    :keyword source_table: the table to extract extensions from
    :keyword target_table: the name of the table to create
    :keyword source_id: the id field to use in the new table (default: 'id')
    :keyword filter_priority: If true, will use code systems to select a single code,
      in preference order, for use as a display value.
    :keyword code_systems: a list of strings matching the start of the systems field,
      in preference order, for selecting data for filtering. This should not be set
      if filter_priority is false.
    :keyword expected: a schema fragment for what the shape of this field should be.
      If any bit of this schema fragment is not present, there will be no results.
    :keyword extra_fields: extra fields to include, as siblings of the concept.
      It's a list of tuples (field, alias).
    """

    column_hierarchy: list[tuple]
    filter_priority: bool = False
    code_systems: list = None
    expected: list | dict = field(default_factory=lambda: CODEABLE_CONCEPT)
    extra_fields: list[tuple] = None  # candidate for moving into base config


@dataclass(kw_only=True)
class CodingConfig(BaseConfig):
    column_hierarchy: list[tuple]
    filter_priority: bool = False
    code_systems: list = None
    expected: list | dict = field(default_factory=lambda: CODING)


@dataclass(kw_only=True)
class ExtensionConfig(BaseConfig):
    """convenience class for holding parameters for generating extension tables.

    :keyword source_table: the table to extract extensions from
    :keyword source_id: the id column to treat as a foreign key
    :keyword target_table: the name of the table to create
    :keyword target_col_prefix: the string to prepend code/display column names with
    :keyword fhir_extension: the URL of the FHIR resource to select
    :keyword ext_systems: a list of codes, in preference order, to use to select data
    :keyword is_array: a boolean indicating if the targeted field is an array type
    """

    target_col_prefix: str
    fhir_extension: str
    ext_systems: list[str]
    is_array: bool = False


def _check_data_in_fields(
    database: databases.DatabaseBackend,
    code_sources: list[CodeableConceptConfig],
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
            "Detecting available codeableConcepts...",
            # Each column in code_sources requires at most 3 queries to
            # detect valid data is in the DB
            total=len(code_sources),
        )
        for code_source in code_sources:
            code_source.has_data = is_field_populated(
                database=database,
                source_table=code_source.source_table,
                hierarchy=code_source.column_hierarchy,
                expected=code_source.expected,
            )
            progress.advance(task)
    return code_sources


def denormalize_complex_objects(
    database: databases.DatabaseBackend,
    code_sources: list[BaseConfig],
):
    queries = []
    code_sources = _check_data_in_fields(database, code_sources)
    for code_source in code_sources:
        # TODO: This method of pairing classed config objects to
        # specific queries should be considered temporary. This should be
        # replaced at some point by a more generic table schema traversal/
        # generic jinja template approach.
        match code_source:
            case CodeableConceptConfig():
                if code_source.has_data:
                    queries.append(
                        base_templates.get_codeable_concept_denormalize_query(
                            code_source
                        )
                    )
                else:
                    table_cols = [
                        "id",
                        "row",
                        "code",
                        "code_system",
                        "display",
                        "userSelected",
                    ]
                    col_types = [
                        "varchar",
                        "bigint",
                        "varchar",
                        "varchar",
                        "varchar",
                        "boolean",
                    ]
                    if code_source.extra_fields:
                        table_cols += [f[1] for f in code_source.extra_fields]
                        col_types += ["varchar"] * len(code_source.extra_fields)
                    queries.append(
                        base_templates.get_ctas_empty_query(
                            schema_name=database.schema_name,
                            table_name=code_source.target_table,
                            table_cols=table_cols,
                            table_cols_types=col_types,
                        )
                    )
            case CodingConfig():
                if code_source.has_data:
                    queries.append(
                        base_templates.get_coding_denormalize_query(code_source)
                    )
                else:
                    queries.append(
                        base_templates.get_ctas_empty_query(
                            schema_name=database.schema_name,
                            table_name=code_source.target_table,
                            table_cols=["id", "code", "code_system", "display"],
                        )
                    )

    return queries


def validate_schema(
    database: databases.DatabaseBackend,
    expected_table_cols: dict,
) -> dict:
    validated_schema = {}
    for table, cols in expected_table_cols.items():
        query = base_templates.get_column_datatype_query(
            database.schema_name, table, cols.keys()
        )

        try:
            table_schema = database.cursor().execute(query).fetchall()
        except database.operational_errors():
            # A database backend might reasonably raise an exception in cases like
            # the table not existing (Athena does this).
            table_schema = []

        validated_schema[table] = database.parser().validate_table_schema(
            cols, table_schema
        )
    return validated_schema


def is_field_populated(
    *,
    database: databases.DatabaseBackend,
    source_table: str,
    hierarchy: list[tuple],
    expected: list | dict | None = None,
) -> bool:
    """Traverses a complex field and determines if it exists and has data

    Non-core studies that rely on the core tables shouldn't need this method.
    This is just to examine the weird and wonderful world of the raw FHIR tables.

    :keyword database: The database backend
    :keyword source_table: The table to query against
    :keyword hierarchy: a list of tuples defining the FHIR path to the element.
        Each tuple should be of the form ('element_name', dict | list), where
        a dict is a bare nested object and a list is an array object
    :keyword expected: a list of elements that should be present in the field.
        If none, we assume it is a CodeableConcept.
    :returns: a boolean indicating if valid data is present.
    """
    if not is_field_present(
        database=database,
        source_table=source_table,
        source_col=hierarchy[0][0],
        expected=expected,
    ):
        return False
    unnests = []
    source_field = []
    for element in hierarchy:
        if element[1] == list:
            unnests.append(
                {
                    "source_col": ".".join([*source_field, element[0]]),
                    "table_alias": f"{element[0]}_table",
                    "row_alias": f"{element[0]}_row",
                },
            )
            last_table_alias = f"{element[0]}_table"
            last_row_alias = f"{element[0]}_row"
            source_field = [last_table_alias, last_row_alias]
        elif element[1] == dict:
            source_field.append(element[0])
        else:
            raise ValueError(
                "sql_utils.is_field_populated: Unexpected type "
                f"{element[1]} for field {element[0]}"
            )
    query = base_templates.get_is_table_not_empty_query(
        source_table=source_table, field=".".join(source_field), unnests=unnests
    )
    res = database.cursor().execute(query).fetchall()
    if len(res) == 0:
        return False
    return True


def is_field_present(
    *,
    database: databases.DatabaseBackend,
    source_table: str,
    source_col: str,
    expected: list | dict | None = None,
) -> bool:
    """Validation check for a column existing, and having the expected schema

    :keyword database: The database backend
    :keyword source_table: The table to query against
    :keyword source_col: The column to check the schema against
    :keyword expected: a list of elements that should be present in source_col.
        If none, we assume it is a CodeableConcept.
    :returns: a boolean indicating if the schema was found.
    """
    if expected is None:
        expected = CODEABLE_CONCEPT

    table_cols = {source_table: {source_col: expected}}
    schema = validate_schema(database, table_cols)

    def _get_all_values(d: dict) -> list:
        all_values = []
        for value in d.values():
            if isinstance(value, dict):
                all_values += _get_all_values(value)
            else:
                all_values.append(value)
        return all_values

    all_schema_values = _get_all_values(schema)
    return all(all_schema_values)
