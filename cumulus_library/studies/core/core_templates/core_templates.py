import pathlib
import typing

import jinja2

from cumulus_library.template_sql import base_templates

PATH = pathlib.Path(__file__).parent


def get_core_template(
    target_table: str, schema: dict[dict[bool]] = None, config: dict = None
) -> str:
    """Extracts code system details as a standalone table"""
    with open(f"{PATH}/{target_table}.sql.jinja") as file:
        core_template = file.read()
        loader = jinja2.FileSystemLoader(PATH)
        env = jinja2.Environment(loader=loader).from_string(core_template)
        return env.render(schema=schema, config=config)


def validate_schema(cursor: object, schema: str, expected_table_cols, parser):
    validated_schema = {}
    for table, cols in expected_table_cols.items():
        query = base_templates.get_column_datatype_query(schema, table, cols.keys())
        table_schema = cursor.execute(query).fetchall()
        validated_schema[table] = parser.validate_table_schema(cols, table_schema)
    return validated_schema
