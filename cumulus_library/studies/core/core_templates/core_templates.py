import pathlib
import typing


import jinja2

PATH = pathlib.Path(__file__).parent


def get_core_template(target_table: str, schema: dict[dict[bool]]) -> str:
    """Extracts code system details as a standalone table"""
    with open(f"{PATH}/{target_table}.sql.jinja") as file:
        core_template = file.read()
        loader = jinja2.FileSystemLoader(PATH)
        env = jinja2.Environment(loader=loader).from_string(core_template)
        return env.render(schema=schema)
