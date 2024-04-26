import pathlib

from cumulus_library.template_sql import base_templates

PATH = pathlib.Path(__file__).parent


def get_local_template(
    target_table: str,
    schema: dict[dict[bool]] | None = None,
    config: dict | None = None,
) -> str:
    return base_templates.get_base_template(
        target_table, path=PATH, schema=schema, config=config
    )
