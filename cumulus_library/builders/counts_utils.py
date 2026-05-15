import pathlib
import sys

import msgspec

from cumulus_library.builders.statistics_templates import counts_templates


class CountsWorkflowAnnotation(msgspec.Struct, forbid_unknown_fields=True):
    field: str
    join_table: str
    join_field: str
    columns: list[list[str]]
    alt_target: str | None = None


class CountsFilterColumn(msgspec.Struct, forbid_unknown_fields=True):
    name: str
    values: list[str]
    include_nulls: bool


class CountsWorkflowTable(msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True):
    source_table: str
    table_cols: list

    description: str | None = None
    where_clauses: list[str] | None = None
    min_subject: int | None = None
    primary_id: str | None = None
    secondary_table: str | None = None
    secondary_cols: list[str] | None = None
    secondary_id: str | None = None
    alt_secondary_join_id: str | None = None
    annotation: CountsWorkflowAnnotation | None = None
    filter_cols: list[CountsFilterColumn] | None = None


class CountsWorkflow(msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True):
    config_type: str
    tables: dict[str, CountsWorkflowTable]


def load_toml_config(toml_config_path: pathlib.Path) -> dict:
    try:
        with open(toml_config_path, "rb") as file:
            file_bytes = file.read()
            workflow_config = msgspec.to_builtins(
                msgspec.toml.decode(file_bytes, type=CountsWorkflow)
            )
    except msgspec.ValidationError as e:
        sys.exit(f"The counts workflow at {toml_config_path!s} contains an unexpected param: \n{e}")

    # for cases where we want to load a portion of a parseed dict into objects
    for table_name, contents in workflow_config["tables"].items():
        if annotation := contents.get("annotation"):
            workflow_config["tables"][table_name]["annotation"] = counts_templates.CountAnnotation(
                **annotation
            )
        if filter_cols := contents.get("filter_cols"):
            parsed_filters = []
            for col in filter_cols:
                parsed_filters.append(counts_templates.FilterColumn(**col))
            workflow_config["tables"][table_name]["filter_cols"] = parsed_filters
    return workflow_config
