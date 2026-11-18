"""Builds wide tables from NLP results: one row per note (or mention), one column per field"""

from cumulus_library.builders import base_flatten_builder
from cumulus_library.builders.nlp import base_flatten_utils
from cumulus_library.template_sql import base_templates


class NlpWideBuilder(base_flatten_builder.BaseFlattenBuilder):
    """Flattens NLP result tables into wide tables, as declared by a ``nlp_wide`` TOML config.

    Each ``[tables.<name>]`` entry creates ``<study>__llm_<name>_wide`` from the
    ``<study>__nlp_<task>_<model>`` tables for that task, one SELECT per model, UNION ALLed.
    See docs/workflows/nlp-tables.md for the config syntax.
    """

    display_text = "Flattening NLP results into wide tables..."
    config_type = "nlp_wide"
    table_suffix = "wide"
    workflow_type = base_flatten_utils.WideWorkflow

    def _normalize(self, name: str, table: base_flatten_utils.WideTable) -> dict:
        unnests, columns = base_flatten_utils.normalize_wide_table(
            table.unnest_paths, table.columns, owner=name
        )
        # `output_columns`, not `columns`: the sqlfluff lint config already uses `columns` as a
        # sample variable (a list of strings) for other templates, so reusing it here would collide.
        return {"unnest_paths": unnests, "output_columns": columns}

    def _render(self, *, dest_table, table_names, task_versions, normalized) -> str:
        return base_templates.get_template(
            "nlp_wide",
            dest_table=dest_table,
            table_names=table_names,
            task_versions=task_versions,
            **normalized,
        )

    def _empty_columns_and_types(self, normalized: dict) -> tuple[list[str], list[str]]:
        """
        The empty table has exactly the columns the populated one would
        """
        columns = normalized["output_columns"]
        return (
            # Get the metadata column names; use aliases for the data columns
            base_flatten_utils.METADATA_COLUMNS + [c["alias"] for c in columns],
            # Use varchar for all the metadata types, and the types the columns would have
            ["varchar"] * len(base_flatten_utils.METADATA_COLUMNS)
            + [c["sql_type"] for c in columns],
        )
