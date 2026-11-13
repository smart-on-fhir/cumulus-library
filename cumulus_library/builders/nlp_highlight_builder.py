"""Builds Label Studio highlight tables from NLP results: one row per supporting text span"""

from cumulus_library.builders import base_flatten_builder
from cumulus_library.builders.nlp import base_flatten_utils
from cumulus_library.template_sql import base_templates


class NlpHighlightBuilder(base_flatten_builder.BaseFlattenBuilder):
    """Flattens NLP result tables into highlights, as declared by a ``nlp_highlight`` config.

    Each ``[tables.<name>]`` entry creates ``<study>__llm_<name>_highlights`` from the
    ``<study>__nlp_<task>_<model>`` tables for that task, one SELECT per model and mention,
    UNION ALLed. See docs/workflows/nlp-tables.md for the config syntax.
    """

    display_text = "Transforming NLP results into highlights..."
    config_type = "nlp_highlight"
    table_suffix = "highlights"
    workflow_type = base_flatten_utils.HighlightWorkflow

    def _normalize(self, name: str, table: base_flatten_utils.HighlightTable) -> dict:
        return {"mentions": base_flatten_utils.normalize_highlights(table.mentions, owner=name)}

    def _render(self, *, dest_table, table_names, task_versions, normalized) -> str:
        return base_templates.get_template(
            "nlp_highlight",
            dest_table=dest_table,
            table_names=table_names,
            task_versions=task_versions,
            **normalized,
        )

    def _empty_columns_and_types(self, normalized: dict) -> tuple[list[str], list[str]]:
        """
        Every highlight table has the same flat shape, so the empty one is a constant
        """
        cols = base_flatten_utils.HIGHLIGHT_COLUMNS
        return cols, ["varchar"] * len(cols)
