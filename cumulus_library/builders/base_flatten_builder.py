"""Shared machinery for the builders that flatten NLP result tables (wide and highlight).

Both builders do the same things around a different core: load a TOML of ``[tables.<name>]``
entries, find the NLP result tables that feed each one, then render one table per entry (or an
empty one with the right columns, if no result tables exist yet). Only the config shape, the
template, and the empty-table columns differ, so those are what subclasses provide.
"""

import abc
import pathlib
import sys
from collections.abc import Iterable
from typing import ClassVar

import msgspec

from cumulus_library import BaseTableBuilder, base_utils, databases, errors, study_manifest
from cumulus_library.template_sql import base_templates, sql_utils


class BaseFlattenBuilder(BaseTableBuilder, abc.ABC):
    """Base for builders that turn NLP result tables into wide/highlight tables"""

    # What the config_type of a TOML for this builder is, and what to call its tables
    config_type: ClassVar[str]
    table_suffix: ClassVar[str]
    # The msgspec struct describing the whole TOML file
    workflow_type: ClassVar[type[msgspec.Struct]]

    def __init__(self, *args, toml_config_path: pathlib.Path, **kwargs):
        super().__init__(*args, **kwargs)
        toml_config_path = pathlib.Path(toml_config_path)
        try:
            with open(toml_config_path, "rb") as file:
                self._workflow_config = msgspec.toml.decode(file.read(), type=self.workflow_type)
        except Exception as e:
            sys.exit(f"The {self.config_type} workflow at {toml_config_path!s} is invalid: \n{e}")
        if self._workflow_config.config_type != self.config_type:
            sys.exit(
                f"The workflow at {toml_config_path!s} has config_type "
                f"'{self._workflow_config.config_type}', not '{self.config_type}'."
            )
        if not self._workflow_config.tables:
            sys.exit(f"The {self.config_type} workflow at {toml_config_path!s} has no tables.")

        # Normalize everything up front, so that a bad config fails when it is loaded rather
        # than partway through a build.
        self._normalized = {
            name: self._normalize(name, table)
            for name, table in self._workflow_config.tables.items()
        }

    ###########################################################################
    # What subclasses provide
    ###########################################################################
    @abc.abstractmethod
    def _normalize(self, name: str, table: msgspec.Struct) -> dict:
        """Resolves a table's config into the keyword arguments its template needs"""

    @abc.abstractmethod
    def _render(
        self,
        *,
        dest_table: str,
        table_names: list[str],
        task_versions: list[int],
        normalized: dict,
    ) -> str:
        """Renders the table's SQL from its source tables"""

    @abc.abstractmethod
    def _empty_columns_and_types(self, normalized: dict) -> tuple[list[str], list[str]]:
        """Returns (column names, column types) for the table when it has no source tables"""

    ###########################################################################
    # Shared behavior
    ###########################################################################
    @staticmethod
    def dest_table_name(prefix: str, name: str, suffix: str) -> str:
        """Returns the destination table for a table entry, like 'my_study__llm_age_wide'"""
        return f"{prefix}__llm_{name.lower()}_{suffix}"

    def _source_task(self, name: str) -> str:
        return (self._workflow_config.tables[name].source_task or name).lower()

    def _source_tables(self, config: base_utils.StudyConfig, prefix: str, name: str) -> list[str]:
        """Finds the NLP result tables that have data and feed this table, in a stable order"""
        task = self._source_task(name)
        table = self._workflow_config.tables[name]
        stem = f"{prefix}__nlp_{task}_"

        if table.source_models:
            candidates = [f"{stem}{model}" for model in table.source_models]
        else:
            candidates = self._discover_tables(config, stem)
            # If this study has another task whose name extends ours (say 'lab' and
            # 'lab_panel_cbc'), its tables also start with our stem. Those are not ours.
            longer = {
                f"{prefix}__nlp_{self._source_task(other)}_"
                for other in self._workflow_config.tables
                if self._source_task(other) != task and self._source_task(other).startswith(task)
            }
            candidates = [c for c in candidates if not any(c.startswith(x) for x in longer)]

        valid = []
        with base_utils.get_progress_bar() as progress:
            progress_task = progress.add_task(
                f"Discovering NLP tables for {name}...", total=len(candidates)
            )
            for candidate in candidates:
                if self._is_table_valid(config.db, candidate):
                    valid.append(candidate)
                progress.advance(progress_task)
        return sorted(valid)

    @staticmethod
    def _discover_tables(config: base_utils.StudyConfig, stem: str) -> list[str]:
        query = base_templates.get_select_from_single_query(
            schema="information_schema",
            table_name="tables",
            columns=["table_name"],
            where_clauses=[
                [f"table_schema = '{config.schema}'", f"REGEXP_LIKE(table_name, '^{stem}.+$')"]
            ],
        )
        return [row[0] for row in config.db.cursor().execute(query).fetchall()]

    @staticmethod
    def _is_table_valid(database: databases.DatabaseBackend, table_name: str) -> bool:
        """Whether a table exists and has a `result` column to flatten"""
        return sql_utils.is_field_present(
            database=database,
            source_table=table_name,
            source_col="result",
            expected={},
        )

    def _make_query(
        self,
        config: base_utils.StudyConfig,
        prefix: str,
        name: str,
        task_versions: Iterable[int],
    ) -> str:
        dest_table = self.dest_table_name(prefix, name, self.table_suffix)
        normalized = self._normalized[name]
        if table_names := self._source_tables(config, prefix, name):
            return self._render(
                dest_table=dest_table,
                table_names=table_names,
                task_versions=list(task_versions),
                normalized=normalized,
            )
        # Nothing to flatten yet, but create the table anyway so that downstream SQL that
        # reads it still works, and with the same columns it would have had.
        cols, types = self._empty_columns_and_types(normalized)
        return base_templates.get_ctas_empty_query(
            schema_name=config.schema,
            table_name=dest_table,
            table_cols=cols,
            table_cols_types=types,
        )

    def prepare_queries(
        self,
        *args,
        config: base_utils.StudyConfig,
        manifest: study_manifest.StudyManifest,
        **kwargs,
    ):
        prefix = manifest.get_study_prefix()
        if not prefix:
            raise errors.NlpTableConfigError(
                f"{self.config_type} workflows need a study_prefix in the study's manifest"
            )
        for name, table in self._workflow_config.tables.items():
            self.queries.append(self._make_query(config, prefix, name, table.task_versions))
