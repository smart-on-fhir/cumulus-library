"""Class for generating NLP results"""

import json
import pathlib
import sys

import cumulus_fhir_support as cfs
import jambo
import msgspec
import rich

import cumulus_library
from cumulus_library import note_utils
from cumulus_library.builders.nlp import driver, workflow
from cumulus_library.template_sql import base_templates

# NLP is driven by a workflow config. See docs/workflows/nlp.md for more details
# on syntax and expectations.


class NlpBuilder(cumulus_library.BaseTableBuilder):
    def __init__(
        self,
        *args,
        toml_config_path: pathlib.Path,
        notes: note_utils.NoteSource,
        nlp_config: note_utils.NlpConfig | None = None,
        **kwargs,
    ):
        super().__init__()
        self.stats = None
        toml_config_path = pathlib.Path(toml_config_path)
        self._nlp_config = nlp_config or note_utils.NlpConfig()

        try:
            with open(toml_config_path, "rb") as file:
                file_bytes = file.read()
                self._workflow_config = msgspec.toml.decode(file_bytes, type=workflow.NlpWorkflow)

        except Exception as e:
            sys.exit(f"The NLP workflow at {toml_config_path!s} is invalid: \n{e}")

        self._flatten_config(toml_config_path.parent)

        self._notes = notes
        if not self._notes:
            sys.exit(
                "NLP workflow requested, but there are no notes to work with. "
                "Pass --note-dir to provide a folder with FHIR resources like DiagnosticReport "
                "or DocumentReference with inlined clinical notes."
            )

    def _flatten_config(self, config_dir: pathlib.Path) -> None:
        """Takes any non-specified task values from the [shared] table"""
        fields = [x.name for x in msgspec.inspect.type_info(workflow.NlpShared).fields]
        for task in self._workflow_config.task:
            # Grab values from [shared] if not specified
            for field in fields:
                if getattr(task, field) is None:
                    shared_val = getattr(self._workflow_config.shared, field)
                    setattr(task, field, shared_val)

            # Validate task a little
            if not task.name:
                raise ValueError("A task name must be provided")
            if not task.response_schema:
                raise ValueError(f"A response schema must be provided for task {task.name}")

            # Convert task schema filenames to the JSON schema itself.
            # Be strict here, just for safety's sake - we can ease up if needed later.
            if "/" in task.response_schema:
                raise ValueError("response_schema must be a simple filename, no path elements")

            # Load and parse the response JSON schema into a pydantic model, but check first to
            # ensure we don't already have an inline JSON definition (useful in tests).
            if task.response_schema.lstrip().startswith("{"):
                task.response_schema = json.loads(task.response_schema)
            else:
                schema_path = config_dir.joinpath(task.response_schema)
                with open(schema_path, "rb") as f:
                    task.response_schema = json.load(f)

            # We are currently using the Python project `jambo`, but it's a new project, less than
            # a year old. If it becomes obsolete, we can reimplement the parts we care about by
            # maybe adapting the StackOverflow answers below and augmenting it with (at least)
            # $defs/$ref support. It wouldn't be pretty, but...
            # https://stackoverflow.com/questions/73841072/
            task.response_schema = jambo.SchemaConverter.build(task.response_schema)

    def _make_note_filter(
        self, table_refs: dict[str, cfs.RefSet], task: workflow.NlpTask
    ) -> cfs.NoteFilter:
        extra = {}
        if refs := table_refs.get(task.select_by_table):
            extra["select_by_ref"] = refs
        return cfs.make_note_filter(
            reject_by_regex=task.reject_by_regex,
            reject_by_word=task.reject_by_word,
            salt=self._nlp_config.salt,
            select_by_regex=task.select_by_regex,
            select_by_word=task.select_by_word,
            **extra,
        )

    def _print_note_stats(self, *, names: list[str], stats: driver.NlpStats) -> None:
        rich.print(" Notes processed:")
        table = rich.table.Table("", "", box=None, show_header=False)
        table.add_row(" Available:", f"{stats.available:,}")
        table.add_row(" Had text:", f"{stats.had_text:,}")
        for idx, name in enumerate(names):
            suffix = f" ({name})" if name else ""
            table.add_row(f" Considered{suffix}:", f"{stats.considered[idx]:,}")
            table.add_row(f" Got response{suffix}:", f"{stats.got_response[idx]:,}")
        rich.get_console().print(table)

    def _run_nlp(self, config: cumulus_library.StudyConfig) -> None:
        # Gather note filters together
        cursor = config.db.cursor()
        select_by_tables = {
            task.select_by_table for task in self._workflow_config.task if task.select_by_table
        }

        if select_by_tables and not self._nlp_config.salt:
            raise RuntimeError(
                "Cannot calculate anonymized resource IDs without a PHI dir defined. "
                "Pass --etl-phi-dir and try again."
            )

        table_refs = {table: note_utils.get_table_refs(cursor, table) for table in select_by_tables}
        note_filters = [
            self._make_note_filter(table_refs, task) for task in self._workflow_config.task
        ]

        # Go through notes one by one and run NLP on them (save it to class, so we can examine them
        # in tests)
        self.stats = driver.run_nlp(
            self._notes,
            tasks=self._workflow_config.task,
            filters=note_filters,
            nlp_config=self._nlp_config,
        )

        # Print stat block, because that's interesting feedback
        # TODO MIKE: print token stats too
        self._print_note_stats(names=[t.name for t in self._workflow_config.task], stats=self.stats)

    def _table_is_view(self, study_config: cumulus_library.StudyConfig) -> bool:
        # When we create a table from parquet with duckdb, we are injecting the data and it
        # needs to already exist on disk. So we must run the NLP beforehand.
        # But in Athena, the table is dynamic and we can create the table first, so that even if
        # cumulus-library gets interrupted during NLP, the user can still see data in there.
        # Thus, we indicate here whether the database table we're gonna make will act like a view
        # or a materialized table.
        return study_config.db.db_type == "athena"

    def _headers(self) -> list[str]:
        # If you change these, change the schema definition in nlp_builder.py too
        return [
            "note_ref",
            "encounter_ref",
            "subject_ref",
            "generated_on",
            "task_version",
            "model",
            "system_fingerprint",
            "result",
        ]

    def _col_types_for_task(self, task: workflow.NlpTask) -> list[str]:
        # If you change these, change the schema definition in nlp_builder.py too
        return [
            "VARCHAR",
            "VARCHAR",
            "VARCHAR",
            "VARCHAR",
            "INT",
            "VARCHAR",
            "VARCHAR",
            "VARCHAR",  # TODO MIKE: this last one is a lie for now - convert to struct
        ]

    def prepare_queries(
        self,
        *args,
        config: cumulus_library.StudyConfig,
        **kwargs,
    ):
        if not self._table_is_view(config):
            self._run_nlp(config)

        self.queries = [
            base_templates.get_ctas_empty_query(  # TODO MIKE: use _from_parquet
                schema_name=config.schema,
                table_name=driver.table_name_for_task(task, self._nlp_config),
                table_cols=self._headers(),
                table_cols_types=self._col_types_for_task(task),
            )
            for task in self._workflow_config.task
        ]

    def post_execution(
        self,
        config: cumulus_library.StudyConfig,
        *args,
        **kwargs,
    ):
        if self._table_is_view(config):
            self._run_nlp(config)
