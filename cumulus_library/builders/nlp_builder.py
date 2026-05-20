"""Class for generating NLP results"""

import json
import pathlib
import sys

import cumulus_fhir_support as cfs
import jambo
import msgspec
import rich

import cumulus_library
from cumulus_library import base_utils, note_utils
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
                "Finishing the build early because an NLP workflow was encountered.\n"
                "If you want to run this study's NLP tasks, pass --note-dir to "
                "provide a folder with FHIR resources like DiagnosticReport "
                "or DocumentReference with inlined clinical notes."
            )

    def _flatten_config(self, config_dir: pathlib.Path) -> None:
        """Takes any non-specified task values from the [shared] table"""
        fields = [x.name for x in msgspec.inspect.type_info(workflow.NlpShared).fields]
        for table_slug, task in self._workflow_config.tables.items():
            # Grab values from [shared] if not specified
            for field in fields:
                if getattr(task, field) is None:
                    shared_val = getattr(self._workflow_config.shared, field)
                    setattr(task, field, shared_val)

            # Validate task a little
            if not task.response_schema:
                raise ValueError(f"A response schema must be provided for table '{table_slug}'")

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

    def _print_token_stats(self, stats: driver.NlpStats) -> None:
        tokens = stats.token_stats
        rich.print("\n Token usage:")
        table = rich.table.Table("", "", box=None, show_header=False)
        table.add_row(" New input tokens:", f"{tokens.new_input_tokens:,}")
        table.add_row(" Input tokens read from cache:", f"{tokens.cache_read_input_tokens:,}")
        if tokens.cache_written_input_tokens:
            # This stat is only relevant for bedrock, so only show it if it's used
            table.add_row(
                " Input tokens written to cache:", f"{tokens.cache_written_input_tokens:,}"
            )
        table.add_row(" Output tokens:", f"{tokens.output_tokens:,}")

        # Estimate cost, if provided
        if prices := stats.token_prices:
            cost = (
                tokens.new_input_tokens * prices.new_input_tokens
                + tokens.cache_read_input_tokens * prices.cache_read_input_tokens
                + tokens.cache_written_input_tokens * prices.cache_written_input_tokens
                + tokens.output_tokens * prices.output_tokens
            )
            cost /= 1_000  # all prices are "per 1,000 tokens"
            cost *= prices.multiplier
            when = prices.date.strftime("%b %Y")
            table.add_row(f" Estimated cost (as of {when}):", f"${cost:.2f}")

        rich.get_console().print(table)

    def _run_nlp(self, config: cumulus_library.StudyConfig) -> None:
        # Gather note filters together
        cursor = config.db.cursor()
        select_by_tables = {
            task.select_by_table
            for task in self._workflow_config.tables.values()
            if task.select_by_table
        }

        # Add some extra checks if we are writing to a database like Athena that does not hold PHI
        if not config.db.can_hold_original_ids():
            # Only let approved studies run NLP, just because NLP can easily allow PHI through,
            # if you're careless with your prompts. Like having NLP quote directly from the note
            # into a field that isn't named "spans" (we automatically convert "spans" at least).
            study_allowlist = base_utils.get_study_allowlist()
            if self._nlp_config.target not in study_allowlist:
                raise RuntimeError(
                    f"The '{self._nlp_config.target}' study is not authorized to run NLP against "
                    "this database. Consider using a local duckdb database instead, or contact "
                    "the Cumulus Library project to allow this study to use unrestricted NLP."
                )

            # In order to compare anonymized IDs, we need the salt.
            if select_by_tables and not self._nlp_config.salt:
                raise RuntimeError(
                    "Cannot calculate anonymized resource IDs without a PHI dir defined. "
                    "Pass --etl-phi-dir and try again."
                )

        table_refs = {table: note_utils.get_table_refs(cursor, table) for table in select_by_tables}
        note_filters = [
            self._make_note_filter(table_refs, task)
            for task in self._workflow_config.tables.values()
        ]

        # Go through notes one by one and run NLP on them (save it to class, so we can examine them
        # in tests)
        self.stats = driver.run_nlp(
            self._notes,
            tables=self._workflow_config.tables,
            filters=note_filters,
            nlp_config=self._nlp_config,
            db=config.db,
        )

        # Print stat block, because that's interesting feedback
        if self._nlp_config.show_stats:
            task_slugs = list(self._workflow_config.tables)
            self._print_note_stats(names=task_slugs, stats=self.stats)
            self._print_token_stats(self.stats)

    def _table_is_view(self, study_config: cumulus_library.StudyConfig) -> bool:
        # When we create a table from parquet with duckdb, we are injecting the data and it
        # needs to already exist on disk. So we must run the NLP beforehand.
        # But in Athena, the table is dynamic and we can create the table first, so that even if
        # cumulus-library gets interrupted during NLP, the user can still see data in there.
        # Thus, we indicate here whether the database table we're gonna make will act like a view
        # or a materialized table.
        return study_config.db.db_type == "athena"

    def prepare_queries(
        self,
        *args,
        config: cumulus_library.StudyConfig,
        **kwargs,
    ):
        if not self._table_is_view(config):
            self._run_nlp(config)

        for table_slug, task in self._workflow_config.tables.items():
            table_schema = driver.schema_for_task(task)
            location = str(
                driver.output_path_for_task(self._nlp_config.target, table_slug, task, config.db)
            )
            remote_types = config.db.col_parquet_types_from_pyarrow(table_schema)
            self.queries.append(
                base_templates.get_ctas_from_parquet_query(
                    schema_name=config.schema,
                    table_name=driver.table_name_for_task(table_slug, self._nlp_config),
                    local_location=location,
                    remote_location=location,
                    table_cols=table_schema.names,
                    remote_table_cols_types=remote_types,
                )
            )

    def post_execution(
        self,
        config: cumulus_library.StudyConfig,
        *args,
        **kwargs,
    ):
        if self._table_is_view(config):
            self._run_nlp(config)
