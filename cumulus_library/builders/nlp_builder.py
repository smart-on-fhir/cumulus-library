"""Class for generating NLP results"""

import pathlib
import sys
from collections.abc import Callable

import msgspec
import rich

import cumulus_library
from cumulus_library import note_utils

# NLP is driven by a workflow config. See docs/workflows/nlp.md for more details
# on syntax and expectations.


class NlpShared(msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True):
    system_prompt: str | None = None
    user_prompt: str | None = None
    select_by_word: list[str] | None = None
    select_by_regex: list[str] | None = None
    select_by_table: str | None = None  # TODO
    reject_by_word: list[str] | None = None
    reject_by_regex: list[str] | None = None


class NlpTask(NlpShared):
    version: int = 0
    response_schema: str = ""
    name: str = ""


class NlpWorkflow(msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True):
    config_type: str
    task: list[NlpTask]
    shared: NlpShared = msgspec.field(default_factory=NlpShared)


class NlpBuilder(cumulus_library.BaseTableBuilder):
    def __init__(
        self,
        *args,
        toml_config_path: pathlib.Path,
        notes: note_utils.NoteSource,
        **kwargs,
    ):
        super().__init__()

        try:
            with open(toml_config_path, "rb") as file:
                file_bytes = file.read()
                self._workflow_config = msgspec.toml.decode(file_bytes, type=NlpWorkflow)

        except Exception as e:
            sys.exit(f"The NLP workflow at {toml_config_path!s} is invalid: \n{e}")

        self._flatten_config()

        self._notes = notes
        if not self._notes:
            sys.exit(
                "NLP workflow requested, but there are no notes to work with. "
                "Pass --note-dir to provide a folder with FHIR resources like DiagnosticReport "
                "or DocumentReference with inlined clinical notes."
            )

    def _flatten_config(self) -> None:
        """Takes any non-specified task values from the [shared] table"""
        fields = [x.name for x in msgspec.inspect.type_info(NlpShared).fields]
        for task in self._workflow_config.task:
            for field in fields:
                if getattr(task, field) is None:
                    shared_val = getattr(self._workflow_config.shared, field)
                    setattr(task, field, shared_val)

    def _make_note_filter(
        self, table_refs: dict[str, note_utils.TableRefs], task: NlpTask
    ) -> Callable[[dict, str], bool]:
        extra = {}
        if refs := table_refs.get(task.select_by_table):
            extra["select_by_note_ref"] = refs.notes
            extra["select_by_patient_ref"] = refs.patients
        return note_utils.make_note_filter(
            reject_by_regex=task.reject_by_regex,
            reject_by_word=task.reject_by_word,
            select_by_regex=task.select_by_regex,
            select_by_word=task.select_by_word,
            **extra,
        )

    def _print_note_stats(
        self,
        *,
        names: list[str],
        available: int,
        had_text: int,
        considered: list[int],
        got_response: list[int],
    ) -> None:
        rich.print(" Notes processed:")
        table = rich.table.Table("", "", box=None, show_header=False)
        table.add_row(" Available:", f"{available:,}")
        table.add_row(" Had text:", f"{had_text:,}")
        for idx, name in enumerate(names):
            suffix = f" ({name})" if name else ""
            table.add_row(f" Considered{suffix}:", f"{considered[idx]:,}")
            table.add_row(f" Got response{suffix}:", f"{got_response[idx]:,}")
        rich.get_console().print(table)

    def prepare_queries(
        self,
        *args,
        config: cumulus_library.StudyConfig,
        **kwargs,
    ):
        # Set up some stat counters
        available = 0
        had_text = 0
        considered = [0] * len(self._workflow_config.task)
        got_response = [0] * len(self._workflow_config.task)

        # Gather note filters together
        cursor = config.db.cursor()
        select_by_tables = {task.select_by_table for task in self._workflow_config.task}

        if list(filter(None, select_by_tables)) and not self._notes.salt:
            raise RuntimeError(
                "Cannot calculate anonymized resource IDs without a PHI dir defined. "
                "Pass --etl-phi-dir and try again."
            )

        table_refs = {
            table: note_utils.get_table_refs(cursor, table) for table in select_by_tables if table
        }
        note_filters = [
            self._make_note_filter(table_refs, task) for task in self._workflow_config.task
        ]

        # Go through notes one by one and run NLP on them
        for note_res in self._notes.progress_iter("Running NLP..."):
            available += 1

            try:
                text = note_utils.get_text_from_note_res(note_res)
            except Exception:  # noqa: S112
                continue
            had_text += 1

            for idx, task in enumerate(self._workflow_config.task):
                if not note_filters[idx](note_res, text=text, salt=self._notes.salt):
                    continue
                considered[idx] += 1

                # TODO: the actual NLP :D

        # Print stat block, because that's interesting feedback
        self._print_note_stats(
            names=[t.name for t in self._workflow_config.task],
            available=available,
            had_text=had_text,
            considered=considered,
            got_response=got_response,
        )
