import datetime
import json
import re
import string

import cumulus_fhir_support as cfs
import rich

from cumulus_library import errors, note_utils

from . import caching, models, workflow

ESCAPED_WHITESPACE = re.compile(r"(\\\s)+")


class NlpStats:
    def __init__(self, size: int):
        self.available = 0
        self.had_text = 0
        self.considered = [0] * size
        self.got_response = [0] * size


def run_nlp(
    notes: note_utils.NoteSource,
    *,
    nlp_config: note_utils.NlpConfig,
    tasks: list[workflow.NlpTask],
    filters: list[cfs.NoteFilter],
) -> NlpStats:
    """Iterates through the notes, filtering as it goes, and passes notes to NLP"""
    stats = NlpStats(len(tasks))
    pool = NlpNotePool(nlp_config)

    for note_res in notes.progress_iter("Running NLP..."):
        stats.available += 1

        try:
            text = cfs.get_text_from_note_res(note_res)
        except Exception:  # noqa: S112
            continue
        stats.had_text += 1

        for idx, task in enumerate(tasks):
            if not filters[idx](note_res, text=text):
                continue
            stats.considered[idx] += 1

            try:
                stats.got_response[idx] += pool.add_note(task, note_res, text)
            except Exception as exc:
                rich.print("Failed to process note:", exc)

    # Finalize each task (in case a batch is waiting to be sent)
    for idx, task in enumerate(tasks):
        try:
            stats.got_response[idx] += pool.finalize(task)
        except Exception as exc:
            rich.print("Failed to process note:", exc)

    return stats


### Internal driver API to help manage the process


class NlpNotePool:
    """
    NLP has a couple different batch limits going on. This class pools the notes until one is hit.

    First, you have the general memory batch limit - we don't want to hold too many notes at once
    or we'll hit out-of-memory errors.

    Second (if we're doing batch-processing of NLP), there's the provider's batch limits, either
    in byte size and/or number-of-notes.

    But consumers of this class don't have to care about all that. It will just accept notes and
    either send them out to NLP or not depending on where the limits are.
    """

    def __init__(self, nlp_config: note_utils.NlpConfig):
        self._config = nlp_config
        self._model = models.create_model(nlp_config)
        self._provider = self._model.provider

        self._notes = {}  # task_name -> list[output row]

        if self._config.use_batching and not self._provider.supports_batches:
            raise errors.CumulusLibraryError(
                f"Model {self.provider.model_name} does not support batching."
            )
        if not self._config.phi_dir:
            raise errors.CumulusLibraryError(
                "NLP requires the --etl-phi-dir argument. Please provide a PHI dir and try again."
            )

    def add_note(self, task: workflow.NlpTask, note_res: dict, text: str) -> int:
        """Returns number of successfully processed notes. Might raise an exception."""
        if self._config.use_batching:
            # TODO MIKE: finish batching support
            return 0  # pragma: no cover

        prompt = self._make_prompt(task, text)
        response = self._model.prompt(prompt)
        self._add_response(task, note_res, text, response)
        return 1

    def finalize(self, task: workflow.NlpTask) -> int:
        """Returns number of successfully processed notes. Might raise an exception."""
        if self._config.use_batching:
            # TODO MIKE: finish batching support
            return 0  # pragma: no cover

        self._write_notes_to_output()
        return 0

    def _make_prompt(self, task: workflow.NlpTask, text: str) -> models.Prompt:
        schema = task.response_schema.model_json_schema()
        system = task.system_prompt or ""
        system = system.replace("%JSON-SCHEMA%", json.dumps(schema))

        user = task.user_prompt or "%CLINICAL-NOTE%"
        user = user.replace("%CLINICAL-NOTE%", text)

        return models.Prompt(
            system=system,
            user=user,
            schema=task.response_schema,
            cache_dir=cfs.FsPath(self._config.phi_dir),
            cache_namespace=f"{self._table_name(task)}_v{task.version}",
            cache_checksum=caching.cache_checksum(text),
        )

    def _table_name(self, task: workflow.NlpTask) -> str:
        return table_name_for_task(task, self._config)

    def _add_response(
        self, task: workflow.NlpTask, note_res: dict, text: str, response: models.PromptResponse
    ) -> None:
        # Track some basic note metadata (ref, subject, encounter)
        note_ref = f"{note_res.get('resourceType')}/{note_res.get('id')}"
        subject_ref = note_res.get("subject", {}).get("reference")

        encounters = note_res.get("context", {}).get("encounter", [])
        if encounters:
            encounter_ref = encounters[0].get("reference")
        else:  # check for dxreport encounter field
            encounter_ref = note_res.get("encounter", {}).get("reference")

        # Convert pydantic model to JSON and fix up spans to be ints instead of strings.
        # Pass serialize_as_any=True so we don't get warnings about finding strings when it
        # expected an Enum (all our enums are strings and default values are often strings)
        parsed = response.answer.model_dump(mode="json", serialize_as_any=True)
        self._fix_spans(note_ref, text, parsed)

        # If you change these, change the schema definition in nlp_builder.py too
        new_row = {
            "note_ref": note_ref,
            "encounter_ref": encounter_ref,
            "subject_ref": subject_ref,
            # Since this date is stored as a string, use UTC time for easy comparisons
            "generated_on": datetime.datetime.now(datetime.UTC).isoformat(),
            "task_version": task.version,
            "model": self._model.MODEL_ID,
            "system_fingerprint": response.fingerprint,
            "result": parsed,
        }

        # Add new row to pending notes
        table = self._table_name(task)
        self._notes.setdefault(table, []).append(new_row)

        # Do we have enough to write out?
        pending_notes = sum(len(x) for x in self._notes.values())
        if pending_notes >= self._config.note_limit:
            self._write_notes_to_output()

    def _fix_spans(self, note_ref: str, text: str, parsed: dict) -> bool:
        """Converts string spans into integer spans."""
        all_found = True

        for key, value in parsed.items():
            if key != "spans":
                # descend as needed
                if isinstance(value, dict):
                    all_found &= self._fix_spans(note_ref, text, value)
                if isinstance(value, list) and value and isinstance(value[0], dict):
                    all_found &= all([self._fix_spans(note_ref, text, v) for v in value])
                continue

            old_spans = value or []
            new_spans = []
            for span in old_spans:
                # Now we need to find this span in the original text.
                # However, LLMs like to mess with us, and the span is not always accurate to the
                # original text (e.g. whitespace, case, punctuation differences).
                # So be a little fuzzy.
                orig_span = span
                span = span.strip(string.punctuation + string.whitespace)
                span = re.escape(span)
                # Replace sequences of whitespace with a whitespace regex, to allow the span
                # returned by the LLM to match regardless of what the LLM does with whitespace and
                # to ignore how we trim trailing whitespace from the original note.
                span = ESCAPED_WHITESPACE.sub(r"\\s+", span)

                found = False
                for match in re.finditer(span, text, re.IGNORECASE):
                    found = True
                    new_spans.append(match.span())
                if not found:
                    all_found = False
                    rich.print(
                        f"Could not match span received from NLP server for {note_ref}: {orig_span}"
                    )

            parsed[key] = new_spans

        return all_found

    def _write_notes_to_output(self) -> None:
        print("TODO MIKE: write this to parquet", self._notes)  # noqa: T201
        self._notes = {}


def table_name_for_task(task: workflow.NlpTask, nlp_config: note_utils.NlpConfig) -> str:
    return f"{nlp_config.target}__nlp2_{task.name}"  # TODO MIKE: nlp prefix?
