import datetime
import enum
import json
import re
import string
import tempfile
import types
import typing

import cumulus_fhir_support as cfs
import pyarrow
import pydantic
import rich

from cumulus_library import base_utils, databases, errors, note_utils

from . import caching, models, workflow

ESCAPED_WHITESPACE = re.compile(r"(\\\s)+")
PARQUET_PATTERN = re.compile(r"nlp\.([0-9]+)\.parquet")


class NlpStats:
    def __init__(self, size: int):
        self.available = 0
        self.had_text = 0
        self.considered = [0] * size
        self.got_response = [0] * size
        self.token_stats = models.TokenStats()
        self.token_prices = None


def run_nlp(
    notes: note_utils.NoteSource,
    *,
    nlp_config: note_utils.NlpConfig,
    tables: dict[str, workflow.NlpTask],
    filters: list[cfs.NoteFilter],
    db: databases.DatabaseBackend,
) -> NlpStats:
    """Iterates through the notes, filtering as it goes, and passes notes to NLP"""
    stats = NlpStats(len(tables))

    # If asked to clean, do it
    if nlp_config.clean:
        for table_slug, task in tables.items():
            # We wanna clean out all previous uploads for this table.
            # Do this before making the pool because that looks at the files in the folder to
            # get the first batch number to use.
            root = output_path_for_task(nlp_config.target, table_slug, task, db).parent
            table_with_version = re.compile(rf"/{table_slug}_v[0-9]+(\.ids)?$")
            for folder in root.ls():
                if table_with_version.search(str(folder)):
                    folder.rm()

    # Read ID refs for any already-uploaded notes
    prev_upload_refs = [
        read_upload_refs_for_task(nlp_config.target, table_slug, task, db)
        for table_slug, task in tables.items()
    ]

    # Loop through every note and add to the note pool for NLP processing
    pool = NlpNotePool(nlp_config, db=db, tables=tables)
    pool.prepare(notes)
    for note_res in notes.progress_iter("Running NLP..."):
        stats.available += 1

        note_ref = f"{note_res['resourceType']}/{note_res['id']}"

        try:
            text = cfs.get_text_from_note_res(note_res)
        except Exception:  # noqa: S112
            continue
        stats.had_text += 1

        for idx, table_slug in enumerate(tables):
            if note_ref in prev_upload_refs[idx]:
                continue
            if not filters[idx](note_res, text=text):
                continue
            stats.considered[idx] += 1

            try:
                stats.got_response[idx] += pool.add_note(table_slug, note_res, text)
            except Exception as exc:
                rich.print("Failed to process note:", exc)

    # Finalize each task (in case a batch is waiting to be sent)
    try:
        pool.finalize()
    except Exception as exc:
        rich.print("Failed to process note:", exc)

    stats.token_stats = pool.token_stats
    stats.token_prices = pool.token_prices

    return stats


def table_name_for_task(table_slug: str, nlp_config: note_utils.NlpConfig) -> str:
    return f"{nlp_config.target}__{table_slug}"


def schema_for_task(task: workflow.NlpTask) -> pyarrow.Schema:
    result_schema = convert_pydantic_fields_to_pyarrow(task.response_schema.model_fields)
    return pyarrow.schema(
        [
            pyarrow.field("note_ref", pyarrow.string()),
            pyarrow.field("encounter_ref", pyarrow.string()),
            pyarrow.field("subject_ref", pyarrow.string()),
            pyarrow.field("generated_on", pyarrow.string()),
            pyarrow.field("task_version", pyarrow.int32()),
            pyarrow.field("model", pyarrow.string()),
            pyarrow.field("system_fingerprint", pyarrow.string()),
            pyarrow.field("result", result_schema),
        ]
    )


def output_path_for_task(
    prefix: str, table_slug: str, task: workflow.NlpTask, db: databases.DatabaseBackend
) -> cfs.FsPath:
    upload_slug = table_slug + f"_v{task.version}"
    if base_path := db.get_remote_upload_path(prefix, upload_slug):
        return cfs.FsPath(base_path)

    # OK, database backend doesn't support remote files, let's write it locally to our cache
    # Note: this is the same location for all duckdb files... file_upload has same issue.
    cache_root = base_utils.get_user_cache_dir()
    return cfs.FsPath(cache_root, "nlp", prefix, upload_slug)


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

    def __init__(
        self,
        nlp_config: note_utils.NlpConfig,
        *,
        db: databases.DatabaseBackend,
        tables: dict[str, workflow.NlpTask],
    ):
        self._config = nlp_config
        self._model = models.create_model(nlp_config)
        self._provider = self._model.provider
        self._db = db
        self._tables = tables

        self._notes = {}  # table_slug -> list[output row]

        if self._config.use_batching and not self._provider.supports_batches:
            raise errors.CumulusLibraryError(
                f"Model {self._provider.model_name} does not support batching."
            )
        if not self._config.phi_dir:
            raise errors.CumulusLibraryError(
                "NLP requires the --etl-phi-dir argument. Please provide a PHI dir and try again."
            )

    @property
    def token_stats(self) -> models.TokenStats:
        return self._model.stats

    @property
    def token_prices(self) -> models.TokenStats:
        return self._model.prices

    def prepare(self, notes: note_utils.NoteSource) -> None:
        # In batching mode, we need to do some preparations.
        # Namely, we (a) have to resume any previous batches.
        # And (b) we need to send our own (new) batches off.
        # Then when we do the normal loop of adding notes later, we'll get the cached results of
        # both (a) and (b) above.
        # We want to get it from the cache, because when we send batches off, we don't include
        # enough metadata to generate a proper PromptResult (like the references and span
        # conversion, etc).
        # So we need to crawl through the input notes and match it up with the cached NLP results.

        if self._config.use_batching:
            self._resume_existing_batches()
            self._create_new_batches(notes)

    def add_note(self, table_slug: str, note_res: dict, text: str) -> int:
        """Returns number of successfully processed notes. Might raise an exception."""
        task = self._tables[table_slug]

        prompt = self._make_prompt(table_slug, task, text)
        response = self._model.prompt(prompt)
        self._add_response(table_slug, task, note_res, text, response)
        return 1

    def finalize(self) -> None:
        """Returns number of successfully processed notes. Might raise an exception."""
        try:
            self._write_notes_to_output()
        finally:
            # If any of our tasks wrote no rows, let's write out a zero-row parquet so that we can
            # still create a table based off it for duckdb (which requires parquets).
            for table_slug, task in self._tables.items():
                if self._next_parquet_path(table_slug, task).name == "nlp.0.parquet":
                    self._write_single_parquet(table_slug, task, [])

    def _table_name(self, table_slug: str) -> str:
        return table_name_for_task(table_slug, self._config)

    def _cache_dir(self) -> cfs.FsPath:
        return cfs.FsPath(self._config.phi_dir)

    def _cache_namespace(self, table_slug: str, task: workflow.NlpTask) -> str:
        return f"{self._table_name(table_slug)}_v{task.version}_{self._model.MODEL_ID}"

    def _make_prompt(self, table_slug: str, task: workflow.NlpTask, text: str) -> models.Prompt:
        schema = task.response_schema.model_json_schema()
        system = task.system_prompt or ""
        system = system.replace("%JSON-SCHEMA%", json.dumps(schema))

        user = task.user_prompt or "%CLINICAL-NOTE%"
        user = user.replace("%CLINICAL-NOTE%", text)

        return models.Prompt(
            system=system,
            user=user,
            schema=task.response_schema,
            cache_dir=self._cache_dir(),
            cache_namespace=self._cache_namespace(table_slug, task),
            cache_checksum=caching.cache_checksum(text),
        )

    def _add_response(
        self,
        table_slug: str,
        task: workflow.NlpTask,
        note_res: dict,
        text: str,
        response: models.PromptResponse,
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

        # If you change these, change the schema definition in schema_for_task() as well as the
        # nlp.md documentation.
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
        self._notes.setdefault(table_slug, []).append(new_row)

        # Do we have enough to write out?
        pending_notes = sum(len(x) for x in self._notes.values())
        if pending_notes >= self._config.note_limit:
            self._write_notes_to_output()

    def _resume_existing_batches(self) -> None:
        # Maybe we got interrupted and need to resume.
        # Check all tables for any existing batch file.
        for table_slug, task in self._tables.items():
            cache_namespace = self._cache_namespace(table_slug, task)
            metadata = caching.cache_metadata_read(self._cache_dir(), cache_namespace)
            batch_ids = metadata.get(f"batches-{self._config.provider}")
            if batch_ids:
                rich.print(f" Resuming previously created batches for '{table_slug}'.")
                self._wait_for_batches(
                    batch_ids,
                    schema=task.response_schema,
                    cache_namespace=cache_namespace,
                )

    def _create_new_batches(self, notes: note_utils.NoteSource) -> None:
        table_batches = {}  # table_slug -> set[batch_id]
        cache_dir = self._cache_dir()

        # Feed every note into the provider
        with tempfile.TemporaryDirectory() as tmp_dir:
            for note_res in notes.progress_iter("Creating batches..."):
                try:
                    text = cfs.get_text_from_note_res(note_res)
                except Exception:  # noqa: S112
                    continue

                for table_slug, task in self._tables.items():
                    prompt = self._make_prompt(table_slug, task, text)
                    batch_ids = table_batches.setdefault(table_slug, set())
                    batch_ids.add(self._provider.add_to_batch(prompt, tmp_dir))

            for table_slug, task in self._tables.items():
                cache_namespace = self._cache_namespace(table_slug, task)
                batch_ids.add(self._provider.finish_batch(cache_dir, cache_namespace))
                batch_ids.discard(None)  # drop any None's from calls that didn't create a batch

        # Now wait for all the results
        for table_slug, task in self._tables.items():
            if batch_ids := table_batches.get(table_slug):
                rich.print(
                    f" Waiting for batches for '{table_slug}' to finish "
                    "(can be resumed if interrupted)."
                )
                self._wait_for_batches(
                    batch_ids,
                    cache_namespace=self._cache_namespace(table_slug, task),
                    schema=task.response_schema,
                )

    def _wait_for_batches(
        self,
        batch_ids: set[str],
        *,
        schema: type[pydantic.BaseModel],
        cache_namespace: str,
    ) -> None:
        status_box = rich.text.Text()
        count = len(batch_ids)
        cache_dir = self._cache_dir()

        with rich.get_console().status(status_box):
            for batch_id in batch_ids:
                plural = "" if count == 1 else "es"
                status_box.plain = (
                    f"Waiting for {count} batch{plural} to finish… (may take up to a day)"
                )

                self._provider.wait_for_batch(
                    batch_id,
                    schema=schema,
                    cache_dir=cache_dir,
                    cache_namespace=cache_namespace,
                )

                count -= 1

        count = len(batch_ids)
        batch_plural = "" if count == 1 else "es"
        rich.print(f" Waited for {count} batch{batch_plural}.")

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
        notes = self._notes
        self._notes = {}

        for table_slug, task in self._tables.items():
            if table_slug in notes:
                rows = notes[table_slug]
                self._write_single_parquet(table_slug, task, rows)
                add_upload_refs_for_task(self._config.target, table_slug, task, self._db, rows)

    def _next_parquet_path(self, table_slug: str, task: workflow.NlpShared) -> cfs.FsPath:
        folder = output_path_for_task(self._config.target, table_slug, task, self._db)

        # First, determine what the next upload number should be
        basenames = [path.name for path in folder.ls()]
        matches = [PARQUET_PATTERN.match(basename) for basename in basenames]
        numbers = [int(match.group(1)) for match in matches if match]
        next_index = max(numbers, default=-1) + 1

        return folder.joinpath(f"nlp.{next_index}.parquet")

    def _write_single_parquet(
        self, table_slug: str, task: workflow.NlpTask, rows: list[dict]
    ) -> int:
        path = self._next_parquet_path(table_slug, task)
        path.parent.makedirs()

        # Build the pyarrow table (with schema) and write it out
        table = pyarrow.Table.from_pylist(rows, schema=schema_for_task(task))
        pyarrow.parquet.write_table(table, str(path), compression="snappy", filesystem=path.fs)


def upload_refs_path(
    prefix: str, table_slug: str, task: workflow.NlpTask, db: databases.DatabaseBackend
) -> cfs.FsPath:
    path = output_path_for_task(prefix, table_slug, task, db)
    return cfs.FsPath(str(path) + ".ids")


def read_upload_refs_for_task(
    prefix: str, table_slug: str, task: workflow.NlpTask, db: databases.DatabaseBackend
) -> set[str]:
    path = upload_refs_path(prefix, table_slug, task, db)
    refs = path.read_text(default="")
    return set(refs.splitlines())


def add_upload_refs_for_task(
    prefix: str,
    table_slug: str,
    task: workflow.NlpTask,
    db: databases.DatabaseBackend,
    rows: list[dict],
) -> set[str]:
    refs = sorted(f"{row['note_ref']}" for row in rows)  # sort for tests
    path = upload_refs_path(prefix, table_slug, task, db)
    path.parent.makedirs()
    mode = "a" if path.exists() else "w"  # work around memory:// fs having bad "a" semantics
    with path.open(mode) as f:
        for ref in refs:
            f.write(ref)
            f.write("\n")


def convert_pydantic_fields_to_pyarrow(
    fields: dict[str, pydantic.fields.FieldInfo],
) -> pyarrow.StructType:
    return pyarrow.struct(
        [
            pyarrow.field(name, pyarrow.list_(pyarrow.list_(pyarrow.int32(), 2)), nullable=True)
            if name == "spans"
            else pyarrow.field(name, _convert_type_to_pyarrow(info.annotation), nullable=True)
            for name, info in fields.items()
        ]
    )


def _convert_type_to_pyarrow(annotation) -> pyarrow.DataType:
    # Since we only need to handle a small amount of possible types, we just do this ourselves
    # rather than relying on an external library.
    if origin := typing.get_origin(annotation):  # e.g. "Annotated", "UnionType", "list"
        sub_type = typing.get_args(annotation)[0]
        if origin is typing.Union or origin is types.UnionType:
            # This is gonna be something like "str | None" so just grab first arg.
            # We mark all our fields are nullable at the pyarrow layer.
            return _convert_type_to_pyarrow(sub_type)
        elif origin is typing.Annotated:
            annotation = sub_type
        elif issubclass(origin, list):
            return pyarrow.list_(_convert_type_to_pyarrow(sub_type))
        else:
            raise ValueError(f"Unsupported type {annotation}")  # pragma: no cover

    if issubclass(annotation, str):
        return pyarrow.string()
    elif issubclass(annotation, bool):
        return pyarrow.bool_()
    elif issubclass(annotation, int):
        return pyarrow.int32()
    elif issubclass(annotation, float):
        return pyarrow.float32()
    elif issubclass(annotation, enum.Enum):
        return pyarrow.string()  # for now, assume all enums are strings
    elif issubclass(annotation, pydantic.BaseModel):
        return convert_pydantic_fields_to_pyarrow(annotation.model_fields)

    raise ValueError(f"Unsupported type {annotation}")  # pragma: no cover
