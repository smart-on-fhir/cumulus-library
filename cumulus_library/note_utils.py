import argparse
import binascii
import io
import os
import pathlib
from collections.abc import Generator

import cumulus_fhir_support as cfs
import rich

from cumulus_library import base_utils, databases, errors

#########################
# Reading notes from disk
#########################


class NoteSource:
    def __init__(
        self,
        note_dirs: list[str | pathlib.Path] | None = None,
    ):
        self._files: dict[cfs.FsPath, int] | None = None
        self._total_size = 0

        self._note_dirs = note_dirs

    def __bool__(self) -> bool:
        return bool(self._note_dirs)

    def progress_iter(self, label: str) -> Generator[dict]:
        with base_utils.get_progress_bar() as progress:
            task = progress.add_task(label, total=None)
            self._scan()
            progress.update(task, total=self._total_size)
            for file, size in self._files.items():
                offset = 0
                for row in cfs.read_multiline_json_with_details(file):
                    progress.advance(task, row["byte_offset"] - offset)
                    offset = row["byte_offset"]
                    yield row["json"]
                progress.advance(task, size - offset)

    def _scan(self) -> None:
        """Lazily ensure that we've scanned files"""
        if self._files is not None:
            return  # already scanned

        rich.print("Scanning note dir...")
        self._files = self._flatten_note_dirs()
        self._total_size = sum(self._files.values())

    def _flatten_note_dirs(self) -> dict[cfs.FsPath, int]:
        """Converts a list of folders into a list of found filenames.

        Flattening like this is useful for performance reasons, to only scan the dirs once.
        For similar performance reason, when using the list of files, please have care with the
        number of times we actually read through them.
        """
        note_types = {"DiagnosticReport", "DocumentReference"}

        infos = {}
        for one_dir in self._note_dirs or []:
            fspath = cfs.FsPath(one_dir)

            # Actually scan for matching files and grab their size
            paths = list(cfs.list_multiline_json_in_dir(fspath, note_types, recursive=True))
            for idx, path in enumerate(paths):
                path = cfs.FsPath(path)
                with path.open() as f:
                    # There doesn't seem to be a reliable way to get the uncompressed size of
                    # gzip'd files, so we'll open and seek to the end. We want the uncompressed
                    # size, because when iterating through it, cfs will give us the byte_offset
                    # of the uncompressed stream. This can take time for big files, but accurate
                    # progress bars are super helpful feedback.
                    size = f.seek(0, io.SEEK_END)
                infos[path] = size

        return infos


#########################
# Reading database tables
#########################


# Get table refs (will make immediate query)
def get_table_refs(cursor, table: str) -> cfs.RefSet:
    if set(table) - databases.SQL_NAME_CHARS:
        raise ValueError(f"Invalid SQL table name '{table}'")
    rows = cursor.execute(f'SELECT * FROM "{table}"')  # noqa: S608

    cols = [field[0] for field in rows.description]
    scanner = cfs.make_note_ref_scanner(cols, is_anon=True)

    refs = cfs.RefSet()
    for row in rows.fetchall():
        if ref := scanner(row):
            refs.add_ref(ref)

    return refs


###################
# NLP Configuration
###################


def _parse_mlflow_tags(values: list[str] | None) -> dict[str, str]:
    """Parses repeated --mlflow-tag KEY=VALUE arguments into a dict.

    Deliberately explicit rather than inferring tags from the run name: a run name like
    "model_gpt-oss-120b" has no unambiguous split point, and guessing at one silently drops
    part of the value.
    """
    tags = {}
    for value in values or []:
        key, sep, val = value.partition("=")
        if not sep or not key.strip():
            raise errors.CumulusLibraryError(
                f"Invalid --mlflow-tag '{value}'. Expected the form KEY=VALUE."
            )
        tags[key.strip()] = val
    return tags


class NlpConfig:
    def __init__(self, args: argparse.Namespace | None = None):
        """
        Creates an NlpConfig instance from CLI args.

        Only pass None as an argument in test contexts, where you don't care about full NLP
        working as expected.
        """
        args = args or {}
        self.model = args.get("nlp_model")
        self.provider = args.get("nlp_provider", "local")
        # One or more Azure deployments to spread requests across (--azure-deployment, which
        # may be repeated). Empty means "use the model name", which is what Azure defaults to.
        self.azure_deployments = args.get("azure_deployments") or []
        self.use_batching = args.get("batch_nlp", False)
        self.chunksize = args.get("chunksize") or 100000
        self.clean = args.get("clean_nlp", False)
        self.phi_dir = args.get("etl_phi_dir")
        self.target = args.get("target")
        self.show_stats = args.get("nlp_stats")

        # <DevModeFlags>
        # Dev mode (--dev) unlocks study-development flags that aren't useful when simply
        # building a study. See cli_parser.add_dev_argument.
        self.dev = args.get("dev", False)
        # An optional subset of workflow tasks to build (--nlp-subtask). None means "all tasks".
        self.subtasks = args.get("nlp_subtasks")
        # Overrides the default table selection logic defined by workflow files
        self.select_by_table = args.get("select_by_table")
        # MLflow experiment tracking. All of these are dev-mode only, so on a normal run they
        # are missing from args and everything below falls back to "off".
        self.mlflow = args.get("mlflow", False)
        # The flag wins over the environment variable, which matches how MLflow's own tooling
        # behaves and keeps the value testable without touching os.environ.
        self.mlflow_uri = args.get("mlflow_uri") or os.environ.get("MLFLOW_TRACKING_URI")
        self.mlflow_experiment = args.get("mlflow_experiment")
        self.mlflow_run_name = args.get("mlflow_run_name")
        self.mlflow_log_traces = args.get("mlflow_log_traces", False)
        self.mlflow_tags = _parse_mlflow_tags(args.get("mlflow_tags"))
        # </DevModeFlags>

        # How many requests to keep in flight at once. Defaults to one worker per deployment,
        # so passing several deployments parallelizes without any extra flags, while a single
        # endpoint (local vLLM, Bedrock) stays serial until you explicitly ask for more.
        concurrency = args.get("nlp_concurrency")
        if concurrency is None:
            concurrency = len(self.azure_deployments) or 1
        self.concurrency = max(1, concurrency)

        self.salt = None
        if self.phi_dir:
            codebook_path = cfs.FsPath(self.phi_dir, "codebook.json")
            codebook = codebook_path.read_json(default={})
            if id_salt := codebook.get("id_salt"):
                self.salt = binascii.unhexlify(id_salt)
