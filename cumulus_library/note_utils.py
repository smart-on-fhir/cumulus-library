import argparse
import binascii
import pathlib
from collections.abc import Generator

import cumulus_fhir_support as cfs

from cumulus_library import base_utils, databases

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
                    offset = row["byte_offset"]
                    progress.advance(task, offset)
                    yield row["json"]
                progress.advance(task, size - offset)

    def _scan(self) -> None:
        """Lazily ensure that we've scanned files"""
        if self._files is not None:
            return  # already scanned

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

            # Actually scan for matching files
            paths = list(cfs.list_multiline_json_in_dir(fspath, note_types, recursive=True))
            sizes = fspath.fs.sizes(paths)
            for idx, path in enumerate(paths):
                infos[cfs.FsPath(path)] = sizes[idx]

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
        self.azure_deployment = args.get("azure_deployment")
        self.use_batching = args.get("batch_nlp", False)
        self.clean = args.get("clean_nlp", False)
        self.phi_dir = args.get("etl_phi_dir")
        self.target = args.get("target")

        if self.phi_dir:
            codebook_path = cfs.FsPath(self.phi_dir, "codebook.json")
            codebook = codebook_path.read_json(default={})
            if id_salt := codebook.get("id_salt"):
                self.salt = binascii.unhexlify(id_salt)
        else:
            self.salt = None

        # Hard code a global memory limit for now. This limit is the number of notes we're willing
        # to hold in memory at once. Once we hit this limit, we write out the current notes.
        # This can be configurable in the future, as needed.
        self.note_limit = 100_000
