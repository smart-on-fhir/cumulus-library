import binascii
import dataclasses
import json
import os
import pathlib
import urllib.parse
from collections.abc import Generator

import cumulus_fhir_support as cfs
import fsspec

from cumulus_library import base_utils, databases

#########################
# Reading notes from disk
#########################


@dataclasses.dataclass(kw_only=True)
class _FileInfo:
    fs: fsspec.AbstractFileSystem
    path: str
    size: int


class NoteSource:
    def __init__(
        self,
        note_dirs: list[str | pathlib.Path] | None = None,
        *,
        phi_dir: str | pathlib.Path | None = None,
        s3_region: str | None = None,
    ):
        self._files: list[_FileInfo] | None = None
        self._total_size = 0

        self._note_dirs = note_dirs
        self._s3_region = s3_region

        self._salt = None
        if phi_dir:
            fs, phi_dir = self._get_fsspec_path(phi_dir)
            with fs.open(os.path.join(phi_dir, "codebook.json")) as f:
                codebook = json.load(f)
            if id_salt := codebook.get("id_salt"):
                self._salt = binascii.unhexlify(id_salt)

    def __bool__(self) -> bool:
        return bool(self._note_dirs)

    @property
    def salt(self) -> bytes | None:
        return self._salt

    def progress_iter(self, label: str) -> Generator[dict]:
        with base_utils.get_progress_bar() as progress:
            task = progress.add_task(label, total=None)
            self._scan()
            progress.update(task, total=self._total_size)
            for file in self._files:
                offset = 0
                for row in cfs.read_multiline_json_with_details(file.path, fsspec_fs=file.fs):
                    offset = row["byte_offset"]
                    progress.advance(task, offset)
                    yield row["json"]
                progress.advance(task, file.size - offset)

    def _scan(self) -> None:
        """Lazily ensure that we've scanned files"""
        if self._files is not None:
            return  # already scanned

        self._files = self._flatten_note_dirs()
        self._total_size = sum(f.size for f in self._files)

    def _get_fsspec_path(self, path: str | pathlib.Path) -> tuple[fsspec.AbstractFileSystem, str]:
        path = str(path)  # allow pathlib.Path objects but stringify them here
        parsed = urllib.parse.urlparse(path)
        protocol = parsed.scheme or "file"
        path = path if protocol == "file" else os.path.abspath(path)

        # Initialize a fsspec filesystem
        options = {}
        if protocol == "s3" and self._s3_region:
            options["client_kwargs"] = {"region_name": self._s3_region}
        fs = fsspec.filesystem(protocol, **options)

        return fs, path

    def _flatten_note_dirs(self) -> list[_FileInfo]:
        """Converts a list of folders into a list of found filenames.

        Flattening like this is useful for performance reasons, to only scan the dirs once.
        For similar performance reason, when using the list of files, please have care with the
        number of times we actually read through them.
        """
        note_types = {"DiagnosticReport", "DocumentReference"}

        infos = []
        for one_dir in self._note_dirs or []:
            fs, one_dir = self._get_fsspec_path(one_dir)

            # Actually scan for matching files
            paths = list(
                cfs.list_multiline_json_in_dir(one_dir, note_types, fsspec_fs=fs, recursive=True)
            )
            sizes = fs.sizes(paths)
            for idx, path in enumerate(paths):
                infos.append(_FileInfo(fs=fs, path=path, size=sizes[idx]))

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
