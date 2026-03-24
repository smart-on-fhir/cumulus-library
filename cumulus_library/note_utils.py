import base64
import binascii
import dataclasses
import email
import hmac
import json
import os
import pathlib
import re
import urllib.parse
from collections.abc import Callable, Container, Generator, Iterable, Sequence

import cumulus_fhir_support as cfs
import fsspec
import inscriptis

from cumulus_library import base_utils, databases

#########################
# Reading notes from disk
#########################


class RemoteAttachment(ValueError):
    """A note was requested, but it was only available remotely"""


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


################
# Anonymized IDs
################


def anon_id(id_val: str | None, salt: bytes) -> str | None:
    if not id_val:
        return None
    return hmac.new(salt, digestmod="sha256", msg=id_val.encode()).hexdigest()


def anon_ref(ref_val: str | None, salt: bytes) -> str | None:
    if not ref_val:
        return None
    try:
        res_type, id_val = ref_val.split("/", 1)
    except ValueError:
        return None
    return f"{res_type}/{anon_id(id_val, salt)}"


######################
# Note text extraction
######################


def get_text_from_note_res(note_res: dict) -> str:
    """
    Returns the clinical text contained in the given note resource.

    It will try to find the simplest version (plain text) or convert html to plain text if needed.

    Will raise an exception if text cannot be found.
    """
    attachment = _get_clinical_note_attachment(note_res)
    text = _get_note_from_attachment(attachment)

    mimetype, _ = _parse_content_type(attachment["contentType"])
    if mimetype in {"text/html", "application/xhtml+xml"}:
        # An HTML note can confuse/stall NLP.
        # It may include mountains of spans/styling or inline base64 images that aren't relevant
        # to our interests.
        #
        # Inscriptis makes a very readable version of the note, with a focus on maintaining the
        # HTML layout.
        text = inscriptis.get_text(text)

    return text.strip()


def _parse_content_type(content_type: str) -> (str, str):
    """Returns (mimetype, encoding)"""
    msg = email.message.EmailMessage()
    msg["content-type"] = content_type
    return msg.get_content_type(), msg.get_content_charset("utf8")


def _mimetype_priority(mimetype: str) -> int:
    """
    Returns priority of mimetypes for docref notes.

    0 means "ignore"
    Higher numbers are higher priority
    """
    if mimetype == "text/plain":
        return 3
    elif mimetype == "text/html":
        return 2
    elif mimetype == "application/xhtml+xml":
        return 1
    return 0


def _get_note_from_attachment(attachment: dict) -> str:
    """
    Decodes or downloads a note from an attachment.

    Note that it is assumed a contentType is provided.

    :returns: the attachment's note text
    """
    _mimetype, charset = _parse_content_type(attachment["contentType"])

    if attachment.get("data") is not None:
        return base64.standard_b64decode(attachment["data"]).decode(charset)

    if attachment.get("url") is not None:
        raise RemoteAttachment(
            "Some clinical note texts are only available via URL. "
            "You may want to inline your notes with SMART Fetch."
        )

    # Shouldn't ever get here, because get_clinical_note_attachment already checks this,
    # but just in case...
    raise ValueError("No data or url field present")  # pragma: no cover


def _get_clinical_note_attachment(resource: dict) -> dict:
    match resource["resourceType"]:
        case "DiagnosticReport":
            attachments = resource.get("presentedForm", [])
        case "DocumentReference":
            attachments = [
                content["attachment"]
                for content in resource.get("content", [])
                if "attachment" in content
            ]
        case _:
            raise ValueError(f"{resource['resourceType']} is not a supported clinical note type.")

    # Find the best attachment to use, based on mimetype.
    # We prefer basic text documents, to avoid confusing NLP with extra formatting (like <body>).
    best_attachment_index = -1
    best_attachment_priority = 0
    for index, attachment in enumerate(attachments):
        if "contentType" in attachment:
            mimetype, _ = _parse_content_type(attachment["contentType"])
            priority = _mimetype_priority(mimetype)
            if priority > best_attachment_priority:
                best_attachment_priority = priority
                best_attachment_index = index

    if best_attachment_index < 0:
        # We didn't find _any_ of our target text content types.
        raise ValueError("No textual mimetype found")

    attachment = attachments[best_attachment_index]

    if attachment.get("data") is None and not attachment.get("url"):
        raise ValueError("No data or url field present")

    return attachments[best_attachment_index]


#####################
# Table ref discovery
#####################


@dataclasses.dataclass
class TableRefs:
    notes: set[str] | None = None
    patients: set[str] | None = None


# Get table refs (will make immediate query, if a table is defined)
def get_table_refs(cursor, table: str | None) -> TableRefs:
    if not table:
        return TableRefs()

    if set(table) - databases.SQL_NAME_CHARS:
        raise ValueError(f"Invalid SQL table name '{table}'")
    rows = cursor.execute(f"SELECT * FROM {table}")  # noqa: S608

    columns = [field[0].casefold() for field in rows.description]
    ref_getter = _make_ref_getter(table, columns, is_anon=True)

    refs = TableRefs()
    for row in rows.fetchall():
        note_ref, pat_ref = ref_getter(row)
        if note_ref:
            if refs.notes is None:
                refs.notes = set()
            refs.notes.add(note_ref)
        if pat_ref:
            if refs.patients is None:
                refs.patients = set()
            refs.patients.add(pat_ref)

    return refs


def _make_ref_getter(
    table: str, fieldnames: Sequence[str], *, is_anon: bool = False
) -> Callable[[Sequence[str]], tuple[str | None, str | None]]:
    """Returns a callable that returns (note ref, patient ref)"""
    get_dxr = _find_header(fieldnames, "DiagnosticReport", is_anon=is_anon)
    get_doc = _find_header(fieldnames, "DocumentReference", is_anon=is_anon)
    get_pat = _find_header(fieldnames, "Patient", is_anon=is_anon)

    if not get_dxr and not get_doc and not get_pat:
        raise ValueError(f"No patient or note IDs found in table '{table}'.")

    # A method that takes a row of a table and returns a patient/note ref from it
    def getter(row: Sequence[str]) -> str | None:
        if get_dxr:
            if val := get_dxr(row):
                return val, None
        if get_doc:
            if val := get_doc(row):
                return val, None
        # If and only if we don't have any resource ID matchers, we'll check by patient
        if not get_dxr and not get_doc and get_pat:
            if val := get_pat(row):
                return None, val
        return None, None

    return getter


def _find_header(
    fieldnames: Sequence[str], res_type: str, *, is_anon: bool = False
) -> Callable[[Sequence[str]], str | None] | None:
    folded = res_type.casefold()
    id_names = [f"{folded}_id"]
    ref_names = [f"{folded}_ref"]

    if res_type in {"DiagnosticReport", "DocumentReference"}:
        ref_names.append("document_ref")
        ref_names.append("note_ref")
    if res_type == "DocumentReference":
        id_names.append("docref_id")
    if res_type == "Patient":
        id_names.append("subject_id")
        ref_names.append("subject_ref")

    if is_anon:
        # Look for both anon_ and normal versions, but prefer an explicit column in case both exist
        id_names = [f"anon_{x}" for x in id_names] + id_names
        ref_names = [f"anon_{x}" for x in ref_names] + ref_names

    for field in id_names:
        if field in fieldnames:
            idx = fieldnames.index(field)
            return lambda x: f"{res_type}/{x[idx]}"
    for field in ref_names:
        if field in fieldnames:
            idx = fieldnames.index(field)
            prefix = f"{res_type}/"
            return lambda x: x[idx] if x[idx].startswith(prefix) else None

    return None


################
# Note filtering
################


_ESCAPED_WHITESPACE = re.compile(r"(\\\s)+")


def make_note_filter(
    *,
    reject_by_regex: Iterable[str] | None = None,
    reject_by_word: Iterable[str] | None = None,
    select_by_note_ref: Container[str] | None = None,
    select_by_patient_ref: Container[str] | None = None,
    select_by_regex: Iterable[str] | None = None,
    select_by_word: Iterable[str] | None = None,
) -> Callable[[dict, str], bool]:
    pattern = _compile_filter_regex(
        reject_by_regex=reject_by_regex,
        reject_by_word=reject_by_word,
        select_by_regex=select_by_regex,
        select_by_word=select_by_word,
    )

    def note_filter(note_res: dict, *, text: str, salt: bytes | None = None) -> bool:
        if not _filter_status(note_res):
            return False

        if salt and not _filter_refs(
            note_res,
            salt=salt,
            select_by_patient_ref=select_by_patient_ref,
            select_by_note_ref=select_by_note_ref,
        ):
            return False

        if pattern.search(text) is None:
            return False

        return True

    return note_filter


def _filter_status(note_res: dict) -> bool:
    """If the resource status is WIP, obsolete, or entered-in-error, reject it"""
    note_type = note_res.get("resourceType")
    note_id = note_res.get("id")

    # Require basic note fields, so that other filters can use these without guards
    if not note_type or not note_id:
        return False

    match note_type:
        case "DiagnosticReport":
            valid_status_types = {"final", "amended", "corrected", "appended", "unknown", None}
            return note_res.get("status") in valid_status_types

        case "DocumentReference":
            good_status = note_res.get("status") in {"current", None}  # status of DocRef itself
            # docStatus is status of clinical note attachments
            good_doc_status = note_res.get("docStatus") in {"final", "amended", None}
            return good_status and good_doc_status

        case _:  # pragma: no cover
            return False  # pragma: no cover


def _filter_refs(
    note_res: dict,
    *,
    salt: bytes,
    select_by_patient_ref: Container[str] | None,
    select_by_note_ref: Container[str] | None,
) -> bool:
    """Returns False if refs are provided and this note doesn't match any of them"""
    if select_by_patient_ref is not None:
        # Both DxReports and DocRefs use subject
        subject_ref = anon_ref(note_res.get("subject", {}).get("reference"), salt)
        if subject_ref not in select_by_patient_ref:
            return False

    if select_by_note_ref is not None:
        note_ref = f"{note_res['resourceType']}/{anon_id(note_res['id'], salt)}"
        if note_ref not in select_by_note_ref:
            return False

    return True


def _user_regex_to_pattern(term: str) -> str:
    """Takes a user search regex and adds some boundaries to it"""
    # Make a custom version of \b that allows non-word characters to be on edge of the term too.
    # For example:
    #   This misses: re.match(r"\ba\+\b", "a+")
    #   But this hits: re.match(r"\ba\+", "a+")
    # So to work around that, we look for the word boundary ourselves.
    edge = r"(\W|$|^)"
    return f"{edge}({term}){edge}"


def _user_word_to_pattern(term: str) -> str:
    """Takes a user search term and turns it into a clinical-note-appropriate regex"""
    term = re.escape(term)
    # Allow multi-word "words" (like "severe cough") have any kind of whitespace in between them,
    # as they may cross line endings in the note (which can happen for normal paragraph wrapping).
    term = _ESCAPED_WHITESPACE.sub(r"\\s+", term)
    return _user_regex_to_pattern(term)


def _combine_regexes(*, by_regex: Iterable[str] | None, by_word: Iterable[str] | None) -> str:
    patterns = []
    if by_regex:
        patterns.extend(_user_regex_to_pattern(regex) for regex in set(by_regex))
    if by_word:
        patterns.extend(_user_word_to_pattern(word) for word in set(by_word))
    return "|".join(patterns)


def _compile_filter_regex(
    *,
    reject_by_regex: Iterable[str] | None,
    reject_by_word: Iterable[str] | None,
    select_by_regex: Iterable[str] | None,
    select_by_word: Iterable[str] | None,
) -> re.Pattern:
    select = _combine_regexes(by_word=select_by_word, by_regex=select_by_regex)

    reject = _combine_regexes(by_word=reject_by_word, by_regex=reject_by_regex)
    if reject:
        # Use negative lookahead
        reject = rf"^(?!.*{reject})"

    if reject and select:
        # Add positive lookahead
        final = f"{reject}(?=.*{select})"
    elif reject:
        final = reject
    elif select:
        final = select
    else:
        final = ""  # an empty pattern will match anything

    return re.compile(final, re.IGNORECASE)
