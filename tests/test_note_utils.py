import binascii
import json
import os
from unittest import mock

import pytest

from cumulus_library import (
    note_utils,
)

# Some convenience salt values to use
SALT_STR = "e359191164cd209708d93551f481edd048946a9d844c51dea1b64d3f83dfd1fa"
SALT_BYTES = binascii.unhexlify(SALT_STR)


@mock.patch("rich.progress.Progress.advance")
def test_note_source_iter(mock_advance, tmp_path):
    dxr1 = json.dumps({"resourceType": "DiagnosticReport", "id": "dxr1"})
    dxr2 = json.dumps({"resourceType": "DiagnosticReport", "id": "dxr2"})
    docref1 = json.dumps({"resourceType": "DocumentReference", "id": "docref1"})
    docref2 = json.dumps({"resourceType": "DocumentReference", "id": "docref2"})

    os.makedirs(f"{tmp_path}/1")
    with open(f"{tmp_path}/1/dxr.ndjson", "w", encoding="utf8") as f:
        f.write(f"{dxr1}\n{dxr2}")
    # Write patient file, which should be ignored
    with open(f"{tmp_path}/1/pat.ndjson", "w", encoding="utf8") as f:
        json.dump({"resourceType": "Patient", "id": "pat1"}, f)

    os.makedirs(f"{tmp_path}/2/subdir")  # confirm we are checking recursively w/ a subdir
    with open(f"{tmp_path}/2/subdir/docref.ndjson", "w", encoding="utf8") as f:
        f.write(f"{docref1}\n{docref2}")

    source = note_utils.NoteSource([f"{tmp_path}/1", f"{tmp_path}/2"])

    ids = [x["id"] for x in source.progress_iter("testing")]
    assert ids == ["dxr1", "dxr2", "docref1", "docref2"]

    skips = [call[0][1] for call in mock_advance.call_args_list]
    assert skips == [0, len(dxr1) + 1, len(dxr2), 0, len(docref1) + 1, len(docref2)]

    # Confirm we can iterate twice
    ids = [x["id"] for x in source.progress_iter("testing")]
    assert ids == ["dxr1", "dxr2", "docref1", "docref2"]


def test_note_source_phi_dir(tmp_path):
    """Just confirm we parse the salt from a codebook file"""
    with open(f"{tmp_path}/codebook.json", "w") as f:
        json.dump({"id_salt": SALT_STR}, f)

    source = note_utils.NoteSource(phi_dir=tmp_path)
    assert source.salt == SALT_BYTES


def test_get_table_refs():
    cursor = mock.MagicMock()
    cursor.execute.return_value.description = [["documentreference_id"]]
    cursor.execute.return_value.fetchall.return_value = [("a",), ("b",)]

    refs = note_utils.get_table_refs(cursor, "my_table")
    assert list(refs) == ["DocumentReference/a", "DocumentReference/b"]


def test_get_table_refs_bad_table():
    with pytest.raises(ValueError, match="Invalid SQL table name"):
        note_utils.get_table_refs(None, "table; drop USERS")
