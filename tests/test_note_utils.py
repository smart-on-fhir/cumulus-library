import base64
import json
import os
from unittest import mock

import pytest

from cumulus_library import (
    note_utils,
)


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


@mock.patch("cumulus_fhir_support.list_multiline_json_in_dir", return_value=["a"])
@mock.patch("cumulus_fhir_support.read_multiline_json_with_details", return_value=[])
@mock.patch("fsspec.filesystem")
def test_note_source_s3(mock_filesystem, mock_read, mock_list):
    """Confirm that we correctly create and pass the fsspec object around - no actual I/O here"""
    fs = mock_filesystem.return_value
    fs.sizes.return_value = [10]

    source = note_utils.NoteSource(["s3://mockbucket/"], s3_region="cloud9")

    assert list(source.progress_iter("testing")) == []
    assert mock_filesystem.call_args == mock.call("s3", client_kwargs={"region_name": "cloud9"})
    assert mock_list.call_args[1]["fsspec_fs"] == fs
    assert mock_read.call_args[1]["fsspec_fs"] == fs


@pytest.mark.parametrize(
    "res_type,attachments,result",
    [
        (  # simple DxReport text
            "DiagnosticReport",
            [("text/plain", "hello", "url")],
            "hello",
        ),
        (  # simple DocRef text
            "DocumentReference",
            [("text/plain", "hello", "url")],
            "hello",
        ),
        (  # prefer text over any html variant
            "DocumentReference",
            [
                ("text/html", "html", None),
                ("text/plain", "text", None),
                ("application/xhtml+xml", "xhtml", None),
            ],
            "text",
        ),
        (  # prefer html over xhtml
            "DocumentReference",
            [("text/html", "html", None), ("application/xhtml+xml", "xhtml", None)],
            "html",
        ),
        (  # but accept xhtml
            "DocumentReference",
            [("application/xhtml+xml", "xhtml", None)],
            "xhtml",
        ),
        (  # strips html
            "DocumentReference",
            [("text/html", "<html><body>He<b>llooooo</b></html>", None)],
            "Hellooooo",
        ),
        (  # strips xhtml
            "DocumentReference",
            [("application/xhtml+xml", "<html><body>He<b>llooooo</b></html>", None)],
            "Hellooooo",
        ),
        (  # does not strips text
            "DocumentReference",
            [("text/plain", "<html><body>He<b>llooooo</b></html>", None)],
            "<html><body>He<b>llooooo</b></html>",
        ),
        (  # strips surrounding whitespace
            "DocumentReference",
            [("text/plain", "\n\n hello   world \n\n", None)],
            "hello   world",
        ),
        (  # respects charset
            "DiagnosticReport",
            [("text/plain; charset=utf16", b"\xff\xfeh\x00e\x00l\x00l\x00o\x00", None)],
            "hello",
        ),
        (  # bad charset
            "DiagnosticReport",
            [("text/plain", b"\xff\xfeh\x00e\x00l\x00l\x00o\x00", None)],
            pytest.raises(UnicodeDecodeError, match="invalid start byte"),
        ),
        (  # unsupported mime type
            "DiagnosticReport",
            [("application/pdf", "pdf", None)],
            pytest.raises(ValueError, match="No textual mimetype found"),
        ),
        (  # no attachments
            "DiagnosticReport",
            [],
            pytest.raises(ValueError, match="No textual mimetype found"),
        ),
        (  # url only
            "DiagnosticReport",
            [("text/plain", None, "url")],
            pytest.raises(note_utils.RemoteAttachment, match="only available via URL"),
        ),
        (  # bad resource type
            "Patient",
            [],
            pytest.raises(ValueError, match="Patient is not a supported clinical note type"),
        ),
        (  # no data or url
            "DocumentReference",
            [("text/plain", None, None)],
            pytest.raises(ValueError, match="No data or url field present"),
        ),
    ],
)
def test_get_text_from_note_res(res_type, attachments, result):
    note_res = {"resourceType": res_type}

    # Build attachment list
    attachments = [
        {
            "contentType": attachment[0],
            "data": attachment[1],
            "url": attachment[2],
        }
        for attachment in attachments
    ]
    for attachment in attachments:
        if data := attachment["data"]:
            if isinstance(data, str):
                data = data.encode()
            attachment["data"] = base64.standard_b64encode(data).decode()
    if res_type == "DiagnosticReport":
        note_res["presentedForm"] = attachments
    elif res_type == "DocumentReference":
        note_res["content"] = [{"attachment": a} for a in attachments]

    # Grab text and compare
    if isinstance(result, str):
        assert note_utils.get_text_from_note_res(note_res) == result
    else:
        with result:
            note_utils.get_text_from_note_res(note_res)


@pytest.mark.parametrize(
    "res,text,kwargs,selected",
    [
        (  # default, no regexes, no status - should pass
            {"resourceType": "DiagnosticReport"},
            "",
            {},
            True,
        ),
        (  # unsupported type
            {"resourceType": "Patient"},
            "",
            {},
            False,
        ),
        (  # bad status
            {"resourceType": "DiagnosticReport", "status": "registered"},
            "",
            {},
            False,
        ),
        (  # bad status
            {"resourceType": "DiagnosticReport", "status": "partial"},
            "",
            {},
            False,
        ),
        (  # bad status
            {"resourceType": "DiagnosticReport", "status": "preliminary"},
            "",
            {},
            False,
        ),
        (  # bad status
            {"resourceType": "DiagnosticReport", "status": "cancelled"},
            "",
            {},
            False,
        ),
        (  # bad status
            {"resourceType": "DiagnosticReport", "status": "entered-in-error"},
            "",
            {},
            False,
        ),
        (  # bad status
            {"resourceType": "DocumentReference", "status": "superseded"},
            "",
            {},
            False,
        ),
        (  # bad status
            {"resourceType": "DocumentReference", "status": "entered-in-error"},
            "",
            {},
            False,
        ),
        (  # bad docStatus
            {"resourceType": "DocumentReference", "docStatus": "preliminary"},
            "",
            {},
            False,
        ),
        (  # bad docStatus
            {"resourceType": "DocumentReference", "docStatus": "entered-in-error"},
            "",
            {},
            False,
        ),
        (  # select word (negative case)
            {"resourceType": "DiagnosticReport"},
            "hello world",
            {"select_by_word": ["bye"]},
            False,
        ),
        (  # select word (positive case)
            {"resourceType": "DiagnosticReport"},
            "hello, world",
            {"select_by_word": ["hello"]},
            True,
        ),
        (  # select word (multiple selections, or'd together)
            {"resourceType": "DiagnosticReport"},
            "hello world",
            {"select_by_word": ["bye", "hello"]},
            True,
        ),
        (  # select word (weird characters)
            {"resourceType": "DiagnosticReport"},
            "hel*lo.1+ world",
            {"select_by_word": ["hel*lo.1+"]},
            True,
        ),
        (  # select word (substring isn't matched)
            {"resourceType": "DiagnosticReport"},
            "hello world",
            {"select_by_word": ["hell"]},
            False,
        ),
        (  # select word (multi word)
            {"resourceType": "DiagnosticReport"},
            "hello world, mr smith",
            {"select_by_word": ["mr smith"]},
            True,
        ),
        (  # select regex (matches)
            {"resourceType": "DiagnosticReport"},
            "hello world",
            {"select_by_regex": ["hell."]},
            True,
        ),
        (  # select regex (can cross word boundaries)
            {"resourceType": "DiagnosticReport"},
            "hello world",
            {"select_by_regex": ["hell.*d"]},
            True,
        ),
        (  # select regex (can cross word boundaries, but still respects word ends)
            {"resourceType": "DiagnosticReport"},
            "hello world",
            {"select_by_regex": ["hell.*r"]},
            False,
        ),
        (  # select word and regex (matches either)
            {"resourceType": "DiagnosticReport"},
            "hello world",
            {"select_by_word": ["world"], "select_by_regex": ["h."]},
            True,
        ),
        (  # reject word (by itself, without matching, we should select note)
            {"resourceType": "DiagnosticReport"},
            "hello world",
            {"reject_by_word": ["bye"]},
            True,
        ),
        (  # reject word (by itself, with matching, we should reject note)
            {"resourceType": "DiagnosticReport"},
            "hello world",
            {"reject_by_word": ["hello"]},
            False,
        ),
        (  # reject word (multiple options, will reject either)
            {"resourceType": "DiagnosticReport"},
            "hello world",
            {"reject_by_word": ["hello", "bye"]},
            False,
        ),
        (  # reject word (and select it, reject should win)
            {"resourceType": "DiagnosticReport"},
            "hello world",
            {"reject_by_word": ["hello"], "select_by_word": ["hello"]},
            False,
        ),
        (  # reject word (miss) and select word (hit)
            {"resourceType": "DiagnosticReport"},
            "hello world",
            {"reject_by_word": ["bye"], "select_by_word": ["hello"]},
            True,
        ),
        (  # reject regex (simple match)
            {"resourceType": "DiagnosticReport"},
            "hello world",
            {"reject_by_regex": ["he..o"]},
            False,
        ),
        (  # reject regex (across words)
            {"resourceType": "DiagnosticReport"},
            "hello world",
            {"reject_by_regex": ["he.*rld"]},
            False,
        ),
        (  # reject word and regex (either should reject)
            {"resourceType": "DiagnosticReport"},
            "hello world",
            {"reject_by_regex": ["he..o"], "reject_by_word": ["bye"]},
            False,
        ),
        (  # select on non-first lines
            {"resourceType": "DiagnosticReport"},
            "hello\nworld",
            {"select_by_word": ["world"]},
            True,
        ),
        (  # reject on non-first lines
            {"resourceType": "DiagnosticReport"},
            "hello\nworld",
            {"reject_by_word": ["world"]},
            False,
        ),
        (  # select across lines and whitespace, if multiple words provided
            {"resourceType": "DiagnosticReport"},
            "hello  \n  world",
            {"select_by_word": ["hello world"]},
            True,
        ),
        (  # (don't) select across lines with other stuff in there, confirming a lack of match
            {"resourceType": "DiagnosticReport"},
            "hello\n.world",
            {"select_by_word": ["hello world"]},
            False,
        ),
    ],
)
def test_note_filter(res, text, kwargs, selected):
    note_filter = note_utils.make_note_filter(**kwargs)
    assert selected == note_filter(res, text), kwargs
