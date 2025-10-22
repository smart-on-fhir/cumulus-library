"""Tests for core__documentreference"""

import json

from tests import testbed_utils


def get_has_text_fields(testbed: testbed_utils.LocalTestbed) -> dict[str, bool]:
    con = testbed.build()
    df = con.sql("SELECT * FROM core__documentreference").df()
    rows = json.loads(df.to_json(orient="records"))
    return {row["id"]: row["aux_has_text"] for row in rows}


def test_core_docref_aux_has_text_no_fields(tmp_path):
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add_document_reference("nothing")
    fields = get_has_text_fields(testbed)
    assert fields == {"nothing": False}


def test_core_docref_aux_has_text_just_data(tmp_path):
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add_document_reference(
        "plain-data-with-charset",
        content=[
            {"attachment": {"contentType": "text/plain; charset=ascii", "data": "blarg"}},
            {"attachment": {"size": 40}},
        ],
    )
    testbed.add_document_reference(
        "html-data",
        content=[{"attachment": {"contentType": "text/html", "data": "blarg"}}],
    )
    testbed.add_document_reference(
        "xhtml-data",
        content=[{"attachment": {"contentType": "application/xhtml+xml", "data": "blarg"}}],
    )
    testbed.add_document_reference(
        "bad-type",
        content=[{"attachment": {"contentType": "text/bogus", "data": "blarg"}}],
    )
    testbed.add_document_reference("no-type", content=[{"attachment": {"data": "blarg"}}])
    testbed.add_document_reference(
        "no-data",
        content=[{"attachment": {"contentType": "text/plain; charset=ascii", "size": 10}}],
    )
    testbed.add_document_reference("no-content")
    fields = get_has_text_fields(testbed)
    assert fields == {
        "plain-data-with-charset": True,
        "html-data": True,
        "xhtml-data": True,
        "bad-type": False,
        "no-type": False,
        "no-data": False,
        "no-content": False,
    }


def test_core_docref_aux_has_text_just_data_ext(tmp_path):
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add_document_reference(
        "has-data-ext",
        content=[
            {
                "attachment": {
                    "contentType": "text/html",
                    "_data": {
                        "extension": [
                            {"url": "bogus"},
                            {
                                "url": "http://hl7.org/fhir/StructureDefinition/data-absent-reason",
                                "valueCode": "masked",
                            },
                        ],
                    },
                },
            },
        ],
    )
    testbed.add_document_reference(
        "no-data",
        content=[{"attachment": {"contentType": "text/html"}}],
    )
    testbed.add_document_reference("no-content")
    fields = get_has_text_fields(testbed)
    assert fields == {
        "has-data-ext": True,
        "no-data": False,
        "no-content": False,
    }


def test_core_docref_aux_has_text_both_fields(tmp_path):
    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add_document_reference(
        "has-data-ext",
        content=[
            {
                "attachment": {
                    "contentType": "text/html",
                    "_data": {
                        "extension": [
                            {"url": "bogus"},
                            {
                                "url": "http://hl7.org/fhir/StructureDefinition/data-absent-reason",
                                "valueCode": "masked",
                            },
                        ],
                    },
                },
            },
        ],
    )
    testbed.add_document_reference(
        "has-data",
        content=[{"attachment": {"contentType": "text/html", "data": "blarg"}}],
    )
    testbed.add_document_reference(
        "no-data",
        content=[{"attachment": {"contentType": "text/html"}}],
    )
    testbed.add_document_reference("no-content")
    fields = get_has_text_fields(testbed)
    assert fields == {
        "has-data-ext": True,
        "has-data": True,
        "no-data": False,
        "no-content": False,
    }
