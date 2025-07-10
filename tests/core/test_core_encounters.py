"""Tests for core__encounter"""

import json

from tests import testbed_utils


def test_core_enc_class(tmp_path):
    """Verify that we handle multiply rows as needed when multiple options appear"""
    v2_sys = "http://terminology.hl7.org/CodeSystem/v2-0004"
    v3_sys = "http://terminology.hl7.org/CodeSystem/v3-ActCode"

    testbed = testbed_utils.LocalTestbed(tmp_path)
    testbed.add_encounter("o", **{"class": {"code": "O", "system": v2_sys}})
    testbed.add_encounter("obsenc", **{"class": {"code": "OBSENC", "system": v3_sys}})
    testbed.add_encounter("unsupported", **{"class": {"code": "?", "system": v3_sys}})

    con = testbed.build()
    df = con.sql("SELECT id, class_code, class_display FROM core__encounter ORDER BY id").df()
    rows = json.loads(df.to_json(orient="records"))
    assert rows == [
        {"id": "o", "class_code": "AMB", "class_display": "ambulatory"},
        {"id": "obsenc", "class_code": "OBSENC", "class_display": "observation encounter"},
        {"id": "unsupported", "class_code": "?", "class_display": None},
    ]
