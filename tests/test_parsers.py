"""Test for FHIR resource parsers"""

from contextlib import nullcontext as does_not_raise
import json

from unittest.mock import mock_open, patch

import pytest

from fhirclient.models.coding import Coding

from cumulus_library.parsers import fhir_valueset


@pytest.mark.parametrize(
    "filepath,result_len,raises",
    [
        (
            "./tests/test_data/parser_sample_data/fhir_valueset.json",
            3,
            does_not_raise(),
        ),
        (None, 0, pytest.raises(TypeError)),
        ("./tests/test_data/missing.txt", 0, pytest.raises(FileNotFoundError)),
        (
            "./tests/test_data/count_synthea_patient.csv",
            0,
            pytest.raises(json.decoder.JSONDecodeError),
        ),
    ],
)
def test_include_coding(filepath, result_len, raises):
    with raises:
        result = fhir_valueset.get_include_coding(filepath)
        assert len(result) == result_len


@pytest.mark.parametrize(
    "coding,view_name,raises",
    [
        (
            [Coding({"code": "1", "display": "A", "system": "foo"})],
            "test",
            does_not_raise(),
        ),
        (
            [Coding({"code": "1", "display": "A", "system": "foo"})],
            None,
            pytest.raises(TypeError),
        ),
        (
            [{"code": "1", "display": "A", "system": "foo"}],
            "test",
            pytest.raises(AttributeError),
        ),
    ],
)
def test_write_create_sql(coding, view_name, raises):
    with raises:
        with patch("builtins.open", mock_open()) as mock_fs:
            fhir_valueset.write_view_sql(view_name, coding)
            mock_fs.assert_called()
