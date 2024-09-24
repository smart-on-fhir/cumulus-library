import json
import pathlib
from contextlib import nullcontext as does_not_raise
from unittest import mock

import pytest

from cumulus_library import base_utils, errors
from cumulus_library.builders.valueset import vsac


@mock.patch("cumulus_library.apis.umls.UmlsApi")
@pytest.mark.parametrize(
    "steward,oid,umls,force,raises,expected",
    [
        ("acep", "oid", None, False, does_not_raise(), 1),
        ("acep", "oid", "1234567", False, does_not_raise(), 1),
        ("acep", "oid", None, True, does_not_raise(), 1),
        ("invalid", "bad oid", None, False, pytest.raises(errors.ApiError), 0),
    ],
)
def test_download_oid_data(
    mock_api, mock_db, steward, oid, umls, force, raises, expected, tmp_path
):
    with raises:
        with open(pathlib.Path(__file__).parent.parent / "test_data/valueset/vsac_resp.json") as f:
            resp = json.load(f)
        if steward == "acep":
            mock_api.return_value.get_vsac_valuesets.return_value = resp
        else:
            mock_api.return_value.get_vsac_valuesets.side_effect = errors.ApiError
        config = base_utils.StudyConfig(
            db=mock_db,
            schema="test",
            umls_key=umls,
            force_upload=force,
            options={"steward": steward},
        )
        vsac.download_oid_data(steward=steward, oid=oid, path=tmp_path, config=config)
        output_dir = list(tmp_path.glob("*"))
        assert len(output_dir) == 4
        for filename in [f"{steward}.json", f"{steward}.tsv", f"{steward}.parquet", "duck.db"]:
            assert len([x for x in output_dir if filename in str(x)]) == expected
        with open(tmp_path / f"{steward}.tsv") as f:
            tsv = f.readlines()
            assert tsv[0].strip() == (
                "1010600\tbuprenorphine 2 MG / naloxone 0.5 MG Sublingual Film"
            )
            assert tsv[-1].strip() == "998213\t1 ML morphine sulfate 4 MG/ML Prefilled Syringe"
        redownload = vsac.download_oid_data(steward=steward, oid=oid, path=tmp_path, config=config)
        assert redownload == force


@mock.patch("cumulus_library.builders.valueset.vsac.download_oid_data")
def test_cli(mock_download):
    vsac.main(
        cli_args=["--steward=acep", "--oid=123", "--api-key=456", "--force-upload", "--path=/tmp"]
    )
    assert mock_download.is_called
