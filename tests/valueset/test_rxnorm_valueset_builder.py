import json
import os
import pathlib
import tomllib
from unittest import mock

from cumulus_library import study_manifest
from cumulus_library.builders.valueset import (
    rxnorm_valueset_builder,
    static_builder,
    valueset_utils,
)


@mock.patch.dict(
    os.environ,
    clear=True,
)
@mock.patch("cumulus_library.apis.umls.UmlsApi")
def test_rxnorm_valueset_builder(mock_api, mock_db_config_rxnorm):
    data_path = pathlib.Path(__file__).parent.parent / "test_data/valueset/"
    with open(data_path / "vsac_resp.json") as f:
        resp = json.load(f)
    mock_api.return_value.get_vsac_valuesets.return_value = resp
    manifest = study_manifest.StudyManifest(data_path)

    with open(data_path / "valueset.toml", "rb") as file:
        toml_config = tomllib.load(file)
    valueset_config = valueset_utils.ValuesetConfig(
        expansion_rules_file=toml_config.get("expansion_rules_file"),
        keyword_file=toml_config.get("keyword_file"),
        table_prefix=toml_config.get("target_table", ""),
        umls_stewards=toml_config.get("umls_stewards"),
        vsac_stewards=toml_config.get("vsac_stewards"),
    )

    cursor = mock_db_config_rxnorm.db.cursor()
    s_builder = static_builder.StaticBuilder()
    s_builder.execute_queries(
        config=mock_db_config_rxnorm, manifest=manifest, valueset_config=valueset_config
    )
    builder = rxnorm_valueset_builder.RxNormValuesetBuilder()
    builder.execute_queries(
        config=mock_db_config_rxnorm, manifest=manifest, valueset_config=valueset_config
    )
    res = cursor.execute("select * from test__rela ORDER BY 1,2,3,4,5").fetchall()
    assert len(res) == 1200
    assert res[0] == (
        1819,
        "(-)-buprenorphine",
        "SY",
        "DRUGBANK",
        1818,
        "RN",
        "reformulated_to",
        4716626,
        "acep",
    )
    assert res[-1] == (
        1819,
        "buprenorphine",
        "SU",
        "MTHSPL",
        1655031,
        "RO",
        "has_ingredient",
        86130850,
        "acep",
    )