import json
import pathlib
import tomllib
from unittest import mock

import pytest

from cumulus_library import study_manifest
from cumulus_library.builders.valueset import (
    rxnorm_valueset_builder,
    static_builder,
    valueset_utils,
)


@pytest.mark.parametrize("prefix", [(""), ("foo")])
@mock.patch("cumulus_library.apis.umls.UmlsApi")
@mock.patch("cumulus_library.base_utils.get_user_cache_dir")
def test_rxnorm_valueset_builder(mock_user_dir, mock_api, mock_db_config_rxnorm, prefix, tmp_path):
    mock_user_dir.return_value = tmp_path
    data_path = pathlib.Path(__file__).parents[2] / "test_data/valueset/"
    with open(data_path / "vsac_resp.json") as f:
        resp = json.load(f)
    mock_api.return_value.get_vsac_valuesets.return_value = resp
    manifest = study_manifest.StudyManifest(data_path)

    with open(data_path / "valueset.toml", "rb") as file:
        toml_config = tomllib.load(file)
    valueset_config = valueset_utils.ValuesetConfig(
        rules_file=toml_config.get("rules_file"),
        keyword_file=toml_config.get("keyword_file"),
        table_prefix=toml_config.get("target_table", ""),
        umls_stewards=toml_config.get("umls_stewards"),
        vsac_stewards=toml_config.get("vsac_stewards"),
    )
    if prefix:
        valueset_config.table_prefix = prefix
        prefix += "_"

    cursor = mock_db_config_rxnorm.db.cursor()
    s_builder = static_builder.StaticBuilder()
    s_builder.execute_queries(
        config=mock_db_config_rxnorm, manifest=manifest, valueset_config=valueset_config
    )
    builder = rxnorm_valueset_builder.RxNormValuesetBuilder()
    builder.execute_queries(
        config=mock_db_config_rxnorm, manifest=manifest, valueset_config=valueset_config
    )
    query = f"""select * from test__{prefix}vsac_valuesets"""
    res = cursor.execute(query)
    query = f"""select * from test__{prefix}rxnconso"""
    res = cursor.execute(f"select * from test__{prefix}rela ORDER BY 1,2,3,4,5").fetchall()
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
