import json
import pathlib
import tomllib
from unittest import mock

import pytest

from cumulus_library import study_manifest
from cumulus_library.actions import builder as build_action
from cumulus_library.builders.valueset import (
    rxnorm_valueset_builder,
    static_builder,
    valueset_utils,
)


@pytest.mark.parametrize("prefix", [(None), (""), ("foo")])
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
        table_prefix=toml_config.get("table_prefix", prefix),
        umls_stewards=toml_config.get("umls_stewards"),
        vsac_stewards=toml_config.get("vsac_stewards"),
    )
    if prefix:
        prefix += "_"
    else:
        prefix = ""
    cursor = mock_db_config_rxnorm.db.cursor()
    build_action.run_protected_table_builder(config=mock_db_config_rxnorm, manifest=manifest)
    s_builder = static_builder.StaticBuilder()
    s_builder.execute_queries(
        config=mock_db_config_rxnorm,
        manifest=manifest,
        valueset_config=valueset_config,
        toml_path=tmp_path,
    )
    builder = rxnorm_valueset_builder.RxNormValuesetBuilder()
    builder.execute_queries(
        config=mock_db_config_rxnorm,
        manifest=manifest,
        valueset_config=valueset_config,
        toml_path=tmp_path,
    )
    query = f"""select * from test__{prefix}vsac_valuesets"""
    res = cursor.execute(query)
    query = f"""select * from test__{prefix}rxnconso"""
    res = cursor.execute(f"select * from test__{prefix}rela ORDER BY 1,2,3,4,5").fetchall()
    assert len(res) == 1200
    assert res[0] == (
        "1819",
        "(-)-buprenorphine",
        "SY",
        "DRUGBANK",
        "1151359",
        "RO",
        "has_ingredient",
        "18636093",
        "acep",
    )
    assert res[-1] == (
        (
            "1819",
            "buprenorphine",
            "SU",
            "MTHSPL",
            "904879",
            "RO",
            "has_ingredient",
            "5110638",
            "acep",
        )
    )


# Q: Who passes mock_db_config to this function??
# A: They are fixtures in tests/conftest.py


#   We still configure VSAC, which uses the UMLS API; without mock, the test
# depends on cached files or a real API call
@mock.patch("cumulus_library.base_utils.get_user_cache_dir")
@mock.patch("cumulus_library.apis.umls.UmlsApi")
def test_no_umls_valuesets_table_when_empty_umls(
    mock_api, mock_cache_dir, mock_db_config_rxnorm, tmp_path
):
    mock_cache_dir.return_value = tmp_path

    test_path = pathlib.Path(__file__).parents[2] / "test_data/valueset/"
    manifest = study_manifest.StudyManifest(test_path)
    with open(test_path / "valueset.toml", "rb") as valueset_toml:
        toml_config = tomllib.load(valueset_toml)

    with open(test_path / "vsac_resp.json") as vsac_resp:
        resp = json.load(vsac_resp)

    mock_api.return_value.get_vsac_valuesets.return_value = resp

    #   The production TOML loader (builders/valueset_builder) uses {} for a
    # missing source. Not setting it explicitly can cause an iteration error
    # before the final assertion
    valueset_config = valueset_utils.ValuesetConfig(
        rules_file=toml_config.get("rules_file"),
        keyword_file=toml_config.get("keyword_file"),
        table_prefix=toml_config.get("table_prefix"),
        umls_stewards={},
        vsac_stewards=toml_config.get("vsac_stewards"),
    )

    build_action.run_protected_table_builder(config=mock_db_config_rxnorm, manifest=manifest)

    builder = static_builder.StaticBuilder()
    builder.execute_queries(
        config=mock_db_config_rxnorm,
        manifest=manifest,
        valueset_config=valueset_config,
        toml_path=tmp_path,
        #   Using toml_path=test_path writes generated files into your test
        # data directory; the cache mock above doesn't override an explicit
        # path
    )

    rxnorm_builder = rxnorm_valueset_builder.RxNormValuesetBuilder()
    rxnorm_builder.execute_queries(
        config=mock_db_config_rxnorm,
        manifest=manifest,
        valueset_config=valueset_config,
        toml_path=tmp_path,
    )

    result = (
        mock_db_config_rxnorm.db.cursor()
        .execute(
            "SELECT EXISTS ( "
            "   SELECT tablename "
            "   FROM pg_tables "
            "   WHERE tablename = 'test__umls_valuesets' "
            ");"
        )
        .fetchall()
    )

    assert (False,) in result
