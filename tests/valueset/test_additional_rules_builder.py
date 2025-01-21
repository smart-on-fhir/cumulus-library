import json
import pathlib
import shutil
import tomllib
from unittest import mock

import pytest

from cumulus_library import study_manifest
from cumulus_library.builders.valueset import (
    additional_rules_builder,
    rxnorm_valueset_builder,
    static_builder,
    valueset_utils,
)


@pytest.mark.parametrize("prefix", [(""), ("foo")])
@mock.patch("cumulus_library.apis.umls.UmlsApi")
def test_additional_rules(mock_api, mock_db_config_rxnorm, prefix, tmp_path):
    data_path = pathlib.Path(__file__).parent.parent / "test_data/valueset/"
    test_path = tmp_path / "valueset"
    shutil.copytree(data_path, test_path)
    with open(test_path / "vsac_resp.json") as f:
        resp = json.load(f)
    mock_api.return_value.get_vsac_valuesets.return_value = resp
    manifest = study_manifest.StudyManifest(test_path)

    with open(test_path / "valueset.toml", "rb") as file:
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
    query = (
        f"""CREATE TABLE umls.tty_description AS SELECT * FROM 
read_csv('{test_path}/tty.tsv',"""
        """    columns ={'tty': 'VARCHAR', 'tty_str': 'varchar'},
    delim = '\t',
    header = true
)"""
    )
    cursor.execute(query)

    s_builder = static_builder.StaticBuilder()
    s_builder.execute_queries(
        config=mock_db_config_rxnorm, manifest=manifest, valueset_config=valueset_config
    )
    r_builder = rxnorm_valueset_builder.RxNormValuesetBuilder()
    r_builder.execute_queries(
        config=mock_db_config_rxnorm, manifest=manifest, valueset_config=valueset_config
    )
    builder = additional_rules_builder.AdditionalRulesBuilder()
    builder.execute_queries(
        config=mock_db_config_rxnorm, manifest=manifest, valueset_config=valueset_config
    )
    res = cursor.execute(f"select * from test__{prefix}rela")
    for table_conf in [
        {
            "name": f"test__{prefix}potential_rules",
            "columns": 10,
            "count": 48,
            "first": (
                1819,
                1818,
                "BN",
                "BN",
                4716626,
                "RN",
                "reformulated_to",
                "Buprenorphine",
                "Subutex",
                "subutex",
            ),
            "last": (
                1819,
                1818,
                "SY",
                "BN",
                4716626,
                "RN",
                "reformulated_to",
                "Buprenophine",
                "Subutex",
                "subutex",
            ),
        },
        {
            "name": f"test__{prefix}included_rels",
            "columns": 10,
            "count": 2,
            "first": (
                1819,
                1818,
                "BN",
                "BN",
                4716626,
                "RN",
                "reformulated_to",
                "Buprenorphine",
                "Subutex",
                "subutex",
            ),
            "last": (
                1819,
                1818,
                "BN",
                "BN",
                4716626,
                "RN",
                "reformulated_to",
                "Buprenorphine",
                "Subutex",
                "subutex",
            ),
        },
        {
            "name": f"test__{prefix}included_keywords",
            "columns": 10,
            "count": 48,
            "first": (
                1819,
                1818,
                "BN",
                "BN",
                4716626,
                "RN",
                "reformulated_to",
                "Buprenorphine",
                "Subutex",
                "subutex",
            ),
            "last": (
                1819,
                1818,
                "SY",
                "BN",
                4716626,
                "RN",
                "reformulated_to",
                "Buprenophine",
                "Subutex",
                "subutex",
            ),
        },
        # The following table has fewer rows than the proceding due to duplication
        # in the key lookup. The union operation removes extra rows.
        {
            "name": f"test__{prefix}combined_ruleset",
            "columns": 10,
            "count": 20,
            "first": (
                1819,
                1818,
                "BN",
                "BN",
                4716626,
                "RN",
                "reformulated_to",
                "Buprenorphine",
                "Subutex",
                "subutex",
            ),
            "last": (
                1819,
                1818,
                "SY",
                "BN",
                4716626,
                "RN",
                "reformulated_to",
                "Buprenophine",
                "Subutex",
                "subutex",
            ),
        },
    ]:
        res = cursor.execute(
            f"Select * from {table_conf['name']} order by "
            f"{','.join([str(x + 1) for x in range(table_conf['columns'])])}"
        )
        data = res.fetchall()
        assert len(data) == table_conf["count"]
        assert data[0] == table_conf["first"]
        if table_conf["count"] > 1:
            assert data[-1] == table_conf["last"]
