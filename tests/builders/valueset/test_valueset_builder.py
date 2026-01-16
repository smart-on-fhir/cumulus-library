import json
import pathlib
from contextlib import nullcontext as does_not_raise
from unittest import mock

import pytest

from cumulus_library import study_manifest
from cumulus_library.builders import valueset_builder

data_path = pathlib.Path(__file__).parents[2] / "test_data/valueset/"


@pytest.mark.parametrize(
    ("config_path,tables,raises"),
    [
        (data_path / "valueset.toml", 18, does_not_raise()),
        (data_path / "valueset_vsac_only.toml", 17, does_not_raise()),
        (data_path / "valueset_umls_only.toml", 18, does_not_raise()),
        (data_path / "valueset_keyword_only.toml", 17, does_not_raise()),
        (data_path / "invalid.toml", 0, pytest.raises(SystemExit)),
    ],
)
@mock.patch("cumulus_library.apis.umls.UmlsApi")
def test_valueset_builder(mock_api, mock_db_config_rxnorm, config_path, tables, raises, tmp_path):
    with raises:
        with open(data_path / "vsac_resp.json") as f:
            resp = json.load(f)
        mock_api.return_value.get_vsac_valuesets.return_value = resp
        manifest = study_manifest.StudyManifest(data_path)
        cursor = mock_db_config_rxnorm.db.cursor()
        query = (
            f"""CREATE TABLE umls.tty_description AS SELECT * FROM 
    read_csv('{data_path}/tty.tsv',"""
            """    columns ={'tty': 'VARCHAR', 'tty_str': 'varchar'},
        delim = '\t',
        header = true
    )"""
        )
        cursor.execute(query)

        builder = valueset_builder.ValuesetBuilder(config_path, data_path / "valueset_data")
        builder.execute_queries(mock_db_config_rxnorm, manifest=manifest, toml_path=tmp_path)
        table_count = cursor.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name LIKE 'test__%' "
        ).fetchone()
        assert table_count[0] == tables


@mock.patch("cumulus_library.apis.umls.UmlsApi")
def test_prefix_handling(mock_api, mock_db_config_rxnorm, tmp_path):
    with open(data_path / "vsac_resp.json") as f:
        resp = json.load(f)
    mock_api.return_value.get_vsac_valuesets.return_value = resp
    manifest = study_manifest.StudyManifest(data_path)
    cursor = mock_db_config_rxnorm.db.cursor()
    query = (
        f"""CREATE TABLE umls.tty_description AS SELECT * FROM 
    read_csv('{data_path}/tty.tsv',"""
        """    columns ={'tty': 'VARCHAR', 'tty_str': 'varchar'},
        delim = '\t',
        header = true
    )"""
    )
    cursor.execute(query)
    builder = valueset_builder.ValuesetBuilder(
        data_path / "valueset_prefix.toml", data_path / "valueset_data"
    )
    builder.execute_queries(mock_db_config_rxnorm, manifest=manifest, toml_path=tmp_path)
    tables = cursor.execute("SELECT table_name FROM information_schema.tables").fetchall()
    tables += cursor.execute("SELECT table_name FROM information_schema.views").fetchall()
    for table in [
        ("test__foo_all_rxnconso_keywords",),
        ("test__foo_combined_ruleset",),
        ("test__foo_included_rels",),
        ("test__foo_keywords",),
        ("test__foo_potential_rules",),
        ("test__foo_rxnconso_keywords",),
        ("test__foo_search_rules",),
        ("test__foo_search_rules_descriptions",),
        ("test__foo_umls_valuesets",),
        ("test__foo_umls_valuesets_rels",),
        ("test__foo_valuesets",),
        ("test__foo_vsac_valuesets",),
        ("test__foo_rxnconso",),
        ("test__foo_rxnrel",),
        ("test__foo_rxnsty",),
        ("test__foo_included_keywords",),
        ("test__foo_rela",),
        ("test__foo_vsac_valuesets_hydrated",),
    ]:
        assert table in tables

    builder = valueset_builder.ValuesetBuilder(
        data_path / "valueset.toml", data_path / "valueset_data"
    )
    builder.execute_queries(mock_db_config_rxnorm, manifest=manifest, toml_path=tmp_path)
    tables = cursor.execute("SELECT table_name FROM information_schema.tables").fetchall()
    for table in [
        ("test__all_rxnconso_keywords",),
        ("test__combined_ruleset",),
        ("test__included_rels",),
        ("test__keywords",),
        ("test__potential_rules",),
        ("test__rxnconso_keywords",),
        ("test__search_rules",),
        ("test__search_rules_descriptions",),
        ("test__umls_valuesets",),
        ("test__umls_valuesets_rels",),
        ("test__valuesets",),
        ("test__vsac_valuesets",),
        ("test__rxnconso",),
        ("test__rxnrel",),
        ("test__rxnsty",),
        ("test__included_keywords",),
        ("test__rela",),
        ("test__vsac_valuesets_hydrated",),
    ]:
        assert table in tables
