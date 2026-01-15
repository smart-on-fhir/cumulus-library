import json
import pathlib
import shutil
from unittest import mock

import pytest

from cumulus_library import study_manifest
from cumulus_library.builders.valueset import static_builder, valueset_utils


@pytest.mark.parametrize(
    "filtered,prefix_str,mapping,expected",
    [
        (
            None,
            "",
            None,
            {
                "headers": ["foo", "bar"],
                "data": [
                    ("val_1", "val_2"),
                    ("val_3", "val_4"),
                ],
            },
        ),
        (
            "filtered.csv",
            "",
            None,
            {"headers": ["foo", "bar"], "data": [("val_2", None)]},
        ),
        (
            None,
            "",
            None,
            {
                "headers": ["foo", "bar"],
                "data": [("val_1", "val_2"), ("val_3", "val_4")],
            },
        ),
        (
            None,
            "foo",
            [{"from": "bar", "to": "baz", "map_dict": {"val_2": "val_5"}}],
            {
                "headers": ["foo", "bar", "baz"],
                "data": [
                    ("val_1", "val_2", "val_5"),
                    ("val_3", "val_4", None),
                ],
            },
        ),
    ],
)
@mock.patch("cumulus_library.base_utils.get_user_cache_dir")
@mock.patch("cumulus_library.apis.umls.UmlsApi")
def test_static_tables(
    mock_api,
    mock_cache_dir,
    tmp_path,
    mock_db_config,
    filtered,
    prefix_str,
    mapping,
    expected,
):
    mock_cache_dir.return_value = tmp_path
    with open(pathlib.Path(__file__).parents[2] / "test_data/valueset/vsac_resp.json") as f:
        resp = json.load(f)
        mock_api.return_value.get_vsac_valuesets.return_value = resp
    test_path = pathlib.Path(__file__).parents[2] / "test_data/valueset/"
    shutil.copy(test_path / "static/static_table.csv", tmp_path / "static_table.csv")
    shutil.copy(test_path / "static/filtered.csv", tmp_path / "filtered.csv")
    valueset_config = valueset_utils.ValuesetConfig(
        vsac_stewards={"acep": "2.16.840.1.113762.1.4.1106.68"}
    )
    if prefix_str:
        valueset_config.table_prefix = prefix_str
        prefix_str += "_"
    builder = static_builder.StaticBuilder()
    filtered = tmp_path / filtered if filtered else None
    builder.get_keywords_table_configs = lambda config, manifest, prefix: [
        static_builder.TableConfig(
            file_path=tmp_path / "static_table.csv",
            delimiter=",",
            table_name=f"{prefix_str}test_table",
            headers=["foo", "bar"],
            dtypes={"foo": "str", "bar": "str"},
            parquet_types=["STR", "STR"],
            filtered_path=filtered,
            map_cols=mapping,
        )
    ]
    builder.execute_queries(
        config=mock_db_config,
        manifest=study_manifest.StudyManifest(test_path),
        valueset_config=valueset_config,
        toml_path=tmp_path,
    )
    result = mock_db_config.db.cursor().execute(f"select * from test__{prefix_str}test_table")
    cols = [col[0] for col in result.description]
    assert cols == expected["headers"]
    assert result.fetchall() == expected["data"]


@mock.patch("cumulus_library.base_utils.get_user_cache_dir")
@mock.patch("cumulus_library.apis.umls.UmlsApi")
def test_custom_rules(mock_api, mock_cache_dir, tmp_path, mock_db_config):
    mock_cache_dir.return_value = tmp_path
    with open(pathlib.Path(__file__).parents[2] / "test_data/valueset/vsac_resp.json") as f:
        resp = json.load(f)
        mock_api.return_value.get_vsac_valuesets.return_value = resp

    test_path = pathlib.Path(__file__).parents[2] / "test_data/valueset/"
    shutil.copy(test_path / "static/static_table.csv", tmp_path / "static_table.csv")
    shutil.copy(test_path / "static/filtered.csv", tmp_path / "filtered.csv")
    valueset_config = valueset_utils.ValuesetConfig(
        vsac_stewards={"acep": "2.16.840.1.113762.1.4.1106.68"},
        rules_file="rules_file.tsv",
        table_prefix="prefix",
    )
    builder = static_builder.StaticBuilder()
    builder.execute_queries(
        config=mock_db_config,
        manifest=study_manifest.StudyManifest(test_path),
        valueset_config=valueset_config,
        toml_path=test_path,
    )
    result = (
        mock_db_config.db.cursor().execute("select * from test__prefix_search_rules").fetchall()
    )
    assert len(result) == 3
    assert ("BN", "reformulated_to", "BN", "Yes", True) in result
