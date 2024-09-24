import pathlib
import shutil

import pytest

from cumulus_library import study_manifest
from cumulus_library.builders.valueset import static_builder, valueset_utils


@pytest.mark.parametrize(
    "filtered,ignore_header,prefix_str,keyword_path,mapping,expected",
    [
        (
            None,
            False,
            "",
            None,
            None,
            {
                "headers": ["foo", "bar"],
                "data": [
                    ("header_1", "header_2"),
                    ("val_1", "val_2"),
                    ("val_3", "val_4"),
                ],
            },
        ),
        (
            "filtered.csv",
            False,
            "",
            None,
            None,
            {"headers": ["foo", "bar"], "data": [("val_2", None)]},
        ),
        (
            None,
            True,
            "",
            "keywords.tsv",
            None,
            {
                "headers": ["foo", "bar"],
                "data": [("val_1", "val_2"), ("val_3", "val_4")],
            },
        ),
        (
            None,
            None,
            "foo",
            None,
            [{"from": "bar", "to": "baz", "map_dict": {"val_2": "val_5"}}],
            {
                "headers": ["foo", "bar", "baz"],
                "data": [
                    ("header_1", "header_2", None),
                    ("val_1", "val_2", "val_5"),
                    ("val_3", "val_4", None),
                ],
            },
        ),
    ],
)
def test_static_tables(
    tmp_path, mock_db_config, filtered, ignore_header, prefix_str, keyword_path, mapping, expected
):
    mock_db_config.options = {"steward": "acep"}
    test_path = pathlib.Path(__file__).parent.parent / "test_data/valueset/"
    shutil.copy(test_path / "static/static_table.csv", tmp_path / "static_table.csv")
    shutil.copy(test_path / "static/filtered.csv", tmp_path / "filtered.csv")
    valueset_config = valueset_utils.ValuesetConfig(
        vsac_stewards={"acep": "2.16.840.1.113762.1.4.1106.68"}
    )
    if prefix_str:
        valueset_config.table_prefix = prefix_str
        prefix_str += "_"
    if keyword_path:
        valueset_config.keyword_path = keyword_path
    builder = static_builder.StaticBuilder()
    filtered = tmp_path / filtered if filtered else None
    builder.get_table_configs = lambda prefix: [
        static_builder.TableConfig(
            file_path=tmp_path / "static_table.csv",
            delimiter=",",
            table_name=f"{prefix_str}test_table",
            headers=["foo", "bar"],
            dtypes={"foo": "str", "bar": "str"},
            parquet_types=["STR", "STR"],
            filtered_path=filtered,
            ignore_header=ignore_header,
            map_cols=mapping,
        )
    ]
    builder.execute_queries(
        config=mock_db_config,
        manifest=study_manifest.StudyManifest(test_path),
        valueset_config=valueset_config,
    )
    result = mock_db_config.db.cursor().execute(f"select * from test__{prefix_str}test_table")
    cols = [col[0] for col in result.description]
    assert cols == expected["headers"]
    assert result.fetchall() == expected["data"]
