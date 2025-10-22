import json
import pathlib
from contextlib import nullcontext as does_not_raise
from unittest import mock

import pytest

from cumulus_library import study_manifest
from cumulus_library.builders import valueset_builder

data_path = pathlib.Path(__file__).parents[2] / "test_data/valueset/"


@pytest.mark.parametrize(
    ("config_path,raises"),
    [
        (data_path / "valueset.toml", does_not_raise()),
        (data_path / "invalid.toml", pytest.raises(SystemExit)),
    ],
)
@mock.patch("cumulus_library.apis.umls.UmlsApi")
def test_valueset_builder(mock_api, mock_db_config_rxnorm, config_path, raises):
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
        builder.execute_queries(mock_db_config_rxnorm, manifest=manifest)
        table_count = cursor.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name LIKE 'test__%' "
        ).fetchone()
        assert table_count[0] == 18
