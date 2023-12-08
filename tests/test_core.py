import datetime

from pathlib import Path

import pytest
import toml


@pytest.mark.parametrize(
    "table",
    [
        ("core__condition"),
        ("core__documentreference"),
        ("core__encounter"),
        ("core__observation"),
        ("core__patient"),
        ("core__count_condition_month"),
        ("core__count_documentreference_month"),
        ("core__count_encounter_month"),
        ("core__count_observation_lab_month"),
        ("core__count_patient"),
    ],
)
def test_core_tables(mock_db_core, table):
    cursor = mock_db_core.cursor()
    table_rows = cursor.execute(f"SELECT * FROM {table}").fetchall()
    print(type(table_rows))
    # For regenerating data if needed
    # with open(f'./tests/test_data/core/{table}.txt','wt', encoding="UTF-8") as f:
    #    for row in table_rows:
    #        f.write(str(f"{row}\n"))
    with open(f"./tests/test_data/core/{table}.txt", "r", encoding="UTF-8") as f:
        ref_table = []
        for row in f.readlines():
            ref_table.append(eval(row))
    for row in ref_table:
        assert row in table_rows
    assert len(table_rows) == len(ref_table)


def test_core_counts_exported(mock_db_core):
    manifest = toml.load(
        f"{Path(__file__).parent.parent}/cumulus_library/studies/core/manifest.toml"
    )
    manifest["export_config"]["export_list"].remove("core__meta_version")
    manifest["export_config"]["export_list"].remove("core__meta_date")
    count_tables = (
        mock_db_core.cursor()
        .execute(
            "SELECT distinct(table_name) FROM information_schema.tables "
            "WHERE table_name LIKE 'core__count_%'"
        )
        .fetchall()
    )
    count_tables = [x[0] for x in count_tables]
    assert set(manifest["export_config"]["export_list"]) == set(count_tables)
