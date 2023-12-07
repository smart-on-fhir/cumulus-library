import datetime
import pytest


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
def test_core_tables(mock_db_core, tmp_path, table):
    cursor = mock_db_core.cursor()
    table_rows = cursor.execute(f"SELECT * FROM {table}").fetchall()
    # For regenerating data if needed
    # with open(f'./tests/test_data/core/{table}.txt','wt', encoding="UTF-8") as f:
    #    f.write(str(table_rows))
    with open(f"./tests/test_data/core/{table}.txt", "r", encoding="UTF-8") as f:
        ref_table = eval(f.read())
    for row in ref_table:
        assert row in table_rows
    assert len(table_rows) == len(ref_table)
