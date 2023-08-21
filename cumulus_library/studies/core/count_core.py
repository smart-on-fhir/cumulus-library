from typing import List
from pathlib import Path
from cumulus_library.schema.counts import (
    count_patient,
    count_encounter,
    get_table_name,
    get_where_clauses,
)

STUDY = "core"


def count_core_patient():
    table_name = get_table_name(STUDY, "count_patient")
    from_table = get_table_name(STUDY, "patient")
    cols = ["age", "gender", "race_display", "ethnicity_display"]
    return count_patient(table_name, from_table, cols)


def count_core_encounter(duration=None):
    table_name = get_table_name(STUDY, "count_encounter")
    from_table = get_table_name(STUDY, "encounter")

    cols = [
        f"start_{duration}",
        "enc_class_display",
        "age_at_visit",
        "gender",
        "race_display",
        "ethnicity_display",
    ]

    return count_encounter(table_name, from_table, cols)


def _count_core_encounter_type(table_name, cols, duration):
    """
    Encounter Type information is for every visit, and therefore this
    SQL should be precise in which fields to select (This is a BIG query).

    :param table_name: name of the view from "core__encounter_type"
    :param cols: from "core__encounter_type"
    :param duration: None or ''month', 'year'
    :return: SQL commands
    """
    table_name = get_table_name(STUDY, table_name, duration)
    from_table = get_table_name(STUDY, "encounter_type")

    if duration:
        cols.append(f"start_{duration}")

    where = get_where_clauses(min_subject=10)

    return count_encounter(table_name, from_table, cols, where_clauses=where)


def count_core_encounter_type(duration=None):
    cols = [
        "enc_class_display",
        "enc_type_display",
        "enc_service_display",
        "enc_priority_display",
    ]
    return _count_core_encounter_type("count_encounter_type", cols, duration)


def count_core_encounter_enc_type(duration="month"):
    cols = ["enc_class_display", "enc_type_display"]
    return _count_core_encounter_type("count_encounter_enc_type", cols, duration)


def count_core_encounter_service(duration="month"):
    cols = ["enc_class_display", "enc_service_display"]
    return _count_core_encounter_type("count_encounter_service", cols, duration)


def count_core_encounter_priority(duration="month"):
    cols = ["enc_class_display", "enc_priority_display"]
    return _count_core_encounter_type("count_encounter_priority", cols, duration)


def concat_table_sql(sql_list: List[str]) -> str:
    """
    :param create_view_list: SQL prepared statements
    :param filename: path to output file, default 'count.sql' in PWD
    """
    seperator = "-- ###########################################################"
    concat = list()
    for statement in sql_list:
        concat.append(seperator + "\n" + statement + "\n")

    return "\n".join(concat)


def write_sql(view_list_sql: List[str], filename="count_core.sql") -> None:
    """
    :param view_list_sql: SQL prepared statements
    :param filename: path to output file, default 'count_core.sql' in PWD
    """
    sql_optimizer = concat_table_sql(view_list_sql)

    with open(f"{Path(__file__).parent.absolute()}/{filename}", "w") as fout:
        fout.write("-- noqa: disable=all\n")
        fout.write(sql_optimizer)


if __name__ == "__main__":
    write_sql(
        [
            count_core_patient(),
            count_core_encounter("month"),
            count_core_encounter_type(),
            count_core_encounter_type("month"),
            count_core_encounter_enc_type("month"),
            count_core_encounter_service("month"),
            count_core_encounter_priority("month"),
        ]
    )
