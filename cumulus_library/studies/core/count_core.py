from typing import List
from cumulus_library.schema import counts

STUDY_PREFIX = "core"


def table(tablename: str, duration=None) -> str:
    if duration:
        return f"{STUDY_PREFIX}__{tablename}_{duration}"
    else:
        return f"{STUDY_PREFIX}__{tablename}"


def count_patient():
    view_name = table("count_patient")
    from_table = table("patient")
    cols = ["age", "gender", "race_display", "ethnicity_display"]

    return counts.count_patient(view_name, from_table, cols)


def count_encounter(duration=None):
    view_name = table("count_encounter", duration)
    from_table = table("encounter")

    cols = [
        f"start_{duration}",
        "enc_class_display",
        "age_at_visit",
        "gender",
        "race_display",
        "ethnicity_display",
    ]

    return counts.count_encounter(view_name, from_table, cols)


def _count_encounter_type(view_name, cols, duration):
    """
    Encounter Type information is for every visit, and therefore this
    SQL should be precise in which fields to select (This is a BIG query).

    :param view_name: name of the view from "core__encounter_type"
    :param cols: from "core__encounter_type"
    :param duration: None or ''month', 'year'
    :return: SQL commands
    """
    view_name = table(view_name, duration)
    from_table = table("encounter_type")

    if duration:
        cols.append(f"start_{duration}")

    where = counts.where_clauses(min_subject=10)

    return counts.count_encounter(view_name, from_table, cols, where)


def count_encounter_type(duration=None):
    cols = [
        "enc_class_display",
        "enc_type_display",
        "enc_service_display",
        "enc_priority_display",
    ]
    return _count_encounter_type("count_encounter_type", cols, duration)


def count_encounter_enc_type(duration="month"):
    cols = ["enc_class_display", "enc_type_display"]
    return _count_encounter_type("count_encounter_enc_type", cols, duration)


def count_encounter_service(duration="month"):
    cols = ["enc_class_display", "enc_service_display"]
    return _count_encounter_type("count_encounter_service", cols, duration)


def count_encounter_priority(duration="month"):
    cols = ["enc_class_display", "enc_priority_display"]
    return _count_encounter_type("count_encounter_priority", cols, duration)


def concat_view_sql(create_view_list: List[str]) -> str:
    """
    :param create_view_list: SQL prepared statements
    :param filename: path to output file, default 'count.sql' in PWD
    """
    seperator = "-- ###########################################################"
    concat = list()

    for create_view in create_view_list:
        concat.append(seperator + "\n" + create_view + "\n")

    return "\n".join(concat)


def write_view_sql(view_list_sql: List[str], filename="count_core.sql") -> None:
    """
    :param view_list_sql: SQL prepared statements
    :param filename: path to output file, default 'count_core.sql' in PWD
    """
    sql_optimizer = concat_view_sql(view_list_sql)
    sql_optimizer = sql_optimizer.replace("ORDER BY cnt desc", "")
    sql_optimizer = sql_optimizer.replace("CREATE or replace VIEW", "CREATE TABLE")

    with open(filename, "w") as fout:
        fout.write(sql_optimizer)


if __name__ == "__main__":
    write_view_sql(
        [
            count_patient(),
            count_encounter("month"),
            count_encounter_type(),
            count_encounter_type("month"),
            count_encounter_enc_type("month"),
            count_encounter_service("month"),
            count_encounter_priority("month"),
        ]
    )
