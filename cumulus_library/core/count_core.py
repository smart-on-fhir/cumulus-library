from cumulus_library_core.schema.columns import ColumnEnum as Column
from cumulus_library_core.schema import counts


def count_core_patient():
    view_name = "count_core_patient"
    from_name = "core_patient"
    cols = [Column.gender, Column.age, Column.race_display, Column.postalcode3]
    return counts.count_patient(view_name, from_name, cols)


def count_core_encounter(duration=Column.start_month):
    view_name = counts.name_view("count_core_encounter", duration)
    from_name = "core_encounter"
    cols = [
        duration,
        Column.enc_class,
        Column.age_at_visit,
        Column.gender,
        Column.race,
        Column.postalcode3,
    ]
    return counts.count_encounter(view_name, from_name, cols)


def count_core_study_period(duration=Column.author_month):
    view_name = counts.name_view("count_core_study_period", duration)
    from_name = "core_study_period"
    cols = ["author_month", "doc_type_display", "gender", "race_display"]
    order = "author_month asc"
    return counts.count_encounter(view_name, from_name, cols, order)


def count_core_condition_icd(duration=Column.cond_month):
    view_name = counts.name_view("count_core_condition_icd")
    from_name = "join_core_condition_icd"
    cols = [duration, Column.cond_code_display, Column.enc_class_code]
    return counts.count_encounter(view_name, from_name, cols)
