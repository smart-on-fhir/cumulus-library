# pylint: disable=W,C,R
from cumulus_library.schema.valueset import DurationUnits
from cumulus_library.schema.columns import ColumnEnum as Column
from cumulus_library.errors import LibraryError

##################################################
# Google Style Guide
#
# https://google.github.io/styleguide/pyguide.html
# Design by composition composition for
#
# Query Grammar (in progress)
#
# Functions = support iterable lists/types
# Objects = typesafety only
##################################################


def drop(table_or_view) -> str:
    return f"DROP table if exists {table_or_view};"


def create_view(view_name) -> str:
    return f"CREATE or replace VIEW {view_name} AS "


def name_view(count_from_table: str, duration_col=None):
    if duration_col:
        return f"{count_from_table}_{as_duration(duration_col).name}"
    else:
        return f"{count_from_table}"


def as_duration(col: str) -> DurationUnits:
    if "_months" in col:
        return DurationUnits.months
    elif "_weeks" in col:
        return DurationUnits.weeks
    elif "_days" in col:
        return DurationUnits.days


def str_columns(cols) -> str:
    if isinstance(cols, str):
        return cols
    if isinstance(cols, Column):
        return cols.name
    if isinstance(cols, list):
        targets = [str_columns(c) for c in cols]
        return ", ".join(targets)


def where_clauses(clause=None, min_subject=10) -> str:
    if not clause:
        return f"cnt_subject >= {min_subject}"
    elif isinstance(clause, str):
        return clause
    elif isinstance(clause, list):
        return str_columns(clause) + ", cnt desc"
    else:
        raise LibraryError(f"where_sql() invalid clause {clause}")


def order_by_sql(order=None, cnt_desc=True) -> str:
    if not order:
        if cnt_desc:
            return "cnt desc;"
    elif isinstance(order, str):
        return order
    elif isinstance(order, list):
        if cnt_desc:
            return str_columns(order) + ", cnt desc;"
        else:
            return str_columns(order) + ";"


def count_patient(
    view_name: str, from_table: str, cols_object, where=None, order_by=None
) -> str:
    return count_query(
        view_name, from_table, cols_object, where, order_by, cnt_encounter=False
    )


def count_encounter(
    view_name: str, from_table: str, cols_object, where=None, order_by=None
) -> str:
    return count_query(
        view_name, from_table, cols_object, where, order_by, cnt_encounter=True
    )


def count_query(
    view_name: str,
    from_table: str,
    cols_object,
    where=None,
    order_by=None,
    cnt_encounter=None,
) -> str:
    cols = str_columns(cols_object)

    cnt_subject = "count(distinct subject_ref)   as cnt_subject"

    if cnt_encounter:
        cnt_encounter = ", count(distinct encounter_ref)   as cnt_encounter"
    else:
        cnt_encounter = False

    return f"""
    {create_view(view_name)}
    with powerset as
    (
        select
        {cnt_subject}
        {cnt_encounter if cnt_encounter else ''}
        , {cols}        
        FROM {from_table}
        group by CUBE
        ( {cols} )
    )
    select
          {'cnt_encounter ' if cnt_encounter else 'cnt_subject'} as cnt 
        , {cols}
    from powerset 
    WHERE {where_clauses(where)} 
    ORDER BY {order_by_sql(order_by)}
    """.strip()
