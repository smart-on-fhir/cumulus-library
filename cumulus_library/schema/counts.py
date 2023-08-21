from typing import Union

from cumulus_library.schema.valueset import DurationUnits
from cumulus_library.schema.columns import ColumnEnum as Column
from cumulus_library.template_sql import templates
from cumulus_library.errors import LibraryError


def get_table_name(study_prefix: str, table_name: str, duration=None) -> str:
    if duration:
        return f"{study_prefix}__{table_name}_{duration}"
    else:
        return f"{study_prefix}__{table_name}"


def get_where_clauses(
    clause: Union[list, str, None] = None, min_subject: int = 10
) -> str:
    print(clause)
    print(min_subject)
    if clause is None:
        return [f"cnt_subject >= {min_subject}"]
    elif isinstance(clause, str):
        return [clause]
    elif isinstance(clause, list):
        return clause
    else:
        raise LibraryError(f"get_where_clauses invalid clause {clause}")


def get_count_query(
    table_name: str, source_table: str, table_cols: list, **kwargs
) -> str:
    if not table_name or not source_table or not table_cols:
        raise LibraryError(
            "count_query missing required arguments. " f"output table: {table_name}"
        )
    for key in kwargs:
        if key not in ["min_subject", "where_clauses", "cnt_encounter"]:
            raise LibraryError(f"count_query recieved unexpected key: {key}")
    return templates.get_count_query(table_name, source_table, table_cols, **kwargs)


def count_patient(
    view_name: str,
    from_table: str,
    table_cols: list,
    where_clauses=None,
) -> str:
    return get_count_query(
        view_name, from_table, table_cols, where_clauses=where_clauses
    )


def count_encounter(
    view_name: str, from_table: str, table_cols: list, where_clauses=None
) -> str:
    return get_count_query(
        view_name,
        from_table,
        table_cols,
        where_clauses=where_clauses,
        cnt_encounter=True,
    )
