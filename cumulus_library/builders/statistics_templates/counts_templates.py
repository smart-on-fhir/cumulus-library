from dataclasses import dataclass
from pathlib import Path

from cumulus_library import errors
from cumulus_library.template_sql import base_templates


@dataclass
class CountColumn:
    name: str
    db_type: str
    alias: str


@dataclass(kw_only=True)
class FilterColumn:
    name: str
    values: list[str]
    include_nulls: bool

    # alias is only included for jinja compatibility reasons with CountColumn
    @property
    def alias(self) -> str | None:
        return None  # pragma: no cover


@dataclass(kw_only=True)
class CountAnnotation:
    """Defines a table to use as a source for annotating a count dataset.

    :keyword field: the field in the powerset source table to use for the join.
        Note: in most cases, this field should not be included in your powerset.
    :keyword join_table: the name of the table to join from. If the table is
        in a different schema, use '"schema"."table' as the format
    :keyword join_field: the column in the table to join on
    :keyword columns: a list of tuples like ('column_name', 'alias' or None) to
        define columns to add to your dataset.
    :keyword alt_target: if present, a column to use from the annotation table
        instead of the one defined in `field` as the primary column in the
        annotated power set. This is intended to be the column representing
        the field of highest granularity, but functionally treats the combination
        of all fields in columns as a unique key.

    """

    field: str
    join_table: str
    join_field: str
    columns: list[str] | list[tuple[str, str, str | None]] | list[CountColumn]
    alt_target: str | None = None


def _cast_table_col(col):
    if isinstance(col, str):
        return CountColumn(name=col, db_type="VARCHAR", alias=None)
    elif isinstance(col, CountColumn):
        return col
    else:
        return CountColumn(name=col[0], db_type=col[1], alias=col[2])


def _cast_filter_col(filter_col):
    if isinstance(filter_col, tuple) or isinstance(filter_col, list):
        return FilterColumn(name=filter_col[0], values=filter_col[1], include_nulls=filter_col[2])
    elif isinstance(filter_col, FilterColumn):
        return filter_col


def get_count_query(
    table_name: str,
    source_table: str,
    table_cols: str | list[list[str] | CountColumn],
    *args,
    min_subject: int = 10,
    where_clauses: list[str] | None = None,
    primary_id: str | None = None,
    secondary_id: str | None = None,
    alt_secondary_join_id: str | None = None,
    secondary_table: str | None = None,
    secondary_cols: list[str] = [],
    patient_link: str | None = None,  # deprecated legacy arg, v6.0.0
    annotation: CountAnnotation | None = None,
    filter_status: bool | None = False,
    filter_cols: list[tuple[str, list[str], bool]] | list[FilterColumn] = [],
    **kwargs,
) -> str:
    """Generates count tables for generating study outputs"""

    if primary_id is None:
        if patient_link:  # pragma: no cover
            primary_id = patient_link
        else:
            primary_id = "subject_ref"
    path = Path(__file__).parent

    # we are going to paper over a couple of dataclass vs dicts and lists interactions here to allow
    # for differing levels of user complexity - but the end goal is to get data out in one form for
    # template management reasons
    table_col_classed = []
    for item in table_cols:
        table_col_classed.append(_cast_table_col(item))

    table_cols = table_col_classed
    if annotation:
        annotation_col_classed = []
        for item in annotation.columns:
            annotation_col_classed.append(_cast_table_col(item))
        annotation.columns = annotation_col_classed
    if filter_status and len(filter_cols) == 0:
        raise errors.CountsBuilderError(  # pragma: no cover
            "When filtering in a CountsBuilder, both 'filter_status' and "
            "'filter_cols' must be supplied."
        )
    filter_cols_classed = []
    if filter_cols:
        for filter_col in filter_cols:
            filter_cols_classed.append(_cast_filter_col(filter_col))
    filter_cols = filter_cols_classed
    query = base_templates.get_template(
        "count",
        path,
        table_name=table_name,
        primary_table=source_table,
        table_cols=table_cols,
        min_subject=min_subject,
        where_clauses=where_clauses,
        primary_id=primary_id,
        secondary_table=secondary_table,
        secondary_id=secondary_id,
        alt_secondary_join_id=alt_secondary_join_id,
        secondary_cols=secondary_cols,
        annotation=annotation,
        filter_status=filter_status,
        filter_cols=filter_cols,
    )
    # workaround for conflicting sqlfluff enforcement
    return query.replace("-- noqa: disable=LT02\n", "")
