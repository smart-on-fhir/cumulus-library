""" validates sql output of psm table sql generation """

from contextlib import nullcontext as does_not_raise

import pytest

from cumulus_library.errors import CumulusLibraryError
from cumulus_library.template_sql.statistics.psm_templates import (
    get_distinct_ids,
    get_create_covariate_table,
)


@pytest.mark.parametrize(
    "columns,source_table,join_id,filter_table,expected,raises",
    [
        (
            ["a", "b"],
            "source",
            "ref_id",
            "filter",
            """SELECT DISTINCT
    "source"."a",
    "source"."b"
FROM source
WHERE
    "source"."ref_id" NOT IN (
        SELECT "filter"."ref_id"
        FROM filter
    )""",
            does_not_raise(),
        ),
        (
            ["a", "b"],
            "source",
            None,
            None,
            """SELECT DISTINCT
    "source"."a",
    "source"."b"
FROM source""",
            does_not_raise(),
        ),
        (["a", "b"], "source", "ref_id", None, "", pytest.raises(CumulusLibraryError)),
    ],
)
def test_get_distinct_ids(
    columns, source_table, join_id, filter_table, expected, raises
):
    with raises:
        query = get_distinct_ids(columns, source_table, join_id, filter_table)
        assert query == expected


@pytest.mark.parametrize(
    "target,pos_source,neg_source,table_suffix,primary_ref,dep_var,"
    "join_cols_by_table,count_ref,count_table,expected,raises",
    [
        (
            "target",
            "pos_table",
            "neg_table",
            "2024_01_01_11_11_11",
            "subject_id",
            "has_flu",
            {},
            None,
            None,
            """CREATE TABLE target AS (
    SELECT DISTINCT
        sample_cohort."subject_id",
        sample_cohort."has_flu",
        neg_table.code
    FROM "pos_table_sampled_ids_2024_01_01_11_11_11" AS sample_cohort,
        "neg_table",
    WHERE
        sample_cohort."subject_id" = "neg_table"."subject_id"
    ORDER BY sample_cohort."subject_id"
)""",
            does_not_raise(),
        ),
        (
            "target",
            "pos_table",
            "neg_table",
            "2024_01_01_11_11_11",
            "subject_id",
            "has_flu",
            {
                "join_table": {
                    "join_id": "enc_ref",
                    "included_cols": [["a"], ["b", "c"]],
                }
            },
            "enc_ref",
            "join_table",
            """CREATE TABLE target AS (
    SELECT DISTINCT
        sample_cohort."subject_id",
        sample_cohort."has_flu",
        (
            SELECT COUNT(DISTINCT subject_id)
            FROM "join_table"
            WHERE sample_cohort."enc_ref" = "join_table"."enc_ref"
        ) AS instance_count,
        "join_table"."a",
        "join_table"."b" AS "c",
        neg_table.code
    FROM "pos_table_sampled_ids_2024_01_01_11_11_11" AS sample_cohort,
        "neg_table",
        "join_table"
    WHERE
        sample_cohort."subject_id" = "neg_table"."subject_id"
    AND sample_cohort."enc_ref"
    = "join_table"."enc_ref"
    ORDER BY sample_cohort."subject_id"
)""",
            does_not_raise(),
        ),
        (
            "target",
            "pos_table",
            "neg_table",
            "2024_01_01_11_11_11",
            "subject_id",
            "has_flu",
            {},
            "join_table",
            None,
            "",
            pytest.raises(CumulusLibraryError),
        ),
    ],
)
def test_create_covariate_table(
    target,
    pos_source,
    neg_source,
    table_suffix,
    primary_ref,
    dep_var,
    join_cols_by_table,
    count_ref,
    count_table,
    expected,
    raises,
):
    with raises:
        query = get_create_covariate_table(
            target,
            pos_source,
            neg_source,
            table_suffix,
            primary_ref,
            dep_var,
            join_cols_by_table,
            count_ref,
            count_table,
        )
        assert query == expected
