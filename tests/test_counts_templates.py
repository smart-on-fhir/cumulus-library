""" validates sql output of counts table sql generation """

import pytest

from cumulus_library.template_sql.statistics.counts_templates import get_count_query


@pytest.mark.parametrize(
    "expected,filter_resource,where_clauses,fhir_resource,min_subject",
    [
        (
            """CREATE TABLE test_table AS (
    WITH
    filtered_table AS (
        SELECT
            subject_ref,
            "age",
            "sex"
        FROM test_source
    ),
    
    null_replacement AS (
        SELECT
            subject_ref,
            coalesce("age", 'missing-or-null') AS "age",
            coalesce("sex", 'missing-or-null') AS "sex"
        FROM filtered_table
    ),

    powerset AS (
        SELECT
            count(DISTINCT subject_ref) AS cnt_subject,
            "age",
            "sex"
        FROM null_replacement
        GROUP BY
            cube(
                "age",
                "sex"
            )
    )

    SELECT
        cnt_subject AS cnt,
        "age",
        "sex"
    FROM powerset
    WHERE 
        cnt_subject >= 10
);""",
            None,
            None,
            None,
            None,
        ),
        (
            """CREATE TABLE test_table AS (
    WITH
    null_replacement AS (
        SELECT
            subject_ref,
            coalesce("age", 'missing-or-null') AS "age",
            coalesce("sex", 'missing-or-null') AS "sex"
        FROM test_source
        
    ),

    powerset AS (
        SELECT
            count(DISTINCT subject_ref) AS cnt_subject,
            "age",
            "sex"
        FROM null_replacement
        GROUP BY
            cube(
                "age",
                "sex"
            )
    )

    SELECT
        cnt_subject AS cnt,
        "age",
        "sex"
    FROM powerset
    WHERE 
        cnt_subject >= 5
);""",
            False,
            None,
            None,
            5,
        ),
        (
            """CREATE TABLE test_table AS (
    WITH
    filtered_table AS (
        SELECT
            subject_ref,
            encounter_ref,
            "age",
            "sex"
        FROM test_source
        WHERE status = 'finished'
    ),
    
    null_replacement AS (
        SELECT
            subject_ref,
            encounter_ref,
            coalesce("age", 'missing-or-null') AS "age",
            coalesce("sex", 'missing-or-null') AS "sex"
        FROM filtered_table
    ),

    powerset AS (
        SELECT
            count(DISTINCT subject_ref) AS cnt_subject,
            count(DISTINCT encounter_ref) AS cnt_encounter,
            "age",
            "sex"
        FROM null_replacement
        GROUP BY
            cube(
                "age",
                "sex"
            )
    )

    SELECT
        cnt_encounter AS cnt,
        "age",
        "sex"
    FROM powerset
    WHERE
        age > 10
        AND sex ==  'F'
        
);""",
            True,
            ["age > 10", "sex ==  'F'"],
            "encounter",
            None,
        ),
    ],
)
def test_count_query(
    expected,
    filter_resource,
    where_clauses,
    fhir_resource,
    min_subject,  # pylint: disable=unused-argument
):
    kwargs = {}
    for kwarg in ["filter_resource", "where_clauses", "fhir_resource", "min_subject"]:
        if eval(kwarg) is not None:  # pylint: disable=eval-used
            kwargs[kwarg] = eval(kwarg)  # pylint: disable=eval-used
    query = get_count_query("test_table", "test_source", ["age", "sex"], **kwargs)
    with open("output.sql", "w", encoding="UTF-8") as f:
        f.write(query)
    assert query == expected
