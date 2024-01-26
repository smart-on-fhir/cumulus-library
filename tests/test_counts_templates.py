""" validates sql output of counts table sql generation """

import pytest

from cumulus_library.template_sql.statistics.counts_templates import get_count_query


@pytest.mark.parametrize(
    "expected,kwargs",
    [
        (
            """CREATE TABLE test_table AS (
    WITH
    filtered_table AS (
        SELECT
            s.subject_ref,
            --noqa: disable=RF03, AL02
            s."age",
            s."sex"
            --noqa: enable=RF03, AL02
        FROM test_source AS s
    ),
    
    null_replacement AS (
        SELECT
            subject_ref,
            coalesce(
                cast(age AS varchar), 
                'cumulus__missing-or-null'
            ) AS age,
            coalesce(
                cast(sex AS varchar), 
                'cumulus__missing-or-null'
            ) AS sex
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
            {},
        ),
        (
            """CREATE TABLE test_table AS (
    WITH
    null_replacement AS (
        SELECT
            subject_ref,
            coalesce(
                cast(age AS varchar), 
                'cumulus__missing-or-null'
            ) AS age,
            coalesce(
                cast(sex AS varchar), 
                'cumulus__missing-or-null'
            ) AS sex
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
            {
                "filter_resource": False,
                "where_clauses": None,
                "fhir_resource": None,
                "min_subject": 5,
            },
        ),
        (
            """CREATE TABLE test_table AS (
    WITH
    filtered_table AS (
        SELECT
            s.subject_ref,
            s.encounter_ref,
            --noqa: disable=RF03, AL02
            s."age",
            s."sex"
            --noqa: enable=RF03, AL02
        FROM test_source AS s
        WHERE s.status = 'finished'
    ),
    
    null_replacement AS (
        SELECT
            subject_ref,
            encounter_ref,
            coalesce(
                cast(age AS varchar), 
                'cumulus__missing-or-null'
            ) AS age,
            coalesce(
                cast(sex AS varchar), 
                'cumulus__missing-or-null'
            ) AS sex
        FROM filtered_table
    ),

    powerset AS (
        SELECT
            count(DISTINCT subject_ref) AS cnt_subject,
            count(DISTINCT encounter_ref) AS cnt_encounter_ref,
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
        cnt_encounter_ref AS cnt,
        "age",
        "sex" 
    FROM powerset
    WHERE
        age > 10
        AND sex ==  'F'
        
);""",
            {
                "filter_resource": True,
                "where_clauses": ["age > 10", "sex ==  'F'"],
                "fhir_resource": "encounter",
                "min_subject": None,
            },
        ),
    ],
)
def test_count_query(expected, kwargs):
    query = get_count_query("test_table", "test_source", ["age", "sex"], **kwargs)
    with open("output.sql", "w") as f:
        f.write(query)
    assert query == expected
