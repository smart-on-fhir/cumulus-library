""" validates sql output of counts table sql generation """

import pytest

from cumulus_library.statistics.statistics_templates import counts_templates


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
                'cumulus__none'
            ) AS age,
            coalesce(
                cast(sex AS varchar),
                'cumulus__none'
            ) AS sex
        FROM filtered_table
    ),

    powerset AS (
        SELECT
            count(DISTINCT subject_ref) AS cnt_subject_ref,
            "age",
            "sex",
            concat_ws(
                '-',
                COALESCE("age",''),
                COALESCE("sex",'')
            ) AS id
        FROM null_replacement
        GROUP BY
            cube(
            "age",
            "sex"
            )
    )

    SELECT
        p.cnt_subject_ref AS cnt,
        p."age",
        p."sex"
    FROM powerset AS p
    WHERE 
        cnt_subject_ref >= 10
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
                'cumulus__none'
            ) AS age,
            coalesce(
                cast(sex AS varchar),
                'cumulus__none'
            ) AS sex
        FROM test_source
        
    ),

    powerset AS (
        SELECT
            count(DISTINCT subject_ref) AS cnt_subject_ref,
            "age",
            "sex",
            concat_ws(
                '-',
                COALESCE("age",''),
                COALESCE("sex",'')
            ) AS id
        FROM null_replacement
        GROUP BY
            cube(
            "age",
            "sex"
            )
    )

    SELECT
        p.cnt_subject_ref AS cnt,
        p."age",
        p."sex"
    FROM powerset AS p
    WHERE 
        cnt_subject_ref >= 5
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
                'cumulus__none'
            ) AS age,
            coalesce(
                cast(sex AS varchar),
                'cumulus__none'
            ) AS sex
        FROM filtered_table
    ),
    secondary_powerset AS (
        SELECT
            count(DISTINCT encounter_ref) AS cnt_encounter_ref,
            "age",
            "sex",
            concat_ws(
                '-',
                COALESCE("age",''),
                COALESCE("sex",'')
            ) AS id
        FROM null_replacement
        GROUP BY
            cube(
            "age",
            "sex"
            )
    ),

    powerset AS (
        SELECT
            count(DISTINCT subject_ref) AS cnt_subject_ref,
            "age",
            "sex",
            concat_ws(
                '-',
                COALESCE("age",''),
                COALESCE("sex",'')
            ) AS id
        FROM null_replacement
        GROUP BY
            cube(
            "age",
            "sex"
            )
    )

    SELECT
        s.cnt_encounter_ref AS cnt,
        p."age",
        p."sex"
    FROM powerset AS p
    JOIN secondary_powerset AS s on s.id = p.id
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
    query = counts_templates.get_count_query(
        "test_table", "test_source", ["age", "sex"], **kwargs
    )
    # Snippet for getting updated template output
    # with open("output.sql", "w") as f:
    #     f.write(query)
    assert query == expected
