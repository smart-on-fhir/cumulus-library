"""validates sql output of counts table sql generation"""

import pytest

from cumulus_library import CountAnnotation
from cumulus_library.builders.statistics_templates import counts_templates


def test_col_casts():
    expected = counts_templates.CountColumn(name="foo", db_type="VARCHAR", alias=None)
    assert expected == counts_templates._cast_table_col("foo")
    assert expected == counts_templates._cast_table_col(expected)
    assert expected == counts_templates._cast_table_col(["foo", "VARCHAR", None])
    expected = counts_templates.FilterColumn(name="bar", values=["baz"], include_nulls=True)
    assert expected == counts_templates._cast_filter_col(("bar", ["baz"], True))
    assert expected == counts_templates._cast_filter_col(expected)


@pytest.mark.parametrize(
    "expected,kwargs",
    [
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
        p.cnt_subject_ref >= 10
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
        p.cnt_subject_ref >= 5
);""",
            {
                "filter_resource": False,
                "where_clauses": None,
                "fhir_resource": None,
                "min_subject": 5,
                "annotation": None,
            },
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
        age > 10
        AND sex ==  'F'
        
);""",
            {
                "filter_resource": True,
                "where_clauses": ["age > 10", "sex ==  'F'"],
                "fhir_resource": "encounter",
                "min_subject": None,
                "annotation": None,
            },
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
            p."sex",
            j."A",
            j."y"
    FROM powerset AS p
        JOIN other_table j ON p.code = j.other_field
    WHERE 
        p.cnt_subject_ref >= None
);""",
            {
                "filter_resource": False,
                "where_clauses": None,
                "fhir_resource": "encounter",
                "min_subject": None,
                "annotation": CountAnnotation(
                    field="code",
                    join_table="other_table",
                    join_field="other_field",
                    columns=[("A", "VARCHAR", None), ("B", "VARCHAR", "y")],
                ),
            },
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
        sum(p.cnt_subject_ref) AS cnt,
            j."A",
            j."y"
    FROM powerset AS p
        JOIN other_table j ON p.code = j.other_field
    WHERE 
        p.cnt_subject_ref >= None
    GROUP BY
            j."A",
            j."y"
    ORDER BY A ASC
);""",
            {
                "filter_resource": False,
                "where_clauses": None,
                "fhir_resource": "encounter",
                "min_subject": None,
                "annotation": CountAnnotation(
                    field="code",
                    join_table="other_table",
                    join_field="other_field",
                    columns=[("A", "varchar", None), ("B", "varchar", "y")],
                    alt_target="A",
                ),
            },
        ),
    ],
)
def test_count_query(expected, kwargs):
    query = counts_templates.get_count_query("test_table", "test_source", ["age", "sex"], **kwargs)
    # Snippet for getting updated template output
    # with open("output.sql", "w") as f:
    #     f.write(query)
    assert query == expected
