""" tests for jinja sql templates """

import pytest
from pandas import DataFrame

from cumulus_library.template_sql import base_templates, sql_utils


def test_alias_table():
    expected = """CREATE OR REPLACE VIEW target
AS SELECT * FROM source;"""
    query = base_templates.get_alias_table_query("source", "target")
    assert query == expected


def test_select_all():
    expected = """SELECT * FROM source;"""
    query = base_templates.get_select_all_query("source")
    assert query == expected


def test_codeable_concept_denormalize_all_creation():
    expected = """CREATE TABLE target__concepts AS (
    WITH

    system_code_col_0 AS (
        SELECT DISTINCT
            s.id AS id,
            u.codeable_concept.code AS code,
            u.codeable_concept.display AS display,
            u.codeable_concept.system AS code_system
        FROM
            source AS s,
            UNNEST(s.code_col) AS cc (cc_row),
            UNNEST(cc.cc_row.coding) AS u (codeable_concept)
    ), --noqa: LT07

    union_table AS (
        SELECT
            id,
            code_system,
            code,
            display
        FROM system_code_col_0
        
    )
    SELECT
        id,
        code,
        code_system,
        display
    FROM union_table
);
"""
    config = sql_utils.CodeableConceptConfig(
        source_table="source",
        source_id="id",
        column_name="code_col",
        target_table="target__concepts",
        is_array=True,
    )
    query = base_templates.get_codeable_concept_denormalize_query(config)
    assert query == expected


def test_codeable_concept_denormalize_filter_creation():
    expected = """CREATE TABLE target__concepts AS (
    WITH

    system_code_col_0 AS (
        SELECT DISTINCT
            s.id AS id,
            '0' AS priority,
            u.codeable_concept.code AS code,
            u.codeable_concept.display AS display,
            u.codeable_concept.system AS code_system
        FROM
            source AS s,
            UNNEST(s.code_col.coding) AS u (codeable_concept)
        WHERE
            u.codeable_concept.system LIKE 'http://snomed.info/sct'
    ), --noqa: LT07

    system_code_col_1 AS (
        SELECT DISTINCT
            s.id AS id,
            '1' AS priority,
            u.codeable_concept.code AS code,
            u.codeable_concept.display AS display,
            u.codeable_concept.system AS code_system
        FROM
            source AS s,
            UNNEST(s.code_col.coding) AS u (codeable_concept)
        WHERE
            u.codeable_concept.system LIKE 'http://hl7.org/fhir/sid/icd-10-cm'
    ), --noqa: LT07

    union_table AS (
        SELECT
            id,
            priority,
            code_system,
            code,
            display
        FROM system_code_col_0
        UNION
        SELECT
            id,
            priority,
            code_system,
            code,
            display
        FROM system_code_col_1
        
    ),

    partitioned_table AS (
        SELECT
            id,
            code,
            code_system,
            display,
            priority,
            ROW_NUMBER()
                OVER (
                    PARTITION BY id
                    ORDER BY priority ASC
                ) AS available_priority
        FROM union_table
        GROUP BY id, priority, code_system, code, display
        ORDER BY priority ASC
    )

    SELECT
        id,
        code,
        code_system,
        display
    FROM partitioned_table
    WHERE available_priority = 1
);
"""

    config = sql_utils.CodeableConceptConfig(
        source_table="source",
        source_id="id",
        column_name="code_col",
        target_table="target__concepts",
        is_array=False,
        filter_priority=True,
        code_systems=[
            "http://snomed.info/sct",
            "http://hl7.org/fhir/sid/icd-10-cm",
        ],
    )
    query = base_templates.get_codeable_concept_denormalize_query(config)
    assert query == expected


def test_get_column_datatype_query():
    expected = """SELECT
    column_name,
    data_type
FROM information_schema.columns
WHERE
    table_schema = 'schema_name'
    AND table_name = 'table_name'
    AND LOWER(column_name) IN ('foo', 'bar') --noqa: LT05"""

    query = base_templates.get_column_datatype_query(
        schema_name="schema_name",
        table_name="TABLE_NAME",
        column_names=["foo", "BAR"],
    )
    assert query == expected


def test_create_view_query_creation():
    expected = """CREATE OR REPLACE VIEW test_view AS (
    SELECT * FROM (
        VALUES
        ('foo','foo'),
        ('bar','bar')
    )
        AS t
        ("a","b")
);"""
    query = base_templates.get_create_view_query(
        view_name="test_view",
        dataset=[["foo", "foo"], ["bar", "bar"]],
        view_cols=["a", "b"],
    )
    assert query == expected


@pytest.mark.parametrize(
    "expected,schema,table,cols,types",
    [
        (
            """CREATE TABLE IF NOT EXISTS "test_schema"."test_table"
AS (
    SELECT * FROM (
        VALUES
        (cast(NULL AS varchar),cast(NULL AS varchar))
    )
        AS t ("a","b")
);""",
            "test_schema",
            "test_table",
            ["a", "b"],
            [],
        ),
        (
            """CREATE TABLE IF NOT EXISTS "test_schema"."test_table"
AS (
    SELECT * FROM (
        VALUES
        (cast(NULL AS integer),cast(NULL AS varchar))
    )
        AS t ("a","b")
);""",
            "test_schema",
            "test_table",
            ["a", "b"],
            ["integer", "varchar"],
        ),
    ],
)
def test_ctas_empty_query_creation(expected, schema, table, cols, types):
    query = base_templates.get_ctas_empty_query(
        schema_name=schema, table_name=table, table_cols=cols, table_cols_types=types
    )
    assert query == expected


def test_ctas_query_creation():
    expected = """CREATE TABLE "test_schema"."test_table" AS (
    SELECT * FROM (
        VALUES
        (cast('foo' AS varchar),cast('foo' AS varchar)),
        (cast('bar' AS varchar),cast('bar' AS varchar))
    )
        AS t ("a","b")
);"""
    query = base_templates.get_ctas_query(
        schema_name="test_schema",
        table_name="test_table",
        dataset=[["foo", "foo"], ["bar", "bar"]],
        table_cols=["a", "b"],
    )
    assert query == expected
    query = base_templates.get_ctas_query_from_df(
        schema_name="test_schema",
        table_name="test_table",
        df=DataFrame({"a": ["foo", "bar"], "b": ["foo", "bar"]}),
    )
    assert query == expected


def test_extension_denormalize_creation():
    expected = """CREATE TABLE target_table AS (
    WITH

    system_omb AS (
        SELECT DISTINCT
            s.source_id AS id,
            '0' AS priority,
            'omb' AS system, -- noqa: RF04
            ext_child.ext.valuecoding.code AS prefix_code,
            ext_child.ext.valuecoding.display AS prefix_display
        FROM
            source_table AS s,
            UNNEST(extension) AS ext_parent (ext),
            UNNEST(ext_parent.ext.extension) AS ext_child (ext)
        WHERE
            ext_parent.ext.url = 'fhir_extension'
            AND ext_child.ext.url = 'omb'
            AND ext_child.ext.valuecoding.display != ''
    ),

    system_text AS (
        SELECT DISTINCT
            s.source_id AS id,
            '1' AS priority,
            'text' AS system, -- noqa: RF04
            ext_child.ext.valuecoding.code AS prefix_code,
            ext_child.ext.valuecoding.display AS prefix_display
        FROM
            source_table AS s,
            UNNEST(extension) AS ext_parent (ext),
            UNNEST(ext_parent.ext.extension) AS ext_child (ext)
        WHERE
            ext_parent.ext.url = 'fhir_extension'
            AND ext_child.ext.url = 'text'
            AND ext_child.ext.valuecoding.display != ''
    ),

    union_table AS (
        SELECT
            id,
            priority,
            system,
            prefix_code,
            prefix_display
        FROM system_omb
        UNION
        SELECT
            id,
            priority,
            system,
            prefix_code,
            prefix_display
        FROM system_text
        
        ORDER BY id, priority
    )

    SELECT
        id,
        system,
        prefix_code,
        prefix_display
    FROM (
        SELECT
            id,
            system,
            LOWER(prefix_code) AS prefix_code,
            LOWER(
                prefix_display
            ) AS prefix_display,
            ROW_NUMBER()
                OVER (
                    PARTITION BY id, system
                    ORDER BY priority ASC
                ) AS available_priority
        FROM union_table
        GROUP BY id, system, priority
    )
    WHERE available_priority = 1
);"""
    config = sql_utils.ExtensionConfig(
        "source_table",
        "source_id",
        "target_table",
        "prefix",
        "fhir_extension",
        ["omb", "text"],
    )
    query = base_templates.get_extension_denormalize_query(config)
    assert query == expected
    config = sql_utils.ExtensionConfig(
        "source_table",
        "source_id",
        "target_table",
        "prefix",
        "fhir_extension",
        ["omb", "text"],
        is_array=True,
    )
    query = base_templates.get_extension_denormalize_query(config)
    array_sql = """LOWER(
                ARRAY_JOIN(
                    ARRAY_SORT(
                        ARRAY_AGG(
                            prefix_code
                        )
                    ), '; '
                )
            )
                AS prefix_code,
            LOWER(
                ARRAY_JOIN(
                    ARRAY_SORT(
                        ARRAY_AGG(
                            prefix_display
                        )
                    ), '; '
                )
            ) AS prefix_display,"""
    assert array_sql in query


def test_insert_into_query_creation():
    expected = """INSERT INTO test_table
("a","b")
VALUES
('foo','foo'),
('bar','bar');"""
    query = base_templates.get_insert_into_query(
        table_name="test_table",
        table_cols=["a", "b"],
        dataset=[["foo", "foo"], ["bar", "bar"]],
    )
    assert query == expected
    expected = """INSERT INTO test_table
("a","b")
VALUES
('foo',VARCHAR 'foo'),
('bar',VARCHAR 'bar');"""
    query = base_templates.get_insert_into_query(
        table_name="test_table",
        table_cols=["a", "b"],
        dataset=[["foo", "foo"], ["bar", "bar"]],
        type_casts={"b": "VARCHAR"},
    )
    assert query == expected


def test_is_table_not_empty():
    expected = """SELECT
    field_name
FROM
    table_name
WHERE
    field_name IS NOT NULL
LIMIT 1;"""
    query = base_templates.get_is_table_not_empty_query(
        source_table="table_name", field="field_name"
    )
    assert query == expected

    expected = """SELECT
    field_name
FROM
    table_name,
    UNNEST(t) AS a (b),
    UNNEST(x) AS y (z)
WHERE
    field_name IS NOT NULL
LIMIT 1;"""
    query = base_templates.get_is_table_not_empty_query(
        source_table="table_name",
        field="field_name",
        unnests=[
            {"source_col": "t", "table_alias": "a", "row_alias": "b"},
            {"source_col": "x", "table_alias": "y", "row_alias": "z"},
        ],
    )
    assert query == expected

    expected = """SELECT
    field_name
FROM
    table_name
WHERE
    field_name IS NOT NULL
    AND field_name LIKE 's%' --noqa: LT02
    AND field_name IS NOT NULL --noqa: LT02
LIMIT 1;"""

    query = base_templates.get_is_table_not_empty_query(
        source_table="table_name",
        field="field_name",
        conditions=["field_name LIKE 's%'", "field_name IS NOT NULL"],
    )
    assert query == expected


def test_get_code_system_pairs():
    expected = """CREATE TABLE output_table AS
SELECT DISTINCT
    'hasarray' AS table_name,
    'acol' AS column_name,
    t2.row2.code,
    t2.row2.display,
    t2.row2.system
FROM hasarray,
UNNEST(acol) AS t1 (row1),
UNNEST(t1.row1.coding) AS t2 (row2)
UNION
SELECT DISTINCT
    'noarray' AS table_name,
    'col' AS column_name,
    t.row.code,
    t.row.display,
    t.row.system
FROM noarray,
UNNEST(col.coding) AS t (row)
UNION
SELECT DISTINCT
    'bare' AS table_name,
    'bcol' AS column_name,
    bcol.code,
    bcol.display,
    bcol.system
FROM bare
UNION
SELECT *
FROM (
    VALUES (
        (
            'empty',
            'empty',
            '',
            '',
            ''
        )
    )
)
    AS t (table_name, column_name, code, display, system)"""
    query = base_templates.get_code_system_pairs(
        "output_table",
        [
            {
                "table_name": "hasarray",
                "column_name": "acol",
                "is_bare_coding": False,
                "is_array": True,
                "has_data": True,
            },
            {
                "table_name": "noarray",
                "column_name": "col",
                "is_bare_coding": False,
                "is_array": False,
                "has_data": True,
            },
            {
                "table_name": "bare",
                "column_name": "bcol",
                "is_bare_coding": True,
                "is_array": False,
                "has_data": True,
            },
            {
                "table_name": "empty",
                "column_name": "empty",
                "is_bare_coding": False,
                "is_array": False,
                "has_data": False,
            },
        ],
    )
    assert query == expected
