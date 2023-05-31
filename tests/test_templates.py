""" tests for jinja sql templates """
from cumulus_library.template_sql.templates import (
    get_ctas_query,
    get_insert_into_query,
    get_extension_denormalize_query,
    ExtensionConfig,
)


def test_ctas_query_creation():
    expected = """CREATE TABLE "test_schema"."test_table" AS (
    SELECT * FROM (
        VALUES
        ((cast('foo' AS varchar),cast('foo' AS varchar))),
        ((cast('bar' AS varchar),cast('bar' AS varchar)))
    )
    AS t -- noqa: L025
    (a,b)
);"""
    query = get_ctas_query(
        schema_name="test_schema",
        table_name="test_table",
        dataset=[["foo", "foo"], ["bar", "bar"]],
        table_cols=["a", "b"],
    )
    print(query)
    assert query == expected


def test_insert_into_query_creation():
    expected = """INSERT INTO test_table
(a,b)
VALUES
(('foo','foo')),
(('bar','bar'));"""
    query = get_insert_into_query(
        table_name="test_table",
        table_cols=["a", "b"],
        dataset=[["foo", "foo"], ["bar", "bar"]],
    )
    assert query == expected


def test_extension_denormalize_creation():
    expected = """CREATE TABLE target_table AS (
    WITH

    system_omb AS (
        SELECT DISTINCT
            s.id,
            '0' AS priority,
            'omb' AS system, -- noqa: RF04
            ext_child.ext.valuecoding.code AS prefix_code,
            ext_child.ext.valuecoding.display AS prefix_display
        FROM
            source_table AS s,
            UNNEST(extension) AS ext_parent (ext), --noqa: AL05
            UNNEST(ext_parent.ext.extension) AS ext_child (ext) --noqa: AL05
        WHERE
            ext_parent.ext.url = 'fhir_extension'
            AND ext_child.ext.url = 'omb'
            AND ext_child.ext.valuecoding.display != ''
    ), --noqa: LT07

    system_text AS (
        SELECT DISTINCT
            s.id,
            '1' AS priority,
            'text' AS system, -- noqa: RF04
            ext_child.ext.valuecoding.code AS prefix_code,
            ext_child.ext.valuecoding.display AS prefix_display
        FROM
            source_table AS s,
            UNNEST(extension) AS ext_parent (ext), --noqa: AL05
            UNNEST(ext_parent.ext.extension) AS ext_child (ext) --noqa: AL05
        WHERE
            ext_parent.ext.url = 'fhir_extension'
            AND ext_child.ext.url = 'text'
            AND ext_child.ext.valuecoding.display != ''
    ), --noqa: LT07

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
            ARRAY_AGG(prefix_code) AS prefix_code,
            ARRAY_AGG(
                prefix_display
            ) AS prefix_display,
            ROW_NUMBER()
            OVER (
                PARTITION BY id, system
            ) AS available_priority
        FROM union_table
        GROUP BY id, system
    )
    WHERE available_priority = 1
);"""
    config = ExtensionConfig(
        "source_table",
        "source_id",
        "target_table",
        "prefix",
        "fhir_extension",
        ["omb", "text"],
    )
    query = get_extension_denormalize_query(config)
    assert query == expected
