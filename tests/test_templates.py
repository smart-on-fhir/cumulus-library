""" tests for jinja sql templates """
from cumulus_library.template_sql.templates import (
    get_codeable_concept_denormalize_query,
    get_create_view_query,
    get_ctas_query,
    get_insert_into_query,
    get_extension_denormalize_query,
    ExtensionConfig,
)


def test_create_view_query_creation():
    expected = """CREATE OR REPLACE VIEW test_view AS (
    SELECT * FROM (
        VALUES
        ('foo','foo'),
        ('bar','bar')
    )
    AS t -- noqa: L025
    (a,b)
);"""
    query = get_create_view_query(
        view_name="test_view",
        dataset=[["foo", "foo"], ["bar", "bar"]],
        view_cols=["a", "b"],
    )
    print(query)
    assert query == expected


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


def test_codeable_concept_denormalize_creation():
    expected = """CREATE TABLE target__concepts AS (
    WITH

    system_0 AS (
        SELECT DISTINCT
            s.id AS id,
            '0' AS priority,
            u.codeable_concept.code AS code,
            u.codeable_concept.display AS display,
            u.codeable_concept.system AS code_system
        FROM
            source AS s,
            UNNEST(s.code_col.coding) AS u (codeable_concept) --noqa: AL05
        WHERE
            u.codeable_concept.system = 'http://snomed.info/sct'
    ), --noqa: LT07

    system_1 AS (
        SELECT DISTINCT
            s.id AS id,
            '1' AS priority,
            u.codeable_concept.code AS code,
            u.codeable_concept.display AS display,
            u.codeable_concept.system AS code_system
        FROM
            source AS s,
            UNNEST(s.code_col.coding) AS u (codeable_concept) --noqa: AL05
        WHERE
            u.codeable_concept.system = 'http://hl7.org/fhir/sid/icd-10-cm'
    ), --noqa: LT07

    union_table AS (
        SELECT
            id,
            priority,
            code_system,
            code,
            display
        FROM system_0
        UNION
        SELECT
            id,
            priority,
            code_system,
            code,
            display
        FROM system_1
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
);"""
    query = get_codeable_concept_denormalize_query(
        "source",
        "code_col",
        "target__concepts",
        ["http://snomed.info/sct", "http://hl7.org/fhir/sid/icd-10-cm"],
    )
    print(query)
    assert query == expected
