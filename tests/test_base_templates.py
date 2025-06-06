"""tests for jinja sql templates"""

from contextlib import nullcontext as does_not_raise

import pytest
from pandas import DataFrame

from cumulus_library import db_config, errors
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


@pytest.mark.parametrize(
    "db_type,flattening",
    [
        (
            "athena",
            """SELECT
            t.id AS id,
            row,
            r."code_col"
        FROM
            source AS t,
            UNNEST(t."code_col") WITH ORDINALITY AS r ("code_col", row)""",
        ),
        (
            "duckdb",
            """WITH
        data_and_row_num AS (
            SELECT
                t.id AS id,
                generate_subscripts(t."code_col", 1) AS row,
                UNNEST(t."code_col") AS "code_col" -- must unnest in SELECT here
            FROM source AS t
        )
        SELECT
            id,
            row,
            "code_col"
        FROM data_and_row_num""",
        ),
    ],
)
def test_codeable_concept_denormalize_all_creation(db_type, flattening):
    db_config.db_type = db_type
    expected = f"""CREATE TABLE target__concepts AS (
    WITH

    flattened_rows AS (
        {flattening}
    ),

    system_code_col_0 AS (
        SELECT DISTINCT
            s.id AS id,
            s.row,
            u.coding.code,
            u.coding.display,
            u.coding.system,
            u.coding.userSelected
        FROM
            flattened_rows AS s,
            UNNEST(s.code_col.coding) AS u (coding)
    ), --noqa: LT07

    union_table AS (
        SELECT
            id,
            row,
            system,
            code,
            display,
            userSelected
        FROM system_code_col_0
        
    )
    SELECT
        id,
        row,
        code,
        system,
        display,
        userSelected
    FROM union_table
);
"""
    config = sql_utils.CodeableConceptConfig(
        source_table="source",
        source_id="id",
        column_hierarchy=[("code_col", list)],
        target_table="target__concepts",
    )
    query = base_templates.get_codeable_concept_denormalize_query(config)
    assert query == expected


def test_codeable_concept_denormalize_filter_creation():
    # fmt: off
    expected = r"""CREATE TABLE target__concepts AS (
    WITH

    system_code_col_0 AS (
        SELECT DISTINCT
            s.id AS id,
            0 AS row,
            '0' AS priority,
            u.coding.code,
            u.coding.display,
            u.coding.system,
            u.coding.userSelected
        FROM
            source AS s,
            UNNEST(s.code_col.coding) AS u (coding)
        WHERE
            REGEXP_LIKE(u.coding.system, '^http://snomed\.info/sct$')
    ), --noqa: LT07

    system_code_col_1 AS (
        SELECT DISTINCT
            s.id AS id,
            0 AS row,
            '1' AS priority,
            u.coding.code,
            u.coding.display,
            u.coding.system,
            u.coding.userSelected
        FROM
            source AS s,
            UNNEST(s.code_col.coding) AS u (coding)
        WHERE
            REGEXP_LIKE(u.coding.system, '^http://hl7\.org/fhir/sid/icd-10-cm$')
    ), --noqa: LT07

    system_code_col_2 AS (
        SELECT DISTINCT
            s.id AS id,
            0 AS row,
            '2' AS priority,
            u.coding.code,
            u.coding.display,
            u.coding.system,
            u.coding.userSelected
        FROM
            source AS s,
            UNNEST(s.code_col.coding) AS u (coding)
        WHERE
            REGEXP_LIKE(u.coding.system, '^https://fhir\.cerner\.com/.*/codeSet/71$')
    ), --noqa: LT07

    union_table AS (
        SELECT
            id,
            row,
            priority,
            system,
            code,
            display,
            userSelected
        FROM system_code_col_0
        UNION
        SELECT
            id,
            row,
            priority,
            system,
            code,
            display,
            userSelected
        FROM system_code_col_1
        UNION
        SELECT
            id,
            row,
            priority,
            system,
            code,
            display,
            userSelected
        FROM system_code_col_2
        
    ),

    partitioned_table AS (
        SELECT
            id,
            row,
            code,
            system,
            display,
            userSelected,
            priority,
            ROW_NUMBER()
                OVER (
                    PARTITION BY id
                    ORDER BY priority ASC, code ASC
                ) AS available_priority
        FROM union_table
        GROUP BY
            id, row, priority, system, code, display, userSelected
    )

    SELECT
        id,
        code,
        system,
        display,
        userSelected
    FROM partitioned_table
    WHERE available_priority = 1
);
"""
    # fmt: on

    config = sql_utils.CodeableConceptConfig(
        source_table="source",
        source_id="id",
        column_hierarchy=[("code_col", dict)],
        target_table="target__concepts",
        filter_priority=True,
        code_systems=[
            "http://snomed.info/sct",
            "http://hl7.org/fhir/sid/icd-10-cm",
            "https://fhir.cerner.com/%/codeSet/71",
        ],
    )
    query = base_templates.get_codeable_concept_denormalize_query(config)
    assert query == expected


def test_codeable_concept_denormalize_error():
    with pytest.raises(errors.CumulusLibraryError):
        config = sql_utils.CodeableConceptConfig(
            source_table="source",
            source_id="id",
            column_hierarchy=[("code_col", dict), ("code_col", dict), ("code_col", dict)],
            target_table="target__concepts",
            filter_priority=True,
            code_systems=[
                "http://snomed.info/sct",
                "http://hl7.org/fhir/sid/icd-10-cm",
                "https://fhir.cerner.com/%/codeSet/71",
            ],
        )
        base_templates.get_codeable_concept_denormalize_query(config)


def test_get_coding_denormalize_query():
    expected = """CREATE TABLE core__documentreference_dn_format AS (
    WITH

    system_format_0 AS (
        SELECT DISTINCT
            s.id AS id,
            u.parent_col.format.code,
            u.parent_col.format.display,
            u.parent_col.format.system
        FROM
            documentreference AS s,
            UNNEST(s.content) AS u (parent_col)
    ), --noqa: LT07

    union_table AS (
        SELECT
            id,
            system,
            code,
            display
        FROM system_format_0
        
    )
    SELECT
        id,
        code,
        system,
        display
    FROM union_table
);
"""
    config = sql_utils.CodingConfig(
        source_table="documentreference",
        source_id="id",
        column_hierarchy=[("content", list), ("format", dict)],
        target_table="core__documentreference_dn_format",
        expected={"format": sql_utils.CODING},
    )
    query = base_templates.get_coding_denormalize_query(config)
    assert query == expected
    with pytest.raises(errors.CumulusLibraryError):
        config = sql_utils.CodingConfig(
            source_table="documentreference",
            source_id="id",
            column_hierarchy=[("content", list)],
            target_table="core__documentreference_dn_format",
            expected={"format": sql_utils.CODING},
        )
        base_templates.get_coding_denormalize_query(config)


def test_get_column_datatype_query():
    expected = """SELECT
    column_name,
    data_type
FROM information_schema.columns
WHERE
    table_schema = 'schema_name'
    AND table_name IN ('table_name')"""

    query = base_templates.get_column_datatype_query(
        schema_name="schema_name",
        table_names="TABLE_NAME",
    )
    assert query == expected
    expected = """SELECT
    column_name,
    data_type,
    table_name --noqa: LT02
FROM information_schema.columns
WHERE
    table_schema = 'schema_name'
    AND table_name IN ('table_name')
    AND LOWER(column_name) IN ('foo', 'bar') --noqa: LT02,LT05"""

    query = base_templates.get_column_datatype_query(
        schema_name="schema_name",
        table_names="TABLE_NAME",
        column_names=["foo", "BAR"],
        include_table_names=True,
    )
    assert query == expected


@pytest.mark.parametrize(
    "tables,table_aliases,column_aliases,distinct,expected,raises",
    [
        (
            ["table_a", "view_b"],
            None,
            None,
            False,
            """CREATE TABLE IF NOT EXISTS table AS
SELECT
    a.foo,
    b.bar,
    b.baz
FROM table_a AS a,
    view_b AS b
WHERE
    b.bar = a.foo
    AND b.baz != a.foo""",
            does_not_raise(),
        ),
        (
            ["table_a", "view_b"],
            None,
            None,
            True,
            """CREATE TABLE IF NOT EXISTS table AS
SELECT DISTINCT
    a.foo,
    b.bar,
    b.baz
FROM table_a AS a,
    view_b AS b
WHERE
    b.bar = a.foo
    AND b.baz != a.foo""",
            does_not_raise(),
        ),
        (
            ["table_a", "view_b"],
            ["b", "c"],
            {"b.baz": "foobar"},
            False,
            """CREATE TABLE IF NOT EXISTS table AS
SELECT
    a.foo,
    b.bar,
    b.baz AS foobar
FROM table_a AS b,
    view_b AS c
WHERE
    b.bar = a.foo
    AND b.baz != a.foo""",
            does_not_raise(),
        ),
        (["table_a"], [], {}, False, "", pytest.raises(errors.CumulusLibraryError)),
    ],
)
def test_create_table_from_tables(
    tables, table_aliases, column_aliases, distinct, expected, raises
):
    with raises:
        query = base_templates.get_create_table_from_tables(
            table_name="table",
            tables=tables,
            table_aliases=table_aliases,
            columns=["a.foo", "b.bar", "b.baz"],
            column_aliases=column_aliases,
            join_clauses=["b.bar = a.foo", "b.baz != a.foo"],
            distinct=distinct,
        )
        assert query == expected


def test_get_create_table_from_union():
    expected = """CREATE TABLE union_table AS -- noqa: disable=LT02
SELECT
    foo,
    bar,
    baz
FROM table_a
UNION
SELECT
    foo,
    bar,
    baz
FROM view_b
"""
    query = base_templates.get_create_table_from_union(
        table_name="union_table",
        tables=["table_a", "view_b"],
        columns=["foo", "bar", "baz"],
    )
    assert query == expected


@pytest.mark.parametrize(
    "tables,table_aliases,column_aliases,distinct,expected,raises",
    [
        (
            ["table_a", "view_b"],
            None,
            None,
            False,
            """CREATE OR REPLACE VIEW view AS
SELECT
    a.foo,
    b.bar,
    b.baz
FROM table_a AS a,
    view_b AS b
WHERE
    b.bar = a.foo
    AND b.baz != a.foo""",
            does_not_raise(),
        ),
        (
            ["table_a", "view_b"],
            None,
            None,
            True,
            """CREATE OR REPLACE VIEW view AS
SELECT DISTINCT
    a.foo,
    b.bar,
    b.baz
FROM table_a AS a,
    view_b AS b
WHERE
    b.bar = a.foo
    AND b.baz != a.foo""",
            does_not_raise(),
        ),
        (
            ["table_a", "view_b"],
            ["b", "c"],
            {"b.baz": "foobar"},
            False,
            """CREATE OR REPLACE VIEW view AS
SELECT
    a.foo,
    b.bar,
    b.baz AS foobar
FROM table_a AS b,
    view_b AS c
WHERE
    b.bar = a.foo
    AND b.baz != a.foo""",
            does_not_raise(),
        ),
        (["table_a"], [], {}, False, "", pytest.raises(errors.CumulusLibraryError)),
    ],
)
def test_create_view_from_tables(tables, table_aliases, column_aliases, distinct, expected, raises):
    with raises:
        query = base_templates.get_create_view_from_tables(
            view_name="view",
            tables=tables,
            table_aliases=table_aliases,
            columns=["a.foo", "b.bar", "b.baz"],
            column_aliases=column_aliases,
            join_clauses=["b.bar = a.foo", "b.baz != a.foo"],
            distinct=distinct,
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
    WHERE 1 = 0 -- ensure empty table
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
    WHERE 1 = 0 -- ensure empty table
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


@pytest.mark.parametrize(
    "expected,db_type,schema,table,cols,remote_types",
    [
        (
            """CREATE EXTERNAL TABLE IF NOT EXISTS `test_athena`.`remote_table` ( a String, b Int
)
STORED AS PARQUET
LOCATION 's3://bucket/data/'
tblproperties ("parquet.compression"="SNAPPY");""",
            "athena",
            "test_athena",
            "remote_table",
            ["a", "b"],
            ["String", "Int"],
        ),
        (
            """CREATE TABLE IF NOT EXISTS "test_duckdb"."local_table" AS SELECT "a", "b"
FROM read_parquet('./tests/test_data/*.parquet')""",
            "duckdb",
            "test_duckdb",
            "local_table",
            ["a", "b"],
            ["String", "Int"],
        ),
    ],
)
def test_ctas_from_parquet(expected, db_type, schema, table, cols, remote_types):
    db_config.db_type = db_type
    query = base_templates.get_ctas_from_parquet_query(
        schema_name=schema,
        table_name=table,
        local_location="./tests/test_data/*.parquet",
        remote_location="s3://bucket/data/",
        table_cols=cols,
        remote_table_cols_types=remote_types,
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
            UNNEST(s.extension) AS ext_parent (ext),
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
            UNNEST(s.extension) AS ext_parent (ext),
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
                    PARTITION BY id
                    ORDER BY priority ASC
                ) AS available_priority
        FROM union_table
        GROUP BY id, system, priority
    )
    WHERE available_priority = 1
);"""
    config = sql_utils.ExtensionConfig(
        source_table="source_table",
        source_id="source_id",
        target_table="target_table",
        target_col_prefix="prefix",
        fhir_extension="fhir_extension",
        ext_systems=["omb", "text"],
    )
    query = base_templates.get_extension_denormalize_query(config)
    assert query == expected
    config = sql_utils.ExtensionConfig(
        source_table="source_table",
        source_id="source_id",
        target_table="target_table",
        target_col_prefix="prefix",
        fhir_extension="fhir_extension",
        ext_systems=["omb", "text"],
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
    expected = """INSERT INTO "test"."test_table"
("a","b")
VALUES
('foo','foo'),
('bar','bar');"""
    query = base_templates.get_insert_into_query(
        schema="test",
        table_name="test_table",
        table_cols=["a", "b"],
        dataset=[["foo", "foo"], ["bar", "bar"]],
    )
    assert query == expected
    expected = """INSERT INTO "test"."test_table"
("a","b")
VALUES
('foo',VARCHAR 'foo'),
('bar',VARCHAR 'bar');"""
    query = base_templates.get_insert_into_query(
        schema="test",
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
