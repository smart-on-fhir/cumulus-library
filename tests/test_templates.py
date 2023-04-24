import pytest
from cumulus_library.templates import get_ctas_query, get_insert_into_query


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
