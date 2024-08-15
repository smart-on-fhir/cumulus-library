---
title: SQL best practices
parent: Library
nav_order: 5
# audience: clinical researcher or engineer familiar with project
# type: reference
---

This doc contains low-level advice on SQL patterns that will help
avoid errors and make queries more performant.

# General Syntax

Our primary target is
[Amazon Athena](https://aws.amazon.com/athena/),
which is based off of
[Trino](https://trino.io/);
Searching for topics on either of these DBs is a good way to debug syntax issues
not mentioned the scope of this document. The 
[Trino functions reference](https://trino.io/docs/current/functions.html)
is particularly useful.

We use
[DuckDB](https://duckdb.org/)
for our unit tests, and it could be used as a datastore locally if desired. If there
is a difference between DuckDb and Athena/Trino, we patch DuckDB with a
[user defined function](https://duckdb.org/docs/api/python/function.html).
You can see some examples of this in the
[DuckDB DatabaseBackend](https://github.com/smart-on-fhir/cumulus-library/blob/main/cumulus_library/databases/duckdb.py#L37).
Hopefully you will not have to do this. If you do, we are probably interested
in supporting it, so please reach out and let us know.

## Dates

FHIR dates/times come across the wires as ISO 8061-formatted strings.
To get them into actual timestamps, use the `from_is08061_timestamp` function as
part of your query, and case to either `date` or `timestamp`.If you want to get a
specific portion of a date, like a month or year, use the `date_trunc` function. 
An example use case:

```sql
 SELECT 
        date(from_iso8601_timestamp(dr.date)) AS doc_date,
        date_trunc('day', date(from_iso8601_timestamp(dr."context"."period"."start")))
            AS author_day,
    FROM documentreference AS dr
```

## Arrays

For 90% of use cases, avoid array handling entirely! The `core` study exists to shield
you from this. This is only relevant if you're doing something that is fussy, or if you
have a reason to work from raw FHIR objects in the database.

FHIR objects are deeply nested, and usually we want to unpack these values to get them
into 2D tables. The ETL has done a lot of work already to make this safe for you, but it's
not perfect - we need the database to have a complete schema for a nested object, and the
ETL will fill this out for two levels of depth, but there's some cases where we just can't
guess how deep of a schema we need to prepopulate (extension can recurse indefinetely). In
cases where we do go deep, the ETL will try to infer the schema from the data. This can cause
headaches if someone else tries to run your study, since they may have different data.

So - if you're going deep into the data model, you may want to check `information_schema.columns`
for the field in question, and verify that the field actually exists, before doing any
additional query. This also means that generally for raw FHIR traversal, you should use the
[TableBuilder pattern](https://docs.smarthealthit.org/cumulus/library/creating-sql-with-python.html#working-with-tablebuilders)
so you have access to boolean logic.

There is good 
[documentation](https://docs.aws.amazon.com/athena/latest/ug/querying-arrays.html)
for querying arrays in athena, but the following functions are particularly useful:

- [unnest](https://docs.aws.amazon.com/athena/latest/ug/filtering-with-unnest.html)
will break out an array into a table, where keys are the column names and the rows
are the values in arrays
- [array_sort](https://docs.aws.amazon.com/athena/latest/ug/sorting-arrays.html)
does exactly what you'd think - provides the contents of a list in alpha sorted order
- [array_join](https://docs.aws.amazon.com/athena/latest/ug/converting-arrays-to-strings.html)
lets you convert arrays into a string, joined by the delimiter of your choice
- [array_agg](https://docs.aws.amazon.com/athena/latest/ug/arrays-and-aggregation.html)
lets you concatenate together arrays. This can be very slow, so use with caution

## Cubes

Cubing tables before export is central to our strategy for masking PII. We provide a 
[count builder](https://github.com/smart-on-fhir/cumulus-library/blob/main/cumulus_library/statistics/counts.py)
specifically to manage the details of this for you, but you can implement this yourself
if you prefer using the
[cube operator](https://trino.io/docs/current/sql/select.html#cube).

Importantly - make sure the table(s) you're using for your cube do not contain explicit
null values, since these are indistinguishable from the nulls created when cubing outputs.
Instead,
[coalesce](https://trino.io/docs/current/functions/conditional.html#coalesce)
fields to some static value, like 'None', if required.

# Performance

There are several best practices outlined in the
[Athena performance docs](https://docs.aws.amazon.com/athena/latest/ug/performance-tuning.html#performance-tuning-query-optimization-techniques)
that you can use to try and improve query performance, but we'll call out several patterns
here that we have found useful for improving performance in our specific use case.

## Split complex queries out into simpler chained subqueries

Athena (and most other large database systems) use a hive-style map-reduce algorithm to
run queries across several nodes to improve throughput. If the query analysis can't identify
a good way to farm out a query, it will run that query on a single node, which can take 
significantly longer

It is significantly faster to chain together simpler subqueries. In one example, using a
NOT IN clause to compare the contents of a column to a list of ~300 words, the complex
query took over an hour, while chaining 300 subqueries together with a UNION clause
took around 40 seconds, so its worth really leaning in to atomization (or writing
templates that will create more atomic queries, more on that below) if you start seeing
long run times. Try to limit number of joins, array operations, and aggregation inside
a single query.

## Limit group by/order by usage

The group by/limit by operation tends to be confined to a single node for execution, and
may interfere with map/reduce spreading, so it's best to avoid it unless its needed. Consider
using these once, and the end of a chain of subqueires, and having it be the only operation
performed at this step.

It is generally faster to sort data outside of Athena if possible. This is the approach
we use with exporting powersets - we do the sorting as we generate the csv/parquet artifacts.

## Join order is important

Due to the mechanism by which Athena computes joins, it is generally faster to join, from
left to right, in order of largest to smallest table.

## Use views sparingly

Especially when writing queries designed for use at multiple institutions, be judicious
with your use of views - as the underlying tables get large, views can become a bottleneck
for institutions with large populations (especially state-level health information
exchanges). Good candidates for usage of views include static data sets (like coding
definition tables), and study tables intended for very small populations by design.

# Jinja templating

If you've worked with data tools like DBT before, you may already be familiar with
[Jinja templates](https://pypi.org/project/Jinja2/)
as a way of making SQL more dynamic at runtime. If not, learning just
[a few commands](https://docs.hyperquery.ai/docs/basic-syntax-overview)
will enable most of the basic functionality you'll need to conditionally render SQL.
We have a collection of 
[generic templates](https://github.com/smart-on-fhir/cumulus-library/tree/main/cumulus_library/template_sql)
that cover some basic use cases, and several studies have custom jinja templates as well. The
[base template function](https://github.com/smart-on-fhir/cumulus-library/blob/main/cumulus_library/template_sql/base_templates.py#L20)
handles the loading and populating of these tables at run time (and base_template.py
contains a bunch of convenience functions that demonstrate the proper way to invoke it),
so you shouldn't need to worry about the lower level jinja infrastructure if you reuse
this entry point.