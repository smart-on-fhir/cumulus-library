---
title: Creating SQL with Python
parent: Library
nav_order: 4
# audience: clinical researcher or engineer familiar with project
# type: tutorial
---

# Creating SQL with python

Before jumping into this doc, take a look at 
[Creating Studies](creating-studies.md).
If you're just working with `core` tables related to the US Core FHIR profiles, you 
may not be interested in this, or only need to look at the 
[Working with TableBuilders](#working-with-tablebuilders)
and the
[Generating count tables](#generating-counts-tables)
sections.

## Why would I even need to think about this?

There are three main reasons why you would need to use python to generate sql:
- You would like to make use of the 
[helper class we've built](#generating-counts-tables)
for ease of creating count tables in a structured manual.
- You have a dataset you'd like to 
[load into a table from a static file](#adding-a-static-dataset),
separate from the ETL tables.
- The gnarly one: you are working against the raw FHIR resource tables, and are 
trying to access 
[nested data](#querying-nested-data) in Athena. 
  - We infer datatypes in the ETL based on the presence of data once we get past 
  the top level elements, and so the structure may vary depending on the
  implementation, either at the EHR level or at the FHIR interface level.


We've got examples of all three of these cases in this repo, and we'll reference
those as examples as we go.

## Utilities

There are two main bits of infrastructure we use for programmatic tables:
The TableBuilder class, and the collection of template SQL.

### Working with TableBuilders

We have a base
[TableBuilder class](../cumulus_library/base_table_builder.py)
that
all the above use cases leverage. At a high level, here's what it provides:

- A `prepare_queries` function, which is where you put your custom logic. It
should create an array of queries in `self.queries`. The CLI will pass in a cursor
object and database/schema name, so if you need to interrogate the dataset to decide
how to structure your queries, you can.
- An `execute_queries` function, which will run `prepare_queries` and then apply
those queries to the database. You shouldn't need to touch this function -
just be aware this is how your queries actually get run.
- A `write_queries` function, which will write your queries from `prepare_function`
to disk. If you are creating multiple queries in one go, calling `comment_queries`
before `write_queries` will insert some spacing elements for readability.
- A `display_text` string, which is what will be shown with a progress bar when your
queries are being executed.

You can either extend this class directly (like `builder_*.py` files in 
`cumulus_library/studies/core`) or create a specific class to add reusable functions
for a repeated use case (like in `cumulus_library/schema/counts.py`).

TableBuilder SQL generally should go through a template SQL generator, so that
your SQL has been validated. If you're just working on counts, you don't need
to worry about this detail, but otherwise, the following section talks about
our templating mechanism.

### Working with template SQL

If you are only worried about building counts tables, skip this section - 
we've got enough wrappers that you shouldn't need to worry about this
level of detail.

For validating SQL, we are using 
[Jinja templates](https://jinja.palletsprojects.com/en/3.1.x/)
to create validated SQL in a repeatable manner. We don't expect you to write these
templates - instead, using the 
[template function library](../cumulus_library/template_sql/templates.py)
you can provide a series of arguments to these templates that will allow you to
generate standard types of SQL tables, as well as using templates targeted for
bespoke operations. 

When you're thinking about a query that you'd need to create, first check the
template function library to see if something already exists. Basic CRUD
should be covered, as well as unnestings for some common FHIR objects.

## Use cases

### Generating counts tables
A thing we do over and over as part of studies is generate powerset counts tables
against a filtered resource to get data about a certain kind of clinical population.
Since this is so common we created a class just for this, and we're using it in all
studies the Cumulus team is directly authoring.

The [CountsBuilder class](../cumulus_library/schema/counts.py) 
provides a number of convenience methods that are available for use (this covers
mechanics of generation). You can see examples of usage in the 
[Core counts builder](../cumulus_library/studies/core/count_core.py)
(which is where the business logic of your study lives). 

- `get_table_name` will scan the study's `manifest.toml` and auto prepend a table
name with whatever the study prefix is.
- `get_where_clauses` will format a string, or an array, of where clauses in a
manner that the table constructors will expect.
- `count_[condition,document,encounter,observation,patient]` will take a target table
name, a source table, and an array of columns, and produce the appropriate powerset
table to count that resource. You can optionally provide a list of where statements
for filtering, or can change the minimum bin size used to include data
- The `count_*` functions pass through to `get_count_query` - if you have a use
case we're not covering, you can use this interface directly. We'd love to hear
about it - we'd consider covering it and/or take PRs for new features

As a convenience, if you include a `if __name__ == "__main__":` clause like you
see in `count_core.py`, you can invoke the builder's output by invoking it with
python, which is a nice way to get example SQL output for inclusion in github.
This is where the 
[count core sql output](../cumulus_library/studies/core/count_core.sql)
originated from.

Add your count generator file to the `counts_builder_config` section of your
`manifest.toml` to include it in your build invocations.

### Adding a static dataset

*NOTE* - we have an
[open issue](https://github.com/smart-on-fhir/cumulus-library/issues/58)
to develop a faster methodology for adding new datasets.

Occasionally you will have a dataset from a third party that is useful for working
with your dataset. In the vocab study (requiring a license to use), we 
[add coding system data](../cumulus_library/studies/vocab/vocab_icd_builder.py)
from flat files to athena. If you need to do this, you should extend the base
TableBuilder class, and your `prepare_queries` function should do the following,
leveraging the
[template function library](../cumulus_library/template_sql/templates.py):
- Use the `get_ctas_query` function to get a CREATE TABLE AS statement to 
instantiate your table in athena
- Since athena SQL queries are limited in size to 262144 bytes, if you have
a large dataset, break it up into smaller chunks
- Use the `get_insert_into` function to add the data from each table to
the chunk you just created.

Add the dataset uploader to the `table_builder_config` section of your
`manifest.toml` to include it in your build - this will make this data
available for downstream queries

### Querying nested data

Are you trying to access data from deep within raw FHIR tables? I'm so sorry.
Here's an example of how this can get fussy with code systems:

A FHIR coding element may be an array, or it may be a singleton, or it may
be a singleton wrapped an array. It may be fully populated, or partially populated,
or completely absent. There may be one code per record, or multiple codes per record,
and you may only be interested in a subset of these codes.

This means you may have differing schemas in Athena from one site's data to another
(especially if they come from different EHR systems, where implementation details
may differ). In order to handle this, you need to create a standard output
representation that accounts for all the different permutations you have, and
conform data to match that. The 
[encounter coding](../cumulus_library/studies/core/builder_encounter_coding.py)
and
[condition codeableConcept](../cumulus_library/studies/core/builder_condition_codeableconcept.py)
builders both jump through hoops to try and get this data into flat tables for
downstream use.

This is a pretty open ended design problem, but based on our experience, your
`prepare_queries` implementation should attempt the following steps:
- Check if your table has any data at all
- If it does, inspect the table schema to see if the field you're interested in
is populated with the schema elements you're expecting
  - If yes, it's safe to grab them
  - If no, you will need to manually initialize them to an appropriate null value
- If you are dealing with deeply nested objects, you may need to repeat the above
more than once
- Write a jinja template that handles the conditionally present data, and a 
template function to invoke that template
- Test this against data samples from as many different EHR vendors as you can
- Be prepared to need to update this when you hit a condition you didn't expect
- Create a distinct table that has an ID for joining back to the original
- Perform this join as appropriate to create a table with unnested data

You may find it useful to use the `--builder [filename]` sub argument of the cli
build command to run just your builder for iteration. The
[Sample bulk FHIR datasets](https://github.com/smart-on-fhir/sample-bulk-fhir-datasets)
can provide an additional testbed database above and beyond whatever you produce
in house.

Add this builder to the `table_builder_config` section of your
`manifest.toml` - this will make this data available for downstream queries.

Good luck! If you think you're dealing with a pretty common case, you can reach
out to us on the 
[discussion forum](https://github.com/smart-on-fhir/cumulus/discussions)
and we may be able to provide an implementation for you, or provide assistance
if you're dealing with a particular edge case.