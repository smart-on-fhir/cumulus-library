---
title: Creating Library Studies
parent: Library
nav_order: 3
# audience: clinical researcher or engineer familiar with project
# type: tutorial
---

# Creating Library Studies

The following guide talks about how to use the Cumulus Library to create new
aggregations in support of ongoing projects.

## Setup

If you are going to be creating new studies, we recommend, but do not require, adding
an environment variable, `CUMULUS_LIBRARY_PATH`, pointing to the folder in which 
you'll be working on study development. `cumulus-library` will look in each 
subdirectory of that folder for manifest files, so you can run several studies
at once. 

If you're not doing this, you can always add the `--study-dir path/to/dir` argument
to any build/export call to tell it where to look for your work.

## Creating a new study

If you're authoring a study, you just need to do two things to get started:

- Make a new directory inside the directory you're keeping studies in. The name of this
directory will be the name you use to run it using the `cumulus-library` cli command.
In this document, we're calling this directory `my_study` as an example.
- Make a new file, `manifest.toml`. A
[toml file](https://toml.io/en/)
is a config file format - you don't need to worry too much about the details of this 
format, as we'll show you in this document how the library uses these files to run your
study. You can copy the following template as an example, which has comments describing
what each section does:

```toml
# 'study_prefix' should be a string at the start of each table. We'll use this
# to clean up queries, so it should be unique. Name your tables in the following
# format: [study_prefix]__[table_name]. It should probably, but not necessarily,
# be the same name as the folder the study definition is in.
>>>>>>> 1e357f9 (v2.0 docs update)
study_prefix = "my_study"

# For most use cases, this should not be required, but if you need to programmatically
# build tables, you can provide a list of files implementing BaseTableBuilder.
# See vocab and core studies for examples of this pattern. These run before
# any SQL execution
# [table_builder_config]
# file_names = [
#     "my_table_builder.py",
# ]

# The following section describes all tables that should be generated directly
# from SQL files.
[sql_config]
# 'file_names' defines a list of sql files to execute, in order, in this folder.
# Recommended order: Any ancillary config (like a list of condition codes),
# tables/view selecting subsets of data from FHIR data, tables/views creating 
# summary statistics.
file_names = [
    "setup.sql",
    "lab_observations.sql",
    "counts.sql",
    "date_range.sql"
]


# The following section defines parameters related to exporting study data from
# your athena database
[export_config]
# The following tables will be output to disk when an export is run. In most cases,
# only count tables should be output in this way.
export_list = [
    "template__count_influenza_test_month",
]

# For generating counts table in a more standardized manner, we have a class in the 
# main library you can extend that will handle most of the logic of assembling 
# queries for you. We use this pattern for generating the core tables, as well
# other studies authored inside BCH. These will always be run after any other
# SQL queries have been generated
# [counts_builder_config]
# file_names = [
#    "count.py"
# ]

# For more specialized statistics, we provide a toml-based config entrypoint. The
# details of these configs will vary, depending on which statistical method you're
# invoking. For more details, see the statistics section of the docs for a list of
# supported approaches.
# These will run last, so all the data in your study will exist by the time these
# are invoked.
# [statistics_config]
# file_names = 
# [
#    "psm_config.toml"
# ]

```

There are other hooks you can use in the manifest for more advanced control over
how you can generate sql - these are commented out in the above template, and you can
delete them if you don't need them. See 
[Creating SQL with python](creating-sql-with-python.md)
for more information.

If you're familiar with git workflows, we recommend creating a git repo for your study, to
help version your study in case of changes.

### Writing SQL queries

Most users have a workflow that looks like this:
  - Write queries in the [AWS Athena console](https://aws.amazon.com/athena/) while
  you are exploring the data
    - We recommend trying to keep your studies pointed at the `core` tables. The
    raw FHIR resource tables contain a lot of nested data that can be tricky
    to write cross-EHR queries against.
    For example, an EHR may store Medication information in the `medication` or
    the `medicationrequest` raw resource tables,
    but the `core__medication` hides that complexity and is always available,
    regardless of the specific EHR approach.
    You can look at the
    [Core study documentation](core-study-details.md) 
    for details about that study's contents.
    If you _do_ need some data that is not available in the `core` tables,
    make sure you look at the
    [Creating SQL with python](creating-sql-with-python.md)
    guide for help to safely extract datasets from the raw resource tables.
  - Move queries to a file as you finalize them
  - Build your study with the CLI to make sure your queries load correctly.

#### sqlfluff

We use [sqlfluff](https://github.com/sqlfluff/sqlfluff) to help maintain a consistent
style across many different SQL query authors. We recommend using sqlfluff as you
are developing your queries to ensure your sql is matching the style of other
authors, but it is not required. You can copy our
[sqlfluff config](https://github.com/smart-on-fhir/cumulus-library/blob/main/cumulus_library/.sqlfluff)
into your study if you'd like to use the same style we are.

There are two commands you can run inside your study's directory to check formatting:
  - `sqlfluff lint` will show you any variations from the expected styling rules
  - `sqlfluff fix` will try to make your autocorrect your queries to match the
  expected style

In order to both make your queries parsable to other humans, and to have `sqlfluff`
be maximally helpful, we have a few requirements and some suggestions for query
styling.

#### Hard Requirements
  For all of these, Cumulus Library will notify you if you write a query that breaks
  one of these rules when you build your study.
  - All your tables **must** start with a string like `my_study__`. 
  - Relatedly, `__` (two underscores) **is a reserved character string**.
  Your table names should have exactly one of these.
  - We have **three reserved table prefixes:** `etl_`,  `nlp_`, and `lib_`.
  These are fine to use elsewhere in the table name, just not at the beginning.
  For example, `my_study__nlp_counts` would cause an error, 
  but `my_study__counts_nlp` would be fine.


#### Requirements for accepting PRs
 - **Count tables must use the CUBE operator** to create powersets of data. See the
  [Trino docs](https://trino.io/docs/current/sql/select.html#cube)
  for more information about its syntax. The core study, and other studies produced
  by the core Cumulus team, provide examples of its usage.
  - For PHI reverse identification protection, **exclude rows from count tables if
  they have a small number of members**, e.g. less than 10.

#### Recommended
  - You may want to select a SQL style guide as a reference. Mozilla provides a
  [SQL style guide](https://docs.telemetry.mozilla.org/concepts/sql_style.html),
  which our `sqlfluff` config enforces. If you have a different style you'd like
  to use, you can update the `.sqlfluff` config to allow this. For example,
  [Gitlab's data team](https://about.gitlab.com/handbook/business-technology/data-team/platform/sql-style-guide/)
  has a style guide that is more centered around DBT, but is more prescriptive
  around formatting.
  - Don't implicitly reference columns tables. Either use the full table name,
  or give the table an alias, and use that any time you are referencing a column.
  - Don't use the `*` wildcard in your final tables. Explicitly list the columns
  with table name/alias - `sqlfluff` has a hard time inferring what's going on otherwise.
  - We are currently asking for all caps for SQL keywords like `SELECT` and four-space
  indentation for queries. `sqlfluff fix` will apply this for you, but it may be easier
  to find other problems if you adhere to this from the start.
  - Agggregate count tables should have the first word after the study prefix be
  `count_`, and otherwise the word `count` should not be used.

#### Metadata tables
  - Creating a table called `my_study__meta_date` with two columns, `min date` and
  `max date`, and populating it with the start and end date of your study, will
  allow other Cumulus tools to detect study date ranges, and otherwise bakes the
  study date range into your SQL for future reference.
  - Creating a `my_study__meta_version` with one column, `data_package_version`, and
  giving it an integer value as shown in this snippet:
  ```sql
  CREATE TABLE my_study__meta_version AS
  SELECT 1 AS data_package_version;
  ```
  allows you to signal versions for use in segregating data downstream, like in the
  Cumulus Aggregator. Increment it when your counts output changes format,
  and thus third parties need to rerun your study from scratch. If this is not
  set, the version will implicitly be set to zero.

## Testing studies

If you have a Cumulus database in Athena already,
you can easily point at that during study development.

But it may also be faster or easier to work with local files,
where you can add edge cases.
Cumulus Library has an optional database backend driven by local ndjson just for that!

### Set up your ndjson

You can grab fake
[Synthea data](https://github.com/smart-on-fhir/sample-bulk-fhir-datasets)
or use the result of actual bulk-export results from your EHR.

Once you have that,
run [Cumulus ETL](https://docs.smarthealthit.org/cumulus/etl/)
on your source ndjson with the
`--output-format=ndjson` flag and pointing at some local directory.
For example:
```shell
docker compose run \
  --volume local_dir:/in \
  cumulus-etl \
  /in/ndjson \
  /in/output \
  /in/phi \
  --output-format=ndjson
```

This will generate a tree of processed & anonymized ndjson
(just like the ETL normally makes).

### Run your study on the local ndjson

Now you can run Cumulus Library but point it at the output files with the
`--db-type=duckdb` and `--load-ndjson-dir=DIR` flags. For example:
```shell
cumulus-library build \
  --db-type duckdb \
  --load-ndjson-dir local_dir/output \
  --database local_dir/duck.db \
  --target my_study
```

### Adding edge cases

Not only is this faster than talking to Athena,
but you can edit the local ndjson to add interest edge cases that you want your
SQL to be able to handle.

We use this feature in the library and our studies for automated unit testing.

## Sharing studies

If you want to share your study as an official Cumulus study, please let us know
via the [discussion forum](https://github.com/smart-on-fhir/cumulus/discussions) -
we can talk more about what makes sense for your use case.

If you write a paper using the Cumulus library, please 
[cite the project](https://smarthealthit.org/cumulus/)

## Snapshotting/archiving studies

If you need to freeze a study at a specific point in time (like if you're working
on a publication), you can create an archive of that study using the `archive`
command in the Cumulus library CLI. Just be aware that this archive may contain
sensitive data, and so make sure your store the archive someplace that complies
with your organization's security policies.
