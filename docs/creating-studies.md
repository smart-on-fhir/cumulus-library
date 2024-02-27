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

There are two ways to get started with a new study:

1. Use `cumulus-library` to create a manifest for you. You can do this with by running:
```bash
cumulus-library create ./path/to/your/study/dir
```
We'll create that folder if it doesn't already exist. 

2. Fork the [
Cumulus Library template repo](https://github.com/smart-on-fhir/cumulus-library-template),
renaming your fork, and cloning it directly from github.

We recommend you use a name relevant to your study (we'll use `my_study` for this
document). This folder name is what you will pass as a `--target` to 
`cumulus-library` when you run your study's queries.

Once you've made a new study,
the `manifest.toml` file is where you can change your study's configuration.
The initial manifest has comments describing all the possible configuration parameters
you can supply, but for most studies you'll have something that looks like this:

```
study_prefix = "my_study"

[sql_config]
file_names = [
    "my_setup.sql",
    "my_cross_tables.sql",
    "my_counts.sql",
]

[export_config]
export_list = [
    "my_study__counts_month",
]
```

Talking about what these three sections do:
  - `study_prefix` is the expected prefix you will be adding to all tables your
  study creates. We'll autocheck this to make sure in several places - this helps
  to guarantee another researcher doesn't have a study artifact that collides
  with yours.
  - `sql_config.file_names` is the list of sql files that your study will run (in order).
  We recommend having one sql file per topic. They should all be in the same
  folder as your manifest file.
  - `export_config.export_list` is the list of tables that will be downloaded
  when `cumulus-library export` is run.
  Cumulus is designed with the idea of shipping around aggregate
  counts to reduce exposure of limited datasets, and so we recommend only exporting
  "count" tables.

There are other hooks you can use in the manifest for more advanced control over
how you can generate SQL. See [Creating SQL with python](creating-sql-with-python.md)
for more information.

We recommend creating a git repo per study, to help version your study data, which
you can do in the same directory as the manifest file. If you've forked your study from
the template, you've already checked this step off.

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
    If you _do_ need some data that is not available in the `core` tables,
    make sure you look at the
    [Creating SQL with python](creating-sql-with-python.md)
    guide for help to safely extract datasets from the raw resource tables.
  - Move queries to a file as you finalize them
  - Build your study with the CLI to make sure your queries load correctly.

#### sqlfluff

We use [sqlfluff](https://github.com/sqlfluff/sqlfluff) to help maintain a consistent
style across many different SQL query authors. We recommend using `sqlfluff` as you
are developing your queries to ensure your SQL is matching the style of other
authors. We copy over our `sqlfluff` rules when you use `cumulus-library` to create
a study, so no additional configuration should be needed.

There are two commands you will want to run inside your study's directory:
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
 - **Count tables must use the CUBE function** to create powersets of data. See the
  [CUBE section of the Presto docs](https://prestodb.io/docs/current/sql/select.html#group-by-clause)
  for more information about this `GROUP BY` type.
  The `core` and `template` projects contain examples.
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

We use this feature in some of our studies to even add automated unit tests.

## Sharing studies

If you want to share your study as an official Cumulus study, please let us know
via the [discussion forum](https://github.com/smart-on-fhir/cumulus/discussions) -
we can talk more about what makes sense for your use case.

If you write a paper using the Cumulus library, please 
[cite the project](https://smarthealthit.org/cumulus/)
