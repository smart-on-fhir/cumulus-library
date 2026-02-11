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

If you're authoring a study, you just need to do three things to get started:

- Make a new directory inside the directory you're keeping studies in. The name of this
directory will be the name you use to run it using the `cumulus-library` cli command.
In this document, we're calling this directory `my_study` as an example.
- Make a new file, `__init__.py`, which contains the following:
```python
__version__='0.1.0'
```
The CLI will use this to display the study's version on demand. Consider using
[semantic versioning rules](https://semver.org/#semantic-versioning-specification-semver)
to update this value as appropriate.
- Make a new file, `manifest.toml`. A
[toml file](https://toml.io/en/)
is a config file format - you don't need to worry too much about the details of this 
format, as we'll show you in this document how the library uses these files to run your
study. You can copy the following template as an example, which has comments describing
what each section does:

```toml
# 'study_prefix' should be a string at the start of each table. We'll use this
# to remove tables from prior builds, so it should be unique. Name your tables
# in the following format: [study_prefix]__[table_name]. It should probably, 
# but not necessarily,
# be the same name as the folder the study definition is in.
study_prefix = "my_study"

# 'description' should describe the purpose of your study. It will be displayed
# in the Cumulus Dashboard, and is a way to communicate the goals of your study
# to a user who is unfamiliar with it. It should be about a paragraph or two
# in length. We use triple quoting to allow for multiline strings, and slashes
# to escape the newlines, so that the dashboard can handle rendering based
# on how big the browser window is
description = """\
This study is an example that shows how to use a manifest. \
It highlights the syntax and logic of how the manifest works, \
and provides some high level guidance as to the types of artifacts \
that a Cumulus study has available. \

Above is an intentional newline indicating a paragraph break. \
If you see this text in the dashboard, go update the manifest of your \
study with some descriptive text. \
"""

# 'build_types' contains a list of ways to orchestrate the various stages
# of a study build (more on stages below). It allows you to specify ways
# that certain steps should be executed.

# A common use case we have goes like this: we want to define one or more
# cohorts from a population based on data from an EHR. After that's done,
# we may, depending on the study's goals, want to iterate on some prompt engineering
# strategies to extract data from clinical notes. After that's completed,
# we want to use the extracted features from those notes to classify members
# of those cohorts, and then generate counts from the results. Those would
# be three different kinds of stages, and you could rebuild each one independently.

# Since order is not important, if it makes more sense to you to define the stages
# first and then list build_types later, go for it.

# We'll always create a build type called 'all' which will run all stages in the order
# they are defined in the file. We'll also create a stage named 'default', if one does
# not exists, which behaves the same all 'all', but you can override the default stage
# if you want a different behavior. Other stages can be run from the command line by
# passing `-b build_target`

[build_types]
cohort = ['define_population', 'select_cohorts']
counts = ['count_cohorts']

# The stages section defines how queries/builders/workflows should be grouped
# and ordered. A stage is a list of actions. Stages are run sequentially in order.

# Actions can have the following fields:
#   - 'description', which will be displayed in the CLI while the action is being run
#   - 'files', which is a list of files to process (raw sql, python builders, or toml workflows)
#   - 'action_type', which determines what the action does. Valid types:
#      - 'serial' (default if action_type is not specified) runs the files in order, one at a time
#      - 'parallel' will run the files concurrently, which is faster but assumes no interdependencies
#      - 'submanifest' loads a list of actions from another file. Those actions use the
#        same format as those from the main manifest

# This double bracket defines a new array inside of stages, define_population
[[stages.define_population]]
description = 'Upload files defining some conditions & select patients'
files = [
  'file_upload_workflow_definition.toml',
]
# Using this same name again inside a double bracket causes a new item to be added
# to the end of the define_population list.
[[stages.define_population]]
description = 'Select patients by characteristics'
files = [
  'select_patients.sql',
]
[[stages.select_cohorts]]
description = 'Define cohorts'
files = [
  'cohort_submanifest.toml'
]
action_type = "submanifest"
[[stages.count_cohorts]]
description = 'Define cohorts'
files = [
  'cohort_by_age.py',
  'cohort_by_gender.py'
]
action_type = "parallel"

[export_config]

# The following tables will be exported and labeled as aggregate count tables.
# In most cases, tables should go in this list.

count_list = [
    "my_study__count_influenza_test_month",
]

# In some cases, you may want to annotate count data with metadata from another
# source. Most commonly, this is used to add data related to a code (like its
# display value or code system) from another source. These tables should go
# in this section.

annotated_count_list = [
    "my_study__count_influenza_test_month_annotated",
]

# Some specific tables (like those produced by data metrics) are a special type
# of tables, that are flat summary statistics tables. They should go in this list.

flat_list = [
    "my_study__q_influenza",
]

# Tables that shouldn't go through aggregation, but instead contain data about
# the export itself, should be marked as metadata. Two types are expected as of this
# writing: a `meta_date` table, outlining the period over which the study extends,
# and a `meta_version` table, which should be incremented whenever the format of
# your export tables changes. See the core study for examples of how to structure
# these tables.

meta_list = [
  "my_study__meta_date",
  "my_study__meta_version",
]

# The following section is for advanced/unusual study use cases

# [advanced_options]

# If you want to override the default schema name to an explicit one, you can define
# the name of this schema here. 99% of the time, this is not the behavior you want -
# you want library data to be in the same schema as your data source, since this allows
# you to keep track of where your source data for a given study run came from.
#
# The intended use case for this is for static/slow moving data sets that are external
# to your EHR data - this is typically things like coding systems.
#
# These should be read only use cases - if you want to do additional iterations with
# the contents of one of these reference sets, do it in the study, not in the reference
# itself.

# use_dedicated_schema="alternate_schema_name"

# if you want to give your study a prefix programmatically (like appending a date 
# reflecting the run time) so you can have multiple instances, you can define the path to
# that script here. Be careful, as this may cause some common aggregation use cases
# in the dashboard to fail.

# dynamic_study_prefix = 'my_prefix_script.py'

```

A submanifest looks a lot like a manifest, but just contains a list of actions.
Here's an example submanifest (note that this is optional and you don't have to use it):

```toml
# A submanifest contains a top level list of actions, and then uses the same action
# syntax. At runtime, we'll map it into the stage the submanifest was referenced from.
[[actions]]
description = "Select cohorts by age"
files = ['age_cohort.sql']
action_type = 'serial'
[[actions]]
description = "Create cohorts by medications"
files = ['cohort_rx_1.py', 'cohort_rx_2.py']
action_type = 'parallel'
# Note that you cannot recursively add a submanifest from another submanifest
```

If you want to look at how programmatic sql works, check out  
[Creating SQL with python](creating-sql-with-python.md)
for more information.

If you're familiar with git workflows, we recommend creating a git repo for your study, to
help version your study in case of changes. You can also clone our
[template study](https://github.com/smart-on-fhir/cumulus-library-template),
which provides some other guidance on how to get started with authoring.

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

__Important detail on FHIR arrays__: When we flatten a FHIR element that
is specified as being potentially an array (like many instances of 
CodeableConcept, for example), we create a seperate table from that
field. It can be joined back to the table it was extracted from by the
id field present in both tables.

However - in your study design, you will need to handle cases where
multiple items may exist in these tables. It is common for multiple
code systems to be used for a single record.

As an example, the Condition resource has a base level CodeableConcept
that _should_ contain a SNOMED code, but often has only an ICD9/10 code,
or a EHR vendor specific code. We handle this case in two ways:
  - The __core__condition_codable_concepts_display__ table contains one
  record per resource, where we specify a priority order and take the
  first valid code we find, which is ok for cases where you aren't
  very concerned about a specific coding and are just looking to get
  an idea of what data you have
  - The __core__condition_codable_concepts_all__ table contains
  every code for every system found. This is useful when you are specifically
  looking for data associated with a given clinical coding system, but
  if you are not careful, you can cause a condition to be counted twice
  by not specifying a coding system when joining this table with the
  base condition table.

Your approach to handling this is going to be dictated by the specific
clinical context you're working with. In cases where we don't specify
two table types for an array resource, you should assume that we are
following the second pattern and account for that in your queries.

#### sqlfluff

We use [sqlfluff](https://github.com/sqlfluff/sqlfluff) to help maintain a consistent
style across many different SQL query authors. We recommend using sqlfluff as you
are developing your queries to ensure your SQL is matching the style of other
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
  - Create a table called `my_study__meta_date` with two `DATE` columns, `min_date`
  and `max_date`, and populating it with the start and end date of your study, will
  allow other Cumulus tools to detect study date ranges, and otherwise bakes the
  study date range into your SQL for future reference. This table is required.
    - If you are pulling your dates from resources, it's recommended to cap `max_date` to the
    current time (`LEAST(max_date, CURRENT_DATE)`), since resource data could have typos or
    be planned events that put `max_date` in the future (which is both inaccurate and may cause
    parsing issues if the date is too far forward).
  - Create a `my_study__meta_version` with one column, `data_package_version`, and
  giving it an integer value as shown in this snippet:
  ```sql
  CREATE TABLE my_study__meta_version AS
  SELECT 1 AS data_package_version;
  ```
  allows you to signal versions for use in segregating data downstream, like in the
  Cumulus Aggregator. Increment it when your counts output changes format,
  and thus third parties need to rerun your study from scratch. If this is not
  set, the version will implicitly be set to zero.
  - Add these meta tables to `export_list` in your `manifest.toml`.

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

### Run your study on the local ndjson

Now you can run Cumulus Library but point it at the ndjson folder with the
`--db-type=duckdb` and `--load-ndjson-dir=DIR` flags. For example:
```shell
cumulus-library build \
  --db-type duckdb \
  --load-ndjson-dir local_dir/ndjson/ \
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
