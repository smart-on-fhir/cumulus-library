<!-- Target audience: clinical researcher familiar with project, helpful direct tone -->
# Creating Library Studies

The following guide talks about how to use the Cumulus Library to create new
aggregations in support of ongoing projects.

## Forking this repo

We're recommending the Github fork methodology to allow you to stay up to date
with Cumulus while working on configuring your own projects. 

In the upper right of the Github webpage, you'll see a button labeled `fork`.
Click on it, and it will bring you to a page allowing you to configure how your
copy associated with your github account will work - for most use cases, the
defaults are fine. Click `Create fork` and you'll have your own private copy.
Use that copy for cloning the code to your personal machine.

If there are new commits to the primary Cumulus Library repo, you'll see a note
about this just under the green `Code` button - you can click `sync fork` to get
any changes and apply them to your copy, after which you can pull them down to
machines your team is using to develop.

## Creating a new study

Studies are automatically detected by Cumulus Library when they are placed in the
`/cumulus-library/studies` directory, assuming they have a manifest file. The
easiest way to make a new study is to copy the template study to a new directory,
which you can do via the command line or via your system's file system GUI, and
rename the folder to something relevant to your study (we'll use `my_study` for
this document. The folder name is the same thing you will supply to the
`cumulus-library` command as a target when you want to bulk update data.

Once you've made a new study, the `manifest.toml` is the place you let cumulus
library know how you want your study to be run against the remote database. The
template manifest has comments describing all the possible configuration parameters
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
  - `sql_config.file_names` is a list of sql files, in order, that your study should
  create. We recommend having one sql file per topic. They should all be in the same
  location as your manifest file.
  - `export_config.export_list` defines a list of tables to write to csv/parquet when
  data is exported. Cumulus is designed with the idea of shipping around aggregate
  counts to reduce exposure of limited datasets, and so we recommend only exporting
  count tables.

### Writing SQL queries

Most users have a workflow that looks like this:
  - Write queries in the [AWS Athena console](https://aws.amazon.com/athena/) while
  you are exploring the data
  - Move queries to a file as you finalize them
  - Build your study with the CLI to make sure your queries load correctly.

We use [sqlfluff](https://github.com/sqlfluff/sqlfluff) to help maintain a consistent
style across many different SQL query authors. There are two commands you may want to
run inside your study's directory:
  - `sqlfluff lint` will show you any variations from the expected styling rules
  - `sqlfluff fix` will try to make your autocorrect your queries to match the
  expected style

In order to both make your queries parsable to other humans, and to have sqlfluff
be maximally helpful, we have a few requirements and some suggestions for query
styling.

**Hard Requirements**
  For all of these, Cumulus Library will notify you if you write a query that breaks
  one of these rules.
  - All your tables **must** start with a string like `my_study__`. 
  - Relatedly, **`__` is a reserved character string**. Your table names should have
  exactly one of these. We :might: add other use cases for these in the future,
  but as of this writing we don't plan to. C
  - We have **three reserved post-study prefrix substrings: `etl_`,  `nlp_`, and 
  `lib_`** so that we don't drop tables created by other processes. These are fine
  to use elsewhere; `my_study__nlp_counts` would cause an error, but 
  `my_study__counts_nlp` would be fine.

**Requirements for accepting PRs**
 - **Count tables must use the CUBE function** to create powersets of data. See the
  [CUBE section of groupby](https://prestodb.io/docs/current/sql/select.html#group-by-clause)
  for more information about this groupby type. The core and template projects
  provide an example of its usage.
  - For PHI reverse identification protection, **exclude rows from count tables if
  they have a small number of members**, i.e. less than 10.

**Recommended**
  - You may want to select a SQL style guide as a reference.
  [Gitlab's data team](https://about.gitlab.com/handbook/business-technology/data-team/platform/sql-style-guide/)
  has an example of this, though their are other choices.
  - Don't implicitly reference columns tables. Either use the full table name,
  or give the table an alias, and use that any time you are referencing a column.
  - Don't use the * wildcard in your final tables. Explictly list the columns
  with table name/alias - sqlfluff has a hard time inferring what's going on otherwise.
  - We are currently asking for all caps for sql keywords like SELECT and 4 space
  nesting for queries. `sqlfluff fix` will apply this for you, but it may be easier
  to find other problems if you lightly adhere to this from the start.
  - Agggregate count tables should have the first word after the study prefix be
  `count`, and otherwise the word `count` should not be used.
  - Creating a table called `my_study__meta_date` with two columns, `min date` and
  `max date`, and populating it with the start and end date of your study, will
  allow other Cumulus tools to detect study date ranges, and otherwise bakes the
  study date range into your SQL for future reference.

## Sharing studies

If you want to share your study as part of a publication, you can open a PR - one
of the optional targets will be the main `cumulus-library-core` repo, and the
maintainers will be notified of it. If you write a paper using the Cumulus library,
please cite us.