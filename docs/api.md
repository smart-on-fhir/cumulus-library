---
title: Clinical study API reference
parent: Library
nav_order: 10
# audience: Clinical researchers/engineers trying to create a study
# type: reference
---
# Package API reference
{: .no_toc }

The following describes at a high level some of the classes available to help
you build studies programmatically. They are all importable directly from
the base `cumulus_library` package.

This is aimed at clinical study usage, rather than data prep usage,
so some of the fussier bits will be glossed over

- TOC
{:toc}

## BaseTableBuilder
*self, manifest: study_manifest.StudyManifest | None = None*

The `BaseTableBuilder` class is what every python based builder inherits from.
It is designed to help wrangle the logistics of going from a query to executing
that query against a database. 

A BaseTableBuilder has two class variables:

**`queries`**: A list to append queries to.
**`display`**: Text displayed while the builder is running in the CLI

A `StudyManifest` is available if, for some reason, you'd like to access study
information at setup. For most clinical use cases, you can safely ignore this.


### prepare_queries
*self, config: base_utils.StudyConfig, manifest: study_manifest.StudyManifest,\*args,\*\*kwargs,*

A `BaseTableBuilder` has one method you are meant to implement, `prepare_queries`,
whose job is to create SQL however it makes sense for your use case. These queries
can be stored in the class's `queries` variable, which a different method,
`execute_queries`, will invoke before running those queries against the database.
You should not (and indeed, you may get a warning about it if you try) override
`execute_queries` - just assume that it's there and doing whatever you need it to
do.

So, using `BaseTableBuilder` might look as simple as this:

```python
from cumulus_library import BaseTableBuilder, StudyConfig, StudyManifest

class MyBuilder(BaseTableBuilder):
    def my_custom_function(table_name:str):
        return f"CREATE TABLE study__table AS (SELECT * FROM core__{table_name})"

    def prepare_queries(self,
        config: StudyConfig,
        manifest: StudyManifest,
        *args,
        **kwargs,
    ):
        self.queries.append("CREATE TABLE study__table AS (SELECT * FROM core__patient)")
        for resource in ['encounter', 'condition']:
            self.queries.append(self.my_custom_function(resource))
```

### write_queries
*self, path: pathlib.Path | None = None*

You don't need to write your queries to disk - this will build them dynamically on the fly,
so that they are always up to date with your builder. If you want to see the query for debugging,
you can use `write_queries` to write files to disk (or, you can use the `generate-sql` subcommand
of the `cumulus-library` CLI):

```python
import pathlib
from cumulus_library import BaseTableBuilder, StudyConfig, StudyManifest

class MyBuilder(BaseTableBuilder):    
    def prepare_queries(self,
        config: StudyConfig,
        manifest: StudyManifest,
        *args,
        **kwargs,
    ):
        self.queries.append("CREATE TABLE study__table AS (SELECT * FROM core__patient);")
        self.write_queries(pathlib.Path.cwd() / "output.sql")
```

### comment_queries
*self, doc_str:str | None=None*

If you want to make query output a bit nicer for human eyes, `comment_queries` will insert
a divider between each query. if *`doc_str`* is provided, it will be appended to the front
of a list of queries (`generate-sql` uses this for its disclaimer).

So, a usage like this:
```python
import pathlib
from cumulus_library import BaseTableBuilder, StudyConfig, StudyManifest

class MyBuilder(BaseTableBuilder):    
    def prepare_queries(self,
        config: StudyConfig,
        manifest: StudyManifest,
        *args,
        **kwargs,
    ):
        self.queries = ["SELECT foo FROM TABLE;", "SELECT bar FROM TABLE;"]
        self.comment_queries()
        self.write_queries(pathlib.Path.cwd() / "output.sql")
```

Would produce output like this:
```sql
SELECT foo FROM TABLE;

-- ###########################################################

SELECT bar FROM TABLE;
```

## StudyConfig
*db: databases.DatabaseBackend,schema: str, drop_table: bool = False, force_upload: bool = False, verbose: bool = False, stats_build: bool = False, stats_clean: bool = False, umls_key: str | None = None, options: dict | None = None*

The StudyConfig object is passed to every class based off BaseStudyBuilder. It's a 
repository of things that the CLI may think a study needs to perform a particular
action.

The following fields are relevant to clinical studies. The rest can be ignored, as
they are primarily intended for query execution:
**db**: an interface to the database in question. `DatabaseBackend` is beyond the scope
of this document, but if you need to inspect a database to dynamically change a query,
`db.cursor()` will return a python 
[PEP-249](https://peps.python.org/pep-0249/) 
compatible database cursor you can pass queries to.
**schema** The name of the database schema being connected to.
**options** This is a dictionary of custom options that can be passed at the CLI. If
you need to allow user-determined behavior for your study, this is where they would
be accessed.

## StudyManifest
*study_path: pathlib.Path | None = None, data_path: pathlib.Path | None = None, \*, options: dict[str, str] | None = None*

A `StudyManifest` represents the contents of a `manifest.toml` file. It also contains
a number of convenience methods for accessing data from the study manifest.

You don't need to worry about creating one of these objects - you can safely assume that
your builder will be passed one of these via the CLI. We're only including methods
that are relevant to clinical studies, rather than data prep/execution.

See 
[Creating studies](creating-studies.md) 
for more info about what goes into a manifest and why.

### get_study_prefix
*self -> str | None*
Returns the prefix defined in the manifest, if present

### get_dedicated_schema
*self -> str | None*
Returns the dedicated schema defined in the manifest, if present

## CountsBuilder
*self, study_prefix: str | None = None, manifest: study_manifest.StudyManifest | None = None*

A `CountsBuilder` extends `BaseStudyBuilder`. Its primary feature is providing a
repeatable way to create cube tables for export to a coordinating site. For clinical
studies, every table you export should be created with this class.

The `CountsBuilder` makes the following assumptions:
- The table you are counting contains the resource refs from the core tables relevant
  to the particular subject
  - Conditions are counted by related `encounter_ref`, since conditions often occur more
    than once for an encounter to support billing, and we will automatically join
    the encounter table for purposes of getting this other countable ref.
  - Document references, encounters, and observations will count their own associated IDs
  - All other resources will count patients

## count_\[resource]
*self, table_name: str, source_table: str, table_cols: list, where_clauses: list | None = None, min_subject: int | None = None, annotation: counts_templates.CountAnnotation | None = None,) -> str*

Every cumulus-supported resource has a similarly structured count generator function.
Just select the type that's appropriate for the resource you're interested in.

In detail, the expected arguments are as follows:

- *table_name*: The name of the counts table to create. It must start with study prefix.
- *source_table*: The table to create counts data from.
- *table_cols*: The columns from *source_table* to add to the count table
- *where_clauses*: An array of where clauses to use for filtering the data
- *min_subject*: An integer setting the minimum bin size, for masking small sample size 
    sets to help preserve patient anonymity. Note: if you define where_clauses, this is not used, and you should provide an equivalent method of binning patients
- *annotation*: An external source to use for adding metadata to a counts table. See `CountAnnotation` below for more info.

A count generator returns the function created from the counts template.

Here's an example of a `CountsBuilder` in use:

```python
from cumulus_library import CountsBuilder, StudyConfig, StudyManifest

class MyBuilder(CountsBuilder):    
    def prepare_queries(self,
        config: StudyConfig,
        manifest: StudyManifest,
        *args,
        **kwargs,
    ):
        self.queries.append(
            self.count_condition(
                'my_study__condition_count',
                'core__condition',
                ['code','recorddate_year']
            )
        )
```

This would count instances of condition codes by year.

## CountAnnotation
*field: str, join_table: str, join_field: str, columns: list\[tuple\[str, str | None]],alt_target: str | None*

A CountAnnotation object can be supplied to a `count[resource]` function to indicate
a table that should be joined to it post-counting. The intended purpose of this is
to allow metadata about code systems to be joined without including them in the
cubing function, which adds to both size and runtime. 

In detail, the expected arguments are as follows:

- *field* The field from the count table to target for the join.
- *join_table* The table to use as a source of annotation data.
- *join_field* The field from *join_table* to join against *field*
- *columns* A list of tuples, describing column to join from *join_table*. 
  The first value in the tuple is the name of the column or a string literal,
  and the second value, if supplied, is the alias to use for that column.
- *alt_target* Optionally, a column, which should be in *columns*, to be the new target for counting,
  causing *field* to only be used for joining. Multiple values for *field* will be
  summed based on the value in *columns*, and sorted by *alt_target*. It's intended
  that *alt_target* have the highest granularity of any column in *columns*.

Using our example from `CountsBuilder` above, here's an example of how we'd use this
to annotate data:

```python
from cumulus_library import CountAnnotation CountsBuilder, StudyConfig, StudyManifest

class MyBuilder(CountsBuilder):    
    def prepare_queries(self,
        config: StudyConfig,
        manifest: StudyManifest,
        *args,
        **kwargs,
    ):
        self.queries.append(
            self.count_condition(
                'my_study__condition_count',
                'core__condition',
                ['code','recorddate_year']
                annotation=CountAnnotation(
                    field='code',
                    join_table='"umls"."icd10_tree"',
                    join_column='code',
                    columns=[
                        ("'icd_10'","system"),
                        ("str", None)
                    ]
                )
            )
        )
```

This would add the string 'icd_10' in a column named `system`, and the UMLS display value
in a column named `str`, as it is in the source.

## get_template
*filename_stem: str, path: pathlib.Path | None = None, \*\*kwargs: dict*

We use
[Jinja](https://jinja.palletsprojects.com/en/stable/)
in cases where we want to dynamically build out SQL statements. Jinja itself is
beyond the scope of this document, but as a simple example, a template like this:

```sql
SELECT * FROM my_study__{{ table_name }};
```

Would allow you to pass a table_name into this template, and thus reuse it in multiple
places. `get_template` abstracts away all the nuance of jinja environment loading,
and just allows you to pass a number of variables into a template for execution.

In detail, the expected arguments are as follows:

- *filename_stem* The filename of your template, before the first period (since there
  are a number of different conventions for jinja file extensions)
- *path* The location of the template; if not provided, we assume it's in the same
  location as the file invoking it.
- *\*\*kwargs* We don't specify these to allow for customizibility, but any additional
  keyword arguments will get passed into the template for usage.

So, given the query above in a file named `example_select.sql.jinja`, you could use
in in a builder in the same directory as follows:

```python
from cumulus_library import BaseTableBuilder, get_templates, StudyConfig, StudyManifest

class MyBuilder(BaseTableBuilder):
     def prepare_queries(self,
        config: StudyConfig,
        manifest: StudyManifest,
        *args,
        **kwargs,
    ):
        self.queries.append(get_templates('example_query', table_name='condition'))
```