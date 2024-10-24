# Maintainer notes

This document is intended for users contributing/maintaining this repository.
It is not comprehensive, but aims to capture items relevant to architecture
that are not covered in another document.

## Intended usage and database schemas

Since these terms are used somewhat haphazardly in different database implementations, 
we'll quickly define them for the purposes of this document:

- database - a single instance of a database product, local or in the cloud. it can
contain several schemas.
- schema - a namespaced collection of tables inside a database

Cumulus, as a holistic system, is designed to allow querying against the entire history
of a medical institution. You do not need to preselect a cohort - that can be done
by the author of a given study. We generally recommend using this approach, and it
is the one that we are trying to use in house.

However, for technical and philosophical reasons, users may wish instead to select
a cohort at their EHR, and upload that data to a specific named schema in their
database, and work against that. It's important that we remember this use case
as we roll out new features.

From the perspective of this repository, and studies which run on top of it, it's 
important to remember these dual use cases - we should never make assumptions 
about which database schema will be used, and it may change from one run to the next.
But all data associated with a single schema (source data and Cumulus studies) should
exist inside that schema.

As of this writing, the sole exception to this is for third party vocabulary systems.
For these, the CLI will automatically create these in a unique schema, basically
(but not enforced) as read only tables that can be referenced by other studies
via cross-database joining. Additional tables should not be created by users in these
schemas.

A user could elect to use these vocabulary builders and skip the entire rest of the
Cumulus ecosystem, if they wanted to. 

## Advanced study features

These features are for very narrow and advanced use cases,
designed for internal project studies (like `core`, `discovery`, or `data_metrics`).

### Dynamic prefixes

The `data_metrics` study wants to be able to generate an analysis of a single study cohort's data.
In order to do this, it needs to privately namespace that analysis.

The solution we use for this is to allow a study to dynamically generate the prefix it will use.
Thus, the `data_metrics` study can use a prefix like `data_metrics_hypertension__` for a
`hypertension` study and `data_metrics_covid__` for a `covid` study.

#### Config
Add a field called `dynamic_study_prefix` pointing at a local Python file.
If this field is present, any `study_prefix` field is ignored.
```toml
dynamic_study_prefix = "gen_prefix.py"
```

#### Generator file

Your generator file will be called as a Python script,
with the `--option` arguments that Cumulus Library gets
(but without the `--option` bit).
You should ignore unknown options, for forward compatibility.

You should print your desired prefix to stdout.

Example:
```python
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--study")
args, _rest = parser.parse_known_args()

if args.study:
  print(f"data_metrics_{args.study}")
else:
  print("data_metrics")
```

#### Usage

Your study still has to be selected using its original name (`--target=data_metrics`),
but the resulting tables will be prefixed using the generated study name.

This command line would build a `data_metrics_hypertension` study:
```sh
cumulus-library build -t data_metrics --option study:hypertension
```
