# Contributing to Cumulus Library

## Set up your dev environment

To use the same dev environment as us, you'll want to run these commands:
```sh
pip install .[dev]
pre-commit install
```

This will install dependencies & build tools,
as well as set up an auto-formatter commit hook.

## Adding new resources

Things to keep in mind:
- If the new resource links to Encounter,
  add it to the completion checking done in the Encounter code.
  (Though consider the effect this will have on existing encounters.)

## Rebuilding the reference SQL

We keep some reference SQL in git,
to help us track unexpected changes and verify our SQL indenting.
These are stored in `cumulus_library/studies/*/reference_sql/`

You can regenerate these yourself when you make changes to SQL:

```sh
cumulus-library generate-sql \
  --db-type duckdb \
  --database :memory: \
  --load-ndjson-dir tests/test_data/duckdb_data \
  --target core \
  --target discovery
```
