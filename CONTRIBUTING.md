# Contributing to Cumulus Library

## Set up your dev environment

To use the same dev environment as us, you'll want to run these commands:
```sh
pip install .[dev]
pre-commit install
```

This will install dependencies & build tools,
as well as set up an auto-formatter commit hook.

## Running tests

Tests can be run with `pytest` like you might expect,
but you'll first want to set up a fake `test` AWS profile.

1. Edit `~/.aws/credentials`
2. Add a block like:
```
[test]
aws_access_key_id = test
aws_secret_access_key = test
```
3. Then run `pytest`

## Adding new resources

Things to keep in mind:
- If the new resource links to Encounter,
  add it to the completion checking done in the Encounter code.
  (Though consider the effect this will have on existing encounters.)
- Try to keep the columns in spec-order,
  except please move any isolated reference fields to the end,
  since they are not really human-readable and
  this makes it nicer to visually scan the table.
- Add a self-Reference field for easier joining.
  For example, if you're adding a `condition` table, in addition to
  the `id` field, add a `condition_ref` field defined like:
  `concat('Condition/', id) AS condition_ref`

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

## Rebuilding the Markdown table docs

We keep some generated documentation in git,
to help us keep our docs in sync with table changes.
This is stored in `docs/core-study-details.md`

You can regenerate these yourself when you make changes to table layout:
1. Rebuild the `core` study in Athena.
1. Run `generate-md` against Athena:
```sh
cumulus-library generate-md --target core [database args pointing at your athena db]
```

Then take the contents of `cumulus_library/studies/core/core_generated.md`
and paste them into `docs/core-study-details.md`
(being careful to not erase any manually entered descriptions).
