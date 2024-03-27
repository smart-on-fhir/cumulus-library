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
