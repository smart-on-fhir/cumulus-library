# Contributing to Cumulus Library

## Set up your dev environment

To use the same dev environment as us, you'll want to run these commands:
```sh
pip install .[dev]
pre-commit install --hook-type pre-commit --hook-type pre-push
```

This will install dependencies & build tools,
as well as set up a `black` auto-formatter commit hook.
