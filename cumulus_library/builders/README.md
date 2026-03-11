# SQL builders

This folder contains various builders, either exposed as part of the package
API or configurable via toml config files, that are intended to be used
as starting points in studies.

## Class-based builders

These are intended to be used directly via python

### BaseTableBuilder(base_table_builder.py)

All builders inherit from this class. This handles the prepare/execute
hooks that Cumulus Library uses to invoke queries against your
database. If you're writing a custom builder, base it off this and
implement `prepare_queries()`.

## Workflows

The following builders are meant to be used as workflows. This means that the builder
action will supply configuration options from an external file, and generally
speaking non-maintainers don't need to interact with these builders directly.

### CountsBuilder(counts_builder.py)

The CountsBuilder provides conveniences for generating aggregate tables
by summarizing data at the FHIR resource level. These are the basic
output units of Cumulus Library.

As long as your table has the appropriate resource refs (always the
ref of the resource in question, and occasionally also either subject
or encounter refs), all you need is to specify the columns you want
and optional join clauses, and it will handle constructing the 
counts query for you

### Propensity score matching (psm.py)

This handles generating a cohort around a specific variable of interest.
See the
[PSM documentation](https://docs.smarthealthit.org/cumulus/library/statistics/propensity-score-matching.html)
for more info