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

### CountsBuilder(counts.py)

The CountsBuilder provides conveniences for generating aggregate tables
by summarizing data at the FHIR resource level. These are the basic
output units of Cumulus Library.

As long as your table has the appropriate resource refs (always the
ref of the resource in question, and occasionally also either subject
or encounter refs), all you need is to specify the columns you want
and optional join clauses, and it will handle constructing the 
counts query for you

## Config-based Builders

These builders are not meant to be imported as base classes in python
directly (though you could if you wanted). Instead, these provide
a configuration file format, in toml, which you can use to customize
a strict set of build steps.

### Propensity score matching (psm.py)

This handles generating a cohort around a specific variable of interest.
See the
[PSM documentation](https://docs.smarthealthit.org/cumulus/library/statistics/propensity-score-matching.html)
for more info