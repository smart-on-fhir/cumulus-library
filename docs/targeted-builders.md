---
title: Targeted Builders
parent: Library
nav_order: 8
has_children: true
# audience: Clinical researchers interested in publications
# type: reference
---

# Statistics reference

This page contains detailed documentation on table building utilities utilities provided 
for use in Cumulus studies.

## Specific builder modules

- [Propensity Score Matching](builders/propensity-score-matching.md).
- [Valueset Compilation](builders/valueset.md).

## General usage guidelines

You can invoke a statistic task from your study's manifest the same way that you
would run SQL or python files - the only difference is that you point it at another
toml file, which allows stats configs to have different input parameters depending
on the analysis you're trying to perform.

In your manifest, you'd add a section like this:
```toml
[statistics_config]
file_names = [
    "psm_config.toml"
]
```

We'll use this as a way to load statistics configurations. Since some of these 
statistical methods may be quasi-experimental (i.e. perform a random sampling),
we will persist these outputs outside of a study lifecycle. 

The first time you run a `build` against a study with a statistics config that 
has not previously been run before, it will be executed, and it should generate
a table in your database with a timestamp, along with a view that points to that
table. Subsequent updates will not replace that data, unless you provide the
`--statistics` argument. If you do, it will create a new timestamped table,
point the view to your newest table, and leave the old one in place in case
you need to get those results back at a later point in time.

When you `clean` a study with statistics, by default all statistics artifacts
will be ignored, and persist in the database. If you want to remove these,
the `--statistics` argument will remove all these stats artifacts.