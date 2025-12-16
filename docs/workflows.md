---
title: Workflows
parent: Library
nav_order: 8
has_children: true
# audience: Clinical researchers interested in publications
# type: reference
---

# Workflow reference

This page contains detailed documentation on table building utilities provided 
for use in Cumulus studies.

## Specific workflows

- [File Upload](workflows/file-upload.md)
- [Propensity Score Matching](workflows/propensity-score-matching.md)
- [Valueset Compilation](workflows/valueset.md)

## General usage guidelines

You can invoke a workflow from your study's manifest the same way that you
would run SQL or python files. All toml files in a manifest's study list are
assumed to be workflow configurations

Since some of these workflow methods may be quasi-experimental statistics
(i.e. perform a random sampling),
in those cases, we will persist these outputs outside of a study lifecycle. 

The first time you run a `build` against a study with a workflow with statistics that 
has not previously been run before, it will be executed, and it should generate
a table in your database with a timestamp, along with a view that points to that
table. Subsequent updates will not replace that data, unless you provide the
`--statistics` argument. If you do, it will create a new timestamped table,
point the view to your newest table, and leave the old one in place in case
you need to get those results back at a later point in time.

When you `clean` a study with a workflow that generates statistics, by default all
statistics artifacts will be ignored, and persist in the database. If you want to 
remove these, the `--statistics` argument will remove all these stats artifacts.