---
title: Interquartile range
parent: Workflows
grand_parent: Library
nav_order: 3
# audience: clinical researchers, IRB reviewers
# type: reference
---

# Interquartile Range

This document aims to provide help in configuring the generation of one or more
interquartile ranges (IQRs) to provide trimmed estimates of a particular element as part
of a Cumulus Library study.

## Use Cases

An IQR is the basis for the common box plot representation of data in statistics.
As a trimmed estimator, it provides an algorithm for eliminating dataset outliers,
based on the delta between the first and third quartile values.
[Wikipedia](https://en.wikipedia.org/wiki/Interquartile_range)
has a good summary of the algorithm and applications. Cumulus aims to calculate
all of the points mentioned for you, and downstream in the Cumulus dashboard
we'll create the visualization for you.

## Configuring an IQR task

The config you reference in your study manifest is expected to contain a number of 
field definitions. We **strongly** recommend starting from the below template, which
contains details on the expectations of each value.

```toml
# This is a config file for uploading one or more IQR dataset for your study.

# You can define an arbitrary number of these types of statistical representations
# for your study. 

# config_type should  be "iqr" - we use this to distinguish from other
# configurable builders
config_type="iqr"

# You can define an arbitrary number of distributions to generate - just add an array
# entry for each one
[[distributions]]
name="patient_birth_year" # name should be a sql safe name - i.e. no spaces
display_name="Range of patient birth years" # display_name will be shown in the dashboard
source_table="core__patient" # The table to pull the data from
target_col= "birth_year" # A column from the table to generate a distribution for.
                         # This should be either numeric or date type.
```