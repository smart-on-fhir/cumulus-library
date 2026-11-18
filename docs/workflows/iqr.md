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

## IQR outputs
An IQR task will contain the following datapoints (with example data, which can be either
numeric or date type:

|minimum|median|maximum|q1|q3|iqr|lower_whisker|upper_whisker|
|-------|------|-------|---|---|---|-----------|-------------|
|1900-01-01|2024-06-01|2112-12-01|2021-10-01|2025-05-01|1308 00:00:00.000|2016-06-01|2030-08-01|


The data in the columns is as follows:
- *minimum*: the lowest value in the dataset
- *median*: the middle value in the dataset
- *maximum*: the largest value in the dataset
- *q1*: the point in the dataset marking the transition between the first and second quartiles
- *q3*: the point in the dataset marking the transition between the third and fourth quartiles
- *iqr*: the interquartile range, equal to `q3 - q1`
- *lower_whisker*: the smallest point that is greater than or equal to `q1 - (iqr * 1.5)`
- *upper_whisker*: the largest point that is less than or equal to `q3 + (iqr * 1.5)`