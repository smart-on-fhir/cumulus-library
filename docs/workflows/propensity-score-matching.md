---
title: Propensity Score Matching
parent: Workflows
grand_parent: Library
nav_order: 2
# audience: clinical researchers, IRB reviewers
# type: reference
---

# Propensity Score Matching (PSM)

This document aims to provide help in configuring/running PSM analysis as part of a
Cumulus Library study.

## What is propensity score matching?

Propensity score matching is a statistical technique for generating an experimental
cohort from a larger population around a dependent variable. In our context, this 
allows you to compare the positive/negative members of a given population to determine
if there are any social determinants of health (SDOH) that may be an indicator of
a higher probability of exhibiting a given disease/symptom.

The expected workflow looks something like this:

- Define a set of symptoms that are indicators of the condition you are investigating
- From a general population, ID the instances that match this condition set
- Select an appropriate sample size for your randomly selected cohorts
- Run the PSM module, which will sample from your condition matches, generate a
  negative set from the remaining general population, normalize these two sets,
  and generate data that can be fed into statistical methods to help you analyze
  the population differences w.r.t SDOH.

## Configuring a PSM task

The PSM config you reference in your study manifest is expected to contain a number of 
field definitions. We :strongly: recommend starting from the below template, which
contains details on the expectations of each value.
```toml
# This is a config file for generating a propensity score matching (PSM) definition. 

# You can use this for selecting records for an expert review process, and you can 
# also use it to generate statistics around your population that meets your selection
# criteria versus those that do not.

# This attempts to handle the complexities of generating SQL queries for you,
# but you do need to know a little bit about what your data looks like in the
# database. We recommend that you only attempt to use this after you have decided
# on the first draft of your cohort selection criteria

# config_type should always be "psm" - we use this to distinguish from other
# statistic type runs
config_type = "psm"

# classification_json should reference a file in the same directory as this config,
# which matches a category to a set of ICD codes. As an example, you could use
# an existing guide like DSM5 classifications for this, but you could also use
# something like VSAC, or create your own.
classification_json = "dsm5_classifications.json"

# pos_source_table should be a curated table built as part of a study, which
# has entities matching your selection criteria (probably patients, but it could
# be another base FHIR resource)
pos_source_table = "study__diagnosis_cohort"

# neg_source_table should be the primary table your positive source was built from,
# i.e. it should contain all members that weren't identified as part of your cohort.
# It should usually be one of the core FHIR resource tables.
neg_source_table = "core__condition"

# target_table should be the name of the table you're storing your PSM cohort in. It 
# should be prefixed by 'studyname__'
target_table = "study__psm_encounter_covariate"

# primary_ref should be the column name from your pos_source_table that is the item
# of interest. it should have the same name as it did when it was selected 
#from neg_source_table
primary_ref = 'encounter_ref'

# count_ref is an optional second ref in your positive_source table that can be used
# to id the number of instances associated with your primary_ref. It is only used
# for validation
count_ref = 'subject_ref'

# count_table is the table to use to select your count_ref from. It should :probably:
# be the same as your neg_source_table
count_table = 'study__condition'

# dependent_variable is the name to use for identifying which cohort a record is in.
# It should be phrased such that a value of true would indicate it is originally from
# your pos_source_table.
dependent_variable = "example_diagnosis"

# pos_sample_size is the number of records to select from your pos_source_table.
# It should be no smaller than 20.
pos_sample_size = 50

# neg_sample_size is the number of records to select from your neg_source_table.
# It should be no smaller than 20.
neg_sample_size = 1000

# You can, if needed, select a new random seed value for count sampling. This is used
# to make sure that, for a given population, you'll always get the same sample set
# for repeatability. You probably don't need to change this in most cases.
 seed = 1234567890

# [join_cols_by_table.table_name] allows you to add arbitrary data from other sources
# to your target_table. it should be comprised of two keys:
#   - join_id - the field to use to join to your cohort table. It should :probably: 
#   be the primary ref.
#   - included_cols - a list of columns to join from the table in question. An array
#   of one string string will be included as the column name. An array of two strings
#   will create an alias, like "table_name.first_string AS second_str"
# You can join as many tables as you like.
[join_cols_by_table.study__encounter]
join_id = "encounter_ref"
included_cols = [
    ["gender"], 
    ["race_display", "race"]
]

```