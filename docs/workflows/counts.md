---
title: File Upload
parent: Workflows
grand_parent: Library
nav_order: 1
# audience: clinical researchers, IRB reviewers
# type: reference
---

# File Upload

This document aims to provide help in configuring counts workflows as part of a
Cumulus Library study

## Use cases

A counts table is the final data artifact produced by a cumulus study. It is an anonymized
powerset that can be shared with a federated network to enable large population scale
datasets as part of clinical care. A study, in almost every case, should produce :at least:
one of these, and often times dozens of them.

You can create these by hand in SQL, if you're careful, and we do have a python based way
of generating these as well (which this workflow leverages). This workflow aims to abstract
away as much of the implementation details as possible, aiming to let you create as many
tables as you need, in one place, with just a few lines of configuration for each.

## Configuring a counts workflow

The config you reference in your study manifest is expected to contain a number of 
field definitions. We **strongly** recommend starting from the below template, which
contains details on the expectations of each value. We will run some validation to determine
if it's correctly formatted, but starting from the template can help you avoid pitfalls.

```toml
# This is a config file for creating one or more counts tables as part of your study.

# This handles 95% SQL creation for you (you may still want to put a where clause or two
# in, which it provides a hook for. 

# config_type should be "counts" - we use this to distinguish from other
# configurable workflows
type="counts"

[tables.name] # the table in db will be called 'study_prefix__name', snaked cased for sql compatibility
# The following keys must be defined in all cases

source_table= "core__patient"
table_cols = ["gender","birthdate","postalcode_3"]

# The rest of these keys are optional, and target specific use cases. You can uncomment
# the examples as you need them

## description is a user facing string that will be pushed downstream to the cumulus dashboard
#description = "A count of patients by location across the entire hospital service area"

## min_subject allows you to specify the smallest population size to include in a bucket.
## The default is 10. Decreasing this number may affect the identifiability of individuals
## in the dataset, so proceed with caution when changing this.
# min_subject = 20

## where clauses is an array of filter terms.
# where_clauses =[
#     "birthdate IS NOT NULL"
#     "postalcode_3 IS NOT NULL"
# ]

## By default, we will count by subject_ref. If you want to count a different primary ID,
## you can specify that here.
# primary_id = "encounter_ref"

## If you want to join a secondary table, you can specify that here. By default, it will
## be joined on the primary ID. 
# secondary_table="core__encounter"
# secondary_cols = ["age_at_visit"]

## You can specify a second ID type to stratify a table by. If you also have a secondary
## table, it will be added to the join clause with the primary table
# secondary_id = "subject_ref"

## If you want to not join by secondary ID, or just to specify another column to join by,
## you can use this field to declare an alternate join target
# alt_secondary_join_id = "arbitrary_field"

## As an alternate form of specifying where clauses, you can supply one or more lists to
## filter_cols to handle the common case of things like 'only return rows where a column
## contains one of these values', commonly a coding system in our use case. Each
## filter contains the following:
##   - The name of the column
##   - A list of exact matches for the value in that column
##   - A boolean to indicate if nulls should be included or not
# filter_cols = [
#     ['code_system', ['http://terminology.hl7.org/CodeSystem/condition-category'], true]
# ]

#### annotation section ####

## if you want to use a table to annotate rows with labels from another dataset, like
## from a coding system, you can define a count annotation source. All parameters,
## except alt_target, are required.

# [ table.name.annotation ]

## field is the column from the primary table to use to join with the annotating table
# field='loinc_code'

## join_table is the table to use as the annotating table
# join_table='loinc.loinc_groups'

## join_field is the field to join on from the annotation table.
# join_field= 'code'

## columns is a list of columns to bring over as annotations. Each column is a tuple
## of three values:
##   - the column name
##   - the sql datatype to use for the column
##   - a name to use as an alias, or None
# columns = [
#  [ "display", "varchar", None ],
#  [ "system", "varchar", "code_system"]
# ]

## alt target lets you specify a different column from the annotation table to use to
## join with the primary table. If not specified, it's assume they share the same
## column name.
# alt_target = "code"

#### end annotation section ####

# This is just an example of how you'd chain mutliple tables together in one file.
[tables.other_name]
source_table= "core__condition"
table_cols = ["code","recordeddate_month"]
```

As a less abstract example of usage, here is the workflow we use for unit testing,
which exercises each of the individual parameters to create 8 counts tables:

```toml
config_type = "counts"
[tables.basic_count]
source_table = "core__patient"
table_cols = ["gender","birthdate","postalcode_3"]

[tables.wheres]
source_table = "core__patient"
table_cols = ["gender","birthdate","postalcode_3"]
where_clauses = [
    "gender = 'female'"
]

[tables.wheres_min_subject]
source_table = "core__patient"
table_cols = ["gender","birthdate","postalcode_3"]
where_clauses = [
    "gender = 'female'"
]
min_subject = 5

[tables.primary_id]
source_table = "core__observation"
table_cols = ["status","observation_code"]
primary_id = "observation_ref"

[tables.secondary_table]
source_table = "core__patient"
table_cols = ["gender","birthdate","postalcode_3"]
secondary_table = 'core__encounter'
secondary_cols = ["status"]

[tables.annotated]
source_table = "core__condition"
table_cols = ["code", "clinicalstatus_code"]
min_subject = 2

[tables.annotated.annotation]
field = "code"
join_table = '"main"."snomed"'
join_field = "code"
columns = [
    ["system","varchar"],
    ["display","varchar", "display_name"]
]

[tables.filtered]
source_table = "core__condition"
table_cols = ["code", "clinicalstatus_code"]
min_subject = 2
filter_cols = [
    ["clinicalstatus_code",["resolved"],false]
]
```

### Join interactions in detail

Since count table joins are a little complex, here's a few usage examples, using
count_encounter (with  a subject, encounter,  participant, and serviceprovider ref) 
as the primary table, and core__condition (with a subject, encounter, and condition ref) 
as a secondary.

In all cases, `source_table` is "count_encounter", and `table_cols` is ["class_display"]

Assuming no `secondary_table` is defined:
  - `primary_id` not set, no secondary
    - Counts patients by encounter class
  - `primrary_id` set to "serviceprovider_ref", no secondary
    - Counts service providers by the types of encounters they participated in
Assuming `secondary_table` is "count__condition", and `secondary_cols` is ["code"]
  - `primary_id` not set
    - Counts patients by encounter class and condition code (a patient will be counted
      more than once if they have more than one observed condition)
  - `primary_id` not set, `secondary_id` set to "encounter_ref"
    - Counts the unique encounters per patient by encounter class and condition code 
      (a patient who has multiple encounters with the same observed condition will be
      counted more than once, as will a patient with multiple conditions)
  - `primary_id` not set, `secondary_id` set to "condition_ref", `alt_secondary_join_id`
    set to `encounter_ref`
    - Counts the unique instances of a conditions per patient's encounter by encounter class
      and condition code (this would result in the count of conditions likely being 1, unless
      the EHR was configured to reuse a single condition ref across the lifespan of the encounter)
  - `primary_id` set to `encounter_ref`
    - Counts the unique encounters by encounter class and condition code
  - `primary_id` set to `encounter_ref`, `secondary_id` set to `subject_ref`
    - Counts the unique patients per encounter by encounter class and condition code (which
      we would expect to be 1 in all cases, so this would be a mistake in a real world use case)
  - `primary_id` set to `encounter_ref`, `secondary_id` set to "condition_ref", `alt_secondary_join_id`
    set to `subject_ref`
    - Counts the unique encounters and conditions per encounter and patient (another case where we'd
    expect the patient to be 1 in all cases)
