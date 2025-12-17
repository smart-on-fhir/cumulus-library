---
title: File Upload
parent: Workflows
grand_parent: Library
nav_order: 1
# audience: clinical researchers, IRB reviewers
# type: reference
---

# File Upload

This document aims to provide help in configuring file upload workflows as part of a
Cumulus Library study

## Use cases

It's very common that you're going to end up with a data artifact that looks like one
of these:

- A list of codes for a specific condition
- A definition set of an entire code system
- Data that was created as part of an external analysis
  (like a chart review or an NLP task)

The goal of this workflow is to abstract away as much of the infrastructure as possible,
and just get your data into a database so you can query it. It can read the following
formats: `.csv`, `.bsv`, `.tsv`, `.xlsx`, and `.parquet`. Under the hood it will
do a conversion to parquet to minimize the disk space used.


{: .note }
If you're running an athena based study, you'll need to have permissions to upload a file
to the S3 bucket your athena database uses. The way that you're granted these permissions
may vary depending on how your organization manages access, so you'll need to talk to your 
IT department about the process for this. It's likely, but not guaranteed, that it will be covered by the same means you use to run library queries in Athena in the first place.

## Configuring a file upload task

The config you reference in your study manifest is expected to contain a number of 
field definitions. We **strongly** recommend starting from the below template, which
contains details on the expectations of each value.

```toml
# This is a config file for uploading one or more files to create tables from in your study.

# This handles the file upload and SQL creation for you. It just needs to know the file path,
# what you want to name it, and an optional param for text files

# config_type should  be "file_upload" - we use this to distinguish from other
# configurable builders
config_type="file_upload"

# The rest of the entries in this workflow should all be in the tables dictionary.
# The keys in the table dictionary define your table names, and the study name will
# automatically be prepended from the prefix in your manifest.
# So an entry like [tables.my_table] results in a table like `my_study__my_table`.
[tables.table_1]
# `file` should be a relative path from your workflow to the file you want to upload
file = "dataset_1.csv"
# in the case of csv/tsv/bsv, we all know the delimiters implied by the filetype 
# are just a suggestion, so if you have a non-comma delimiter, you can specify it
# with the delimiter key.
delimiter = '\t' # \t is shorthand for a tab
# By default, each column in a file upload will be a string type. If you'd like to
# have different data types, you can give a list of types in the order of the columns
# on the datasource.
# These should be hive types. See 
# https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types
# for more info.
col_types = ["STRING","DATE","DOUBLE"]
# multiple uploads can be specified in a single workflow. In 99% of cases, you probably
# want to have one upload workflow, and run it at the start of your manifest, and upload
# everything you might need at once.
[tables.table_2]
# For excel files, we assume we are taking the first sheet in the spreadsheet.
file = "dataset_2.xlsx"
[tables.table_3]
# You probably won't need to convert things directly to parquet, but if you do, there are
# no caveats - what you upload is what you get.
file = "dataset_3.parquet"
# By default, we'll upload a file every time a study runs. If you'd rather upload on demand,
# like in the case of a coding study with a large payload, set always_upload to false.
# You can then use the --force_upload cli flag to manually upload new versions.
always_upload=false
```