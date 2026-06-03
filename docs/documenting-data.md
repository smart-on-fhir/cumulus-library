---
title: Documenting data
parent: Library
nav_order: 4
# audience: Clinical researcher with low to medium familiarity with project
# type: tutorial
---

# Documenting data

One major aspect of a Cumulus study is that it can be multi-institutional. This means that
someone at another healthcare facility might end up looking at data without having the full
context about what it means, or what your goals are trying to accomplish.

To help your work speak for itself, library studies provide a few different ways to describe their
contents. When you use these approaches, we can do fancy things with your documentation later,
like rendering it in the dashboard in the appropriate contexts. This guide will walk you through
how to set these up for your study.

## Data dictionaries

You may have already created a 
[data dictionary](https://www.nnlm.gov/resources/data/data-glossary/data-dictionary)
as part of your work - it's a common approach to describing what fields in a dataset mean. This
is useful on its own when describing fields found from a data set, but becomes crucial when looking
at values you've derived, like a computable phenotype.

Cumulus supports five kinds of fields for describing items in a data dictionary:
    - `name` : The name of the column in your tables (required)
    - `display` : A user-friendly display name
    - `description` : A brief description of what the field is meant to represent
    - `details` : A longer description of the field, appropriate for things like describing custom values
    - `type`: The format of the data, for use in the dashboard. One of: string, integer, float, boolean, day, week, month, year

Note that you can only have one entry per named column, so make sure that names are unique if they don't apply to the
same concepts

To use a data dictionary, just add the following line to your study's manifest, in the top section before any
of the nested elements:

```toml
data_dictionary = "path/from/manifest/to/dictionary.csv"
```

You can specify a data dictionary in csv, json, or toml formats. Here's examples of each from the `discovery` study:

CSV:
```csv
name,display,description,details,type
table_name,"Table name","FHIR resource",,"string"
column_name,"Column name","FHIR CodeableConcept","This may be a deeply nested path, depending on the location of the element in the FHIR spec","string"
code,"Code","Coding value",,"string"
display,"Display","Display text","When possible, consider using a known good source for this data, since it is not always consistent, depending on the EHR implementation","string"
system,"System","Coding system",,"string"
```

JSON:
```json
{
    "fields": [
        {
            "name": "table_name",
            "display": "Table Name",
            "description": "FHIR resource",
            "details": "",
            "type": "string"
        },
        {
            "name": "column_name",
            "display": "Column Name",
            "description": "FHIR CodeableConcept",
            "details": "This may be a deeply nested path, depending on the location of the element in the FHIR spec",
            "type": "string"
        },
        {
            "name": "code",
            "display": "Code",
            "description": "Coding value",
            "details": "",
            "type": "string"
        },
        {
            "name": "display",
            "display": "Display",
            "description": "Display text",
            "details": "When possible, consider using a known good source for this data, since it is not always consistent, depending on the EHR implementation",
            "type": "string"
        },
        {
            "name": "system",
            "display": "System",
            "description": "Coding system",
            "details": "",
            "type": "string"
        }
    ]
}
```

TOML:
```toml
[[fields]]
name = "table_name"
display = "Table Name"
description = "FHIR resource"
details = ""
type = "string"

[[fields]]
name = "column_name"
display = "Column Name"
description = "FHIR CodeableConcept"
details = "This may be a deeply nested path, depending on the location of the element in the FHIR spec"
type = "string"

[[fields]]
name = "code"
display = "Code"
description = "Coding value"
details = ""
type = "string"

[[fields]]
name = "display"
display = "Display"
description = "Display text"
details = "When possible, consider using a known good source for this data, since it is not always consistent, depending on the EHR implementation"
type = "string"

[[fields]]
name = "system"
display = "System"
description = "Coding system"
details = ""
type = "string"
```

### Which fields should be documented?

The fields Cumulus uses are those related to exported data - so your count outputs should have some of this documentation. But, from
an understandibility perspective, you may want to document your entire study data model. This can be a helpful artifact for the
supplement of a publication, and generally makes your work more parsable by others.

## Study structure

While column-level details get you the 'what', they don't completely capture the 'why', or the 'how', of a study. We have a few
other ways your can provide that data.

For every exported table, you can provide a `description`, which aims to say to others 'what is this table trying to convey'.
You can provide this in one of two ways:

- As an entry in a
[counts workflow](workflows/counts.md),
as shown in this example from the core study, which is the preferred way of doing this:

```toml
config_type = "counts"

[tables.count_allergyintolerance_month]
source_table = "core__allergyintolerance"
description = """A general count of patient allergic reactions by month.

This table provides a summary snapshot of all allergic reactions for the entire patient population
that have been loaded into a database for use by the Cumulus ecosystem. It bins by intolerance category,
intolerance code, the reaction manifestation, and the month the reaction was observed in. 
It is primarily intended as a validation tool to ensure that data has been successfully extracted 
from a source system via the FHIR data format.
"""
primary_id = "patient_ref"
table_cols = [
    ["category", "varchar"],
    ["recordedDate_month", "date"],
    ["code_display", "varchar"],
    ["reaction_manifestation_display", "varchar"],
]
```
- As an entry in the manifest, in the related export action. You can mix tables with and without descriptions.

```toml
study_prefix = "my_study"
[[stages.counts]]
tables = [
    "my_study__no_description",
    {
        name = "my_study__with_description",
        description ="A description of this table"
    }
]
type = "export:counts"
```

In the manifest, you can also provide a description for the study as a whole. This is a good place to describe your research goals,
and also to talk about any relevant inclusion criterion or information about your study cohort(s). Here's how we use this in the
core study:

```toml
study_prefix = "core"
description = """This study aims to provide a flattened set of tables suitable for analysts
from the tables created from FHIR ndjson by Cumulus ETL.

Most studies should use these tables as the base for their work. For more info about the
core study, see https://docs.smarthealthit.org/cumulus/library/core-study-details.html
"""
```