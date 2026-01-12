---
title: Valuesets
parent: Workflows
grand_parent: Library
nav_order: 3
# audience: clinical researchers, IRB reviewers
# type: reference
---

# Valueset construction

This document aims to provide help in configuring/running a compound clinical valueset
as part of a Cumulus Library study.

## What is valueset construction?

When we refer to valuesets in this context, we are talking about a grouping of codes
related to a specific clinical topic (in our default case, we think about drugs
with specific ingredients). These usually end up consisting of groups of codes
from one of the major coding systems in the 
[Unified Medical Langauge System](https://www.nlm.nih.gov/research/umls/index.html) (UMLS).
There are several ways a user could select a group of related ingredients:

- Look up a curated valueset from the 
[Valueset Authority Center](https://vsac.nlm.nih.gov/) (VSAC), where organizations curate
and maintain these groupings
- Look up codings in a specific UMLS valueset, by string search
- Manual valueset construction, by hand or from an ontology system, by a set of keywords

The valueset constructor attempts to automate this by allowing you to specify any of these
methods, and then uses the superset of all of all of them to begin a directed search
using the 
[UMLS relations](https://www.ncbi.nlm.nih.gov/books/NBK9685/table/ch03.T.related_concepts_file_mrrel_rrf/), 
following a defined set of rules to determine a group of drugs containing these ingredients.

As an example, in our
[Opioid Valueset](https://github.com/smart-on-fhir/cumulus-library-opioid-valueset/)
study, we use a combination of methods to define a group of drugs containing opiates,
which results in a higher positive identification than any single method by itself

## Configuring a valueset task

The config you reference in your study manifest is expected to contain a number of 
field definitions. We **strongly** recommend starting from the below template, which
contains details on the expectations of each value.

```toml
# This is a config file for generating a compound clinical valueset definition. 

# You can use this to find a set of related concepts from one or more starting
# seeds, which can then be used to filter study cohorts by the presence of these
# codes.

# This attempts to handle the complexities of generating SQL queries for you.
# You'll need to have the UMLS/RxNorm studies installed, and you may need
# to do a bit of preliminary lookup in VSAC to identify oids as a starting point

# config_type should  be "valueset" - we use this to distinguish from other
# configurable builders
config_type = "valueset"

# table_prefix allows you to namespace valueset tables inside your study. If
# supplied, this will name tables like [study name]__[table_prefix]_[various tables].
# This will allow you to add more than one of this kind of configurable builder
# per study

table_prefix = ""

# keyword_file allows you to build a valueset from a group of related substrings.
# It is expected to have one search term per line. Its path will be followed relative
# to the location of this file.

keyword_file = ""

# rules_file allows you to specify a custom group of rules to use to
# traverse the UMLS MRREL tables, by allowing you to list relation types that
# should be included.
# Our default configuration is based on looking up drugs from their ingredients,
# and that is our recommended use case for this, so you probably don't need to provide this
# unless you are experimenting with a different kind of lookup topic.
# Our default config is in 
# cumulus_library/builders/valuesets/lookup_drug_from_ingredient_rules.tsv,
# which you can use as a reference for starting your own. The column names corespond
# to the column definitions in UMLS's MRREL table, with the exception of 'include'
# which should be one of 'Yes' (use the rule), 'No' (Ingore the rule), or 'Keyword'
# (Use the rule, but only if the seed was derived via keyword lookup).

# For most cases, you should not need to supply this.
# expansion_rules_file = "custom_rules.tsv"


# The vsac_stewards section allows you to specify any number of VSAC valueset OIDs
# to use to build your compound valueset. The name on the left will be added to
# some tables to help you identify the source of a particular code. Codes can (and
# likely will) belong to more than one valueset. This will automatically download
# the valueset definitions using the VSAC API.

[vsac_stewards]
stweard_1 = "1.23.456.7.891012.3.4.5678.90"
steward_2 = "1.2.3.4.5.6.7890.1.2.3.456.789.0.1"

# The umls_stewards section allows you to specify lookups in a source vocabulary
# inside of the UMLS MRCONSO table. The string after the period in [umls_stewards.name]
# will be added to some tables to help identify the source of a particular code.
# Two arguments are expected for each UMLS lookup:
#   - "sab": the code used to identify the system in the SAB column of MRCONSO
#   - "search_terms": An array of words to use for identifying topics by substring

[umls_stewards.steward_3]
sab = "MED-RT"
search_terms = ["Asprin"]
[umls_stewards.steward_4]
sab = "MED-RT"
search_terms = ["Tylenol", "Ibuprofin"]
```

## Using the constructed outputs:

Valueset construction creates a lot of tables, but there are two primary ones you can
consider to be the outputs of the process:

- `{study_prefix}__valuesets` - this table contains all of the starting codes from the various systems
  you specified in your configuration
- `{study_prefix}__combined_ruleset` - this table contains all of the final relations of valueset seeds
  to targets defined by a particular ruleset

The first table can be used as a debug tool/documentation for what terms are being used
for lookup, and the second one can be used to select targeted data from UMLS. For our
default use case, the most common way to do this is to use the 'to' direction of the
relationship, by taking distinct string values of the field `str2`, as your list of
drugs related to the initial ingredient set.

A brief synopsis of the tables in question (which match the naming conventions in the
rxnorm tables):

### {study_prefix}__valuesets


|Column | Type  |Description|
|-------|-------|-----------|
|rxcui  |varchar|The unique ID of the concept in RxNorm|
|str    |varchar|The display value of the concept|
|tty    |varchar|The term type code|
|sab    |varchar|The source abbreviation|
|code   |varchar|The identified in the source vocabulary|
|steward|varchar|The steward name you defined in the configuration|

### valueset__combined_ruleset

|Column |   Type    |Description|
|-------|-----------|-----------|
|rxcui1 |varchar    |The unique ID of the first concept|
|rxcui2 |varchar    |The unique ID of the second concept|
|tty1   |varchar    |The term type code of the first concept|
|tty2   |varchar    |The term type code of the second concept|
|rui    |varchar    |The unique ID of the relationship|
|rel    |varchar    |The relationship type code|
|rela   |varchar    |Additional relationship type label|
|str1   |varchar    |The display name of the first concept|
|str2   |varchar    |The display name of the second concept|
|keyword|varchar(10)|The keyword (if any) that caused the relationship to be selected|