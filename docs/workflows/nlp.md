---
title: NLP
parent: Workflows
grand_parent: Library
nav_order: 3
# audience: clinical researchers, IRB reviewers
# type: reference
---

# NLP

This document aims to provide help in configuring natural language processing (NLP) workflows as
part of a Cumulus Library study.

## Use cases

The coded metadata in FHIR inevitably only tells part of the story.
There will always be some interesting extra bits in the clinical notes (which aren't easily exposed
in the core tables for study use).
In those cases, you'll want to define some NLP workflows that can run a custom prompt and clinical
notes through an LLM.

The results of that prompt will be parsed into a structured format and written back into an Athena
table for the rest of your study to reference.

As an example, you might be working on a study related to infectious diseases.
You could write an NLP workflows with a prompt like "Analyze the following clinical note and
indicate if the patient has a flu."
(That's an overly simple prompt, but prompt engineering is outside the scope of this guide.)

Then you could capture the results of that prompt as `"has_flu": True/False`,
which will get stored in an Athena table of your choosing (along with some metadata about the
results).

And then the rest of your study SQL can reference those results to further categorize / process
patients.

## Configuring an NLP Workflow

The config you reference in your study manifest is expected to contain a number of 
field definitions. We **strongly** recommend starting from the below template, which
contains details on the expectations of each value.

```toml
# This is a config file for defining one or more NLP tasks that will create tables in your study.

# config_type should be "nlp" - we use this to distinguish from other configurable builders
config_type="nlp"

# You define NLP tasks/tables with the 'tables' dictionary.
# The keys in the table dictionary define your table names, and the study name will
# automatically be prepended from the prefix in your manifest.
# So an entry like [tables.my_table] results in a table like `my_study__my_table`.
[tables.table_1]

# `version` will be a number that you will increment every time you change the table definition
# in a clinically-relevant way. i.e. if you change the prompt or the response schema, you should
# bump the version. This version is used as part of the NLP caching layer, so if you don't bump it,
# you will get incorrect results from the cache.
version = 0

# `system_prompt` defines the system prompt to the model. System prompts are where you put your
# main instructions to the model. The system prompt will be the same for all the notes.
# If you put `%JSON-SCHEMA%` somewhere in the prompt, it will be replaced by a JSON schema of your
# expected response (see below). This is recommended, to help some models return the right format.
system_prompt = """
You are a clinical chart reviewer. Your task is to blah blah blah...

Pydantic Schema:
%JSON-SCHEMA%
"""

# `user_prompt` defines the user prompt to the model. This is where you might put the data to be
# processed.
# If you put `%CLINICAL-NOTE%` somewhere in the prompt, it will be replaced by the current clinical
# note. If you do not define this field or leave it empty, just the text of the clinical note will
# be used (i.e. the default is `"%CLINICAL-NOTE%"`).
user_prompt = ""

# `response_schema` points at a file adjacent to this workflow file. It contains a JSON schema
# for the expected response from the LLM. If the LLM's response cannot be parsed into this format,
# it will be ignored. This schema also helps defines the resulting table schema.
# Often, studies will use a bit of Python code to generate this file from some Pydantic models.
# See the example_nlp study or other studies for comparison.
response_schema = "schema.json"

# `select_by_word` allows you to define words that will cause notes to be included for NLP
# processing. If no selection rules are defined, all notes are selected by default. But once a
# selection rule is defined, a note that matches any of them will be included.
# Words are matched at word boundaries (e.g. "fever" will not match "fevers").
# Words can include whitespace.
select_by_word = ["fever", "severe cold"]

# `select_by_regex` allows you to define regular expressions that will cause notes to be included
# for NLP processing, just like `select_by_word`. Again, regexes will match at word boundaries.
# The regex rules are standard Python rules: https://docs.python.org/3/library/re.html
select_by_regex: ["fevers?"]

# `select_by_table` allows you to define a table name that will be searched for note identifiers,
# to choose which notes are selected for NLP processing. Column names that will be found include
# `note_ref`, `documentreference_id`, `documentreference_ref`, `diagnosticreport_id`, and
# `diagnosticreport_ref`. This is useful if your study wants to calculate which notes are most
# interesting to examine for NLP, then creates a table that holds such references.
select_by_table: "my_study__interesting_notes"

# `reject_by_word` allows you to define words that cause notes to **not** be included for NLP
# processing, even if selected by one of the above selection options.
reject_by_word: ["kidney", "fake"]

# `reject_by_regex` allows you to define regular expressions that will cause notes to **not** be
# included for NLP processing, even if selected by one of the above selection options.
reject_by_regex: ["kidneys?"]

# The `shared` dictionary allows you to share configuration between multiple tables.
# For example, you may use the same selection/rejection criteria for multiple related NLP tables.
# Or the same prompts, just with different schemas. Using the `shared` dictionary greatly reduces
# your configuration burden.
[shared]

# The following fields can be defined here, and will be used if a table does not define its own
# value for it. (i.e. these are fallback/default values)
system_prompt: "Default system prompt"
user_prompt: "Default user prompt"
select_by_word: ["default", "words"]
select_by_regex: ["default", "regex"]
select_by_table: "default_table"
reject_by_word: ["default", "words"]
reject_by_regex: ["default", regex"]
```

## Result Table Format

Tables created by the NLP workflow will have the following fields:

- note_ref: a string like `DocumentReference/abc`
- encounter_ref: a string like `Encounter/abc`
- subject_ref: a string like `Patient/abc`
- generated_on: a string with the time-of-result-generation in UTC
- task_version: a number with the `version` field from the NLP table config
- model: a string with the name of the model used
- system_fingerprint: some LLMs provide a fingerprint, which can help track server-side changes
- result: a struct, with its shape defined by the `response_schema` field

## Running an NLP Workflow

NLP workflows require extra configuration that normal study workflows do not.
Namely, how to connect to the LLM of choice.
You can pass these to Cumulus Library when building a study and any NLP workflows will use them.

- `--note-dir=PATH`: point this at the root folder of your FHIR NDJSON note documents
- `--etl-phi-dir=PATH`: point this at the PHI folder that you use for Cumulus ETL (the third
  argument when running the ETL process) - NLP caches are kept here as well as the information
  needed to compare anonymized IDs with the original note IDs
- `--nlp-model=MODEL`: choose a model to use for this run; pass `help` to get a list of options
- `--nlp-provider=PROVIDER`: choose a provider to use for this run; can be `azure` or `bedrock` but
  defaults to `local` (a locally run NLP)
  - If using `azure`, you also need to set the `AZURE_OPENAI_API_KEY` and `AZURE_OPENAI_ENDPOINT`
    environment variables.
  - If using `bedrock`, you need to make sure that your AWS configuration can be found (probably
    by setting the `AWS_PROFILE` environment variable).
  - If using `local`, see below for instructions on using Docker to run local LLMs
- `--azure-deployment=NAME`: when using the Azure provider, you may need to provide a deployment
  name (defaults to model name)
- `--batch-nlp`: if set, NLP will be done in batch mode, which can take up to a day to finish, but
  will be much cheaper
- `--clean-nlp`: if set, previous NLP results for the workflow will be deleted first
- `--no-nlp-stats`: if set, note and token stats will not be printed to the console

## Using a Local LLM

TODO MIKE
