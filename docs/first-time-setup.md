---
title: First Time Setup
parent: Library
nav_order: 1
# audience: clinical researcher or engineer familiar with project, working locally
# type: howto
---

# First Time Setup

## Prerequisites

1. Python 3.10 or later.
2. Access to an AWS cloud services account.
See the [AWS setup guide](./aws-setup.md) for more information on this.
3. An Athena database populated by
[Cumulus ETL](https://docs.smarthealthit.org/cumulus/etl/)
version 1.0 or later

## Installation

You can install directly from pypi by running:

`pip install cumulus-library`

## Command line usage

Installing adds a `cumulus-library` command for interacting with Athena.
It provides several actions for users:

- `build` will create new study tables, replacing previously created versions
(more information on this in [Creating studies](./creating-studies.md)).
- `clean` will remove studies from Athena, in case you no longer need them
- `export` will output the data in the tables to both a `.csv` and
`.parquet` file. The former is intended for human review, while the latter is
more compressed and should be preferred (if supported) for use when
loading data into analytics packages.
- `import` will re-insert a previously exported study into the database
- `upload` will send data you exported to the
[Cumulus Aggregator](https://docs.smarthealthit.org/cumulus/aggregator/)
- `generate-sql` and `generate-md` both create documentation artifacts, for
users authoring studies
- `version` will provide the installed version of `cumulus-library` and all present studies

You can use `--target` to specify a specific study to be run. You can use it multiple
times to configure several studies in order. You can use `--study-dir` with most arguments
to target a directory where you are working on studies/working with studies that aren't
available to install with `pip`

Several `pip` installable studies will automatically be added to the list of available
studies to run. See [study list](./study-list.md) for more details.

There are several other options - use `--help` to get a detailed list of commands.

## Example usage: building and exporting the core study

Let's walk through configuring and creating the core study in Athena. With
this completed, you'll be ready to move on to [Creating studies](./creating-studies.md)).

- First, follow the instructions in the readme of the 
[Sample Database](https://github.com/smart-on-fhir/cumulus-library-sample-database),
if you haven't already. Our following steps assume you use the default names and
deploy in Amazon's US-East zone.
- Configure your system to talk to AWS as mentioned in the [AWS setup guide](./aws-setup.md)
- Now we'll build the tables we'll need to run the core study. The `core` study 
creates tables for commonly used base FHIR resources like `Patient` and `Observation`.
To do this, run the following command:
```bash
cumulus-library build --target core
```
This usually takes around five minutes, but once it's done, you won't need to build
`core` again unless the data changes.
You should see some progress bars like this while the tables are being created:
```
Creating core study in db... ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100% 0:00:00
```
- You can use the AWS Athena console to view these tables directly, but you can also
download designated study artifacts. To do the latter, run the following command:
```bash
cumulus-library export --target core ./path/to/my/data/dir/
```
And this will download some example count aggregates to the `data_export` directory
inside of this repository. There's only a few tables, but this will give you an idea
of what kind of output to expect.

## Next steps

Now that you are all set up, you can learn how to [create studies](./creating-studies.md) of your own!
