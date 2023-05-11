---
title: First Time Setup
parent: Library
nav_order: 1
# audience: clinical researcher or engineer familiar with project, working locally
# type: howto
---

# First Time Setup

## Installation

As a prerequisite, you'll need a copy of python 3.9 or later installed on
your system, and you'll need access to an account with access to AWS cloud services.

If you are using the library just to execute existing studies, you can install
the Cumulus library CLI with `pip install -e .`.

If you are going to be designing queries, you should instead install cumulus with
the dev dependencies, with `pip install -e .[dev]`. After you've done this, you
should install the pre-commit hooks with `pre-commit install`, so that your queries
will have linting automatically run.

You will also need to make sure your machine is configured correctly to talk to AWS
services. See the [AWS setup guide](./aws-setup.md) for more information on this.

## Command line usage

Installing adds a `cumulus-library` command for interacting with
athena. There are three primary modes most users will be interested in:

- `--create` will create a manifest file for you so you can start working on
authoring queires (more information on this in 
[Creating studies](./creating-studies.md)).
- `--build` will create new study tables, replacing previously created versions
(more information on this in [Creating studies](./creating-studies.md)).
- `--export` will output the data in the tables to both a `.csv` and
`.parquet` file. The former is intended for human review, while the latter is
more compressed and should be preferred (if supported) for use when transmitting
data/loading data into analytics packages.

By default, all available studies will be used by build and export., but you can use
or `--target` to specify a specific study to be run. You can use it multiple
times to configure several studies in order. The `vocab`, in particular, can take a
bit of time to generate, so we recommend using targets after your initial configuration.

There are several other options - use `--help` to get a detailed list of commands.

## Example usage: building and exporting the template study

Let's walk through configuring and creating a template study in Athena. With
this completed, you'll be ready to move on to [Creating studies](./creating-studies.md)).

- First, follow the instructions in the readme of the 
[Sample Database](https://github.com/smart-on-fhir/cumulus-library-sample-database),
if you haven't already. Our follown steps assume you use the default names and
deploy in Amazon's US-East zone.
- Configure your system to talk to AWS as mentioned in the [AWS setup guide](./aws-setup.md)
- Now we'll build the tables we'll need to run the template study. The `vocab`
study creates mappings of system codes to strings, and the `core` study creates
tables for commonly used base FHIR resources like `Patient` and `Observation`
using that vocab. To do this, run the following command:
```bash
cumulus-library --build --target vocab --target core
```
This usually takes around five minutes, but once it's done, you won't need build
`vocab` again unless there's a coding system addition, and you'll only need to build
`core` again if data changes (and only if you're using the synthetic dataset). You
should see some progress bars like this while the tables are being created:
```
Uploading vocab__icd data... ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╸ 100% 0:00:00
Creating vocab study in db... ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100% 0:00:00
Creating core study in db... ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100% 0:00:00
```
- Now, we'll build the template study. Run a very similar command to target `template`:
```bash
cumulus-library --build --target template
```
This should be much faster - these tables will be created in around 15 seconds.
- You can use the AWS Athena console to view these tables directly, but you can also
download designated study artifacts. To do the latter, run the following command:
```bash
cumulus-library --build --target export
```
And this will download some example count aggregates to the `data_export` directory
inside of this repository. There's only a few bins, but this will give you an idea
of what kind of output to expect. Here's the first few lines:
```
cnt,influenza_lab_code,influenza_result_display,influenza_test_month
102,,,
70,,NEGATIVE (QUALIFIER VALUE),
70,"{code=92142-9, display=Influenza virus A RNA [Presence] in Respiratory specimen by NAA with probe detection, system=http://loinc.org}",,
70,"{code=92141-1, display=Influenza virus B RNA [Presence] in Respiratory specimen by NAA with probe detection, system=http://loinc.org}",,
69,"{code=92141-1, display=Influenza virus B RNA [Presence] in Respiratory specimen by NAA with probe detection, system=http://loinc.org}",NEGATIVE (QUALIFIER VALUE),
```

## Next steps

Now that you are all set up, you can learn how to [create studies](./creating-studies.md) of your own!
