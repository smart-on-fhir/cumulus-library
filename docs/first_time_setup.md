# First Time Setup

## Installation

As a prerequesite, you'll need a copy of python 3.9 or later installed on your system, and you'll need access to an account with access to AWS cloud services.

If you are using the library just to execute existing studies, you can install the Cumulus library CLI with `pip install -e .`.

 If you are going to be designing queries, you should instead install cumulus with the dev dependencies, with `pip install -e .[dev]`. After you've done this, you should install the pre-commit hooks with `pre-commit install`, so that your queries will have linting automatically run.

## AWS setup

Cumulus library executes queries against an Amazon Athena datastore. A [sample project](https://github.com/smart-on-fhir/cumulus-library-sample-database) for creating such a datastore is available for testing purposes if you don't have a running Cumulus ETL instance.

It's recommended that you have a AWS profile configured with a default profile that allows you to connect to your AWS environment with Amazon CLI tools. If one is not present, you'll want to set the following two environment variables to the appropriate values:

`CUMULUS_LIBRARY_PROFILE` : The profile name ('default' is usually the right value)
`CUMULUS_LIBRARY_REGION` : The AWS region your bucket is in

The schema/S3 bucket can be passed via command line arguments, but can be set via the following environment variables for convenience:

`CUMULUS_LIBRARY_SCHEMA` : The name of the schema Athena will use (usually 'default')
`CUMULUS_LIBRARY_S3` : The URL of your S3 bucket

The AWS profile you are using for the library should have the following permissions:
- Glue access to starting/stopping crawlers
- Glue Get/create database permission for your glue catalog and the database
- Glue CRUD permissions for tables and partitions for the catalog, database, and all tables
- Athena CRUD query access and queing permissions
- S3 CRUD access to your ETL bucket (along with any secrets/kms keys)

A [sample IAM policy](./sample_iam_policy.json) is available as a starting point.

## Command line usage

Installing adds a `cumulus-library` command line interface for interacting with athena. There are two primary modes most users will be interested in:

- `-b` or `--build` will remove old study configurations, if they exist, and then recreate views & tables (more information on this [Creating studies](./creating_studies.md)).
- `-e` or `--export` will output the data in the tables to both a `.csv` and `.parquet` file. The former is intended for human review, while the latter is more compressed and should be preferred (if supported) for use when transmitting data/loading data into analytics packages.

By default, all available studies will be used by these commands, but you can use `-t` or `--target` to specify a specific study to be run. You can use it multiple times to configure several studies in order. The `vocab`, in particular, can take a bit of time to generate, so we recommend using targets after your initial configuration.

There are several other options - use `-h` to get a detailed list of commands.

## Example usage: building and exporting the template study

Let's walk through configuring and creating a template study in Athena. With this completed, you'll be ready to move on to [Creating studies](./creating_studies.md)).

- First, follow the instructions in the readme of the [Sample Database](https://github.com/smart-on-fhir/cumulus-library-sample-database), if you haven't already. Our follown steps assume you use the default names and deploy in Amazon's US-East zone.
- Let's configure your environment variables. 
  - On Mac/Linux, add the following lines to your shell config (likely `~/.bashrc` or `~/.zshrc`)

    ```
    export CUMULUS_LIBRARY_S3="s3://cumulus-library-sample-db"
    export CUMULUS_LIBRARY_REGION="us-east-1"
    export CUMULUS_LIBRARY_PROFILE="default"
    export CUMULUS_LIBRARY_WORKGROUP="cumulus_library_sample_db"
    export CUMULUS_LIBRARY_SCHEMA="cumulus_library_sample_db"
    ```
    Once these are set, open a new terminal and verify with `echo $CUMULUS_LIBRARY_S3`

  - On Windows, you can run the following in a command window to set the variables for that session:

    ```
    set CUMULUS_LIBRARY_S3=s3://cumulus-library-sample-db
    set CUMULUS_LIBRARY_REGION=us-east-1
    set CUMULUS_LIBRARY_PROFILE=default
    set CUMULUS_LIBRARY_WORKGROUP=cumulus_library_sample_db
    set CUMULUS_LIBRARY_SCHEMA=cumulus_library_sample_db
    ```

    You can also set these permanently by navigating to Control Panel -> System -> Advance system settings -> Advanced -> Environment variables, and adding each variable inidividually in the UI.
- Now we'll build the tables we'll need to run the template study. The `vocab` study creates mappings of system codes to strings, and the `core` study creates tables for commonly used base FHIR resources like `Patient` and `Observation` using that vocab. To do this, run the following command:
```
./cumulus_library/cli.py --build --target vocab --target core
```
This usually takes around five minutes, but once it's done, you won't need build `vocab` again unless there's a coding system addition, and you'll only need to build `core` again if data changes (and only if you're using the synthetic dataset). You should see some progress bars like this while the tables are being created:
```
Uploading vocab__icd data... ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╸ 100% 0:00:00
Creating vocab study in db... ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100% 0:00:00
Creating core study in db... ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100% 0:00:00
```
- Now, we'll build the template study. Run a very similar command to target `template`:
```
./cumulus_library/cli.py --build --target template
```
This should be much faster - these tables will be created in around 15 seconds.
- You can use the AWS Athena console to view these tables directly, but you can also download designated study artifacts. To do the latter, run the following command:
```
./cumulus_library/cli.py --build --target export
```
And this will download some example count aggregates to the `data_export` directory inside of this repository. There's only a few bins, but this will give you an idea of what kind of output to expect. Here's the first few lines:
```
cnt,influenza_lab_code,influenza_result_display,influenza_test_month
102,,,
70,,NEGATIVE (QUALIFIER VALUE),
70,"{code=92142-9, display=Influenza virus A RNA [Presence] in Respiratory specimen by NAA with probe detection, system=http://loinc.org}",,
70,"{code=92141-1, display=Influenza virus B RNA [Presence] in Respiratory specimen by NAA with probe detection, system=http://loinc.org}",,
69,"{code=92141-1, display=Influenza virus B RNA [Presence] in Respiratory specimen by NAA with probe detection, system=http://loinc.org}",NEGATIVE (QUALIFIER VALUE),
```