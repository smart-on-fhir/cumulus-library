# Cumulus Library
1. [installation](#setup)
2. [CLI](#CLI)
   * [core](#cumulus-library-core)
   * [clean](#cumulus-library-clean)
3. [upload](#upload)
4. [patient counts](#patient-counts)
  * US-CORE FHIR
    * [Patient](#count_core_patient)
    * [Encounter *by day*](#count_core_encounter_day)
    * [Encounter *by month*](#count_core_encounter_month)
    * [DocumentReference *by month*](#count_core_documentreference_month)
    * [Condition *by month*](#count_core_condition_icd10_month)
    * [Observation lab *by month*](#count_core_observation_lab_month)

------------------------------------------------------------------------   
# setup
Install library dependencies with pip: `pip install -e .`.

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

------------------------------------------------------------------------
# CLI
[cumulus-library](https://github.com/smart-on-fhir/library/cumulus-library/cli.py) will create views for each study with data from [Cumulus ETL](https://github.com/smart-on-fhir/cumulus-etl).

You can provide arguments several ways, in this order of priority:
- As command line arguments
  - `cumulus-library -s s3://your-etl-bucket/path/to/library/storage -b`
- Via environment variables
  - i.e. `set CUMULUS_LIBRARY_S3=s3://your-etl-bucket/path/to/library/storage -b; cumulus-library -b`
- For connection info only, via AWS CLI config/credential files
  - See the [AWS Docs](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html) for more info

By default, passing the `-b` parameter will build tables and views in Athena. You can use `-t [studyname]` to specify a study by name; if not provided, all cumulus infrastructure will be built.

For more information, see the help built into cumulus-library by running `python library/cumulus-library -h`


*default*: clean and rebuild all study views
- `core` US-CORE FHIR views
- `covid` study views 
- `suicidality` study views
- `lyme` study views
- `clean` drop views

## cumulus-library core 
* Define FHIR vocabularies and concepts 
* Define SITE specific terms such as codes for **Emergency Department Note**
* Create common SQL views from FHIR US-CORE resources.
    * `core_patient` 
    * `core_encounter`
    * `core_documentreference`
    * `core_observation_lab`
    * `core_condition`
  
## cumulus-library clean 
- `clean` will remove any existing views

------------------------------------------------------------------------
# upload

To export data for sharing with other institutions, add the `-e` flag to any invocation of `library/cumulus-library`, which will write data to `./data_export/[study_name]`. This data is reset on every export run, and is programmatically crawled, so don't use this directory as a location for any scratch work/analysis.

You can load these files into your analysis toolset of choice, or a locally hosted copy of the cumulus dashboard. Parquet will be more performant for most cases, but csvs are also exported for convenience.

To send data to the cumulus aggregator for sharing with other locations, invoke the bulk upload script with your provided username and site id, i.e. `data_export/bulk_upload.py -u [user_name] -i [site_id]`, or by setting the `CUMULUS_AGGREGATOR_USER` and `CUMULUS_AGGREGATOR_ID` environment variables and running `data_export/bulk_upload.py`.

------------------------------------------------------------------------
# PATIENT COUNTS

Cumulus calculates the **patient count** for every patient group using the [SQL CUBE function](https://prestodb.io/docs/current/sql/select.html#group-by-clause).  
*THRESHOLDS* are applied to ensure *no patient group has < 10 patients*.
 
Patient count can be the 
- number of **unique patients**
- number of **unique patient encounters**
- number of **unique patient encounters with documented medical note**
- Other types of counts are also possible, these are the most common.

Example

    #count (total) =
    #count patients age 9 =
    #count patients age 9 and rtPCR POS =
    #count patients age 9 and rtPCR NEG =
    #count rtPCR POS =
    #count rtPCR NEG =

[SQL CUBE](https://prestodb.io/docs/current/sql/select.html#group-by-clause) produces a "[Mathematical Power Set](http://en.wikipedia.org/wiki/Power_set)" for every patient subgroup.  
These numbers are useful inputs for maths that leverage [Joint Probability Distributions](https://en.wikipedia.org/wiki/Joint_probability_distribution). 

Examples: 

- [Odds Ratio](https://en.wikipedia.org/wiki/Odds_ratio) of patient group A vs B 
- [Relative Risk Ratio](https://en.wikipedia.org/wiki/Relative_risk) of patient group A vs B
- [Chi-Squared Test](https://en.wikipedia.org/wiki/Chi-squared_test) significance of difference between patient groups
- [Entropy and Mutual Information](https://en.wikipedia.org/wiki/Mutual_information) (core information theory measures) 
- [Decision Tree](https://en.wikipedia.org/wiki/Decision_tree) sorts patients into different predicted classes, with visualized tree   
- [Naive Bayes Classifier](https://en.wikipedia.org/wiki/Naive_Bayes_classifier) very fast probabilistic classifier
- others



------------------------------------------------------------------------ 

## count_core_condition_icd10_month
| Variable	|	Description	|
| --------	|	--------	|
| cnt	|	Count	|
| cond_month	|	Month condition recorded	|
| cond_code_display	|	Condition code	|
| enc_class_code	|	Encounter Code (Healthcare Setting)	|


## count_core_documentreference_month
| Variable	|	Description	|
| --------	|	--------	|
| cnt	|	Count	|
| author_month	|	Month document was authored	|
| enc_class_code	|	Encounter Code (Healthcare Setting)	|
| doc_type_display	|	Type of Document (display)	|


## count_core_encounter_day
| Variable	|	Description	|
| --------	|	--------	|
| cnt	|	Count	|
| enc_class_code	|	Encounter Code (Healthcare Setting)	|
| start_date	|	Day patient encounter started	|


## count_core_encounter_month
| Variable	|	Description	|
| --------	|	--------	|
| cnt	|	Count	|
| enc_class_code	|	Encounter Code (Healthcare Setting)	|
| start_month	|	Month patient encounter started	|
| age_at_visit	|	Patient Age at Encounter	|
| gender	|	Biological sex at birth	|
| race_display	|	Patient reported race	|
| postalcode3	|	Patient 3 digit zip	|


## count_core_observation_lab_month
| Variable	|	Description	|
| --------	|	--------	|
| cnt	|	Count	|
| lab_month	|	Month of lab result	|
| lab_code	|	Laboratory Code	|
| lab_result_display	|	Laboratory result	|
| enc_class_code	|	Encounter Code (Healthcare Setting)	|


## count_core_patient
| Variable	|	Description	|
| --------	|	--------	|
| cnt	|	Count	|
| gender	|	Biological sex at birth	|
| age	|	Age in years calculated since DOB	|
| race_display	|	Patient reported race	|
| postalcode3	|	Patient 3 digit zip	|

