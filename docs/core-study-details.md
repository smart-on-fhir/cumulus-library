---
title: Core Study Details
parent: Library
nav_order: 6
# audience: clinical researchers, IRB reviewers
# type: reference
---

# Core study details

The core study aims to provide the following features:

- **FHIR US Core V4 Profile data** - For resources that are supported by Cumulus, 
we attempt to extract all required/extensible fields from a FHIR dataset, if present
- **Flattened FHIR resources** - The core study provided flattened tables from nested
FHIR resources, making queries easier to construct without having to worry about the
nuances of conditional unnesting against potentially missing data
- **Summary Exports** - The core study will provide some basic count data across the
entire cohort that you've processed via ETL, which can be useful as a verification step
for validating the integrity of the data you're extracting from your EHR system

If you are authoring a study, and are focused only on clinical analysis (i.e. you
aren't working on data quality/data governance issues), we **strongly** recommend you
use the core study as the starting point for your own work. See 
[Creating Studies](./example.md)
for more information.

## Design philosophy

One of the main goals of the core study is to abstract away complex sql details, and to
make some basic joins between tables to make it easy for someone who does not speak FHIR
to intuit what information is in tables.

As a result, we've made several intentional decisions about how the tables in this study
are structured:

- We never include nested data directly, instead always extracting data from the FHIR data
  in a safe way.
  - We often store these nested contents seperately in a denormalized table, with `_dn_` in
    the name - analysts can safely ignore these tables in most cases
  - Flattening the nested data means there will often be more than one row per unique ID.
    You should take this into account when doing statistics, and make sure you're counting
    unique IDs, not bare rows
- We prefer optimizing for access speed over space efficiency
  - Many of our tables are effectively materialized views, joined from two or more tables
    and stored as a unique instance. Since some of these operations take a long time
    (5 minutes for large observation tables), we'll do it once rather than on access.
    - This means that if you update the base FHIR tables, you should rerun the core study
      as well to update the data.

# Table format

Unless otherwise noted, column names correspond to a FHIR path relative to the resource
table in question, with resources delineated by underscores. So, 'category_code' in the 
Condition table would correspond to the code element inside of the category element of
the Condition resource.

Common exceptions to this rule:

- `cnt` is a Cumulus specific notation for a column that contains counts of resources.
- `age_at_visit` is calculated from a Patient's birthDate and the period.start.day of an Encounter
- `postalcode_3` is calculated from a Patient's address.postalCode
- The Encounter resource includes several elements from the referenced Patient (`gender`, `postalcode_3`,
  and the US Core race and ethnicity extensions) that are commonly used in informatics analysis
- Fields that contain date values are presented by different rounding methods (day, week, month, year) for convenience for various binning strategies
- We construct `*_ref` fields from a resource's base id field (i.e creating a `Patient/123456` `patient_ref` field from an `id` of `123456`), to make it easier to join data with reference fields in other resources.
- `aux_*` fields are additional calculated fields, derived from the raw FHIR data

The core tables include all FHIR required/must support fields noted in the
[FHIR resource profiles(http://hl7.org/fhir/us/core/STU4/artifacts.html#structures-resource-profiles).
Additionally, there are fields that are useful to informatics analysis that are commonly available
from EHRs, but are not guaranteed to be populated, so consult with your research partners if
you are authoring a study using some of these data elements.

## Completion tracking

If not all the resources for a given encounter are loaded into the database yet,
that encounter is considered "incomplete" and may be left out of the core tables.

You can see which encounters were ignored as incomplete by examining the
`core__incomplete_encounters` table which holds the ID of all incomplete encounters.

Usually, you can resolve this by making sure you've run the ETL process
for each of the following encounter-linked resources:
- AllergyIntolerance
- Condition
- DiagnosticReport
- DocumentReference
- EpisodeOfCare
- MedicationRequest
- MedicationDispense
- Observation
- Procedure
- ServiceRequest
- Specimen

## Optional fields

The `core` study includes several fields that are considered optional by US Core.
Such fields are not even marked as "Must Support" in US Core
and are defined in the base FHIR spec only.

These are included due to their general utility in clinical informatics studies.
In practice, we have found that this data is usually present in FHIR exports from
EHR systems, but note that it is not guaranteed that a study relying on these
fields will work across multiple institutions without some additional work.

Per resource, the optional fields are as follows:
- AllergyIntolerance
  - type
  - category
  - criticality
  - encounter
  - recordedDate
  - reaction.substance
  - reaction.severity
- Condition
  - encounter
  - recordedDate
- DiagnosticReport
  - conclusionCode
  - specimen
- DocumentReference
  - docStatus
- Encounter
  - serviceType
  - priority
- EpisodeOfCare
  - everything (it has no US Core profile)
- Location
  - identifier
  - alias
  - type
  - partOf
- Observation - laboratory
  - encounter
- Observation - vital signs
  - encounter
  - valueCodeableConcept
  - interpretation
- Organization
  - identifier
  - type
  - alias
  - partOf
- Practitioner
  - identifier
  - active
  - qualification.code
- PractitionerRole
  - identifier
  - active
- Procedure
  - category
  - encounter
- ServiceRequest
  - encounter
  - specimen
- Specimen
  - status

## Deprecation Notice

The `core__observation` table is currently deprecated, and will be removed in a
future version. When possible, use one of the targeted profile tables (labs,
vital signs) instead.

## core count tables

### core__count_allergyintolerance_month

|            Column            | Type  |Description|
|------------------------------|-------|-----------|
|cnt                           |bigint |           |
|category                      |varchar|           |
|recordeddate_month            |varchar|           |
|code_display                  |varchar|           |
|reaction_manifestation_display|varchar|           |


### core__count_condition_month

|            Column            | Type  |Description|
|------------------------------|-------|-----------|
|cnt               |bigint |count      |
|category_code     |varchar|Encounter Code (Healthcare Setting)|
|recordeddate_month|varchar|Month condition recorded|
|code_display      |varchar|Condition code display|
|code              |varchar|Condition code|


### core__count_diagnosticreport_month

| Column           | Type  | Description                     |
|------------------|-------|---------------------------------|
| cnt              |bigint | Count                           |
| category_display |varchar| Service category                |
| code_display     |varchar| Code for this diagnostic report |
| issued_month     |varchar| When this version was made      |


### core__count_documentreference_month

|     Column      | Type  |Description|
|-----------------|-------|-----------|
|cnt              |bigint |Count      |
|type_display |varchar|Type of Document (display)|
|author_month|varchar|Month document was authored|
|class_display|varchar|Encounter Code (Healthcare Setting)|


### core__count_encounter_all_types

|       Column       | Type  |Description|
|--------------------|-------|-----------|
|cnt                 |bigint |Count      |
|class_display       |varchar|Encounter Code (Healthcare Setting)|
|type_display        |varchar|Encounter Type|
|servicetype_display |varchar|Encounter Service|
|priority_display    |varchar|Encounter Priority|


### core__count_encounter_all_types_month

|       Column       | Type  |Description|
|--------------------|-------|-----------|
|cnt                 |bigint |Count      |
|class_display       |varchar|Encounter Code (Healthcare Setting)|
|type_display        |varchar|Encounter Type|
|servicetype_display |varchar|Encounter Service|
|priority_display    |varchar|Encounter Priority|
|period_start_month  |varchar|Month encounter recorded|


### core__count_encounter_month

|      Column      | Type  |Description|
|------------------|-------|-----------|
|cnt               |bigint |Count      |
|period_start_month|varchar|Month encounter recorded|
|class_display     |varchar|Encounter Code (Healthcare Setting)|
|age_at_visit      |varchar|Patient Age at Encounter|
|gender            |varchar|Biological sex at birth|
|race_display      |varchar|Patient reported race|
|ethnicity_display |varchar|Patient reported ethnicity|


### core__count_encounter_priority_month

|       Column       | Type  |Description|
|--------------------|-------|-----------|
|cnt                 |bigint |Count      |
|class_display       |varchar|Encounter Code (Healthcare Setting)|
|priority_display    |varchar|Encounter Priority|
|period_start_month  |varchar|Month encounter recorded|


### core__count_encounter_service_month
|       Column      | Type  |Description|
|-------------------|-------|-----------|
|cnt                |bigint |Count      |
|class_display      |varchar|Encounter Code (Healthcare Setting)|
|servicetype_display|varchar|Encounter Service|
|period_start_month |varchar|Month encounter recorded|


### core__count_encounter_type_month

|      Column      | Type  |Description|
|------------------|-------|-----------|
|cnt               |bigint |           |
|class_display     |varchar|           |
|type_display      |varchar|           |
|period_start_month|varchar|           |


### core__count_medicationdispense_category_month

|       Column       | Type  |Description|
|--------------------|-------|-----------|
|cnt                 |bigint |           |
|status              |varchar|           |
|category_display    |varchar|           |
|whenhandedover_month|varchar|           |


### core__count_medicationdispense_dosage_month

|       Column       | Type  |Description|
|--------------------|-------|-----------|
|cnt                 |BIGINT |           |
|dosage_route_text   |VARCHAR|           |
|dosage_timing_text  |VARCHAR|           |
|dosage_dose_unit    |VARCHAR|           |
|whenhandedover_month|VARCHAR|           |


### core__count_medicationdispense_month

|       Column       | Type  |Description|
|--------------------|-------|-----------|
|cnt                 |bigint |           |
|status              |varchar|           |
|whenhandedover_month|varchar|           |
|medication_display  |varchar|           |


### core__count_medicationdispense_type_month

|       Column       | Type  |Description|
|--------------------|-------|-----------|
|cnt                 |bigint |           |
|status              |varchar|           |
|type_display        |varchar|           |
|whenhandedover_month|varchar|           |


### core__count_medicationrequest_dispense_month

|           Column            | Type  |Description|
|-----------------------------|-------|-----------|
|cnt                          |BIGINT |           |
|intent                       |VARCHAR|           |
|dispense_refills_allowed     |VARCHAR|           |
|expected_supply_duration_unit|VARCHAR|           |
|authoredon_month             |VARCHAR|           |


### core__count_medicationrequest_dosage_month

|       Column        | Type  |Description|
|---------------------|-------|-----------|
|cnt                  |bigint |           |
|dosage_route_display |varchar|           |
|dosage_timing_text   |varchar|           |
|dosage_as_needed_bool|varchar|           |
|authoredon_month     |varchar|           |


### core__count_medicationrequest_month

|      Column      | Type  |Description|
|------------------|-------|-----------|
|cnt               |bigint |Count      |
|status            |varchar|Perscribing event state|
|intent            |varchar|Medication order kind|
|authoredon_month  |varchar|Month medication request issued|
|medication_display|varchar|Medication Name|


### core__count_observation_lab_month

|           Column           | Type  |Description|
|----------------------------|-------|-----------|
|cnt                         |bigint |Count      |
|effectivedatetime_month     |varchar|Month of lab result|
|observation_code            |varchar|Lab result coding|
|valuecodeableconcept_display|varchar|Lab result display text|
|class_display               |varchar|Encounter Code (Healthcare Setting)|


### core__count_patient

|     Column      | Type  |Description|
|-----------------|-------|-----------|
|cnt              |bigint |Count      |
|gender           |varchar|Biological sex at birth|
|race_display     |varchar|Patient reported race|
|ethnicity_display|varchar|Patient reported ethnicity|


### core__count_procedure_month

|        Column         | Type  |Description|
|-----------------------|-------|-----------|
|cnt                    |bigint |           |
|category_display       |varchar|           |
|code_display           |varchar|           |
|performeddatetime_month|varchar|           |


### core__count_servicerequest_month

|     Column     | Type  |Description|
|----------------|-------|-----------|
|cnt             |bigint |           |
|category_display|varchar|           |
|code_display    |varchar|           |
|authoredon_month|varchar|           |


### core__count_specimen_month

|   Column   | Type  |Description|
|------------|-------|-----------|
|cnt         |bigint |           |
|type_display|varchar|           |


## core base tables

### core__allergyintolerance

|            Column            |  Type   |Description|
|------------------------------|---------|-----------|
|id                            |varchar|           |
|clinicalStatus_code           |varchar|           |
|verificationStatus_code       |varchar|           |
|type                          |varchar|           |
|category                      |varchar|           |
|criticality                   |varchar|           |
|code_code                     |varchar|           |
|code_system                   |varchar|           |
|code_display                  |varchar|           |
|recordeddate                  |timestamp|           |
|recordeddate_week             |date     |           |
|recordeddate_month            |date     |           |
|recordeddate_year             |date     |           |
|reaction_row                  |bigint   |           |
|reaction_substance_code       |varchar|           |
|reaction_substance_system     |varchar|           |
|reaction_substance_display    |varchar|           |
|reaction_manifestation_code   |varchar|           |
|reaction_manifestation_system |varchar|           |
|reaction_manifestation_display|varchar|           |
|reaction_severity             |varchar|           |
|allergyintolerance_ref        |varchar|           |
|patient_ref                   |varchar|           |
|encounter_ref                 |varchar|           |


### core__allergyintolerance_dn_clinical_status

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__allergyintolerance_dn_code

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__allergyintolerance_dn_reaction_manifestation

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|row         |bigint |           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__allergyintolerance_dn_reaction_substance

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|row         |bigint |           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__allergyintolerance_dn_verification_status

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__condition

|        Column         |  Type   |Description|
|-----------------------|---------|-----------|
|id                     |varchar|           |
|category_code          |varchar|           |
|category_system        |varchar|           |
|category_display       |varchar|           |
|code                   |varchar|           |
|system                 |varchar|           |
|code_display           |varchar|           |
|subject_ref            |varchar|           |
|encounter_ref          |varchar|           |
|condition_ref          |varchar|           |
|recordeddate           |timestamp|           |
|recordeddate_week      |date     |           |
|recordeddate_month     |date     |           |
|recordeddate_year      |date     |           |
|onsetdateTime          |timestamp|           |
|abatementdateTime      |timestamp|           |
|clinicalStatus_code    |varchar|           |
|verificationStatus_code|varchar|           |


### core__condition_codable_concepts_all

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__condition_codable_concepts_display

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__condition_dn_category

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|row         |bigint |           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__condition_dn_clinical_status

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__condition_dn_verification_status

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__diagnosticreport

|          Column           |  Type   |Description|
|---------------------------|---------|-----------|
|id                         |varchar|           |
|status                     |varchar|           |
|category_code              |varchar|           |
|category_system            |varchar|           |
|category_display           |varchar|           |
|code_code                  |varchar|           |
|code_system                |varchar|           |
|code_display               |varchar|           |
|effectivedateTime          |timestamp|           |
|effectivedateTime_day      |date     |           |
|effectivedateTime_week     |date     |           |
|effectivedateTime_month    |date     |           |
|effectivedateTime_year     |date     |           |
|effectivePeriod_start      |timestamp|           |
|effectivePeriod_start_day  |date     |           |
|effectivePeriod_start_week |date     |           |
|effectivePeriod_start_month|date     |           |
|effectivePeriod_start_year |date     |           |
|effectivePeriod_end        |timestamp|           |
|effectivePeriod_end_day    |date     |           |
|effectivePeriod_end_week   |date     |           |
|effectivePeriod_end_month  |date     |           |
|effectivePeriod_end_year   |date     |           |
|issued                     |timestamp|           |
|issued_day                 |date     |           |
|issued_week                |date     |           |
|issued_month               |date     |           |
|issued_year                |date     |           |
|conclusionCode_code        |varchar|           |
|conclusionCode_system      |varchar|           |
|conclusionCode_display     |varchar|           |
|aux_has_text               |boolean  |           |
|diagnosticreport_ref       |varchar|           |
|subject_ref                |varchar|           |
|encounter_ref              |varchar|           |
|performer_ref              |varchar|           |
|specimen_ref               |varchar|           |
|result_ref                 |varchar|           |


### core__diagnosticreport_dn_category

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|row         |bigint |           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__diagnosticreport_dn_code

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__diagnosticreport_dn_conclusioncode

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|row         |bigint |           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__documentreference

|       Column        |  Type   |Description|
|---------------------|---------|-----------|
|id                   |varchar|           |
|status               |varchar|           |
|type_code            |varchar|           |
|type_system          |varchar|           |
|type_display         |varchar|           |
|category_code        |varchar|           |
|docStatus            |varchar|           |
|date                 |timestamp|           |
|author_day           |date     |           |
|author_week          |date     |           |
|author_month         |date     |           |
|author_year          |date     |           |
|format_code          |varchar|           |
|aux_has_text         |boolean  |           |
|subject_ref          |varchar|           |
|encounter_ref        |varchar|           |
|author_ref           |varchar|           |
|documentreference_ref|varchar|           |


### core__documentreference_dn_category

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|row         |bigint |           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__documentreference_dn_format

|Column | Type  |Description|
|-------|-------|-----------|
|id     |varchar|           |
|code   |varchar|           |
|system |varchar|           |
|display|varchar|           |


### core__documentreference_dn_type

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__encounter

|           Column           | Type  |Description|
|----------------------------|-------|-----------|
|id                          |varchar|           |
|status                      |varchar|           |
|class_code                  |varchar|           |
|class_display               |varchar|           |
|type_code                   |varchar|           |
|type_system                 |varchar|           |
|type_display                |varchar|           |
|serviceType_code            |varchar|           |
|serviceType_system          |varchar|           |
|serviceType_display         |varchar|           |
|priority_code               |varchar|           |
|priority_system             |varchar|           |
|priority_display            |varchar|           |
|reasonCode_code             |varchar|           |
|reasonCode_system           |varchar|           |
|reasonCode_display          |varchar|           |
|dischargeDisposition_code   |varchar|           |
|dischargeDisposition_system |varchar|           |
|dischargeDisposition_display|varchar|           |
|age_at_visit                |bigint |           |
|gender                      |varchar|           |
|race_display                |varchar|           |
|ethnicity_display           |varchar|           |
|postalCode_3                |varchar|           |
|period_start_day            |date   |           |
|period_end_day              |date   |           |
|period_start_week           |date   |           |
|period_start_month          |date   |           |
|period_start_year           |date   |           |
|subject_ref                 |varchar|           |
|episodeOfCare_ref           |varchar|           |
|participant_ref             |varchar|           |
|serviceProvider_ref         |varchar|           |
|encounter_ref               |varchar|           |


### core__encounter_dn_dischargedisposition

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__encounter_dn_priority

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|row         |bigint |           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__encounter_dn_reasoncode

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|row         |bigint |           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__encounter_dn_servicetype

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|row         |bigint |           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__encounter_dn_type

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|row         |bigint |           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__episodeofcare

|      Column      |  Type   |Description|
|------------------|---------|-----------|
|id                |varchar|           |
|status            |varchar|           |
|type_code         |varchar|           |
|type_system       |varchar|           |
|type_display      |varchar|           |
|period_start      |timestamp|           |
|period_start_day  |date     |           |
|period_start_week |date     |           |
|period_start_month|date     |           |
|period_start_year |date     |           |
|period_end        |timestamp|           |
|period_end_day    |date     |           |
|period_end_week   |date     |           |
|period_end_month  |date     |           |
|period_end_year   |date     |           |
|episodeofcare_ref |varchar|           |
|patient_ref       |varchar|           |


### core__episodeofcare_dn_type

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|row         |bigint |           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__fhir_act_encounter_code_v3

|Column |   Type    |Description|
|-------|-----------|-----------|
|code   |varchar(6) |           |
|display|varchar(21)|           |


### core__fhir_mapping_code_system_uri

|  Column   |   Type    |Description|
|-----------|-----------|-----------|
|code_system|varchar(6) |           |
|uri        |varchar(37)|           |


### core__fhir_mapping_expected_act_encounter_code_v3

|   Column   |   Type    |Description|
|------------|-----------|-----------|
|expected    |varchar(6) |           |
|found       |varchar(6) |           |
|found_system|varchar(48)|           |


### core__fhir_mapping_resource_uri

| Column |   Type    |Description|
|--------|-----------|-----------|
|resource|varchar(25)|           |
|uri     |varchar(73)|           |


### core__incomplete_encounter

|Column| Type  |Description|
|------|-------|-----------|
|id    |varchar|           |


### core__lib_build_source

|Column| Type  |Description|
|------|-------|-----------|
|stage |varchar|           |
|name  |varchar|           |
|type  |varchar|           |


### core__lib_ref_summary

|   Column    |  Type   |Description|
|-------------|---------|-----------|
|table_name   |varchar|           |
|ref_type     |varchar|           |
|ref_count    |INTEGER  |           |
|delta_percent|DOUBLE   |           |
|event_time   |timestamp|           |


### core__lib_transactions

|    Column     |    Type    |Description|
|---------------|------------|-----------|
|study_name     |varchar     |           |
|library_version|varchar     |           |
|status         |varchar     |           |
|event_time     |timestamp(3)|           |
|message        |varchar     |           |


### core__location

|         Column          | Type  |Description|
|-------------------------|-------|-----------|
|id                       |varchar|           |
|identifier_value         |varchar|           |
|identifier_system        |varchar|           |
|status                   |varchar|           |
|name                     |varchar|           |
|alias                    |varchar|           |
|type_code                |varchar|           |
|type_system              |varchar|           |
|type_display             |varchar|           |
|location_ref             |varchar|           |
|managing_organization_ref|varchar|           |
|part_of_ref              |varchar|           |


### core__location_dn_type

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|row         |bigint |           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__medication_dn_code

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|row         |bigint |           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__medicationdispense

|        Column        |  Type   |Description|
|----------------------|---------|-----------|
|id                    |VARCHAR  |           |
|status                |VARCHAR  |           |
|category_code         |VARCHAR  |           |
|category_system       |VARCHAR  |           |
|category_display      |VARCHAR  |           |
|type_code             |VARCHAR  |           |
|type_system           |VARCHAR  |           |
|type_display          |VARCHAR  |           |
|medication_code       |VARCHAR  |           |
|medication_system     |VARCHAR  |           |
|medication_display    |VARCHAR  |           |
|quantity_value        |DOUBLE   |           |
|quantity_unit         |VARCHAR  |           |
|quantity_system       |VARCHAR  |           |
|quantity_code         |VARCHAR  |           |
|days_supply_value     |DOUBLE   |           |
|days_supply_unit      |VARCHAR  |           |
|days_supply_system    |VARCHAR  |           |
|days_supply_code      |VARCHAR  |           |
|whenPrepared          |TIMESTAMP|           |
|whenPrepared_month    |DATE     |           |
|whenHandedOver        |TIMESTAMP|           |
|whenHandedOver_day    |DATE     |           |
|whenHandedOver_week   |DATE     |           |
|whenHandedOver_month  |DATE     |           |
|whenHandedOver_year   |DATE     |           |
|medicationdispense_ref|VARCHAR  |           |
|subject_ref           |VARCHAR  |           |
|encounter_ref         |VARCHAR  |           |
|medicationrequest_ref |VARCHAR  |           |
|performer_ref         |VARCHAR  |           |


### core__medicationdispense_dn_category

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__medicationdispense_dn_contained_code

|   Column    | Type  |Description|
|-------------|-------|-----------|
|id           |varchar|           |
|row          |bigint |           |
|contained_id |varchar|           |
|resource_type|varchar|           |
|code         |varchar|           |
|system       |varchar|           |
|display      |varchar|           |
|userselected |boolean|           |


### core__medicationdispense_dn_dosage_route

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|row         |bigint |           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__medicationdispense_dn_dose_rate_type

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |VARCHAR|           |
|row         |BIGINT |           |
|dose_row    |BIGINT |           |
|code        |VARCHAR|           |
|system      |VARCHAR|           |
|display     |VARCHAR|           |
|userSelected|BOOLEAN|           |


### core__medicationdispense_dn_inline_code

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__medicationdispense_dn_type

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__medicationdispense_dosageinstruction

|          Column           |  Type   |Description|
|---------------------------|---------|-----------|
|id                         |VARCHAR  |           |
|row                        |BIGINT   |           |
|dose_row                   |BIGINT   |           |
|dosage_sequence            |BIGINT   |           |
|dosage_text                |VARCHAR  |           |
|dosage_patient_instruction |VARCHAR  |           |
|dosage_as_needed_bool      |BOOLEAN  |           |
|dosage_route_text          |VARCHAR  |           |
|dosage_timing_text         |VARCHAR  |           |
|dosage_timing_count        |BIGINT   |           |
|dosage_timing_count_max    |BIGINT   |           |
|dosage_timing_duration     |DOUBLE   |           |
|dosage_timing_duration_max |DOUBLE   |           |
|dosage_timing_duration_unit|VARCHAR  |           |
|dosage_timing_frequency    |BIGINT   |           |
|dosage_timing_frequency_max|BIGINT   |           |
|dosage_timing_period       |DOUBLE   |           |
|dosage_timing_period_max   |DOUBLE   |           |
|dosage_timing_period_unit  |VARCHAR  |           |
|dosage_timing_offset       |BIGINT   |           |
|dosage_timing_bounds_start |DATE     |           |
|dosage_timing_bounds_end   |DATE     |           |
|dosage_dose_type           |VARCHAR  |           |
|dosage_dose_value          |DOUBLE   |           |
|dosage_dose_low_value      |DOUBLE   |           |
|dosage_dose_high_value     |DOUBLE   |           |
|dosage_dose_unit           |VARCHAR  |           |
|dosage_dose_system         |VARCHAR  |           |
|dosage_dose_code           |VARCHAR  |           |
|dosage_dose_rate_type_text |VARCHAR  |           |
|whenHandedOver             |TIMESTAMP|           |
|whenHandedOver_month       |DATE     |           |
|medicationdispense_ref     |VARCHAR  |           |
|subject_ref                |VARCHAR  |           |
|encounter_ref              |VARCHAR  |           |


### core__medicationrequest

|            Column            |  Type   |Description|
|------------------------------|---------|-----------|
|id                            |VARCHAR  |           |
|status                        |VARCHAR  |           |
|intent                        |VARCHAR  |           |
|category_code                 |VARCHAR  |           |
|category_system               |VARCHAR  |           |
|category_display              |VARCHAR  |           |
|status_reason_code            |VARCHAR  |           |
|status_reason_system          |VARCHAR  |           |
|status_reason_display         |VARCHAR  |           |
|status_reason_text            |VARCHAR  |           |
|course_of_therapy_code        |VARCHAR  |           |
|course_of_therapy_system      |VARCHAR  |           |
|course_of_therapy_display     |VARCHAR  |           |
|course_of_therapy_text        |VARCHAR  |           |
|reportedBoolean               |BOOLEAN  |           |
|reported_ref                  |VARCHAR  |           |
|medication_code               |VARCHAR  |           |
|medication_system             |VARCHAR  |           |
|medication_display            |VARCHAR  |           |
|dispense_refills_allowed      |BIGINT   |           |
|dispense_quantity_value       |DOUBLE   |           |
|dispense_quantity_unit        |VARCHAR  |           |
|dispense_quantity_system      |VARCHAR  |           |
|dispense_quantity_code        |VARCHAR  |           |
|expected_supply_duration_value|DOUBLE   |           |
|expected_supply_duration_unit |VARCHAR  |           |
|validity_period_start         |DATE     |           |
|validity_period_end           |DATE     |           |
|authoredOn                    |TIMESTAMP|           |
|authoredOn_month              |DATE     |           |
|medicationrequest_ref         |VARCHAR  |           |
|subject_ref                   |VARCHAR  |           |
|encounter_ref                 |VARCHAR  |           |
|requester_ref                 |VARCHAR  |           |
|prior_prescription_ref        |VARCHAR  |           |


### core__medicationrequest_dn_category

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|row         |bigint |           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__medicationrequest_dn_contained_code

|   Column    | Type  |Description|
|-------------|-------|-----------|
|id           |varchar|           |
|row          |bigint |           |
|code         |varchar|           |
|system       |varchar|           |
|display      |varchar|           |
|userselected |boolean|           |
|contained_id |varchar|           |
|resource_type|varchar|           |


### core__medicationrequest_dn_course_of_therapy

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|row         |bigint |           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__medicationrequest_dn_dosage_route

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|row         |bigint |           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__medicationrequest_dn_dose_rate_type

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |VARCHAR|           |
|row         |BIGINT |           |
|dose_row    |BIGINT |           |
|code        |VARCHAR|           |
|system      |VARCHAR|           |
|display     |VARCHAR|           |
|userSelected|BOOLEAN|           |


### core__medicationrequest_dn_inline_code

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__medicationrequest_dn_status_reason

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|row         |bigint |           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__medicationrequest_dosageinstruction

|          Column           |  Type   |Description|
|---------------------------|---------|-----------|
|id                         |VARCHAR  |           |
|row                        |BIGINT   |           |
|dose_row                   |BIGINT   |           |
|dosage_sequence            |BIGINT   |           |
|dosage_text                |VARCHAR  |           |
|dosage_patient_instruction |VARCHAR  |           |
|dosage_as_needed_bool      |BOOLEAN  |           |
|dosage_route_text          |VARCHAR  |           |
|dosage_timing_text         |VARCHAR  |           |
|dosage_timing_count        |BIGINT   |           |
|dosage_timing_count_max    |BIGINT   |           |
|dosage_timing_duration     |DOUBLE   |           |
|dosage_timing_duration_max |DOUBLE   |           |
|dosage_timing_duration_unit|VARCHAR  |           |
|dosage_timing_frequency    |BIGINT   |           |
|dosage_timing_frequency_max|BIGINT   |           |
|dosage_timing_period       |DOUBLE   |           |
|dosage_timing_period_max   |DOUBLE   |           |
|dosage_timing_period_unit  |VARCHAR  |           |
|dosage_timing_offset       |BIGINT   |           |
|dosage_timing_bounds_start |DATE     |           |
|dosage_timing_bounds_end   |DATE     |           |
|dosage_dose_type           |VARCHAR  |           |
|dosage_dose_value          |DOUBLE   |           |
|dosage_dose_low_value      |DOUBLE   |           |
|dosage_dose_high_value     |DOUBLE   |           |
|dosage_dose_unit           |VARCHAR  |           |
|dosage_dose_system         |VARCHAR  |           |
|dosage_dose_code           |VARCHAR  |           |
|dosage_dose_rate_type_text |VARCHAR  |           |
|authoredOn                 |TIMESTAMP|           |
|authoredOn_month           |DATE     |           |
|medicationrequest_ref      |VARCHAR  |           |
|subject_ref                |VARCHAR  |           |
|encounter_ref              |VARCHAR  |           |


### core__meta_date

| Column |Type|Description|
|--------|----|-----------|
|min_date|date|           |
|max_date|date|           |


### core__meta_version

|       Column       | Type  |Description|
|--------------------|-------|-----------|
|data_package_version|INTEGER|           |


### core__observation

|           Column           |  Type   |Description|
|----------------------------|---------|-----------|
|id                          |varchar|           |
|category_code               |varchar|           |
|category_system             |varchar|           |
|status                      |varchar|           |
|observation_code            |varchar|           |
|observation_system          |varchar|           |
|interpretation_code         |varchar|           |
|interpretation_system       |varchar|           |
|interpretation_display      |varchar|           |
|effectivedateTime           |timestamp|           |
|effectivedateTime_day       |date     |           |
|effectivedateTime_week      |date     |           |
|effectivedateTime_month     |date     |           |
|effectivedateTime_year      |date     |           |
|valueCodeableConcept_code   |varchar|           |
|valueCodeableConcept_system |varchar|           |
|valueCodeableConcept_display|varchar|           |
|valueQuantity_value         |DOUBLE   |           |
|valueQuantity_comparator    |varchar|           |
|valueQuantity_unit          |varchar|           |
|valueQuantity_system        |varchar|           |
|valueQuantity_code          |varchar|           |
|valueString                 |varchar|           |
|dataAbsentReason_code       |varchar|           |
|dataAbsentReason_system     |varchar|           |
|dataAbsentReason_display    |varchar|           |
|subject_ref                 |varchar|           |
|encounter_ref               |varchar|           |
|specimen_ref                |varchar|           |
|observation_ref             |varchar|           |


### core__observation_component_code

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|row         |bigint |           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__observation_component_dataabsentreason

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|row         |bigint |           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__observation_component_interpretation

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|row         |bigint |           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__observation_component_valuecodeableconcept

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|row         |bigint |           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__observation_component_valuequantity

|  Column  | Type  |Description|
|----------|-------|-----------|
|id        |varchar|           |
|row       |bigint |           |
|value     |DOUBLE |           |
|comparator|varchar|           |
|unit      |varchar|           |
|system    |varchar|           |
|code      |varchar|           |


### core__observation_dn_category

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|row         |bigint |           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__observation_dn_code

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__observation_dn_dataabsentreason

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|row         |bigint |           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__observation_dn_interpretation

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|row         |bigint |           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__observation_dn_valuecodeableconcept

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__observation_lab

|           Column           | Type  |Description|
|----------------------------|-------|-----------|
|id                          |varchar|           |
|observation_code            |varchar|           |
|observation_system          |varchar|           |
|category_code               |varchar|           |
|category_system             |varchar|           |
|valueCodeableConcept_code   |varchar|           |
|valueCodeableConcept_system |varchar|           |
|valueCodeableConcept_display|varchar|           |
|effectivedateTime_day       |date   |           |
|effectivedateTime_week      |date   |           |
|effectivedateTime_month     |date   |           |
|effectivedateTime_year      |date   |           |
|status                      |varchar|           |
|subject_ref                 |varchar|           |
|encounter_ref               |varchar|           |
|specimen_ref                |varchar|           |
|observation_ref             |varchar|           |


### core__observation_vital_signs

|           Column           | Type  |Description|
|----------------------------|-------|-----------|
|id                          |varchar|           |
|observation_code            |varchar|           |
|observation_system          |varchar|           |
|category_code               |varchar|           |
|category_system             |varchar|           |
|valueCodeableConcept_code   |varchar|           |
|valueCodeableConcept_system |varchar|           |
|valueCodeableConcept_display|varchar|           |
|valueQuantity_value         |DOUBLE |           |
|valueQuantity_comparator    |varchar|           |
|valueQuantity_unit          |varchar|           |
|valueQuantity_system        |varchar|           |
|valueQuantity_code          |varchar|           |
|status                      |varchar|           |
|interpretation_code         |varchar|           |
|interpretation_system       |varchar|           |
|interpretation_display      |varchar|           |
|effectivedateTime_day       |date   |           |
|effectivedateTime_week      |date   |           |
|effectivedateTime_month     |date   |           |
|effectivedateTime_year      |date   |           |
|subject_ref                 |varchar|           |
|encounter_ref               |varchar|           |
|observation_ref             |varchar|           |


### core__organization

|     Column      | Type  |Description|
|-----------------|-------|-----------|
|id               |varchar|           |
|identifier_value |varchar|           |
|identifier_system|varchar|           |
|active           |boolean|           |
|type_code        |varchar|           |
|type_system      |varchar|           |
|type_display     |varchar|           |
|name             |varchar|           |
|alias            |varchar|           |
|organization_ref |varchar|           |
|part_of_ref      |varchar|           |


### core__organization_dn_type

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|row         |bigint |           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__patient

|     Column      | Type  |Description|
|-----------------|-------|-----------|
|id               |varchar|           |
|gender           |varchar|           |
|birthdate        |date   |           |
|postalCode_3     |varchar|           |
|subject_ref      |varchar|           |
|race_display     |varchar|           |
|ethnicity_display|varchar|           |


### core__patient_ext_ethnicity

|     Column      | Type  |Description|
|-----------------|-------|-----------|
|id               |varchar|           |
|system           |varchar|           |
|ethnicity_code   |varchar|           |
|ethnicity_display|varchar|           |


### core__patient_ext_race

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|system      |varchar|           |
|race_code   |varchar|           |
|race_display|varchar|           |


### core__practitioner

|          Column          | Type  |Description|
|--------------------------|-------|-----------|
|id                        |varchar|           |
|identifier_value          |varchar|           |
|identifier_system         |varchar|           |
|active                    |boolean|           |
|qualification_code_code   |varchar|           |
|qualification_code_system |varchar|           |
|qualification_code_display|varchar|           |
|practitioner_ref          |varchar|           |


### core__practitioner_dn_qualification_code

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|row         |bigint |           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__practitionerrole

|       Column       | Type  |Description|
|--------------------|-------|-----------|
|id                  |varchar|           |
|identifier_value    |varchar|           |
|identifier_system   |varchar|           |
|active              |boolean|           |
|code_code           |varchar|           |
|code_system         |varchar|           |
|code_display        |varchar|           |
|specialty_code      |varchar|           |
|specialty_system    |varchar|           |
|specialty_display   |varchar|           |
|practitionerrole_ref|varchar|           |
|practitioner_ref    |varchar|           |
|organization_ref    |varchar|           |
|location_ref        |varchar|           |


### core__practitionerrole_dn_code

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|row         |bigint |           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__practitionerrole_dn_specialty

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|row         |bigint |           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__procedure

|          Column           |  Type   |Description|
|---------------------------|---------|-----------|
|id                         |varchar|           |
|status                     |varchar|           |
|category_code              |varchar|           |
|category_system            |varchar|           |
|category_display           |varchar|           |
|code_code                  |varchar|           |
|code_system                |varchar|           |
|code_display               |varchar|           |
|performeddateTime          |timestamp|           |
|performeddateTime_day      |date     |           |
|performeddateTime_week     |date     |           |
|performeddateTime_month    |date     |           |
|performeddateTime_year     |date     |           |
|performedPeriod_start      |timestamp|           |
|performedPeriod_start_day  |date     |           |
|performedPeriod_start_week |date     |           |
|performedPeriod_start_month|date     |           |
|performedPeriod_start_year |date     |           |
|performedPeriod_end        |timestamp|           |
|performedPeriod_end_day    |date     |           |
|performedPeriod_end_week   |date     |           |
|performedPeriod_end_month  |date     |           |
|performedPeriod_end_year   |date     |           |
|procedure_ref              |varchar|           |
|subject_ref                |varchar|           |
|encounter_ref              |varchar|           |


### core__procedure_dn_category

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|row         |bigint |           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__procedure_dn_code

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__servicerequest

|           Column           |  Type   |Description|
|----------------------------|---------|-----------|
|id                          |varchar|           |
|status                      |varchar|           |
|intent                      |varchar|           |
|category_code               |varchar|           |
|category_system             |varchar|           |
|category_display            |varchar|           |
|code_code                   |varchar|           |
|code_system                 |varchar|           |
|code_display                |varchar|           |
|occurrencedateTime          |timestamp|           |
|occurrencedateTime_day      |date     |           |
|occurrencedateTime_week     |date     |           |
|occurrencedateTime_month    |date     |           |
|occurrencedateTime_year     |date     |           |
|occurrencePeriod_start      |timestamp|           |
|occurrencePeriod_start_day  |date     |           |
|occurrencePeriod_start_week |date     |           |
|occurrencePeriod_start_month|date     |           |
|occurrencePeriod_start_year |date     |           |
|occurrencePeriod_end        |timestamp|           |
|occurrencePeriod_end_day    |date     |           |
|occurrencePeriod_end_week   |date     |           |
|occurrencePeriod_end_month  |date     |           |
|occurrencePeriod_end_year   |date     |           |
|authoredOn                  |timestamp|           |
|authoredOn_day              |date     |           |
|authoredOn_week             |date     |           |
|authoredOn_month            |date     |           |
|authoredOn_year             |date     |           |
|servicerequest_ref          |varchar|           |
|subject_ref                 |varchar|           |
|encounter_ref               |varchar|           |
|requester_ref               |varchar|           |
|specimen_ref                |varchar|           |


### core__servicerequest_dn_category

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|row         |bigint |           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__servicerequest_dn_code

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__specimen

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|status      |varchar|           |
|type_code   |varchar|           |
|type_system |varchar|           |
|type_display|varchar|           |
|specimen_ref|varchar|           |
|subject_ref |varchar|           |


### core__specimen_dn_type

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |
