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
[Creating Studies](./creating-studies.md)
for more information.

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

Usually, you can resolve this by running the ETL process again for the encounters,
making sure to include all associated resources.

## Optional fields

The core study includes several fields that are considered optional by FHIR/US core.
These are included due to their general utility in clinical informatics studies.
In practice, we have found that this data is usually present in FHIR exports from
EHR systems, but note that it is not guaranteed that a study relying on these
fields will work across multiple institutions without some additional work.

Per resource, the optional fields are as follows:
- Condition
  - recordedDate
  - encounter_ref
- Encounter
  - serviceType
  - priority
  - reasonCode
  - dischargeDisposition
- Observation
  - dataAbsentReason
- Observation - vital signs
  - valueCodeableConcept
  - interpretation
- Patient
  - US Core race extension
  - US Core ethnicity extension

## Deprecation Notice

The `core__observation` table is currently deprecated, and will be removed in a
future version. When possible, use one of the targeted profile tables (labs,
vital signs) instead.

## core count tables

### core__count_condition_month

|      Column      | Type  |Description|
|------------------|-------|-----------|
|cnt               |bigint |count      |
|category_code     |varchar|Encounter Code (Healthcare Setting)|
|recordeddate_month|varchar|Month condition recorded|
|code_display      |varchar|Condition code|


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

|     Column       | Type  |Description|
|------------------|-------|-----------|
|cnt               |bigint |Count      |
|period_start_month|varchar|Month encounter recorded|
|class_display     |varchar|Encounter Code (Healthcare Setting|
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

|      Column       | Type  |Description|
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


### core__count_medicationrequest_month

|     Column       | Type  |Description|
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


## core base tables

### core__condition

|        Column         | Type  |Description|
|-----------------------|-------|-----------|
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
|recordeddate           |date   |           |
|recordeddate_week      |date   |           |
|recordeddate_month     |date   |           |
|recordeddate_year      |date   |           |
|clinicalstatus_code    |varchar|           |
|verificationstatus_code|varchar|           |


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


### core__documentreference

|       Column        | Type  |Description|
|---------------------|-------|-----------|
|id                   |varchar|           |
|status               |varchar|           |
|type_code            |varchar|           |
|type_system          |varchar|           |
|type_display         |varchar|           |
|category_code        |varchar|           |
|docstatus            |varchar|           |
|date                 |date   |           |
|author_day           |date   |           |
|author_week          |date   |           |
|author_month         |date   |           |
|author_year          |date   |           |
|format_code          |varchar|           |
|subject_ref          |varchar|           |
|encounter_ref        |varchar|           |
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


### core__ed_note

|  Column   |   Type    |Description|
|-----------|-----------|-----------|
|from_system|varchar(3) |           |
|from_code  |varchar(14)|           |
|analyte    |varchar(28)|           |
|code_system|varchar(16)|           |
|code       |varchar(7) |           |
|display    |varchar(33)|           |


### core__encounter

|           Column           |   Type    |Description|
|----------------------------|-----------|-----------|
|id                          |varchar    |           |
|status                      |varchar    |           |
|class_code                  |varchar(6) |           |
|class_display               |varchar(21)|           |
|type_code                   |varchar    |           |
|type_system                 |varchar    |           |
|type_display                |varchar    |           |
|servicetype_code            |varchar    |           |
|servicetype_system          |varchar    |           |
|servicetype_display         |varchar    |           |
|priority_code               |varchar    |           |
|priority_system             |varchar    |           |
|priority_display            |varchar    |           |
|reasoncode_code             |varchar    |           |
|reasoncode_system           |varchar    |           |
|reasoncode_display          |varchar    |           |
|dischargedisposition_code   |varchar    |           |
|dischargedisposition_system |varchar    |           |
|dischargedisposition_display|varchar    |           |
|age_at_visit                |bigint     |           |
|gender                      |varchar    |           |
|race_display                |varchar    |           |
|ethnicity_display           |varchar    |           |
|postalcode_3                |varchar    |           |
|period_start_day            |date       |           |
|period_end_day              |date       |           |
|period_start_week           |date       |           |
|period_start_month          |date       |           |
|period_start_year           |date       |           |
|subject_ref                 |varchar    |           |
|encounter_ref               |varchar    |           |


### core__encounter_dn_dischargedisposition

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|row         |bigint |           |
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

| Column |   Type   |Description|
|--------|----------|-----------|
|expected|varchar(5)|           |
|found   |varchar(5)|           |


### core__fhir_mapping_resource_uri

| Column |   Type    |Description|
|--------|-----------|-----------|
|resource|varchar(25)|           |
|uri     |varchar(73)|           |


### core__incomplete_encounter

|Column| Type  |Description|
|------|-------|-----------|
|id    |varchar|           |


### core__lib_transactions

|    Column     |    Type    |Description|
|---------------|------------|-----------|
|study_name     |varchar     |           |
|library_version|varchar     |           |
|status         |varchar     |           |
|event_time     |timestamp(3)|           |
|message        |varchar     |           |


### core__medication_dn_code

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|row         |bigint |           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__medicationrequest

|      Column      | Type  |Description|
|------------------|-------|-----------|
|id                |varchar|           |
|status            |varchar|           |
|intent            |varchar|           |
|category_code     |varchar|           |
|category_system   |varchar|           |
|category_display  |varchar|           |
|reportedboolean   |boolean|           |
|reported_ref      |varchar|           |
|subject_ref       |varchar|           |
|encounter_ref     |varchar|           |
|authoredon        |date   |           |
|authoredon_month  |date   |           |
|medication_code   |varchar|           |
|medication_system |varchar|           |
|medication_display|varchar|           |


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


### core__medicationrequest_dn_inline_code

|   Column   | Type  |Description|
|------------|-------|-----------|
|id          |varchar|           |
|code        |varchar|           |
|system      |varchar|           |
|display     |varchar|           |
|userselected|boolean|           |


### core__meta_date

| Column |Type|Description|
|--------|----|-----------|
|min_date|date|           |
|max_date|date|           |


### core__meta_version

|       Column       | Type  |Description|
|--------------------|-------|-----------|
|data_package_version|integer|           |


### core__observation

|           Column           | Type  |Description|
|----------------------------|-------|-----------|
|id                          |varchar|           |
|category_code               |varchar|           |
|category_system             |varchar|           |
|status                      |varchar|           |
|observation_code            |varchar|           |
|observation_system          |varchar|           |
|interpretation_code         |varchar|           |
|interpretation_system       |varchar|           |
|interpretation_display      |varchar|           |
|effectivedatetime_day       |date   |           |
|effectivedatetime_week      |date   |           |
|effectivedatetime_month     |date   |           |
|effectivedatetime_year      |date   |           |
|valuecodeableconcept_code   |varchar|           |
|valuecodeableconcept_system |varchar|           |
|valuecodeableconcept_display|varchar|           |
|valuequantity_value         |double |           |
|valuequantity_comparator    |varchar|           |
|valuequantity_unit          |varchar|           |
|valuequantity_system        |varchar|           |
|valuequantity_code          |varchar|           |
|valuestring                 |varchar|           |
|dataabsentreason_code       |varchar|           |
|dataabsentreason_system     |varchar|           |
|dataabsentreason_display    |varchar|           |
|subject_ref                 |varchar|           |
|encounter_ref               |varchar|           |
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
|value     |double |           |
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
|valuecodeableconcept_code   |varchar|           |
|valuecodeableconcept_system |varchar|           |
|valuecodeableconcept_display|varchar|           |
|effectivedatetime_day       |date   |           |
|effectivedatetime_week      |date   |           |
|effectivedatetime_month     |date   |           |
|effectivedatetime_year      |date   |           |
|status                      |varchar|           |
|subject_ref                 |varchar|           |
|encounter_ref               |varchar|           |
|observation_ref             |varchar|           |


### core__observation_vital_signs

|           Column           | Type  |Description|
|----------------------------|-------|-----------|
|id                          |varchar|           |
|observation_code            |varchar|           |
|observation_system          |varchar|           |
|category_code               |varchar|           |
|category_system             |varchar|           |
|valuecodeableconcept_code   |varchar|           |
|valuecodeableconcept_system |varchar|           |
|valuecodeableconcept_display|varchar|           |
|valuequantity_value         |double |           |
|valuequantity_comparator    |varchar|           |
|valuequantity_unit          |varchar|           |
|valuequantity_system        |varchar|           |
|valuequantity_code          |varchar|           |
|status                      |varchar|           |
|interpretation_code         |varchar|           |
|interpretation_system       |varchar|           |
|interpretation_display      |varchar|           |
|effectivedatetime_day       |date   |           |
|effectivedatetime_week      |date   |           |
|effectivedatetime_month     |date   |           |
|effectivedatetime_year      |date   |           |
|subject_ref                 |varchar|           |
|encounter_ref               |varchar|           |
|observation_ref             |varchar|           |


### core__patient

|     Column      | Type  |Description|
|-----------------|-------|-----------|
|id               |varchar|           |
|gender           |varchar|           |
|birthdate        |date   |           |
|postalcode_3     |varchar|           |
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


### core__study_period

|       Column        |   Type    |Description|
|---------------------|-----------|-----------|
|period_start_day     |date       |           |
|period_start_week    |date       |           |
|period_start_month   |date       |           |
|period_end_day       |date       |           |
|age_at_visit         |bigint     |           |
|author_day           |date       |           |
|author_week          |date       |           |
|author_month         |date       |           |
|author_year          |date       |           |
|gender               |varchar    |           |
|race_display         |varchar    |           |
|ethnicity_display    |varchar    |           |
|subject_ref          |varchar    |           |
|encounter_ref        |varchar    |           |
|status               |varchar    |           |
|documentreference_ref|varchar    |           |
|diff_enc_note_days   |bigint     |           |
|enc_class_code       |varchar(6) |           |
|enc_class_display    |varchar(21)|           |
|doc_type_code        |varchar    |           |
|doc_type_display     |varchar    |           |
|ed_note              |boolean    |           |

