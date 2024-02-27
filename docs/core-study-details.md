---
title: Core Study Details
parent: Library
nav_order: 5
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

## core count tables

### core__count_condition_month
|      Column      | Type  |Description|
|------------------|-------|-----------|
|cnt               |bigint |count      |
|cond_category_code|varchar|Encounter Code (Healthcare Setting)|
|cond_month        |varchar|Month condition recorded|
|cond_code_display |varchar|Condition code|

### core__count_documentreference_month
|     Column      | Type  |Description|
|-----------------|-------|-----------|
|cnt              |bigint |Count      |
|doc_type_display |varchar|Type of Document (display)|
|author_month     |varchar|Month document was authored|
|enc_class_display|varchar|Encounter Code (Healthcare Setting)|

### core__count_encounter_enc_type_month
|     Column      | Type  |Description|
|-----------------|-------|-----------|
|cnt              |bigint |Count      |
|enc_class_display|varchar|Encounter Code (Healthcare Setting)|
|enc_type_display |varchar|Encounter Type|
|start_month      |varchar|Month encounter recorded|

### core__count_encounter_month
|     Column      | Type  |Description|
|-----------------|-------|-----------|
|cnt              |bigint |Count      |
|start_month      |varchar|Month encounter recorded|
|enc_class_display|varchar|Encounter Code (Healthcare Setting|
|age_at_visit     |varchar|Patient Age at Encounter|
|gender           |varchar|Biological sex at birth|
|race_display     |varchar|Patient reported race|
|ethnicity_display|varchar|Patient reported ethnicity|

### core__count_encounter_priority_month
|       Column       | Type  |Description|
|--------------------|-------|-----------|
|cnt                 |bigint |Count      |
|enc_class_display   |varchar|Encounter Code (Healthcare Setting)|
|enc_priority_display|varchar|Encounter Priority|
|start_month         |varchar|Month encounter recorded|

### core__count_encounter_service_month
|      Column       | Type  |Description|
|-------------------|-------|-----------|
|cnt                |bigint |Count      |
|enc_class_display  |varchar|Encounter Code (Healthcare Setting)|
|enc_service_display|varchar|Encounter Service|
|start_month        |varchar|Month encounter recorded|

### core__count_encounter_type
|       Column       | Type  |Description|
|--------------------|-------|-----------|
|cnt                 |bigint |Count      |
|enc_class_display   |varchar|Encounter Code (Healthcare Setting)|
|enc_type_display    |varchar|Encounter Type|
|enc_service_display |varchar|Encounter Service|
|enc_priority_display|varchar|Encounter Priority|

### core__count_encounter_type_month
|       Column       | Type  |Description|
|--------------------|-------|-----------|
|cnt                 |bigint |Count      |
|enc_class_display   |varchar|Encounter Code (Healthcare Setting)|
|enc_type_display    |varchar|Encounter Type|
|enc_service_display |varchar|Encounter Service|
|enc_priority_display|varchar|Encounter Priority|
|start_month         |varchar|Month encounter recorded|

### core__count_medicationrequest_month
|     Column     | Type  |Description|
|----------------|-------|-----------|
|cnt             |bigint |Count      |
|status          |varchar|Perscribing event state|
|intent          |varchar|Medication order kind|
|authoredon_month|varchar|Month medication request issued|
|rx_display      |varchar|Medication Name|

### core__count_observation_lab_month
|      Column      | Type  |Description|
|------------------|-------|-----------|
|cnt               |bigint |Count      |
|lab_month         |varchar|Month of lab result|
|lab_code          |varchar|Lab result coding|
|lab_result_display|varchar|Lab result display text|
|enc_class_display |varchar|Encounter Code (Healthcare Setting)|

### core__count_patient
|     Column      | Type  |Description|
|-----------------|-------|-----------|
|cnt              |bigint |Count      |
|gender           |varchar|Biological sex at birth|
|race_display     |varchar|Patient reported race|
|ethnicity_display|varchar|Patient reported ethnicity|

## core base tables

### core__condition
|     Column     | Type  |Description|
|----------------|-------|-----------|
|id              |varchar|           |
|category_code   |varchar|           |
|category_display|varchar|           |
|code            |varchar|           |
|code_system     |varchar|           |
|code_display    |varchar|           |
|subject_ref     |varchar|           |
|encounter_ref   |varchar|           |
|condition_ref   |varchar|           |
|recordeddate    |date   |           |
|recorded_week   |date   |           |
|recorded_month  |date   |           |
|recorded_year   |date   |           |

### core__condition_codable_concepts_all
|  Column   | Type  |Description|
|-----------|-------|-----------|
|id         |varchar|           |
|code       |varchar|           |
|code_system|varchar|           |
|display    |varchar|           |

### core__condition_codable_concepts_display
|  Column   | Type  |Description|
|-----------|-------|-----------|
|id         |varchar|           |
|code       |varchar|           |
|code_system|varchar|           |
|display    |varchar|           |

### core__documentreference
|       Column       | Type  |Description|
|--------------------|-------|-----------|
|id                  |varchar|           |
|doc_type_code       |varchar|           |
|doc_type_code_system|varchar|           |
|doc_type_display    |varchar|           |
|status              |varchar|           |
|docstatus           |varchar|           |
|encounter_ref       |varchar|           |
|author_date         |date   |           |
|author_week         |date   |           |
|author_month        |date   |           |
|author_year         |date   |           |
|subject_ref         |varchar|           |
|doc_ref             |varchar|           |

### core__documentreference_dn_type
|  Column   | Type  |Description|
|-----------|-------|-----------|
|id         |varchar|           |
|code       |varchar|           |
|code_system|varchar|           |
|display    |varchar|           |

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
|        Column        |   Type    |Description|
|----------------------|-----------|-----------|
|id                    |varchar    |           |
|enc_class_code        |varchar(6) |           |
|enc_class_display     |varchar(21)|           |
|status                |varchar    |           |
|type_code             |varchar    |           |
|type_code_system      |varchar    |           |
|sevicetype_code       |varchar    |           |
|sevicetype_code_system|varchar    |           |
|priority_code         |varchar    |           |
|priority_code_system  |varchar    |           |
|reasoncode_code       |varchar    |           |
|reasoncode_code_system|varchar    |           |
|age_at_visit          |bigint     |           |
|start_date            |date       |           |
|end_date              |date       |           |
|start_week            |date       |           |
|start_month           |date       |           |
|start_year            |date       |           |
|subject_ref           |varchar    |           |
|encounter_ref         |varchar    |           |
|gender                |varchar    |           |
|race_display          |varchar    |           |
|ethnicity_display     |varchar    |           |
|postalcode3           |varchar    |           |

### core__encounter_dn_priority
|  Column   | Type  |Description|
|-----------|-------|-----------|
|id         |varchar|           |
|code       |varchar|           |
|code_system|varchar|           |
|display    |varchar|           |

### core__encounter_dn_reasoncode
|  Column   | Type  |Description|
|-----------|-------|-----------|
|id         |varchar|           |
|code       |varchar|           |
|code_system|varchar|           |
|display    |varchar|           |

### core__encounter_dn_servicetype
|  Column   | Type  |Description|
|-----------|-------|-----------|
|id         |varchar|           |
|code       |varchar|           |
|code_system|varchar|           |
|display    |varchar|           |

### core__encounter_dn_type
|  Column   | Type  |Description|
|-----------|-------|-----------|
|id         |varchar|           |
|code       |varchar|           |
|code_system|varchar|           |
|display    |varchar|           |

### core__encounter_type
|        Column        |   Type    |Description|
|----------------------|-----------|-----------|
|id                    |varchar    |           |
|enc_class_code        |varchar(6) |           |
|enc_class_display     |varchar(21)|           |
|enc_type_system       |varchar    |           |
|enc_type_code         |varchar    |           |
|enc_type_display      |varchar    |           |
|enc_service_system    |varchar    |           |
|enc_service_code      |varchar    |           |
|enc_service_display   |varchar    |           |
|enc_priority_system   |varchar    |           |
|enc_priority_code     |varchar    |           |
|enc_priority_display  |varchar    |           |
|enc_reasoncode_code   |varchar    |           |
|enc_reasoncode_display|varchar    |           |
|status                |varchar    |           |
|age_at_visit          |bigint     |           |
|start_date            |date       |           |
|end_date              |date       |           |
|start_week            |date       |           |
|start_month           |date       |           |
|start_year            |date       |           |
|subject_ref           |varchar    |           |
|encounter_ref         |varchar    |           |
|gender                |varchar    |           |
|race_display          |varchar    |           |
|ethnicity_display     |varchar    |           |
|postalcode3           |varchar    |           |

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

### core__lib_transactions
|    Column     |    Type    |Description|
|---------------|------------|-----------|
|study_name     |varchar     |           |
|library_version|varchar     |           |
|status         |varchar     |           |
|event_time     |timestamp(3)|           |

### core__medication
|   Column    | Type  |Description|
|-------------|-------|-----------|
|id           |varchar|           |
|encounter_ref|varchar|           |
|patient_ref  |varchar|           |
|code         |varchar|           |
|display      |varchar|           |
|code_system  |varchar|           |
|userselected |boolean|           |

### core__medicationrequest
|       Column       | Type  |Description|
|--------------------|-------|-----------|
|id                  |varchar|           |
|status              |varchar|           |
|intent              |varchar|           |
|authoredon          |date   |           |
|authoredon_month    |date   |           |
|category_code       |varchar|           |
|category_code_system|varchar|           |
|rx_code_system      |varchar|           |
|rx_code             |varchar|           |
|rx_display          |varchar|           |
|subject_ref         |varchar|           |

### core__medicationrequest_dn_category
|  Column   | Type  |Description|
|-----------|-------|-----------|
|id         |varchar|           |
|code       |varchar|           |
|code_system|varchar|           |
|display    |varchar|           |

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
|             Column             | Type  |Description|
|--------------------------------|-------|-----------|
|id                              |varchar|           |
|category_code                   |varchar|           |
|category_code_system            |varchar|           |
|status                          |varchar|           |
|observation_code                |varchar|           |
|observation_code_system         |varchar|           |
|interpretation_code             |varchar|           |
|interpretation_code_system      |varchar|           |
|interpretation_display          |varchar|           |
|valuecodeableconcept_code       |varchar|           |
|valuecodeableconcept_code_system|varchar|           |
|valuecodeableconcept_display    |varchar|           |
|obs_date                        |date   |           |
|obs_week                        |date   |           |
|obs_month                       |date   |           |
|obs_year                        |date   |           |
|subject_ref                     |varchar|           |
|encounter_ref                   |varchar|           |
|observation_ref                 |varchar|           |

### core__observation_dn_category
|  Column   | Type  |Description|
|-----------|-------|-----------|
|id         |varchar|           |
|code       |varchar|           |
|code_system|varchar|           |
|display    |varchar|           |

### core__observation_dn_code
|  Column   | Type  |Description|
|-----------|-------|-----------|
|id         |varchar|           |
|code       |varchar|           |
|code_system|varchar|           |
|display    |varchar|           |

### core__observation_dn_interpretation
|  Column   | Type  |Description|
|-----------|-------|-----------|
|id         |varchar|           |
|code       |varchar|           |
|code_system|varchar|           |
|display    |varchar|           |

### core__observation_dn_valuecodeableconcept
|  Column   | Type  |Description|
|-----------|-------|-----------|
|id         |varchar|           |
|code       |varchar|           |
|code_system|varchar|           |
|display    |varchar|           |

### core__observation_lab
|        Column        | Type  |Description|
|----------------------|-------|-----------|
|id                    |varchar|           |
|lab_code              |varchar|           |
|lab_code_system       |varchar|           |
|category_code         |varchar|           |
|category_code_system  |varchar|           |
|lab_result_code       |varchar|           |
|lab_result_code_system|varchar|           |
|lab_result_display    |varchar|           |
|lab_date              |date   |           |
|lab_week              |date   |           |
|lab_month             |date   |           |
|lab_year              |date   |           |
|status                |varchar|           |
|subject_ref           |varchar|           |
|encounter_ref         |varchar|           |
|observation_ref       |varchar|           |

### core__observation_vital_signs
|             Column             | Type  |Description|
|--------------------------------|-------|-----------|
|id                              |varchar|           |
|observation_code                |varchar|           |
|observation_code_system         |varchar|           |
|category_code                   |varchar|           |
|category_code_system            |varchar|           |
|valuecodeableconcept_code       |varchar|           |
|valuecodeableconcept_code_system|varchar|           |
|valuecodeableconcept_display    |varchar|           |
|status                          |varchar|           |
|interpretation_code             |varchar|           |
|interpretation_code_system      |varchar|           |
|interpretation_display          |varchar|           |
|obs_date                        |date   |           |
|obs_week                        |date   |           |
|obs_month                       |date   |           |
|obs_year                        |date   |           |
|subject_ref                     |varchar|           |
|encounter_ref                   |varchar|           |
|observation_ref                 |varchar|           |

### core__patient
|     Column      | Type  |Description|
|-----------------|-------|-----------|
|id               |varchar|           |
|gender           |varchar|           |
|birthdate        |date   |           |
|postalcode3      |varchar|           |
|subject_ref      |varchar|           |
|race_display     |varchar|           |
|ethnicity_display|varchar|           |

### core__patient_ext_ethnicity
|     Column      |   Type    |Description|
|-----------------|-----------|-----------|
|id               |varchar    |           |
|system           |varchar(11)|           |
|ethnicity_code   |varchar    |           |
|ethnicity_display|varchar    |           |

### core__patient_ext_race
|   Column   |   Type    |Description|
|------------|-----------|-----------|
|id          |varchar    |           |
|system      |varchar(11)|           |
|race_code   |varchar    |           |
|race_display|varchar    |           |

### core__study_period
|      Column      |   Type    |Description|
|------------------|-----------|-----------|
|start_date        |date       |           |
|start_week        |date       |           |
|start_month       |date       |           |
|end_date          |date       |           |
|age_at_visit      |bigint     |           |
|author_date       |date       |           |
|author_week       |date       |           |
|author_month      |date       |           |
|author_year       |date       |           |
|gender            |varchar    |           |
|race_display      |varchar    |           |
|ethnicity_display |varchar    |           |
|subject_ref       |varchar    |           |
|encounter_ref     |varchar    |           |
|status            |varchar    |           |
|doc_ref           |varchar    |           |
|diff_enc_note_days|bigint     |           |
|enc_class_code    |varchar(6) |           |
|enc_class_display |varchar(21)|           |
|doc_type_code     |varchar    |           |
|doc_type_display  |varchar    |           |
|ed_note           |boolean    |           |
