-- counts
DROP TABLE IF EXISTS count_core_patient;
DROP TABLE IF EXISTS count_core_encounter_day;
DROP TABLE IF EXISTS count_core_documentreference_month;
DROP TABLE IF EXISTS count_core_encounter_month;
DROP TABLE IF EXISTS count_core_condition_icd10_month;
DROP TABLE IF EXISTS count_core_observation_lab_month;

-- define FHIR
DROP TABLE IF EXISTS fhir_define;
DROP TABLE IF EXISTS fhir_vocab;

-- define SITE
DROP TABLE IF EXISTS site_ed_note;
DROP TABLE IF EXISTS site_pcr;

-- temp tables if previous run was not clean
DROP TABLE IF EXISTS temp_documentreference;
DROP TABLE IF EXISTS temp_observation_lab;

-- core (materialized views)
DROP TABLE IF EXISTS core_study_period;
DROP TABLE IF EXISTS core_condition;
DROP TABLE IF EXISTS core_documentreference;
DROP TABLE IF EXISTS core_encounter;
DROP TABLE IF EXISTS core_observation_lab;
DROP TABLE IF EXISTS core_patient;

-- metadata
DROP TABLE IF EXISTS core_meta_date;
