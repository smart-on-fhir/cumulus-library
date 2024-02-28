CREATE TABLE core__observation_lab AS

SELECT
    co.id,
    co.observation_code AS lab_code,
    co.observation_code_system AS lab_code_system,
    co.category_code,
    co.category_code_system,
    co.valuecodeableconcept_code AS lab_result_code,
    co.valuecodeableconcept_code_system AS lab_result_code_system,
    co.valuecodeableconcept_display AS lab_result_display,
    co.obs_date AS lab_date,
    co.obs_week AS lab_week,
    co.obs_month AS lab_month,
    co.obs_year AS lab_year,
    co.status,
    co.subject_ref,
    co.encounter_ref,
    co.observation_ref
FROM core__observation AS co;

CREATE TABLE core__observation_vital_signs AS
SELECT
    co.id,
    co.observation_code,
    co.observation_code_system,
    co.category_code,
    co.category_code_system,
    co.valuecodeableconcept_code,
    co.valuecodeableconcept_code_system,
    co.valuecodeableconcept_display,
    co.status,
    co.interpretation_code,
    co.interpretation_code_system,
    co.interpretation_display,
    co.obs_date,
    co.obs_week,
    co.obs_month,
    co.obs_year,
    co.subject_ref,
    co.encounter_ref,
    co.observation_ref
FROM core__observation AS co
WHERE co.category_code = 'vital-signs';
