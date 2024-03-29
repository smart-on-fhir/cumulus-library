CREATE TABLE core__observation_lab AS

SELECT
    co.id,
    co.observation_code,
    co.observation_code_system,
    co.category_code,
    co.category_code_system,
    co.valueCodeableConcept_code,
    co.valueCodeableConcept_code_system,
    co.valueCodeableConcept_display,
    co.effectiveDateTime_day,
    co.effectiveDateTime_week,
    co.effectiveDateTime_month,
    co.effectiveDateTime_year,
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
    co.valueCodeableConcept_code,
    co.valueCodeableConcept_code_system,
    co.valueCodeableConcept_display,
    co.status,
    co.interpretation_code,
    co.interpretation_code_system,
    co.interpretation_display,
    co.effectiveDateTime_day,
    co.effectiveDateTime_week,
    co.effectiveDateTime_month,
    co.effectiveDateTime_year,
    co.subject_ref,
    co.encounter_ref,
    co.observation_ref
FROM core__observation AS co
WHERE co.category_code = 'vital-signs';
