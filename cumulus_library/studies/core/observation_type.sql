-- This file should be replaced with programmatic builders at some point.
-- But for now, while we create the cow paths of this pattern, they're manual.
-- We can pave them later.

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
FROM
    core__observation AS co
WHERE
    co.category_code = 'laboratory'
    AND co.category_code_system
    = 'http://terminology.hl7.org/CodeSystem/observation-category';

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
    co.valueQuantity_value,
    co.valueQuantity_comparator,
    co.valueQuantity_unit,
    co.valueQuantity_code_system,
    co.valueQuantity_code,
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
FROM
    core__observation AS co
WHERE
    co.category_code = 'vital-signs'
    AND co.category_code_system
    = 'http://terminology.hl7.org/CodeSystem/observation-category';
