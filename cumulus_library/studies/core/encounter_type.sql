-- ############################################################################
-- # Encounter.Type, Encounter.serviceType, Encounter.Priority
--
-- use is OPTIONAL

CREATE TABLE core__encounter_type AS

SELECT DISTINCT
    e.enc_class_code,
    e.enc_class_display,
    cec.code AS codableconcept_code,
    cec.code_system AS codableconcept_system,
    cec.display AS codableconcept_display,
    e.reason_code,
    e.age_at_visit,
    e.start_date,
    e.end_date,
    e.start_week,
    e.start_month,
    e.start_year,
    e.subject_ref,
    e.encounter_ref,
    e.encounter_id,
    e.gender,
    e.race_display,
    e.ethnicity_display,
    e.postalcode3
FROM core__encounter AS e
LEFT JOIN core__encounter_coding AS cec ON e.encounter_id = cec.id;
