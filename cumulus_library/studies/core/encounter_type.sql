-- ############################################################################
-- # Encounter.Type, Encounter.serviceType, Encounter.Priority
--
-- use is OPTIONAL

CREATE TABLE core__encounter_type AS

SELECT DISTINCT
    e.enc_class_code,
    e.enc_class_display,
    coalesce(cet.code_system, 'None') AS enc_type_system,
    coalesce(cet.code, 'None') AS enc_type_code,
    coalesce(cet.display, 'None') AS enc_type_display,
    coalesce(ces.code_system, 'None') AS enc_service_system,
    coalesce(ces.code, 'None') AS enc_service_code,
    coalesce(ces.display, 'None') AS enc_service_display,
    coalesce(cep.code_system, 'None') AS enc_priority_system,
    coalesce(cep.code, 'None') AS enc_priority_code,
    coalesce(cep.display, 'None') AS enc_priority_display,
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
LEFT JOIN core__encounter_dn_type AS cet ON e.encounter_id = cet.id
LEFT JOIN core__encounter_dn_servicetype AS ces ON e.encounter_id = ces.id
LEFT JOIN core__encounter_dn_priority AS cep ON e.encounter_id = cep.id;
