-- ############################################################################
-- # Encounter.Type, Encounter.serviceType, Encounter.Priority
--
-- use is OPTIONAL

create table core__encounter_type as WITH
join_enc_type as (
    select distinct encounter_id, as_coding from core__encounter,
    unnest(enc_type) as t (as_row),
    unnest(as_row.coding) as t (as_coding)
),
join_service as (
    select distinct encounter_id, as_coding from core__encounter,
    unnest(service_type.coding) as t (as_coding)
),
join_priority as (
    select distinct encounter_id, as_coding from core__encounter,
    unnest(priority.coding) as t (as_coding)
)
SELECT DISTINCT
    E.enc_class_code,
    E.enc_class_display,
    COALESCE(join_enc_type.as_coding.system, 'None')   as enc_type_system,
    COALESCE(join_enc_type.as_coding.code, 'None')     as enc_type_code,
    COALESCE(join_enc_type.as_coding.display, 'None')  as enc_type_display,
    COALESCE(join_service.as_coding.system, 'None')    as enc_service_system,
    COALESCE(join_service.as_coding.code, 'None')      as enc_service_code,
    COALESCE(join_service.as_coding.display, 'None')   as enc_service_display,
    COALESCE(join_priority.as_coding.system, 'None')   as enc_priority_system,
    COALESCE(join_priority.as_coding.code, 'None')     as enc_priority_code,
    COALESCE(join_priority.as_coding.display, 'None')  as enc_priority_display,
    E.reason_code,
    E.age_at_visit,
    E.start_date,
    E.end_date,
    E.start_week,
    E.start_month,
    E.start_year,
    E.subject_ref,
    E.encounter_ref,
    E.encounter_id,
    E.gender,
    E.race_display,
    E.ethnicity_display,
    E.postalcode3
FROM        core__encounter as E
LEFT JOIN   join_enc_type   ON E.encounter_id = join_enc_type.encounter_id
LEFT JOIN   join_service    ON E.encounter_id = join_service.encounter_id
LEFT JOIN   join_priority   ON E.encounter_id = join_priority.encounter_id
;
