-- #############################################################
-- Encounter
-- https://build.fhir.org/ig/HL7/US-Core/StructureDefinition-us-core-encounter.html

CREATE TABLE core__encounter AS
WITH temp_encounter AS (
    SELECT DISTINCT
        e.period,
        e.status,
        e.class,
        e.type,
        e.serviceType,
        e.priority,
        e.reasonCode,
        e.subject.reference AS subject_ref,
        e.id AS encounter_id,
        date(from_iso8601_timestamp(e.period."start")) AS start_date,
        date(from_iso8601_timestamp(e.period."end")) AS end_date,
        concat('Encounter/', e.id) AS encounter_ref
    FROM encounter AS e
)

SELECT DISTINCT
    e.class AS enc_class,
    e.class.code AS enc_class_code,
    e.class.display AS enc_class_display,
    e.type AS enc_type,
    e.serviceType as service_type,
    e.priority,
    e.reasonCode as reason_code,
    date_diff('year', date(p.birthdate), e.start_date) AS age_at_visit,
    date_trunc('day', e.start_date) AS start_date,
    date_trunc('day', e.end_date) AS end_date,
    date_trunc('week', e.start_date) AS start_week,
    date_trunc('month', e.start_date) AS start_month,
    date_trunc('year', e.start_date) AS start_year,
    e.subject_ref,
    e.encounter_ref,
    e.encounter_id,
    p.gender,
    p.race_display,
    p.ethnicity_display,
    p.postalcode3
FROM temp_encounter AS e, core__patient AS p
WHERE
    e.subject_ref = p.subject_ref
    AND start_date BETWEEN date('2016-06-01') AND current_date;

-- ############################################################################
-- # Encounter.Type, Encounter.serviceType, Encounter.Priority

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
    COALESCE(join_enc_type.as_coding.system, '?')   as enc_type_system,
    COALESCE(join_enc_type.as_coding.code, '?')     as enc_type_code,
    COALESCE(join_enc_type.as_coding.display, '?')  as enc_type_display,
    COALESCE(join_service.as_coding.system, '?')    as enc_service_system,
    COALESCE(join_service.as_coding.code, '?')      as enc_service_code,
    COALESCE(join_service.as_coding.display, '?')   as enc_service_display,
    COALESCE(join_priority.as_coding.system, '?')   as enc_priority_system,
    COALESCE(join_priority.as_coding.code, '?')     as enc_priority_code,
    COALESCE(lower(join_priority.as_coding.display), '?')  as enc_priority_display,
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
