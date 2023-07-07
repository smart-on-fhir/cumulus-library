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
        e.servicetype,
        e.priority,
        e.reasoncode,
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
    e.servicetype AS service_type,
    e.priority,
    e.reasoncode AS reason_code,
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
