-- #############################################################
-- Encounter
-- https://build.fhir.org/ig/HL7/US-Core/StructureDefinition-us-core-encounter.html

DROP TABLE IF EXISTS core_encounter;
DROP TABLE IF EXISTS core_encounter_patient;

CREATE TABLE core_encounter AS
WITH temp_encounter AS (
    SELECT DISTINCT
        e.period,
        e.status,
        e.class,
        e.type,
        e.subject.reference AS subject_ref,
        e.id AS encounter_id,
        date(from_iso8601_timestamp(e.period."start")) AS start_date,
        date(from_iso8601_timestamp(e.period."end")) AS end_date,
        concat('Encounter/', e.id) AS encounter_ref
    -- , reasonCode
    -- , dischargeDisposition
    FROM encounter AS e
)

SELECT DISTINCT
    e.class AS enc_class,
    e.type AS enc_type,
    date_diff('year', date(p.birthdate), e.start_date) AS age_at_visit,
    date_trunc('day', e.start_date) AS start_date,
    date_trunc('day', e.end_date) AS end_date,
    date_trunc('week', e.start_date) AS start_week,
    date_trunc('month', e.start_date) AS start_month,
    date_trunc('year', e.start_date) AS start_year,
    e.subject_ref,
    e.encounter_ref,
    e.encounter_id
FROM temp_encounter AS e, core_patient AS p
WHERE
    e.subject_ref = p.subject_ref
    AND start_date BETWEEN date('2016-06-01') AND current_date;

CREATE OR REPLACE VIEW join_encounter_patient AS
SELECT
    e.enc_class,
    e.enc_type,
    e.age_at_visit,
    e.start_date,
    e.end_date,
    e.start_week,
    e.start_month,
    e.start_year,
    e.subject_ref,
    e.encounter_ref,
    e.encounter_id,
    enc_class.code AS enc_class_code,
    p.gender,
    p.race,
    p.postalcode3
FROM core_encounter AS e, core_patient AS p
WHERE e.subject_ref = p.subject_ref;


CREATE OR REPLACE VIEW count_core_encounter_month AS
WITH powerset AS (
    SELECT
        count(DISTINCT e.subject_ref) AS cnt_subject,
        count(DISTINCT e.encounter_id) AS cnt_encounter,
        e.enc_class.code AS enc_class_code,
        e.start_month,
        e.age_at_visit,
        p.gender,
        p.race,
        p.postalcode3
    FROM core_encounter AS e, core_patient AS p
    WHERE e.subject_ref = p.subject_ref
    GROUP BY
        cube(
            e.enc_class,
            e.start_month,
            e.age_at_visit,
            p.gender,
            p.race,
            p.postalcode3
        )
)

SELECT DISTINCT
    powerset.cnt_encounter AS cnt,
    powerset.enc_class_code,
    powerset.start_month,
    powerset.age_at_visit,
    powerset.gender,
    race.display AS race_display,
    powerset.postalcode3
FROM powerset
WHERE powerset.cnt_subject >= 10
ORDER BY
    powerset.start_month ASC, powerset.enc_class_code ASC, powerset.age_at_visit ASC;

CREATE OR REPLACE VIEW count_core_encounter_day AS
WITH powerset AS (
    SELECT
        count(DISTINCT e.subject_ref) AS cnt_subject,
        count(DISTINCT e.encounter_id) AS cnt_encounter,
        enc_class.code AS enc_class_code,
        e.start_date
    FROM core_encounter AS e, core_patient AS p
    WHERE e.subject_ref = p.subject_ref
    GROUP BY cube(e.enc_class, e.start_date)
)

SELECT DISTINCT
    cnt_encounter AS cnt,
    enc_class_code,
    start_date
FROM powerset
WHERE cnt_subject >= 10
ORDER BY start_date ASC, enc_class_code ASC;
